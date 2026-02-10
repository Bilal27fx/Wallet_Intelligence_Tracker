#!/usr/bin/env python3
"""
Wallet Migration Detector
==========================
Détecte quand un smart wallet transfère >70% de son portefeuille vers un nouveau wallet (EOA),
récupère l'historique du wallet fils, injecte les prix d'achat originaux de la mère via
inherited_price_per_token, puis marque le fils pour scoring (is_scored=0).

Workflow (cf. WALLET_MIGRATION_WORKFLOW.md) :
1. Query smart_wallets INNER JOIN wallets
2. fetch_recent_transactions(wallet_mère, 168h) — filter[operation_types]=send
3. analyze_transfers_for_migration() → destination candidate si >70% portfolio
4. EOA check — is_contract_single() → ignorer si smart contract ou API fail
5. INSERT fils dans wallets (period='migration', is_scored=0, transactions_extracted=0)
6. fetch historique Zerion du fils par token (fungible_id depuis tokens_data)
7. _inherit_prices() → inherited_price_per_token sur direction='in' du fils
8. _save_migration() → wallet_migrations
"""

import sqlite3
import json
import os
import sys
import time
import uuid
import requests
from pathlib import Path
from datetime import datetime, timezone, timedelta
import pandas as pd
from dotenv import load_dotenv

# Import du vérificateur de smart contract
sys.path.append(str(Path(__file__).parent.parent / "token_discovery"))
from smart_contrat_remover import ContractChecker

# Import des fonctions de fetch/stockage transactions Zerion (même dossier)
from live_wallet_transaction_tracker_extractor_zerion import (
    get_token_transaction_history_zerion_full,
    analyze_and_store_complete_transactions
)

_contract_checker = ContractChecker()

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

ROOT = Path(__file__).parent.parent.parent
DB_PATH = ROOT / "data" / "db" / "wit_database.db"

load_dotenv(dotenv_path=ROOT / ".env")
API_KEY_1 = os.getenv("ZERION_API_KEY")
API_KEY_2 = os.getenv("ZERION_API_KEY_2")
API_KEYS = [k for k in [API_KEY_1, API_KEY_2] if k]
_api_index = 0


def _get_api_key():
    return API_KEYS[_api_index]


def _rotate_api_key():
    global _api_index
    _api_index = (_api_index + 1) % len(API_KEYS)


def _zerion_headers():
    return {
        "accept": "application/json",
        "authorization": f"Basic {_get_api_key()}"
    }


# ─────────────────────────────────────────────
# Étape 2 — Zerion : transactions send du wallet mère
# ─────────────────────────────────────────────

def fetch_recent_transactions(wallet_address, hours_lookback=168, retries=3):
    """
    Récupère les transactions de type send du wallet mère sur les 7 derniers jours.
    Zerion trie par date desc → on s'arrête dès que la page dépasse le cutoff.

    Returns:
        list: transactions brutes Zerion
    """
    url = (
        f"https://api.zerion.io/v1/wallets/{wallet_address}/transactions/"
        f"?filter[operation_types]=send&currency=usd&page[size]=100"
    )

    all_transactions = []
    page_cursor = None
    page_count = 0
    max_pages = 10

    while page_count < max_pages:
        paginated_url = url
        if page_cursor:
            paginated_url += f"&page[after]={page_cursor}"

        for attempt in range(retries):
            try:
                response = requests.get(paginated_url, headers=_zerion_headers(), timeout=15)

                if response.status_code == 429:
                    _rotate_api_key()
                    time.sleep(3)
                    continue

                response.raise_for_status()
                data = response.json()
                transactions = data.get("data", [])
                page_count += 1

                if not transactions:
                    return all_transactions

                all_transactions.extend(transactions)

                # Arrêter si la dernière tx de la page dépasse la fenêtre
                oldest_date_str = transactions[-1].get("attributes", {}).get("mined_at", "")
                if oldest_date_str:
                    oldest_dt = datetime.fromisoformat(oldest_date_str.replace("Z", "+00:00"))
                    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_lookback)
                    if oldest_dt < cutoff:
                        return all_transactions

                links = data.get("links", {})
                next_url = links.get("next")
                if not next_url:
                    return all_transactions

                if "page%5Bafter%5D=" in next_url:
                    page_cursor = next_url.split("page%5Bafter%5D=")[1].split("&")[0]
                elif "page[after]=" in next_url:
                    page_cursor = next_url.split("page[after]=")[1].split("&")[0]
                else:
                    return all_transactions

                time.sleep(1.5)
                break

            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(2)
                else:
                    print(f"   Erreur Zerion {wallet_address[:10]}...: {e}")
                    return all_transactions

    return all_transactions


# ─────────────────────────────────────────────
# Étape 3 — Analyse des transfers pour détecter migration
# ─────────────────────────────────────────────

def analyze_transfers_for_migration(transactions, portfolio_value, min_transfer_pct=70, max_days=7):
    """
    Agrège la valeur envoyée par adresse destination via transfer["recipient"].
    Si une destination reçoit >min_transfer_pct du portfolio → migration candidate.

    Règle : toujours lire transfer["recipient"] depuis Zerion, jamais recipient_address BDD.

    Returns:
        dict | None: {destination, total_value, transfer_percentage, tokens_data}
    """
    if portfolio_value <= 0:
        return None

    cutoff = datetime.now(timezone.utc) - timedelta(days=max_days)
    dest_value = {}   # {address: float}
    dest_tokens = {}  # {address: list}

    for tx in transactions:
        tx_date_str = tx.get("attributes", {}).get("mined_at", "")
        if tx_date_str:
            tx_dt = datetime.fromisoformat(tx_date_str.replace("Z", "+00:00"))
            if tx_dt < cutoff:
                continue

        for transfer in tx.get("attributes", {}).get("transfers", []):
            if transfer.get("direction") != "out":
                continue

            recipient = transfer.get("recipient")
            if not recipient:
                continue

            value = float(transfer.get("value") or 0)
            if value <= 0:
                continue

            fungible_info = transfer.get("fungible_info") or {}
            symbol = (fungible_info.get("symbol") or "UNKNOWN").upper()
            implementations = fungible_info.get("implementations") or []
            contract_address = implementations[0].get("address") if implementations else None
            quantity_data = transfer.get("quantity") or {}
            quantity = float(quantity_data.get("numeric", 0)) if isinstance(quantity_data, dict) else 0

            dest_value[recipient] = dest_value.get(recipient, 0) + value
            if recipient not in dest_tokens:
                dest_tokens[recipient] = []
            dest_tokens[recipient].append({
                "symbol": symbol,
                "contract_address": contract_address,
                "fungible_id": fungible_info.get("id"),
                "quantity_transferred": quantity,
                "value_usd": value
            })

    if not dest_value:
        return None

    top_dest = max(dest_value, key=dest_value.get)
    top_value = dest_value[top_dest]
    pct = (top_value / portfolio_value) * 100

    if pct >= min_transfer_pct:
        return {
            "destination": top_dest,
            "total_value": top_value,
            "transfer_percentage": pct,
            "tokens_data": dest_tokens[top_dest]
        }

    return None


# ─────────────────────────────────────────────
# Classe principale
# ─────────────────────────────────────────────

class WalletMigrationDetector:

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path

    # ── Point d'entrée principal ──────────────

    def detect_migrations(self, hours_lookback=168, min_transfer_percentage=70):
        """
        Workflow complet de détection et traitement des migrations.

        Ordre strict (cf. WALLET_MIGRATION_WORKFLOW.md) :
        1. Query smart_wallets INNER JOIN wallets
        2. fetch_recent_transactions (mère)
        3. analyze_transfers_for_migration
        4. EOA check
        5. _insert_fils_wallet
        6. _fetch_fils_history
        7. _inherit_prices (APRÈS fetch, sinon 0 rows updatées)
        8. _save_migration
        """
        print(f"\n=== DÉTECTION DE MIGRATIONS DE WALLETS ===")
        print(f"Période: {hours_lookback}h | Seuil: >{min_transfer_percentage}%")

        migrations_detected = []

        try:
            # Étape 1 — Source : smart_wallets uniquement
            conn = sqlite3.connect(self.db_path)
            wallets_df = pd.read_sql_query(
                "SELECT w.wallet_address, w.total_portfolio_value "
                "FROM wallets w "
                "INNER JOIN smart_wallets sw ON w.wallet_address = sw.wallet_address "
                "WHERE w.is_active = 1 AND w.total_portfolio_value > 0 "
                "ORDER BY w.total_portfolio_value DESC",
                conn
            )
            conn.close()

            print(f"{len(wallets_df)} smart wallets à analyser")

            for _, row in wallets_df.iterrows():
                wallet = row["wallet_address"]
                portfolio_value = float(row["total_portfolio_value"] or 0)

                print(f"\n[{wallet[:10]}...] portfolio=${portfolio_value:,.0f}")

                # Étape 2 — Transactions send du wallet mère (7 jours)
                transactions = fetch_recent_transactions(wallet, hours_lookback=hours_lookback)
                if not transactions:
                    print(f"   Aucune transaction send récente")
                    time.sleep(0.5)
                    continue
                print(f"   {len(transactions)} transactions send récupérées")

                # Étape 3 — Détecter migration candidate
                result = analyze_transfers_for_migration(
                    transactions, portfolio_value, min_transfer_pct=min_transfer_percentage
                )
                if not result:
                    print(f"   Seuil non atteint, pas de migration")
                    time.sleep(0.5)
                    continue

                destination = result["destination"]
                total_value = result["total_value"]
                pct = result["transfer_percentage"]
                tokens_data = result["tokens_data"]

                print(f"   Migration candidate: {wallet[:10]}... → {destination[:10]}...")
                print(f"   ${total_value:,.2f} ({pct:.1f}%) | {len(tokens_data)} tokens")

                # Étape 4 — EOA check obligatoire
                is_contract = _contract_checker.is_contract_single(destination)
                if is_contract is True:
                    print(f"   Smart contract → ignoré")
                    time.sleep(0.5)
                    continue
                if is_contract is None:
                    print(f"   Vérification EOA impossible → ignoré par sécurité")
                    time.sleep(0.5)
                    continue
                print(f"   EOA confirmé")

                conn = sqlite3.connect(self.db_path)

                # Étape 5 — INSERT fils dans wallets (period='migration')
                self._insert_fils_wallet(conn, destination)

                # Étape 6 — Fetch historique Zerion du fils par token
                session_id = str(uuid.uuid4())[:8]
                self._fetch_fils_history(conn, destination, tokens_data, session_id)

                # Étape 7 — Injection prix hérités (APRÈS fetch)
                self._inherit_prices(conn, old_wallet=wallet, new_wallet=destination, tokens_data=tokens_data)

                # Étape 8 — Enregistrer la migration
                migration = self._save_migration(
                    conn,
                    old_wallet=wallet,
                    new_wallet=destination,
                    tokens_data=tokens_data,
                    total_value=total_value,
                    transfer_percentage=pct
                )

                conn.close()

                if migration:
                    migrations_detected.append(migration)
                    print(f"   Migration complète enregistrée")

                time.sleep(1)

            print(f"\n=== DÉTECTION TERMINÉE : {len(migrations_detected)} migrations ===")
            return migrations_detected

        except Exception as e:
            print(f"Erreur detect_migrations: {e}")
            import traceback
            traceback.print_exc()
            return []

    # ── Étape 5 ───────────────────────────────

    def _insert_fils_wallet(self, conn, fils_address):
        """
        Insère le wallet fils dans wallets avec period='migration'.
        INSERT OR IGNORE — si déjà présent on ne touche à rien.

        Champs clés :
          - period = 'migration'     → identifie les wallets fils
          - is_active = 1
          - is_scored = 0            → sera scoré au prochain cycle
          - transactions_extracted = 0 → historique pas encore récupéré
        """
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO wallets (
                wallet_address, period, is_active, is_scored,
                transactions_extracted, total_portfolio_value,
                created_at, updated_at
            ) VALUES (?, 'migration', 1, 0, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """, (fils_address,))
        conn.commit()

        if cursor.rowcount > 0:
            print(f"   Fils inséré dans wallets (period='migration'): {fils_address[:10]}...")
        else:
            print(f"   Fils déjà présent dans wallets: {fils_address[:10]}...")

    # ── Étape 6 ───────────────────────────────

    def _fetch_fils_history(self, conn, fils_address, tokens_data, session_id):
        """
        Récupère et stocke l'historique complet du wallet fils pour chaque token transféré.
        Utilise fungible_id depuis tokens_data (jamais de fetch sans fungible_id).
        Met à jour transactions_extracted=1 après fetch réussi.

        Règles :
        - Skip les tokens sans fungible_id
        - Rate limiting 1.5s entre tokens (cf. doc)
        - price_per_token JAMAIS modifié ici → Zerion brut uniquement
        """
        tokens_fetched = 0

        # Dédupliquer par fungible_id
        seen = set()
        unique_tokens = []
        for t in tokens_data:
            fid = t.get("fungible_id")
            if fid and fid not in seen:
                seen.add(fid)
                unique_tokens.append(t)

        print(f"   Fetch historique fils: {len(unique_tokens)} tokens à récupérer")

        for token in unique_tokens:
            fungible_id = token.get("fungible_id")
            symbol = token.get("symbol", "UNKNOWN")
            contract_address = token.get("contract_address")

            if not fungible_id:
                print(f"   Skip {symbol}: pas de fungible_id")
                continue

            print(f"   Fetch {symbol} ({fungible_id[:20]}...)")
            raw_transactions = get_token_transaction_history_zerion_full(fils_address, fungible_id)

            if raw_transactions:
                count = analyze_and_store_complete_transactions(
                    session_id, fils_address, symbol, fungible_id,
                    contract_address, raw_transactions
                )
                print(f"   {symbol}: {count} transactions stockées")
                tokens_fetched += 1
            else:
                print(f"   {symbol}: aucune transaction trouvée")

            time.sleep(1.5)

        # Marquer l'historique comme extrait
        if tokens_fetched > 0:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE wallets SET transactions_extracted = 1, updated_at = CURRENT_TIMESTAMP "
                "WHERE wallet_address = ?",
                (fils_address,)
            )
            conn.commit()
            print(f"   transactions_extracted = 1 pour {fils_address[:10]}...")

    # ── Étape 7 ───────────────────────────────

    def _inherit_prices(self, conn, old_wallet, new_wallet, tokens_data):
        """
        Injecte le prix d'achat moyen pondéré du wallet mère dans inherited_price_per_token
        du wallet fils, uniquement sur direction='in'.

        Règles strictes :
        - NE JAMAIS modifier price_per_token (colonne Zerion read-only)
        - Uniquement direction='in'
        - Condition IS NULL → idempotent
        - Si aucun achat trouvé chez la mère → skip le token
        """
        cursor = conn.cursor()
        inherited = 0

        for token in tokens_data:
            symbol = token["symbol"]

            # Prix d'achat moyen pondéré du wallet mère
            cursor.execute("""
                SELECT SUM(ABS(quantity) * price_per_token) / SUM(ABS(quantity))
                FROM transaction_history
                WHERE wallet_address = ?
                AND symbol = ?
                AND action_type = 'buy'
                AND price_per_token > 0
                AND quantity != 0
            """, (old_wallet, symbol))
            result = cursor.fetchone()
            original_price = result[0] if result and result[0] else None

            if not original_price:
                print(f"   Pas de prix achat pour {symbol} chez {old_wallet[:10]}... → skip")
                continue

            # Injection sur les receives du fils uniquement
            cursor.execute("""
                UPDATE transaction_history
                SET inherited_price_per_token = ?,
                    is_inherited_from_wallet = ?
                WHERE wallet_address = ?
                AND symbol = ?
                AND direction = 'in'
                AND inherited_price_per_token IS NULL
            """, (original_price, old_wallet, new_wallet, symbol))

            rows = cursor.rowcount
            inherited += rows
            print(f"   {symbol}: ${original_price:.6f} hérité → {rows} tx mises à jour")

        conn.commit()
        print(f"   Total héritage: {inherited} transactions")

    # ── Étape 8 ───────────────────────────────

    def _save_migration(self, conn, old_wallet, new_wallet, tokens_data, total_value, transfer_percentage):
        """
        Enregistre la migration dans wallet_migrations.
        INSERT OR IGNORE → idempotent si le detector tourne plusieurs fois.
        """
        try:
            migration_date = datetime.utcnow().isoformat()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR IGNORE INTO wallet_migrations (
                    old_wallet, new_wallet, migration_date,
                    tokens_transferred, total_value_transferred,
                    transfer_percentage, is_validated
                ) VALUES (?, ?, ?, ?, ?, ?, 1)
            """, (
                old_wallet, new_wallet, migration_date,
                json.dumps(tokens_data), total_value, transfer_percentage
            ))
            conn.commit()
            return {
                "old_wallet": old_wallet,
                "new_wallet": new_wallet,
                "migration_date": migration_date,
                "tokens_data": tokens_data,
                "total_value": total_value,
                "transfer_percentage": transfer_percentage
            }
        except Exception as e:
            print(f"   Erreur _save_migration: {e}")
            conn.rollback()
            return None

    # ── Utilitaires ───────────────────────────

    def get_wallet_migration_chain(self, wallet_address):
        """Retourne la chaîne complète de migrations pour un wallet."""
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(
                "SELECT old_wallet, new_wallet, migration_date, tokens_transferred "
                "FROM wallet_migrations "
                "WHERE old_wallet = ? OR new_wallet = ? "
                "ORDER BY migration_date ASC",
                conn, params=[wallet_address, wallet_address]
            )
            conn.close()
            if df.empty:
                return None
            return [
                {
                    "old_wallet": row["old_wallet"],
                    "new_wallet": row["new_wallet"],
                    "migration_date": row["migration_date"],
                    "tokens": json.loads(row["tokens_transferred"]) if row["tokens_transferred"] else []
                }
                for _, row in df.iterrows()
            ]
        except Exception as e:
            print(f"Erreur get_wallet_migration_chain: {e}")
            return None

    def get_effective_buy_price(self, wallet_address, symbol):
        """
        Prix d'achat effectif : inherited_price_per_token en priorité, sinon avg buy.
        Utilisé par le moteur FIFO via COALESCE.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT inherited_price_per_token
                FROM transaction_history
                WHERE wallet_address = ? AND symbol = ? AND inherited_price_per_token IS NOT NULL
                LIMIT 1
            """, (wallet_address, symbol))
            result = cursor.fetchone()
            if result and result[0]:
                conn.close()
                return result[0]
            cursor.execute("""
                SELECT AVG(price_per_token)
                FROM transaction_history
                WHERE wallet_address = ? AND symbol = ? AND action_type = 'buy'
            """, (wallet_address, symbol))
            result = cursor.fetchone()
            conn.close()
            return result[0] if result and result[0] else None
        except Exception as e:
            print(f"Erreur get_effective_buy_price: {e}")
            return None


# ─────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────

def run_migration_detection(hours_lookback=168, min_transfer_percentage=70):
    detector = WalletMigrationDetector()
    migrations = detector.detect_migrations(
        hours_lookback=hours_lookback,
        min_transfer_percentage=min_transfer_percentage
    )

    if migrations:
        print(f"\n=== RÉSUMÉ ({len(migrations)} migrations) ===")
        for i, m in enumerate(migrations, 1):
            print(f"{i}. {m['old_wallet'][:10]}... → {m['new_wallet'][:10]}...")
            print(f"   Date : {m['migration_date'][:16]}")
            print(f"   Valeur : ${m['total_value']:,.2f} ({m['transfer_percentage']:.1f}%)")
            print(f"   Tokens : {len(m['tokens_data'])}")

    return migrations


if __name__ == "__main__":
    run_migration_detection(hours_lookback=168, min_transfer_percentage=70)
