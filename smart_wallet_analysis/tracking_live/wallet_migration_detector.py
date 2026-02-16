#!/usr/bin/env python3
"""Détecteur de migrations de wallets."""

import sqlite3
import json
import os
import time
import uuid
import requests
from datetime import datetime, timezone, timedelta
import pandas as pd
from dotenv import load_dotenv

from smart_wallet_analysis.config import DB_PATH, ENV_PATH, MIGRATION_DETECTOR
from smart_wallet_analysis.logger import get_logger
from smart_wallet_analysis.token_discovery_manual.smart_contrat_remover import ContractChecker
from smart_wallet_analysis.tracking_live.live_wallet_transaction_tracker_extractor_zerion import (
    get_token_transaction_history_zerion_full,
    analyze_and_store_complete_transactions
)

load_dotenv(dotenv_path=ENV_PATH)

logger = get_logger("tracking_live.migration")

_MD = MIGRATION_DETECTOR

API_KEYS = [k for k in [os.getenv("ZERION_API_KEY"), os.getenv("ZERION_API_KEY_2")] if k]
_api_index = 0

_contract_checker = ContractChecker()


def _get_api_key():
    """Retourne la clé API active."""
    return API_KEYS[_api_index]


def _rotate_api_key():
    """Bascule vers la clé API suivante."""
    global _api_index
    _api_index = (_api_index + 1) % len(API_KEYS)


def _zerion_headers():
    """Construit les headers Zerion."""
    return {"accept": "application/json", "authorization": f"Basic {_get_api_key()}"}


def fetch_recent_transactions(wallet_address, hours_lookback=None, retries=None):
    """Récupère les transactions send du wallet mère sur la période."""
    hours_lookback = _MD["HOURS_LOOKBACK"] if hours_lookback is None else hours_lookback
    retries = _MD["RETRIES"] if retries is None else retries

    url = (
        f"https://api.zerion.io/v1/wallets/{wallet_address}/transactions/"
        f"?filter[operation_types]=send&currency=usd&page[size]=100"
    )
    all_transactions, page_cursor, page_count = [], None, 0

    while page_count < _MD["MAX_PAGES"]:
        paginated_url = url + (f"&page[after]={page_cursor}" if page_cursor else "")

        for attempt in range(retries):
            try:
                response = requests.get(paginated_url, headers=_zerion_headers(), timeout=15)

                if response.status_code == 429:
                    _rotate_api_key()
                    time.sleep(_MD["RATE_LIMIT_SLEEP_SECONDS"])
                    continue

                response.raise_for_status()
                data = response.json()
                transactions = data.get("data", [])
                page_count += 1

                if not transactions:
                    return all_transactions

                all_transactions.extend(transactions)

                oldest_date_str = transactions[-1].get("attributes", {}).get("mined_at", "")
                if oldest_date_str:
                    oldest_dt = datetime.fromisoformat(oldest_date_str.replace("Z", "+00:00"))
                    if oldest_dt < datetime.now(timezone.utc) - timedelta(hours=hours_lookback):
                        return all_transactions

                next_url = data.get("links", {}).get("next")
                if not next_url:
                    return all_transactions

                if "page%5Bafter%5D=" in next_url:
                    page_cursor = next_url.split("page%5Bafter%5D=")[1].split("&")[0]
                elif "page[after]=" in next_url:
                    page_cursor = next_url.split("page[after]=")[1].split("&")[0]
                else:
                    return all_transactions

                time.sleep(_MD["PAGE_DELAY_SECONDS"])
                break

            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(_MD["RETRY_DELAY_SECONDS"])
                else:
                    logger.error(f"Erreur Zerion {wallet_address[:10]}...: {e}")
                    return all_transactions

    return all_transactions


def analyze_transfers_for_migration(transactions, portfolio_value, min_transfer_pct=None, max_days=None):
    """Détecte une migration candidate sur une fenêtre récente."""
    min_transfer_pct = _MD["MIN_TRANSFER_PERCENTAGE"] if min_transfer_pct is None else min_transfer_pct
    max_days = _MD["MAX_DAYS"] if max_days is None else max_days

    if portfolio_value <= 0:
        return None

    cutoff = datetime.now(timezone.utc) - timedelta(days=max_days)
    dest_value, dest_tokens = {}, {}

    for tx in transactions:
        tx_date_str = tx.get("attributes", {}).get("mined_at", "")
        if tx_date_str and datetime.fromisoformat(tx_date_str.replace("Z", "+00:00")) < cutoff:
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

            finfo = transfer.get("fungible_info") or {}
            symbol = (finfo.get("symbol") or "UNKNOWN").upper()
            impls = finfo.get("implementations") or []
            contract_address = impls[0].get("address") if impls else None
            qty_data = transfer.get("quantity") or {}
            quantity = float(qty_data.get("numeric", 0)) if isinstance(qty_data, dict) else 0

            dest_value[recipient] = dest_value.get(recipient, 0) + value
            dest_tokens.setdefault(recipient, []).append({
                "symbol": symbol,
                "contract_address": contract_address,
                "fungible_id": finfo.get("id"),
                "quantity_transferred": quantity,
                "value_usd": value
            })

    if not dest_value:
        return None

    top_dest = max(dest_value, key=dest_value.get)
    pct = (dest_value[top_dest] / portfolio_value) * 100

    if pct >= min_transfer_pct:
        return {"destination": top_dest, "total_value": dest_value[top_dest], "transfer_percentage": pct, "tokens_data": dest_tokens[top_dest]}
    return None


class WalletMigrationDetector:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path

    def detect_migrations(self, hours_lookback=None, min_transfer_percentage=None):
        """Détecte et traite les migrations de wallets."""
        hours_lookback = _MD["HOURS_LOOKBACK"] if hours_lookback is None else hours_lookback
        min_transfer_percentage = _MD["MIN_TRANSFER_PERCENTAGE"] if min_transfer_percentage is None else min_transfer_percentage

        logger.info(f"DÉTECTION MIGRATIONS — {hours_lookback}h | seuil >{min_transfer_percentage}%")
        migrations_detected = []

        try:
            conn = sqlite3.connect(self.db_path)
            wallets_df = pd.read_sql_query(
                "SELECT w.wallet_address, w.total_portfolio_value "
                "FROM wallets w INNER JOIN smart_wallets sw ON w.wallet_address = sw.wallet_address "
                "WHERE w.is_active = 1 AND w.total_portfolio_value > 0 "
                "ORDER BY w.total_portfolio_value DESC",
                conn
            )
            conn.close()
            logger.info(f"{len(wallets_df)} smart wallets à analyser")

            for _, row in wallets_df.iterrows():
                wallet = row["wallet_address"]
                portfolio_value = float(row["total_portfolio_value"] or 0)
                logger.info(f"[{wallet[:10]}...] portfolio=${portfolio_value:,.0f}")

                transactions = fetch_recent_transactions(wallet, hours_lookback=hours_lookback)
                if not transactions:
                    logger.info(f"  Aucune transaction send récente")
                    time.sleep(_MD["SHORT_SLEEP_SECONDS"])
                    continue
                logger.info(f"  {len(transactions)} transactions send récupérées")

                result = analyze_transfers_for_migration(transactions, portfolio_value, min_transfer_pct=min_transfer_percentage)
                if not result:
                    logger.info(f"  Seuil non atteint, pas de migration")
                    time.sleep(_MD["SHORT_SLEEP_SECONDS"])
                    continue

                destination = result["destination"]
                total_value = result["total_value"]
                pct = result["transfer_percentage"]
                tokens_data = result["tokens_data"]
                logger.info(f"  Migration: {wallet[:10]}... → {destination[:10]}... ${total_value:,.2f} ({pct:.1f}%) | {len(tokens_data)} tokens")

                is_contract = _contract_checker.is_contract_single(destination)
                if is_contract is True:
                    logger.info(f"  Smart contract → ignoré")
                    time.sleep(_MD["SHORT_SLEEP_SECONDS"])
                    continue
                if is_contract is None:
                    logger.warning(f"  Vérification EOA impossible → ignoré par sécurité")
                    time.sleep(_MD["SHORT_SLEEP_SECONDS"])
                    continue
                logger.info(f"  EOA confirmé")

                conn = sqlite3.connect(self.db_path)
                self._insert_fils_wallet(conn, destination)
                session_id = str(uuid.uuid4())[:8]
                self._fetch_fils_history(conn, destination, tokens_data, session_id)
                self._inherit_prices(conn, old_wallet=wallet, new_wallet=destination, tokens_data=tokens_data)
                migration = self._save_migration(conn, old_wallet=wallet, new_wallet=destination,
                                                 tokens_data=tokens_data, total_value=total_value,
                                                 transfer_percentage=pct)
                conn.close()

                if migration:
                    migrations_detected.append(migration)
                    logger.info(f"  Migration complète enregistrée")

                time.sleep(_MD["AFTER_MIGRATION_SLEEP_SECONDS"])

            logger.info(f"DÉTECTION TERMINÉE — {len(migrations_detected)} migrations")
            return migrations_detected

        except Exception as e:
            logger.error(f"Erreur detect_migrations: {e}", exc_info=True)
            return []

    def _insert_fils_wallet(self, conn, fils_address):
        """Insère le wallet fils dans wallets."""
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO wallets (
                wallet_address, period, is_active, is_scored,
                transactions_extracted, total_portfolio_value,
                created_at, updated_at
            ) VALUES (?, 'migration', 1, 0, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """, (fils_address,))
        conn.commit()
        status = "inséré" if cursor.rowcount > 0 else "déjà présent"
        logger.info(f"  Fils {status} dans wallets: {fils_address[:10]}...")

    def _fetch_fils_history(self, conn, fils_address, tokens_data, session_id):
        """Récupère l'historique complet du wallet fils."""
        seen = set()
        unique_tokens = []
        for t in tokens_data:
            fid = t.get("fungible_id")
            if fid and fid not in seen:
                seen.add(fid)
                unique_tokens.append(t)

        logger.info(f"  Fetch historique fils: {len(unique_tokens)} tokens")
        tokens_fetched = 0

        for token in unique_tokens:
            fungible_id = token.get("fungible_id")
            symbol = token.get("symbol", "UNKNOWN")
            if not fungible_id:
                logger.warning(f"  Skip {symbol}: pas de fungible_id")
                continue

            logger.info(f"  Fetch {symbol} ({fungible_id[:20]}...)")
            raw_transactions = get_token_transaction_history_zerion_full(fils_address, fungible_id)
            if raw_transactions:
                count = analyze_and_store_complete_transactions(
                    session_id, fils_address, symbol, fungible_id,
                    token.get("contract_address"), raw_transactions
                )
                logger.info(f"  {symbol}: {count} transactions stockées")
                tokens_fetched += 1
            else:
                logger.warning(f"  {symbol}: aucune transaction trouvée")

            time.sleep(_MD["PAGE_DELAY_SECONDS"])

        if tokens_fetched > 0:
            conn.execute(
                "UPDATE wallets SET transactions_extracted = 1, updated_at = CURRENT_TIMESTAMP WHERE wallet_address = ?",
                (fils_address,)
            )
            conn.commit()
            logger.info(f"  transactions_extracted = 1 pour {fils_address[:10]}...")

    def _inherit_prices(self, conn, old_wallet, new_wallet, tokens_data):
        """Injecte les prix d'achat hérités sur le wallet fils."""
        cursor = conn.cursor()
        inherited = 0

        for token in tokens_data:
            symbol = token["symbol"]
            cursor.execute("""
                SELECT SUM(ABS(quantity) * price_per_token) / SUM(ABS(quantity))
                FROM transaction_history
                WHERE wallet_address = ? AND symbol = ? AND action_type = 'buy'
                AND price_per_token > 0 AND quantity != 0
            """, (old_wallet, symbol))
            result = cursor.fetchone()
            original_price = result[0] if result and result[0] else None

            if not original_price:
                logger.info(f"  Pas de prix achat pour {symbol} chez {old_wallet[:10]}... → skip")
                continue

            cursor.execute("""
                UPDATE transaction_history
                SET inherited_price_per_token = ?, is_inherited_from_wallet = ?
                WHERE wallet_address = ? AND symbol = ? AND direction = 'in'
                AND inherited_price_per_token IS NULL
            """, (original_price, old_wallet, new_wallet, symbol))
            rows = cursor.rowcount
            inherited += rows
            logger.info(f"  {symbol}: ${original_price:.6f} hérité → {rows} tx mises à jour")

        conn.commit()
        logger.info(f"  Total héritage: {inherited} transactions")

    def _save_migration(self, conn, old_wallet, new_wallet, tokens_data, total_value, transfer_percentage):
        """Enregistre la migration dans wallet_migrations."""
        try:
            migration_date = datetime.utcnow().isoformat()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR IGNORE INTO wallet_migrations (
                    old_wallet, new_wallet, migration_date,
                    tokens_transferred, total_value_transferred,
                    transfer_percentage, is_validated
                ) VALUES (?, ?, ?, ?, ?, ?, 1)
            """, (old_wallet, new_wallet, migration_date, json.dumps(tokens_data), total_value, transfer_percentage))
            conn.commit()
            return {"old_wallet": old_wallet, "new_wallet": new_wallet, "migration_date": migration_date,
                    "tokens_data": tokens_data, "total_value": total_value, "transfer_percentage": transfer_percentage}
        except Exception as e:
            logger.error(f"Erreur _save_migration: {e}")
            conn.rollback()
            return None

    def get_wallet_migration_chain(self, wallet_address):
        """Retourne la chaîne complète de migrations pour un wallet."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                df = pd.read_sql_query(
                    "SELECT old_wallet, new_wallet, migration_date, tokens_transferred "
                    "FROM wallet_migrations WHERE old_wallet = ? OR new_wallet = ? ORDER BY migration_date ASC",
                    conn, params=[wallet_address, wallet_address]
                )
            if df.empty:
                return None
            return [{"old_wallet": r["old_wallet"], "new_wallet": r["new_wallet"],
                     "migration_date": r["migration_date"],
                     "tokens": json.loads(r["tokens_transferred"]) if r["tokens_transferred"] else []}
                    for _, r in df.iterrows()]
        except Exception as e:
            logger.error(f"Erreur get_wallet_migration_chain: {e}")
            return None

    def get_effective_buy_price(self, wallet_address, symbol):
        """Retourne le prix d'achat effectif d'un token."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT inherited_price_per_token FROM transaction_history
                    WHERE wallet_address = ? AND symbol = ? AND inherited_price_per_token IS NOT NULL LIMIT 1
                """, (wallet_address, symbol))
                result = cursor.fetchone()
                if result and result[0]:
                    return result[0]
                cursor.execute("""
                    SELECT AVG(price_per_token) FROM transaction_history
                    WHERE wallet_address = ? AND symbol = ? AND action_type = 'buy'
                """, (wallet_address, symbol))
                result = cursor.fetchone()
                return result[0] if result and result[0] else None
        except Exception as e:
            logger.error(f"Erreur get_effective_buy_price: {e}")
            return None


def run_migration_detection(hours_lookback=None, min_transfer_percentage=None):
    """Lance la détection de migrations."""
    detector = WalletMigrationDetector()
    migrations = detector.detect_migrations(
        hours_lookback=hours_lookback,
        min_transfer_percentage=min_transfer_percentage
    )
    if migrations:
        logger.info(f"RÉSUMÉ — {len(migrations)} migrations")
        for i, m in enumerate(migrations, 1):
            logger.info(f"  {i}. {m['old_wallet'][:10]}... → {m['new_wallet'][:10]}... "
                        f"${m['total_value']:,.2f} ({m['transfer_percentage']:.1f}%) | "
                        f"{len(m['tokens_data'])} tokens | {m['migration_date'][:16]}")
    return migrations


if __name__ == "__main__":
    run_migration_detection()
