import os
import time
import requests
import pandas as pd
import json
import sqlite3
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import uuid
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Configuration globale ===
load_dotenv(dotenv_path=Path(__file__).parent.parent.parent / ".env")

# Récupération des deux clés API
API_KEY_1 = os.getenv("ZERION_API_KEY")
API_KEY_2 = os.getenv("ZERION_API_KEY_2")

if not API_KEY_1:
    raise ValueError("❌ Clé API principale manquante. Vérifie ton fichier .env (ZERION_API_KEY).")
if not API_KEY_2:
    raise ValueError("❌ Clé API secondaire manquante. Vérifie ton fichier .env (ZERION_API_KEY_2).")

# Système de rotation des clés API
API_KEYS = [API_KEY_1, API_KEY_2]
api_key_index = 0

def get_current_api_key():
    """Retourne la clé API actuellement utilisée"""
    global api_key_index
    return API_KEYS[api_key_index]

def rotate_api_key():
    """Fait tourner vers la clé API suivante"""
    global api_key_index
    api_key_index = (api_key_index + 1) % len(API_KEYS)
    print(f"🔄 Rotation vers clé API {api_key_index + 1}")
    return API_KEYS[api_key_index]

# === Fichiers et dossiers ===
ROOT = Path(__file__).parent.parent.parent  # Remonter de module/tracking_live/ vers la racine
DB_PATH = ROOT / "data" / "db" / "wit_database.db"


MIN_TOKEN_QUANTITY = 0.001  # Seuil minimum de quantité de token pour être pris en compte
BATCH_SIZE = 5
DELAY_BETWEEN_BATCHES = 10

# === Initialisation (plus besoin de dossiers CSV)
def init_folders():
    print("🗄️ Utilisation exclusive de la base de données SQLite")

# === Récupération des smart wallets depuis la BDD avec jointures optimisées
def get_smart_wallets_from_db():
    """Récupère les smart wallets depuis la nouvelle table smart_wallets"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Récupération des smart wallets depuis la nouvelle table
        query = """
            SELECT 
                wallet_address,  
                optimal_threshold_tier,
                quality_score,
                threshold_status,
                optimal_roi,
                optimal_winrate,
                optimal_trades,
                optimal_gagnants,
                optimal_perdants,
                optimal_neutres,
                global_roi
            FROM smart_wallets
            WHERE optimal_threshold_tier > 0
            ORDER BY optimal_threshold_tier DESC
            LIMIT 100
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        wallets = df['wallet_address'].tolist()
        print(f"📊 {len(wallets)} smart wallets chargés (optimal_threshold_tier > 0)")
        
        # Afficher le top 5 pour debug
        if not df.empty:
            print("🏆 TOP 5 SMART WALLETS:")
            for _, row in df.head(5).iterrows():
                print(f"   • {row['wallet_address'][:12]}... | Score: {row['optimal_threshold_tier']:.1f} | ROI: {row['optimal_roi']:+.1f}% | SR: {row['optimal_winrate']:.1%}")
        
        return wallets
        
    except Exception as e:
        print(f"❌ Erreur lors de la récupération des wallets: {e}")
        return []

# === Configuration session HTTP avec retry automatique
def create_http_session():
    """Crée une session HTTP avec retry automatique et timeouts optimisés"""
    session = requests.Session()
    
    # Configuration retry automatique
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

# === Récupération du fungible_id via Zerion API
def get_fungible_id_zerion(contract_address, chain, token_symbol="", session=None):
    """Récupère le fungible_id d'un token via l'API Zerion /fungibles"""
    
    # Cas spécial : ETH natif (pas de contract_address)
    if token_symbol.upper() == "ETH" and not contract_address:
        return "eth"  # ID standard pour ETH natif sur toutes les chains
    
    # Cas normal : token avec contract_address
    if not contract_address or not chain:
        return ""
    
    if not session:
        session = create_http_session()
    
    url = f"https://api.zerion.io/v1/fungibles/?filter[implementation_address]={contract_address.lower()}&filter[implementation_chain_id]={chain}"
    
    headers = {
        "accept": "application/json",
        "authorization": f"Basic {get_current_api_key()}"
    }
    
    try:
        response = session.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        fungibles = data.get("data", [])
        
        if fungibles:
            # Prendre le premier résultat (devrait être unique)
            fungible_id = fungibles[0].get("id", "")
            return fungible_id
        else:
            return ""
            
    except requests.exceptions.Timeout:
        print(f"⏰ Timeout récupération fungible_id pour {contract_address}")
        return ""
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print(f"🚧 Rate limit atteint pour fungible_id {contract_address}, rotation clé API...")
            rotate_api_key()
            time.sleep(3)
        else:
            print(f"⚠️ Erreur HTTP {e.response.status_code} pour {contract_address}")
        return ""
    except Exception as e:
        error_msg = str(e)
        if "429 error responses" in error_msg or "rate limit" in error_msg.lower():
            print(f"🚧 Rate limit détecté pour fungible_id {contract_address}, rotation clé API...")
            rotate_api_key()
            time.sleep(3)
            # Retry avec la nouvelle clé API
            print(f"🔄 Retry fungible_id avec nouvelle clé pour {contract_address}")
            return get_fungible_id_zerion(contract_address, chain, token_symbol, session)
        else:
            print(f"⚠️ Erreur récupération fungible_id pour {contract_address}: {e}")
        return ""

# === Récupération des balances via Zerion API
def get_token_balances_zerion(address):
    """Récupère les balances d'un wallet via l'API Zerion"""
    session = create_http_session()
    url = f"https://api.zerion.io/v1/wallets/{address}/positions/?filter[positions]=only_simple&currency=usd&filter[trash]=only_non_trash&sort=value"
    
    headers = {
        "accept": "application/json",
        "authorization": f"Basic {get_current_api_key()}"
    }
    
    try:
        response = session.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        
        data = response.json()
        all_positions = data.get("data", [])
        
        # Filtrer les positions > MIN_USD
        filtered_tokens = []
        for pos in all_positions:
            attrs = pos.get("attributes", {})
            fungible_info = attrs.get("fungible_info", {})
            
            # Quantité
            quantity_data = attrs.get("quantity", 0)
            if isinstance(quantity_data, dict):
                amount = float(quantity_data.get("numeric", 0))
            else:
                amount = float(quantity_data or 0)
            
            # Valeur USD
            value_data = attrs.get("value", 0)
            if isinstance(value_data, dict):
                usd_value = float(value_data.get("numeric", 0))
            else:
                usd_value = float(value_data or 0)
            
            # Filtrer selon la quantité de token (plus pertinent que USD pour détecter accumulations)
            if amount < MIN_TOKEN_QUANTITY:
                continue
            
            # Garder aussi le filtre USD pour éviter des micro-positions sans valeur
            if usd_value < 500:  # Seuil modéré pour éviter spam
                continue
            
            # Token info
            token = fungible_info.get("symbol", "UNKNOWN")
            
            # Chain et contrat
            implementations = fungible_info.get("implementations", [])
            if implementations:
                chain = implementations[0].get("chain_id", "")
                contract_address = implementations[0].get("address", "")
                contract_decimals = implementations[0].get("decimals", "")
            else:
                chain = ""
                contract_address = ""
                contract_decimals = ""
            
            # Récupérer le fungible_id (passer le token symbol pour ETH natif)
            fungible_id = get_fungible_id_zerion(contract_address, chain, token, session)
            
            # Petit délai pour éviter de surcharger l'API
            time.sleep(0.3)
            
            filtered_tokens.append({
                "token": token.strip().upper(),
                "amount": amount,
                "usd_value": usd_value,
                "chain": chain,
                "contract_address": contract_address,
                "contract_decimals": contract_decimals,
                "fungible_id": fungible_id
            })
        
        return pd.DataFrame(filtered_tokens)
        
    except requests.exceptions.Timeout:
        print(f"⏰ Timeout API Zerion pour {address}")
        return pd.DataFrame()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print(f"🚧 Rate limit API Zerion pour {address}, rotation clé API...")
            rotate_api_key()
            time.sleep(5)
        elif e.response.status_code == 404:
            print(f"❌ Wallet {address} non trouvé")
        else:
            print(f"⚠️ Erreur HTTP {e.response.status_code} pour {address}")
        return pd.DataFrame()
    except Exception as e:
        error_msg = str(e)
        if "429 error responses" in error_msg or "rate limit" in error_msg.lower():
            print(f"🚧 Rate limit détecté pour {address}, rotation clé API...")
            rotate_api_key()
            time.sleep(5)
            # Retry avec la nouvelle clé API
            print(f"🔄 Retry avec nouvelle clé API pour {address}")
            return get_token_balances_zerion(address)
        else:
            print(f"❌ Erreur Zerion API pour {address}: {e}")
        return pd.DataFrame()

# === Gestion des données en base ===
def get_existing_wallet_tokens(wallet_address):
    """Récupère les tokens actuels d'un wallet depuis la jointure smart_wallets + tokens"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Vérifier d'abord que le wallet est dans smart_wallets, puis récupérer UNIQUEMENT ses tokens en portefeuille
        query = """
            SELECT t.symbol, t.current_amount, t.current_usd_value, t.contract_address, 
                   t.chain, t.fungible_id, t.updated_at
            FROM tokens t
            WHERE t.wallet_address = ?
            AND t.in_portfolio = 1
            AND EXISTS (
                SELECT 1 FROM smart_wallets sw 
                WHERE sw.wallet_address = t.wallet_address 
                AND sw.optimal_threshold_tier > 0
            )
        """
        
        cursor = conn.cursor()
        cursor.execute(query, (wallet_address,))
        
        tokens_data = {}
        for row in cursor.fetchall():
            symbol, amount, usd_value, contract_address, chain, fungible_id, updated_at = row
            tokens_data[symbol] = {
                "amount": amount,
                "usd_value": usd_value,
                "contract_address": contract_address,
                "chain": chain,
                "fungible_id": fungible_id,
                "updated_at": updated_at
            }
        
        conn.close()
        return tokens_data
        
    except Exception as e:
        print(f"⚠️ Erreur lecture BDD pour {wallet_address}: {e}")
        return {}

def update_wallet_tokens_in_db(wallet_address, tokens_data):
    """
    Met à jour les tokens d'un wallet dans la base de données
    IMPORTANT: Les tokens actuels sont marqués in_portfolio=1, 
    les anciens tokens sont conservés avec in_portfolio=0 pour l'historique ROI
    """
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()
        
        # STRATÉGIE: Marquer les anciens tokens de la dernière session comme "anciens"
        # puis insérer les nouveaux tokens actuels
        cursor.execute("""
            UPDATE tokens 
            SET in_portfolio = 0, updated_at = ? 
            WHERE wallet_address = ? AND in_portfolio = 1
        """, (datetime.now().isoformat(), wallet_address))
        
        # Insérer les nouveaux tokens actuels du portefeuille avec in_portfolio=1
        for token_data in tokens_data:
            cursor.execute("""
                INSERT OR REPLACE INTO tokens (
                    wallet_address, fungible_id, symbol, contract_address, 
                    chain, current_amount, current_usd_value, 
                    current_price_per_token, updated_at, in_portfolio
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                wallet_address,
                token_data["fungible_id"],
                token_data["token"],
                token_data["contract_address"],
                token_data.get("chain", ""),
                token_data["amount"],
                token_data["usd_value"],
                token_data["usd_value"] / token_data["amount"] if token_data["amount"] > 0 else 0,
                datetime.now().isoformat(),
                1  # in_portfolio = 1 pour les tokens actuels
            ))
        
        # Mettre à jour les informations du wallet
        total_value = sum(token["usd_value"] for token in tokens_data)
        token_count = len(tokens_data)
        
        cursor.execute("""
            UPDATE wallets 
            SET total_portfolio_value = ?, token_count = ?, last_sync = ?, updated_at = ?
            WHERE wallet_address = ?
        """, (total_value, token_count, datetime.now().isoformat(), datetime.now().isoformat(), wallet_address))
        
        # Si le wallet n'existe pas, l'insérer
        if cursor.rowcount == 0:
            cursor.execute("""
                INSERT INTO wallets (
                    wallet_address, total_portfolio_value, token_count, 
                    last_sync, created_at, updated_at, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                wallet_address, total_value, token_count,
                datetime.now().isoformat(), datetime.now().isoformat(), 
                datetime.now().isoformat(), True
            ))
        
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Erreur mise à jour BDD pour {wallet_address}: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False


def log_token_change_to_db(change_data, change_type):
    """Enregistre un changement de token dans l'historique des transactions"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Déterminer l'opération et l'action basées sur le type de changement
        if change_type == "new_token":
            operation_type = "buy"
            action_type = "new_position"
            quantity = change_data["amount"]
        elif change_type == "accumulation":
            operation_type = "buy"
            action_type = "increase_position"
            quantity = change_data["amount_change"]
        elif change_type == "reduction":
            operation_type = "sell"
            action_type = "decrease_position"
            quantity = abs(change_data["amount_change"])
        elif change_type == "exit":
            operation_type = "sell"
            action_type = "close_position"
            quantity = change_data["old_amount"]
        else:
            return False
        
        # Générer un hash unique pour cette transaction
        transaction_hash = f"live_tracking_{change_data['wallet_address']}_{change_data['token']}_{change_type}_{datetime.now().isoformat()}"
        
        cursor.execute("""
            INSERT INTO transaction_history (
                wallet_address, fungible_id, symbol, date, hash, 
                operation_type, action_type, contract_address, 
                quantity, total_value_usd
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            change_data["wallet_address"],
            change_data.get("fungible_id", ""),
            change_data["token"],
            datetime.now().isoformat(),
            transaction_hash,
            operation_type,
            action_type,
            change_data.get("contract_address", ""),
            quantity,
            change_data.get("new_usd_value", change_data.get("usd_value", 0))
        ))
        
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        print(f"⚠️ Erreur enregistrement changement en BDD: {e}")
        return False

def detect_position_changes_sql(wallet_address, current_tokens_data, session_id):
    """Détecte les changements avec requêtes SQL optimisées"""
    changes = {
        "new_tokens": [],
        "accumulations": [],
        "reductions": [],
        "exits": []
    }
    
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")  # Pour éviter les lock
        cursor = conn.cursor()
        
        # 1. Récupérer les positions de la session précédente
        # CORRECTIF: Récupérer les tokens qui étaient in_portfolio=1 AVANT la mise à jour actuelle
        cursor.execute("""
            SELECT t.symbol, t.current_amount as amount, t.current_usd_value as usd_value, 
                   COALESCE(t.current_price_per_token, 0) as price_per_token,
                   t.contract_address, t.fungible_id
            FROM tokens t
            WHERE t.wallet_address = ?
            AND t.in_portfolio = 1
            AND EXISTS (
                SELECT 1 FROM smart_wallets sw 
                WHERE sw.wallet_address = t.wallet_address 
                AND sw.optimal_threshold_tier > 0
            )
        """, (wallet_address,))
        
        previous_positions = {}
        for row in cursor.fetchall():
            symbol, amount, usd_value, price_per_token, contract_address, fungible_id = row
            previous_positions[symbol] = {
                "amount": amount or 0,
                "usd_value": usd_value or 0,
                "price_per_token": price_per_token or 0,
                "contract_address": contract_address or "",
                "fungible_id": fungible_id or ""
            }
        
        # 2. Convertir positions actuelles
        current_positions = {token["token"]: token for token in current_tokens_data}
        
        # 3. Analyser les changements
        current_symbols = set(current_positions.keys())
        previous_symbols = set(previous_positions.keys())
        
        # Différencier NOUVEAUX tokens vs RETOURS (réaccumulations)
        new_symbols = current_symbols - previous_symbols
        for symbol in new_symbols:
            pos = current_positions[symbol]
            
            # Vérifier si c'est vraiment nouveau ou un retour
            # IMPORTANT: Inclure contract_address pour éviter les faux positifs avec des tokens de même symbole
            cursor.execute("""
                SELECT COUNT(*) FROM tokens 
                WHERE wallet_address = ? AND symbol = ? AND contract_address = ?
            """, (wallet_address, symbol, pos["contract_address"]))
            
            has_history = cursor.fetchone()[0] > 0
            change_type = "RETOUR" if has_history else "NEW"
            
            change = {
                "token": symbol,
                "amount": pos["amount"],
                "usd_value": pos["usd_value"],
                "contract_address": pos["contract_address"],
                "chain": pos.get("chain", ""),
                "fungible_id": pos["fungible_id"],
                "wallet_address": wallet_address,
                "change_type": change_type
            }
            changes["new_tokens"].append(change)
            
            # Enregistrer en BDD avec fungible_id
            cursor.execute("""
                INSERT OR IGNORE INTO wallet_position_changes (
                    session_id, wallet_address, symbol, contract_address, change_type,
                    old_amount, new_amount, amount_change, change_percentage,
                    old_usd_value, new_usd_value, usd_change, detected_at, price_per_token, fungible_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                session_id, wallet_address, symbol, pos["contract_address"], change_type,
                0, pos["amount"], pos["amount"], 100,
                0, pos["usd_value"], pos["usd_value"],
                datetime.now().isoformat(), pos.get("price_per_token", 0), pos["fungible_id"]
            ))
        
        # Tokens modifiés
        common_symbols = current_symbols.intersection(previous_symbols)
        for symbol in common_symbols:
            current = current_positions[symbol]
            previous = previous_positions[symbol]
            
            amount_change = current["amount"] - previous["amount"]
            usd_change = current["usd_value"] - previous["usd_value"]
            
            # Changement significatif (>0.1% ET plus de $10 de variation)
            change_pct_threshold = 0.001  # 0.1%
            min_usd_change = 10  # $10 minimum
            
            if (abs(amount_change) / max(previous["amount"], 0.001) > change_pct_threshold and 
                abs(usd_change) > min_usd_change):
                change_type = "ACCUMULATION" if amount_change > 0 else "REDUCTION"
                change_pct = (amount_change / previous["amount"]) * 100
                
                change = {
                    "token": symbol,
                    "old_amount": previous["amount"],
                    "new_amount": current["amount"],
                    "amount_change": amount_change,
                    "change_pct": change_pct,
                    "old_usd_value": previous["usd_value"],
                    "new_usd_value": current["usd_value"],
                    "usd_change": usd_change,
                    "wallet_address": wallet_address,
                    "change_type": change_type
                }
                
                if change_type == "ACCUMULATION":
                    changes["accumulations"].append(change)
                else:
                    changes["reductions"].append(change)
                
                # Enregistrer en BDD avec fungible_id
                cursor.execute("""
                    INSERT OR IGNORE INTO wallet_position_changes (
                        session_id, wallet_address, symbol, contract_address, change_type,
                        old_amount, new_amount, amount_change, change_percentage,
                        old_usd_value, new_usd_value, usd_change, detected_at, price_per_token, fungible_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    session_id, wallet_address, symbol, current["contract_address"], change_type,
                    previous["amount"], current["amount"], amount_change, change_pct,
                    previous["usd_value"], current["usd_value"], usd_change,
                    datetime.now().isoformat(), current.get("price_per_token", 0), current["fungible_id"]
                ))
        
        # Tokens sortis
        exited_symbols = previous_symbols - current_symbols
        for symbol in exited_symbols:
            previous = previous_positions[symbol]
            old_amount = previous.get("amount", 0) or 0
            old_usd_value = previous.get("usd_value", 0) or 0
            change = {
                "token": symbol,
                "old_amount": old_amount,
                "old_usd_value": old_usd_value,
                "wallet_address": wallet_address,
                "change_type": "EXIT"
            }
            changes["exits"].append(change)
            
            # Enregistrer en BDD avec fungible_id
            cursor.execute("""
                INSERT OR IGNORE INTO wallet_position_changes (
                    session_id, wallet_address, symbol, contract_address, change_type,
                    old_amount, new_amount, amount_change, change_percentage,
                    old_usd_value, new_usd_value, usd_change, detected_at, price_per_token, fungible_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                session_id, wallet_address, symbol, previous.get("contract_address", ""), "EXIT",
                old_amount, 0, -old_amount, -100,
                old_usd_value, 0, -old_usd_value,
                datetime.now().isoformat(), 0, previous.get("fungible_id", "")
            ))
        
        # 4. Les positions actuelles sont déjà mises à jour dans la table tokens par update_wallet_tokens_in_db
        # Plus besoin de wallet_positions_current
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        print(f"❌ Erreur SQL détection changements {wallet_address}: {e}")
        if conn:
            conn.rollback()
            conn.close()
    
    return changes

# === Traitement d'un batch de wallets (VERSION SQL OPTIMISÉE)
def process_wallet_batch_sql(wallets, position_changes_found, session_id):
    for address in wallets:
        # Plus de sauvegarde CSV - tout en base de données
        print(f"\n=== {address} | TRACKING LIVE SQL OPTIMISÉ ===")

        df = get_token_balances_zerion(address)
        if df.empty:
            print(f"❌ Aucun token avec quantité significative détecté.")
            continue

        # Convertir DataFrame en liste de dicts
        current_tokens_data = df.to_dict('records')
        
        # Détecter changements avec requêtes SQL optimisées
        changes = detect_position_changes_sql(address, current_tokens_data, session_id)
        
        # Analyser les changements détectés
        total_changes = len(changes["new_tokens"]) + len(changes["accumulations"]) + len(changes["reductions"]) + len(changes["exits"])
        
        if total_changes > 0:
            print(f"🔄 {total_changes} changements détectés:")
            
            if changes["new_tokens"]:
                print(f"  🆕 {len(changes['new_tokens'])} nouveaux tokens")
                for token_info in changes["new_tokens"]:
                    print(f"     + {token_info['token']}: {token_info['amount']:,.6f} tokens (${token_info['usd_value']:,.0f})")
            
            if changes["accumulations"]: 
                print(f"  📈 {len(changes['accumulations'])} accumulations")
                for acc in changes["accumulations"]:
                    old_amount = acc.get('old_amount', 0) or 0
                    new_amount = acc.get('new_amount', 0) or 0
                    change_pct = acc.get('change_pct', 0) or 0
                    print(f"     ↗️ {acc['token']}: +{change_pct:+.1f}% ({old_amount:,.6f} → {new_amount:,.6f} tokens)")
            
            if changes["reductions"]:
                print(f"  📉 {len(changes['reductions'])} réductions") 
                for red in changes["reductions"]:
                    old_amount = red.get('old_amount', 0) or 0
                    new_amount = red.get('new_amount', 0) or 0
                    change_pct = red.get('change_pct', 0) or 0
                    print(f"     ↘️ {red['token']}: {change_pct:+.1f}% ({old_amount:,.6f} → {new_amount:,.6f} tokens)")
            
            if changes["exits"]:
                print(f"  🚪 {len(changes['exits'])} sorties complètes")
                for exit in changes["exits"]:
                    old_amount = exit.get('old_amount', 0) or 0
                    print(f"     ❌ {exit['token']}: {old_amount:,.6f} → 0 tokens")
            
            # Sauvegarder les changements pour traitement ultérieur
            position_changes_found[address] = changes
        else:
            print(f"✅ Aucun changement significatif détecté")

        # Mettre à jour les données en base de données
        update_success = update_wallet_tokens_in_db(address, current_tokens_data)
        if not update_success:
            print(f"⚠️ Erreur mise à jour BDD pour {address}")

        total_value = df["usd_value"].sum()
        print(f"💰 Valeur totale : ${total_value:,.2f}")
        print(f"🪙 {len(df)} tokens avec quantité > {MIN_TOKEN_QUANTITY}")

        # Délai entre wallets pour éviter rate limiting
        time.sleep(3)

# === Fonction principale optimisée SQL
def run_live_wallet_changes_tracker():
    """Pipeline principal optimisé pour tracking des changements avec SQL"""
    session_id = str(uuid.uuid4())[:8]
    
    print(f"🚀 TRACKING CHANGEMENTS POSITIONS - Session {session_id}")
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🗄️ Version SQL optimisée avec jointures")
    
    init_folders()
    smart_wallets = get_smart_wallets_from_db()
    position_changes_found = {}
    
    total_changes = 0
    wallets_with_changes = 0
    
    print(f"🎯 Analyse de {len(smart_wallets)} smart wallets pour changements")

    for i in range(0, len(smart_wallets), BATCH_SIZE):
        batch = smart_wallets[i:i + BATCH_SIZE]
        print(f"\n🚀 Batch {i // BATCH_SIZE + 1} / {(len(smart_wallets) + BATCH_SIZE - 1) // BATCH_SIZE}")
        
        batch_changes = process_wallet_batch_sql(batch, position_changes_found, session_id)
        
        if i + BATCH_SIZE < len(smart_wallets):
            print(f"⏳ Pause {DELAY_BETWEEN_BATCHES}s...")
            time.sleep(DELAY_BETWEEN_BATCHES)

    # Toutes les données sont maintenant stockées en base de données
    # Plus besoin de système de cache fichier - tout est géré par les tables SQL
    
    # Sauvegarder tous les changements détectés
    if position_changes_found:
        # Compter les changements totaux
        total_new_tokens = sum(len(changes["new_tokens"]) for changes in position_changes_found.values())
        total_accumulations = sum(len(changes["accumulations"]) for changes in position_changes_found.values())
        total_reductions = sum(len(changes["reductions"]) for changes in position_changes_found.values())
        total_exits = sum(len(changes["exits"]) for changes in position_changes_found.values())
        
        changes_summary = {
            "timestamp": datetime.now().isoformat(),
            "position_changes": position_changes_found,
            "summary": {
                "wallets_with_changes": len(position_changes_found),
                "new_tokens": total_new_tokens,
                "accumulations": total_accumulations,
                "reductions": total_reductions,
                "exits": total_exits,
                "total_changes": total_new_tokens + total_accumulations + total_reductions + total_exits
            }
        }
        
        
        print(f"\n🔄 RÉSUMÉ CHANGEMENTS DÉTECTÉS:")
        print(f"  🆕 Nouveaux tokens: {total_new_tokens}")
        print(f"  📈 Accumulations: {total_accumulations}")
        print(f"  📉 Réductions: {total_reductions}")  
        print(f"  🚪 Sorties: {total_exits}")
        print(f"  📊 Total: {changes_summary['summary']['total_changes']} changements sur {len(position_changes_found)} wallets")
        print(f"\n💾 DONNÉES SAUVEGARDÉES EN BASE:")
        print(f"  📊 Changements stockés dans wallet_position_changes")
        print(f"  📈 Positions actuelles dans tokens")
        
    else:
        print("✅ Aucun changement de position détecté dans cette session")
        print(f"\n💾 DONNÉES EN BASE (AUCUN NOUVEAU CHANGEMENT):")
        print(f"  📊 Historique complet disponible dans les tables SQL")

    print("\n🎯 Tous les top wallets ont été traités pour la détection de changements de positions.")
    return True

# === Lancement direct
# === Fonction utilitaire pour consulter les changements
def get_recent_position_changes(hours=24, limit=50):
    """Récupère les changements récents avec jointure sur smart_wallets"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        query = """
            SELECT 
                wpc.session_id,
                wpc.wallet_address,
                sw.optimal_threshold_tier,
                wpc.symbol,
                wpc.change_type,
                wpc.amount_change,
                wpc.change_percentage,
                wpc.usd_change,
                wpc.detected_at
            FROM wallet_position_changes wpc
            LEFT JOIN smart_wallets sw ON wpc.wallet_address = sw.wallet_address
            WHERE wpc.detected_at >= datetime('now', '-{} hours')
            ORDER BY wpc.detected_at DESC
            LIMIT {}
        """.format(hours, limit)
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
        
    except Exception as e:
        print(f"❌ Erreur récupération changements récents: {e}")
        return pd.DataFrame()

def print_recent_changes_summary():
    """Affiche un résumé des changements récents"""
    print(f"\n📈 RÉSUMÉ CHANGEMENTS DERNIÈRES 24H:")
    print("-" * 60)
    
    df = get_recent_position_changes(24, 20)
    
    if df.empty:
        print("   Aucun changement récent détecté")
        return
    
    # Compter par type
    type_counts = df['change_type'].value_counts()
    
    print(f"📊 Statistiques:")
    for change_type, count in type_counts.items():
        print(f"   {change_type}: {count}")
    
    print(f"\n🔝 Top changements récents:")
    for _, row in df.head(10).iterrows():
        symbol = row['symbol']
        change_type = row['change_type']
        
        if change_type in ['ACCUMULATION', 'REDUCTION'] and pd.notna(row['change_percentage']):
            change_str = f"{row['change_percentage']:+.1f}%"
        else:
            change_str = f"${row['usd_change']:,.0f}" if pd.notna(row['usd_change']) else "N/A"
        
        timestamp = row['detected_at'][:16] if pd.notna(row['detected_at']) else "N/A"
        
        

if __name__ == "__main__":
    # Lancer le tracking des changements
    run_live_wallet_changes_tracker()
    
    # Afficher résumé
    print_recent_changes_summary()