import os
import time
import requests
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Import database utilities
import sys
sys.path.append(str(Path(__file__).parent.parent.parent / "db"))
from database_utils import insert_wallet, insert_token, get_wallet, DatabaseManager

# === Configuration globale ===
load_dotenv(dotenv_path=Path(__file__).parent.parent.parent / ".env")

# Système de rotation des clés API
class APIKeyManager:
    def __init__(self):
        self.keys = [
            os.getenv("ZERION_API_KEY"),
            os.getenv("ZERION_API_KEY_2")
        ]
        # Filtrer les clés vides
        self.keys = [k for k in self.keys if k]

        if not self.keys:
            raise ValueError("❌ Aucune clé API Zerion trouvée dans .env")

        self.current_index = 0
        self.current_key = self.keys[self.current_index]
        print(f"🔑 {len(self.keys)} clé(s) API Zerion chargée(s)")

    def get_key(self):
        """Retourne la clé API active"""
        return self.current_key

    def rotate_key(self):
        """Passe à la clé suivante"""
        if len(self.keys) <= 1:
            print("⚠️ Une seule clé API disponible, impossible de rotationner")
            return False

        self.current_index = (self.current_index + 1) % len(self.keys)
        self.current_key = self.keys[self.current_index]
        print(f"🔄 Rotation vers clé API #{self.current_index + 1}")
        return True

# Instance globale du gestionnaire de clés
api_manager = APIKeyManager()

# Seuils de filtrage
MIN_TOKEN_VALUE_USD = 500  # 500 USD minimum par token
MIN_WALLET_VALUE_USD = 100000  # 200k USD minimum
MAX_WALLET_VALUE_USD = 50000000  # 50M USD maximum
MIN_TOKENS_PER_WALLET = 3  # Minimum 3 tokens par wallet
MAX_TOKENS_PER_WALLET = 60  # Maximum 30 tokens par wallet

# === Configuration ===
ROOT = Path(__file__).parent.parent.parent
DB_PATH = ROOT / "data" / "db" / "wit_database.db"
PERIODS = ["14d", "30d", "200d", "360d", "manual"]

BATCH_SIZE = 5
DELAY_BETWEEN_BATCHES = 5

# === Lecture des wallets depuis la base de données
def get_wallet_period_mapping():
    """Récupère les wallets depuis la table wallet_brute"""
    try:
        import sqlite3
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        
        # Récupérer tous les wallets uniques avec leur temporalité
        cursor.execute("""
            SELECT DISTINCT wallet_address, temporality 
            FROM wallet_brute 
            ORDER BY wallet_address
        """)
        
        results = cursor.fetchall()
        conn.close()
        
        wallet_to_period = {}
        for wallet_address, temporality in results:
            # Utiliser la temporalité de la base ou "manual" par défaut
            wallet_to_period[wallet_address] = temporality if temporality else "manual"
        
        print(f"📊 {len(wallet_to_period)} wallets uniques récupérés depuis wallet_brute")
        return wallet_to_period
        
    except Exception as e:
        print(f"❌ Erreur lecture wallet_brute: {e}")
        return {}

# Plus besoin de cache CSV - on utilise la BDD directement

# === Récupération du fungible_id via Zerion API
def get_fungible_id_zerion(contract_address, chain, token_symbol=""):
    """Récupère le fungible_id d'un token via l'API Zerion /fungibles"""
    
    # Cas spécial : ETH natif (pas de contract_address)
    if token_symbol.upper() == "ETH" and not contract_address:
        return "eth"  # ID standard pour ETH natif sur toutes les chains
    
    # Cas normal : token avec contract_address
    if not contract_address or not chain:
        return ""
    
    url = f"https://api.zerion.io/v1/fungibles/?filter[implementation_address]={contract_address.lower()}&filter[implementation_chain_id]={chain}"
    
    headers = {
        "accept": "application/json",
        "authorization": f"Basic {api_manager.get_key()}"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)

        # Gestion quota API (429)
        if response.status_code == 429:
            print(f"🚧 Quota atteint sur clé actuelle, rotation...")
            if api_manager.rotate_key():
                time.sleep(5)  # Pause avant retry
                return get_fungible_id_zerion(contract_address, chain, token_symbol)  # Retry avec nouvelle clé
            else:
                print("❌ Toutes les clés API ont atteint leur quota")
                return ""

        response.raise_for_status()

        data = response.json()
        fungibles = data.get("data", [])

        if fungibles:
            # Prendre le premier résultat (devrait être unique)
            fungible_id = fungibles[0].get("id", "")
            return fungible_id
        else:
            return ""

    except Exception as e:
        print(f"⚠️ Erreur récupération fungible_id pour {contract_address}: {e}")
        return ""

# === Récupération des balances via Zerion API
def get_token_balances_zerion(address):
    """Récupère les balances d'un wallet via l'API Zerion avec filtrage par valeur wallet"""
    url = f"https://api.zerion.io/v1/wallets/{address}/positions/?filter[positions]=only_simple&currency=usd&filter[trash]=only_non_trash&sort=value"
    
    headers = {
        "accept": "application/json",
        "authorization": f"Basic {api_manager.get_key()}"
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)

        # Gestion quota API (429)
        if response.status_code == 429:
            print(f"🚧 Quota atteint sur clé actuelle pour {address}, rotation...")
            if api_manager.rotate_key():
                time.sleep(5)  # Pause avant retry
                return get_token_balances_zerion(address)  # Retry avec nouvelle clé
            else:
                print("❌ Toutes les clés API ont atteint leur quota")
                return pd.DataFrame()

        response.raise_for_status()

        data = response.json()
        all_positions = data.get("data", [])
        
        # Calculer d'abord la valeur totale du wallet avec gestion des None
        def safe_float(value, default=0):
            if value is None:
                return default
            if isinstance(value, dict):
                return float(value.get("numeric", default))
            try:
                return float(value)
            except (TypeError, ValueError):
                return default
        
        total_wallet_value = sum(
            safe_float(pos.get("attributes", {}).get("value"))
            for pos in all_positions
        )
        
        # Filtrer par valeur minimale du wallet (200k USD)
        if total_wallet_value < MIN_WALLET_VALUE_USD:
            print(f"❌ Wallet {address}: ${total_wallet_value:,.0f} < ${MIN_WALLET_VALUE_USD:,.0f} minimum - SKIP")
            return pd.DataFrame()
        
        # Filtrer par valeur maximale du wallet (50M USD)
        if total_wallet_value > MAX_WALLET_VALUE_USD:
            print(f"❌ Wallet {address}: ${total_wallet_value:,.0f} > ${MAX_WALLET_VALUE_USD:,.0f} maximum - SKIP (probablement exchange/whale)")
            return pd.DataFrame()
        
        print(f"✅ Wallet {address}: ${total_wallet_value:,.0f} - VALIDE")
        
        # Définir les tokens à exclure du comptage (tous types de stablecoins + ETH/BTC + wrapped + staked)
        EXCLUDED_TOKENS = {
            # Stablecoins
            'USDC', 'USDT', 'DAI', 'BUSD', 'FRAX', 'TUSD', 'USDP', 'GUSD', 'LUSD', 'MIM', 'USTC', 'UST',
            'USDD', 'USDN', 'HUSD', 'SUSD', 'CUSD', 'DUSD', 'OUSD', 'MUSD', 'ZUSD', 'RUSD', 'VUSD',
            'USDX', 'USDK', 'EURS', 'EURT', 'CADC', 'XSGD', 'IDRT', 'TRYB', 'NZDS', 'BIDR',
            
            # ETH et toutes ses variantes
            'ETH', 'WETH', 'ETHEREUM', 'STETH', 'WSTETH', 'RETH', 'CBETH', 'FRXETH', 'SFRXETH',
            'ANKRETH', 'SETH2', 'ALETH', 'AETHC', 'QETH', 'EETH', 'WEETH', 'OETH', 'WOETH',
            'METH', 'SWETH', 'XETH', 'LSETH', 'UNIETH', 'PXETH', 'APXETH', 'YETH', 'EZETH',
            'RSETH', 'UNIAETH', 'ETHX', 'SAETH', 'TETH', 'VETH', 'DETH', 'HETH', 'PTETH',
            
            # BTC et toutes ses variantes  
            'BTC', 'WBTC', 'BITCOIN', 'RENBTC', 'SBTC', 'HBTC', 'OBTC', 'TBTC', 'WIBBTC',
            'PBTC', 'XBTC', 'BBTC', 'FBTC', 'LBTC', 'CBTC', 'VBTC', 'RBTC', 'KBTC', 'ABTC',
            'BTCB', 'BBTC', 'MBTC', 'UBTC', 'DBTC', 'NBTC', 'GBTC', 'YBTC', 'ZBTC',
            
            # BNB et variantes
            'BNB', 'BNBCHAIN', 'WBNB', 'BBNB', 'SBNB', 'VBNB',
            
            # Autres majeurs wrapped/staked
            'WMATIC', 'SMATIC', 'STMATIC', 'MATIC', 'POLYGON',
            'WAVAX', 'SAVAX', 'STAVAX', 'AVAX', 'AVALANCHE',
            'WSOL', 'SSOL', 'STSOL', 'SOL', 'SOLANA',
            'WFTM', 'SFTM', 'STFTM', 'FTM', 'FANTOM',
            'WDOT', 'SDOT', 'STDOT', 'DOT', 'POLKADOT',
            'WADA', 'SADA', 'STADA', 'ADA', 'CARDANO',
            'WATOM', 'SATOM', 'STATOM', 'ATOM', 'COSMOS',
            'NEAR', 'WNEAR', 'STNEAR', 'LINEAR',
            'LUNA', 'WLUNA', 'STLUNA', 'TERRA',
            'ONE', 'WONE', 'STONE', 'HARMONY',
            'WONE', 'TONE', 'VONE', 'SONE'
        }
        
        # Vérifier d'abord le nombre total de tokens avec valeur > $500
        valid_positions = []
        excluded_positions = []
        
        for pos in all_positions:
            attrs = pos.get("attributes", {})
            usd_value = safe_float(attrs.get("value"))
            
            if usd_value >= MIN_TOKEN_VALUE_USD:  # $500 minimum
                fungible_info = attrs.get("fungible_info", {})
                token_symbol = fungible_info.get("symbol", "UNKNOWN").upper()
                
                if token_symbol in EXCLUDED_TOKENS:
                    excluded_positions.append(pos)
                else:
                    valid_positions.append(pos)
        
        print(f"    📊 {len(valid_positions)} tokens valides (hors stablecoins/ETH/BTC/wrapped)")
        print(f"    🚫 {len(excluded_positions)} tokens exclus (stablecoins/ETH/BTC/wrapped)")

        # Rejeter le wallet s'il a moins de MIN_TOKENS_PER_WALLET tokens valides (hors exclus)
        if len(valid_positions) < MIN_TOKENS_PER_WALLET:
            print(f"    🚫 Wallet rejeté: {len(valid_positions)} tokens valides < {MIN_TOKENS_PER_WALLET} (minimum requis)")
            return pd.DataFrame()
        
        # Combiner les positions valides ET exclues pour l'extraction complète
        all_valid_positions = valid_positions + excluded_positions
        
        # Appliquer la limite MAX_TOKENS_PER_WALLET sur le total
        if len(all_valid_positions) > MAX_TOKENS_PER_WALLET:
            print(f"    🚫 Wallet rejeté: {len(all_valid_positions)} tokens > {MAX_TOKENS_PER_WALLET} (maximum autorisé)")
            return pd.DataFrame()
        
        # Traiter tous les tokens valides (inclut les exclus pour l'extraction complète)
        filtered_tokens = []
        for pos in all_valid_positions:
            attrs = pos.get("attributes", {})
            fungible_info = attrs.get("fungible_info", {})
            
            # Quantité avec gestion des None
            amount = safe_float(attrs.get("quantity"))
            
            # Valeur USD avec gestion des None  
            usd_value = safe_float(attrs.get("value"))
            
            # Pas besoin de re-filtrer par valeur, déjà fait dans valid_positions
            
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
            fungible_id = get_fungible_id_zerion(contract_address, chain, token)
            
            # Petit délai pour éviter de surcharger l'API
            time.sleep(0.2)
            
            filtered_tokens.append({
                "token": token.strip().upper(),
                "amount": amount,
                "usd_value": usd_value,
                "chain": chain,
                "contract_address": contract_address,
                "contract_decimals": contract_decimals,
                "fungible_id": fungible_id
            })
        
        # Vérifier le nombre minimum de tokens après filtrage
        if len(filtered_tokens) < MIN_TOKENS_PER_WALLET:
            print(f"❌ Wallet {address}: {len(filtered_tokens)} tokens < {MIN_TOKENS_PER_WALLET} minimum - SKIP")
            return pd.DataFrame()
        
        print(f"🪙 {len(filtered_tokens)} tokens valides ({MIN_TOKENS_PER_WALLET}-{MAX_TOKENS_PER_WALLET} requis)")
        return pd.DataFrame(filtered_tokens)
        
    except Exception as e:
        print(f"❌ Erreur Zerion API pour {address}: {e}")
        return pd.DataFrame()

# === Traitement d'un batch de wallets avec stockage BDD
def process_wallet_batch(wallets, wallet_to_period):
    for address in wallets:
        periode = wallet_to_period.get(address, "unk")
        if periode not in PERIODS and periode != "unk":
            # Si ce n'est pas une période standard, utiliser "manual"
            periode = "manual"
        if periode not in PERIODS:
            continue

        # Vérifier si wallet existe déjà en BDD
        existing_wallet = get_wallet(address)
        if existing_wallet:
            print(f"⏩ {address} déjà en BDD — SKIP")
            continue


        print(f"\n=== {address} | Période : {periode} ===")
        df = get_token_balances_zerion(address)
        if df.empty:
            print("❌ Wallet ne respecte pas les critères de filtrage.")
            continue

        total_value = df["usd_value"].sum()
        print(f"💰 Valeur totale : ${total_value:,.2f}")
        print(f"🪙 {len(df)} tokens ({MIN_TOKENS_PER_WALLET}-{MAX_TOKENS_PER_WALLET} range, min ${MIN_TOKEN_VALUE_USD} par token)")

        # 1. Insérer le wallet en BDD
        success = insert_wallet(address, periode, total_value)
        if not success:
            print(f"❌ Erreur insertion wallet {address}")
            continue
        
        print(f"✅ Wallet {address} inséré en BDD")

        # 2. Insérer chaque token en BDD
        tokens_inserted = 0
        for _, row in df.iterrows():
            token_success = insert_token(
                wallet_address=address,
                fungible_id=row['fungible_id'],
                symbol=row['token'],
                contract_address=row['contract_address'],
                chain=row['chain'],
                amount=row['amount'],
                usd_value=row['usd_value'],
                price=row['usd_value'] / row['amount'] if row['amount'] > 0 else 0
            )
            if token_success:
                tokens_inserted += 1
        
        print(f"✅ {tokens_inserted}/{len(df)} tokens insérés pour {address}")
        print("📦 Données stockées en base de données")

# === Fonction principale
def run_wallet_balance_pipeline():
    wallet_to_period = get_wallet_period_mapping()
    addresses = list(wallet_to_period.keys())

    print(f"🎯 {len(addresses)} wallets à traiter")

    for i in range(0, len(addresses), BATCH_SIZE):
        batch = addresses[i:i + BATCH_SIZE]
        print(f"\n🚀 Batch {i // BATCH_SIZE + 1} / {len(addresses) // BATCH_SIZE + 1}")
        process_wallet_batch(batch, wallet_to_period)

        if i + BATCH_SIZE < len(addresses):
            print(f"⏳ Pause {DELAY_BETWEEN_BATCHES}s...")
            time.sleep(DELAY_BETWEEN_BATCHES)

    print("\n🎯 Tous les wallets ont été traités et stockés en BDD.")

# === Lancement direct
if __name__ == "__main__":
    run_wallet_balance_pipeline()
