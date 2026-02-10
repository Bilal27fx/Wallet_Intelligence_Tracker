import os
import json
import yaml
import time
import requests
import pandas as pd
import sqlite3
from pathlib import Path
from dotenv import load_dotenv
import sys

# Ajouter le module smart_contrat_remover au path
sys.path.insert(0, str(Path(__file__).parent.parent / "token_discovery"))
from smart_contrat_remover import ContractChecker

# === Chargement des variables d'environnement
load_dotenv()
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_BASE_URL = "https://api.dune.com/api/v1"

# === Constantes
ROOT = Path(__file__).parent.parent.parent
DUNE_YML_PATH = ROOT / "config" / "dune.yml"
INPUT_JSON_PATH = Path(__file__).parent.parent / "explosive_tokens_manual.json"
EXPORT_DIR = ROOT / "data" / "raw" / "csv" / "top_wallets"
CACHE_PATH = ROOT / "data" / "cache" / "early_wallets_extracted_manual.csv"
DB_PATH = ROOT / "data" / "db" / "wit_database.db"

HEADERS = {
    "Content-Type": "application/json",
    "X-Dune-API-Key": DUNE_API_KEY
}
MAX_WAIT_TIME = 700
SLEEP_INTERVAL = 5

# Mapping des chaînes vers les configs Dune
CHAIN_MAPPING = {
    "base": "base",
    "ethereum": "ethereum",
    "bnbchain": "bnb",
    "bnb": "bnb",
    "bsc": "bnb"  # Autres alias pour BNB Smart Chain
}

def execute_dune_query(query_id, parameters):
    exec_url = f"{DUNE_BASE_URL}/query/{query_id}/execute"
    res = requests.post(exec_url, headers=HEADERS, json={"query_parameters": parameters})
    if res.status_code != 200:
        raise Exception(f"❌ Lancement échoué : {res.text}")

    execution_id = res.json()["execution_id"]
    print(f"⏳ Execution ID : {execution_id}")

    status_url = f"{DUNE_BASE_URL}/execution/{execution_id}/status"
    result_url = f"{DUNE_BASE_URL}/execution/{execution_id}/results"

    waited = 0
    while waited < MAX_WAIT_TIME:
        status = requests.get(status_url, headers=HEADERS).json()
        state = status.get("state")
        print(f"⌛ Status : {state} — {waited}s")

        if state == "QUERY_STATE_COMPLETED":
            break
        elif state in ["QUERY_STATE_FAILED", "QUERY_STATE_ERRORED"]:
            raise Exception(f"❌ Erreur de requête : {state}")

        time.sleep(SLEEP_INTERVAL)
        waited += SLEEP_INTERVAL

    if waited >= MAX_WAIT_TIME:
        raise TimeoutError("⏰ Timeout dépassé")

    res = requests.get(result_url, headers=HEADERS)
    rows = res.json().get("result", {}).get("rows", [])
    return pd.DataFrame(rows)


def load_dune_config():
    with open(DUNE_YML_PATH, "r") as f:
        return yaml.safe_load(f)


def convert_period_to_days(perf_window):
    """Convert period string like '250j' to integer days"""
    if perf_window.endswith('j'):
        return int(perf_window[:-1])
    elif perf_window.endswith('d'):
        return int(perf_window[:-1])
    else:
        # Try to convert directly if it's just a number
        try:
            return int(perf_window)
        except ValueError:
            raise ValueError(f"Format de période non reconnu: {perf_window}")


def load_cache():
    if CACHE_PATH.exists():
        return pd.read_csv(CACHE_PATH)
    else:
        return pd.DataFrame(columns=["token_address", "chain", "perf_window"])


def update_cache(df_cache, token_address, chain, perf_window):
    df_cache.loc[len(df_cache)] = {
        "token_address": token_address,
        "chain": chain,
        "perf_window": perf_window
    }
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_cache.to_csv(CACHE_PATH, index=False)

def filter_eoa_wallets(df):
    """Filtre les wallets pour ne garder que les EOA (pas les smart contracts)"""
    try:
        print(f"🔍 Filtrage EOA sur {len(df)} wallets...")
        
        # Initialiser le checker de contrats
        checker = ContractChecker()
        
        # Extraire les adresses uniques
        addresses = df['wallet'].unique().tolist()
        print(f"📊 {len(addresses)} adresses uniques à vérifier")
        
        # Vérifier chaque adresse (traitement simplifié pour éviter les batches complexes)
        eoa_addresses = []
        for i, address in enumerate(addresses, 1):
            print(f"  [{i}/{len(addresses)}] Vérification {address[:10]}...")
            
            is_contract = checker.is_contract_single(address)
            
            if is_contract is None:
                print(f"    ❌ Erreur API, exclusion par sécurité")
                continue
            elif is_contract:
                print(f"    🏗️ Smart contract détecté, exclusion")
                continue
            else:
                print(f"    👤 EOA confirmé, conservation")
                eoa_addresses.append(address)
            
            # Petite pause entre appels
            if i < len(addresses):
                time.sleep(0.2)
        
        # Filtrer le DataFrame pour ne garder que les EOA
        df_filtered = df[df['wallet'].isin(eoa_addresses)]
        
        print(f"✅ Filtrage terminé: {len(df_filtered)}/{len(df)} wallets conservés (EOA uniquement)")
        return df_filtered
        
    except Exception as e:
        print(f"❌ Erreur lors du filtrage EOA: {e}")
        print(f"⚠️ Conservation de tous les wallets par sécurité")
        return df

def insert_wallets_to_db(df, token_address, token_symbol, chain, temporality):
    """Insère les wallets dans la table wallet_brute (après filtrage EOA)"""
    try:
        # Filtrer pour ne garder que les EOA
        df_eoa = filter_eoa_wallets(df)
        
        if df_eoa.empty:
            print("⚠️ Aucun EOA trouvé après filtrage, skip insertion")
            return True
        
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        
        for _, row in df_eoa.iterrows():
            cursor.execute("""
                INSERT OR IGNORE INTO wallet_brute 
                (wallet_address, token_address, token_symbol, contract_address, chain, temporality)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                row['wallet'],
                token_address,
                token_symbol,
                token_address,  # contract_address = token_address
                chain,
                temporality
            ))
        
        conn.commit()
        conn.close()
        print(f"💾 {len(df_eoa)} wallets EOA insérés dans wallet_brute")
        return True
        
    except Exception as e:
        print(f"❌ Erreur insertion BDD: {e}")
        return False

def is_already_processed_db(token_address, chain, perf_window):
    """Vérifie si le token a déjà été traité en base"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) FROM wallet_brute 
            WHERE token_address = ? AND chain = ? AND temporality = ?
        """, (token_address, chain, perf_window))
        
        count = cursor.fetchone()[0]
        conn.close()
        
        return count > 0
        
    except Exception as e:
        print(f"❌ Erreur vérification BDD: {e}")
        return False

def ensure_wallet_brute_table():
    """S'assure que la table wallet_brute existe"""
    try:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS wallet_brute (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_address TEXT NOT NULL,
                token_address TEXT NOT NULL,
                token_symbol TEXT,
                contract_address TEXT NOT NULL,
                chain TEXT NOT NULL,
                temporality TEXT NOT NULL,
                detection_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                UNIQUE(wallet_address, token_address, temporality)
            )
        """)
        
        # Index pour performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallet_brute_token ON wallet_brute(token_address)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallet_brute_wallet ON wallet_brute(wallet_address)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallet_brute_chain ON wallet_brute(chain)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallet_brute_temporality ON wallet_brute(temporality)")
        
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Erreur création table wallet_brute: {e}")
        return False


def run_manual_token_discovery():
    """
    Exécute la découverte de tokens avec le fichier manual JSON
    """
    if not INPUT_JSON_PATH.exists():
        print(f"❌ Fichier d'entrée non trouvé: {INPUT_JSON_PATH}")
        return

    # S'assurer que la table wallet_brute existe
    if not ensure_wallet_brute_table():
        print(f"❌ Impossible de créer/vérifier la table wallet_brute")
        return

    dune_config = load_dune_config()
    cache_df = load_cache()
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    # Charger le fichier JSON des tokens explosifs
    try:
        with open(INPUT_JSON_PATH, "r") as f:
            tokens = json.load(f)
    except Exception as e:
        print(f"❌ Erreur lecture JSON: {e}")
        return

    print(f"🚀 Traitement de {len(tokens)} tokens explosifs")

    for i, token in enumerate(tokens, 1):
        try:
            token_address = token["token_address"].lower()
            perf_window_str = token["perf_window"]
            chain = token["chain"].lower()
            
            print(f"\n[{i}/{len(tokens)}] 🎯 Token: {token_address}")
            print(f"📊 Période: {perf_window_str} | Chaîne: {chain}")
            
        except KeyError as e:
            print(f"[SKIP] Clé manquante dans token {i}: {e}")
            continue

        # Vérifier si déjà traité en base de données
        if is_already_processed_db(token_address, chain, perf_window_str):
            print(f"⏩ Déjà traité en BDD : {token_address} [{perf_window_str}]")
            continue

        # Convertir la période en jours avec temporalité selon le type
        try:
            perf_days = convert_period_to_days(perf_window_str)
            
            # Récupérer le type de temporalité depuis le JSON
            token_type = token.get("type", 1)  # Type 1 par défaut si non spécifié
            
            if token_type == 1:
                early_days = perf_days + 90  # Type 1: fenêtre d'accumulation de 50j
                print(f"📅 Type 1 temporalité: accumulation {early_days}j (perf + 90j)")
            elif token_type == 2:
                early_days = perf_days + 20  # Type 2: fenêtre d'accumulation de 20j
                print(f"📅 Type 2 temporalité: accumulation {early_days}j (perf + 20j)")
            else:
                print(f"⚠️  Type {token_type} non reconnu, utilisation Type 1 par défaut")
                early_days = perf_days + 90
                
        except ValueError as e:
            print(f"[SKIP] {e}")
            continue

        # Récupérer la configuration Dune pour cette chaîne
        chain_key = CHAIN_MAPPING.get(chain)
        if not chain_key:
            print(f"[SKIP] Chaîne non supportée: {chain}")
            continue

        query_id = dune_config.get(chain_key, {}).get("top_wallet")
        if not query_id:
            print(f"[SKIP] Aucune query Dune pour {chain}")
            continue

        # Paramètres pour la requête Dune
        params = {
            "token_address": token_address,
            "perf_window": perf_days,
            "early_window": early_days
        }

        print(f"[📡] Query ID : {query_id}")
        print(f"[🔧] Params : perf={perf_days}j, early={early_days}j")

        try:
            df = execute_dune_query(query_id, params)
            if df.empty:
                print("⚠️ Aucun résultat — skip")
                update_cache(cache_df, token_address, chain, perf_window_str)
                continue

            # Insérer dans la base de données
            token_symbol = token.get("symbol", "UNKNOWN")
            success = insert_wallets_to_db(df, token_address, token_symbol, chain, perf_window_str)
            
            if success:
                print(f"📈 {len(df)} wallets stockés en BDD")
                
                # Optionnel : conserver aussi l'export CSV pour compatibilité
                # export_name = f"{token_address}_{perf_window_str}_{chain}_manual.csv"
                # export_path = EXPORT_DIR / export_name
                # df.to_csv(export_path, index=False)
                # print(f"💾 Export CSV (compat) : {export_path}")

            # Mettre à jour le cache
            update_cache(cache_df, token_address, chain, perf_window_str)

        except Exception as e:
            print(f"[❌] Erreur requête : {e}")
            continue

    print(f"\n✅ Traitement terminé !")


if __name__ == "__main__":
    run_manual_token_discovery()