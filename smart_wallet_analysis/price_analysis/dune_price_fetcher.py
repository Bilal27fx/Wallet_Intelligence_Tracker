#!/usr/bin/env python3
"""
Dune Analytics Price Fetcher
Récupère les prix historiques des tokens de consensus via l'API Dune
"""

import os
import time
import json
import requests
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dotenv import load_dotenv

# === Configuration ===
load_dotenv(dotenv_path=Path(__file__).parent.parent.parent / ".env")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
if not DUNE_API_KEY:
    raise ValueError("❌ Clé API Dune manquante. Ajoute DUNE_API_KEY dans ton fichier .env")

# Configuration Dune (mêmes variables que dune_api_loop_manual.py)
DUNE_BASE_URL = "https://api.dune.com/api/v1"
HEADERS = {
    "Content-Type": "application/json",
    "X-Dune-API-Key": DUNE_API_KEY
}
MAX_WAIT_TIME = 300
SLEEP_INTERVAL = 5
ROOT_DIR = Path(__file__).parent.parent.parent
CONSENSUS_DATA_DIR = ROOT_DIR / "data" / "backtesting" / "consensus_configurable"
PRICE_DATA_DIR = ROOT_DIR / "data" / "price_analysis"
DB_PATH = ROOT_DIR / "data" / "db" / "wit_database.db"

def execute_dune_query(query_id, parameters):
    """Fonction globale pour exécuter une requête Dune - exactement comme dans dune_api_loop_manual.py"""
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
    result_data = res.json()
    
    # Debug: afficher la structure complète de la réponse
    print(f"📋 Structure réponse: {list(result_data.keys())}")
    if "result" in result_data:
        result_section = result_data["result"]
        print(f"📋 Structure result: {list(result_section.keys()) if isinstance(result_section, dict) else type(result_section)}")
        if isinstance(result_section, dict) and "rows" in result_section:
            rows = result_section["rows"]
            print(f"📋 Nombre de rows: {len(rows)}")
        else:
            print(f"📋 Contenu result: {result_section}")
    
    rows = result_data.get("result", {}).get("rows", [])
    return rows


class DunePriceFetcher:
    """Gestionnaire pour exécuter des requêtes Dune Analytics avec query ID fixe"""
    
    def __init__(self):
        self.headers = HEADERS
        self.base_url = DUNE_BASE_URL
        self.max_wait_time = MAX_WAIT_TIME
        self.sleep_interval = SLEEP_INTERVAL
        
        # Créer dossier de sortie
        PRICE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    def execute_dune_query(self, query_id: int, parameters: Dict) -> Optional[List[Dict]]:
        """Exécute une requête Dune avec paramètres et retourne les résultats"""
        try:
            # Lancer l'exécution exactement comme dans dune_api_loop_manual.py
            exec_url = f"{self.base_url}/query/{query_id}/execute"
            res = requests.post(exec_url, headers=self.headers, json={"query_parameters": parameters})
            
            if res.status_code != 200:
                print(f"  ❌ Lancement échoué: {res.text}")
                return None

            execution_id = res.json()["execution_id"]
            print(f"  ⏳ Execution ID: {execution_id}")

            # Attendre la completion
            status_url = f"{self.base_url}/execution/{execution_id}/status"
            result_url = f"{self.base_url}/execution/{execution_id}/results"

            waited = 0
            while waited < self.max_wait_time:
                status = requests.get(status_url, headers=self.headers).json()
                state = status.get("state")
                print(f"  ⌛ Status: {state} — {waited}s")

                if state == "QUERY_STATE_COMPLETED":
                    break
                elif state in ["QUERY_STATE_FAILED", "QUERY_STATE_ERRORED"]:
                    print(f"  ❌ Erreur de requête: {state}")
                    return None

                time.sleep(self.sleep_interval)
                waited += self.sleep_interval

            if waited >= self.max_wait_time:
                print(f"  ⏰ Timeout dépassé")
                return None

            # Récupérer les résultats exactement comme dans dune_api_loop_manual.py
            res = requests.get(result_url, headers=self.headers)
            rows = res.json().get("result", {}).get("rows", [])
            
            return rows
            
        except Exception as e:
            print(f"  ❌ Exception: {e}")
            return None

def load_consensus_data() -> List[Dict]:
    """Charge les données de consensus depuis les fichiers JSON"""
    consensus_files = list(CONSENSUS_DATA_DIR.glob("consensus_backtesting_*.json"))
    
    if not consensus_files:
        print("❌ Aucun fichier de consensus trouvé")
        return []
    
    # Prendre le fichier le plus récent
    latest_file = max(consensus_files, key=lambda f: f.stat().st_mtime)
    print(f"📂 Chargement des consensus depuis: {latest_file.name}")
    
    try:
        with open(latest_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"✅ {len(data)} consensus chargés")
        return data
        
    except Exception as e:
        print(f"❌ Erreur lecture fichier consensus: {e}")
        return []

def save_to_database(consensus_data: List[Dict], results: Dict, execution_id: str = None):
    """Sauvegarde les prix de consensus dans la base de données"""
    
    if not results:
        print("❌ Aucun résultat à sauvegarder en BDD")
        return
    
    print(f"\n💾 SAUVEGARDE EN BASE DE DONNÉES")
    print("=" * 50)
    
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        
        saved_count = 0
        total_rows = 0
        
        for token_data in results.values():
            token = token_data['token']
            contract = token_data['contract'] 
            chain = token_data['chain']
            days_since = token_data['days_since_consensus_end']
            price_data = token_data['price_data']
            
            # Calculer la date de consensus depuis days_since_consensus_end
            consensus_info = next((c for c in consensus_data if c['token'] == token), None)
            if not consensus_info:
                print(f"  ⚠️  {token}: Pas de données de consensus trouvées")
                continue
                
            # Calculer la date de fin de consensus
            consensus_date = (datetime.now() - timedelta(days=days_since)).strftime('%Y-%m-%d')
            print(f"    📅 Date consensus calculée: {consensus_date}")
            
            print(f"  💾 {token} ({len(price_data)} jours de prix)")
            
            for day_data in price_data:
                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO consensus_prices (
                            token_symbol, contract_address, chain,
                            consensus_date, days_since_consensus,
                            price_date, nb_trades, avg_price_usd, vwap_price_usd,
                            volume_usd, volume_token, execution_id, fetched_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        token,
                        contract.lower(),
                        chain,
                        consensus_date,
                        days_since,
                        day_data['day'],
                        day_data.get('nb_trades'),
                        day_data.get('avg_price_usd'),
                        day_data.get('vwap_price_usd'),
                        day_data.get('volume_usd'),
                        day_data.get('volume_token'),
                        execution_id,
                        datetime.now()
                    ))
                    total_rows += 1
                    
                except Exception as e:
                    print(f"    ❌ Erreur ligne {day_data['day']}: {e}")
            
            saved_count += 1
        
        conn.commit()
        conn.close()
        
        print(f"✅ {saved_count}/{len(results)} tokens sauvegardés")
        print(f"✅ {total_rows} lignes de prix insérées en BDD")
        
    except Exception as e:
        print(f"❌ Erreur sauvegarde BDD: {e}")
        if 'conn' in locals():
            conn.close()

def create_price_query_sql() -> str:
    """Retourne le SQL template pour les prix de tokens"""
    return """
-- Prix moyen journalier d'un token sur les DEX depuis N jours en arrière
-- Paramètres : {{contract}} (Address), {{chain}} (Text), {{offset_days}} (Int)

WITH token_trades AS (
  SELECT
      CAST(block_time AS date) AS day,

      CASE
        WHEN token_bought_address = {{contract}} THEN token_bought_amount
        WHEN token_sold_address   = {{contract}} THEN token_sold_amount
        ELSE NULL
      END AS token_amount,

      amount_usd,

      CASE
        WHEN token_bought_address = {{contract}} THEN amount_usd / NULLIF(token_bought_amount, 0)
        WHEN token_sold_address   = {{contract}} THEN amount_usd / NULLIF(token_sold_amount, 0)
        ELSE NULL
      END AS price_usd
  FROM dex.trades
  WHERE
      blockchain = '{{chain}}'
      AND block_time >= date_add('day', -{{offset_days}}, now())
      AND amount_usd IS NOT NULL
      AND amount_usd > 0
      AND (
        token_bought_address = {{contract}}
        OR token_sold_address = {{contract}}
      )
)
SELECT
    day,
    COUNT(*) AS nb_trades,
    AVG(price_usd) AS avg_price_usd,
    SUM(price_usd * token_amount) / NULLIF(SUM(token_amount), 0) AS vwap_price_usd,
    SUM(amount_usd) AS volume_usd,
    SUM(token_amount) AS volume_token
FROM token_trades
GROUP BY day
ORDER BY day;
"""

def process_consensus_tokens():
    """Traite tous les tokens de consensus pour récupérer leurs prix via query ID 5948361"""
    
    print("🚀 === DUNE ANALYTICS PRICE FETCHER ===")
    print("=" * 60)
    
    # 1. Charger les données de consensus
    consensus_data = load_consensus_data()
    if not consensus_data:
        return
    
    # 2. Créer dossier de sortie
    PRICE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # 3. Utiliser le query ID fixe existant
    QUERY_ID = 5948361
    print(f"🔗 Utilisation de la requête Dune existante: {QUERY_ID}")
    
    print(f"\n📊 TRAITEMENT DE {len(consensus_data)} TOKENS")
    print("=" * 60)
    
    # 4. Traiter chaque token
    results = {}
    successful_queries = 0
    
    for i, consensus in enumerate(consensus_data, 1):
        token = consensus['token']
        contract = consensus['contract']
        chain = consensus['chain']
        days_since = consensus['days_since_consensus_end']
        
        print(f"\n[{i}/{len(consensus_data)}] 🪙 {token} ({contract[:10]}...)")
        print(f"  📅 Consensus terminé il y a: {days_since} jours")
        
        # Utiliser le nouveau paramètre offset_days selon la nouvelle requête SQL
        params = {
            "contract": contract.lower(),
            "chain": chain,
            "offset_days": days_since + 5  # +5 jours de marge pour avoir des données complètes
        }
        
        print(f"  📅 Récupération des {days_since + 5} derniers jours (consensus + marge)")
        print(f"  🔧 Params: contract={contract[:10]}..., chain={chain}, offset_days={days_since + 5}")
        
        try:
            # Utiliser exactement la même fonction globale que dune_api_loop_manual.py
            rows = execute_dune_query(QUERY_ID, params)
            
            if rows:
                print(f"  ✅ {len(rows)} jours de données récupérés")
                
                results[token] = {
                    'token': token,
                    'contract': contract,
                    'chain': chain,
                    'days_since_consensus_end': days_since,
                    'days_queried': days_since + 5,
                    'price_data': rows,
                    'parameters_used': params
                }
                successful_queries += 1
            else:
                print(f"  ❌ Aucun résultat retourné")
                
        except Exception as e:
            print(f"  ❌ Erreur requête: {e}")
        
        # Rate limiting
        time.sleep(3)
    
    # 5. Sauvegarder tous les résultats
    if results:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        execution_id = f"fetch_{timestamp}"
        
        # Sauvegarde en base de données
        save_to_database(consensus_data, results, execution_id)
        
        # Sauvegarde en JSON (backup)
        output_file = PRICE_DATA_DIR / f"consensus_prices_{timestamp}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n✅ RÉSULTATS SAUVEGARDÉS:")
        print(f"   💾 Base de données: consensus_prices table")
        print(f"   📁 Fichier JSON: {output_file}")
        print(f"📊 {successful_queries}/{len(consensus_data)} tokens traités avec succès")
        print(f"🔗 Query ID utilisé: {QUERY_ID}")
        print(f"🆔 Execution ID: {execution_id}")
    else:
        print("\n❌ Aucun résultat à sauvegarder")

if __name__ == "__main__":
    process_consensus_tokens()