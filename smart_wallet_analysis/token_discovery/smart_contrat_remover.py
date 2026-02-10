import pandas as pd
import requests
import time
import os
import json
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional, Dict, List
import pickle

# === Chargement de l'API key
load_dotenv()
ETHERSCAN_API = os.getenv("ETHERSCAN_API_KEY")
if not ETHERSCAN_API:
    raise ValueError("❌ Clé API manquante : ETHERSCAN_API_KEY")

# === Configuration
ROOT = Path(__file__).parent.parent.parent
INPUT_FILE = ROOT / "data/processed/wallets_sources_new.csv"
OUTPUT_FILE = ROOT / "data/processed/wallets_sources_EOA_only.csv"
CACHE_FILE = ROOT / "data/cache/contract_cache.pkl"

# Rate limiting amélioré
BATCH_SIZE = 20  # Traiter par batch
DELAY_BETWEEN_CALLS = 0.5  # 500ms entre appels
DELAY_BETWEEN_BATCHES = 5  # 5s entre batch
MAX_RETRIES = 3
TIMEOUT = 10

class ContractChecker:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'WIT-Contract-Checker/1.0'})

    def is_contract_single(self, address: str, retry_count: int = 0) -> Optional[bool]:
        """Vérifie une seule adresse avec retry"""

        url = "https://api.etherscan.io/v2/api"
        params = {
            "chainid": "1",  # Ethereum mainnet
            "module": "proxy",
            "action": "eth_getCode",
            "address": address,
            "apikey": ETHERSCAN_API
        }

        try:
            response = self.session.get(url, params=params, timeout=TIMEOUT)
            response.raise_for_status()

            data = response.json()

            # Gestion des erreurs API
            if data.get("status") == "0" and "rate limit" in data.get("message", "").lower():
                print(f"⚠️ Rate limit atteint, pause de {DELAY_BETWEEN_BATCHES * 2}s...")
                time.sleep(DELAY_BETWEEN_BATCHES * 2)
                if retry_count < MAX_RETRIES:
                    return self.is_contract_single(address, retry_count + 1)
                return None

            code = data.get("result", "")
            is_contract = bool(code and code != "0x")

            status = "contract" if is_contract else "EOA"
            print(f"  {address[:10]}... → {status} (code: '{code[:20]}...')")

            # DEBUG: Afficher la réponse complète pour le premier wallet
            if not hasattr(self, '_debug_done'):
                print(f"    DEBUG - Réponse API complète: {data}")
                self._debug_done = True

            return is_contract

        except requests.exceptions.RequestException as e:
            print(f"  {address[:10]}... → error: {e}")
            if retry_count < MAX_RETRIES:
                time.sleep(DELAY_BETWEEN_CALLS * (retry_count + 1))
                return self.is_contract_single(address, retry_count + 1)
            return None

    def check_contracts_batch(self, addresses: List[str]) -> Dict[str, bool]:
        """Traite un batch d'adresses avec gestion d'erreurs"""
        results = {}

        for i, address in enumerate(addresses):
            result = self.is_contract_single(address)

            if result is not None:
                results[address] = result
            else:
                # En cas d'erreur, considérer comme contrat (exclusion)
                results[address] = True
                print(f"  {address[:10]}... → EXCLUDED (error)")

            # Délai entre appels sauf pour le dernier
            if i < len(addresses) - 1:
                time.sleep(DELAY_BETWEEN_CALLS)

        return results

def filter_only_eoa_wallets(input_path=INPUT_FILE, output_path=OUTPUT_FILE):
    """Pipeline principal optimisé avec cache et batch processing"""

    print("🚀 === SMART CONTRACT REMOVER OPTIMISÉ ===")
    print(f"📂 Input: {input_path}")
    print(f"📋 Output: {output_path}")
    print(f"⚙️ Config: Batch={BATCH_SIZE}, Delay={DELAY_BETWEEN_CALLS}s")
    print("=" * 60)

    # Charger les données
    df = pd.read_csv(input_path)
    addresses = df["wallet"].unique().tolist()

    print(f"📊 {len(addresses)} adresses uniques à vérifier")

    # Initialiser le checker
    checker = ContractChecker()

    # Traiter toutes les adresses (plus de cache)
    all_results = {}
    total_batches = (len(addresses) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, len(addresses))
        batch_addresses = addresses[start_idx:end_idx]

        print(f"\n📦 Batch {batch_num + 1}/{total_batches} - {len(batch_addresses)} adresses")
        print("-" * 40)

        batch_results = checker.check_contracts_batch(batch_addresses)
        all_results.update(batch_results)

        # Pause entre batches (sauf le dernier)
        if batch_num < total_batches - 1:
            print(f"⏳ Pause {DELAY_BETWEEN_BATCHES}s avant batch suivant...")
            time.sleep(DELAY_BETWEEN_BATCHES)

    # Filtrer les EOA seulement
    eoa_addresses = [addr for addr, is_contract in all_results.items() if not is_contract]
    df_filtered = df[df["wallet"].isin(eoa_addresses)]

    print(f"\n🔍 DEBUG:")
    print(f"   Total results: {len(all_results)}")
    print(f"   EOA addresses found: {len(eoa_addresses)}")
    print(f"   DataFrame filtered rows: {len(df_filtered)}")
    if len(eoa_addresses) > 0:
        print(f"   First 3 EOA: {eoa_addresses[:3]}")

    # Sauvegarder résultats
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if len(df_filtered) == 0:
        print("⚠️ ATTENTION: Aucun EOA trouvé, sauvegarde d'un fichier vide")
        # Créer un DataFrame vide avec les bonnes colonnes
        empty_df = pd.DataFrame(columns=df.columns)
        empty_df.to_csv(output_path, index=False)
    else:
        df_filtered.to_csv(output_path, index=False)
        print(f"✅ {len(df_filtered)} lignes sauvegardées")

    # Statistiques finales
    total_addresses = len(addresses)
    contracts_count = sum(all_results.values())
    eoa_count = len(eoa_addresses)
    errors_count = total_addresses - len(all_results)

    print(f"\n" + "=" * 60)
    print(f"📊 RÉSULTATS:")
    print(f"   📂 Total adresses: {total_addresses}")
    print(f"   🏗️ Contrats détectés: {contracts_count} ({contracts_count/total_addresses*100:.1f}%)")
    print(f"   👤 EOA gardés: {eoa_count} ({eoa_count/total_addresses*100:.1f}%)")
    print(f"   ❌ Erreurs: {errors_count}")
    print(f"✅ Export terminé: {output_path}")

# === Exécution directe
if __name__ == "__main__":
    filter_only_eoa_wallets()
