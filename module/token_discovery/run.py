# === PIPELINE TOKEN DISCOVERY ===
# Ce script exécute toute la chaîne de détection des tokens performants,
# et l'identification des wallets early + filtrage.

from .top_token_performer import process_periods
from .top_token_performer_contract import extract_and_save_simplified_data
from .evm_contract_extractor import filter_all_evm_contracts
from .dune_api_loop import run_token_discovery
from .discovery_filter import process_new_wallets
from .smart_contrat_remover import filter_only_eoa_wallets
import time

def main():
    # === 1. Détection des tokens les plus performants ===
    # Récupère les top tokens sur différentes périodes (ex: 14d, 30d, etc.)
    print("\n🔁 [1/6] Récupération des tokens les plus performants...")
    process_periods(periods=["14d", "30d", "200d", "1y"], top_n=8, max_tokens=1500, delay_between=15)

    # === 2. Enrichissement des tokens via CoinGecko + CMC ===
    # Permet de récupérer le nom, l'adresse du contrat, la chaîne, etc.
    print("\n🔁 [2/6] Récupération des contrats (CMC + CoinGecko)...")
    extract_and_save_simplified_data()
    time.sleep(5)  # Pause pour respecter les limites API

    # === 3. Filtrage pour ne garder qu’un seul contrat EVM par token ===
    # Priorité donnée à Ethereum si plusieurs contrats sont trouvés.
    print("\n🔁 [3/6] Filtrage des contrats EVM (1 par token, priorité Ethereum)...")
    filter_all_evm_contracts()

    # === 4. Récupération des wallets "early" ===
    # Utilise Dune pour trouver les wallets qui ont acheté avant le pump.
    print("\n🔁 [4/6] Recuperation des wallets early...")
    run_token_discovery()

    # === 5. Filtrage des wallets déjà connus ===
    # Compare avec la base existante et isole les nouveaux wallets détectés.
    print("\n🔁 [5/6] Filtrage des wallets...")
    process_new_wallets()

    # === 6. Suppression des smart contracts ===
    # Garde uniquement les EOA (wallets externes, pas de contrats).
    print("\n🔁 [6/6] Suppression des smarts contracts ...")
    filter_only_eoa_wallets()

    print("\n✅ Pipeline Token Discovery terminée avec succès.")


if __name__ == "__main__":
    main()
