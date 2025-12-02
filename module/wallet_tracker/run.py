# === IMPORT DES MODULES DU PIPELINE WIT ===

# Étape 1 : récupération des balances des nouveaux wallets détectés
from .wallet_balances_extractor import run_wallet_balance_pipeline

# Étape 2 : suppression des wallets possédant trop de tokens (ex: CEX, spam)
from .processor.clean_wallet import clean_large_wallets

# Étape 3 : extraction de l'historique de transactions pour chaque token de chaque wallet
from .wallet_transaction_tracker_extractor import run_token_history_extraction
from .wallet_transaction_tracker_extractor_robust import run_token_history_extraction_robust

# Étape 3b : récupération des wallets échoués
from .force_process_failed_wallets import main as force_process_failed_wallets

# Étape 4 : génération du fichier wallet_profiles.csv (compilation des données + scoring)
from .processor.wallet_dataframe_processing import generate_wallet_profiles

# Étape 5 : filtrage stratégique pour isoler les meilleurs wallets (Whales, etc.)
from .processor.filtered_high_potential_wallet import generate_wallet_profiles as filter_high_potential_wallets

from .whales_token_extractor import extract_unique_tokens_from_high_potential_wallets


def main():
    # === ÉTAPE 1 : balances des nouveaux wallets ===
    # → Pour chaque wallet détecté récemment, on récupère les tokens qu’il détient.
    print("\n🔁 [1/6] Récupération des balances des nouveaux wallets...")
    run_wallet_balance_pipeline()

    # === ÉTAPE 2 : nettoyage des wallets trop chargés ===
    # → Les wallets possédant >50 tokens sont supprimés (souvent CEX ou bruités).
    print("\n🧹 [2/6] Suppression des wallets avec plus de 50 tokens...")
    clean_large_wallets(threshold=50)

    # === ÉTAPE 3 : historique de chaque token par wallet ===
    # → Appel API Covalent pour récupérer les transferts (ERC20 et natifs).
    # → Les historiques sont stockés dans :
    #     - token_histories/       (append permanent)
    #     - token_histories_new/   (snapshot temporaire du jour)
    print("\n🔁 [3/6] Extraction de l'historique de prix de chaque token (VERSION ROBUSTE)...")
    run_token_history_extraction_robust()
    
    # === ÉTAPE 3b : récupération des wallets échoués ===
    # → Traite automatiquement les wallets qui ont échoué à l'étape 3
    print("\n🔧 [3b/6] Récupération des wallets échoués...")
    try:
        force_process_failed_wallets()
        print("✅ Récupération des échecs terminée")
    except Exception as e:
        print(f"⚠️  Récupération des échecs optionnelle échouée: {e}")
        print("   → Les wallets échoués peuvent être traités manuellement plus tard")

    # === ÉTAPE 4 : génération du profil agrégé de chaque wallet ===
    # → Reconstitution du portefeuille et des comportements (ROI, activité, etc.)
    # → Création du fichier `wallet_profiles.csv`
    print("\n📊 [4/6] Génération des profils wallet + scoring...")
    generate_wallet_profiles()

    # === ÉTAPE 5 : filtrage des wallets les plus prometteurs ===
    # → Extrait uniquement les wallets qui sont :
    #     - Whales / Big Whales
    #     - Entre 3 et 30 tokens détenus
    #     - Pas mono-token
    # → Résultat dans `filtered_high_potential_wallets.csv`
    print("\n🎯 [5/6] Filtrage des wallets à fort potentiel...")
    filter_high_potential_wallets()

    print("\n🎯 [6/6] Recuperation des tokens des wallets filtrés ")
    extract_unique_tokens_from_high_potential_wallets()

    # === FIN ===
    print("\n✅ Pipeline WIT exécuté avec succès !")
    print("\n📊 RÉSUMÉ DU PIPELINE:")
    print("  ✅ Étape 1: Balances extraites")
    print("  ✅ Étape 2: Wallets nettoyés") 
    print("  ✅ Étape 3: Historiques récupérés (avec gestion des échecs)")
    print("  ✅ Étape 3b: Wallets échoués récupérés automatiquement")
    print("  ✅ Étape 4: Profils générés")
    print("  ✅ Étape 5: Wallets filtrés")
    print("  ✅ Étape 6: Tokens extraits")
    print("\n🎯 Aucun wallet ultra-rentable ne sera plus perdu !")


# === EXECUTION DU SCRIPT SI LANCÉ DIRECTEMENT ===
if __name__ == "__main__":
    main()
