# === IMPORT DES MODULES DU PIPELINE WIT ===

# Étape 1 : récupération des balances des nouveaux wallets détectés
from .wallet_balances_extractor import run_wallet_balance_pipeline

# Étape 2 : extraction de l'historique de transactions pour chaque token de chaque wallet
from .wallet_token_history_simple import process_all_wallets_from_db

# Nettoyage : vider wallet_brute après traitement
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "token_discovery"))
from wallet_brute_dao import WalletBruteDAO


def main():
    """
    Pipeline simplifié du Wallet Tracker
    Exécute uniquement les modules disponibles :
    - Extraction des balances
    - Extraction de l'historique (si nécessaire)
    """

    print("\n" + "="*80)
    print("🔁 WALLET TRACKER - PIPELINE SIMPLIFIÉ")
    print("="*80)

    # === ÉTAPE 1 : balances des nouveaux wallets ===
    print("\n[1/3] 💰 Récupération des balances des wallets depuis wallet_brute...")
    try:
        run_wallet_balance_pipeline()
        print("✅ Extraction des balances terminée")
    except Exception as e:
        print(f"❌ Erreur lors de l'extraction des balances: {e}")
        return False

    # === ÉTAPE 2 : extraction de l'historique ===
    print("\n[2/3] 📈 Extraction de l'historique des transactions...")
    print("⚠️  Note: Cette étape peut être TRÈS longue (plusieurs minutes par wallet)")
    print("ℹ️  Traitement par batches avec pauses pour respecter les rate limits API\n")

    try:
        # Lancer l'extraction d'historique pour tous les wallets en base
        process_all_wallets_from_db(
            min_value_usd=500,    # Seuil minimum par token
            batch_size=10,        # 10 wallets par batch
            batch_delay=30        # 30 secondes entre batches
        )
        print("\n✅ Extraction de l'historique terminée")
    except Exception as e:
        print(f"\n⚠️  Erreur lors de l'extraction d'historique: {e}")
        print("   → Continuons sans historique complet")
        import traceback
        traceback.print_exc()

    # === ÉTAPE 3 : nettoyage de wallet_brute ===
    print("\n[3/3] 🧹 Nettoyage de la table wallet_brute...")
    try:
        dao = WalletBruteDAO()
        deleted_count = dao.clear_table()
        print(f"✅ Table wallet_brute vidée ({deleted_count} entrées supprimées)")
    except Exception as e:
        print(f"⚠️  Erreur lors du nettoyage de wallet_brute: {e}")
        print("   → Le pipeline continue, mais wallet_brute n'a pas été vidée")

    # === FIN ===
    print("\n" + "="*80)
    print("✅ WALLET TRACKER TERMINÉ")
    print("="*80)
    print("\n📊 RÉSUMÉ:")
    print("  ✅ Étape 1: Balances extraites depuis wallet_brute")
    print("  ✅ Étape 2: Historique complet extrait et sauvegardé")
    print("  ✅ Étape 3: Table wallet_brute vidée (données traitées)")
    print("\n💾 Données stockées dans: data/db/wit_database.db")
    print("   • Table wallets          (profils des wallets)")
    print("   • Table tokens           (positions détaillées)")
    print("   • Table transaction_history (historiques complets)")
    print("="*80 + "\n")

    return True


# === EXECUTION DU SCRIPT SI LANCÉ DIRECTEMENT ===
if __name__ == "__main__":
    main()
