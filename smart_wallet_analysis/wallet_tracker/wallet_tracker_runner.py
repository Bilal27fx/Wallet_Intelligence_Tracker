from .wallet_balances_extractor import run_wallet_balance_pipeline
from .wallet_token_history_simple import process_all_wallets_from_db
from smart_wallet_analysis.config import WALLET_TRACKER
from smart_wallet_analysis.logger import get_logger
from smart_wallet_analysis.token_discovery_manual.wallet_brute_dao import WalletBruteDAO

logger = get_logger("wallet_tracker.runner")

_WT = WALLET_TRACKER


def main():
    """Pipeline du Wallet Tracker : balances → historique → nettoyage wallet_brute."""
    logger.info("WALLET TRACKER - DÉMARRAGE")

    logger.info("[1/3] Récupération des balances depuis wallet_brute")
    try:
        run_wallet_balance_pipeline()
        logger.info("Extraction des balances terminée")
    except Exception as e:
        logger.error(f"Erreur extraction des balances: {e}")
        return False

    logger.info("[2/3] Extraction de l'historique des transactions")
    try:
        process_all_wallets_from_db(
            batch_size=_WT["BATCH_SIZE_DEFAULT"],
            batch_delay=_WT["BATCH_DELAY_SECONDS"]
        )
        logger.info("Extraction de l'historique terminée")
    except Exception as e:
        logger.warning(f"Erreur extraction historique: {e} → on continue")

    logger.info("[3/3] Nettoyage de la table wallet_brute")
    try:
        dao = WalletBruteDAO()
        deleted_count = dao.clear_table()
        logger.info(f"Table wallet_brute vidée ({deleted_count} entrées supprimées)")
    except Exception as e:
        logger.warning(f"Erreur nettoyage wallet_brute: {e}")

    logger.info("WALLET TRACKER TERMINÉ")
    return True


if __name__ == "__main__":
    main()
