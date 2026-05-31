#!/usr/bin/env python3
"""Runner manuel pour découverte de wallets depuis explosive_tokens_manual.json."""

from smart_wallet_analysis.logger import get_logger
from smart_wallet_analysis.token_discovery_manual.dune_api_loop_manual import run_manual_token_discovery

logger = get_logger("token_discovery.runner_manual")


def main():
    """Lance la découverte de wallets pour les tokens ajoutés manuellement dans explosive_tokens_manual.json."""
    logger.info("=== DÉCOUVERTE MANUELLE DE WALLETS ===")
    logger.info("Source : explosive_tokens_manual.json")
    logger.info("Mode : Dune API → wallet_brute")

    try:
        run_manual_token_discovery()
        logger.info("✅ Découverte manuelle terminée avec succès")
    except Exception as e:
        logger.error(f"❌ Erreur lors de la découverte manuelle : {e}")
        raise


if __name__ == "__main__":
    main()
