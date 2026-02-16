#!/usr/bin/env python3
"""Pipeline complet de découverte de tokens explosifs."""

from smart_wallet_analysis.logger import get_logger
from smart_wallet_analysis.token_discovery_manual.detection_token_explosif import run_detection
from smart_wallet_analysis.token_discovery_manual.price_history_fetcher import run_price_history_fetch
from smart_wallet_analysis.token_discovery_manual.explosion_detector import run_explosion_detection
from smart_wallet_analysis.token_discovery_manual.dune_api_loop_manual import run_discovery_from_db

logger = get_logger("token_discovery.runner")


def run_token_discovery_pipeline():
    """Détection → Historique prix → Explosion → Dune wallet discovery."""
    logger.info("=== TOKEN DISCOVERY PIPELINE ===")

    logger.info("[1/4] Détection tokens explosifs (GeckoTerminal)")
    run_detection()

    logger.info("[2/4] Récupération historique de prix")
    run_price_history_fetch()

    logger.info("[3/4] Détection période d'explosion")
    run_explosion_detection()

    logger.info("[4/4] Dune wallet discovery")
    run_discovery_from_db()

    logger.info("=== TOKEN DISCOVERY PIPELINE TERMINÉ ===")


if __name__ == "__main__":
    run_token_discovery_pipeline()
