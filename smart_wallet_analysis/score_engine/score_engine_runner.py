#!/usr/bin/env python3
"""Runner principal du Score Engine."""

import argparse
import sys
from datetime import datetime

from smart_wallet_analysis.config import PIPELINES
from smart_wallet_analysis.logger import get_logger
from smart_wallet_analysis.score_engine.fifo_clean_simple import run_fifo_analysis
from smart_wallet_analysis.score_engine.wallet_scorer import run_wallet_scoring

logger = get_logger("score_engine.runner")
_PL = PIPELINES


def run_score_engine_pipeline(quality_filter: float = 0.0, show_stats: bool = True) -> bool:
    """Exécute le pipeline complet : FIFO → scoring wallets."""
    start_time = datetime.now()
    logger.info("DÉMARRAGE DU PIPELINE SCORE ENGINE | date=%s", start_time.strftime("%Y-%m-%d %H:%M:%S"))
    if quality_filter:
        logger.info("Filtre qualité demandé: %.2f (non utilisé dans ce runner)", quality_filter)

    try:
        logger.info("ÉTAPE 1/2 - ANALYSE FIFO")
        run_fifo_analysis()
        logger.info("FIFO Analysis terminée")
    except Exception as e:
        logger.error("Erreur FIFO Analysis: %s", e)
        return False

    try:
        logger.info("ÉTAPE 2/2 - SCORING DES WALLETS")
        success = run_wallet_scoring()
        if not success:
            logger.warning("Scoring wallets n'a produit aucun résultat")
            return False
        logger.info("Scoring terminé")
    except Exception as e:
        logger.error("Erreur Wallet Scoring: %s", e)
        return False

    duration = datetime.now() - start_time
    logger.info("PIPELINE TERMINÉ | durée=%s", duration)
    return True


def main():
    """Point d'entrée principal."""
    parser = argparse.ArgumentParser(description="Runner principal du Score Engine")
    parser.add_argument("--no-stats", action="store_true", help="Désactiver les statistiques")
    args = parser.parse_args()

    success = run_score_engine_pipeline(show_stats=not args.no_stats)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
