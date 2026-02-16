#!/usr/bin/env python3
"""Runner principal du Score Engine."""

import argparse
import sys
from datetime import datetime

from smart_wallet_analysis.config import PIPELINES
from smart_wallet_analysis.logger import get_logger
from smart_wallet_analysis.score_engine.fifo_clean_simple import run_fifo_analysis
from smart_wallet_analysis.score_engine.wallet_scoring_system import (
    score_all_wallets,
    display_top_wallets,
    save_qualified_wallets,
    get_qualified_wallets_stats,
)
from smart_wallet_analysis.score_engine.simple_wallet_analyzer import (
    analyze_qualified_wallets,
)
from smart_wallet_analysis.score_engine.optimal_threshold_analyzer import (
    OptimalThresholdAnalyzer,
)

logger = get_logger("score_engine.runner")
_PL = PIPELINES


def run_score_engine_pipeline(
    quality_filter: float = 0.0,
    show_stats: bool = True,
    min_score: int = _PL["SCORING_MIN_SCORE_DEFAULT"],
):
    """Exécute le pipeline complet d'analyse des smart wallets."""

    start_time = datetime.now()
    logger.info(
        "DÉMARRAGE DU PIPELINE SCORE ENGINE | date=%s qualité=%s min_score=%s stats=%s",
        start_time.strftime("%Y-%m-%d %H:%M:%S"),
        quality_filter,
        min_score,
        "on" if show_stats else "off",
    )

    errors = []

    try:
        logger.info("ÉTAPE 1/4 - ANALYSE FIFO")
        run_fifo_analysis()
        logger.info("FIFO Analysis terminée")

    except Exception as e:
        logger.error(f"Erreur FIFO Analysis: {e}")
        errors.append(("FIFO Analysis", str(e)))
        return False

    try:
        logger.info("ÉTAPE 2/4 - SCORING DES WALLETS")
        scored_wallets = score_all_wallets(min_score=min_score)

        if not scored_wallets:
            logger.warning("Aucun wallet qualifié trouvé")
            return False

        display_top_wallets(scored_wallets, top_n=20)
        save_qualified_wallets(scored_wallets)

        if show_stats:
            get_qualified_wallets_stats()

        logger.info(f"Scoring terminé - {len(scored_wallets)} wallets qualifiés")

    except Exception as e:
        logger.error(f"Erreur Wallet Scoring: {e}")
        errors.append(("Wallet Scoring", str(e)))
        return False

    try:
        logger.info("ÉTAPE 3/4 - ANALYSE PAR PALIERS")
        analyze_qualified_wallets()
        logger.info("Analyse par paliers terminée")

    except Exception as e:
        logger.error(f"Erreur Simple Wallet Analyzer: {e}")
        errors.append(("Simple Wallet Analyzer", str(e)))
        return False

    try:
        logger.info(f"ÉTAPE 4/4 - CALCUL SEUILS OPTIMAUX (qualité ≥ {quality_filter})")
        analyzer = OptimalThresholdAnalyzer()
        results = analyzer.analyze_all_qualified_wallets(quality_filter=quality_filter)

        if show_stats:
            analyzer.get_smart_wallets_threshold_stats()

        if quality_filter > 0:
            logger.info(f"{len(results)} wallets exceptionnels (qualité ≥ {quality_filter})")
            for i, result in enumerate(results[:5], 1):
                threshold_str = f"{result['optimal_threshold']}K" if result['optimal_threshold'] else "N/A"
                logger.info(f"  {i}. {result['wallet_address'][:10]}... seuil={threshold_str} qualité={result['quality']:.3f}")
        else:
            logger.info(f"{len(results)} wallets analysés au total")

        logger.info("Calcul des seuils optimaux terminé")

    except Exception as e:
        logger.error(f"Erreur Optimal Threshold: {e}")
        errors.append(("Optimal Threshold", str(e)))
        return False

    duration = datetime.now() - start_time
    logger.info(f"PIPELINE TERMINÉ | durée={duration}")

    if errors:
        logger.warning(f"{len(errors)} erreur(s): {errors}")
        return False

    return True


def main():
    """Point d'entrée principal."""

    parser = argparse.ArgumentParser(
        description='Runner principal du Score Engine',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  python -m smart_wallet_analysis.score_engine.score_engine_runner
  python -m smart_wallet_analysis.score_engine.score_engine_runner --quality 0.9
  python -m smart_wallet_analysis.score_engine.score_engine_runner --min-score 30 --no-stats
        """
    )

    parser.add_argument(
        '--quality',
        type=float,
        default=0.0,
        help='Filtre qualité minimum pour optimal_threshold (0.0-1.0, défaut: 0.0)'
    )

    parser.add_argument(
        '--no-stats',
        action='store_true',
        help='Désactiver l\'affichage des statistiques détaillées'
    )

    parser.add_argument(
        '--min-score',
        type=int,
        default=_PL["SCORING_MIN_SCORE_DEFAULT"],
        help=f"Score minimum wallet (défaut: {_PL['SCORING_MIN_SCORE_DEFAULT']})"
    )

    args = parser.parse_args()

    if args.quality < 0 or args.quality > 1:
        logger.error("--quality doit être entre 0.0 et 1.0")
        sys.exit(1)

    success = run_score_engine_pipeline(
        quality_filter=args.quality,
        show_stats=not args.no_stats,
        min_score=args.min_score,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
