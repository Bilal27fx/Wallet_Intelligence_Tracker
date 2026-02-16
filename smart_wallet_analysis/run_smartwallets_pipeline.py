#!/usr/bin/env python3
"""Runner orchestrateur smart wallets."""

import sys
import time
from datetime import datetime
from pathlib import Path

# Permet l'execution directe du fichier:
# python smart_wallet_analysis/run_smartwallets_pipeline.py
if __package__ is None or __package__ == "":
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from smart_wallet_analysis.config import PIPELINES, SMART_WALLETS_PIPELINE
from smart_wallet_analysis.logger import get_logger
from smart_wallet_analysis.tracking_live.run import run_complete_live_tracking
from smart_wallet_analysis.score_engine.fifo_clean_simple import run_smart_wallets_fifo
from smart_wallet_analysis.score_engine.wallet_scoring_system import score_all_wallets, save_qualified_wallets
from smart_wallet_analysis.score_engine.simple_wallet_analyzer import analyze_qualified_wallets
from smart_wallet_analysis.score_engine.optimal_threshold_analyzer import OptimalThresholdAnalyzer
from smart_wallet_analysis.consensus_live.consensus_live_detector import run_live_consensus_detection
from smart_wallet_analysis.Telegram.telegram_bot import send_consensus_to_telegram

_PL = PIPELINES
_SW = SMART_WALLETS_PIPELINE
logger = get_logger("smart_wallets.pipeline")


def _log_section(title, width=80):
    """Affiche un en-tête de section."""
    line = "=" * width
    logger.info("")
    logger.info("%s", line)
    logger.info("%s", title)
    logger.info("%s", line)
    logger.info("")


def _build_telegram_data(consensus_signals):
    """Convertit les signaux consensus au format Telegram."""
    telegram_data = {}
    for signal in consensus_signals:
        symbol = signal["symbol"]
        telegram_data[symbol] = {
            "symbol": symbol,
            "total_investment": signal["total_investment"],
            "contract_address": signal["contract_address"],
            "detection_date": signal["detection_date"],
            "token_info": signal.get("token_info", {}),
            "performance": signal.get("performance", {}),
            "whale_count": signal["whale_count"],
            "signal_type": signal["signal_type"],
        }
    return telegram_data


def run_tracking_and_fifo_pipeline():
    """Pipeline complet: tracking → FIFO → scoring → analyses."""
    _log_section("🎯 PIPELINE COMPLET D'ANALYSE SMART WALLETS")
    logger.info("⏰ Démarrage: %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    start_time = time.time()
    step_times = {}

    _log_section("📡 ÉTAPE 1/6: TRACKING LIVE DES SMART WALLETS")

    step1_start = time.time()
    try:
        run_complete_live_tracking(
            enable_transaction_tracking=True,
            min_usd=_PL["TRACKING_MIN_USD"],
            hours_lookback=_PL["TRACKING_HOURS_LOOKBACK"]
        )
        step_times['tracking'] = time.time() - step1_start
        logger.info(
            "✅ Étape 1 terminée en %.2fs (%.1f min)",
            step_times['tracking'],
            step_times['tracking'] / 60
        )
    except Exception as e:
        logger.error("❌ Erreur lors du tracking live: %s", e)
        return False

    logger.info("⏸️ Pause de %s secondes...", _SW["PAUSE_AFTER_TRACKING_SECONDS"])
    time.sleep(_SW["PAUSE_AFTER_TRACKING_SECONDS"])

    _log_section("🧮 ÉTAPE 2/6: ANALYSE FIFO SMART WALLETS")

    step2_start = time.time()
    try:
        result = run_smart_wallets_fifo()
        step_times['fifo'] = time.time() - step2_start

        if result:
            logger.info(
                "✅ Étape 2 terminée en %.2fs (%.1f min)",
                step_times['fifo'],
                step_times['fifo'] / 60
            )
        else:
            logger.warning("⚠️ Étape 2 terminée avec des avertissements")
    except Exception as e:
        logger.error("❌ Erreur lors de l'analyse FIFO: %s", e)
        return False

    logger.info("⏸️ Pause de %s secondes...", _SW["PAUSE_BETWEEN_STEPS_SECONDS"])
    time.sleep(_SW["PAUSE_BETWEEN_STEPS_SECONDS"])

    _log_section("⭐ ÉTAPE 3/6: SCORING DES WALLETS")

    step3_start = time.time()
    try:
        scored_wallets = score_all_wallets(min_score=_PL["SCORING_MIN_SCORE_DEFAULT"])

        if scored_wallets:
            save_qualified_wallets(scored_wallets)
            step_times['scoring'] = time.time() - step3_start
            logger.info("✅ Étape 3 terminée en %.2fs", step_times['scoring'])
            logger.info("%s wallets qualifiés", len(scored_wallets))
        else:
            logger.warning("⚠️ Aucun wallet qualifié")
            step_times['scoring'] = time.time() - step3_start
    except Exception as e:
        logger.error("❌ Erreur lors du scoring: %s", e)
        return False

    logger.info("⏸️ Pause de %s secondes...", _SW["PAUSE_BETWEEN_STEPS_SECONDS"])
    time.sleep(_SW["PAUSE_BETWEEN_STEPS_SECONDS"])

    _log_section("📊 ÉTAPE 4/6: ANALYSE PAR PALIERS (3K-12K)")

    step4_start = time.time()
    try:
        analyze_qualified_wallets()
        step_times['paliers'] = time.time() - step4_start
        logger.info(
            "✅ Étape 4 terminée en %.2fs (%.1f min)",
            step_times['paliers'],
            step_times['paliers'] / 60
        )
    except Exception as e:
        logger.error("❌ Erreur lors de l'analyse par paliers: %s", e)
        return False

    logger.info("⏸️ Pause de %s secondes...", _SW["PAUSE_BETWEEN_STEPS_SECONDS"])
    time.sleep(_SW["PAUSE_BETWEEN_STEPS_SECONDS"])

    _log_section("🎯 ÉTAPE 5/6: ANALYSE DES SEUILS OPTIMAUX")

    step5_start = time.time()
    try:
        analyzer = OptimalThresholdAnalyzer()
        analyzer.analyze_all_qualified_wallets(quality_filter=_SW["QUALITY_FILTER"])
        step_times['seuils'] = time.time() - step5_start
        logger.info(
            "✅ Étape 5 terminée en %.2fs (%.1f min)",
            step_times['seuils'],
            step_times['seuils'] / 60
        )
    except Exception as e:
        logger.error("❌ Erreur lors de l'analyse des seuils: %s", e)
        return False

    logger.info("⏸️ Pause de %s secondes...", _SW["PAUSE_BETWEEN_STEPS_SECONDS"])
    time.sleep(_SW["PAUSE_BETWEEN_STEPS_SECONDS"])

    _log_section("🔍 ÉTAPE 6/6: DÉTECTION CONSENSUS LIVE")

    step6_start = time.time()
    try:
        signals = run_live_consensus_detection()
        if signals:
            telegram_data = _build_telegram_data(signals)
            sent = send_consensus_to_telegram(telegram_data)
            if sent:
                logger.info("✅ Telegram: %s signal(s) envoyé(s)", len(telegram_data))
            else:
                logger.error("❌ Telegram: échec d'envoi des signaux")
        else:
            logger.info("ℹ️ Aucun nouveau consensus: aucun envoi Telegram")
        step_times['consensus'] = time.time() - step6_start
        logger.info("✅ Étape 6 terminée en %.2fs (%.1f min)", step_times['consensus'], step_times['consensus'] / 60)
    except Exception as e:
        logger.error("❌ Erreur lors du consensus live: %s", e)
        return False

    total_duration = time.time() - start_time

    _log_section("🏆 PIPELINE COMPLET TERMINÉ")
    logger.info("⏱️ Durée totale: %.2fs (%.1f min)", total_duration, total_duration / 60)
    logger.info("📋 Détail des étapes:")
    logger.info("• Étape 1 (Tracking): %.2fs (%.1f min)", step_times['tracking'], step_times['tracking'] / 60)
    logger.info("• Étape 2 (FIFO): %.2fs (%.1f min)", step_times['fifo'], step_times['fifo'] / 60)
    logger.info("• Étape 3 (Scoring): %.2fs", step_times['scoring'])
    logger.info("• Étape 4 (Paliers): %.2fs (%.1f min)", step_times['paliers'], step_times['paliers'] / 60)
    logger.info("• Étape 5 (Seuils): %.2fs (%.1f min)", step_times['seuils'], step_times['seuils'] / 60)
    logger.info("• Étape 6 (Consensus): %.2fs (%.1f min)", step_times['consensus'], step_times['consensus'] / 60)
    logger.info("⏰ Fin: %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    return True


if __name__ == "__main__":
    try:
        success = run_tracking_and_fifo_pipeline()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.warning("⚠️ Pipeline interrompu par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        logger.error("❌ Erreur fatale: %s", e)
        sys.exit(1)
