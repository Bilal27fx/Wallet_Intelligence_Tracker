#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Pipeline de re-scoring quotidien des smart wallets."""

import sys
import time
import sqlite3
from datetime import datetime

from smart_wallet_analysis.config import DB_PATH, PIPELINES
from smart_wallet_analysis.logger import get_logger
from smart_wallet_analysis.tracking_live.run import run_rescoring_transaction_update
from smart_wallet_analysis.score_engine.fifo_clean_simple import SimpleFIFOAnalyzer
from smart_wallet_analysis.score_engine.wallet_scorer import run_wallet_scoring
from smart_wallet_analysis.wallet_tracker.wallet_token_history_simple import extract_wallet_simple_history

_PL = PIPELINES
logger = get_logger("scoring_pipeline.runner")


def _log_section(title, width=70):
    """Affiche un en-tête de section."""
    line = "=" * width
    logger.info("")
    logger.info("%s", line)
    logger.info("%s", title)
    logger.info("%s", line)
    logger.info("")


def get_wallets_to_rescore():
    """Récupère tous les wallets présents dans transaction_history."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT DISTINCT wallet_address
            FROM transaction_history
            ORDER BY wallet_address
        """)

        wallets = [row[0] for row in cursor.fetchall()]
        conn.close()

        logger.info("📊 %s wallets dans transaction_history", len(wallets))
        return wallets

    except Exception as e:
        logger.error("❌ Erreur récupération wallets: %s", e)
        return []


def update_transaction_histories(wallets_list):
    """Met à jour l'historique transactions pour la liste de wallets."""
    _log_section("📊 ÉTAPE 1: MISE À JOUR DES TRANSACTIONS")

    if not wallets_list:
        logger.warning("⚠️ Aucun wallet à mettre à jour")
        return 0

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        placeholders = ','.join('?' * len(wallets_list))
        cursor.execute(f"""
            SELECT w FROM (
                SELECT DISTINCT wallet_address AS w FROM transaction_history
                WHERE wallet_address IN ({placeholders})
            )
            WHERE w NOT IN (SELECT DISTINCT wallet_address FROM tokens)
        """, wallets_list)
        wallets_without_positions = [row[0] for row in cursor.fetchall()]
        conn.close()
    except Exception as e:
        logger.warning("⚠️ Erreur détection wallets sans positions: %s", e)
        wallets_without_positions = []

    if wallets_without_positions:
        logger.warning(
            "⚠️ %s wallet(s) sans positions dans tokens -> extraction complète",
            len(wallets_without_positions)
        )
        for i, wallet in enumerate(wallets_without_positions, 1):
            logger.info(
                "[%s/%s] 🔄 Extraction complète: %s...",
                i,
                len(wallets_without_positions),
                wallet[:12]
            )
            try:
                extract_wallet_simple_history(wallet, min_value_usd=_PL["RESCORING_MIN_USD"])
            except Exception as e:
                logger.error("❌ Erreur extraction %s: %s", wallet[:12], e)
        logger.info(
            "✅ Extraction complète terminée pour %s wallet(s)",
            len(wallets_without_positions)
        )

    changes_count = run_rescoring_transaction_update(
        wallet_list=wallets_list,
        min_usd=_PL["RESCORING_MIN_USD"],
        hours_lookback=_PL["RESCORING_HOURS_LOOKBACK"]
    )

    logger.info("✅ Mise à jour terminée: %s wallets avec changements", changes_count)
    return changes_count


def run_fifo_analysis_full():
    """Lance une analyse FIFO complète sur tous les wallets."""
    _log_section("📊 ÉTAPE 2: ANALYSE FIFO (TOUS LES WALLETS)")

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(DISTINCT wallet_address) FROM transaction_history")
        total_wallets = cursor.fetchone()[0]
        logger.info("📊 %s wallets à analyser", total_wallets)

        logger.info("🗑️ Suppression de l'ancienne analyse FIFO...")
        cursor.execute("DELETE FROM token_analytics")
        conn.commit()
        conn.close()

        logger.info("🔄 Lancement de l'analyse FIFO...")
        analyzer = SimpleFIFOAnalyzer()
        analyzer.analyze_all_wallets()

        logger.info("✅ Analyse FIFO terminée")
        return True

    except Exception as e:
        logger.error("❌ Erreur FIFO analysis: %s", e)
        return False


def run_wallet_scoring_full():
    """Lance le scoring de tous les wallets."""
    _log_section("📊 ÉTAPE 3: SCORING DES WALLETS")

    try:
        run_wallet_scoring()
        logger.info("✅ Scoring terminé")
        return True

    except Exception as e:
        logger.error("❌ Erreur wallet scoring: %s", e)
        return False


def get_final_stats():
    """Retourne les statistiques finales du pipeline."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(DISTINCT wallet_address) FROM token_analytics")
        analyzed_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM wallet_scoring WHERE score_final IS NOT NULL")
        scored_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM wallet_watchlist WHERE statut = 'pending'")
        watchlist_count = cursor.fetchone()[0]
        conn.close()
        return {
            'analyzed_wallets': analyzed_count,
            'scored_wallets': scored_count,
            'watchlist_wallets': watchlist_count,
        }
    except Exception as e:
        logger.error("❌ Erreur récupération stats: %s", e)
        return {}


def run_complete_scoring_pipeline():
    """Exécute le pipeline complet de re-scoring quotidien."""
    start_time = time.time()

    _log_section("🎯 PIPELINE 2: RE-SCORING QUOTIDIEN", width=80)
    logger.info("⏰ Démarrage: %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    wallets_to_rescore = get_wallets_to_rescore()

    if not wallets_to_rescore:
        logger.error("❌ Aucun wallet à re-scorer")
        return False

    changes = update_transaction_histories(wallets_to_rescore)

    if not run_fifo_analysis_full():
        logger.error("❌ Erreur lors de l'analyse FIFO")
        return False

    if not run_wallet_scoring_full():
        logger.error("❌ Erreur lors du scoring")
        return False

    elapsed = time.time() - start_time
    stats = get_final_stats()

    _log_section("✅ PIPELINE 2 TERMINÉ AVEC SUCCÈS", width=80)
    logger.info("⏱️ Durée totale: %.1f minutes", elapsed / 60)
    logger.info("📊 Wallets analysés: %s", stats.get('analyzed_wallets', 0))
    logger.info("⭐ Wallets scorés: %s", stats.get('scored_wallets', 0))
    logger.info("👀 Watchlist: %s", stats.get('watchlist_wallets', 0))
    logger.info("🔄 Wallets avec changements: %s", changes)
    logger.info("🏁 Fin: %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    return True


if __name__ == "__main__":
    try:
        success = run_complete_scoring_pipeline()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.warning("⚠️ Pipeline interrompu par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        logger.error("💥 Erreur fatale: %s", e)
        sys.exit(1)
