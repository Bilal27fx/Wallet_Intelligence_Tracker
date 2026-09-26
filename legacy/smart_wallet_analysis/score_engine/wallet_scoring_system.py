#!/usr/bin/env python3
"""Système de scoring intelligent des wallets."""

import sqlite3
import math

from smart_wallet_analysis.config import DB_PATH, SCORE_ENGINE, PIPELINES, WALLET_BALANCES
from smart_wallet_analysis.logger import get_logger

logger = get_logger("score_engine.scoring")

_WS = SCORE_ENGINE["WALLET_SCORING"]
_EXCLUDED = SCORE_ENGINE["EXCLUDED_TOKENS"]
_EXCLUDED_PLACEHOLDERS = ",".join("?" * len(_EXCLUDED))
_PL = PIPELINES
_WB = WALLET_BALANCES

def calculate_wallet_score(wallet_address):
    """Calcule le score d'un wallet depuis token_analytics."""
    conn = sqlite3.connect(DB_PATH)

    portfolio_query = """
        SELECT total_portfolio_value
        FROM wallets
        WHERE wallet_address = ?
    """
    portfolio_result = conn.execute(portfolio_query, [wallet_address]).fetchone()
    portfolio_value = portfolio_result[0] if portfolio_result else 0

    query = f"""
        SELECT token_symbol, total_invested, roi_percentage
        FROM token_analytics
        WHERE wallet_address = ?
        AND token_symbol NOT IN ({_EXCLUDED_PLACEHOLDERS})
        ORDER BY total_invested DESC
    """

    tokens = conn.execute(query, [wallet_address, *_EXCLUDED]).fetchall()
    conn.close()

    if not tokens:
        return None

    nb_trades = len(tokens)
    total_invested = sum(t[1] for t in tokens)

    if nb_trades < _WS["MIN_TRADES"]:
        return None

    gagnants_significatifs = sum(1 for t in tokens if t[2] > _WS["ROI_WIN_THRESHOLD"])
    if gagnants_significatifs < _WS["MIN_SIGNIFICANT_WINS"]:
        return None

    roi_contributions_positive = []
    for t in tokens:
        if t[2] > 0:
            roi_contributions_positive.append(t[1] * t[2])

    top_n = _WS["ROI_CONCENTRATION_TOP_N"]
    if len(roi_contributions_positive) >= top_n:
        roi_contributions_sorted = sorted(roi_contributions_positive, reverse=True)
        total_positive_contribution = sum(roi_contributions_sorted)

        if total_positive_contribution > 0:
            top_contribution = sum(roi_contributions_sorted[:top_n])
            concentration_ratio = top_contribution / total_positive_contribution
            if concentration_ratio > _WS["ROI_CONCENTRATION_MAX_RATIO"]:
                return None

    roi_values = [t[2] for t in tokens]

    weighted_roi = sum(t[1] * t[2] for t in tokens) / total_invested if total_invested > 0 else 0

    gagnants = sum(1 for t in tokens if t[2] >= _WS["ROI_WIN_THRESHOLD"])
    perdants = sum(1 for t in tokens if t[2] < _WS["ROI_LOSS_THRESHOLD"])
    neutres = nb_trades - gagnants - perdants

    taux_reussite = (gagnants / nb_trades * 100) if nb_trades > 0 else 0

    roi_score = min(100, max(0, (weighted_roi - _WS["ROI_SCORE_BASE"]) / _WS["ROI_SCORE_DIVISOR"]))
    activity_score = min(100, max(0, math.log(nb_trades) / math.log(_WS["ACTIVITY_LOG_MAX_TRADES"]) * 100)) if nb_trades > 0 else 0
    success_score = min(100, max(0, taux_reussite * _WS["SUCCESS_SCORE_MULTIPLIER"]))

    ratio_gagnants = gagnants / nb_trades if nb_trades > 0 else 0
    ratio_perdants = perdants / nb_trades if nb_trades > 0 else 0
    quality_bonus = (ratio_gagnants - ratio_perdants) * _WS["QUALITY_BONUS_MULTIPLIER"]
    quality_bonus = max(0, min(50, quality_bonus))

    weights = _WS["SCORE_WEIGHTS"]
    final_score = (
        roi_score * weights["ROI"] +
        activity_score * weights["ACTIVITY"] +
        success_score * weights["SUCCESS"] +
        quality_bonus * weights["QUALITY"]
    )

    thresholds = _WS["CLASS_THRESHOLDS"]
    if final_score >= thresholds["ELITE"]:
        classification = "ELITE"
    elif final_score >= thresholds["EXCELLENT"]:
        classification = "EXCELLENT"
    elif final_score >= thresholds["BON"]:
        classification = "BON"
    elif final_score >= thresholds["MOYEN"]:
        classification = "MOYEN"
    else:
        classification = "FAIBLE"

    return {
        'wallet_address': wallet_address,
        'final_score': round(final_score, 2),
        'classification': classification,
        'weighted_roi': round(weighted_roi, 2),
        'nb_trades': nb_trades,
        'taux_reussite': round(taux_reussite, 2),
        'total_invested': total_invested,
        'gagnants': gagnants,
        'perdants': perdants,
        'neutres': neutres,
        'roi_score': round(roi_score, 2),
        'activity_score': round(activity_score, 2),
        'success_score': round(success_score, 2),
        'quality_bonus': round(quality_bonus, 2)
    }

def score_all_wallets(min_score=0):
    """Score tous les wallets et les classe par performance."""
    logger.info("SCORING TOUS LES WALLETS")

    conn = sqlite3.connect(DB_PATH)
    query = f"""
        SELECT DISTINCT ta.wallet_address
        FROM token_analytics ta
        JOIN wallets w ON ta.wallet_address = w.wallet_address
        WHERE ta.token_symbol NOT IN ({_EXCLUDED_PLACEHOLDERS})
        AND w.total_portfolio_value >= ?
    """
    wallets = conn.execute(query, list(_EXCLUDED) + [_WB["MIN_WALLET_VALUE_USD"]]).fetchall()
    conn.close()

    logger.info(f"{len(wallets)} wallets candidats")

    scored_wallets = []
    qualified_count = 0

    for wallet in wallets:
        score_data = calculate_wallet_score(wallet[0])
        if score_data and score_data['final_score'] >= min_score:
            scored_wallets.append(score_data)
            qualified_count += 1

    scored_wallets.sort(key=lambda x: x['final_score'], reverse=True)

    logger.info(f"{qualified_count} wallets qualifiés | score minimum: {min_score}")

    return scored_wallets

def display_top_wallets(scored_wallets, top_n=20):
    """Affiche le top N des wallets."""
    logger.info(f"TOP {top_n} WALLETS")
    for i, wallet in enumerate(scored_wallets[:top_n], 1):
        wallet_short = wallet['wallet_address'][:10] + "..." + wallet['wallet_address'][-8:]
        gpn = f"{wallet['gagnants']}/{wallet['perdants']}/{wallet['neutres']}"
        logger.info(f"{i:<4} {wallet_short} score={wallet['final_score']:.1f} "
                    f"class={wallet['classification']} roi={wallet['weighted_roi']:.1f}% "
                    f"trades={wallet['nb_trades']} reussite={wallet['taux_reussite']:.1f}% gpn={gpn}")

def analyze_score_distribution(scored_wallets):
    """Analyse la distribution des scores."""
    if not scored_wallets:
        logger.warning("Aucun wallet à analyser")
        return

    scores = [w['final_score'] for w in scored_wallets]
    rois = [w['weighted_roi'] for w in scored_wallets]
    logger.info(f"Score moyen={sum(scores)/len(scores):.2f} médian={sorted(scores)[len(scores)//2]:.2f} max={max(scores):.2f} | ROI moyen={sum(rois)/len(rois):.2f}%")

    classifications = {}
    for wallet in scored_wallets:
        classif = wallet['classification']
        classifications[classif] = classifications.get(classif, 0) + 1

    for classif, count in sorted(classifications.items(),
                                 key=lambda x: ['ELITE', 'EXCELLENT', 'BON', 'MOYEN', 'FAIBLE'].index(x[0])):
        pct = count / len(scored_wallets) * 100
        logger.info(f"  {classif}: {count} wallets ({pct:.1f}%)")

def save_qualified_wallets(scored_wallets):
    """Sauvegarde les wallets qualifiés dans la table wallet_qualified."""
    if not scored_wallets:
        logger.warning("Aucun wallet à sauvegarder")
        return

    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)

        conn.execute("DELETE FROM wallet_qualified")

        insert_query = """
            INSERT INTO wallet_qualified (
                wallet_address, final_score, classification,
                weighted_roi, nb_trades, taux_reussite, total_invested,
                gagnants, perdants, neutres,
                roi_score, activity_score, success_score, quality_bonus
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        for wallet in scored_wallets:
            data = (
                wallet['wallet_address'],
                wallet['final_score'],
                wallet['classification'],
                wallet['weighted_roi'],
                wallet['nb_trades'],
                wallet['taux_reussite'],
                wallet['total_invested'],
                wallet['gagnants'],
                wallet['perdants'],
                wallet['neutres'],
                wallet['roi_score'],
                wallet['activity_score'],
                wallet['success_score'],
                wallet['quality_bonus']
            )
            conn.execute(insert_query, data)

        conn.commit()
        conn.close()

        logger.info(f"{len(scored_wallets)} wallets qualifiés sauvegardés dans wallet_qualified")

    except sqlite3.OperationalError as e:
        if "database is locked" in str(e):
            logger.warning("Base verrouillée, abandon sauvegarde")
        else:
            logger.error(f"Erreur SQL: {e}")
    except Exception as e:
        logger.error(f"Erreur sauvegarde: {e}")

def get_qualified_wallets_stats():
    """Affiche les statistiques des wallets qualifiés en base."""
    try:
        conn = sqlite3.connect(DB_PATH)

        stats_query = """
            SELECT 
                COUNT(*) as total,
                AVG(final_score) as avg_score,
                AVG(weighted_roi) as avg_roi,
                AVG(nb_trades) as avg_trades,
                MAX(final_score) as max_score,
                MIN(final_score) as min_score
            FROM wallet_qualified
        """
        stats = conn.execute(stats_query).fetchone()

        classif_query = """
            SELECT classification, COUNT(*) as count
            FROM wallet_qualified
            GROUP BY classification
            ORDER BY 
                CASE classification
                    WHEN 'ELITE' THEN 1
                    WHEN 'EXCELLENT' THEN 2
                    WHEN 'BON' THEN 3
                    WHEN 'MOYEN' THEN 4
                    WHEN 'FAIBLE' THEN 5
                END
        """
        classifs = conn.execute(classif_query).fetchall()

        conn.close()

        if stats[0] > 0:
            logger.info(f"STATS wallet_qualified: total={stats[0]} score_moy={stats[1]:.2f} roi_moy={stats[2]:.1f}% trades_moy={stats[3]:.1f} score_max={stats[4]:.2f} score_min={stats[5]:.2f}")
            for classif, count in classifs:
                pct = count / stats[0] * 100
                logger.info(f"  {classif}: {count} wallets ({pct:.1f}%)")
        else:
            logger.warning("Aucun wallet en base")

    except Exception as e:
        logger.error(f"Erreur lecture stats: {e}")

if __name__ == "__main__":
    scored_wallets = score_all_wallets(min_score=_PL["SCORING_MIN_SCORE_DEFAULT"])

    if scored_wallets:
        display_top_wallets(scored_wallets, top_n=20)
        analyze_score_distribution(scored_wallets)

        save_qualified_wallets(scored_wallets)

        get_qualified_wallets_stats()
