#!/usr/bin/env python3
"""Détecte la période d'explosion optimale pour chaque token explosif."""

import sqlite3
from datetime import datetime, timezone

from smart_wallet_analysis.config import GECKO_TOP_PERFORMERS, DB_PATH
from smart_wallet_analysis.logger import get_logger

logger = get_logger("token_discovery.explosion_detector")
_CFG = GECKO_TOP_PERFORMERS


def _get_tokens_with_history():
    """Récupère les tokens qui ont un historique de prix."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT DISTINCT e.token_address, e.chain, e.symbol, e.pool_age_hours
            FROM explosive_tokens_detected e
            INNER JOIN token_explosif_history_prices h
                ON e.token_address = h.token_address AND e.chain = h.chain
        """).fetchall()
    return [{"token_address": r[0], "chain": r[1], "symbol": r[2], "pool_age_hours": r[3]} for r in rows]


def _get_price_history(token_address, chain):
    """Récupère l'historique de prix trié par date."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT date, close FROM token_explosif_history_prices "
            "WHERE token_address = ? AND chain = ? ORDER BY date ASC",
            (token_address, chain),
        ).fetchall()
    return rows  # [(date_str, close), ...]


def _parse_dt(dt_str):
    """Parse YYYY-MM-DD HH:MM:SS ou YYYY-MM-DD en datetime UTC."""
    fmt = "%Y-%m-%d %H:%M:%S" if len(dt_str) > 10 else "%Y-%m-%d"
    return datetime.strptime(dt_str, fmt).replace(tzinfo=timezone.utc)


def _hours_between(dt_str_1, dt_str_2):
    """Calcule le nombre d'heures entre deux datetimes."""
    return (_parse_dt(dt_str_2) - _parse_dt(dt_str_1)).total_seconds() / 3600


def _hours_since_now(dt_str):
    """Calcule le nombre d'heures écoulées depuis une datetime."""
    return (datetime.now(timezone.utc) - _parse_dt(dt_str)).total_seconds() / 3600


def _to_utc_str(dt_str):
    """Normalise une datetime en YYYY-MM-DD HH:MM:SS UTC."""
    d = _parse_dt(dt_str)
    return d.strftime("%Y-%m-%d %H:%M:%S") + " UTC"


def detect_explosion(prices, pool_age_hours):
    """
    Trouve la période d'explosion optimale.
    Score = hours_gap * explosion_pct (favorise le délai + la performance).
    """
    min_hours = _CFG["MIN_HOURS_BEFORE_EXPLOSION"]
    min_pct = _CFG["MIN_EXPLOSION_PCT"]

    if not prices or len(prices) < 2:
        return None

    creation_date = prices[0][0]
    best = None

    for i, (start_date, start_close) in enumerate(prices):
        hours_gap = _hours_between(creation_date, start_date)
        if hours_gap < min_hours or start_close <= 0:
            continue

        # Cherche le pic après ce point d'entrée
        peak_close = start_close
        peak_date = start_date
        for peak_d, peak_c in prices[i + 1:]:
            if peak_c > peak_close:
                peak_close = peak_c
                peak_date = peak_d

        explosion_pct = ((peak_close - start_close) / start_close) * 100
        if explosion_pct < min_pct:
            continue

        score = hours_gap * explosion_pct
        if best is None or score > best["score"]:
            best = {
                "explosion_start_date": _to_utc_str(start_date),
                "explosion_peak_date": _to_utc_str(peak_date),
                "explosion_pct": round(explosion_pct, 2),
                "hours_gap": round(hours_gap, 1),
                "hours_since_now": round(_hours_since_now(start_date), 1),
                "score": round(score, 2),
            }

    return best


def _save_explosion(token_address, chain, result):
    """Met à jour explosive_tokens_detected avec les données d'explosion."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """UPDATE explosive_tokens_detected
               SET explosion_start_date = ?, explosion_peak_date = ?,
                   explosion_pct = ?, hours_gap = ?, score = ?, hours_since_now = ?
               WHERE token_address = ? AND chain = ?""",
            (
                result["explosion_start_date"], result["explosion_peak_date"],
                result["explosion_pct"], result["hours_gap"], result["score"],
                result["hours_since_now"], token_address, chain,
            ),
        )


def run_explosion_detection():
    """Détecte l'explosion optimale pour tous les tokens avec historique."""
    tokens = _get_tokens_with_history()
    if not tokens:
        logger.warning("Aucun token avec historique de prix")
        return

    logger.info("Détection explosion pour %s tokens", len(tokens))
    detected = 0

    for token in tokens:
        address = token["token_address"]
        chain = token["chain"]
        symbol = token["symbol"]
        prices = _get_price_history(address, chain)

        result = detect_explosion(prices, token["pool_age_hours"])
        if result:
            _save_explosion(address, chain, result)
            logger.info(
                "%s (%s): +%.0f%% | gap creation→explosion: %.0fh | il y a %.0fh | %s → %s",
                symbol, chain.upper(), result["explosion_pct"], result["hours_gap"],
                result["hours_since_now"],
                result["explosion_start_date"], result["explosion_peak_date"],
            )
            detected += 1
        else:
            logger.info("%s (%s): aucune explosion ≥%s%% détectée", symbol, chain.upper(), _CFG["MIN_EXPLOSION_PCT"])

    logger.info("%s/%s tokens avec explosion détectée", detected, len(tokens))


if __name__ == "__main__":
    run_explosion_detection()
