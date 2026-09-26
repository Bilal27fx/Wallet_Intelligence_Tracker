#!/usr/bin/env python3
"""Récupère et stocke l'historique de prix (bougies 4H) des tokens explosifs."""

import sqlite3
import time
from datetime import datetime, timezone

import requests

from smart_wallet_analysis.config import GECKO_TOP_PERFORMERS, DB_PATH
from smart_wallet_analysis.logger import get_logger

logger = get_logger("token_discovery.price_history")
_CFG = GECKO_TOP_PERFORMERS
_HEADERS = {"Accept": "application/json;version=20230302"}


def _request(url):
    """Requête HTTP avec rate limiting et retry sur 429."""
    time.sleep(_CFG["RATE_LIMIT_DELAY_SECONDS"])
    try:
        r = requests.get(url, headers=_HEADERS, timeout=_CFG["REQUEST_TIMEOUT_SECONDS"])
        if r.status_code == 429:
            logger.warning("Rate limit, pause %ss", _CFG["RETRY_WAIT_SECONDS"])
            time.sleep(_CFG["RETRY_WAIT_SECONDS"])
            return _request(url)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        logger.error("Erreur requête %s: %s", url, e)
        return None


def _fetch_ohlcv(network, pool_address):
    """Récupère les bougies 4H pour le pool."""
    url = (
        f"{_CFG['BASE_URL']}/networks/{network}/pools/{pool_address}"
        f"/ohlcv/hour?aggregate={_CFG['OHLCV_AGGREGATE']}&limit={_CFG['OHLCV_LIMIT']}"
    )
    data = _request(url)
    if not data:
        return []
    return data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])


def _ts_to_datetime(ts):
    """Convertit un timestamp unix en datetime YYYY-MM-DD HH:MM:SS."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _save_ohlcv(token_address, chain, pool_address, ohlcv_list):
    """Insère les bougies en DB (INSERT OR IGNORE)."""
    if not ohlcv_list:
        return 0
    rows = [
        (token_address, chain, pool_address, _ts_to_datetime(row[0]), row[4], row[5])
        for row in ohlcv_list
    ]
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany(
            """INSERT OR IGNORE INTO token_explosif_history_prices
               (token_address, chain, pool_address, date, close, volume)
               VALUES (?, ?, ?, ?, ?, ?)""",
            rows,
        )
    return len(rows)


def _get_explosive_tokens():
    """Récupère les tokens depuis explosive_tokens_detected."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT token_address, chain, pool_address, symbol, pool_age_hours FROM explosive_tokens_detected"
        ).fetchall()
    return [{"token_address": r[0], "chain": r[1], "pool_address": r[2], "symbol": r[3], "pool_age_hours": r[4] or 0} for r in rows]


def run_price_history_fetch():
    """Récupère l'historique de prix pour tous les tokens explosifs."""
    tokens = _get_explosive_tokens()
    if not tokens:
        logger.warning("Aucun token dans explosive_tokens_detected")
        return

    logger.info("Récupération historique pour %s tokens", len(tokens))
    total_inserted = 0

    for token in tokens:
        symbol = token["symbol"]
        pool = token["pool_address"]
        network = token["chain"]
        address = token["token_address"]

        if not pool:
            logger.warning("Pas de pool_address pour %s, ignoré", symbol)
            continue

        ohlcv = _fetch_ohlcv(network, pool)
        if not ohlcv:
            logger.warning("%s (%s): aucune donnée OHLCV", symbol, network)
            continue

        inserted = _save_ohlcv(address, network, pool, ohlcv)
        total_inserted += inserted
        logger.info("%s (%s) [4H]: %s bougies insérées", symbol, network.upper(), inserted)

    logger.info("Total: %s bougies insérées en DB", total_inserted)


if __name__ == "__main__":
    run_price_history_fetch()
