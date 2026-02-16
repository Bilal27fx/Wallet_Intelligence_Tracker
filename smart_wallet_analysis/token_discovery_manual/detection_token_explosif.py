#!/usr/bin/env python3
"""Détection des tokens explosifs via GeckoTerminal trending/new pools."""

import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from smart_wallet_analysis.config import GECKO_TOP_PERFORMERS, DB_PATH
from smart_wallet_analysis.logger import get_logger

logger = get_logger("token_discovery.detection_explosif")
_CFG = GECKO_TOP_PERFORMERS


def _request(url):
    """Requête HTTP avec rate limiting et retry sur 429."""
    time.sleep(_CFG["RATE_LIMIT_DELAY_SECONDS"])
    try:
        r = requests.get(url, timeout=_CFG["REQUEST_TIMEOUT_SECONDS"])
        if r.status_code == 429:
            logger.warning("Rate limit atteint, pause %ss", _CFG["RETRY_WAIT_SECONDS"])
            time.sleep(_CFG["RETRY_WAIT_SECONDS"])
            return _request(url)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        logger.error("Erreur requête %s: %s", url, e)
        return None


def _fetch_pools(network):
    """Récupère les trending et new pools d'un réseau."""
    pools = []
    for endpoint in ("trending_pools", "new_pools"):
        data = _request(f"{_CFG['BASE_URL']}/networks/{network}/{endpoint}")
        if data:
            batch = data.get("data", [])
            pools.extend(batch)
            logger.info("%s %s: %s pools", network.upper(), endpoint, len(batch))
    return pools


def _pool_age_hours(pool_created_at):
    """Calcule l'âge du pool en heures."""
    if not pool_created_at:
        return 0
    try:
        created = datetime.fromisoformat(pool_created_at.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - created).total_seconds() / 3600
    except Exception:
        return 0


def _passes_filters(attrs, token_address, seen, min_change, min_age, max_age):
    """Vérifie si un pool passe tous les filtres."""
    if not token_address or token_address in seen:
        return False

    price_usd = float(attrs.get("base_token_price_usd", 0) or 0)
    price_change_24h = float(attrs.get("price_change_percentage", {}).get("h24", 0) or 0)
    volume_24h = float(attrs.get("volume_usd", {}).get("h24", 0) or 0)
    liquidity = float(attrs.get("reserve_in_usd", 0) or 0)
    fdv = float(attrs.get("fdv_usd", 0) or 0)

    txns = attrs.get("transactions", {}).get("h24", {})
    buys = int(txns.get("buys", 0) or 0)
    sells = int(txns.get("sells", 0) or 0)
    total = buys + sells
    buys_ratio = buys / total if total > 0 else 0

    age = _pool_age_hours(attrs.get("pool_created_at", ""))
    max_age_hours = _CFG["MAX_POOL_AGE_DAYS"] * 24
    age_ok = age >= min_age and age <= max_age_hours

    return all([
        price_usd > 0,
        price_change_24h >= min_change,
        volume_24h >= _CFG["MIN_VOLUME_24H"],
        liquidity >= _CFG["MIN_LIQUIDITY"],
        fdv >= _CFG["MIN_MARKET_CAP"],
        fdv <= _CFG["MAX_FDV"],
        total >= _CFG["MIN_TXNS_24H"],
        buys_ratio >= _CFG["MIN_BUYS_RATIO"],
        age_ok,
    ])


def _build_token(pool, network):
    """Construit le dict token depuis un pool GeckoTerminal."""
    attrs = pool.get("attributes", {})
    base_token_id = pool.get("relationships", {}).get("base_token", {}).get("data", {}).get("id", "")
    address = base_token_id.replace(f"{network}_", "")
    name = attrs.get("name", "UNKNOWN")
    symbol = name.split("/")[0].strip() if "/" in name else "UNKNOWN"
    pool_address = attrs.get("address", "")
    age = _pool_age_hours(attrs.get("pool_created_at", ""))
    volumes = attrs.get("volume_usd", {})
    liquidity = float(attrs.get("reserve_in_usd", 0) or 0)
    volume_24h = float(volumes.get("h24", 0) or 0)
    txns = attrs.get("transactions", {}).get("h24", {})
    buys = int(txns.get("buys", 0) or 0)
    sells = int(txns.get("sells", 0) or 0)

    return {
        "address": address,
        "symbol": symbol,
        "network": network,
        "pool_address": pool_address,
        "dex": pool.get("relationships", {}).get("dex", {}).get("data", {}).get("id", ""),
        "price_usd": float(attrs.get("base_token_price_usd", 0) or 0),
        "price_change_1h": float(attrs.get("price_change_percentage", {}).get("h1", 0) or 0),
        "price_change_6h": float(attrs.get("price_change_percentage", {}).get("h6", 0) or 0),
        "price_change_24h": float(attrs.get("price_change_percentage", {}).get("h24", 0) or 0),
        "volume_24h": volume_24h,
        "liquidity_usd": liquidity,
        "volume_to_liquidity_ratio": volume_24h / liquidity if liquidity > 0 else 0,
        "fdv": float(attrs.get("fdv_usd", 0) or 0),
        "txns_24h_buys": buys,
        "txns_24h_sells": sells,
        "txns_24h_total": buys + sells,
        "buys_ratio": buys / (buys + sells) if (buys + sells) > 0 else 0,
        "pool_age_hours": round(age, 1),
        "pool_created_at": attrs.get("pool_created_at", ""),
        "detected_at": datetime.now().isoformat(),
        "url": f"https://www.geckoterminal.com/{network}/pools/{pool_address}",
    }


def get_top_performers(network, min_change=None, min_age=None, max_age=None):
    """Récupère les tokens explosifs filtrés pour un réseau donné."""
    min_change = min_change if min_change is not None else _CFG["MIN_PRICE_CHANGE_24H"]
    min_age = min_age if min_age is not None else _CFG["MIN_AGE_HOURS"]

    logger.info("Recherche top performers %s (age: %sh+, perf min: +%s%%)", network.upper(), min_age, min_change)

    pools = _fetch_pools(network)
    if not pools:
        logger.warning("Aucun pool récupéré pour %s", network)
        return []

    seen = set()
    results = []

    for pool in pools:
        attrs = pool.get("attributes", {})
        base_token_id = pool.get("relationships", {}).get("base_token", {}).get("data", {}).get("id", "")
        address = base_token_id.replace(f"{network}_", "")

        if not _passes_filters(attrs, address, seen, min_change, min_age, max_age):
            continue

        seen.add(address)
        results.append(_build_token(pool, network))

        if len(results) >= _CFG["LIMIT"]:
            break

    results.sort(key=lambda x: x["price_change_24h"], reverse=True)
    logger.info("%s tokens explosifs trouvés sur %s", len(results), network.upper())
    return results


def save_to_db(tokens):
    """Insère les tokens détectés dans explosive_tokens_detected."""
    if not tokens:
        return
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany(
            """INSERT OR IGNORE INTO explosive_tokens_detected
               (symbol, token_address, chain, pool_address, pool_age_hours,
                price_change_24h, volume_24h, liquidity_usd, fdv, buys_ratio, detected_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    t["symbol"], t["address"], t["network"], t["pool_address"],
                    t["pool_age_hours"], t["price_change_24h"], t["volume_24h"],
                    t["liquidity_usd"], t["fdv"], t["buys_ratio"], t["detected_at"],
                )
                for t in tokens
            ],
        )
    logger.info("%s tokens sauvegardés en DB", len(tokens))


def run_detection(timeframe="24h"):
    """Lance la détection sur Base et BSC pour la fenêtre donnée."""
    if timeframe in ("7d", "7j"):
        min_age, max_age, min_change = 24, 168, 10
        suffix = "7d"
    else:
        min_age, max_age, min_change = _CFG["MIN_AGE_HOURS"], None, _CFG["MIN_PRICE_CHANGE_24H"]
        suffix = "24h"

    all_tokens = []
    for network in _CFG["NETWORKS"]:
        tokens = get_top_performers(network, min_change=min_change, min_age=min_age, max_age=max_age)
        all_tokens.extend(tokens)

    all_tokens.sort(key=lambda x: x["price_change_24h"], reverse=True)
    save_to_db(all_tokens)
    logger.info("Total: %s tokens explosifs détectés (%s)", len(all_tokens), timeframe)
    return all_tokens


if __name__ == "__main__":
    import sys
    timeframe = sys.argv[1] if len(sys.argv) > 1 else "24h"
    tokens = run_detection(timeframe)
    for i, t in enumerate(tokens[:10], 1):
        age = f"{t['pool_age_hours']:.1f}h" if t["pool_age_hours"] < 48 else f"{t['pool_age_hours']/24:.1f}j"
        logger.info(
            "[%s] %s (%s) | +%.1f%% 24h | vol $%s | age %s",
            i, t["symbol"], t["network"].upper(),
            t["price_change_24h"], f"{t['volume_24h']:,.0f}", age
        )
