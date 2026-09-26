#!/usr/bin/env python3
"""IO DexScreener pour consensus live."""

import requests
import time
from smart_wallet_analysis.logger import get_logger

logger = get_logger("consensus_live.io")

def get_token_info_dexscreener(contract_address, retries=2):
    """Récupère les infos essentielles d'un token via DexScreener."""
    for attempt in range(retries):
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{contract_address}"
            response = requests.get(url, timeout=15)
            response.raise_for_status()

            data = response.json()
            pairs = data.get("pairs", [])

            if pairs:
                best_pair = max(pairs, key=lambda x: float(x.get("volume", {}).get("h24", 0) or 0))
                return {
                    'price_usd': float(best_pair.get("priceUsd", 0)),
                    'market_cap': float(best_pair.get("marketCap", 0)),
                    'liquidity_usd': float(best_pair.get("liquidity", {}).get("usd", 0)),
                    'volume_24h': float(best_pair.get("volume", {}).get("h24", 0)),
                    'price_change_24h': float(best_pair.get("priceChange", {}).get("h24", 0)),
                    'txns_24h_buys': best_pair.get("txns", {}).get("h24", {}).get("buys", 0),
                    'txns_24h_sells': best_pair.get("txns", {}).get("h24", {}).get("sells", 0),
                    'chain_id': best_pair.get("chainId", "")
                }
            return None

        except Exception as e:
            if attempt == retries - 1:
                logger.warning(f"DexScreener error {contract_address}: {e}")
                return None
            time.sleep(1)

    return None

def get_current_price_dexscreener(contract_address, retries=2):
    """Récupère le prix actuel via DexScreener."""
    token_info = get_token_info_dexscreener(contract_address, retries)
    if token_info:
        return token_info['price_usd'] if token_info['price_usd'] > 0 else None
    return None
