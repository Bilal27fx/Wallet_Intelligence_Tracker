#!/usr/bin/env python3
"""
TOKEN ENRICHMENT - ÉTAPE 1 OPTIMISÉE
Fusionne 3 étapes en 1 :
  1. Récupération des top tokens (CoinGecko)
  2. Enrichissement avec contrats (CMC + CoinGecko)
  3. Vérification EVM compatible
"""

import os
import requests
import pandas as pd
import time
from pathlib import Path
from dotenv import load_dotenv
import sys

# Ajouter le DAO
sys.path.insert(0, str(Path(__file__).parent))
from tokens_discovered_dao import TokensDiscoveredDAO

# === Configuration ===
load_dotenv()
CMC_API_KEY = os.getenv("CG_API_KEY")
CMC_HEADERS = {"X-CMC_PRO_API_KEY": CMC_API_KEY}

# Plateformes EVM autorisées
EVM_PLATFORMS = {
    "ethereum", "binance smart chain", "base",
    "arbitrum one", "polygon pos", "optimistic ethereum",
    "avalanche", "fantom", "moonriver", "cronos",
    "bnb", "linea", "scroll", "zksync", "mantle", "blast"
}


# === Fonctions utilitaires ===

def safe_request(url, params=None, headers=None, retries=3, delay=15):
    """Requête HTTP avec retry et gestion du rate limiting"""
    for attempt in range(retries):
        try:
            res = requests.get(url, params=params, headers=headers, timeout=10)
            if res.status_code == 200:
                return res
            elif res.status_code == 429:
                print(f"⚠️ Rate limit → attente {delay}s (tentative {attempt+1}/{retries})")
                time.sleep(delay)
            else:
                print(f"⚠️ Status {res.status_code} → {url}")
                break
        except Exception as e:
            print(f"❌ Exception → {e}")
            time.sleep(delay)
    return None


def is_evm_compatible(platform: str, contract_address: str) -> bool:
    """Vérifie si un token est EVM compatible"""
    if not platform or not contract_address:
        return False
    platform_clean = platform.strip().lower()
    return (
        platform_clean in EVM_PLATFORMS
        and contract_address.startswith("0x")
    )


# === 1. Récupération des top tokens CoinGecko ===

def get_top_tokens_coingecko(period="1y", top_n=8, max_tokens=1000):
    """Récupère les top tokens performants depuis CoinGecko"""

    assert period in ["1h", "24h", "7d", "14d", "30d", "200d", "1y"], "Période invalide"

    all_data = []
    pages_needed = (max_tokens // 250) + (1 if max_tokens % 250 > 0 else 0)

    print(f"[CoinGecko] Récupération top {top_n} tokens sur {period} ({pages_needed} pages)")

    for page in range(1, pages_needed + 1):
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": page,
            "price_change_percentage": period
        }

        res = safe_request(url, params, retries=2, delay=20)
        if res and res.status_code == 200:
            all_data.extend(res.json())
            print(f"   ✅ Page {page}/{pages_needed}")
        else:
            print(f"   ❌ Échec page {page}")

        # Délai important entre pages pour éviter rate limit
        if page < pages_needed:
            time.sleep(3)

    if not all_data:
        print(f"❌ Aucune donnée pour {period}")
        return []

    df = pd.DataFrame(all_data[:max_tokens])
    change_col = f"price_change_percentage_{period}_in_currency"

    if change_col not in df.columns:
        print(f"❌ Colonne manquante : {change_col}")
        return []

    # Filtrage volume > 1M USD
    df = df[df["total_volume"] > 1000000]
    df = df.dropna(subset=[change_col])
    df = df.sort_values(by=change_col, ascending=False)

    top_tokens = df.head(top_n).to_dict('records')
    print(f"✅ {len(top_tokens)} tokens récupérés")

    return top_tokens


# === 2. Enrichissement avec contrats (CMC + CoinGecko) ===

def get_cmc_contract(symbol):
    """Récupère le contrat principal depuis CoinMarketCap"""
    try:
        # 1. Récupérer l'ID CMC
        url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
        params = {"symbol": symbol.upper()}
        res = safe_request(url, params=params, headers=CMC_HEADERS, retries=2, delay=5)

        if not res or res.status_code != 200:
            return None, None

        data = res.json()
        if not data.get("data"):
            return None, None

        cmc_id = data["data"][0]["id"]

        # 2. Récupérer le contrat
        url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/info"
        params = {"id": cmc_id}
        res = safe_request(url, params=params, headers=CMC_HEADERS, retries=2, delay=5)

        if not res or res.status_code != 200:
            return cmc_id, None

        data = res.json()
        platform = data.get("data", {}).get(str(cmc_id), {}).get("platform", {})

        if platform and platform.get("token_address"):
            return cmc_id, {
                "platform": platform.get("name", "").strip().lower(),
                "contract": platform.get("token_address")
            }

    except Exception as e:
        print(f"❌ CMC error pour {symbol}: {e}")

    return None, None


def get_coingecko_contracts(gecko_id):
    """Récupère tous les contrats depuis CoinGecko"""
    url = f"https://api.coingecko.com/api/v3/coins/{gecko_id.lower()}"
    res = safe_request(url, retries=2, delay=20)

    if not res or res.status_code != 200:
        return []

    platforms = res.json().get("platforms", {})
    contracts = []

    for key, val in platforms.items():
        if val and val.startswith("0x"):
            contracts.append({
                "platform": key.strip().lower(),
                "contract": val
            })

    return contracts


def enrich_token_with_contract(token):
    """
    Enrichit un token avec son contrat et vérifie s'il est EVM compatible

    Returns:
        dict avec contract_address, platform, cmc_id, is_evm_compatible
    """
    symbol = token.get("symbol")
    gecko_id = token.get("id")

    print(f"[Enrichissement] {symbol}...", end=" ")

    # 1. Essayer CMC d'abord (plus fiable)
    cmc_id, cmc_contract = get_cmc_contract(symbol)

    contracts = []
    if cmc_contract:
        contracts.append(cmc_contract)

    # 2. Compléter avec CoinGecko
    contracts.extend(get_coingecko_contracts(gecko_id))

    # Dédupliquer
    seen = set()
    unique_contracts = []
    for c in contracts:
        key = (c["platform"], c["contract"])
        if key not in seen:
            seen.add(key)
            unique_contracts.append(c)

    if not unique_contracts:
        print("❌ Aucun contrat")
        return {
            "contract_address": None,
            "platform": None,
            "cmc_id": cmc_id,
            "is_evm_compatible": False
        }

    # 3. Prendre le premier contrat (priorité CMC)
    first = unique_contracts[0]
    is_evm = is_evm_compatible(first["platform"], first["contract"])

    status = "✅ EVM" if is_evm else "⚠️ Non-EVM"
    print(f"{status} ({first['platform'][:15]})")

    return {
        "contract_address": first["contract"],
        "platform": first["platform"],
        "cmc_id": cmc_id,
        "is_evm_compatible": is_evm
    }


# === 3. Sauvegarde optimisée en BDD ===

def save_enriched_token(token, period, rank, dao):
    """Sauvegarde un token enrichi en une seule opération"""

    # Vérifier si le token existe déjà
    existing = dao.get_token_by_coingecko_id_and_period(token['id'], period)

    token_data = {
        'token_id': token['id'],
        'symbol': token['symbol'],
        'name': token['name'],
        'current_price_usd': token.get('current_price'),
        'market_cap_usd': token.get('market_cap'),
        'total_volume_usd': token.get('total_volume'),
        f'price_change_{period}': token.get(f"price_change_percentage_{period}_in_currency"),
        'discovery_period': period,
        'discovery_rank': rank,
        'contract_address': token.get('contract_address'),
        'platform': token.get('platform'),
        'cmc_id': token.get('cmc_id'),
        'is_evm_compatible': token.get('is_evm_compatible', False),
        'has_contract': 1 if token.get('contract_address') else 0
    }

    if existing:
        # UPDATE
        return dao.update_token_full(existing['id'], token_data)
    else:
        # INSERT
        return dao.insert_token_full(token_data)


# === Fonction principale ===

def run_token_enrichment(periods=["14d", "30d", "200d", "1y"], top_n=8, max_tokens=500, delay_between=30):
    """
    Pipeline complet optimisé :
      1. Récupère top tokens
      2. Enrichit avec contrats
      3. Vérifie EVM compatible
      4. Sauvegarde tout en une fois
    """

    print("🚀 TOKEN ENRICHMENT - PIPELINE OPTIMISÉ")
    print("=" * 80)

    dao = TokensDiscoveredDAO()
    total_saved = 0

    for period in periods:
        print(f"\n📊 Période : {period}")
        print("-" * 80)

        # Étape 1 : Récupérer les top tokens
        tokens = get_top_tokens_coingecko(period, top_n, max_tokens)

        if not tokens:
            print(f"❌ Aucun token pour {period}\n")
            continue

        # Étapes 2+3 : Enrichir avec contrats + vérifier EVM
        for rank, token in enumerate(tokens, start=1):
            contract_data = enrich_token_with_contract(token)
            token.update(contract_data)

            # Sauvegarder immédiatement
            if save_enriched_token(token, period, rank, dao):
                total_saved += 1

            time.sleep(2)  # Rate limiting plus long

        print(f"✅ {len(tokens)} tokens traités pour {period}")

        # Pause entre périodes
        if period != periods[-1]:
            print(f"⏳ Pause {delay_between}s avant période suivante...")
            time.sleep(delay_between)

    print(f"\n{'=' * 80}")
    print(f"✅ TERMINÉ : {total_saved} tokens enrichis et sauvegardés")
    print(f"{'=' * 80}\n")


if __name__ == "__main__":
    run_token_enrichment(
        periods=["14d", "30d", "200d", "1y"],
        top_n=8,
        max_tokens=3000,
        delay_between=15
    )
