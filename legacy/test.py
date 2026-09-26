#!/usr/bin/env python3
"""
Token Discovery via GeckoTerminal API
Récupère les tokens avec les meilleures performances 24h sur Base et BSC
"""
import requests
import time
from datetime import datetime, timezone
from pathlib import Path
import json

# Configuration
BASE_URL = "https://api.geckoterminal.com/api/v2"
RATE_LIMIT_DELAY = 1.5  # secondes entre requêtes

def rate_limited_request(url, timeout=15):
    """Effectue une requête avec rate limiting et gestion d'erreurs"""
    try:
        time.sleep(RATE_LIMIT_DELAY)
        response = requests.get(url, timeout=timeout)

        if response.status_code == 429:
            print("⏳ Rate limit atteint, pause 60s...")
            time.sleep(60)
            return rate_limited_request(url, timeout)

        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur requête {url}: {e}")
        return None

def get_top_performers(
    network="base",
    min_price_change_24h=20,
    min_volume_24h=5000,
    min_liquidity=3000,
    max_fdv=100000000,
    min_txns_24h=50,
    min_buys_ratio=0.15,
    min_age_hours=24,
    max_age_hours=None,
    limit=50
):
    """
    Récupère les tokens les plus performants via trending pools

    Args:
        network: "base", "bsc", "eth"
        min_price_change_24h: Variation min 24h (%)
        min_volume_24h: Volume min 24h ($)
        min_liquidity: Liquidité min ($)
        max_fdv: FDV max ($)
        min_txns_24h: Transactions min 24h
        min_buys_ratio: Ratio achats min
        min_age_hours: Age minimum du pool en heures (défaut: 24h)
        max_age_hours: Age maximum du pool en heures (None = illimité)
        limit: Nombre max de résultats

    Returns:
        list: Tokens performants triés
    """

    age_filter_desc = f"{min_age_hours}h-{max_age_hours}h" if max_age_hours else f"{min_age_hours}h+"
    print(f"🔍 Recherche top performers sur {network.upper()} (âge: {age_filter_desc})...")

    # Combiner trending pools et new pools pour avoir plus de diversité
    all_pools = []

    # 1. Trending pools (meilleurs performers récents)
    url_trending = f"{BASE_URL}/networks/{network}/trending_pools"
    data_trending = rate_limited_request(url_trending)
    if data_trending and 'data' in data_trending:
        all_pools.extend(data_trending.get('data', []))
        print(f"📊 {len(data_trending.get('data', []))} trending pools")

    # 2. New pools (pour capturer les nouveaux performers)
    url_new = f"{BASE_URL}/networks/{network}/new_pools"
    data_new = rate_limited_request(url_new)
    if data_new and 'data' in data_new:
        all_pools.extend(data_new.get('data', []))
        print(f"🆕 {len(data_new.get('data', []))} new pools")

    if not all_pools:
        print(f"❌ Pas de données pour {network}")
        return []

    print(f"📊 {len(all_pools)} pools totaux à analyser")

    top_tokens = []
    seen_tokens = set()

    for pool in all_pools:
        attrs = pool.get('attributes', {})

        # Token de base
        base_token_rel = pool.get('relationships', {}).get('base_token', {}).get('data', {})
        token_address = base_token_rel.get('id', '').replace(f"{network}_", "")

        if not token_address or token_address in seen_tokens:
            continue

        # Prix et variations
        price_usd = float(attrs.get('base_token_price_usd', 0) or 0)
        price_changes = attrs.get('price_change_percentage', {})
        price_change_24h = float(price_changes.get('h24', 0) or 0)
        price_change_6h = float(price_changes.get('h6', 0) or 0)
        price_change_1h = float(price_changes.get('h1', 0) or 0)

        # Volume et liquidité
        volumes = attrs.get('volume_usd', {})
        volume_24h = float(volumes.get('h24', 0) or 0)
        liquidity_usd = float(attrs.get('reserve_in_usd', 0) or 0)

        # Market data
        fdv = float(attrs.get('fdv_usd', 0) or 0)
        market_cap = float(attrs.get('market_cap_usd', 0) or 0)

        # Transactions
        txns = attrs.get('transactions', {}).get('h24', {})
        buys = int(txns.get('buys', 0) or 0)
        sells = int(txns.get('sells', 0) or 0)
        total_txns = buys + sells
        buys_ratio = buys / total_txns if total_txns > 0 else 0

        # Nom du pool
        pool_name = attrs.get('name', 'UNKNOWN')
        pool_created = attrs.get('pool_created_at', '')

        # Calculer l'âge du pool
        pool_age_hours = 0
        if pool_created:
            try:
                # Format ISO avec Z -> timezone UTC
                pool_created_dt = datetime.fromisoformat(pool_created.replace('Z', '+00:00'))
                now_utc = datetime.now(timezone.utc)
                pool_age_hours = (now_utc - pool_created_dt).total_seconds() / 3600
            except Exception as e:
                # Si parsing échoue, considérer comme trop récent (0h)
                pool_age_hours = 0

        # DEX info
        dex_rel = pool.get('relationships', {}).get('dex', {}).get('data', {})
        dex_id = dex_rel.get('id', 'unknown')

        # Appliquer les filtres
        age_filter_pass = pool_age_hours >= min_age_hours
        if max_age_hours:
            age_filter_pass = age_filter_pass and pool_age_hours <= max_age_hours

        if not all([
            price_change_24h >= min_price_change_24h,
            volume_24h >= min_volume_24h,
            liquidity_usd >= min_liquidity,
            (fdv <= max_fdv if fdv > 0 else True),
            total_txns >= min_txns_24h,
            buys_ratio >= min_buys_ratio,
            price_usd > 0,
            age_filter_pass  # Filtre d'âge minimum et maximum
        ]):
            continue

        seen_tokens.add(token_address)

        top_tokens.append({
            'address': token_address,
            'symbol': pool_name.split('/')[0].strip() if '/' in pool_name else 'UNKNOWN',
            'network': network,
            'pool_name': pool_name,
            'pool_address': attrs.get('address', ''),
            'dex': dex_id,

            # Prix et performance
            'price_usd': price_usd,
            'price_change_1h': price_change_1h,
            'price_change_6h': price_change_6h,
            'price_change_24h': price_change_24h,

            # Volume et liquidité
            'volume_24h': volume_24h,
            'liquidity_usd': liquidity_usd,
            'volume_to_liquidity_ratio': volume_24h / liquidity_usd if liquidity_usd > 0 else 0,

            # Market data
            'fdv': fdv,
            'market_cap': market_cap,

            # Transactions
            'txns_24h_total': total_txns,
            'txns_24h_buys': buys,
            'txns_24h_sells': sells,
            'buys_ratio': buys_ratio,

            # Métadonnées
            'pool_created_at': pool_created,
            'pool_age_hours': round(pool_age_hours, 1),
            'detected_at': datetime.now().isoformat(),
            'url': f"https://www.geckoterminal.com/{network}/pools/{attrs.get('address', '')}"
        })

        if len(top_tokens) >= limit:
            break

    # Trier par performance 24h décroissante
    top_tokens.sort(key=lambda x: x['price_change_24h'], reverse=True)

    print(f"✅ {len(top_tokens)} tokens filtrés")
    return top_tokens

def display_results(tokens, title="TOP PERFORMERS", limit=15):
    """Affiche les résultats formatés"""
    if not tokens:
        print("❌ Aucun token trouvé")
        return

    print(f"\n{'='*110}")
    print(f"🚀 {title} - TOP {min(limit, len(tokens))}")
    print(f"{'='*110}\n")

    for i, token in enumerate(tokens[:limit], 1):
        # Indicateur de momentum
        momentum = "🔥" if token['price_change_1h'] > 20 else "📈" if token['price_change_1h'] > 0 else "📉"

        # Indicateur de sentiment
        if token['buys_ratio'] >= 0.60:
            sentiment = "🟢 BULLISH"
        elif token['buys_ratio'] >= 0.40:
            sentiment = "🟡 NEUTRE"
        else:
            sentiment = "🔴 BEARISH"

        # Calcul des jours/heures
        age_hours = token['pool_age_hours']
        age_display = f"{age_hours:.1f}h" if age_hours < 48 else f"{age_hours/24:.1f}j"

        print(f"[{i}] {momentum} {token['symbol']} ({token['network'].upper()})")
        print(f"    📍 {token['address']}")
        print(f"    ⏰ Age: {age_display}")
        print(f"    💰 Prix: ${token['price_usd']:.10f}")
        print(f"    📈 Perf: 1h: {token['price_change_1h']:+.1f}% | 6h: {token['price_change_6h']:+.1f}% | 24h: {token['price_change_24h']:+.1f}%")
        print(f"    💵 Volume 24h: ${token['volume_24h']:,.0f}")
        print(f"    💧 Liquidité: ${token['liquidity_usd']:,.0f} (V/L: {token['volume_to_liquidity_ratio']:.2f}x)")
        print(f"    📊 FDV: ${token['fdv']:,.0f}")
        print(f"    🔄 Txns 24h: {token['txns_24h_total']:,} ({token['txns_24h_buys']} buys / {token['txns_24h_sells']} sells)")
        print(f"    {sentiment} | Ratio achats: {token['buys_ratio']:.1%}")
        print(f"    🏪 DEX: {token['dex']}")
        print(f"    🔗 {token['url']}")
        print()

def save_results(tokens, filename="top_performers.json"):
    """Sauvegarde les résultats"""
    output_path = Path(__file__).parent / "data" / "raw" / "json" / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(tokens, f, indent=2)

    print(f"💾 Résultats sauvegardés: {output_path}")

def get_top_gainers_summary(tokens_base, tokens_bsc):
    """Affiche un résumé des meilleurs gainers combinés"""
    all_tokens = tokens_base + tokens_bsc
    all_tokens.sort(key=lambda x: x['price_change_24h'], reverse=True)

    print(f"\n{'='*110}")
    print(f"🏆 TOP 10 GAINERS ABSOLUS (BASE + BSC)")
    print(f"{'='*110}\n")

    for i, token in enumerate(all_tokens[:10], 1):
        print(f"[{i}] {token['symbol']} ({token['network'].upper()}) - +{token['price_change_24h']:.1f}% (24h)")
        print(f"    Vol: ${token['volume_24h']:,.0f} | Liq: ${token['liquidity_usd']:,.0f}")
        print(f"    🔗 {token['url']}")
        print()

# ============================================================================
# EXÉCUTION PRINCIPALE
# ============================================================================

if __name__ == "__main__":
    import sys

    # Paramètre de période depuis la ligne de commande
    timeframe = sys.argv[1] if len(sys.argv) > 1 else "24h"

    if timeframe == "7d" or timeframe == "7j":
        # Recherche sur 1 semaine (tokens entre 24h et 7 jours)
        min_age = 24
        max_age = 168  # 7 jours = 168 heures
        min_change = 10  # Critère plus souple sur 7j
        title_suffix = "DERNIERS 7 JOURS"
        filename_suffix = "7d"
    else:
        # Recherche sur 24h (défaut)
        min_age = 24
        max_age = None
        min_change = 20
        title_suffix = "24H"
        filename_suffix = "24h"

    print("\n" + "="*110)
    print(f"🦎 GECKOTERMINAL API - TOP PERFORMERS {title_suffix}")
    print("="*110)

    # ========== BASE ==========
    print("\n" + "="*110)
    print(f"🔵 BASE NETWORK ({title_suffix})")
    print("="*110 + "\n")

    tokens_base = get_top_performers(
        network="base",
        min_price_change_24h=min_change,
        min_volume_24h=5000,
        min_liquidity=3000,
        max_fdv=100000000,
        min_txns_24h=50,
        min_buys_ratio=0.15,
        min_age_hours=min_age,
        max_age_hours=max_age,
        limit=30
    )

    display_results(tokens_base, title=f"BASE TOP PERFORMERS ({title_suffix})", limit=15)

    if tokens_base:
        save_results(tokens_base, f"top_performers_base_{filename_suffix}.json")

    # ========== BSC ==========
    print("\n" + "="*110)
    print(f"🟡 BSC (BNB CHAIN) ({title_suffix})")
    print("="*110 + "\n")

    tokens_bsc = get_top_performers(
        network="bsc",
        min_price_change_24h=min_change,
        min_volume_24h=5000,
        min_liquidity=3000,
        max_fdv=100000000,
        min_txns_24h=50,
        min_buys_ratio=0.15,
        min_age_hours=min_age,
        max_age_hours=max_age,
        limit=30
    )

    display_results(tokens_bsc, title=f"BSC TOP PERFORMERS ({title_suffix})", limit=15)

    if tokens_bsc:
        save_results(tokens_bsc, f"top_performers_bsc_{filename_suffix}.json")

    # ========== RÉSUMÉ GLOBAL ==========
    print("\n" + "="*110)
    print(f"📊 RÉSUMÉ GLOBAL ({title_suffix})")
    print("="*110)
    print(f"🔵 Base: {len(tokens_base)} tokens performants")
    print(f"🟡 BSC: {len(tokens_bsc)} tokens performants")
    print(f"🎯 Total: {len(tokens_base) + len(tokens_bsc)} opportunités détectées")
    if max_age:
        print(f"⏰ Période analysée: tokens entre {min_age}h et {max_age}h d'âge")
    else:
        print(f"⏰ Période analysée: tokens de {min_age}h+ d'âge")
    print("="*110)

    # Top 10 absolus
    if tokens_base or tokens_bsc:
        get_top_gainers_summary(tokens_base, tokens_bsc)

    print("\n✅ Analyse terminée !\n")
    print("💡 Usage:")
    print("   python test.py       → Analyse 24h (défaut)")
    print("   python test.py 7d    → Analyse 7 jours")
    print()
