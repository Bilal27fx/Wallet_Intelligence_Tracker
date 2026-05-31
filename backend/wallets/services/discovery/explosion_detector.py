"""Token explosion detection service."""
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from common.api_clients import DexScreenerClient
from django.conf import settings


class ExplosionDetectorService:
    """Service to detect token explosion signals."""

    def __init__(self):
        self.client = DexScreenerClient()
        self.volume_threshold = getattr(settings, 'EXPLOSION_VOLUME_THRESHOLD', 100000)
        self.price_change_threshold = getattr(settings, 'EXPLOSION_PRICE_CHANGE', 50)
        self.liquidity_threshold = getattr(settings, 'EXPLOSION_MIN_LIQUIDITY', 50000)

    def check_token_explosion(
        self,
        token_address: str,
        chain: str = "ethereum"
    ) -> Dict[str, Any]:
        """Check if a token shows explosion signals."""
        pairs = self.client.get_token_pairs(token_address)

        if not pairs:
            return {
                'is_exploding': False,
                'reason': 'No trading pairs found'
            }

        main_pair = max(pairs, key=lambda p: p.get('volume', {}).get('h24', 0))

        volume_24h = main_pair.get('volume', {}).get('h24', 0)
        price_change_24h = main_pair.get('priceChange', {}).get('h24', 0)
        liquidity = main_pair.get('liquidity', {}).get('usd', 0)
        txns_24h = main_pair.get('txns', {}).get('h24', {})
        total_txns = txns_24h.get('buys', 0) + txns_24h.get('sells', 0)

        is_exploding = (
            volume_24h >= self.volume_threshold and
            price_change_24h >= self.price_change_threshold and
            liquidity >= self.liquidity_threshold
        )

        return {
            'is_exploding': is_exploding,
            'token_address': token_address,
            'chain': chain,
            'pair_address': main_pair.get('pairAddress'),
            'dex': main_pair.get('dexId'),
            'volume_24h': volume_24h,
            'price_change_24h': price_change_24h,
            'liquidity_usd': liquidity,
            'total_transactions_24h': total_txns,
            'buys_24h': txns_24h.get('buys', 0),
            'sells_24h': txns_24h.get('sells', 0),
            'price_usd': main_pair.get('priceUsd'),
            'market_cap': main_pair.get('marketCap'),
            'detected_at': datetime.now().isoformat()
        }

    def get_explosion_score(self, metrics: Dict[str, Any]) -> float:
        """Calculate explosion score (0-100)."""
        score = 0.0

        volume = metrics.get('volume_24h', 0)
        if volume >= self.volume_threshold:
            score += min(30, (volume / self.volume_threshold) * 10)

        price_change = metrics.get('price_change_24h', 0)
        if price_change > 0:
            score += min(30, price_change / 3)

        liquidity = metrics.get('liquidity_usd', 0)
        if liquidity >= self.liquidity_threshold:
            score += min(20, (liquidity / self.liquidity_threshold) * 5)

        txns = metrics.get('total_transactions_24h', 0)
        if txns > 0:
            score += min(20, txns / 50)

        return min(100, score)

    def monitor_token_trends(
        self,
        token_address: str,
        chain: str = "ethereum"
    ) -> Optional[Dict[str, Any]]:
        """Monitor token for trending signals."""
        explosion_data = self.check_token_explosion(token_address, chain)

        if explosion_data['is_exploding']:
            explosion_data['explosion_score'] = self.get_explosion_score(explosion_data)
            return explosion_data

        return None
