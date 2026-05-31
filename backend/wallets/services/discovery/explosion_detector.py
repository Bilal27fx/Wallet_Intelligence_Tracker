"""Token explosion detection service."""
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from common.api_clients import DexScreenerClient
from django.conf import settings
from wallets.models import ExplosiveToken, TokenPriceHistory
import logging

logger = logging.getLogger(__name__)


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

    def detect_explosion_from_history(
        self,
        token: ExplosiveToken
    ) -> Optional[Dict[str, Any]]:
        """
        Detect optimal explosion period from price history.
        Score = hours_gap * explosion_pct (favors delay + performance).
        """
        min_hours = getattr(settings, 'GECKO_MIN_HOURS_BEFORE_EXPLOSION', 12)
        min_pct = getattr(settings, 'GECKO_MIN_EXPLOSION_PCT', 200)

        # Get price history for this token
        prices = TokenPriceHistory.objects.filter(
            token_address=token.token_address,
            chain=token.chain
        ).order_by('date').values_list('date', 'close')

        if not prices or len(prices) < 2:
            return None

        creation_date = prices[0][0]
        best = None

        for i, (start_date, start_close) in enumerate(prices):
            hours_gap = (start_date - creation_date).total_seconds() / 3600
            if hours_gap < min_hours or start_close <= 0:
                continue

            # Find peak after this entry point
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
            hours_since_now = (datetime.now(start_date.tzinfo) - start_date).total_seconds() / 3600

            if best is None or score > best["score"]:
                best = {
                    "explosion_start_date": start_date,
                    "explosion_peak_date": peak_date,
                    "explosion_pct": round(explosion_pct, 2),
                    "hours_gap": round(hours_gap, 1),
                    "hours_since_now": round(hours_since_now, 1),
                    "score": round(score, 2),
                }

        return best

    def detect_all_explosions(self) -> Dict[str, Any]:
        """Detect explosions for all tokens with price history."""
        # Get tokens that have price history
        tokens_with_history = ExplosiveToken.objects.filter(
            token_address__in=TokenPriceHistory.objects.values_list('token_address', flat=True).distinct()
        )

        if not tokens_with_history.exists():
            logger.warning("No tokens with price history")
            return {'detected': 0, 'total': 0}

        logger.info(f"Detecting explosions for {tokens_with_history.count()} tokens")
        detected = 0

        for token in tokens_with_history:
            result = self.detect_explosion_from_history(token)
            if result:
                # Update token with explosion data
                token.explosion_start_date = result['explosion_start_date']
                token.explosion_peak_date = result['explosion_peak_date']
                token.explosion_pct = result['explosion_pct']
                token.hours_gap = result['hours_gap']
                token.hours_since_now = result['hours_since_now']
                token.score = result['score']
                token.save()

                logger.info(
                    f"{token.symbol} ({token.chain.upper()}): +{result['explosion_pct']:.0f}% | "
                    f"gap: {result['hours_gap']:.0f}h | {result['hours_since_now']:.0f}h ago"
                )
                detected += 1
            else:
                logger.info(f"{token.symbol} ({token.chain.upper()}): no explosion detected")

        logger.info(f"{detected}/{tokens_with_history.count()} tokens with explosion detected")
        return {'detected': detected, 'total': tokens_with_history.count()}
