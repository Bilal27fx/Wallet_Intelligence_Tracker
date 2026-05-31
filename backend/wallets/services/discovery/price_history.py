"""Price history fetcher service."""
import time
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import requests
from django.conf import settings
from wallets.models import ExplosiveToken, TokenPriceHistory

logger = logging.getLogger(__name__)


class PriceHistoryService:
    """Service to fetch and store OHLCV price history for explosive tokens."""

    BASE_URL = "https://api.geckoterminal.com/api/v2"
    RATE_LIMIT_DELAY = 1.5
    REQUEST_TIMEOUT = 15
    RETRY_WAIT = 60
    OHLCV_AGGREGATE = getattr(settings, 'GECKO_OHLCV_AGGREGATE', 4)  # 4H candles
    OHLCV_LIMIT = getattr(settings, 'GECKO_OHLCV_LIMIT', 200)

    def _request(self, url: str) -> Optional[Dict]:
        """HTTP request with rate limiting and retry on 429."""
        time.sleep(self.RATE_LIMIT_DELAY)
        headers = {"Accept": "application/json;version=20230302"}
        try:
            r = requests.get(url, headers=headers, timeout=self.REQUEST_TIMEOUT)
            if r.status_code == 429:
                logger.warning(f"Rate limit, waiting {self.RETRY_WAIT}s")
                time.sleep(self.RETRY_WAIT)
                return self._request(url)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            logger.error(f"Request error {url}: {e}")
            return None

    def _fetch_ohlcv(self, network: str, pool_address: str) -> List:
        """Fetch 4H candles for a pool."""
        url = (
            f"{self.BASE_URL}/networks/{network}/pools/{pool_address}"
            f"/ohlcv/hour?aggregate={self.OHLCV_AGGREGATE}&limit={self.OHLCV_LIMIT}"
        )
        data = self._request(url)
        if not data:
            return []
        return data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])

    def _ts_to_datetime(self, timestamp: int) -> datetime:
        """Convert unix timestamp to datetime."""
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)

    def save_ohlcv(
        self,
        token_address: str,
        chain: str,
        pool_address: str,
        ohlcv_list: List
    ) -> int:
        """Save OHLCV candles to database."""
        if not ohlcv_list:
            return 0

        saved_count = 0
        for row in ohlcv_list:
            timestamp, open_price, high, low, close, volume = row
            date = self._ts_to_datetime(timestamp)

            _, created = TokenPriceHistory.objects.update_or_create(
                token_address=token_address,
                chain=chain,
                date=date,
                defaults={
                    'pool_address': pool_address,
                    'close': close,
                    'volume': volume,
                }
            )
            if created:
                saved_count += 1

        return saved_count

    def fetch_token_history(self, token: ExplosiveToken) -> Dict[str, Any]:
        """Fetch price history for a single token."""
        if not token.pool_address:
            logger.warning(f"No pool_address for {token.symbol}, skipping")
            return {'status': 'skipped', 'reason': 'no_pool_address'}

        ohlcv = self._fetch_ohlcv(token.chain, token.pool_address)
        if not ohlcv:
            logger.warning(f"{token.symbol} ({token.chain}): no OHLCV data")
            return {'status': 'no_data'}

        inserted = self.save_ohlcv(
            token.token_address,
            token.chain,
            token.pool_address,
            ohlcv
        )

        logger.info(f"{token.symbol} ({token.chain.upper()}) [4H]: {inserted} candles saved")
        return {'status': 'success', 'candles_saved': inserted}

    def run_price_history_fetch(self) -> Dict[str, Any]:
        """Fetch price history for all explosive tokens."""
        tokens = ExplosiveToken.objects.all()
        if not tokens.exists():
            logger.warning("No tokens in explosive_tokens_detected")
            return {'total_tokens': 0, 'total_candles': 0}

        logger.info(f"Fetching price history for {tokens.count()} tokens")
        total_candles = 0

        for token in tokens:
            result = self.fetch_token_history(token)
            if result.get('status') == 'success':
                total_candles += result.get('candles_saved', 0)

        logger.info(f"Total: {total_candles} candles saved to DB")
        return {
            'total_tokens': tokens.count(),
            'total_candles': total_candles
        }
