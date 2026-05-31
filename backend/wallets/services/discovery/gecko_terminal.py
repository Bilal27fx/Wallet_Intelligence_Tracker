"""GeckoTerminal API service for token discovery."""
import time
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import requests
from django.conf import settings
from wallets.models import ExplosiveToken

logger = logging.getLogger(__name__)


class GeckoTerminalService:
    """Service to detect explosive tokens via GeckoTerminal."""

    BASE_URL = "https://api.geckoterminal.com/api/v2"
    RATE_LIMIT_DELAY = 1.5
    REQUEST_TIMEOUT = 15
    RETRY_WAIT = 60

    # Filters from config
    MIN_AGE_HOURS = getattr(settings, 'GECKO_MIN_AGE_HOURS', 24)
    MIN_PRICE_CHANGE_24H = getattr(settings, 'GECKO_MIN_PRICE_CHANGE', 20)
    MIN_VOLUME_24H = getattr(settings, 'GECKO_MIN_VOLUME', 5000)
    MIN_LIQUIDITY = getattr(settings, 'GECKO_MIN_LIQUIDITY', 3000)
    MIN_FDV = getattr(settings, 'GECKO_MIN_FDV', 300000)
    MAX_FDV = getattr(settings, 'GECKO_MAX_FDV', 100000000)
    MIN_TXNS_24H = getattr(settings, 'GECKO_MIN_TXNS', 50)
    MIN_BUYS_RATIO = getattr(settings, 'GECKO_MIN_BUYS_RATIO', 0.15)
    MAX_POOL_AGE_DAYS = getattr(settings, 'GECKO_MAX_POOL_AGE_DAYS', 365)
    LIMIT = getattr(settings, 'GECKO_LIMIT', 30)
    NETWORKS = getattr(settings, 'GECKO_NETWORKS', ['base', 'bsc'])

    def _request(self, url: str) -> Optional[Dict]:
        """HTTP request with rate limiting and retry on 429."""
        time.sleep(self.RATE_LIMIT_DELAY)
        try:
            r = requests.get(url, timeout=self.REQUEST_TIMEOUT)
            if r.status_code == 429:
                logger.warning(f"Rate limit reached, waiting {self.RETRY_WAIT}s")
                time.sleep(self.RETRY_WAIT)
                return self._request(url)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            logger.error(f"Request error {url}: {e}")
            return None

    def _fetch_pools(self, network: str) -> List[Dict]:
        """Fetch trending and new pools from a network."""
        pools = []
        for endpoint in ("trending_pools", "new_pools"):
            data = self._request(f"{self.BASE_URL}/networks/{network}/{endpoint}")
            if data:
                batch = data.get("data", [])
                pools.extend(batch)
                logger.info(f"{network.upper()} {endpoint}: {len(batch)} pools")
        return pools

    def _pool_age_hours(self, pool_created_at: str) -> float:
        """Calculate pool age in hours."""
        if not pool_created_at:
            return 0
        try:
            created = datetime.fromisoformat(pool_created_at.replace("Z", "+00:00"))
            return (datetime.now(timezone.utc) - created).total_seconds() / 3600
        except Exception:
            return 0

    def _passes_filters(
        self,
        attrs: Dict,
        token_address: str,
        seen: set,
        min_change: float,
        min_age: float,
        max_age: Optional[float]
    ) -> bool:
        """Check if a pool passes all filters."""
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

        age = self._pool_age_hours(attrs.get("pool_created_at", ""))
        max_age_hours = self.MAX_POOL_AGE_DAYS * 24
        age_ok = age >= min_age and age <= max_age_hours

        return all([
            price_usd > 0,
            price_change_24h >= min_change,
            volume_24h >= self.MIN_VOLUME_24H,
            liquidity >= self.MIN_LIQUIDITY,
            fdv >= self.MIN_FDV,
            fdv <= self.MAX_FDV,
            total >= self.MIN_TXNS_24H,
            buys_ratio >= self.MIN_BUYS_RATIO,
            age_ok,
        ])

    def _build_token_data(self, pool: Dict, network: str) -> Dict[str, Any]:
        """Build token dict from GeckoTerminal pool."""
        attrs = pool.get("attributes", {})
        base_token_id = pool.get("relationships", {}).get("base_token", {}).get("data", {}).get("id", "")
        address = base_token_id.replace(f"{network}_", "")
        name = attrs.get("name", "UNKNOWN")
        symbol = name.split("/")[0].strip() if "/" in name else "UNKNOWN"
        pool_address = attrs.get("address", "")
        age = self._pool_age_hours(attrs.get("pool_created_at", ""))
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
            "price_usd": float(attrs.get("base_token_price_usd", 0) or 0),
            "price_change_24h": float(attrs.get("price_change_percentage", {}).get("h24", 0) or 0),
            "volume_24h": volume_24h,
            "liquidity_usd": liquidity,
            "fdv": float(attrs.get("fdv_usd", 0) or 0),
            "txns_24h_buys": buys,
            "txns_24h_sells": sells,
            "buys_ratio": buys / (buys + sells) if (buys + sells) > 0 else 0,
            "pool_age_hours": round(age, 1),
            "pool_created_at": attrs.get("pool_created_at", ""),
        }

    def get_top_performers(
        self,
        network: str,
        min_change: Optional[float] = None,
        min_age: Optional[float] = None,
        max_age: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """Fetch explosive tokens for a network."""
        min_change = min_change if min_change is not None else self.MIN_PRICE_CHANGE_24H
        min_age = min_age if min_age is not None else self.MIN_AGE_HOURS

        logger.info(f"Fetching top performers {network.upper()} (age: {min_age}h+, perf min: +{min_change}%)")

        pools = self._fetch_pools(network)
        if not pools:
            logger.warning(f"No pools fetched for {network}")
            return []

        seen = set()
        results = []

        for pool in pools:
            attrs = pool.get("attributes", {})
            base_token_id = pool.get("relationships", {}).get("base_token", {}).get("data", {}).get("id", "")
            address = base_token_id.replace(f"{network}_", "")

            if not self._passes_filters(attrs, address, seen, min_change, min_age, max_age):
                continue

            seen.add(address)
            results.append(self._build_token_data(pool, network))

            if len(results) >= self.LIMIT:
                break

        results.sort(key=lambda x: x["price_change_24h"], reverse=True)
        logger.info(f"{len(results)} explosive tokens found on {network.upper()}")
        return results

    def save_to_db(self, tokens: List[Dict[str, Any]]) -> int:
        """Save detected tokens to database."""
        if not tokens:
            return 0

        saved_count = 0
        for token in tokens:
            _, created = ExplosiveToken.objects.update_or_create(
                token_address=token["address"],
                chain=token["network"],
                defaults={
                    'symbol': token["symbol"],
                    'pool_address': token.get("pool_address"),
                    'pool_age_hours': token.get("pool_age_hours"),
                    'price_change_24h': token.get("price_change_24h"),
                    'volume_24h': token.get("volume_24h"),
                    'liquidity_usd': token.get("liquidity_usd"),
                    'fdv': token.get("fdv"),
                    'buys_ratio': token.get("buys_ratio"),
                }
            )
            if created:
                saved_count += 1

        logger.info(f"{saved_count} new tokens saved to DB")
        return saved_count

    def run_detection(self, timeframe: str = "24h") -> Dict[str, Any]:
        """Run detection on all configured networks."""
        if timeframe in ("7d", "7j"):
            min_age, max_age, min_change = 24, 168, 10
        else:
            min_age, max_age, min_change = self.MIN_AGE_HOURS, None, self.MIN_PRICE_CHANGE_24H

        all_tokens = []
        for network in self.NETWORKS:
            tokens = self.get_top_performers(network, min_change=min_change, min_age=min_age, max_age=max_age)
            all_tokens.extend(tokens)

        all_tokens.sort(key=lambda x: x["price_change_24h"], reverse=True)
        saved = self.save_to_db(all_tokens)

        logger.info(f"Total: {len(all_tokens)} explosive tokens detected ({timeframe})")
        return {
            'total_found': len(all_tokens),
            'saved': saved,
            'timeframe': timeframe,
            'tokens': all_tokens
        }
