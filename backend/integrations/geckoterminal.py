"""Client GeckoTerminal (API publique v2)."""

from dataclasses import dataclass
from datetime import datetime

BASE_URL = "https://api.geckoterminal.com/api/v2"
MAX_OHLCV_CANDLES = 1000


@dataclass(frozen=True)
class GtNetwork:
    gt_id: str
    name: str
    coingecko_platform_id: str | None


@dataclass(frozen=True)
class GtPool:
    network: str
    address: str
    token_address: str
    token_symbol: str
    token_decimals: int | None
    price_change_24h_pct: float
    volume_24h_usd: float
    liquidity_usd: float
    fdv_usd: float
    created_at: datetime | None


@dataclass(frozen=True)
class GtToken:
    address: str
    symbol: str
    decimals: int


@dataclass(frozen=True)
class Candle:
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float


def _float(value) -> float:
    return float(value) if value not in (None, "") else 0.0


def _datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


class GeckoTerminalClient:
    def __init__(self, http):
        self._http = http

    def networks(self) -> list[GtNetwork]:
        networks: list[GtNetwork] = []
        page = 1
        while True:
            payload = self._http.get("/networks", params={"page": page})
            for item in payload.get("data", []):
                attributes = item["attributes"]
                networks.append(
                    GtNetwork(
                        item["id"],
                        attributes["name"],
                        attributes.get("coingecko_asset_platform_id"),
                    )
                )
            if not payload.get("links", {}).get("next"):
                return networks
            page += 1

    def trending_pools(self, page: int) -> list[GtPool]:
        params = {"duration": "24h", "page": page, "include": "base_token"}
        return self._pools("/networks/trending_pools", params)

    def top_volume_pools(self, network: str, page: int) -> list[GtPool]:
        params = {"sort": "h24_volume_usd_desc", "page": page, "include": "base_token"}
        return self._pools(f"/networks/{network}/pools", params, network=network)

    def token_pools(self, network: str, token_address: str) -> list[GtPool]:
        params = {"page": 1, "include": "base_token"}
        return self._pools(
            f"/networks/{network}/tokens/{token_address}/pools", params, network=network
        )

    def token(self, network: str, address: str) -> GtToken:
        attributes = self._http.get(f"/networks/{network}/tokens/{address}")["data"]["attributes"]
        return GtToken(
            attributes["address"].lower(),
            attributes.get("symbol") or "",
            int(attributes["decimals"]),
        )

    def ohlcv(
        self, network: str, pool_address: str, timeframe: str, aggregate: int
    ) -> list[Candle]:
        payload = self._http.get(
            f"/networks/{network}/pools/{pool_address}/ohlcv/{timeframe}",
            params={"aggregate": aggregate, "limit": MAX_OHLCV_CANDLES, "currency": "usd"},
        )
        rows = payload["data"]["attributes"]["ohlcv_list"]
        candles = [Candle(int(r[0]), *(float(v) for v in r[1:6])) for r in rows]
        return sorted(candles, key=lambda candle: candle.ts)

    def _pools(self, path: str, params: dict, network: str | None = None) -> list[GtPool]:
        payload = self._http.get(path, params=params)
        tokens = {
            item["id"]: item["attributes"]
            for item in payload.get("included", [])
            if item.get("type") == "token"
        }
        pools = []
        for item in payload.get("data", []):
            relationships = item["relationships"]
            pool_network = network or relationships["network"]["data"]["id"]
            base_id = relationships["base_token"]["data"]["id"]
            token = tokens.get(base_id, {})
            attributes = item["attributes"]
            decimals = token.get("decimals")
            pools.append(
                GtPool(
                    network=pool_network,
                    address=attributes["address"].lower(),
                    token_address=base_id[len(pool_network) + 1 :].lower(),
                    token_symbol=token.get("symbol") or "",
                    token_decimals=int(decimals) if decimals is not None else None,
                    price_change_24h_pct=_float(
                        attributes.get("price_change_percentage", {}).get("h24")
                    ),
                    volume_24h_usd=_float(attributes.get("volume_usd", {}).get("h24")),
                    liquidity_usd=_float(attributes.get("reserve_in_usd")),
                    fdv_usd=_float(attributes.get("fdv_usd")),
                    created_at=_datetime(attributes.get("pool_created_at")),
                )
            )
        return pools
