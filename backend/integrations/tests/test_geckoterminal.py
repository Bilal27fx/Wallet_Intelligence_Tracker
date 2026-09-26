from datetime import UTC, datetime

import respx

from integrations.geckoterminal import BASE_URL, Candle, GeckoTerminalClient, GtNetwork
from integrations.http import JsonHttpClient
from integrations.ratelimit import NoopLimiter

TOKEN = "0xAbC0000000000000000000000000000000000001"


def client() -> GeckoTerminalClient:
    return GeckoTerminalClient(JsonHttpClient(BASE_URL, limiter=NoopLimiter(), max_retries=0))


def pool_payload(network_rel: bool, network: str = "polygon_pos") -> dict:
    relationships = {
        "base_token": {"data": {"id": f"{network}_{TOKEN}", "type": "token"}},
        "quote_token": {"data": {"id": f"{network}_0xquote", "type": "token"}},
    }
    if network_rel:
        relationships["network"] = {"data": {"id": network, "type": "network"}}
    return {
        "data": [
            {
                "id": f"{network}_0xPOOL",
                "type": "pool",
                "attributes": {
                    "address": "0xPOOL",
                    "pool_created_at": "2026-09-20T10:00:00Z",
                    "fdv_usd": "1500000.5",
                    "reserve_in_usd": "80000",
                    "volume_usd": {"h24": "250000"},
                    "price_change_percentage": {"h24": "120.5"},
                },
                "relationships": relationships,
            }
        ],
        "included": [
            {
                "id": f"{network}_{TOKEN}",
                "type": "token",
                "attributes": {"address": TOKEN, "symbol": "PEPE", "decimals": 9},
            }
        ],
    }


@respx.mock
def test_networks_follows_pagination():
    respx.get(f"{BASE_URL}/networks", params={"page": "1"}).respond(
        json={
            "data": [
                {
                    "id": "eth",
                    "attributes": {"name": "Ethereum", "coingecko_asset_platform_id": "ethereum"},
                }
            ],
            "links": {"next": "https://api.geckoterminal.com/api/v2/networks?page=2"},
        }
    )
    respx.get(f"{BASE_URL}/networks", params={"page": "2"}).respond(
        json={
            "data": [
                {
                    "id": "robinhood",
                    "attributes": {"name": "Robinhood", "coingecko_asset_platform_id": None},
                }
            ],
            "links": {"next": None},
        }
    )
    assert client().networks() == [
        GtNetwork("eth", "Ethereum", "ethereum"),
        GtNetwork("robinhood", "Robinhood", None),
    ]


@respx.mock
def test_trending_pools_reads_network_from_relationship():
    respx.get(f"{BASE_URL}/networks/trending_pools").respond(json=pool_payload(network_rel=True))
    [pool] = client().trending_pools(page=1)
    assert pool.network == "polygon_pos"
    assert pool.address == "0xpool"
    assert pool.token_address == TOKEN.lower()
    assert pool.token_symbol == "PEPE"
    assert pool.token_decimals == 9
    assert pool.price_change_24h_pct == 120.5
    assert pool.volume_24h_usd == 250000.0
    assert pool.liquidity_usd == 80000.0
    assert pool.fdv_usd == 1500000.5
    assert pool.created_at == datetime(2026, 9, 20, 10, tzinfo=UTC)


@respx.mock
def test_top_volume_pools_uses_given_network():
    route = respx.get(f"{BASE_URL}/networks/polygon_pos/pools").respond(
        json=pool_payload(network_rel=False)
    )
    [pool] = client().top_volume_pools("polygon_pos", page=2)
    assert pool.network == "polygon_pos"
    assert pool.token_address == TOKEN.lower()
    params = route.calls.last.request.url.params
    assert params["sort"] == "h24_volume_usd_desc"
    assert params["page"] == "2"


@respx.mock
def test_token_returns_decimals():
    respx.get(f"{BASE_URL}/networks/base/tokens/{TOKEN.lower()}").respond(
        json={"data": {"attributes": {"address": TOKEN, "symbol": "PEPE", "decimals": 18}}}
    )
    token = client().token("base", TOKEN.lower())
    assert (token.address, token.symbol, token.decimals) == (TOKEN.lower(), "PEPE", 18)


@respx.mock
def test_ohlcv_returns_ascending_candles():
    route = respx.get(f"{BASE_URL}/networks/base/pools/0xpool/ohlcv/hour").respond(
        json={
            "data": {
                "attributes": {
                    "ohlcv_list": [
                        [7200, 2, 3, 1, 2.5, 100],
                        [3600, 1, 2, 0.5, 1.5, 50],
                    ]
                }
            }
        }
    )
    candles = client().ohlcv("base", "0xpool", "hour", 4)
    assert candles == [
        Candle(3600, 1.0, 2.0, 0.5, 1.5, 50.0),
        Candle(7200, 2.0, 3.0, 1.0, 2.5, 100.0),
    ]
    params = route.calls.last.request.url.params
    assert params["aggregate"] == "4"
    assert params["limit"] == "1000"
    assert params["currency"] == "usd"


@respx.mock
def test_non_evm_addresses_keep_their_case():
    payload = pool_payload(network_rel=True, network="solana")
    payload["data"][0]["attributes"]["address"] = "6E3jZLtF4tqBwZm3"
    payload["data"][0]["relationships"]["base_token"]["data"]["id"] = "solana_PumpAbC"
    payload["included"][0]["id"] = "solana_PumpAbC"
    respx.get(f"{BASE_URL}/networks/trending_pools").respond(json=payload)
    [pool] = client().trending_pools(page=1)
    assert pool.address == "6E3jZLtF4tqBwZm3"
    assert pool.token_address == "PumpAbC"
