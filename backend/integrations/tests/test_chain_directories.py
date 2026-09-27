import respx

from integrations.coingecko import BASE_URL as CG_URL
from integrations.coingecko import CoinGeckoClient
from integrations.http import JsonHttpClient
from integrations.hypersync import CHAINS_URL, HyperSyncDirectory, hypersync_url
from integrations.ratelimit import NoopLimiter
from integrations.zerion import BASE_URL as ZERION_URL
from integrations.zerion import ZerionClient


def http(base: str) -> JsonHttpClient:
    return JsonHttpClient(base, limiter=NoopLimiter(), max_retries=0)


@respx.mock
def test_coingecko_platform_chain_ids():
    respx.get(f"{CG_URL}/asset_platforms").respond(
        json=[
            {"id": "ethereum", "chain_identifier": 1},
            {"id": "robinhood", "chain_identifier": 4663},
            {"id": "solana", "chain_identifier": None},
        ]
    )
    assert CoinGeckoClient(http(CG_URL)).platform_chain_ids() == {"ethereum": 1, "robinhood": 4663}


@respx.mock
def test_zerion_chain_ids_parses_hex_external_id():
    respx.get(f"{ZERION_URL}/chains/").respond(
        json={
            "data": [
                {"id": "ethereum", "attributes": {"external_id": "0x1"}},
                {"id": "robinhood", "attributes": {"external_id": "0x1237"}},
                {"id": "solana", "attributes": {"external_id": None}},
            ]
        }
    )
    assert ZerionClient(http(ZERION_URL)).chain_ids() == {1: "ethereum", 4663: "robinhood"}


@respx.mock
def test_hypersync_directory_keeps_evm_mainnets():
    respx.get(f"{CHAINS_URL}/active_chains").respond(
        json=[
            {"name": "eth", "tier": "GOLD", "chain_id": 1, "ecosystem": "evm"},
            {"name": "robinhood", "tier": "STONE", "chain_id": 4663, "ecosystem": "evm"},
            {"name": "arbitrum-sepolia", "tier": "TESTNET", "chain_id": 421614, "ecosystem": "evm"},
            {"name": "fuel-mainnet", "tier": "GOLD", "chain_id": 9889, "ecosystem": "fuel"},
            {"name": "solana-448h", "tier": "TESTNET", "ecosystem": "solana"},
        ]
    )
    assert HyperSyncDirectory(http(CHAINS_URL)).supported_chain_ids() == {1, 4663}


def test_hypersync_url():
    assert hypersync_url(8453) == "https://8453.hypersync.xyz"


@respx.mock
def test_zerion_chains_reads_rpc_and_native_assets():
    respx.get(f"{ZERION_URL}/chains/").respond(
        json={
            "data": [
                {
                    "id": "base",
                    "attributes": {
                        "external_id": "0x2105",
                        "rpc": {
                            "public_servers_url": ["wss://ws.base", "https://mainnet.base.org/"]
                        },
                    },
                    "relationships": {
                        "native_fungible": {"data": {"type": "fungibles", "id": "eth"}},
                        "wrapped_native_fungible": {"data": {"type": "fungibles", "id": "0xweth"}},
                    },
                },
                {"id": "solana", "attributes": {"external_id": None}},
                {
                    "id": "bare",
                    "attributes": {"external_id": "0x1", "rpc": None},
                    "relationships": {},
                },
            ]
        }
    )
    client = ZerionClient(http(ZERION_URL))
    [base, bare] = client.chains()
    assert (base.zerion_id, base.evm_id, base.rpc_url) == (
        "base",
        8453,
        "https://mainnet.base.org/",
    )
    assert (base.native_fungible_id, base.wrapped_fungible_id) == ("eth", "0xweth")
    assert (bare.rpc_url, bare.native_fungible_id) == ("", "")
    assert client.chain_ids() == {8453: "base", 1: "bare"}
