"""Tests contre les vraies API. Lancer avec : make test args="-m live integrations"."""

import os

import pytest

from integrations import coingecko, geckoterminal, zerion
from integrations.http import JsonHttpClient
from integrations.hypersync import CHAINS_URL, HyperSyncClient, HyperSyncDirectory
from integrations.ratelimit import NoopLimiter

pytestmark = pytest.mark.live


def http(base: str, **kwargs) -> JsonHttpClient:
    return JsonHttpClient(base, limiter=NoopLimiter(), **kwargs)


def test_geckoterminal_trending_and_ohlcv():
    client = geckoterminal.GeckoTerminalClient(http(geckoterminal.BASE_URL))
    pools = client.trending_pools(page=1)
    assert pools
    candles = client.ohlcv(pools[0].network, pools[0].address, "hour", 1)
    assert candles and candles == sorted(candles, key=lambda c: c.ts)


def test_coingecko_maps_base():
    assert coingecko.CoinGeckoClient(http(coingecko.BASE_URL)).platform_chain_ids()["base"] == 8453


def test_hypersync_directory_supports_base():
    assert 8453 in HyperSyncDirectory(http(CHAINS_URL)).supported_chain_ids()


@pytest.mark.skipif(not os.environ.get("ZERION_API_KEY"), reason="ZERION_API_KEY absente")
def test_zerion_supports_base():
    client = zerion.ZerionClient(http(zerion.BASE_URL, auth=(os.environ["ZERION_API_KEY"], "")))
    assert client.chain_ids()[8453] == "base"


@pytest.mark.skipif(not os.environ.get("ENVIO_API_TOKEN"), reason="ENVIO_API_TOKEN absent")
def test_hypersync_reads_blocks_and_transfers():
    client = HyperSyncClient(8453, os.environ["ENVIO_API_TOKEN"], NoopLimiter())
    height = client.height()
    assert client.block_timestamp(height - 100) > 1_700_000_000
    usdc = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    transfers = client.transfers(usdc, height - 20, height - 10, max_transfers=100_000)
    assert transfers
    assert all(t.tx_from.startswith("0x") and len(t.tx_from) == 42 for t in transfers)
