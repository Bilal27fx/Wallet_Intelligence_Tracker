"""Tests contre les vraies API (quelques appels Zerion). Lancer : make test args="-m live"."""

import os
import time

import pytest

from integrations.http import JsonHttpClient
from integrations.hypersync import HyperSyncClient
from integrations.ratelimit import NoopLimiter
from integrations.rpc import RpcClient
from integrations.zerion import BASE_URL, ZerionClient

pytestmark = pytest.mark.live

USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
ROBINHOOD_WALLET = "0x3296219c6167893ca763c6d515df6a542e5cc395"


@pytest.mark.skipif(not os.environ.get("ZERION_API_KEY"), reason="ZERION_API_KEY absente")
def test_zerion_prices_search_and_chart():
    client = ZerionClient(
        JsonHttpClient(BASE_URL, limiter=NoopLimiter(), auth=(os.environ["ZERION_API_KEY"], ""))
    )
    price = client.prices([("base", USDC_BASE)])[("base", USDC_BASE)]
    assert 0.9 < price.price < 1.1
    time.sleep(1.5)
    assert client.search("USDC").implementations["base"][0] == USDC_BASE
    time.sleep(1.5)
    assert len(client.price_chart("eth")) > 300


def test_public_rpc_native_balance():
    rpc = RpcClient(JsonHttpClient("https://mainnet.base.org/", limiter=NoopLimiter()))
    assert rpc.native_balance("0x4200000000000000000000000000000000000006") >= 0


@pytest.mark.skipif(not os.environ.get("ENVIO_API_TOKEN"), reason="ENVIO_API_TOKEN absent")
def test_hypersync_wallet_queries_on_robinhood():
    client = HyperSyncClient(4663, os.environ["ENVIO_API_TOKEN"], NoopLimiter())
    height = client.height()
    assert client.wallet_transfers(ROBINHOOD_WALLET, 0, height)
    assert client.wallet_tx_count(ROBINHOOD_WALLET, 0, height, cap=5) >= 5
    assert client.first_funding(ROBINHOOD_WALLET, height) is not None
