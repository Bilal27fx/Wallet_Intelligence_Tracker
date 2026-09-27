"""Tests contre les vraies API. Lancer avec : make test args="-m live integrations"."""

import os
from datetime import UTC, datetime

import pytest

from apps.discovery.services.explosion import detect_explosion
from apps.discovery.services.settings import Thresholds
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
    pages = client.transfer_pages(usdc, height - 20, height - 10)
    transfers = [transfer for page in pages for transfer in page]
    assert transfers
    assert all(t.tx_from.startswith("0x") and len(t.tx_from) == 42 for t in transfers)


AI_TOKEN = "0x2e8c31162b855a2ffa90f6f8634643ad6f111e18"
LIVE_THRESHOLDS = Thresholds(
    min_change_24h_pct=50,
    min_liquidity_usd=10_000,
    min_volume_usd=50_000,
    peak_volume_window_hours=24,
    min_fdv_usd=100_000,
    max_fdv_usd=100_000_000,
    max_pool_age_hours=720,
    min_multiplier=5,
    min_retention_pct=30,
    confirmation_hours=24,
    sniper_blocks=3,
    min_buy_usd=500,
    max_buyers=300,
    explosion_window_hours=168,
    maturity_hours=336,
    breakout_multiplier=2,
    buyer_window_hours=0,
    min_score=5,
    max_multiplier=10_000,
    hub_min_senders=10,
    vault_follow_depth=2,
    bot_window_days=7,
    vault_min_pct=20,
)


def test_ai_token_trough_is_mid_august():
    client = geckoterminal.GeckoTerminalClient(http(geckoterminal.BASE_URL))
    pool = max(client.token_pools("robinhood", AI_TOKEN), key=lambda p: p.liquidity_usd)
    candles = client.ohlcv("robinhood", pool.address, "hour", 4)
    verdict = detect_explosion(
        candles,
        now_ts=candles[-1].ts,
        pool_created_ts=int(pool.created_at.timestamp()),
        current_liquidity_usd=pool.liquidity_usd,
        thresholds=LIVE_THRESHOLDS,
        window_hours=None,
    )
    trough = datetime.fromtimestamp(verdict.wave.trough.ts, tz=UTC)
    assert datetime(2026, 8, 15, tzinfo=UTC) <= trough <= datetime(2026, 8, 20, tzinfo=UTC)


KNOWN_WALLET = "0x44df085447dbebcf69c6675c3b8a795c7fdeb3f4"


@pytest.mark.skipif(not os.environ.get("ZERION_API_KEY"), reason="ZERION_API_KEY absente")
def test_zerion_portfolio_by_token_and_token_metadata():
    client = zerion.ZerionClient(http(zerion.BASE_URL, auth=(os.environ["ZERION_API_KEY"], "")))
    portfolio = client.portfolio(KNOWN_WALLET)
    assert portfolio.positions and portfolio.total_usd > 0
    assert abs(sum(portfolio.by_chain.values()) - portfolio.total_usd) < 1
    [meta] = client.token_metadata([("robinhood", AI_TOKEN)])
    assert meta.total_supply and meta.total_supply > 0
