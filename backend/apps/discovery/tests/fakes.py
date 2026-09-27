"""Faux clients déterministes : un token qui explose sur Base, avec trois acheteurs."""

from datetime import UTC, datetime, timedelta

from integrations.geckoterminal import Candle, GtNetwork, GtPool, GtToken
from integrations.hypersync import Transfer
from integrations.zerion import ZerionChain

HOUR = 3600
UNIT = 10**18
NOW = datetime(2026, 9, 20, 12, tzinfo=UTC)
POOL_CREATED = NOW - timedelta(hours=60)
TOKEN = "0x" + "1" * 40
POOL = "0x" + "2" * 40
ALICE = "0x" + "a" * 40
SNIPER = "0x" + "5" * 40
SMALL = "0x" + "c" * 40
STRANGER = "0x" + "d" * 40

# Chaîne simulée : un bloc toutes les 2 secondes, le pool est créé au bloc 500.
BLOCK_TIME = 2
GENESIS_TS = int(POOL_CREATED.timestamp()) - 500 * BLOCK_TIME


def block_of(ts: int) -> int:
    return (ts - GENESIS_TS) // BLOCK_TIME


def explosive_candles(start_ts: int, after_peak: float = 3.0) -> list[Candle]:
    """Bas 0.5 à l'heure 10, pic 5.0 à l'heure 20 (×10), puis 3.0 (rétention 60 %)."""
    closes = [1.0] * 10 + [0.5] + [0.5 + 0.45 * i for i in range(1, 10)] + [5.0] + [after_peak] * 30
    return [Candle(start_ts + i * HOUR, c, c, c, c, 100_000) for i, c in enumerate(closes)]


def make_pool(**overrides) -> GtPool:
    values = dict(
        network="base",
        address=POOL,
        token_address=TOKEN,
        token_symbol="BOOM",
        token_decimals=18,
        price_change_24h_pct=120.0,
        volume_24h_usd=200_000.0,
        liquidity_usd=50_000.0,
        fdv_usd=1_000_000.0,
        created_at=POOL_CREATED,
    )
    values.update(overrides)
    return GtPool(**values)


class FakeGeckoTerminal:
    def __init__(self, trending=None, volume=None, token_pools=None, candles=None):
        self.trending = [make_pool()] if trending is None else trending
        self.volume = volume or {}
        self.pools_by_token = token_pools
        self.candles = (
            candles if candles is not None else explosive_candles(int(POOL_CREATED.timestamp()))
        )

    def networks(self):
        return [GtNetwork("base", "Base", "base"), GtNetwork("solana", "Solana", "solana")]

    def trending_pools(self, page):
        return self.trending if page == 1 else []

    def top_volume_pools(self, network, page):
        return self.volume.get(network, []) if page == 1 else []

    def token_pools(self, network, token_address):
        if self.pools_by_token is not None:
            return self.pools_by_token
        return [make_pool()]

    def token(self, network, address):
        return GtToken(address, "BOOM", 18)

    def ohlcv(self, network, pool_address, timeframe, aggregate):
        return self.candles


class FakeCoinGecko:
    def platform_chain_ids(self):
        return {"base": 8453}


class FakeDirectory:
    def supported_chain_ids(self):
        return {8453}


class FakeZerion:
    def chains(self):
        return [ZerionChain("base", 8453, "https://mainnet.base.org/", "eth", "0xweth")]

    def chain_ids(self):
        return {chain.evm_id: chain.zerion_id for chain in self.chains()}


class FakeHyperSync:
    def __init__(self, transfers=None, error=None, page_size=2):
        self._transfers = transfers
        self._error = error
        self.page_size = page_size
        self.transfer_calls = []

    def height(self):
        return block_of(int(NOW.timestamp()))

    def block_timestamp(self, number):
        return GENESIS_TS + number * BLOCK_TIME

    def transfer_pages(self, token, from_block, to_block, senders=None):
        self.transfer_calls.append((token, from_block, to_block, senders))
        if self._error:
            raise self._error
        selected = [
            t
            for t in self._all_transfers()
            if from_block <= t.block < to_block and (senders is None or t.sender in senders)
        ]
        for start in range(0, len(selected), self.page_size):
            yield selected[start : start + self.page_size]

    def _all_transfers(self):
        if self._transfers is not None:
            return self._transfers
        start = int(POOL_CREATED.timestamp())
        low_ts = start + 10 * HOUR
        return [
            # Sniper : 1 bloc après la création du pool, 2 000 tokens à 1 $.
            self._buy(SNIPER, 501, 2000),
            # Alice : 1 000 tokens à 1 $, puis sort 400 tokens avant le pic.
            self._buy(ALICE, 600, 1000),
            Transfer(
                block_of(low_ts + 3 * HOUR), low_ts + 3 * HOUR, ALICE, ALICE, POOL, 400 * UNIT
            ),
            # Petit acheteur : 10 tokens → 10 $, ignoré.
            self._buy(SMALL, 700, 10),
            # Airdrop : le destinataire n'a pas signé, ignoré.
            Transfer(800, self.block_timestamp(800), SMALL, POOL, STRANGER, 5000 * UNIT),
        ]

    def _buy(self, wallet, block, tokens):
        return Transfer(block, self.block_timestamp(block), wallet, POOL, wallet, tokens * UNIT)
