from types import SimpleNamespace

from integrations.hypersync import (
    TRANSFER_TOPIC,
    Funding,
    HyperSyncClient,
    WalletTransfer,
    address_topic,
)
from integrations.ratelimit import NoopLimiter

W = "0x" + "a" * 40
POOL = "0x" + "b" * 40
ROUTER = "0x" + "9" * 40
TOKEN = "0x" + "1" * 40


def page(next_block, logs=(), txs=(), blocks=()):
    return SimpleNamespace(
        next_block=next_block,
        data=SimpleNamespace(logs=list(logs), transactions=list(txs), blocks=list(blocks)),
    )


class FakeInner:
    def __init__(self, pages):
        self.pages = list(pages)
        self.queries = []

    async def get(self, query):
        self.queries.append(query)
        return self.pages.pop(0)

    async def get_height(self):
        return 1000


def tx(hash_="0xt", from_=W, to=ROUTER, value="0x0", block=10):
    return SimpleNamespace(hash=hash_, from_=from_, to=to, value=value, block_number=block)


def make(pages) -> tuple[HyperSyncClient, FakeInner]:
    inner = FakeInner(pages)
    return HyperSyncClient(1, "t", NoopLimiter(), inner=inner), inner


def test_address_topic():
    assert address_topic("0xABC" + "0" * 37) == "0x" + "0" * 24 + "abc" + "0" * 37


def test_tx_count_stops_at_cap():
    client, inner = make([page(100, txs=[tx()] * 3), page(200, txs=[tx()] * 3), page(300)])
    assert client.wallet_tx_count(W, 0, 300, cap=5) == 6
    assert len(inner.queries) == 2


def test_tx_count_reads_all_pages_under_cap():
    client, _ = make([page(100, txs=[tx()] * 2), page(300, txs=[tx()])])
    assert client.wallet_tx_count(W, 0, 300, cap=50) == 3


def test_distinct_counterparties_ignores_self_and_stops_at_cap():
    client, _ = make(
        [
            page(100, txs=[tx(to="0x1"), tx(to="0x1"), tx(from_="0x2", to=W)]),
            page(200, txs=[tx(to="0x3")]),
            page(300, txs=[tx(to="0x4")]),
        ]
    )
    assert client.distinct_counterparties(W, 0, 300, cap=3) == 3


def test_first_funding_is_first_transaction_with_value():
    client, _ = make(
        [
            page(
                500,
                txs=[
                    tx(from_="0xfunder2", to=W, value="0x5", block=40),
                    tx(from_="0xspam", to=W, value="0x0", block=20),
                    tx(from_="0xFUNDER", to=W, value="0xa", block=30),
                ],
            )
        ]
    )
    assert client.first_funding(W, 500) == Funding("0xfunder", 30, 10)


def test_first_funding_none():
    client, _ = make([page(500)])
    assert client.first_funding(W, 500) is None


def test_wallet_transfers_joins_transaction_and_block():
    log = SimpleNamespace(
        block_number=10,
        log_index=3,
        transaction_hash="0xt",
        address=TOKEN.upper().replace("0X", "0x"),
        data=hex(500),
        topics=[TRANSFER_TOPIC, address_topic(POOL), address_topic(W), None],
    )
    nft = SimpleNamespace(
        block_number=10,
        log_index=4,
        transaction_hash="0xt",
        address=TOKEN,
        data="0x",
        topics=[TRANSFER_TOPIC, address_topic(POOL), address_topic(W), address_topic(W)],
    )
    client, inner = make(
        [
            page(
                1000,
                logs=[log, nft],
                txs=[tx(value="0x10")],
                blocks=[SimpleNamespace(number=10, timestamp="0x64")],
            )
        ]
    )
    assert client.wallet_transfers(W, 0, 1000) == [
        WalletTransfer(
            block=10,
            timestamp=100,
            tx_hash="0xt",
            log_index=3,
            token=TOKEN,
            sender=POOL,
            recipient=W,
            amount=500,
            tx_from=W,
            tx_to=ROUTER,
            tx_value=16,
        )
    ]
    [selection_from, selection_to] = inner.queries[0].logs
    assert selection_from.topics == [[TRANSFER_TOPIC], [address_topic(W)]]
    assert selection_to.topics == [[TRANSFER_TOPIC], [], [address_topic(W)]]
