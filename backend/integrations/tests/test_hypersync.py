from types import SimpleNamespace

import pytest

from integrations.errors import TooManyTransfers
from integrations.hypersync import TRANSFER_TOPIC, HyperSyncClient, Transfer
from integrations.ratelimit import NoopLimiter

ALICE = "0x" + "a" * 40
POOL = "0x" + "b" * 40
BOB = "0x" + "c" * 40


def topic(address: str) -> str:
    return "0x" + "0" * 24 + address[2:]


def log(block: int, tx: str, sender: str, recipient: str, amount: int):
    return SimpleNamespace(
        block_number=block,
        transaction_hash=tx,
        data=hex(amount),
        topics=[TRANSFER_TOPIC, topic(sender), topic(recipient)],
    )


def page(next_block: int, logs=(), txs=(), blocks=()):
    return SimpleNamespace(
        next_block=next_block,
        data=SimpleNamespace(logs=list(logs), transactions=list(txs), blocks=list(blocks)),
    )


class FakeInner:
    def __init__(self, pages, height: int = 0):
        self.pages = list(pages)
        self.height = height
        self.from_blocks: list[int] = []

    async def get(self, query):
        self.from_blocks.append(query.from_block)
        return self.pages.pop(0)

    async def get_height(self):
        return self.height


def make_client(inner: FakeInner) -> HyperSyncClient:
    return HyperSyncClient(1, "token", NoopLimiter(), inner=inner)


def test_height():
    assert make_client(FakeInner([], height=123)).height() == 123


def test_block_timestamp_parses_hex_and_caches():
    inner = FakeInner([page(6, blocks=[SimpleNamespace(number=5, timestamp="0x10")])])
    client = make_client(inner)
    assert client.block_timestamp(5) == 16
    assert client.block_timestamp(5) == 16
    assert inner.from_blocks == [5]


def test_transfers_follows_pagination_and_joins_tx_sender():
    inner = FakeInner(
        [
            page(
                150,
                logs=[log(100, "0xt1", POOL, ALICE, 1000)],
                txs=[SimpleNamespace(hash="0xt1", from_=ALICE.upper().replace("0X", "0x"))],
                blocks=[SimpleNamespace(number=100, timestamp="0x64")],
            ),
            page(
                200,
                logs=[
                    log(160, "0xt2", ALICE, BOB, 400),
                    SimpleNamespace(
                        block_number=161,
                        transaction_hash="0xt3",
                        data="0x",
                        topics=[TRANSFER_TOPIC, topic(ALICE), topic(BOB), topic(BOB)],
                    ),
                ],
                txs=[
                    SimpleNamespace(hash="0xt2", from_=ALICE),
                    SimpleNamespace(hash="0xt3", from_=ALICE),
                ],
                blocks=[
                    SimpleNamespace(number=160, timestamp=200),
                    SimpleNamespace(number=161, timestamp=202),
                ],
            ),
        ]
    )
    transfers = make_client(inner).transfers("0xtoken", 50, 200, max_transfers=10)
    assert transfers == [
        Transfer(
            block=100, timestamp=100, tx_from=ALICE, sender=POOL, recipient=ALICE, amount=1000
        ),
        Transfer(block=160, timestamp=200, tx_from=ALICE, sender=ALICE, recipient=BOB, amount=400),
    ]
    assert inner.from_blocks == [50, 150]


def test_transfers_raises_when_over_cap():
    inner = FakeInner(
        [
            page(
                200,
                logs=[log(100, "0xt1", POOL, ALICE, 1), log(101, "0xt1", POOL, ALICE, 1)],
                txs=[SimpleNamespace(hash="0xt1", from_=ALICE)],
                blocks=[
                    SimpleNamespace(number=100, timestamp=1),
                    SimpleNamespace(number=101, timestamp=2),
                ],
            )
        ]
    )
    with pytest.raises(TooManyTransfers):
        make_client(inner).transfers("0xtoken", 0, 200, max_transfers=1)


def test_transfers_accepts_real_topics_padding():
    # HyperSync renvoie toujours 4 topics, complétés par None.
    padded = log(100, "0xt1", POOL, ALICE, 7)
    padded.topics = padded.topics + [None]
    inner = FakeInner(
        [
            page(
                200,
                logs=[padded],
                txs=[SimpleNamespace(hash="0xt1", from_=ALICE)],
                blocks=[SimpleNamespace(number=100, timestamp=1)],
            )
        ]
    )
    [transfer] = make_client(inner).transfers("0xtoken", 0, 200, max_transfers=10)
    assert transfer.amount == 7
