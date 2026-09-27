from types import SimpleNamespace

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
        self.queries = []

    async def get(self, query):
        self.from_blocks.append(query.from_block)
        self.queries.append(query)
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


def collect(client, *args, **kwargs):
    return [list(page) for page in client.transfer_pages(*args, **kwargs)]


def test_transfer_pages_follow_pagination_and_join_tx_sender():
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
    pages = collect(make_client(inner), "0xtoken", 50, 200)
    assert pages == [
        [
            Transfer(
                block=100, timestamp=100, tx_from=ALICE, sender=POOL, recipient=ALICE, amount=1000
            )
        ],
        [
            Transfer(
                block=160, timestamp=200, tx_from=ALICE, sender=ALICE, recipient=BOB, amount=400
            )
        ],
    ]
    assert inner.from_blocks == [50, 150]
    assert inner.queries[0].logs[0].topics == [[TRANSFER_TOPIC]]


def test_transfer_pages_filter_on_senders():
    inner = FakeInner([page(200)])
    collect(make_client(inner), "0xtoken", 0, 200, senders=[ALICE, BOB])
    assert inner.queries[0].logs[0].topics == [[TRANSFER_TOPIC], [topic(ALICE), topic(BOB)]]


def test_transfer_pages_empty_range_makes_no_call():
    inner = FakeInner([])
    assert collect(make_client(inner), "0xtoken", 200, 200) == []
    assert inner.from_blocks == []


def test_transfer_pages_accept_real_topics_padding():
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
    [[transfer]] = collect(make_client(inner), "0xtoken", 0, 200)
    assert transfer.amount == 7
