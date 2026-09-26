from apps.discovery.services.buyers import ZERO_ADDRESS, aggregate_buyers
from integrations.geckoterminal import Candle
from integrations.hypersync import Transfer

UNIT = 10**18
POOL = "0x" + "b" * 40
ROUTER = "0x" + "9" * 40
ALICE = "0x" + "a" * 40
SNIPER = "0x" + "5" * 40
SMALL = "0x" + "c" * 40
AIRDROPPED = "0x" + "d" * 40
FRIEND = "0x" + "e" * 40

# Prix 1 $ jusqu'à t=1000, puis 2 $.
CANDLES = [Candle(0, 1, 1, 1, 1.0, 0), Candle(1000, 2, 2, 2, 2.0, 0)]


def buy(wallet: str, block: int, tokens: int, ts: int = 10, sender: str = POOL) -> Transfer:
    return Transfer(
        block=block,
        timestamp=ts,
        tx_from=wallet,
        sender=sender,
        recipient=wallet,
        amount=tokens * UNIT,
    )


def run(transfers, **overrides):
    params = dict(
        low_block=1000,
        peak_block=2000,
        pool_created_block=100,
        candles=CANDLES,
        decimals=18,
        sniper_blocks=3,
        min_buy_usd=500,
        max_buyers=300,
    )
    params.update(overrides)
    return aggregate_buyers(transfers, **params)


def test_buy_is_signer_receiving_tokens():
    [alice] = run([buy(ALICE, 200, 600)])
    assert alice.wallet == ALICE
    assert alice.first_buy_block == 200
    assert alice.bought_amount == 600 * UNIT
    assert alice.bought_usd == 600.0
    assert not alice.is_sniper


def test_buys_through_router_count():
    assert run([buy(ALICE, 200, 600, sender=ROUTER)])[0].wallet == ALICE


def test_usd_uses_candle_price_at_buy_time():
    [alice] = run([buy(ALICE, 200, 300, ts=10), buy(ALICE, 900, 300, ts=1500)])
    assert alice.bought_usd == 300 * 1.0 + 300 * 2.0


def test_buys_after_low_block_are_not_early():
    assert run([buy(ALICE, 1001, 10_000)]) == []


def test_airdrops_and_mints_are_ignored():
    airdrop = Transfer(
        block=200,
        timestamp=10,
        tx_from=FRIEND,
        sender=FRIEND,
        recipient=AIRDROPPED,
        amount=10_000 * UNIT,
    )
    mint = Transfer(
        block=200,
        timestamp=10,
        tx_from=ALICE,
        sender=ZERO_ADDRESS,
        recipient=ALICE,
        amount=10_000 * UNIT,
    )
    assert run([airdrop, mint]) == []


def test_tokens_sent_before_peak_count_as_sold():
    sell = Transfer(
        block=1500, timestamp=1500, tx_from=ALICE, sender=ALICE, recipient=POOL, amount=200 * UNIT
    )
    after_peak = Transfer(
        block=2500, timestamp=2500, tx_from=ALICE, sender=ALICE, recipient=POOL, amount=100 * UNIT
    )
    [alice] = run([buy(ALICE, 200, 600), sell, after_peak])
    assert alice.sold_amount == 200 * UNIT


def test_sniper_flag():
    [sniper] = run([buy(SNIPER, 103, 1000)])
    assert sniper.is_sniper


def test_small_buyers_are_dropped():
    assert run([buy(SMALL, 200, 10)]) == []


def test_keeps_biggest_buyers_up_to_cap():
    buyers = run(
        [buy(ALICE, 200, 600), buy(SNIPER, 300, 2000), buy(FRIEND, 400, 900)], max_buyers=2
    )
    assert [b.wallet for b in buyers] == [SNIPER, FRIEND]


def test_zero_cap_keeps_everyone_above_threshold():
    buyers = run(
        [buy(ALICE, 200, 600), buy(SNIPER, 300, 2000), buy(FRIEND, 400, 900)], max_buyers=0
    )
    assert len(buyers) == 3
