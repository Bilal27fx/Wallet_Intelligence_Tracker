from apps.discovery.services.buyers import ZERO_ADDRESS, BuyerAggregator
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


def sell(wallet: str, block: int, tokens: int) -> Transfer:
    return Transfer(
        block=block,
        timestamp=block,
        tx_from=wallet,
        sender=wallet,
        recipient=POOL,
        amount=tokens * UNIT,
    )


def run(transfers, sells=(), **overrides):
    params = dict(
        pool_created_block=100, sniper_blocks=3, min_buy_usd=500, max_buyers=300, trough_ts=10
    )
    params.update(overrides)
    aggregator = BuyerAggregator(candles=CANDLES, decimals=18)
    for transfer in transfers:
        aggregator.add([transfer])  # une page par transfert
    kept = aggregator.select(**params)
    aggregator.add_sells(list(sells))
    return kept


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


def test_buys_accumulate_across_pages():
    [alice] = run([buy(ALICE, 200, 300), buy(ALICE, 300, 300)])
    assert (alice.first_buy_block, alice.bought_amount) == (200, 600 * UNIT)


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


def test_sells_before_trough_reduce_the_position():
    [alice] = run([buy(ALICE, 200, 600), sell(ALICE, 250, 100)])
    assert (alice.held_amount, alice.held_usd, alice.sold_amount) == (500 * UNIT, 500.0, 0)
    assert alice.bought_usd == 600.0


def test_trader_who_sold_everything_before_trough_is_dropped():
    trader = [buy(ALICE, 200, 50_000), sell(ALICE, 300, 50_000)]
    [friend] = run(trader + [buy(FRIEND, 400, 600)])
    assert friend.wallet == FRIEND


def test_position_is_valued_at_trough_price():
    # Acheté 300 tokens à 1 $, le creux est à 2 $ : position de 600 $.
    [alice] = run([buy(ALICE, 200, 300)], trough_ts=1500)
    assert (alice.bought_usd, alice.held_usd) == (300.0, 600.0)


def test_ranked_by_position_not_volume():
    churner = [buy(ALICE, 200, 5000), sell(ALICE, 210, 4900)]
    holder = [buy(FRIEND, 300, 1000)]
    assert [b.wallet for b in run(churner + holder)] == [FRIEND]
    assert [b.wallet for b in run(churner + holder, min_buy_usd=50)] == [FRIEND, ALICE]


def test_second_pass_sells_update_kept_buyers_only():
    [alice] = run([buy(ALICE, 200, 600)], sells=[sell(ALICE, 1500, 200), sell(FRIEND, 1500, 50)])
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
