from apps.wallets.services.positions import TradeRecord
from apps.wallets.services.tags import (
    ACCUMULATEUR,
    EARLY_BUYER,
    FLIPPER,
    HOLDER,
    SNIPER,
    EarlyBuy,
    wallet_tags,
)
from apps.wallets.tests.factories import make_thresholds

T = make_thresholds()
H = 3600


def rec(kind, token, amount, ts):
    return TradeRecord(1, token, kind, amount, None, ts, ts, "0xp")


def eb(sniper=False, bought=100, sold=0):
    return EarlyBuy(token="0xx", chain_id=1, is_sniper=sniper, bought=bought, sold_before_peak=sold)


def test_sniper_early_buyer_holder():
    tags = wallet_tags([], [eb(sniper=True, sold=40), eb(sold=90)], set(), T)
    assert tags == sorted([SNIPER, EARLY_BUYER, HOLDER])


def test_not_holder_when_mostly_sold():
    assert HOLDER not in wallet_tags([], [eb(sold=60)], set(), T)


def test_flipper_when_most_positions_sold_fast():
    records = [
        rec("buy", "0xa", 100, 0),
        rec("sell", "0xa", 85, 2 * H),
        rec("buy", "0xb", 100, 0),
        rec("sell", "0xb", 90, 5 * H),
        rec("buy", "0xc", 100, 0),
        rec("sell", "0xc", 100, 48 * H),
    ]
    assert FLIPPER in wallet_tags(records, [], set(), T)


def test_not_flipper_when_slow():
    records = [rec("buy", "0xa", 100, 0), rec("sell", "0xa", 100, 48 * H)]
    assert FLIPPER not in wallet_tags(records, [], set(), T)


def test_accumulator_needs_enough_positions():
    one = [rec("buy", "0xa", 50, 0), rec("buy", "0xa", 50, H), rec("sell", "0xa", 10, 2 * H)]
    two = one + [rec("buy", "0xb", 50, 0), rec("buy", "0xb", 50, H)]
    assert ACCUMULATEUR not in wallet_tags(one, [], set(), T)
    assert ACCUMULATEUR in wallet_tags(two, [], set(), T)


def test_quote_tokens_are_ignored():
    records = [rec("buy", "0xusdc", 100, 0), rec("sell", "0xusdc", 100, H)]
    assert wallet_tags(records, [], {"0xusdc"}, T) == []
