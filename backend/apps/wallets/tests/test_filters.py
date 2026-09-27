import pytest

from apps.wallets.services.filters import (
    ChainActivity,
    farmer_reason,
    history_reason,
    mev_ratio,
    prefilter_reason,
)
from apps.wallets.services.positions import TradeRecord
from apps.wallets.tests.factories import make_thresholds

T = make_thresholds()


def rec(kind, token, block):
    return TradeRecord(1, token, kind, 1, None, block, block, "0xc")


@pytest.mark.parametrize(
    ("activities", "source", "reason"),
    [
        ([ChainActivity(10, 10)], "early_buyer", None),
        ([ChainActivity(1401, 10)], "early_buyer", "bot_frequency"),
        ([ChainActivity(1401, 10)], "linked", "bot_frequency"),
        ([ChainActivity(3, 2)], "early_buyer", "inactive"),
        ([ChainActivity(3, 2)], "linked", None),
        ([ChainActivity(3, 2), ChainActivity(9, 9)], "early_buyer", None),
    ],
)
def test_prefilter(activities, source, reason):
    assert prefilter_reason([(a, T) for a in activities], source) == reason


def test_prefilter_uses_each_chain_threshold():
    strict = make_thresholds(max_txs_per_day=1)
    assert prefilter_reason([(ChainActivity(10, 10), strict)], "early_buyer") == "bot_frequency"


def test_farmer():
    assert farmer_reason(301, T) == "farmer"
    assert farmer_reason(300, T) is None


def test_mev_ratio_counts_same_block_round_trips_excluding_quotes():
    records = [
        rec("buy", "0xa", 1),
        rec("sell", "0xa", 1),
        rec("buy", "0xb", 2),
        rec("sell", "0xb", 3),
        rec("buy", "0xusdc", 4),
        rec("sell", "0xusdc", 4),
    ]
    assert mev_ratio(records, {"0xusdc"}) == 50.0


def test_history_reason():
    three = [rec("buy", t, i) for i, t in enumerate(["0xa", "0xb", "0xc"])]
    assert history_reason(three, set(), T, "early_buyer") is None
    assert history_reason(three[:2], set(), T, "early_buyer") == "too_few_trades"
    assert history_reason(three[:2], set(), T, "linked") is None
    bots = three + [rec("sell", t, i) for i, t in enumerate(["0xa", "0xb", "0xc"])]
    assert history_reason(bots, set(), T, "early_buyer") == "bot_mev"
