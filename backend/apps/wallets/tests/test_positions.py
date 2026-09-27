from apps.wallets.services.positions import TradeRecord, aggregate_positions


def rec(kind, amount, ts, usd=None, token="0xt", chain=1):
    return TradeRecord(chain, token, kind, amount, usd, ts, ts, "0xc")


def test_aggregates_per_chain_and_token():
    stats = aggregate_positions(
        [
            rec("buy", 10, 5, usd=100.0),
            rec("buy", 5, 3),
            rec("sell", 4, 9, usd=60.0),
            rec("send", 2, 10),
            rec("receive", 1, 1),
            rec("buy", 7, 2, token="0xu"),
        ]
    )
    s = stats[(1, "0xt")]
    assert (s.bought, s.sold, s.sent, s.received) == (15, 4, 2, 1)
    assert (s.bought_usd, s.sold_usd, s.buys, s.sells) == (100.0, 60.0, 2, 1)
    assert (s.first_ts, s.last_ts) == (1, 10)
    assert stats[(1, "0xu")].bought == 7
