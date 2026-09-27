from apps.wallets.services.entities import (
    ZERO_ADDRESS,
    big_receive_targets,
    transfer_after_buy_targets,
)
from apps.wallets.services.exchanges import forwarding
from apps.wallets.services.positions import TradeRecord
from integrations.hypersync import WalletTransfer

D = "0x" + "d" * 40
HOT = "0x" + "e" * 40
A = "0x" + "1" * 40
X = "0x" + "2" * 40


def tr(token, sender, recipient, amount, ts):
    return WalletTransfer(
        block=ts,
        timestamp=ts,
        tx_hash=f"0x{ts}",
        log_index=0,
        token=token,
        sender=sender,
        recipient=recipient,
        amount=amount,
        tx_from=sender,
        tx_to=token,
        tx_value=0,
    )


def test_deposit_forwards_everything_to_one_destination():
    transfers = [
        tr(A, "0xu1", D, 100, 0),
        tr(A, D, HOT, 100, 600),
        tr(X, "0xu2", D, 50, 1000),
        tr(X, D, HOT, 49, 2000),
    ]
    assert forwarding(transfers, D, hours=24, min_forward_pct=90) == (100.0, HOT)


def test_wallet_keeping_tokens_is_not_forwarding():
    transfers = [tr(A, "0xu1", D, 100, 0), tr(A, D, HOT, 10, 600)]
    assert forwarding(transfers, D, 24, 90) == (0.0, None)


def test_forward_outside_window_does_not_count():
    transfers = [tr(A, "0xu1", D, 100, 0), tr(A, D, HOT, 100, 2 * 86_400)]
    assert forwarding(transfers, D, 24, 90) == (0.0, None)


def test_no_reception():
    assert forwarding([], D, 24, 90) == (0.0, None)


def rec(kind, token, amount, counterparty="0xpool", chain=1):
    return TradeRecord(chain, token, kind, amount, None, 1, 1, counterparty)


def test_targets_receiving_most_of_a_token():
    records = [
        rec("buy", "0xa", 100),
        rec("send", "0xa", 80, "0xvault"),
        rec("send", "0xa", 5, "0xfriend"),
        rec("receive", "0xusdc", 1000),
        rec("send", "0xusdc", 700, "0xvault"),
        rec("buy", "0xb", 10),
        rec("sell", "0xb", 10),
        rec("send", "0xc", 5, "0xz"),
    ]
    assert transfer_after_buy_targets(records, 70) == {
        (1, "0xvault"): {"token": "0xa", "pct": 80.0}
    }


def usd_rec(kind, usd, counterparty, token="0xt"):
    return TradeRecord("base", token, kind, 1, usd, 1, 1, counterparty)


def test_big_receive_is_a_share_of_all_inflows():
    records = [
        usd_rec("buy", 1000.0, "0xpool"),
        usd_rec("receive", 3000.0, "0xbig"),
        usd_rec("receive", 50.0, "0xdust"),
        usd_rec("receive", 900.0, ZERO_ADDRESS),
        usd_rec("receive", None, "0xunknown"),
    ]
    assert big_receive_targets(records, 30) == {
        ("base", "0xbig"): {"pct": 60.61, "value_usd": 3000.0}
    }


def test_no_inflow_no_target():
    assert big_receive_targets([usd_rec("send", 10.0, "0xa")], 30) == {}
