import pytest

from apps.wallets.services.classify import BUY, RECEIVE, SELL, SEND, classify, classify_all
from integrations.hypersync import WalletTransfer

W = "0x" + "a" * 40
POOL = "0x" + "b" * 40
ROUTER = "0x" + "9" * 40
TOKEN = "0x" + "1" * 40
OTHER = "0x" + "c" * 40


def tr(sender, recipient, tx_from, tx_to):
    return WalletTransfer(
        block=1,
        timestamp=1,
        tx_hash="0xt",
        log_index=0,
        token=TOKEN,
        sender=sender,
        recipient=recipient,
        amount=5,
        tx_from=tx_from,
        tx_to=tx_to,
        tx_value=0,
    )


@pytest.mark.parametrize(
    ("transfer", "kind", "counterparty"),
    [
        (tr(POOL, W, W, ROUTER), BUY, POOL),
        (tr(W, POOL, W, ROUTER), SELL, POOL),
        (tr(W, OTHER, W, TOKEN), SEND, OTHER),
        (tr(OTHER, W, OTHER, TOKEN), RECEIVE, OTHER),
        (tr(POOL, W, OTHER, ROUTER), RECEIVE, POOL),
        (tr(W, OTHER, OTHER, ROUTER), SEND, OTHER),
    ],
)
def test_classify(transfer, kind, counterparty):
    trade = classify(transfer, W.upper().replace("0X", "0x"))
    assert (trade.kind, trade.counterparty) == (kind, counterparty)


def test_self_and_unrelated_transfers_are_ignored():
    assert classify(tr(W, W, W, TOKEN), W) is None
    assert classify(tr(POOL, OTHER, W, ROUTER), W) is None
    assert len(classify_all([tr(W, W, W, TOKEN), tr(POOL, W, W, ROUTER)], W)) == 1
