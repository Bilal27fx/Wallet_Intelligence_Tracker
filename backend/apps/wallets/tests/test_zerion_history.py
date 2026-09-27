from datetime import UTC, datetime
from decimal import Decimal

import pytest

from apps.wallets.services.zerion_history import counterparty, is_quote, movements, trade_kind
from integrations.zerion import NATIVE, ZerionTransaction, ZerionTransfer

W = "0x" + "a" * 40


def zt(index, direction, token="0x" + "1" * 40, symbol="TOK", sender="0xs", recipient=W):
    return ZerionTransfer(
        index=index,
        chain="base",
        token_address=token,
        token_symbol=symbol,
        token_decimals=18,
        fungible_id="f",
        direction=direction,
        amount=10**18,
        quantity=Decimal(1),
        price_usd=1.0,
        value_usd=1.0,
        sender=sender,
        recipient=recipient,
    )


def tx(operation_type, transfers, status="confirmed"):
    return ZerionTransaction(
        zerion_id="z",
        chain="base",
        tx_hash="0xt",
        block=1,
        mined_at=datetime(2026, 9, 1, tzinfo=UTC),
        operation_type=operation_type,
        status=status,
        fee_usd=None,
        transfers=transfers,
        raw={},
    )


@pytest.mark.parametrize(
    ("operation", "direction", "has_in", "has_out", "kind"),
    [
        ("trade", "in", True, True, "buy"),
        ("trade", "out", True, True, "sell"),
        ("send", "out", False, True, "send"),
        ("receive", "in", True, False, "receive"),
        ("execute", "in", True, True, "buy"),
        ("execute", "out", False, True, "send"),
        ("mint", "in", True, False, "receive"),
        ("burn", "out", False, True, "send"),
        ("claim", "in", True, False, "receive"),
        ("trade", "self", True, True, None),
    ],
)
def test_trade_kind(operation, direction, has_in, has_out, kind):
    assert trade_kind(operation, direction, has_in, has_out) == kind


def test_movements_of_a_trade():
    rows = movements(tx("trade", [zt(0, "in"), zt(1, "out", token=NATIVE, symbol="ETH")]))
    assert [(t.index, kind) for t, kind in rows] == [(0, "buy"), (1, "sell")]


def test_failed_transactions_have_no_movement():
    assert movements(tx("trade", [zt(0, "in")], status="failed")) == []


def test_counterparty_depends_on_direction():
    assert counterparty(zt(0, "in", sender="0xfrom")) == "0xfrom"
    assert counterparty(zt(0, "out", recipient="0xto")) == "0xto"


def test_is_quote():
    assert is_quote("usdc", "0x1", ["USDC"])
    assert is_quote("ANY", NATIVE, [])
    assert not is_quote("PEPE", "0x1", ["USDC"])
