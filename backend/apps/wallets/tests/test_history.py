from decimal import Decimal

import pytest

from apps.discovery.models import Wallet
from apps.discovery.tests.factories import make_chain
from apps.wallets.models import TokenPosition, TokenTrade, WalletProfile, WalletTransaction
from apps.wallets.services.qualification import Clients, history_step
from apps.wallets.tests.fakes import BUYER, NOW, TOKEN_A, USDC, FakeWalletHyperSync, FakeZerion
from integrations.errors import BudgetExhausted
from integrations.zerion import NATIVE

pytestmark = pytest.mark.django_db


@pytest.fixture
def profile():
    make_chain()
    return WalletProfile.objects.create(
        wallet=Wallet.objects.create(address=BUYER), status="prefiltered"
    )


def run(p, zerion):
    return history_step(p, Clients(lambda c: FakeWalletHyperSync(), zerion), NOW)


def test_full_history_is_stored_with_zerion_data(profile):
    zerion = FakeZerion()
    assert run(profile, zerion) == "history_fetched"
    assert zerion.calls["transactions"] == 2
    assert profile.history_complete and profile.history_cursor == ""
    assert WalletTransaction.objects.filter(wallet=profile.wallet).count() == 6
    buy = TokenTrade.objects.get(wallet=profile.wallet, token_address=TOKEN_A, kind="buy")
    assert (buy.direction, buy.token_symbol, buy.value_usd, buy.quantity) == (
        "in",
        "A",
        Decimal("1000.00"),
        Decimal("500"),
    )
    assert buy.transaction.operation_type == "trade"
    eth = TokenTrade.objects.get(wallet=profile.wallet, token_address=NATIVE)
    assert (eth.kind, eth.value_usd) == ("sell", Decimal("1000.00"))
    assert TokenTrade.objects.get(wallet=profile.wallet, kind="send").counterparty != ""
    a = TokenPosition.objects.get(wallet=profile.wallet, token_address=TOKEN_A)
    assert (a.bought_amount, a.sent_amount, a.bought_usd) == (
        500 * 10**18,
        450 * 10**18,
        Decimal("1000.00"),
    )
    usdc = TokenPosition.objects.get(wallet=profile.wallet, token_address=USDC)
    assert usdc.received_amount == 5000 * 10**6
    assert profile.last_mined_at == WalletTransaction.objects.latest("mined_at").mined_at


def test_budget_exhaustion_keeps_cursor_and_resumes(profile):
    with pytest.raises(BudgetExhausted):
        run(profile, FakeZerion(budget=1))
    profile.refresh_from_db()
    assert (profile.status, profile.history_cursor, profile.history_complete) == (
        "prefiltered",
        "3",
        False,
    )
    assert WalletTransaction.objects.count() == 3
    resumed = FakeZerion()
    run(profile, resumed)
    assert resumed.calls["transactions"] == 1
    assert WalletTransaction.objects.count() == 6


def test_history_is_idempotent(profile):
    run(profile, FakeZerion())
    profile.status = "prefiltered"
    profile.history_complete = False
    run(profile, FakeZerion())
    assert TokenTrade.objects.count() == 10
