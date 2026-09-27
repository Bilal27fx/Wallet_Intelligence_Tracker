from decimal import Decimal

import pytest

from apps.discovery.models import PipelineSettings, Wallet
from apps.discovery.tests.factories import make_chain
from apps.wallets.models import QualificationSettings, WalletProfile
from apps.wallets.services.qualification import (
    Clients,
    decide_step,
    history_step,
    value_linked_step,
)
from apps.wallets.tests.factories import make_early_buy
from apps.wallets.tests.fakes import (
    BUYER,
    NOW,
    SENDER,
    TOKEN_A,
    VAULT,
    FakeWalletHyperSync,
    FakeZerion,
    buyer_zerion_history,
)
from integrations.errors import BudgetExhausted

pytestmark = pytest.mark.django_db


@pytest.fixture
def chain():
    return make_chain()


def decided(zerion=None, before_decide=None):
    zerion = zerion or FakeZerion()
    clients = Clients(lambda c: FakeWalletHyperSync(), zerion)
    p = WalletProfile.objects.create(
        wallet=Wallet.objects.create(address=BUYER), status="prefiltered"
    )
    history_step(p, clients, NOW)
    if before_decide:
        before_decide(p)
    decide_step(p, clients, NOW, PipelineSettings.load())
    return p, clients


def test_buyer_is_judged_with_its_linked_wallets(chain):
    p, clients = decided()
    assert (p.status, p.filter_reason, p.portfolio_value_usd) == (
        "filtered",
        "portfolio_too_small",
        Decimal("6150.00"),
    )
    vault = WalletProfile.objects.get(wallet__address=VAULT)
    assert value_linked_step(vault, clients, NOW) == "valued"
    p.refresh_from_db()
    assert (p.status, p.linked_value_usd) == ("qualified", Decimal("11350.00"))
    value_linked_step(WalletProfile.objects.get(wallet__address=SENDER), clients, NOW)
    p.refresh_from_db()
    assert (p.status, p.linked_value_usd) == ("qualified", Decimal("13350.00"))


def test_farmer_rechecked_on_zerion(chain):
    QualificationSettings.objects.filter(chain=None).update(max_distinct_tokens=3)
    p, _ = decided()
    assert p.filter_reason == "farmer"


def test_too_few_trades(chain):
    zerion = FakeZerion(histories={BUYER: buyer_zerion_history()[4:]})
    p, _ = decided(zerion)
    assert p.filter_reason == "too_few_trades"


def test_tags_use_early_buys(chain):
    p, _ = decided(
        before_decide=lambda p: make_early_buy(p.wallet, chain, TOKEN_A, is_sniper=True, sold=10)
    )
    assert p.tags == ["HOLDER", "SNIPER"]


def test_portfolio_budget_exhaustion_keeps_status(chain):
    zerion = FakeZerion()
    clients = Clients(lambda c: FakeWalletHyperSync(), zerion)
    p = WalletProfile.objects.create(
        wallet=Wallet.objects.create(address=BUYER), status="prefiltered"
    )
    history_step(p, clients, NOW)
    zerion.budget = sum(zerion.calls.values())
    with pytest.raises(BudgetExhausted):
        decide_step(p, clients, NOW, PipelineSettings.load())
    p.refresh_from_db()
    assert p.status == "history_fetched"


def test_linked_value_does_not_rescue_a_bot(chain):
    p, clients = decided()
    WalletProfile.objects.filter(pk=p.pk).update(filter_reason="bot_frequency")
    value_linked_step(WalletProfile.objects.get(wallet__address=VAULT), clients, NOW)
    p.refresh_from_db()
    assert (p.status, p.filter_reason) == ("filtered", "bot_frequency")
