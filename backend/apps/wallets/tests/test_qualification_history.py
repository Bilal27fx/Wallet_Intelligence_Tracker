from datetime import timedelta
from decimal import Decimal

import pytest

from apps.discovery.models import PipelineSettings, Wallet
from apps.discovery.tests.factories import make_chain
from apps.wallets.models import (
    KnownAddress,
    QualificationSettings,
    TokenPosition,
    TokenTrade,
    WalletProfile,
)
from apps.wallets.services.pricing import sync_quote_assets
from apps.wallets.services.qualification import Clients, history_step, prefilter_step
from apps.wallets.tests.fakes import (
    BUYER,
    NOW,
    TOKEN_A,
    TOKEN_B,
    UNIT,
    USDC,
    VAULT,
    FakeRpc,
    FakeWalletHyperSync,
    FakeZerion,
    buyer_history,
)

pytestmark = pytest.mark.django_db


@pytest.fixture
def chain():
    chain = make_chain(
        native_fungible_id="eth", wrapped_fungible_id="0xweth-id", rpc_url="https://rpc.test/"
    )
    sync_quote_assets(FakeZerion(), PipelineSettings.load())
    return chain


def make_profile(chain, address=BUYER, **overrides):
    wallet, _ = Wallet.objects.get_or_create(address=address)
    return WalletProfile.objects.create(wallet=wallet, chains=[chain.pk], **overrides)


def make_clients(hypersync=None, zerion=None):
    hs = hypersync or FakeWalletHyperSync()
    return Clients(
        hypersync_for=lambda c: hs,
        zerion=zerion or FakeZerion(),
        rpc_for=lambda c: FakeRpc({BUYER: UNIT, VAULT: 5 * UNIT}),
    )


def test_prefilter_passes_active_wallet(chain):
    profile = make_profile(chain)
    assert prefilter_step(profile, make_clients(), NOW) == "prefiltered"
    assert profile.metrics["prefilter"]["base"] == {"txs_7d": 10, "txs_active": 5}


def test_prefilter_filters_bots_and_schedules_recheck(chain):
    profile = make_profile(chain)
    status = prefilter_step(
        profile, make_clients(FakeWalletHyperSync(tx_counts={BUYER: 5000})), NOW
    )
    assert (status, profile.filter_reason) == ("filtered", "bot_frequency")
    assert profile.next_analysis_at == NOW + timedelta(days=30)


def test_prefilter_known_exchange(chain):
    KnownAddress.objects.create(chain=None, address=BUYER, kind="exchange")
    profile = make_profile(chain)
    prefilter_step(profile, make_clients(), NOW)
    assert profile.filter_reason == "exchange"


def test_inactive_only_filters_early_buyers(chain):
    quiet = FakeWalletHyperSync(tx_counts={BUYER: 2})
    early = make_profile(chain)
    assert prefilter_step(early, make_clients(quiet), NOW) == "filtered"
    linked = make_profile(chain, address=VAULT, source="linked", depth=1)
    assert (
        prefilter_step(linked, make_clients(FakeWalletHyperSync(tx_counts={VAULT: 0})), NOW)
        == "prefiltered"
    )


def test_history_saves_priced_trades_and_positions(chain):
    profile = make_profile(chain, status="prefiltered")
    assert history_step(profile, make_clients(), NOW) == "history_fetched"
    assert TokenTrade.objects.filter(wallet=profile.wallet).count() == 9
    buy_a = TokenTrade.objects.get(wallet=profile.wallet, token__address=TOKEN_A, kind="buy")
    buy_b = TokenTrade.objects.get(wallet=profile.wallet, token__address=TOKEN_B, kind="buy")
    assert (buy_a.usd, buy_b.usd) == (Decimal("1000.00"), Decimal("1000.00"))
    assert TokenTrade.objects.get(wallet=profile.wallet, kind="send").usd is None
    a = TokenPosition.objects.get(wallet=profile.wallet, token__address=TOKEN_A)
    assert (a.bought_amount, a.sent_amount, a.balance) == (500 * UNIT, 450 * UNIT, 50 * UNIT)
    usdc = TokenPosition.objects.get(wallet=profile.wallet, token__address=USDC)
    assert (usdc.received_amount, usdc.sold_amount, usdc.sold_usd) == (
        5000 * 10**6,
        1600 * 10**6,
        Decimal("1600.00"),
    )


def test_history_is_idempotent(chain):
    profile = make_profile(chain, status="prefiltered")
    history_step(profile, make_clients(), NOW)
    profile.status = "prefiltered"
    history_step(profile, make_clients(), NOW)
    assert TokenTrade.objects.filter(wallet=profile.wallet).count() == 9
    assert TokenPosition.objects.filter(wallet=profile.wallet).count() == 4


def test_history_filters_farmers(chain):
    QualificationSettings.objects.filter(chain=None).update(max_distinct_tokens=2)
    profile = make_profile(chain, status="prefiltered")
    assert history_step(profile, make_clients(), NOW) == "filtered"
    assert profile.filter_reason == "farmer"
    assert not TokenTrade.objects.exists()


def test_history_filters_occasional_traders(chain):
    hs = FakeWalletHyperSync(transfers={BUYER: buyer_history()[:3]})
    profile = make_profile(chain, status="prefiltered")
    history_step(profile, make_clients(hs), NOW)
    assert profile.filter_reason == "too_few_trades"
