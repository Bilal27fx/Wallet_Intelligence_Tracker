from decimal import Decimal

import pytest

from apps.discovery.models import PipelineSettings, Wallet
from apps.discovery.tests.factories import make_chain
from apps.wallets.models import KnownAddress, QualificationSettings, WalletLink, WalletProfile
from apps.wallets.services.pricing import sync_quote_assets
from apps.wallets.services.qualification import Clients, qualify_wallet
from apps.wallets.tests.factories import make_early_buy
from apps.wallets.tests.fakes import (
    BUYER,
    DEPOSIT,
    FUNDER,
    HOT,
    NOW,
    TOKEN_A,
    UNIT,
    VAULT,
    FakeRpc,
    FakeWalletHyperSync,
    FakeZerion,
    buyer_history,
    deposit_history,
)
from integrations.errors import BudgetExhausted

pytestmark = pytest.mark.django_db


@pytest.fixture
def chain():
    chain = make_chain(
        native_fungible_id="eth", wrapped_fungible_id="0xweth-id", rpc_url="https://rpc.test/"
    )
    sync_quote_assets(FakeZerion(), PipelineSettings.load())
    return chain


def profile_for(chain, address=BUYER):
    wallet, _ = Wallet.objects.get_or_create(address=address)
    profile, _ = WalletProfile.objects.get_or_create(wallet=wallet, defaults={"chains": [chain.pk]})
    return profile


def clients(hypersync=None, zerion=None):
    hs = hypersync or FakeWalletHyperSync()
    return Clients(
        hypersync_for=lambda c: hs,
        zerion=zerion or FakeZerion(),
        rpc_for=lambda c: FakeRpc({BUYER: UNIT, VAULT: 5 * UNIT}),
    )


def test_buying_wallet_is_judged_with_its_vault(chain):
    buyer = profile_for(chain)
    assert qualify_wallet(buyer, clients(), NOW) == "filtered"
    assert buyer.filter_reason == "portfolio_too_small"
    assert buyer.portfolio_value_usd == Decimal("6150.00")
    assert WalletLink.objects.filter(
        from_wallet__address=BUYER, to_wallet__address=VAULT, kind="transfer_after_buy"
    ).exists()
    assert WalletLink.objects.filter(
        from_wallet__address=FUNDER, to_wallet__address=BUYER, kind="funding"
    ).exists()
    vault = WalletProfile.objects.get(wallet__address=VAULT)
    assert (vault.source, vault.depth, vault.status) == ("linked", 1, "pending")

    assert qualify_wallet(vault, clients(), NOW) == "qualified"
    buyer.refresh_from_db()
    assert buyer.status == "qualified"
    assert buyer.entity_id == vault.entity_id
    assert buyer.entity.portfolio_value_usd == Decimal("17500.00")
    assert buyer.entity.profiles.count() == 3


def test_send_to_forwarding_deposit_creates_no_link(chain):
    hs = FakeWalletHyperSync(
        transfers={BUYER: buyer_history(send_to=DEPOSIT), DEPOSIT: deposit_history()},
        tx_counts={DEPOSIT: 3},
        counterparties={HOT: 5000},
    )
    qualify_wallet(profile_for(chain), clients(hs), NOW)
    assert not WalletLink.objects.filter(to_wallet__address=DEPOSIT).exists()
    assert KnownAddress.objects.get(address=DEPOSIT).kind == "cex_deposit"
    assert KnownAddress.objects.get(address=HOT).kind == "exchange"


def test_never_signing_relay_is_a_deposit(chain):
    hs = FakeWalletHyperSync(
        transfers={BUYER: buyer_history(send_to=DEPOSIT), DEPOSIT: deposit_history()},
        tx_counts={DEPOSIT: 0},
    )
    qualify_wallet(profile_for(chain), clients(hs), NOW)
    assert KnownAddress.objects.get(address=DEPOSIT).kind == "cex_deposit"
    assert not WalletLink.objects.filter(to_wallet__address=DEPOSIT).exists()


def test_hot_wallet_destination_is_not_followed(chain):
    hs = FakeWalletHyperSync(counterparties={VAULT: 5000})
    qualify_wallet(profile_for(chain), clients(hs), NOW)
    assert KnownAddress.objects.get(address=VAULT).kind == "exchange"
    assert not WalletProfile.objects.filter(wallet__address=VAULT).exists()


def test_massive_funder_is_a_service(chain):
    funder = Wallet.objects.create(address=FUNDER)
    for i in range(50):
        other = Wallet.objects.create(address=f"0x{i:040x}")
        WalletLink.objects.create(from_wallet=funder, to_wallet=other, kind="funding")
    qualify_wallet(profile_for(chain), clients(), NOW)
    assert not WalletLink.objects.filter(from_wallet=funder, to_wallet__address=BUYER).exists()
    assert KnownAddress.objects.get(address=FUNDER).kind == "service"


def test_follow_depth_zero_links_without_following(chain):
    QualificationSettings.objects.filter(chain=None).update(follow_depth=0)
    qualify_wallet(profile_for(chain), clients(), NOW)
    assert WalletLink.objects.filter(to_wallet__address=VAULT).exists()
    assert WalletProfile.objects.count() == 1


def test_early_buys_feed_tags(chain):
    buyer = profile_for(chain)
    make_early_buy(buyer.wallet, chain, TOKEN_A, is_sniper=True, bought=100, sold=10)
    qualify_wallet(buyer, clients(), NOW)
    assert buyer.tags == ["HOLDER", "SNIPER"]


def test_budget_exhaustion_keeps_history_fetched(chain):
    buyer = profile_for(chain)
    qualify_wallet(buyer, clients(), NOW)  # remplit le cache des prix natifs
    buyer.status = "history_fetched"
    buyer.save()
    with pytest.raises(BudgetExhausted):
        qualify_wallet(buyer, clients(zerion=FakeZerion(exhausted=True)), NOW)
    buyer.refresh_from_db()
    assert buyer.status == "history_fetched"


def test_qualification_is_idempotent(chain):
    buyer = profile_for(chain)
    qualify_wallet(buyer, clients(), NOW)
    links = WalletLink.objects.count()
    buyer.status = "history_fetched"
    buyer.save()
    qualify_wallet(buyer, clients(), NOW)
    assert WalletLink.objects.count() == links
