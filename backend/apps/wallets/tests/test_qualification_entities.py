import pytest

from apps.discovery.models import PipelineSettings, Wallet
from apps.discovery.tests.factories import make_chain
from apps.wallets.models import WalletLink, WalletProfile
from apps.wallets.services import entity_graph
from apps.wallets.services.qualification import (
    compute_priority,
    linked_profiles,
    mark_internal,
    records_for,
)
from apps.wallets.tests.factories import make_early_buy
from apps.wallets.tests.fakes import BUYER, TOKEN_A, TOKEN_B, VAULT

pytestmark = pytest.mark.django_db
AFTER_BUY = WalletLink.Kind.TRANSFER_AFTER_BUY


def test_trades_between_entity_wallets_are_internal(make_trade):
    entity_graph.link(BUYER, VAULT, AFTER_BUY, "zerion", {})
    buyer = Wallet.objects.get(address=BUYER)
    make_trade(buyer, kind="send", counterparty=VAULT)
    make_trade(buyer, kind="buy", counterparty="0x" + "9" * 40)
    assert mark_internal(buyer) == 1
    assert [record.kind for record in records_for(buyer)] == ["buy"]


def test_no_entity_means_nothing_internal(make_trade):
    buyer = Wallet.objects.create(address=BUYER)
    make_trade(buyer, kind="send", counterparty=VAULT)
    assert mark_internal(buyer) == 0


def test_linked_profiles_are_entity_members():
    entity_graph.link(BUYER, VAULT, AFTER_BUY, "zerion", {})
    buyer = Wallet.objects.get(address=BUYER)
    WalletProfile.objects.create(wallet=buyer)
    other = WalletProfile.objects.create(wallet=Wallet.objects.get(address=VAULT), source="linked")
    assert list(linked_profiles(buyer)) == [other]


def test_priority_counts_entity_explosions():
    chain = make_chain()
    one = Wallet.objects.create(address="0x" + "1" * 40)
    two = Wallet.objects.create(address="0x" + "2" * 40)
    solo = Wallet.objects.create(address="0x" + "3" * 40)
    make_early_buy(one, chain, TOKEN_A)
    make_early_buy(two, chain, TOKEN_B)
    make_early_buy(solo, chain, "0x" + "c" * 40)
    entity_graph.link(one.address, two.address, WalletLink.Kind.TRANSFER_TO_VAULT, "hypersync", {})
    one.refresh_from_db()
    cfg = PipelineSettings.load()
    assert compute_priority(one, cfg) > compute_priority(solo, cfg)
