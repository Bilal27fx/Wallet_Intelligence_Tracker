import pytest

from apps.discovery.models import EarlyBuyer, Entity, Wallet
from apps.discovery.services.entity_buys import refresh_entity_buys
from apps.discovery.tests.factories import make_chain
from apps.wallets.models import WalletLink
from apps.wallets.services import entity_graph
from apps.wallets.tests.factories import make_early_buy

pytestmark = pytest.mark.django_db

A, B, C, D = ("0x" + c * 40 for c in "abcd")
VAULT_LINK = WalletLink.Kind.TRANSFER_TO_VAULT


def entity_of(address):
    return Wallet.objects.get(address=address).entity_id


def test_strong_link_creates_entity():
    entity_graph.link(A, B, VAULT_LINK, "hypersync", {"pct": 80})
    assert entity_of(A) is not None and entity_of(A) == entity_of(B)
    assert WalletLink.objects.get().source == "hypersync"


def test_weak_link_does_not_group():
    entity_graph.link(A, B, WalletLink.Kind.FUNDING, "zerion", {})
    assert entity_of(A) is None and entity_of(B) is None


def test_chain_joins_same_entity():
    entity_graph.link(A, B, VAULT_LINK, "hypersync", {})
    entity_graph.link(B, C, WalletLink.Kind.TRANSFER_AFTER_BUY, "zerion", {})
    assert entity_of(A) == entity_of(B) == entity_of(C)


def test_link_between_two_entities_merges():
    entity_graph.link(A, B, VAULT_LINK, "hypersync", {})
    entity_graph.link(C, D, VAULT_LINK, "hypersync", {})
    gone = entity_of(C)
    entity_graph.link(B, C, WalletLink.Kind.BIG_RECEIVE, "zerion", {})
    assert len({entity_of(x) for x in (A, B, C, D)}) == 1
    assert Entity.objects.get(pk=gone).merged_into_id == entity_of(A)


def test_detached_wallet_is_never_relinked():
    entity_graph.link(A, B, VAULT_LINK, "hypersync", {})
    group = entity_of(A)
    entity_graph.detach(Wallet.objects.get(address=B))
    assert entity_of(B) not in (None, group)
    assert entity_graph.link(B, A, WalletLink.Kind.BIG_RECEIVE, "zerion", {}) is None
    assert entity_of(B) != entity_of(A)


def copy_buy(buy, wallet, held_usd):
    buy.pk = None
    buy.wallet, buy.entity_id, buy.held_usd = wallet, wallet.entity_id, held_usd
    buy.save()
    return buy


def test_entity_buys_sum_members_and_rank():
    chain = make_chain()
    a, b, c = (Wallet.objects.create(address=x) for x in (A, B, C))
    first = make_early_buy(a, chain, "0x" + "e" * 40)
    explosion = first.explosion
    entity_graph.link(A, B, VAULT_LINK, "hypersync", {})
    for wallet in (a, b):
        wallet.refresh_from_db()
    EarlyBuyer.objects.filter(pk=first.pk).update(entity=a.entity, held_usd=300)
    copy_buy(EarlyBuyer.objects.get(pk=first.pk), b, 500)
    entity_graph.ensure_entity(c)
    copy_buy(EarlyBuyer.objects.get(pk=first.pk), c, 700)
    assert refresh_entity_buys(explosion) == 2
    ranked = explosion.entity_buys.order_by("rank")
    assert [(float(e.held_usd), e.rank) for e in ranked] == [(800.0, 1), (700.0, 2)]


def test_merge_moves_early_buys_to_kept_entity():
    chain = make_chain()
    a, c = Wallet.objects.create(address=A), Wallet.objects.create(address=C)
    make_early_buy(a, chain, "0x" + "e" * 40)
    make_early_buy(c, chain, "0x" + "f" * 40)
    entity_graph.link(A, B, VAULT_LINK, "hypersync", {})
    entity_graph.link(C, D, VAULT_LINK, "hypersync", {})
    entity_graph.link(B, C, WalletLink.Kind.BIG_RECEIVE, "zerion", {})
    kept = entity_of(A)
    assert set(EarlyBuyer.objects.values_list("entity_id", flat=True)) == {kept}
