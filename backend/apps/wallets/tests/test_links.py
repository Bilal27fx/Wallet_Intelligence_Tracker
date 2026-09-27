import pytest

from apps.discovery.models import Wallet
from apps.discovery.tests.factories import make_chain
from apps.wallets.models import KnownAddress, WalletLink, WalletProfile
from apps.wallets.services.qualification import Clients, history_step, link_step, records_for
from apps.wallets.tests.factories import make_early_buy
from apps.wallets.tests.fakes import (
    BUYER,
    DEPOSIT,
    HOT,
    NOW,
    SENDER,
    START,
    TOKEN_A,
    VAULT,
    FakeWalletHyperSync,
    FakeZerion,
    buyer_zerion_history,
    tr,
)

pytestmark = pytest.mark.django_db


@pytest.fixture
def chain():
    return make_chain()


def fetched(hs=None, zerion=None):
    p = WalletProfile.objects.create(
        wallet=Wallet.objects.create(address=BUYER), status="prefiltered"
    )
    clients = Clients(lambda c: hs or FakeWalletHyperSync(), zerion or FakeZerion())
    history_step(p, clients, NOW)
    return p, clients


def test_strong_links_and_linked_profiles(chain):
    p, clients = fetched()
    link_step(p, clients, NOW, records_for(p.wallet))
    assert WalletLink.objects.filter(
        from_wallet__address=BUYER, to_wallet__address=VAULT, kind="transfer_after_buy"
    ).exists()
    big = WalletLink.objects.get(
        from_wallet__address=SENDER, to_wallet__address=BUYER, kind="big_receive"
    )
    assert big.evidence["pct"] == 65.79
    linked = WalletProfile.objects.filter(source="linked")
    assert sorted(x.wallet.address for x in linked) == [VAULT, SENDER] and all(
        x.depth == 1 for x in linked
    )


def test_funding_is_informative_link(chain):
    p, clients = fetched()
    make_early_buy(p.wallet, chain, TOKEN_A)
    link_step(p, clients, NOW, records_for(p.wallet))
    assert WalletLink.objects.filter(kind="funding", from_wallet__address=SENDER).exists()


def test_exchange_deposit_is_not_linked(chain):
    hs = FakeWalletHyperSync(
        transfers={
            DEPOSIT: [
                tr(START, "0xd1", TOKEN_A, BUYER, DEPOSIT, 1, BUYER, TOKEN_A),
                tr(START + 1, "0xd2", TOKEN_A, DEPOSIT, HOT, 1, HOT, TOKEN_A),
            ]
        },
        tx_counts={DEPOSIT: 0},
    )
    zerion = FakeZerion(histories={BUYER: buyer_zerion_history(send_to=DEPOSIT)})
    p, clients = fetched(hs, zerion)
    link_step(p, clients, NOW, records_for(p.wallet))
    assert not WalletLink.objects.filter(to_wallet__address=DEPOSIT).exists()
    assert KnownAddress.objects.get(address=DEPOSIT).kind == "cex_deposit"
    assert not WalletProfile.objects.filter(wallet__address=DEPOSIT).exists()


def test_link_step_is_idempotent(chain):
    p, clients = fetched()
    link_step(p, clients, NOW, records_for(p.wallet))
    link_step(p, clients, NOW, records_for(p.wallet))
    assert WalletLink.objects.filter(kind__in=["transfer_after_buy", "big_receive"]).count() == 2
