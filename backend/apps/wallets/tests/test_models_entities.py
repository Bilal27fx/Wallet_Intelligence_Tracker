import pytest

from apps.discovery.models import Entity, Wallet
from apps.wallets.models import STRONG_LINKS, WalletLink

pytestmark = pytest.mark.django_db


def test_wallet_belongs_to_entity():
    entity = Entity.objects.create()
    wallet = Wallet.objects.create(address="0x" + "1" * 40, entity=entity)
    assert list(entity.wallets.all()) == [wallet]


def test_vault_link_is_strong_and_defaults():
    assert WalletLink.Kind.TRANSFER_TO_VAULT in STRONG_LINKS
    a = Wallet.objects.create(address="0x" + "1" * 40)
    b = Wallet.objects.create(address="0x" + "2" * 40)
    link = WalletLink.objects.create(from_wallet=a, to_wallet=b, kind="transfer_to_vault")
    assert (link.source, link.rejected) == ("zerion", False)
