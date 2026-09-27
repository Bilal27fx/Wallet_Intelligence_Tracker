import pytest
from django.core.files.uploadedfile import SimpleUploadedFile
from django.urls import reverse

from apps.discovery.models import Wallet
from apps.wallets.models import KnownAddress, WalletProfile

pytestmark = pytest.mark.django_db


@pytest.fixture
def admin_client(client, django_user_model):
    client.force_login(django_user_model.objects.create_superuser("admin", "a@example.com", "pw"))
    return client


@pytest.mark.parametrize(
    "model",
    [
        "walletprofile",
        "wallettransaction",
        "tokentrade",
        "tokenposition",
        "walletlink",
        "knownaddress",
        "qualificationsettings",
        "tokeninfo",
        "portfoliosnapshot",
    ],
)
def test_changelists_load(admin_client, model):
    assert admin_client.get(reverse(f"admin:wallets_{model}_changelist")).status_code == 200


def test_profile_page_loads(admin_client):
    profile = WalletProfile.objects.create(wallet=Wallet.objects.create(address="0x" + "a" * 40))
    assert (
        admin_client.get(
            reverse("admin:wallets_walletprofile_change", args=[profile.pk])
        ).status_code
        == 200
    )


def test_import_view(admin_client):
    url = reverse("admin:wallets_knownaddress_import")
    assert admin_client.get(url).status_code == 200
    csv = SimpleUploadedFile(
        "a.csv", b"address,kind,label,chain\n0x" + b"4" * 40 + b",exchange,OKX,\n"
    )
    assert admin_client.post(url, {"file": csv}).status_code == 302
    assert KnownAddress.objects.get(address="0x" + "4" * 40).source == "import"
