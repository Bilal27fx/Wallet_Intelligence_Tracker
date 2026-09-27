import pytest
from django.core.files.uploadedfile import SimpleUploadedFile
from django.urls import reverse

from apps.discovery.tests.factories import make_chain
from apps.wallets.models import KnownAddress
from apps.wallets.services.exchanges import import_known_addresses

pytestmark = pytest.mark.django_db


@pytest.fixture
def admin_client(client, django_user_model):
    client.force_login(django_user_model.objects.create_superuser("admin", "a@example.com", "pw"))
    return client


@pytest.mark.parametrize(
    "model",
    [
        "walletprofile",
        "entity",
        "tokenposition",
        "tokentrade",
        "walletlink",
        "knownaddress",
        "dailyprice",
        "qualificationsettings",
    ],
)
def test_changelists_load(admin_client, model):
    assert admin_client.get(reverse(f"admin:wallets_{model}_changelist")).status_code == 200


def test_import_known_addresses_skips_invalid_rows():
    make_chain()
    count = import_known_addresses(
        [
            {"address": "0xABC" + "0" * 37, "kind": "exchange", "label": "Binance", "chain": ""},
            {"address": "0x" + "1" * 40, "kind": "bridge", "label": "", "chain": "base"},
            {"address": "pas-une-adresse", "kind": "exchange"},
            {"address": "0x" + "2" * 40, "kind": "inconnu"},
            {"address": "0x" + "3" * 40, "kind": "exchange", "chain": "chaine-inconnue"},
        ]
    )
    assert count == 2
    assert KnownAddress.objects.get(address="0xabc" + "0" * 37).chain is None


def test_import_view(admin_client):
    url = reverse("admin:wallets_knownaddress_import")
    assert admin_client.get(url).status_code == 200
    csv = SimpleUploadedFile(
        "a.csv", b"address,kind,label,chain\n0x" + b"4" * 40 + b",exchange,OKX,\n"
    )
    response = admin_client.post(url, {"file": csv})
    assert response.status_code == 302
    assert KnownAddress.objects.get(address="0x" + "4" * 40).source == "import"
