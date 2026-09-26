from unittest.mock import patch

import pytest
from django.urls import reverse

from apps.discovery.models import Candidate, EarlyBuyer, PipelineSettings
from apps.discovery.tests.factories import make_chain
from apps.discovery.tests.fakes import TOKEN, FakeGeckoTerminal

pytestmark = pytest.mark.django_db


@pytest.fixture
def admin_client(client, django_user_model):
    user = django_user_model.objects.create_superuser("admin", "a@example.com", "pw")
    client.force_login(user)
    return client


@pytest.mark.parametrize(
    "model",
    [
        "chain",
        "detectionsettings",
        "pipelinesettings",
        "candidate",
        "explosion",
        "earlybuyer",
        "wallet",
    ],
)
def test_changelists_load(admin_client, model):
    assert admin_client.get(reverse(f"admin:discovery_{model}_changelist")).status_code == 200


def test_pipeline_settings_cannot_be_added_twice(admin_client):
    PipelineSettings.load()
    assert admin_client.get(reverse("admin:discovery_pipelinesettings_add")).status_code == 403


def test_early_buyers_are_read_only(admin_client):
    assert admin_client.get(reverse("admin:discovery_earlybuyer_add")).status_code == 403
    assert not EarlyBuyer.objects.exists()


def test_manual_add_creates_candidate(admin_client):
    chain = make_chain()
    url = reverse("admin:discovery_candidate_add_manual")
    assert admin_client.get(url).status_code == 200
    with patch("apps.discovery.admin.clients.geckoterminal", return_value=FakeGeckoTerminal()):
        response = admin_client.post(url, {"chain": chain.pk, "address": TOKEN})
    assert response.status_code == 302
    assert Candidate.objects.get().sources == ["manual"]


def test_manual_add_shows_error_when_no_pool(admin_client):
    chain = make_chain()
    url = reverse("admin:discovery_candidate_add_manual")
    with patch(
        "apps.discovery.admin.clients.geckoterminal",
        return_value=FakeGeckoTerminal(token_pools=[]),
    ):
        response = admin_client.post(url, {"chain": chain.pk, "address": TOKEN})
    assert response.status_code == 200
    assert "Aucun pool" in response.content.decode()
    assert not Candidate.objects.exists()
