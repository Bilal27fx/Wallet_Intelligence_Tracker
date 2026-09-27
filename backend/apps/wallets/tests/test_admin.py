import pytest
from django.urls import reverse

pytestmark = pytest.mark.django_db


@pytest.fixture
def admin_client(client, django_user_model):
    client.force_login(django_user_model.objects.create_superuser("admin", "a@example.com", "pw"))
    return client


@pytest.mark.parametrize("model", ["knownaddress", "qualificationsettings"])
def test_changelists_load(admin_client, model):
    assert admin_client.get(reverse(f"admin:wallets_{model}_changelist")).status_code == 200
