import pytest
from django.conf import settings


def test_timezone_is_utc():
    assert settings.TIME_ZONE == "UTC"
    assert settings.USE_TZ is True


def test_database_is_postgresql():
    assert settings.DATABASES["default"]["ENGINE"] == "django.db.backends.postgresql"


@pytest.mark.django_db
def test_admin_redirects_anonymous_to_login(client):
    response = client.get("/admin/")
    assert response.status_code == 302
    assert "/admin/login/" in response["Location"]
