from unittest.mock import patch

from django.urls import reverse

from apps.core import services


def _patch_checks(database, redis, celery):
    return (
        patch("apps.core.services.check_database", return_value=database),
        patch("apps.core.services.check_redis", return_value=redis),
        patch("apps.core.services.check_celery", return_value=celery),
    )


def test_health_url():
    assert reverse("core:health") == "/api/core/health/"


def test_health_returns_200_when_all_ok(api_client):
    db, rd, cl = _patch_checks(services.OK, services.OK, services.OK)
    with db, rd, cl:
        response = api_client.get("/api/core/health/")
    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "checks": {"database": "ok", "redis": "ok", "celery": "ok"},
    }


def test_health_returns_503_when_database_down(api_client):
    db, rd, cl = _patch_checks(services.ERROR, services.OK, services.OK)
    with db, rd, cl:
        response = api_client.get("/api/core/health/")
    assert response.status_code == 503
    assert response.json()["checks"]["database"] == "error"


def test_health_returns_503_when_redis_down(api_client):
    db, rd, cl = _patch_checks(services.OK, services.ERROR, services.OK)
    with db, rd, cl:
        response = api_client.get("/api/core/health/")
    assert response.status_code == 503
    assert response.json()["checks"]["redis"] == "error"


def test_health_returns_503_when_no_worker(api_client):
    db, rd, cl = _patch_checks(services.OK, services.OK, services.ERROR)
    with db, rd, cl:
        response = api_client.get("/api/core/health/")
    assert response.status_code == 503
    assert response.json() == {
        "status": "error",
        "checks": {"database": "ok", "redis": "ok", "celery": "error"},
    }
