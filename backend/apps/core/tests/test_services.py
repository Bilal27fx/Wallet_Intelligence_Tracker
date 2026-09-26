from unittest.mock import patch

import pytest

from apps.core import services


@pytest.mark.django_db
def test_check_database_ok():
    assert services.check_database() == services.OK


def test_check_database_error():
    with patch("apps.core.services.connection.cursor", side_effect=Exception("down")):
        assert services.check_database() == services.ERROR


def test_check_redis_ok():
    assert services.check_redis() == services.OK


def test_check_redis_error(settings):
    settings.REDIS_URL = "redis://localhost:1/0"
    assert services.check_redis() == services.ERROR


def test_check_celery_ok_when_a_worker_replies():
    with patch.object(services.celery_app.control, "ping", return_value=[{"w1": {"ok": "pong"}}]):
        assert services.check_celery() == services.OK


def test_check_celery_error_when_no_worker():
    with patch.object(services.celery_app.control, "ping", return_value=[]):
        assert services.check_celery() == services.ERROR


def test_check_celery_error_when_broker_fails():
    with patch.object(services.celery_app.control, "ping", side_effect=Exception("down")):
        assert services.check_celery() == services.ERROR


def test_run_health_checks_all_ok():
    with (
        patch("apps.core.services.check_database", return_value=services.OK),
        patch("apps.core.services.check_redis", return_value=services.OK),
        patch("apps.core.services.check_celery", return_value=services.OK),
    ):
        assert services.run_health_checks() == {
            "status": "ok",
            "checks": {"database": "ok", "redis": "ok", "celery": "ok"},
        }


def test_run_health_checks_one_failure():
    with (
        patch("apps.core.services.check_database", return_value=services.OK),
        patch("apps.core.services.check_redis", return_value=services.ERROR),
        patch("apps.core.services.check_celery", return_value=services.OK),
    ):
        assert services.run_health_checks() == {
            "status": "error",
            "checks": {"database": "ok", "redis": "error", "celery": "ok"},
        }
