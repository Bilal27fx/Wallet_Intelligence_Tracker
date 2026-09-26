import logging

from django.conf import settings

from apps.core.tasks import heartbeat, ping


def test_ping_returns_pong():
    result = ping.apply()
    assert result.get() == "pong"


def test_heartbeat_logs(caplog):
    with caplog.at_level(logging.INFO, logger="apps.core.tasks"):
        heartbeat.apply()
    assert "heartbeat" in caplog.text


def test_heartbeat_is_scheduled_every_minute():
    entry = settings.CELERY_BEAT_SCHEDULE["core-heartbeat"]
    assert entry["task"] == "apps.core.tasks.heartbeat"
    assert entry["schedule"] == 60.0
