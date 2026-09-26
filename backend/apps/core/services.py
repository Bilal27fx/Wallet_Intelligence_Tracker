"""Checks de santé de l'infrastructure : base de données, Redis, workers Celery."""

import logging

import redis
from django.conf import settings
from django.db import connection

from config.celery import app as celery_app

logger = logging.getLogger(__name__)

OK = "ok"
ERROR = "error"


def check_database() -> str:
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
    except Exception:
        logger.exception("Health check database en échec")
        return ERROR
    return OK


def check_redis() -> str:
    try:
        client = redis.Redis.from_url(
            settings.REDIS_URL, socket_connect_timeout=1, socket_timeout=1
        )
        client.ping()
    except Exception:
        logger.exception("Health check redis en échec")
        return ERROR
    return OK


def check_celery() -> str:
    try:
        replies = celery_app.control.ping(timeout=1.0)
    except Exception:
        logger.exception("Health check celery en échec")
        return ERROR
    if not replies:
        logger.error("Health check celery : aucun worker n'a répondu")
        return ERROR
    return OK


def run_health_checks() -> dict:
    checks = {
        "database": check_database(),
        "redis": check_redis(),
        "celery": check_celery(),
    }
    status = OK if all(value == OK for value in checks.values()) else ERROR
    return {"status": status, "checks": checks}
