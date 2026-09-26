"""Tâches Celery de l'app core."""

import logging

from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task
def ping() -> str:
    """Vérifie la chaîne web → redis → worker."""
    return "pong"


@shared_task
def heartbeat() -> None:
    """Prouve que Beat planifie bien les tâches."""
    logger.info("heartbeat")
