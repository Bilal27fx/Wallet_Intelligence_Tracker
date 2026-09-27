"""Tâches Celery de la qualification des wallets."""

from celery import shared_task


@shared_task
def qualify_wallets_task() -> dict:
    return {}
