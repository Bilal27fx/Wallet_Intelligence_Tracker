"""Tâches Celery de la qualification. Planning : tâche périodique `qualification-daily` (admin)."""

import logging

from celery import shared_task
from django.utils import timezone

from apps.discovery.models import PipelineSettings
from apps.discovery.services import clients
from apps.wallets.models import WalletProfile
from apps.wallets.services.pricing import sync_quote_assets
from apps.wallets.services.qualification import (
    Clients,
    enqueue_profiles,
    filter_out,
    qualify_wallet,
)
from integrations.errors import BudgetExhausted

logger = logging.getLogger(__name__)
IN_PROGRESS = (
    WalletProfile.Status.PENDING,
    WalletProfile.Status.PREFILTERED,
    WalletProfile.Status.HISTORY_FETCHED,
)


def build_clients(cfg: PipelineSettings) -> Clients:
    return Clients(
        hypersync_for=lambda chain: clients.hypersync(chain, cfg),
        zerion=clients.zerion(cfg),
        rpc_for=lambda chain: clients.rpc(chain, cfg),
    )


def record_failure(profile: WalletProfile, exc: Exception, max_attempts: int, now) -> None:
    logger.exception("Échec de qualification du wallet %s", profile.wallet_id, exc_info=exc)
    profile.attempts += 1
    if profile.attempts >= max_attempts:
        filter_out(profile, f"error:{type(exc).__name__}"[:64], now)
    else:
        profile.save(update_fields=["attempts"])


@shared_task
def qualify_wallets_task() -> dict:
    cfg = PipelineSettings.load()
    now = timezone.now()
    try:
        sync_quote_assets(clients.zerion(cfg), cfg)
    except BudgetExhausted:
        logger.warning("Budget Zerion épuisé : actifs de cotation non rafraîchis")
    created = enqueue_profiles(now, cfg)
    ids = list(
        WalletProfile.objects.filter(status__in=IN_PROGRESS)
        .order_by("depth", "id")
        .values_list("id", flat=True)[: cfg.qualification_batch_size]
    )
    for profile_id in ids:
        qualify_wallet_task.delay(profile_id)
    logger.info("%s nouveaux profils, %s wallets programmés", created, len(ids))
    return {"new_profiles": created, "scheduled": len(ids)}


@shared_task
def qualify_wallet_task(profile_id: int) -> str:
    cfg = PipelineSettings.load()
    now = timezone.now()
    profile = WalletProfile.objects.select_related("wallet").get(pk=profile_id)
    if profile.status not in IN_PROGRESS:
        return profile.status
    try:
        return qualify_wallet(profile, build_clients(cfg), now)
    except BudgetExhausted:
        logger.info("Budget Zerion épuisé : wallet %s reporté au prochain passage", profile_id)
        return profile.status
    except Exception as exc:
        record_failure(profile, exc, cfg.max_attempts, now)
        return profile.status
