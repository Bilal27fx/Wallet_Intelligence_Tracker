"""Tâches Celery de la qualification. Planning : tâche périodique `qualification-daily` (admin)."""

import logging
from datetime import timedelta

from celery import shared_task
from django.utils import timezone

from apps.discovery.models import PipelineSettings
from apps.discovery.services import clients
from apps.wallets.models import WalletProfile
from apps.wallets.services.qualification import (
    Clients,
    enqueue_profiles,
    filter_out,
    qualify_wallet,
    refresh_wallet,
)
from apps.wallets.services.raw import refresh_token_info
from integrations.errors import BudgetExhausted

logger = logging.getLogger(__name__)
Status = WalletProfile.Status
Source = WalletProfile.Source
QUALIFYING = (Status.PENDING, Status.PREFILTERED, Status.HISTORY_FETCHED)


def build_clients(cfg: PipelineSettings) -> Clients:
    # Un client HyperSync par chaîne : son cache de timestamps de blocs sert à toutes les étapes.
    hypersync_clients: dict[int, object] = {}

    def hypersync_for(chain):
        if chain.pk not in hypersync_clients:
            hypersync_clients[chain.pk] = clients.hypersync(chain, cfg)
        return hypersync_clients[chain.pk]

    return Clients(hypersync_for=hypersync_for, zerion=clients.zerion(cfg))


def record_failure(profile: WalletProfile, exc: Exception, max_attempts: int, now) -> None:
    logger.exception("Échec de qualification du wallet %s", profile.wallet_id, exc_info=exc)
    profile.attempts += 1
    if profile.attempts >= max_attempts:
        filter_out(profile, f"error:{type(exc).__name__}"[:64], now)
    else:
        profile.save(update_fields=["attempts"])


def scheduled_profiles(now, cfg) -> tuple[list[int], list[int]]:
    """Wallets liés, puis historiques en cours, puis nouveaux (par priorité), puis mises à jour."""
    profiles = WalletProfile.objects.values_list("id", flat=True)
    linked = list(profiles.filter(source=Source.LINKED, status=Status.PENDING).order_by("id"))
    early = profiles.filter(source=Source.EARLY_BUYER)
    in_progress = list(
        early.filter(status__in=[Status.PREFILTERED, Status.HISTORY_FETCHED]).order_by(
            "-priority", "id"
        )
    )
    new = list(early.filter(status=Status.PENDING).order_by("-priority", "id"))
    ids = (linked + in_progress + new)[: cfg.qualification_batch_size]
    room = cfg.qualification_batch_size - len(ids)
    refresh = list(
        early.filter(
            status=Status.QUALIFIED,
            history_complete=True,
            analyzed_at__lte=now - timedelta(days=cfg.history_refresh_days),
        ).order_by("analyzed_at")[: max(room, 0)]
    )
    return ids, refresh


@shared_task
def qualify_wallets_task() -> dict:
    cfg = PipelineSettings.load()
    now = timezone.now()
    created = enqueue_profiles(now, cfg)
    ids, refresh = scheduled_profiles(now, cfg)
    for profile_id in ids:
        qualify_wallet_task.delay(profile_id)
    for profile_id in refresh:
        refresh_wallet_task.delay(profile_id)
    logger.info(
        "%s nouveaux profils, %s programmés, %s mises à jour", created, len(ids), len(refresh)
    )
    refresh_token_info_task.delay()
    return {"new_profiles": created, "scheduled": len(ids), "refresh": len(refresh)}


@shared_task
def refresh_token_info_task() -> int:
    cfg = PipelineSettings.load()
    saved = refresh_token_info(clients.zerion(cfg), timezone.now(), cfg)
    logger.info("%s tokens décrits (métadonnées Zerion)", saved)
    return saved


def _run(profile_id: int, action, allowed) -> str:
    cfg = PipelineSettings.load()
    now = timezone.now()
    profile = WalletProfile.objects.select_related("wallet").get(pk=profile_id)
    if profile.status not in allowed:
        return profile.status
    try:
        return action(profile, build_clients(cfg), now, cfg)
    except BudgetExhausted:
        logger.info("Budget Zerion épuisé : wallet %s repris au prochain passage", profile_id)
        return profile.status
    except Exception as exc:
        record_failure(profile, exc, cfg.max_attempts, now)
        return profile.status


@shared_task
def qualify_wallet_task(profile_id: int) -> str:
    return _run(profile_id, qualify_wallet, QUALIFYING)


@shared_task
def refresh_wallet_task(profile_id: int) -> str:
    return _run(profile_id, refresh_wallet, (Status.QUALIFIED,))
