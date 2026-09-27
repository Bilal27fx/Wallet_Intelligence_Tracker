"""Tâches Celery de la découverte. Planning : tâche périodique `discovery-daily` (admin)."""

import logging
from collections import Counter

from celery import chain, shared_task
from django.utils import timezone

from apps.discovery.models import Candidate, Explosion, PipelineSettings
from apps.discovery.services import clients
from apps.discovery.services.analysis import analyze_candidate, update_retention
from apps.discovery.services.candidates import collect_candidates
from apps.discovery.services.chains import sync_chains
from apps.discovery.services.extraction import extract_buyers

logger = logging.getLogger(__name__)


def record_failure(candidate: Candidate, exc: Exception, max_attempts: int) -> None:
    logger.exception("Échec sur le candidat %s", candidate.pk, exc_info=exc)
    candidate.attempts += 1
    fields = ["attempts", "updated_at"]
    if candidate.attempts >= max_attempts:
        candidate.status = Candidate.Status.REJECTED
        candidate.rejection_reason = f"error:{type(exc).__name__}"[:64]
        fields += ["status", "rejection_reason"]
    candidate.save(update_fields=fields)


@shared_task
def sync_chains_task() -> int:
    cfg = PipelineSettings.load()
    count = sync_chains(
        clients.geckoterminal(cfg),
        clients.coingecko(cfg),
        clients.hypersync_directory(cfg),
        clients.zerion(cfg),
    )
    logger.info("%s chaînes synchronisées", count)
    return count


@shared_task
def collect_candidates_task() -> int:
    cfg = PipelineSettings.load()
    created = collect_candidates(clients.geckoterminal(cfg), cfg, timezone.now())
    logger.info("%s nouveaux candidats", created)
    return created


@shared_task
def analyze_candidates_task() -> dict[str, int]:
    cfg = PipelineSettings.load()
    gt = clients.geckoterminal(cfg)
    now = timezone.now()
    due = Candidate.objects.filter(
        status__in=[Candidate.Status.CANDIDATE, Candidate.Status.WAITING_CONFIRMATION]
    ).select_related("token__chain")
    counts: Counter[str] = Counter()
    for candidate in due:
        try:
            status = analyze_candidate(
                candidate,
                gt=gt,
                hypersync_for=lambda chain_: clients.hypersync(chain_, cfg),
                now=now,
            )
        except Exception as exc:
            record_failure(candidate, exc, cfg.max_attempts)
            status = "error"
        counts[status] += 1

    pending = Explosion.objects.filter(retention_status=Explosion.Retention.PENDING).select_related(
        "candidate__token__chain"
    )
    for explosion in pending:
        try:
            counts[f"retention_{update_retention(explosion, gt=gt, now=now)}"] += 1
        except Exception:
            logger.exception("Échec de la mesure de rétention de l'explosion %s", explosion.pk)
    logger.info("Analyse : %s", dict(counts))
    return dict(counts)


@shared_task
def extract_early_buyers_task() -> int:
    ids = list(
        Candidate.objects.filter(status=Candidate.Status.CONFIRMED).values_list("id", flat=True)
    )
    for candidate_id in ids:
        extract_candidate_buyers.delay(candidate_id)
    return len(ids)


@shared_task
def extract_candidate_buyers(candidate_id: int) -> int:
    cfg = PipelineSettings.load()
    candidate = Candidate.objects.select_related("token__chain", "explosion").get(pk=candidate_id)
    if candidate.status != Candidate.Status.CONFIRMED:
        return 0
    try:
        return extract_buyers(
            candidate,
            gt=clients.geckoterminal(cfg),
            hypersync=clients.hypersync(candidate.token.chain, cfg),
            cfg=cfg,
            now=timezone.now(),
        )
    except Exception as exc:
        record_failure(candidate, exc, cfg.max_attempts)
        return 0


@shared_task
def run_discovery() -> None:
    chain(
        sync_chains_task.si(),
        collect_candidates_task.si(),
        analyze_candidates_task.si(),
        extract_early_buyers_task.si(),
    ).apply_async()
