from unittest.mock import patch

import pytest

from apps.discovery import tasks
from apps.discovery.models import Candidate, Chain, EarlyBuyer, Explosion, PipelineSettings
from apps.discovery.tests.factories import make_candidate, make_token
from apps.discovery.tests.fakes import (
    NOW,
    FakeCoinGecko,
    FakeDirectory,
    FakeGeckoTerminal,
    FakeHyperSync,
    FakeZerion,
)

pytestmark = [pytest.mark.django_db, pytest.mark.usefixtures("young_waves")]


@pytest.fixture
def fake_clients():
    with (
        patch.object(tasks.clients, "geckoterminal", return_value=FakeGeckoTerminal()),
        patch.object(tasks.clients, "coingecko", return_value=FakeCoinGecko()),
        patch.object(tasks.clients, "hypersync_directory", return_value=FakeDirectory()),
        patch.object(tasks.clients, "zerion", return_value=FakeZerion()),
        patch.object(tasks.clients, "hypersync", return_value=FakeHyperSync()),
        patch.object(tasks.timezone, "now", return_value=NOW),
    ):
        yield


def run_pipeline():
    tasks.sync_chains_task.apply()
    tasks.collect_candidates_task.apply()
    tasks.analyze_candidates_task.apply()
    for candidate_id in Candidate.objects.filter(status="confirmed").values_list("id", flat=True):
        tasks.extract_candidate_buyers.apply(args=[candidate_id])


def test_full_pipeline_extracts_buyers(fake_clients):
    run_pipeline()
    assert Chain.objects.active().count() == 1
    candidate = Candidate.objects.get()
    assert candidate.status == Candidate.Status.BUYERS_EXTRACTED
    assert EarlyBuyer.objects.count() == 2


def test_pipeline_is_idempotent(fake_clients):
    run_pipeline()
    run_pipeline()
    assert Candidate.objects.count() == 1
    assert EarlyBuyer.objects.count() == 2


def test_extract_task_schedules_one_subtask_per_confirmed_candidate(fake_clients):
    tasks.sync_chains_task.apply()
    tasks.collect_candidates_task.apply()
    tasks.analyze_candidates_task.apply()
    with patch.object(tasks.extract_candidate_buyers, "delay") as delay:
        assert tasks.extract_early_buyers_task.apply().get() == 1
    delay.assert_called_once_with(Candidate.objects.get().pk)


def test_analyze_task_measures_pending_retention(fake_clients):
    tasks.sync_chains_task.apply()
    tasks.collect_candidates_task.apply()
    tasks.analyze_candidates_task.apply()
    Explosion.objects.update(retention_status="pending", retention_pct=None)
    counts = tasks.analyze_candidates_task.apply().get()
    assert counts["retention_held"] == 1
    assert Explosion.objects.get().retention_status == "held"


def test_failure_increments_attempts_then_rejects(fake_clients):
    tasks.sync_chains_task.apply()
    candidate = make_candidate(make_token(Chain.objects.get(gt_id="base")))
    cfg = PipelineSettings.load()
    cfg.max_attempts = 2
    cfg.save()
    with patch.object(tasks, "analyze_candidate", side_effect=RuntimeError("boom")):
        tasks.analyze_candidates_task.apply()
        candidate.refresh_from_db()
        assert (candidate.status, candidate.attempts) == (Candidate.Status.CANDIDATE, 1)
        tasks.analyze_candidates_task.apply()
    candidate.refresh_from_db()
    assert candidate.status == Candidate.Status.REJECTED
    assert candidate.rejection_reason == "error:RuntimeError"


def test_run_discovery_chains_the_four_tasks():
    with patch.object(tasks, "chain") as chain:
        tasks.run_discovery.apply()
    names = [sig.task for sig in chain.call_args.args]
    assert names == [
        "apps.discovery.tasks.sync_chains_task",
        "apps.discovery.tasks.collect_candidates_task",
        "apps.discovery.tasks.analyze_candidates_task",
        "apps.discovery.tasks.extract_early_buyers_task",
    ]
    chain.return_value.apply_async.assert_called_once()
