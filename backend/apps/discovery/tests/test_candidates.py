from datetime import timedelta

import pytest

from apps.discovery.models import Candidate, PipelineSettings, Pool
from apps.discovery.services.candidates import (
    SOURCE_MANUAL,
    SOURCE_TRENDING,
    SOURCE_VOLUME,
    add_manual_candidate,
    collect_candidates,
    passes_prefilter,
)
from apps.discovery.services.settings import thresholds_for
from apps.discovery.tests.factories import make_chain
from apps.discovery.tests.fakes import NOW, POOL, TOKEN, FakeGeckoTerminal, make_pool

pytestmark = pytest.mark.django_db


@pytest.fixture
def chain():
    return make_chain()


@pytest.mark.parametrize(
    "overrides",
    [
        {"price_change_24h_pct": 10},
        {"liquidity_usd": 100},
        {"volume_24h_usd": 100},
        {"fdv_usd": 50_000},
        {"fdv_usd": 500_000_000},
        {"created_at": NOW - timedelta(days=60)},
        {"created_at": None},
    ],
)
def test_prefilter_rejects(chain, overrides):
    assert not passes_prefilter(make_pool(**overrides), thresholds_for(chain), NOW)


def test_prefilter_accepts(chain):
    assert passes_prefilter(make_pool(), thresholds_for(chain), NOW)


def test_collect_creates_candidate_with_sources_and_metrics(chain):
    gt = FakeGeckoTerminal(
        volume={"base": [make_pool(liquidity_usd=90_000.0, address="0x" + "3" * 40)]}
    )
    assert collect_candidates(gt, PipelineSettings.load(), NOW) == 1
    candidate = Candidate.objects.get()
    assert candidate.token.address == TOKEN
    assert candidate.token.symbol == "BOOM"
    assert candidate.sources == [SOURCE_TRENDING, SOURCE_VOLUME]
    assert candidate.metrics["liquidity_usd"] == 90_000.0
    assert set(Pool.objects.values_list("address", flat=True)) == {POOL, "0x" + "3" * 40}


def test_collect_ignores_inactive_chains(chain):
    gt = FakeGeckoTerminal(trending=[make_pool(network="solana")])
    assert collect_candidates(gt, PipelineSettings.load(), NOW) == 0


def test_collect_is_idempotent(chain):
    collect_candidates(FakeGeckoTerminal(), PipelineSettings.load(), NOW)
    assert collect_candidates(FakeGeckoTerminal(), PipelineSettings.load(), NOW) == 0
    assert Candidate.objects.count() == 1


def test_recently_closed_token_is_not_recandidated(chain):
    collect_candidates(FakeGeckoTerminal(), PipelineSettings.load(), NOW)
    # queryset.update() ne touche pas updated_at : on le fixe explicitement.
    Candidate.objects.update(status=Candidate.Status.REJECTED, updated_at=NOW)
    assert collect_candidates(FakeGeckoTerminal(), PipelineSettings.load(), NOW) == 0


def test_token_closed_before_cooldown_can_come_back(chain):
    collect_candidates(FakeGeckoTerminal(), PipelineSettings.load(), NOW)
    # queryset.update() ne touche pas updated_at : on le fixe explicitement.
    Candidate.objects.update(status=Candidate.Status.REJECTED, updated_at=NOW)
    later = NOW + timedelta(hours=73)
    assert collect_candidates(FakeGeckoTerminal(), PipelineSettings.load(), later) == 1


def test_add_manual_candidate_skips_prefilters(chain):
    gt = FakeGeckoTerminal(token_pools=[make_pool(price_change_24h_pct=-50)])
    candidate = add_manual_candidate(chain, TOKEN.upper().replace("0X", "0x"), gt)
    assert candidate.sources == [SOURCE_MANUAL]
    assert candidate.token.address == TOKEN


def test_add_manual_candidate_without_pool_fails(chain):
    with pytest.raises(ValueError):
        add_manual_candidate(chain, TOKEN, FakeGeckoTerminal(token_pools=[]))


def test_add_manual_candidate_twice_fails(chain):
    add_manual_candidate(chain, TOKEN, FakeGeckoTerminal())
    with pytest.raises(ValueError):
        add_manual_candidate(chain, TOKEN, FakeGeckoTerminal())
