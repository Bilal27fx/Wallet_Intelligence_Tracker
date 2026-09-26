from datetime import timedelta

import pytest

from apps.discovery.models import Candidate, Explosion, Pool
from apps.discovery.services.analysis import analyze_candidate
from apps.discovery.tests.factories import make_candidate, make_chain, make_token
from apps.discovery.tests.fakes import (
    GENESIS_TS,
    HOUR,
    NOW,
    POOL,
    POOL_CREATED,
    TOKEN,
    FakeGeckoTerminal,
    FakeHyperSync,
    block_of,
    explosive_candles,
)

pytestmark = pytest.mark.django_db


@pytest.fixture
def candidate():
    return make_candidate(make_token(make_chain(), address=TOKEN, decimals=9))


def analyze(candidate, gt=None, now=NOW):
    return analyze_candidate(
        candidate,
        gt=gt or FakeGeckoTerminal(),
        hypersync_for=lambda chain: FakeHyperSync(),
        now=now,
    )


def test_confirms_explosion_and_stores_blocks(candidate):
    assert analyze(candidate) == Candidate.Status.CONFIRMED
    candidate.refresh_from_db()
    assert candidate.status == Candidate.Status.CONFIRMED
    explosion = candidate.explosion
    start = int(POOL_CREATED.timestamp())
    assert explosion.low_block == block_of(start + 10 * HOUR)
    assert explosion.peak_block == block_of(start + 20 * HOUR)
    assert float(explosion.multiplier) == 10.0
    assert float(explosion.retention_pct) == 60.0
    assert candidate.token.decimals == 18
    assert Pool.objects.get(address=POOL).created_block == 500


def test_rejects_when_no_explosion(candidate):
    flat = FakeGeckoTerminal(candles=explosive_candles(0)[:10])
    assert analyze(candidate, gt=flat) == Candidate.Status.REJECTED
    candidate.refresh_from_db()
    assert candidate.rejection_reason == "no_explosion"


def test_waits_when_peak_is_recent(candidate):
    recent = POOL_CREATED + timedelta(hours=30)
    assert analyze(candidate, now=recent) == Candidate.Status.WAITING_CONFIRMATION
    candidate.refresh_from_db()
    peak = int(POOL_CREATED.timestamp()) + 20 * HOUR
    assert candidate.next_check_at.timestamp() == peak + 24 * HOUR
    assert not Explosion.objects.exists()


def test_waiting_too_long_is_rejected(candidate):
    Candidate.objects.filter(pk=candidate.pk).update(
        status=Candidate.Status.WAITING_CONFIRMATION, created_at=NOW - timedelta(hours=200)
    )
    candidate.refresh_from_db()
    assert analyze(candidate) == Candidate.Status.REJECTED
    candidate.refresh_from_db()
    assert candidate.rejection_reason == "confirmation_timeout"


def test_inactive_chain_is_rejected(candidate):
    candidate.token.chain.is_enabled = False
    candidate.token.chain.save()
    assert analyze(candidate) == Candidate.Status.REJECTED
    candidate.refresh_from_db()
    assert candidate.rejection_reason == "chain_inactive"


def test_token_without_pool_is_rejected(candidate):
    assert analyze(candidate, gt=FakeGeckoTerminal(token_pools=[])) == Candidate.Status.REJECTED
    candidate.refresh_from_db()
    assert candidate.rejection_reason == "no_pool"


def test_same_peak_as_previous_explosion_is_rejected(candidate):
    analyze(candidate)
    Candidate.objects.filter(pk=candidate.pk).update(status=Candidate.Status.BUYERS_EXTRACTED)
    again = make_candidate(candidate.token)
    assert analyze(again) == Candidate.Status.REJECTED
    again.refresh_from_db()
    assert again.rejection_reason == "already_extracted"


def test_genesis_timestamp_is_consistent_with_fake():
    assert GENESIS_TS + 500 * 2 == int(POOL_CREATED.timestamp())
