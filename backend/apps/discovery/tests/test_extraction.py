from decimal import Decimal

import pytest

from apps.discovery.models import Candidate, EarlyBuyer, PipelineSettings, Wallet
from apps.discovery.services.analysis import analyze_candidate
from apps.discovery.services.extraction import extract_buyers
from apps.discovery.tests.factories import make_candidate, make_chain, make_token
from apps.discovery.tests.fakes import (
    ALICE,
    NOW,
    SNIPER,
    TOKEN,
    UNIT,
    FakeGeckoTerminal,
    FakeHyperSync,
)
from integrations.errors import TooManyTransfers

pytestmark = pytest.mark.django_db


@pytest.fixture
def confirmed():
    candidate = make_candidate(make_token(make_chain(), address=TOKEN))
    analyze_candidate(
        candidate, gt=FakeGeckoTerminal(), hypersync_for=lambda chain: FakeHyperSync(), now=NOW
    )
    candidate.refresh_from_db()
    assert candidate.status == Candidate.Status.CONFIRMED
    return candidate


def extract(candidate, hypersync=None):
    return extract_buyers(
        candidate,
        gt=FakeGeckoTerminal(),
        hypersync=hypersync or FakeHyperSync(),
        cfg=PipelineSettings.load(),
        now=NOW,
    )


def test_stores_significant_eoa_buyers(confirmed):
    assert extract(confirmed) == 2
    confirmed.refresh_from_db()
    assert confirmed.status == Candidate.Status.BUYERS_EXTRACTED
    buyers = {b.wallet.address: b for b in EarlyBuyer.objects.select_related("wallet")}
    assert set(buyers) == {ALICE, SNIPER}
    alice = buyers[ALICE]
    assert alice.bought_amount == Decimal(1000 * UNIT)
    assert alice.bought_usd == Decimal("1000.00")
    assert alice.sold_amount == Decimal(400 * UNIT)
    assert not alice.is_sniper
    assert buyers[SNIPER].is_sniper
    assert buyers[SNIPER].bought_usd == Decimal("2000.00")


def test_queries_transfers_from_pool_creation_to_peak(confirmed):
    hypersync = FakeHyperSync()
    extract(confirmed, hypersync)
    token, from_block, to_block, cap = hypersync.transfer_calls[0]
    assert (token, from_block) == (TOKEN, 500)
    assert to_block == confirmed.explosion.peak_block + 1
    assert cap == PipelineSettings.load().max_transfers_per_token


def test_too_many_transfers_rejects(confirmed):
    assert extract(confirmed, FakeHyperSync(error=TooManyTransfers("trop"))) == 0
    confirmed.refresh_from_db()
    assert (confirmed.status, confirmed.rejection_reason) == (
        Candidate.Status.REJECTED,
        "too_many_transfers",
    )


def test_wallets_are_shared_between_explosions(confirmed):
    Wallet.objects.create(address=ALICE)
    extract(confirmed)
    assert Wallet.objects.filter(address=ALICE).count() == 1
