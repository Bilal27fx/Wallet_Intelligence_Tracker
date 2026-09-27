from decimal import Decimal

import pytest

from apps.discovery.models import (
    Candidate,
    DetectionSettings,
    EarlyBuyer,
    EntityEarlyBuy,
    ExcludedBuyer,
    Explosion,
    PipelineSettings,
    TokenTransfer,
    Wallet,
)
from apps.discovery.services.analysis import analyze_candidate
from apps.discovery.services.extraction import extract_buyers
from apps.discovery.tests.factories import make_candidate, make_chain, make_token
from apps.discovery.tests.fakes import (
    ALICE,
    HOUR,
    NOW,
    POOL_CREATED,
    SNIPER,
    TOKEN,
    UNIT,
    FakeGeckoTerminal,
    FakeHyperSync,
    block_of,
)
from apps.wallets.models import WalletLink
from integrations.hypersync import Transfer

pytestmark = [pytest.mark.django_db, pytest.mark.usefixtures("young_waves")]


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
    # Valorisées au prix du creux (0,5 $) : 1 000 tokens → 500 $, 2 000 → 1 000 $.
    assert (alice.held_amount, alice.held_usd) == (Decimal(1000 * UNIT), Decimal("500.00"))
    assert buyers[SNIPER].held_usd == Decimal("1000.00")
    assert alice.sold_amount == Decimal(400 * UNIT)
    assert not alice.is_sniper
    assert buyers[SNIPER].is_sniper
    assert buyers[SNIPER].bought_usd == Decimal("2000.00")
    assert alice.entity_id is not None
    assert EntityEarlyBuy.objects.count() == 2


def test_scan_then_entity_pass_on_retained_wallets(confirmed):
    hypersync = FakeHyperSync()
    extract(confirmed, hypersync)
    explosion = confirmed.explosion
    first, *rest = hypersync.transfer_calls
    assert first == (TOKEN, 500, explosion.trough_block + 1, None, None)
    assert [call[1:] for call in rest] == [
        (explosion.trough_block + 1, explosion.peak_block + 1, None, None)
    ]
    assert TokenTransfer.objects.filter(explosion=explosion, kind="sell").count() == 1
    explosion.refresh_from_db()
    assert explosion.extraction_status == Explosion.Extraction.COMPLETE


VAULT = "0x" + "7" * 40


def with_vault():
    fake = FakeHyperSync()
    move = Transfer(610, fake.block_timestamp(610), ALICE, ALICE, VAULT, 600 * UNIT, "0xmove")
    return fake._all_transfers() + [move]


def test_vault_inherits_and_joins_alice_entity(confirmed):
    extract(confirmed, FakeHyperSync(transfers=with_vault()))
    alice = EarlyBuyer.objects.get(wallet__address=ALICE)
    vault = EarlyBuyer.objects.get(wallet__address=VAULT)
    assert alice.entity_id == vault.entity_id
    assert (vault.inherited_amount, vault.inherited_from.address) == (Decimal(600 * UNIT), ALICE)
    assert (alice.held_usd, vault.held_usd) == (Decimal("200.00"), Decimal("300.00"))
    assert WalletLink.objects.get(to_wallet__address=VAULT).source == "hypersync"
    assert EntityEarlyBuy.objects.get(entity_id=alice.entity_id).held_usd == Decimal("500.00")


def test_bot_is_excluded_and_recorded(confirmed):
    assert extract(confirmed, FakeHyperSync(tx_counts={SNIPER: 10_000})) == 1
    assert not EarlyBuyer.objects.filter(wallet__address=SNIPER).exists()
    excluded = ExcludedBuyer.objects.get()
    assert (excluded.wallet.address, excluded.reason) == (SNIPER, "bot")


def test_transfer_guard_keeps_partial_buyers(confirmed):
    cfg = PipelineSettings.load()
    cfg.max_transfers_per_token = 2
    cfg.save()
    assert extract(confirmed) == 2
    confirmed.explosion.refresh_from_db()
    assert confirmed.explosion.extraction_status == Explosion.Extraction.PARTIAL


def test_buyer_window_limits_buy_pass(confirmed):
    DetectionSettings.objects.filter(chain=None).update(buyer_window_hours=9)
    hypersync = FakeHyperSync()
    assert extract(confirmed, hypersync) == 0
    start = int(POOL_CREATED.timestamp()) + HOUR
    assert hypersync.transfer_calls[0][1] == block_of(start)


def test_wallets_are_shared_between_explosions(confirmed):
    Wallet.objects.create(address=ALICE)
    extract(confirmed)
    assert Wallet.objects.filter(address=ALICE).count() == 1
