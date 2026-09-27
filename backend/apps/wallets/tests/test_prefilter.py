from datetime import timedelta

import pytest

from apps.discovery.models import PipelineSettings, Wallet
from apps.discovery.tests.factories import make_chain
from apps.wallets.models import KnownAddress, WalletProfile
from apps.wallets.services.qualification import Clients, prefilter_chains, prefilter_step
from apps.wallets.tests.factories import make_early_buy
from apps.wallets.tests.fakes import (
    BUYER,
    NOW,
    POOL,
    ROUTER,
    START,
    TOKEN_A,
    UNIT,
    FakeWalletHyperSync,
    FakeZerion,
    tr,
)

pytestmark = pytest.mark.django_db


@pytest.fixture
def chain():
    return make_chain()


def profile(address=BUYER, **kw):
    wallet, _ = Wallet.objects.get_or_create(address=address)
    return WalletProfile.objects.create(wallet=wallet, **kw)


def run(p, hs=None):
    return prefilter_step(
        p,
        Clients(lambda c: hs or FakeWalletHyperSync(), FakeZerion()),
        NOW,
        PipelineSettings.load(),
    )


def test_prefilter_chains_add_spotted_chain(chain):
    arb = make_chain(gt_id="arbitrum", evm_id=42161, zerion_id="arbitrum")
    make_chain(gt_id="unused", evm_id=99, zerion_id="unused")
    p = profile()
    make_early_buy(p.wallet, arb, TOKEN_A)
    assert sorted(c.gt_id for c in prefilter_chains(p, PipelineSettings.load())) == [
        "arbitrum",
        "base",
    ]


def test_clean_wallet_passes_and_records_active_chains(chain):
    p = profile()
    assert run(p) == "prefiltered"
    assert p.active_chains == ["base"]
    assert p.metrics["prefilter"]["base"] == {"txs_7d": 10, "txs_active": 5}


def test_bot_on_totals(chain):
    p = profile()
    assert run(p, FakeWalletHyperSync(tx_counts={BUYER: 400})) == "filtered"
    assert p.filter_reason == "bot_frequency"
    assert p.next_analysis_at == NOW + timedelta(days=30)


def test_farmer(chain):
    many = [
        tr(START + i, f"0xf{i}", f"0x{i:040x}", POOL, BUYER, UNIT, POOL, f"0x{i:040x}")
        for i in range(301)
    ]
    p = profile()
    run(p, FakeWalletHyperSync(transfers={BUYER: many}))
    assert p.filter_reason == "farmer"


def test_mev_bot(chain):
    round_trips = []
    for i in range(3):
        token = f"0x{i + 1:040x}"
        round_trips += [
            tr(START + i, f"0xb{i}", token, POOL, BUYER, UNIT, BUYER, ROUTER),
            tr(START + i, f"0xs{i}", token, BUYER, POOL, UNIT, BUYER, ROUTER),
        ]
    p = profile()
    run(p, FakeWalletHyperSync(transfers={BUYER: round_trips}))
    assert p.filter_reason == "bot_mev"


def test_known_exchange(chain):
    KnownAddress.objects.create(address=BUYER, kind="exchange")
    p = profile()
    run(p)
    assert p.filter_reason == "exchange"


def test_inactive_only_for_early_buyers(chain):
    quiet = FakeWalletHyperSync(tx_counts={BUYER: 1}, transfers={})
    assert run(profile(), quiet) == "filtered"
