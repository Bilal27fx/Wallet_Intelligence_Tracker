from datetime import timedelta
from unittest.mock import patch

import pytest

from apps.discovery.models import PipelineSettings, Wallet
from apps.discovery.tests.factories import make_chain
from apps.wallets import tasks
from apps.wallets.models import WalletProfile
from apps.wallets.services.qualification import enqueue_profiles
from apps.wallets.tests.factories import make_early_buy
from apps.wallets.tests.fakes import BUYER, NOW, TOKEN_A
from integrations.errors import BudgetExhausted

pytestmark = pytest.mark.django_db


@pytest.fixture
def chain():
    return make_chain()


def test_enqueue_creates_profiles_with_chains(chain):
    extra = make_chain(gt_id="eth", evm_id=1, zerion_id="ethereum")
    cfg = PipelineSettings.load()
    cfg.extra_chains = ["eth"]
    cfg.save()
    make_early_buy(Wallet.objects.create(address=BUYER), chain, TOKEN_A)
    assert enqueue_profiles(NOW, cfg) == 1
    profile = WalletProfile.objects.get()
    assert sorted(profile.chains) == sorted([chain.pk, extra.pk])
    assert enqueue_profiles(NOW, cfg) == 0


def test_enqueue_reopens_filtered_wallet_after_delay_with_new_explosion(chain):
    wallet = Wallet.objects.create(address=BUYER)
    profile = WalletProfile.objects.create(
        wallet=wallet,
        chains=[chain.pk],
        status="filtered",
        filter_reason="inactive",
        analyzed_at=NOW - timedelta(days=40),
        next_analysis_at=NOW - timedelta(days=10),
    )
    make_early_buy(wallet, chain, TOKEN_A)
    enqueue_profiles(NOW, PipelineSettings.load())
    profile.refresh_from_db()
    assert (profile.status, profile.filter_reason) == ("pending", "")


def test_qualify_wallets_task_schedules_batch(chain):
    for i in range(3):
        WalletProfile.objects.create(
            wallet=Wallet.objects.create(address=f"0x{i:040x}"), chains=[chain.pk]
        )
    cfg = PipelineSettings.load()
    cfg.qualification_batch_size = 2
    cfg.save()
    with (
        patch.object(tasks, "sync_quote_assets", side_effect=BudgetExhausted("fini")),
        patch.object(tasks.clients, "zerion"),
        patch.object(tasks.qualify_wallet_task, "delay") as delay,
    ):
        result = tasks.qualify_wallets_task.apply().get()
    assert result == {"new_profiles": 0, "scheduled": 2}
    assert delay.call_count == 2


def test_failure_counts_attempts_then_filters(chain):
    profile = WalletProfile.objects.create(
        wallet=Wallet.objects.create(address=BUYER), chains=[chain.pk]
    )
    cfg = PipelineSettings.load()
    cfg.max_attempts = 2
    cfg.save()
    with (
        patch.object(tasks, "build_clients"),
        patch.object(tasks, "qualify_wallet", side_effect=RuntimeError("x")),
    ):
        tasks.qualify_wallet_task.apply(args=[profile.pk])
        profile.refresh_from_db()
        assert (profile.status, profile.attempts) == ("pending", 1)
        tasks.qualify_wallet_task.apply(args=[profile.pk])
    profile.refresh_from_db()
    assert (profile.status, profile.filter_reason) == ("filtered", "error:RuntimeError")


def test_budget_exhaustion_is_not_a_failure(chain):
    profile = WalletProfile.objects.create(
        wallet=Wallet.objects.create(address=BUYER), chains=[chain.pk]
    )
    with (
        patch.object(tasks, "build_clients"),
        patch.object(tasks, "qualify_wallet", side_effect=BudgetExhausted("x")),
    ):
        tasks.qualify_wallet_task.apply(args=[profile.pk])
    profile.refresh_from_db()
    assert (profile.status, profile.attempts) == ("pending", 0)
