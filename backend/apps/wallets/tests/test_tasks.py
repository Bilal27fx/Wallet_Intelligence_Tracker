from datetime import timedelta
from unittest.mock import patch

import pytest

from apps.discovery.models import PipelineSettings, Wallet
from apps.discovery.tests.factories import make_chain
from apps.wallets import tasks
from apps.wallets.models import WalletProfile
from apps.wallets.services.qualification import (
    Clients,
    compute_priority,
    enqueue_profiles,
    qualify_wallet,
    refresh_wallet,
)
from apps.wallets.tests.factories import make_early_buy
from apps.wallets.tests.fakes import (
    BUYER,
    NOW,
    POOL,
    SENDER,
    TOKEN_A,
    TOKEN_B,
    VAULT,
    FakeWalletHyperSync,
    FakeZerion,
    buyer_zerion_history,
    ztransfer,
    ztx,
)
from integrations.errors import BudgetExhausted

pytestmark = pytest.mark.django_db


@pytest.fixture
def chain():
    return make_chain()


def test_priority_prefers_more_explosions(chain):
    one = Wallet.objects.create(address="0x" + "1" * 40)
    two = Wallet.objects.create(address="0x" + "2" * 40)
    make_early_buy(one, chain, TOKEN_A)
    make_early_buy(two, chain, TOKEN_A)
    make_early_buy(two, chain, TOKEN_B)
    cfg = PipelineSettings.load()
    assert compute_priority(two, cfg) > compute_priority(one, cfg) > 0


def test_priority_weights_rug_explosions_down(chain):
    held = Wallet.objects.create(address="0x" + "1" * 40)
    rugged = Wallet.objects.create(address="0x" + "2" * 40)
    make_early_buy(held, chain, TOKEN_A)
    make_early_buy(rugged, chain, TOKEN_B, retention_status="rug")
    cfg = PipelineSettings.load()
    assert compute_priority(held, cfg) > compute_priority(rugged, cfg) > 0


def test_enqueue_creates_profiles_and_reopens_filtered(chain):
    wallet = Wallet.objects.create(address=BUYER)
    make_early_buy(wallet, chain, TOKEN_A)
    assert enqueue_profiles(NOW, PipelineSettings.load()) == 1
    profile = WalletProfile.objects.get()
    assert profile.priority > 0
    WalletProfile.objects.filter(pk=profile.pk).update(
        status="filtered",
        filter_reason="inactive",
        analyzed_at=NOW - timedelta(days=40),
        next_analysis_at=NOW - timedelta(days=10),
    )
    make_early_buy(wallet, chain, TOKEN_B)
    enqueue_profiles(NOW, PipelineSettings.load())
    profile.refresh_from_db()
    assert (profile.status, profile.history_complete) == ("pending", False)


def test_full_pipeline_for_buyer_then_linked_wallets(chain):
    clients = Clients(lambda c: FakeWalletHyperSync(), FakeZerion())
    cfg = PipelineSettings.load()
    buyer = WalletProfile.objects.create(wallet=Wallet.objects.create(address=BUYER))
    assert qualify_wallet(buyer, clients, NOW, cfg) == "filtered"
    for address in (VAULT, SENDER):
        linked = WalletProfile.objects.get(wallet__address=address)
        assert qualify_wallet(linked, clients, NOW, cfg) == "valued"
    buyer.refresh_from_db()
    assert buyer.status == "qualified"


def test_scheduling_order(chain):
    cfg = PipelineSettings.load()
    cfg.qualification_batch_size = 4
    cfg.save()

    def mk(address, **kw):
        return WalletProfile.objects.create(wallet=Wallet.objects.create(address=address), **kw).pk

    new_low = mk("0x" + "1" * 40, priority=1)
    new_high = mk("0x" + "2" * 40, priority=5)
    in_progress = mk("0x" + "3" * 40, status="prefiltered", priority=0)
    linked = mk("0x" + "4" * 40, source="linked", depth=1)
    stale = mk(
        "0x" + "5" * 40,
        status="qualified",
        history_complete=True,
        analyzed_at=NOW - timedelta(days=8),
    )
    ids, refresh = tasks.scheduled_profiles(NOW, cfg)
    assert ids == [linked, in_progress, new_high, new_low]
    assert refresh == []
    cfg.qualification_batch_size = 10
    cfg.save()
    assert tasks.scheduled_profiles(NOW, cfg)[1] == [stale]


def test_refresh_fetches_only_new_transactions(chain):
    zerion = FakeZerion()
    clients = Clients(lambda c: FakeWalletHyperSync(), zerion)
    cfg = PipelineSettings.load()
    buyer = WalletProfile.objects.create(wallet=Wallet.objects.create(address=BUYER))
    qualify_wallet(buyer, clients, NOW, cfg)
    newer = ztx("z6", 0, "trade", [ztransfer(0, "in", TOKEN_B, "B", 10, 50.0, POOL)], 106)
    zerion.histories[BUYER] = [newer] + buyer_zerion_history()
    before = zerion.calls["transactions"]
    refresh_wallet(buyer, clients, NOW, cfg)
    assert zerion.calls["transactions"] - before == 1
    assert buyer.wallet.transactions.count() == 7


def test_task_failure_and_budget(chain):
    profile = WalletProfile.objects.create(wallet=Wallet.objects.create(address=BUYER))
    cfg = PipelineSettings.load()
    cfg.max_attempts = 2
    cfg.save()
    with (
        patch.object(tasks, "build_clients"),
        patch.object(tasks, "qualify_wallet", side_effect=BudgetExhausted("x")),
    ):
        tasks.qualify_wallet_task.apply(args=[profile.pk])
    profile.refresh_from_db()
    assert (profile.status, profile.attempts) == ("pending", 0)
    with (
        patch.object(tasks, "build_clients"),
        patch.object(tasks, "qualify_wallet", side_effect=RuntimeError("x")),
    ):
        tasks.qualify_wallet_task.apply(args=[profile.pk])
        tasks.qualify_wallet_task.apply(args=[profile.pk])
    profile.refresh_from_db()
    assert (profile.status, profile.filter_reason) == ("filtered", "error:RuntimeError")


def test_daily_task_schedules_subtasks(chain):
    WalletProfile.objects.create(wallet=Wallet.objects.create(address=BUYER))
    with (
        patch.object(tasks.qualify_wallet_task, "delay") as delay,
        patch.object(tasks.refresh_wallet_task, "delay"),
    ):
        result = tasks.qualify_wallets_task.apply().get()
    assert result == {"new_profiles": 0, "scheduled": 1, "refresh": 0}
    delay.assert_called_once()


def test_build_clients_reuses_one_hypersync_client_per_chain(chain):
    with (
        patch.object(tasks.clients, "zerion"),
        patch.object(tasks.clients, "hypersync", side_effect=lambda c, cfg: object()),
    ):
        built = tasks.build_clients(PipelineSettings.load())
        assert built.hypersync_for(chain) is built.hypersync_for(chain)
