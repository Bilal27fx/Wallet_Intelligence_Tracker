import pytest
from django_celery_beat.models import PeriodicTask

from apps.discovery.models import DetectionSettings, PipelineSettings
from apps.discovery.services.settings import Thresholds, thresholds_for
from apps.discovery.tests.factories import make_chain

pytestmark = pytest.mark.django_db


def test_migration_creates_global_defaults():
    thresholds = thresholds_for(make_chain())
    assert thresholds == Thresholds(
        min_change_24h_pct=50.0,
        min_liquidity_usd=10_000.0,
        min_volume_usd=50_000.0,
        peak_volume_window_hours=24,
        min_fdv_usd=100_000.0,
        max_fdv_usd=100_000_000.0,
        max_pool_age_hours=720,
        min_multiplier=5.0,
        min_retention_pct=30.0,
        confirmation_hours=24,
        sniper_blocks=3,
        min_buy_usd=500.0,
        max_buyers=300,
        explosion_window_hours=168,
        maturity_hours=336,
        breakout_multiplier=2.0,
        buyer_window_hours=0,
        min_score=5.0,
        max_multiplier=10_000.0,
        hub_min_senders=10,
        vault_follow_depth=2,
        bot_window_days=7,
        vault_min_pct=20.0,
    )


def test_chain_override_wins_and_empty_fields_inherit():
    chain = make_chain()
    DetectionSettings.objects.create(chain=chain, min_multiplier=3, max_buyers=0)
    thresholds = thresholds_for(chain)
    assert thresholds.min_multiplier == 3.0
    assert thresholds.max_buyers == 0
    assert thresholds.min_buy_usd == 500.0


def test_changes_are_read_at_each_call():
    chain = make_chain()
    DetectionSettings.objects.filter(chain=None).update(min_buy_usd=1000)
    assert thresholds_for(chain).min_buy_usd == 1000.0


def test_daily_periodic_task_exists():
    task = PeriodicTask.objects.get(name="discovery-daily")
    assert task.task == "apps.discovery.tasks.run_discovery"
    assert (task.crontab.minute, task.crontab.hour) == ("0", "6")
    assert task.enabled


def test_pipeline_defaults_for_explosion_v2():
    cfg = PipelineSettings.load()
    assert cfg.sell_pass_batch_size == 500
    assert cfg.max_transfers_per_token == 2_000_000
    assert cfg.zerion_operation_types == "trade,send,receive,execute,mint,burn,claim"
    assert cfg.token_info_refresh_days == 30
    assert float(cfg.rug_priority_weight) == 0.2
