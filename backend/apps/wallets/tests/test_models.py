import pytest
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django_celery_beat.models import PeriodicTask

from apps.discovery.models import PipelineSettings, Wallet
from apps.discovery.tests.factories import make_chain
from apps.wallets.models import (
    KnownAddress,
    QualificationSettings,
    TokenPosition,
    TokenTrade,
    WalletProfile,
    WalletTransaction,
)
from apps.wallets.services.settings import qualification_thresholds

pytestmark = pytest.mark.django_db


def test_global_defaults_from_migration():
    t = qualification_thresholds()
    assert (t.max_txs_per_day, t.history_days) == (50, 180)
    assert (t.big_receive_pct, t.transfer_after_buy_pct) == (30.0, 70.0)


def test_chain_override_and_inheritance():
    chain = make_chain()
    QualificationSettings.objects.create(chain=chain, history_days=90)
    t = qualification_thresholds(chain)
    assert t.history_days == 90
    assert t.max_txs_per_day == 50


def test_global_settings_require_every_threshold():
    with pytest.raises(ValidationError):
        QualificationSettings(chain=None, history_days=10).clean()


def test_daily_task_scheduled_at_8():
    task = PeriodicTask.objects.get(name="qualification-daily")
    assert task.task == "apps.wallets.tasks.qualify_wallets_task"
    assert (task.crontab.minute, task.crontab.hour) == ("0", "8")


def test_transfer_is_unique_per_transaction_index():
    wallet = Wallet.objects.create(address="0x" + "a" * 40)
    tx = WalletTransaction.objects.create(
        wallet=wallet,
        zerion_id="z1",
        chain="base",
        tx_hash="0xt",
        mined_at="2026-09-27T00:00Z",
        operation_type="trade",
        status="confirmed",
    )
    values = dict(
        transaction=tx,
        wallet=wallet,
        transfer_index=0,
        chain="base",
        token_address="native",
        kind="buy",
        direction="in",
        quantity=1,
        amount=1,
        mined_at="2026-09-27T00:00Z",
    )
    TokenTrade.objects.create(**values)
    with pytest.raises(IntegrityError), transaction.atomic():
        TokenTrade.objects.create(**values)


def test_position_balance():
    position = TokenPosition(bought_amount=10, received_amount=5, sold_amount=3, sent_amount=2)
    assert position.balance == 10


def test_blocking_known_addresses():
    chain = make_chain()
    KnownAddress.objects.create(chain=None, address="0x1", kind="exchange")
    KnownAddress.objects.create(chain=chain, address="0x2", kind="cex_deposit")
    KnownAddress.objects.create(chain=chain, address="0x3", kind="stablecoin")
    assert KnownAddress.objects.blocking_for("0x1", [chain.pk]).exists()
    assert KnownAddress.objects.blocking_for("0x2", [chain.pk]).exists()
    assert not KnownAddress.objects.blocking_for("0x3", [chain.pk]).exists()


def test_profile_defaults():
    profile = WalletProfile.objects.create(wallet=Wallet.objects.create(address="0x" + "b" * 40))
    assert (
        profile.status,
        profile.source,
        profile.depth,
        profile.tags,
        profile.history_complete,
    ) == (
        "pending",
        "early_buyer",
        0,
        [],
        False,
    )


def test_pipeline_v2_defaults():
    cfg = PipelineSettings.load()
    assert cfg.prefilter_chains == ["base", "robinhood", "bsc", "eth", "arc"]
    assert "USDC" in cfg.quote_symbols
    assert (cfg.zerion_daily_budget, cfg.zerion_requests_per_min, cfg.history_refresh_days) == (
        1800,
        300,
        7,
    )
