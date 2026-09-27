import pytest
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django_celery_beat.models import PeriodicTask

from apps.discovery.models import Wallet
from apps.discovery.tests.factories import make_chain, make_token
from apps.wallets.models import (
    KnownAddress,
    QualificationSettings,
    TokenPosition,
    TokenTrade,
    WalletProfile,
)
from apps.wallets.services.settings import qualification_thresholds

pytestmark = pytest.mark.django_db


def test_global_defaults_from_migration():
    t = qualification_thresholds()
    assert t.max_txs_per_day == 200
    assert t.history_days == 365
    assert t.min_portfolio_usd == 10_000.0
    assert t.transfer_after_buy_pct == 70.0
    assert t.accumulator_min_positions == 2


def test_chain_override_and_inheritance():
    chain = make_chain()
    QualificationSettings.objects.create(chain=chain, history_days=90)
    t = qualification_thresholds(chain)
    assert t.history_days == 90
    assert t.max_txs_per_day == 200


def test_global_settings_require_every_threshold():
    with pytest.raises(ValidationError):
        QualificationSettings(chain=None, history_days=10).clean()


def test_daily_task_scheduled_at_8():
    task = PeriodicTask.objects.get(name="qualification-daily")
    assert task.task == "apps.wallets.tasks.qualify_wallets_task"
    assert (task.crontab.minute, task.crontab.hour) == ("0", "8")


def test_trade_is_unique_per_log_and_wallet():
    token = make_token()
    wallet = Wallet.objects.create(address="0x" + "a" * 40)
    values = dict(
        wallet=wallet,
        token=token,
        kind="buy",
        amount=1,
        block=1,
        at="2026-09-27T00:00Z",
        tx_hash="0xt",
        log_index=0,
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
    assert (profile.status, profile.source, profile.depth, profile.tags) == (
        "pending",
        "early_buyer",
        0,
        [],
    )
