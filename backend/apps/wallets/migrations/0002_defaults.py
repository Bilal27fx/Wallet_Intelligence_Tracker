"""Seuils globaux par défaut et tâche quotidienne de qualification. Ensuite, tout se gère dans l'admin."""

from django.db import migrations

DEFAULTS = {
    "max_txs_per_day": 200,
    "max_distinct_tokens": 300,
    "inactive_days": 90,
    "min_txs_active": 5,
    "history_days": 365,
    "max_mev_ratio": 30,
    "min_distinct_buys": 3,
    "min_portfolio_usd": 10_000,
    "max_portfolio_usd": 50_000_000,
    "transfer_after_buy_pct": 70,
    "follow_depth": 1,
    "funder_max_wallets": 50,
    "hot_wallet_min_counterparties": 1000,
    "deposit_forward_pct": 90,
    "deposit_forward_hours": 24,
    "flipper_hours": 24,
    "flipper_min_sold_pct": 80,
    "flipper_min_share_pct": 50,
    "holder_min_pct": 50,
    "accumulator_max_out_pct": 20,
    "accumulator_min_positions": 2,
    "early_buyer_min_explosions": 2,
    "refilter_after_days": 30,
}


def create_defaults(apps, schema_editor):
    Settings = apps.get_model("wallets", "QualificationSettings")
    CrontabSchedule = apps.get_model("django_celery_beat", "CrontabSchedule")
    PeriodicTask = apps.get_model("django_celery_beat", "PeriodicTask")
    Settings.objects.get_or_create(chain=None, defaults=DEFAULTS)
    schedule, _ = CrontabSchedule.objects.get_or_create(
        minute="0", hour="8", day_of_week="*", day_of_month="*", month_of_year="*", timezone="UTC"
    )
    PeriodicTask.objects.get_or_create(
        name="qualification-daily",
        defaults={"task": "apps.wallets.tasks.qualify_wallets_task", "crontab": schedule},
    )


def remove_defaults(apps, schema_editor):
    apps.get_model("django_celery_beat", "PeriodicTask").objects.filter(
        name="qualification-daily"
    ).delete()
    apps.get_model("wallets", "QualificationSettings").objects.filter(chain=None).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("wallets", "0001_initial"),
        ("django_celery_beat", "0019_alter_periodictasks_options"),
    ]

    operations = [migrations.RunPython(create_defaults, remove_defaults)]
