"""Réglages globaux par défaut et tâche quotidienne. Ensuite, tout se gère dans l'admin."""

from django.db import migrations

DETECTION_DEFAULTS = {
    "min_change_24h_pct": 50,
    "min_liquidity_usd": 10_000,
    "min_volume_usd": 50_000,
    "peak_volume_window_hours": 24,
    "min_fdv_usd": 100_000,
    "max_fdv_usd": 100_000_000,
    "max_pool_age_hours": 720,
    "min_multiplier": 5,
    "min_retention_pct": 30,
    "confirmation_hours": 24,
    "confirmation_timeout_hours": 168,
    "sniper_blocks": 3,
    "min_buy_usd": 500,
    "max_buyers": 300,
}


def create_defaults(apps, schema_editor):
    DetectionSettings = apps.get_model("discovery", "DetectionSettings")
    CrontabSchedule = apps.get_model("django_celery_beat", "CrontabSchedule")
    PeriodicTask = apps.get_model("django_celery_beat", "PeriodicTask")

    DetectionSettings.objects.get_or_create(chain=None, defaults=DETECTION_DEFAULTS)
    schedule, _ = CrontabSchedule.objects.get_or_create(
        minute="0",
        hour="6",
        day_of_week="*",
        day_of_month="*",
        month_of_year="*",
        timezone="UTC",
    )
    PeriodicTask.objects.get_or_create(
        name="discovery-daily",
        defaults={"task": "apps.discovery.tasks.run_discovery", "crontab": schedule},
    )


def remove_defaults(apps, schema_editor):
    apps.get_model("django_celery_beat", "PeriodicTask").objects.filter(
        name="discovery-daily"
    ).delete()
    apps.get_model("discovery", "DetectionSettings").objects.filter(chain=None).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("discovery", "0001_initial"),
        ("django_celery_beat", "0019_alter_periodictasks_options"),
    ]

    operations = [migrations.RunPython(create_defaults, remove_defaults)]
