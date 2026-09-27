from django.db import migrations


def update_defaults(apps, schema_editor):
    apps.get_model("wallets", "QualificationSettings").objects.filter(chain=None).update(
        max_txs_per_day=50, history_days=180, big_receive_pct=30
    )
    apps.get_model("discovery", "PipelineSettings").objects.filter(pk=1).update(
        zerion_daily_budget=1800, zerion_requests_per_min=300
    )


class Migration(migrations.Migration):
    dependencies = [("wallets", "0005_qualification_v2"), ("discovery", "0007_pipeline_v2")]
    operations = [migrations.RunPython(update_defaults, migrations.RunPython.noop)]
