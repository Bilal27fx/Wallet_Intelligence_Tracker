from django.db import migrations


def reset(apps, schema_editor):
    apps.get_model("wallets", "WalletLink").objects.all().delete()
    apps.get_model("wallets", "TokenTrade").objects.all().delete()
    apps.get_model("wallets", "TokenPosition").objects.all().delete()
    Profile = apps.get_model("wallets", "WalletProfile")
    Profile.objects.filter(source="linked").delete()
    Profile.objects.update(
        status="pending", filter_reason="", tags=[], portfolio_value_usd=None, metrics={},
        attempts=0, analyzed_at=None, next_analysis_at=None, entity=None,
    )
    apps.get_model("wallets", "Entity").objects.all().delete()


class Migration(migrations.Migration):
    dependencies = [("wallets", "0002_defaults")]
    operations = [migrations.RunPython(reset, migrations.RunPython.noop)]
