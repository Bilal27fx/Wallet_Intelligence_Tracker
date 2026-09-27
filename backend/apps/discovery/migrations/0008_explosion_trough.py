from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [("discovery", "0007_pipeline_v2")]

    operations = [
        migrations.RenameField("explosion", "low_block", "trough_block"),
        migrations.RenameField("explosion", "low_at", "trough_at"),
    ]
