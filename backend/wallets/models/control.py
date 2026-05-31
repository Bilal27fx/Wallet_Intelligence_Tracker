"""Control panel models."""
from django.db import models


class PipelineControl(models.Model):
    """Dummy model for pipeline control panel."""

    class Meta:
        managed = False
        verbose_name = 'Pipeline Control Panel'
        verbose_name_plural = 'Pipeline Control Panel'
        app_label = 'wallets'
