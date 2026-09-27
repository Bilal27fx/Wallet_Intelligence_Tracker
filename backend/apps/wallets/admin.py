"""Admin de la qualification (complété à la tâche 9)."""

from django.contrib import admin

from apps.wallets.models import KnownAddress, QualificationSettings

admin.site.register(KnownAddress)
admin.site.register(QualificationSettings)
