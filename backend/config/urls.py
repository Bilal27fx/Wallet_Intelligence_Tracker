from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/core/", include("apps.core.urls")),
    path("api/discovery/", include("apps.discovery.urls")),
    path("api/wallets/", include("apps.wallets.urls")),
]
