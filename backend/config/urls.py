"""URL configuration for WIT V1."""
from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    # path('api/', include('api.urls')),  # Task 9: Create API endpoints
]
