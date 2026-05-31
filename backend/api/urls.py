"""API URLs."""
from django.urls import path, include

urlpatterns = [
    path('', include('wallets.urls')),
]
