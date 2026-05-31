"""Wallets API URLs."""
from rest_framework.routers import DefaultRouter
from wallets.views import (
    WalletViewSet,
    TokenViewSet,
    TransactionViewSet,
    WalletPositionChangeViewSet,
    TokenAnalyticsViewSet,
    WalletTierPerformanceViewSet,
    WalletQualifiedViewSet,
    SmartWalletViewSet,
    ConsensusSignalViewSet,
)

router = DefaultRouter()

# Wallet endpoints
router.register(r'wallets', WalletViewSet, basename='wallet')
router.register(r'tokens', TokenViewSet, basename='token')
router.register(r'transactions', TransactionViewSet, basename='transaction')
router.register(r'position-changes', WalletPositionChangeViewSet, basename='position-change')

# Analytics endpoints
router.register(r'analytics/tokens', TokenAnalyticsViewSet, basename='token-analytics')
router.register(r'analytics/tiers', WalletTierPerformanceViewSet, basename='tier-performance')
router.register(r'analytics/qualified', WalletQualifiedViewSet, basename='wallet-qualified')
router.register(r'analytics/smart-wallets', SmartWalletViewSet, basename='smart-wallet')

# Consensus endpoints
router.register(r'consensus/signals', ConsensusSignalViewSet, basename='consensus-signal')

urlpatterns = router.urls
