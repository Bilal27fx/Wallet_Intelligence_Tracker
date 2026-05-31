"""Analytics views."""
from rest_framework import viewsets, filters
from wallets.models import (
    TokenAnalytics, WalletTierPerformance,
    WalletQualified, SmartWallet
)
from wallets.serializers import (
    TokenAnalyticsSerializer,
    WalletTierPerformanceSerializer,
    WalletQualifiedSerializer,
    SmartWalletSerializer,
)


class TokenAnalyticsViewSet(viewsets.ReadOnlyModelViewSet):
    """Token Analytics ViewSet."""
    queryset = TokenAnalytics.objects.all().select_related('wallet')
    serializer_class = TokenAnalyticsSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['token_symbol', 'wallet__address']
    ordering_fields = ['roi_percentage', 'total_invested', 'total_realized', 'holding_days']
    ordering = ['-roi_percentage']


class WalletTierPerformanceViewSet(viewsets.ReadOnlyModelViewSet):
    """Wallet Tier Performance ViewSet."""
    queryset = WalletTierPerformance.objects.all().select_related('wallet')
    serializer_class = WalletTierPerformanceSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['wallet__address']
    ordering_fields = ['tier_usd', 'roi_percentage', 'winrate', 'nb_trades']
    ordering = ['-roi_percentage']


class WalletQualifiedViewSet(viewsets.ReadOnlyModelViewSet):
    """Wallet Qualified ViewSet."""
    queryset = WalletQualified.objects.all().select_related('wallet')
    serializer_class = WalletQualifiedSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['wallet__address', 'classification']
    ordering_fields = ['final_score', 'weighted_roi', 'taux_reussite', 'nb_trades']
    ordering = ['-final_score']


class SmartWalletViewSet(viewsets.ReadOnlyModelViewSet):
    """Smart Wallet ViewSet - Top performing wallets."""
    queryset = SmartWallet.objects.all().select_related('wallet')
    serializer_class = SmartWalletSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['wallet__address', 'threshold_status']
    ordering_fields = ['quality_score', 'optimal_roi', 'optimal_winrate']
    ordering = ['-quality_score']
