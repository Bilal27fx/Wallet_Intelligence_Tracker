"""Analytics serializers."""
from rest_framework import serializers
from wallets.models import (
    TokenAnalytics, WalletTierPerformance,
    WalletQualified, SmartWallet
)


class TokenAnalyticsSerializer(serializers.ModelSerializer):
    """Token analytics serializer."""
    wallet_address = serializers.CharField(source='wallet.address', read_only=True)

    class Meta:
        model = TokenAnalytics
        fields = [
            'id', 'wallet_address', 'token_symbol',
            'total_invested', 'total_realized', 'roi_percentage',
            'is_winning', 'status', 'holding_days', 'in_portfolio'
        ]


class WalletTierPerformanceSerializer(serializers.ModelSerializer):
    """Wallet tier performance serializer."""
    wallet_address = serializers.CharField(source='wallet.address', read_only=True)

    class Meta:
        model = WalletTierPerformance
        fields = [
            'id', 'wallet_address', 'tier_usd', 'roi_percentage',
            'winrate', 'nb_trades', 'nb_gagnants', 'is_optimal_tier'
        ]


class WalletQualifiedSerializer(serializers.ModelSerializer):
    """Wallet qualified serializer."""
    wallet_address = serializers.CharField(source='wallet.address', read_only=True)
    wallet_period = serializers.CharField(source='wallet.period', read_only=True)

    class Meta:
        model = WalletQualified
        fields = [
            'wallet_address', 'wallet_period', 'final_score',
            'classification', 'weighted_roi', 'taux_reussite', 'nb_trades'
        ]


class SmartWalletSerializer(serializers.ModelSerializer):
    """Smart wallet serializer."""
    wallet_address = serializers.CharField(source='wallet.address', read_only=True)
    wallet_period = serializers.CharField(source='wallet.period', read_only=True)
    total_portfolio_value = serializers.FloatField(source='wallet.total_portfolio_value', read_only=True)

    class Meta:
        model = SmartWallet
        fields = [
            'wallet_address', 'wallet_period', 'total_portfolio_value',
            'optimal_threshold_tier', 'quality_score', 'threshold_status',
            'optimal_roi', 'optimal_winrate', 'global_roi', 'global_winrate'
        ]
