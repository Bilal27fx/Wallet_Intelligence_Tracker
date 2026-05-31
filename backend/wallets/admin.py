"""Wallets admin."""
from django.contrib import admin
from .models import (
    Wallet, Token, Transaction, WalletPositionChange,
    WalletBrute, TokenAnalytics, WalletTierPerformance,
    WalletQualified, SmartWallet, ConsensusSignal
)


@admin.register(Wallet)
class WalletAdmin(admin.ModelAdmin):
    list_display = ['address', 'period', 'total_portfolio_value', 'is_smart_wallet', 'updated_at']
    list_filter = ['period', 'is_smart_wallet']
    search_fields = ['address']
    readonly_fields = ['created_at', 'updated_at']


@admin.register(Token)
class TokenAdmin(admin.ModelAdmin):
    list_display = ['symbol', 'wallet', 'amount', 'usd_value', 'chain', 'in_portfolio']
    list_filter = ['chain', 'in_portfolio']
    search_fields = ['symbol', 'contract_address', 'wallet__address']


@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    list_display = ['symbol', 'action_type', 'quantity', 'total_value_usd', 'date', 'wallet']
    list_filter = ['action_type', 'operation_type', 'date']
    search_fields = ['symbol', 'hash', 'wallet__address']
    date_hierarchy = 'date'


@admin.register(WalletPositionChange)
class WalletPositionChangeAdmin(admin.ModelAdmin):
    list_display = ['symbol', 'change_type', 'usd_change', 'wallet', 'detected_at']
    list_filter = ['change_type', 'detected_at']
    search_fields = ['symbol', 'wallet__address']


@admin.register(WalletBrute)
class WalletBruteAdmin(admin.ModelAdmin):
    list_display = ['wallet_address', 'temporality', 'chain']
    list_filter = ['temporality', 'chain']
    search_fields = ['wallet_address']


@admin.register(TokenAnalytics)
class TokenAnalyticsAdmin(admin.ModelAdmin):
    list_display = ['token_symbol', 'wallet', 'roi_percentage', 'status', 'is_winning']
    list_filter = ['status', 'is_winning', 'in_portfolio']
    search_fields = ['token_symbol', 'wallet__address']


@admin.register(WalletTierPerformance)
class WalletTierPerformanceAdmin(admin.ModelAdmin):
    list_display = ['wallet', 'tier_usd', 'roi_percentage', 'winrate', 'is_optimal_tier']
    list_filter = ['tier_usd', 'is_optimal_tier']


@admin.register(WalletQualified)
class WalletQualifiedAdmin(admin.ModelAdmin):
    list_display = ['wallet', 'classification', 'final_score', 'weighted_roi', 'taux_reussite']
    list_filter = ['classification']
    search_fields = ['wallet__address']


@admin.register(SmartWallet)
class SmartWalletAdmin(admin.ModelAdmin):
    list_display = ['wallet', 'threshold_status', 'optimal_threshold_tier', 'quality_score', 'optimal_roi']
    list_filter = ['threshold_status']
    search_fields = ['wallet__address']


@admin.register(ConsensusSignal)
class ConsensusSignalAdmin(admin.ModelAdmin):
    list_display = ['token_symbol', 'nb_wallets', 'total_usd_invested', 'market_cap', 'detected_at']
    list_filter = ['detected_at', 'chain']
    search_fields = ['token_symbol', 'contract_address']
    date_hierarchy = 'detected_at'
    filter_horizontal = ['wallets']
