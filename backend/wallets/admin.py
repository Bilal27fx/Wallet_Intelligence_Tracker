"""Wallets admin."""
from django.contrib import admin
from django.contrib import messages
from .models import (
    Wallet, Token, Transaction, WalletPositionChange,
    WalletBrute, TokenAnalytics, WalletTierPerformance,
    WalletQualified, SmartWallet, ConsensusSignal
)


# Admin actions to trigger pipelines
def run_scoring_pipeline_action(modeladmin, request, queryset):
    """Admin action to trigger scoring pipeline."""
    from wallets.tasks import run_scoring_pipeline
    result = run_scoring_pipeline.delay()
    messages.success(
        request,
        f'✓ Scoring pipeline started! Task ID: {result.id}'
    )
run_scoring_pipeline_action.short_description = "🚀 Run Scoring Pipeline (FIFO + Score + Tiers)"


def run_consensus_detection_action(modeladmin, request, queryset):
    """Admin action to trigger consensus detection."""
    from wallets.tasks import run_consensus_detection
    result = run_consensus_detection.delay()
    messages.success(
        request,
        f'✓ Consensus detection started! Task ID: {result.id}'
    )
run_consensus_detection_action.short_description = "🔍 Run Consensus Detection"


def sync_wallet_action(modeladmin, request, queryset):
    """Admin action to sync selected wallets from Zerion."""
    from wallets.tasks import sync_wallet_data
    count = 0
    for wallet in queryset:
        result = sync_wallet_data.delay(wallet.address)
        count += 1
    messages.success(
        request,
        f'✓ Syncing {count} wallet(s) from Zerion...'
    )
sync_wallet_action.short_description = "🔄 Sync selected wallets from Zerion"


@admin.register(Wallet)
class WalletAdmin(admin.ModelAdmin):
    list_display = ['address', 'period', 'total_portfolio_value', 'is_smart_wallet', 'updated_at']
    list_filter = ['period', 'is_smart_wallet']
    search_fields = ['address']
    readonly_fields = ['created_at', 'updated_at']
    actions = [
        run_scoring_pipeline_action,
        run_consensus_detection_action,
        sync_wallet_action,
    ]


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
