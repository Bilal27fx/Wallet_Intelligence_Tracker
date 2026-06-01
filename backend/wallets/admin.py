"""Wallets admin."""
from django.contrib import admin
from django.contrib import messages
from django.shortcuts import redirect
from django.urls import path
from django.template.response import TemplateResponse
from django.utils.html import format_html
from .models import (
    Wallet, Token, Transaction, WalletPositionChange,
    WalletBrute, TokenAnalytics, WalletTierPerformance,
    WalletQualified, SmartWallet, ConsensusSignal, PipelineControl,
    ExplosiveToken, TokenPriceHistory
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


@admin.register(PipelineControl)
class PipelineControlAdmin(admin.ModelAdmin):
    """Custom admin for pipeline control panel with big buttons."""

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def changelist_view(self, request, extra_context=None):
        """Custom control panel view with big buttons."""
        from django_celery_beat.models import PeriodicTask
        from django_celery_results.models import TaskResult

        # Get scheduled tasks
        scheduled_tasks = PeriodicTask.objects.filter(enabled=True).order_by('name')

        # Get recent task executions (last 20)
        recent_tasks = TaskResult.objects.select_related().order_by('-date_created')[:20]

        context = {
            **self.admin_site.each_context(request),
            'title': 'Pipeline Control Panel',
            'scheduled_tasks': scheduled_tasks,
            'recent_tasks': recent_tasks,
            'opts': self.model._meta,
        }

        return TemplateResponse(
            request,
            'admin/wallets/pipeline_control_panel.html',
            context
        )

    def get_urls(self):
        """Add custom URLs for pipeline execution."""
        urls = super().get_urls()
        custom_urls = [
            path('run_all/', self.admin_site.admin_view(self.run_all_pipelines_view), name='run_all_pipelines'),
            path('run_discovery/', self.admin_site.admin_view(self.run_discovery_view), name='run_discovery_pipeline'),
            path('run_wallet_initializer/', self.admin_site.admin_view(self.run_wallet_initializer_view), name='run_wallet_initializer'),
            path('run_tracking/', self.admin_site.admin_view(self.run_tracking_view), name='run_tracking_pipeline'),
            path('run_scoring/', self.admin_site.admin_view(self.run_scoring_view), name='run_scoring_pipeline'),
            path('run_consensus/', self.admin_site.admin_view(self.run_consensus_view), name='run_consensus_detection'),
        ]
        return custom_urls + urls

    def run_all_pipelines_view(self, request):
        """Execute all pipelines in sequence."""
        from celery import chain
        from wallets.tasks import run_discovery_pipeline, run_wallet_initialization, run_scoring_pipeline, run_consensus_detection

        temporality = request.GET.get('temporality', '14d')

        # Chain pipelines to run in sequence
        workflow = chain(
            run_discovery_pipeline.s(temporality=temporality),
            run_wallet_initialization.s(),
            run_scoring_pipeline.s(),
            run_consensus_detection.s()
        )

        result = workflow.apply_async()

        messages.success(
            request,
            format_html(
                '✓ <strong>Tous les pipelines</strong> lancés en séquence!<br>'
                'Workflow ID: <code>{}</code><br>'
                'Ordre: Discovery → Wallet Initializer → Scoring → Consensus',
                result.id
            )
        )
        return redirect('admin:wallets_pipelinecontrol_changelist')

    def run_discovery_view(self, request):
        """Execute discovery pipeline."""
        from wallets.tasks import run_discovery_pipeline
        temporality = request.GET.get('temporality', '14d')
        result = run_discovery_pipeline.delay(temporality=temporality)
        messages.success(
            request,
            format_html(
                '✓ <strong>Discovery Pipeline</strong> lancé avec succès ({})!<br>Task ID: <code>{}</code>',
                temporality,
                result.id
            )
        )
        return redirect('admin:wallets_pipelinecontrol_changelist')

    def run_wallet_initializer_view(self, request):
        """Execute wallet initializer."""
        from wallets.tasks import run_wallet_initialization
        result = run_wallet_initialization.delay()
        messages.success(
            request,
            format_html(
                '✓ <strong>Wallet Initializer</strong> lancé avec succès!<br>'
                'Task ID: <code>{}</code><br>'
                'Initialise les wallets depuis wallet_brute',
                result.id
            )
        )
        return redirect('admin:wallets_pipelinecontrol_changelist')

    def run_tracking_view(self, request):
        """Execute tracking pipeline."""
        from wallets.tasks import run_tracking_pipeline
        result = run_tracking_pipeline.delay()
        messages.success(
            request,
            format_html(
                '✓ <strong>Tracking Pipeline</strong> lancé avec succès!<br>Task ID: <code>{}</code>',
                result.id
            )
        )
        return redirect('admin:wallets_pipelinecontrol_changelist')

    def run_scoring_view(self, request):
        """Execute scoring pipeline."""
        from wallets.tasks import run_scoring_pipeline
        result = run_scoring_pipeline.delay()
        messages.success(
            request,
            format_html(
                '✓ <strong>Scoring Pipeline</strong> lancé avec succès!<br>Task ID: <code>{}</code>',
                result.id
            )
        )
        return redirect('admin:wallets_pipelinecontrol_changelist')

    def run_consensus_view(self, request):
        """Execute consensus detection."""
        from wallets.tasks import run_consensus_detection
        result = run_consensus_detection.delay()
        messages.success(
            request,
            format_html(
                '✓ <strong>Consensus Detection</strong> lancé avec succès!<br>Task ID: <code>{}</code>',
                result.id
            )
        )
        return redirect('admin:wallets_pipelinecontrol_changelist')


@admin.register(ExplosiveToken)
class ExplosiveTokenAdmin(admin.ModelAdmin):
    list_display = ['symbol', 'token_address', 'chain', 'price_change_24h', 'volume_24h', 'fdv', 'detected_at']
    list_filter = ['chain', 'traite', 'detected_at']
    search_fields = ['symbol', 'token_address']
    date_hierarchy = 'detected_at'
    readonly_fields = ['detected_at']


@admin.register(TokenPriceHistory)
class TokenPriceHistoryAdmin(admin.ModelAdmin):
    list_display = ['token_address', 'chain', 'date', 'close', 'volume']
    list_filter = ['chain', 'date']
    search_fields = ['token_address']
    date_hierarchy = 'date'
