"""Admin de la qualification v2 : profils, transactions et mouvements Zerion, liens, réglages."""

import csv
import io

from django.contrib import admin, messages
from django.shortcuts import redirect
from django.template.response import TemplateResponse
from django.urls import path, reverse

from apps.wallets.forms import KnownAddressImportForm
from apps.wallets.models import (
    KnownAddress,
    PortfolioPosition,
    PortfolioSnapshot,
    QualificationSettings,
    TokenInfo,
    TokenPosition,
    TokenTrade,
    WalletLink,
    WalletProfile,
    WalletTransaction,
)
from apps.wallets.services.exchanges import import_known_addresses


class ReadOnlyAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(WalletProfile)
class WalletProfileAdmin(ReadOnlyAdmin):
    list_display = [
        "wallet",
        "status",
        "filter_reason",
        "source",
        "priority",
        "portfolio_value_usd",
        "linked_value_usd",
        "history_complete",
        "active_chains",
        "tags",
    ]
    list_filter = ["status", "filter_reason", "source", "history_complete"]
    search_fields = ["wallet__address"]
    ordering = ["-priority"]
    list_select_related = ["wallet"]


@admin.register(WalletTransaction)
class WalletTransactionAdmin(ReadOnlyAdmin):
    list_display = ["mined_at", "wallet", "chain", "operation_type", "tx_hash", "fee_usd"]
    list_filter = ["operation_type", "chain"]
    search_fields = ["wallet__address", "tx_hash"]
    ordering = ["-mined_at"]
    list_select_related = ["wallet"]


@admin.register(TokenTrade)
class TokenTradeAdmin(ReadOnlyAdmin):
    list_display = [
        "mined_at",
        "wallet",
        "chain",
        "token_symbol",
        "kind",
        "direction",
        "quantity",
        "price_usd",
        "value_usd",
        "counterparty",
    ]
    list_filter = ["kind", "is_internal", "chain"]
    search_fields = ["wallet__address", "token_address", "token_symbol", "transaction__tx_hash"]
    ordering = ["-mined_at"]
    list_select_related = ["wallet"]


@admin.register(TokenPosition)
class TokenPositionAdmin(ReadOnlyAdmin):
    list_display = [
        "wallet",
        "chain",
        "token_symbol",
        "buys",
        "sells",
        "bought_usd",
        "sold_usd",
        "first_at",
        "last_at",
    ]
    list_filter = ["chain"]
    search_fields = ["wallet__address", "token_address", "token_symbol"]
    ordering = ["-bought_usd"]


@admin.register(WalletLink)
class WalletLinkAdmin(ReadOnlyAdmin):
    list_display = [
        "from_wallet",
        "to_wallet",
        "kind",
        "source",
        "rejected",
        "evidence",
        "created_at",
    ]
    list_filter = ["kind", "source", "rejected"]
    search_fields = ["from_wallet__address", "to_wallet__address"]


@admin.register(KnownAddress)
class KnownAddressAdmin(admin.ModelAdmin):
    list_display = ["address", "chain", "kind", "label", "source", "created_at"]
    list_filter = ["kind", "source", "chain"]
    search_fields = ["address", "label"]

    def get_urls(self):
        custom = [
            path(
                "import/",
                self.admin_site.admin_view(self.import_view),
                name="wallets_knownaddress_import",
            )
        ]
        return custom + super().get_urls()

    def import_view(self, request):
        form = KnownAddressImportForm(request.POST or None, request.FILES or None)
        if request.method == "POST" and form.is_valid():
            text = io.TextIOWrapper(form.cleaned_data["file"].file, encoding="utf-8")
            count = import_known_addresses(list(csv.DictReader(text)))
            messages.success(request, f"{count} adresses importées.")
            return redirect(reverse("admin:wallets_knownaddress_changelist"))
        context = {**self.admin_site.each_context(request), "form": form}
        return TemplateResponse(request, "admin/wallets/knownaddress/import_csv.html", context)


@admin.register(QualificationSettings)
class QualificationSettingsAdmin(admin.ModelAdmin):
    list_display = [
        "__str__",
        "min_portfolio_usd",
        "history_days",
        "max_txs_per_day",
        "big_receive_pct",
    ]


@admin.register(TokenInfo)
class TokenInfoAdmin(ReadOnlyAdmin):
    list_display = ["symbol", "chain", "address", "total_supply", "verified", "fetched_at"]
    list_filter = ["verified", "chain"]
    search_fields = ["symbol", "address", "fungible_id"]


class PortfolioPositionInline(admin.TabularInline):
    model = PortfolioPosition
    fields = ["chain", "symbol", "token_address", "position_type", "quantity", "value_usd"]
    readonly_fields = fields
    extra = 0
    can_delete = False

    def has_add_permission(self, request, obj=None):
        return False


@admin.register(PortfolioSnapshot)
class PortfolioSnapshotAdmin(ReadOnlyAdmin):
    list_display = ["wallet", "fetched_at", "total_usd"]
    search_fields = ["wallet__address"]
    exclude = ["raw"]
    inlines = [PortfolioPositionInline]
