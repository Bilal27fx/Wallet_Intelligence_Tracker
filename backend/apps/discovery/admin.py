"""Admin de la découverte : réglages éditables, résultats en lecture seule, ajout manuel."""

from django.contrib import admin, messages
from django.shortcuts import redirect
from django.template.response import TemplateResponse
from django.urls import path, reverse

from apps.discovery.forms import ManualCandidateForm
from apps.discovery.models import (
    Candidate,
    Chain,
    DetectionSettings,
    EarlyBuyer,
    Explosion,
    PipelineSettings,
    Wallet,
)
from apps.discovery.services import clients
from apps.discovery.services.candidates import add_manual_candidate


class ReadOnlyAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(Chain)
class ChainAdmin(admin.ModelAdmin):
    list_display = ["gt_id", "name", "evm_id", "zerion_id", "hypersync_supported", "is_enabled"]
    list_editable = ["is_enabled"]
    list_filter = ["is_enabled", "hypersync_supported"]
    search_fields = ["gt_id", "name"]
    readonly_fields = [
        "gt_id",
        "name",
        "evm_id",
        "zerion_id",
        "hypersync_supported",
        "rpc_url",
        "native_fungible_id",
        "wrapped_fungible_id",
        "updated_at",
    ]

    def has_add_permission(self, request):
        return False


@admin.register(DetectionSettings)
class DetectionSettingsAdmin(admin.ModelAdmin):
    list_display = ["__str__", "min_multiplier", "min_buy_usd", "max_buyers"]


@admin.register(PipelineSettings)
class PipelineSettingsAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        return not PipelineSettings.objects.exists()

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(Candidate)
class CandidateAdmin(admin.ModelAdmin):
    list_display = [
        "token",
        "status",
        "rejection_reason",
        "attempts",
        "next_check_at",
        "created_at",
    ]
    list_filter = ["status", "token__chain", "rejection_reason"]
    search_fields = ["token__address", "token__symbol"]
    list_select_related = ["token__chain"]
    readonly_fields = ["token", "sources", "metrics", "created_at", "updated_at"]

    def has_add_permission(self, request):
        return False

    def get_urls(self):
        custom = [
            path(
                "add-manual/",
                self.admin_site.admin_view(self.add_manual_view),
                name="discovery_candidate_add_manual",
            )
        ]
        return custom + super().get_urls()

    def add_manual_view(self, request):
        form = ManualCandidateForm(request.POST or None)
        error = None
        if request.method == "POST" and form.is_valid():
            gt = clients.geckoterminal(PipelineSettings.load())
            try:
                add_manual_candidate(form.cleaned_data["chain"], form.cleaned_data["address"], gt)
            except ValueError as exc:
                error = str(exc)
            else:
                messages.success(request, "Token ajouté : il sera analysé au prochain passage.")
                return redirect(reverse("admin:discovery_candidate_changelist"))
        context = {**self.admin_site.each_context(request), "form": form, "error": error}
        return TemplateResponse(request, "admin/discovery/candidate/add_manual.html", context)


@admin.register(Explosion)
class ExplosionAdmin(ReadOnlyAdmin):
    list_display = ["candidate", "multiplier", "retention_pct", "low_at", "peak_at"]
    list_select_related = ["candidate__token__chain"]


@admin.register(EarlyBuyer)
class EarlyBuyerAdmin(ReadOnlyAdmin):
    list_display = ["wallet", "explosion", "bought_usd", "is_sniper", "first_buy_at"]
    list_filter = ["is_sniper"]
    ordering = ["-bought_usd"]
    search_fields = ["wallet__address"]
    list_select_related = ["wallet", "explosion__candidate__token__chain"]


@admin.register(Wallet)
class WalletAdmin(ReadOnlyAdmin):
    search_fields = ["address"]
