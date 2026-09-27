"""Modèles de la découverte : chaînes, réglages, tokens, candidats, explosions, acheteurs."""

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import Q

from integrations.zerion import OPERATION_TYPES

THRESHOLD_FIELDS = (
    "min_change_24h_pct",
    "min_liquidity_usd",
    "min_volume_usd",
    "peak_volume_window_hours",
    "min_fdv_usd",
    "max_fdv_usd",
    "max_pool_age_hours",
    "min_multiplier",
    "min_retention_pct",
    "confirmation_hours",
    "sniper_blocks",
    "min_buy_usd",
    "max_buyers",
    "explosion_window_hours",
    "maturity_hours",
    "breakout_multiplier",
    "buyer_window_hours",
    "min_score",
    "max_multiplier",
    "hub_min_senders",
    "vault_follow_depth",
    "bot_window_days",
    "vault_min_pct",
)
CLOSED_STATUSES = ("rejected", "buyers_extracted")
UINT256_DIGITS = 78


class ChainQuerySet(models.QuerySet):
    def active(self):
        return self.filter(is_enabled=True, hypersync_supported=True, evm_id__isnull=False).exclude(
            zerion_id=""
        )


class Chain(models.Model):
    gt_id = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=128)
    evm_id = models.PositiveBigIntegerField(null=True, blank=True)
    zerion_id = models.CharField(max_length=64, blank=True, default="")
    hypersync_supported = models.BooleanField(default=False)
    rpc_url = models.CharField(max_length=300, blank=True, default="")
    native_fungible_id = models.CharField(max_length=100, blank=True, default="")
    wrapped_fungible_id = models.CharField(max_length=100, blank=True, default="")
    is_enabled = models.BooleanField(default=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = ChainQuerySet.as_manager()

    class Meta:
        ordering = ["gt_id"]

    def __str__(self):
        return self.gt_id

    @property
    def is_active(self) -> bool:
        return (
            self.is_enabled
            and self.hypersync_supported
            and self.evm_id is not None
            and self.zerion_id != ""
        )


def _usd():
    return models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)


class DetectionSettings(models.Model):
    chain = models.ForeignKey(
        Chain, null=True, blank=True, on_delete=models.CASCADE, related_name="detection_settings"
    )
    min_change_24h_pct = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    min_liquidity_usd = _usd()
    min_volume_usd = _usd()
    peak_volume_window_hours = models.PositiveIntegerField(null=True, blank=True)
    min_fdv_usd = _usd()
    max_fdv_usd = _usd()
    max_pool_age_hours = models.PositiveIntegerField(null=True, blank=True)
    min_multiplier = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    min_retention_pct = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    confirmation_hours = models.PositiveIntegerField(null=True, blank=True)
    sniper_blocks = models.PositiveIntegerField(null=True, blank=True)
    min_buy_usd = models.DecimalField(
        max_digits=20,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Position minimum détenue au creux ($) pour retenir un acheteur.",
    )
    max_buyers = models.PositiveIntegerField(
        null=True, blank=True, help_text="0 = pas de plafond. Vide = valeur globale."
    )
    explosion_window_hours = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Le pic doit se trouver dans ces dernières heures (ignoré pour un ajout manuel).",
    )
    maturity_hours = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Âge du token au creux à partir duquel une vague compte pleinement. "
        "0 = pas de pondération.",
    )
    breakout_multiplier = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Une vague précédente retombée coupe le creux si elle a dépassé ce multiple.",
    )
    buyer_window_hours = models.PositiveIntegerField(
        null=True, blank=True, help_text="Heures d'achat avant le creux. 0 = depuis le lancement."
    )
    min_score = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Score minimum (multiplicateur × maturité) : écarte les pumps de lancement.",
    )
    max_multiplier = models.DecimalField(
        max_digits=14,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Au-delà, la vague est une anomalie (pool vidé, prix ~0). 0 = pas de plafond.",
    )
    hub_min_senders = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Un destinataire alimenté par au moins autant d'expéditeurs est un hub "
        "(router, exchange) : lui envoyer = sortie.",
    )
    vault_follow_depth = models.PositiveSmallIntegerField(
        null=True, blank=True, help_text="Niveaux de coffres suivis pendant la montée."
    )
    bot_window_days = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        help_text="Jours récents sur lesquels on mesure l'activité (tx signées/jour).",
    )
    vault_min_pct = models.DecimalField(
        max_digits=6,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Envoi minimum (% de ce que l'expéditeur a reçu) qui crée un coffre.",
    )

    class Meta:
        verbose_name = "réglages de détection"
        verbose_name_plural = "réglages de détection"
        constraints = [
            models.UniqueConstraint(
                fields=["chain"], name="discovery_one_settings_per_chain", nulls_distinct=False
            )
        ]

    def __str__(self):
        return f"Réglages {self.chain or 'globaux'}"

    def clean(self):
        if self.chain_id is None:
            missing = [name for name in THRESHOLD_FIELDS if getattr(self, name) is None]
            if missing:
                raise ValidationError(
                    {name: "Obligatoire pour les réglages globaux." for name in missing}
                )


def default_stablecoins() -> list[str]:
    # Conservée pour la migration 0005 (champ supprimé depuis).
    return ["USDC", "USDT", "DAI"]


def default_prefilter_chains() -> list[str]:
    return ["base", "robinhood", "bsc", "eth", "arc"]


def default_quote_symbols() -> list[str]:
    return ["ETH", "WETH", "BNB", "WBNB", "USDC", "USDT", "DAI", "USDC.E", "USDBC", "FDUSD"]


class PipelineSettings(models.Model):
    trending_pages = models.PositiveSmallIntegerField(default=10)
    volume_pages_per_chain = models.PositiveSmallIntegerField(default=3)
    candidate_cooldown_hours = models.PositiveIntegerField(default=72)
    max_transfers_per_token = models.PositiveIntegerField(default=2_000_000)
    max_attempts = models.PositiveSmallIntegerField(default=3)
    gecko_requests_per_min = models.PositiveIntegerField(default=30)
    hypersync_requests_per_min = models.PositiveIntegerField(default=60)
    hypersync_timeout_seconds = models.PositiveIntegerField(
        default=30, help_text="Délai max d'une requête HyperSync avant nouvelle tentative."
    )
    http_timeout_seconds = models.PositiveIntegerField(default=15)
    http_max_retries = models.PositiveSmallIntegerField(default=3)
    http_backoff_seconds = models.PositiveIntegerField(
        default=5, help_text="Attente avant la 1re nouvelle tentative, doublée à chaque essai."
    )
    zerion_daily_budget = models.PositiveIntegerField(default=1800)
    zerion_requests_per_min = models.PositiveIntegerField(default=300)
    prefilter_chains = models.JSONField(
        default=default_prefilter_chains,
        help_text="gt_id des chaînes du pré-filtre HyperSync (+ chaîne où le wallet est repéré).",
    )
    quote_symbols = models.JSONField(
        default=default_quote_symbols,
        help_text="Monnaies de paiement : leurs jambes de trade ne comptent pas comme achats.",
    )
    history_refresh_days = models.PositiveIntegerField(default=7)
    qualification_batch_size = models.PositiveIntegerField(default=100)
    sell_pass_batch_size = models.PositiveIntegerField(
        default=500, help_text="Adresses d'acheteurs par requête de la passe des ventes."
    )
    zerion_operation_types = models.CharField(
        max_length=300,
        default=OPERATION_TYPES,
        help_text="Types de transactions Zerion récupérés (séparés par des virgules).",
    )
    token_info_refresh_days = models.PositiveIntegerField(default=30)
    rug_priority_weight = models.DecimalField(
        max_digits=4,
        decimal_places=2,
        default=0.2,
        help_text="Poids d'une explosion « rug » dans la priorité de qualification.",
    )

    class Meta:
        verbose_name = "réglages du pipeline"
        verbose_name_plural = "réglages du pipeline"

    def __str__(self):
        return "Réglages du pipeline"

    def save(self, *args, **kwargs):
        self.pk = 1
        super().save(*args, **kwargs)

    @classmethod
    def load(cls) -> "PipelineSettings":
        settings, _ = cls.objects.get_or_create(pk=1)
        return settings


class Token(models.Model):
    chain = models.ForeignKey(Chain, on_delete=models.CASCADE, related_name="tokens")
    address = models.CharField(max_length=66)
    symbol = models.CharField(max_length=64, blank=True)
    decimals = models.PositiveSmallIntegerField(default=18)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["chain", "address"], name="discovery_unique_token")
        ]

    def __str__(self):
        return f"{self.symbol or self.address} ({self.chain})"


class Pool(models.Model):
    token = models.ForeignKey(Token, on_delete=models.CASCADE, related_name="pools")
    address = models.CharField(max_length=66)
    created_block = models.PositiveBigIntegerField(null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["token", "address"], name="discovery_unique_pool")
        ]

    def __str__(self):
        return self.address


class CandidateQuerySet(models.QuerySet):
    def open(self):
        return self.exclude(status__in=CLOSED_STATUSES)


class Candidate(models.Model):
    class Status(models.TextChoices):
        CANDIDATE = "candidate", "Candidat"
        ANALYZED = "analyzed", "Analysé"
        WAITING_CONFIRMATION = "waiting_confirmation", "En attente de confirmation"
        CONFIRMED = "confirmed", "Confirmé"
        BUYERS_EXTRACTED = "buyers_extracted", "Acheteurs extraits"
        REJECTED = "rejected", "Rejeté"

    token = models.ForeignKey(Token, on_delete=models.CASCADE, related_name="candidates")
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.CANDIDATE)
    sources = models.JSONField(default=list)
    metrics = models.JSONField(default=dict)
    rejection_reason = models.CharField(max_length=64, blank=True, default="")
    attempts = models.PositiveSmallIntegerField(default=0)
    next_check_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = CandidateQuerySet.as_manager()

    class Meta:
        indexes = [
            models.Index(fields=["status", "next_check_at"], name="discovery_cand_status_check")
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["token"],
                condition=~Q(status__in=CLOSED_STATUSES),
                name="discovery_one_open_candidate",
            )
        ]

    def __str__(self):
        return f"{self.token} — {self.get_status_display()}"


class Explosion(models.Model):
    class Retention(models.TextChoices):
        PENDING = "pending", "À mesurer"
        HELD = "held", "Tenue"
        RUG = "rug", "Rug"

    class Extraction(models.TextChoices):
        COMPLETE = "complete", "Complète"
        PARTIAL = "partial", "Partielle"

    candidate = models.OneToOneField(Candidate, on_delete=models.CASCADE, related_name="explosion")
    trough_block = models.PositiveBigIntegerField()
    trough_at = models.DateTimeField()
    peak_block = models.PositiveBigIntegerField()
    peak_at = models.DateTimeField()
    peak_price = models.FloatField(null=True, blank=True)
    multiplier = models.DecimalField(max_digits=12, decimal_places=2)
    score = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    retention_pct = models.DecimalField(max_digits=7, decimal_places=2, null=True, blank=True)
    retention_status = models.CharField(
        max_length=16, choices=Retention.choices, default=Retention.PENDING
    )
    extraction_status = models.CharField(
        max_length=16, choices=Extraction.choices, blank=True, default=""
    )

    def __str__(self):
        return f"{self.candidate.token} ×{self.multiplier}"


class Entity(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    merged_into = models.ForeignKey(
        "self", null=True, blank=True, on_delete=models.SET_NULL, related_name="absorbed"
    )

    class Meta:
        verbose_name_plural = "entities"

    def __str__(self):
        return f"Entité {self.pk}"


class Wallet(models.Model):
    address = models.CharField(max_length=42, unique=True)
    entity = models.ForeignKey(
        Entity, null=True, blank=True, on_delete=models.SET_NULL, related_name="wallets"
    )

    def __str__(self):
        return self.address


class EarlyBuyer(models.Model):
    explosion = models.ForeignKey(Explosion, on_delete=models.CASCADE, related_name="buyers")
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="early_buys")
    first_buy_block = models.PositiveBigIntegerField()
    first_buy_at = models.DateTimeField()
    bought_amount = models.DecimalField(max_digits=UINT256_DIGITS, decimal_places=0)
    bought_usd = models.DecimalField(max_digits=20, decimal_places=2)
    held_amount = models.DecimalField(
        max_digits=UINT256_DIGITS,
        decimal_places=0,
        default=0,
        help_text="Tokens détenus au creux (achetés − revendus avant le creux).",
    )
    held_usd = models.DecimalField(
        max_digits=20, decimal_places=2, default=0, help_text="Position au creux, au prix du creux."
    )
    sold_amount = models.DecimalField(
        max_digits=UINT256_DIGITS,
        decimal_places=0,
        default=0,
        help_text="Tokens revendus pendant la montée (creux → pic).",
    )
    is_sniper = models.BooleanField(default=False)
    entity = models.ForeignKey(
        Entity, null=True, blank=True, on_delete=models.SET_NULL, related_name="early_buys"
    )
    inherited_amount = models.DecimalField(max_digits=UINT256_DIGITS, decimal_places=0, default=0)
    inherited_usd = models.DecimalField(
        max_digits=20, decimal_places=2, default=0, help_text="Prix de revient hérité."
    )
    inherited_from = models.ForeignKey(
        Wallet, null=True, blank=True, on_delete=models.SET_NULL, related_name="+"
    )

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["explosion", "wallet"], name="discovery_one_buyer_per_explosion"
            )
        ]

    def __str__(self):
        return f"{self.wallet} → {self.explosion}"


class EntityEarlyBuy(models.Model):
    entity = models.ForeignKey(Entity, on_delete=models.CASCADE, related_name="explosion_buys")
    explosion = models.ForeignKey(Explosion, on_delete=models.CASCADE, related_name="entity_buys")
    held_amount = models.DecimalField(max_digits=UINT256_DIGITS, decimal_places=0)
    held_usd = models.DecimalField(max_digits=20, decimal_places=2)
    first_buy_at = models.DateTimeField()
    sold_during_rise_pct = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    rank = models.PositiveIntegerField(default=0)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["entity", "explosion"], name="discovery_one_entity_buy_per_explosion"
            )
        ]
        ordering = ["explosion", "rank"]

    def __str__(self):
        return f"{self.entity} → {self.explosion} (#{self.rank})"


class ExcludedBuyer(models.Model):
    explosion = models.ForeignKey(Explosion, on_delete=models.CASCADE, related_name="excluded")
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="exclusions")
    reason = models.CharField(max_length=32)
    txs_per_day = models.DecimalField(max_digits=12, decimal_places=1, null=True, blank=True)
    held_usd = models.DecimalField(max_digits=20, decimal_places=2, default=0)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["explosion", "wallet"], name="discovery_one_exclusion_per_explosion"
            )
        ]

    def __str__(self):
        return f"{self.wallet} ({self.reason})"


class TokenTransfer(models.Model):
    class Kind(models.TextChoices):
        BUY = "buy", "Achat"
        SELL = "sell", "Vente"
        EXIT = "exit", "Sortie"
        INTERNAL = "internal", "Interne à l'entité"
        VAULT = "vault", "Vers un nouveau coffre"
        RECEIVE = "receive", "Réception"

    explosion = models.ForeignKey(Explosion, on_delete=models.CASCADE, related_name="transfers")
    token = models.ForeignKey(Token, on_delete=models.CASCADE, related_name="transfers")
    tx_hash = models.CharField(max_length=66)
    log_index = models.PositiveIntegerField()
    block = models.PositiveBigIntegerField()
    at = models.DateTimeField()
    tx_from = models.CharField(max_length=42)
    sender = models.CharField(max_length=42)
    recipient = models.CharField(max_length=42)
    amount = models.DecimalField(max_digits=UINT256_DIGITS, decimal_places=0)
    kind = models.CharField(max_length=16, choices=Kind.choices)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["token", "tx_hash", "log_index"], name="discovery_unique_token_transfer"
            )
        ]
        indexes = [models.Index(fields=["explosion", "sender"], name="discovery_transfer_sender")]

    def __str__(self):
        return f"{self.get_kind_display()} {self.tx_hash[:10]}"
