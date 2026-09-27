"""Qualification : profils, historique par token, entités, liens, adresses, prix, réglages."""

from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex
from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import Q

from apps.discovery.models import UINT256_DIGITS, Chain, Token, Wallet

QUALIFICATION_FIELDS = (
    "max_txs_per_day",
    "max_distinct_tokens",
    "inactive_days",
    "min_txs_active",
    "history_days",
    "max_mev_ratio",
    "min_distinct_buys",
    "min_portfolio_usd",
    "max_portfolio_usd",
    "transfer_after_buy_pct",
    "follow_depth",
    "funder_max_wallets",
    "hot_wallet_min_counterparties",
    "deposit_forward_pct",
    "deposit_forward_hours",
    "flipper_hours",
    "flipper_min_sold_pct",
    "flipper_min_share_pct",
    "holder_min_pct",
    "accumulator_max_out_pct",
    "accumulator_min_positions",
    "early_buyer_min_explosions",
    "refilter_after_days",
)
BLOCKING_KINDS = ("exchange", "cex_deposit", "service", "bridge", "router", "mev")


def _amount(**kwargs):
    return models.DecimalField(max_digits=UINT256_DIGITS, decimal_places=0, **kwargs)


class Entity(models.Model):
    portfolio_value_usd = models.DecimalField(
        max_digits=20, decimal_places=2, null=True, blank=True
    )
    tags = ArrayField(models.CharField(max_length=32), default=list, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name_plural = "entities"
        indexes = [GinIndex(fields=["tags"], name="wallets_entity_tags")]

    def __str__(self):
        return f"Entité #{self.pk}"


class WalletProfile(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "En attente"
        PREFILTERED = "prefiltered", "Pré-filtré"
        HISTORY_FETCHED = "history_fetched", "Historique récupéré"
        QUALIFIED = "qualified", "Qualifié"
        FILTERED = "filtered", "Écarté"

    class Source(models.TextChoices):
        EARLY_BUYER = "early_buyer", "Early buyer"
        LINKED = "linked", "Wallet lié"

    wallet = models.OneToOneField(Wallet, on_delete=models.CASCADE, related_name="profile")
    source = models.CharField(max_length=16, choices=Source.choices, default=Source.EARLY_BUYER)
    depth = models.PositiveSmallIntegerField(default=0)
    chains = ArrayField(models.PositiveBigIntegerField(), default=list, blank=True)
    status = models.CharField(max_length=24, choices=Status.choices, default=Status.PENDING)
    filter_reason = models.CharField(max_length=64, blank=True, default="")
    entity = models.ForeignKey(
        Entity, null=True, blank=True, on_delete=models.SET_NULL, related_name="profiles"
    )
    portfolio_value_usd = models.DecimalField(
        max_digits=20, decimal_places=2, null=True, blank=True
    )
    metrics = models.JSONField(default=dict, blank=True)
    tags = ArrayField(models.CharField(max_length=32), default=list, blank=True)
    attempts = models.PositiveSmallIntegerField(default=0)
    analyzed_at = models.DateTimeField(null=True, blank=True)
    next_analysis_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["status", "next_analysis_at"], name="wallets_profile_status"),
            GinIndex(fields=["tags"], name="wallets_profile_tags"),
        ]

    def __str__(self):
        return str(self.wallet)


class TokenPosition(models.Model):
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="positions")
    token = models.ForeignKey(Token, on_delete=models.CASCADE, related_name="positions")
    bought_amount = _amount(default=0)
    sold_amount = _amount(default=0)
    sent_amount = _amount(default=0)
    received_amount = _amount(default=0)
    bought_usd = models.DecimalField(max_digits=20, decimal_places=2, default=0)
    sold_usd = models.DecimalField(max_digits=20, decimal_places=2, default=0)
    buys = models.PositiveIntegerField(default=0)
    sells = models.PositiveIntegerField(default=0)
    first_at = models.DateTimeField()
    last_at = models.DateTimeField()

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["wallet", "token"], name="wallets_unique_position")
        ]

    def __str__(self):
        return f"{self.wallet} · {self.token}"

    @property
    def balance(self):
        return self.bought_amount + self.received_amount - self.sold_amount - self.sent_amount


class TokenTrade(models.Model):
    class Kind(models.TextChoices):
        BUY = "buy", "Achat"
        SELL = "sell", "Vente"
        SEND = "send", "Envoi"
        RECEIVE = "receive", "Réception"

    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="trades")
    token = models.ForeignKey(Token, on_delete=models.CASCADE, related_name="trades")
    kind = models.CharField(max_length=8, choices=Kind.choices)
    amount = _amount()
    usd = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    counterparty = models.CharField(max_length=42, blank=True, default="")
    block = models.PositiveBigIntegerField()
    at = models.DateTimeField()
    tx_hash = models.CharField(max_length=66)
    log_index = models.PositiveIntegerField()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["tx_hash", "log_index", "wallet"], name="wallets_unique_trade"
            )
        ]
        indexes = [
            models.Index(fields=["wallet", "token", "at"], name="wallets_trade_wallet_token")
        ]

    def __str__(self):
        return f"{self.get_kind_display()} {self.token} ({self.tx_hash[:10]})"


class WalletLink(models.Model):
    class Kind(models.TextChoices):
        TRANSFER_AFTER_BUY = "transfer_after_buy", "Transfert après achat"
        FUNDING = "funding", "Financement initial"

    from_wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="links_out")
    to_wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="links_in")
    kind = models.CharField(max_length=24, choices=Kind.choices)
    evidence = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["from_wallet", "to_wallet", "kind"], name="wallets_unique_link"
            )
        ]

    def __str__(self):
        return f"{self.from_wallet} → {self.to_wallet} ({self.kind})"


class KnownAddressQuerySet(models.QuerySet):
    def blocking_for(self, address: str, chain_ids: list[int]):
        return self.filter(
            Q(chain__isnull=True) | Q(chain_id__in=chain_ids),
            address=address.lower(),
            kind__in=BLOCKING_KINDS,
        )


class KnownAddress(models.Model):
    class Kind(models.TextChoices):
        EXCHANGE = "exchange", "Exchange"
        CEX_DEPOSIT = "cex_deposit", "Dépôt d'exchange"
        SERVICE = "service", "Service"
        BRIDGE = "bridge", "Bridge"
        ROUTER = "router", "Router"
        MEV = "mev", "Bot MEV"
        STABLECOIN = "stablecoin", "Stablecoin"
        WRAPPED_NATIVE = "wrapped_native", "Natif wrappé"

    class Source(models.TextChoices):
        IMPORT = "import", "Import"
        MANUAL = "manual", "Manuel"
        AUTO = "auto", "Automatique"

    chain = models.ForeignKey(
        Chain, null=True, blank=True, on_delete=models.CASCADE, related_name="known_addresses"
    )
    address = models.CharField(max_length=42)
    kind = models.CharField(max_length=16, choices=Kind.choices)
    label = models.CharField(max_length=128, blank=True, default="")
    source = models.CharField(max_length=8, choices=Source.choices, default=Source.MANUAL)
    created_at = models.DateTimeField(auto_now_add=True)

    objects = KnownAddressQuerySet.as_manager()

    class Meta:
        verbose_name_plural = "known addresses"
        constraints = [
            models.UniqueConstraint(
                fields=["chain", "address"], name="wallets_unique_known", nulls_distinct=False
            )
        ]

    def __str__(self):
        return f"{self.label or self.address} ({self.kind})"


class DailyPrice(models.Model):
    fungible_id = models.CharField(max_length=100)
    day = models.DateField()
    usd = models.FloatField()

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["fungible_id", "day"], name="wallets_unique_price")
        ]

    def __str__(self):
        return f"{self.fungible_id} {self.day} {self.usd}"


def _int_setting(**kwargs):
    return models.PositiveIntegerField(null=True, blank=True, **kwargs)


def _decimal_setting():
    return models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)


class QualificationSettings(models.Model):
    chain = models.ForeignKey(
        Chain,
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="qualification_settings",
    )
    max_txs_per_day = _int_setting()
    max_distinct_tokens = _int_setting()
    inactive_days = _int_setting()
    min_txs_active = _int_setting()
    history_days = _int_setting()
    max_mev_ratio = _decimal_setting()
    min_distinct_buys = _int_setting()
    min_portfolio_usd = _decimal_setting()
    max_portfolio_usd = _decimal_setting()
    transfer_after_buy_pct = _decimal_setting()
    follow_depth = _int_setting()
    funder_max_wallets = _int_setting()
    hot_wallet_min_counterparties = _int_setting()
    deposit_forward_pct = _decimal_setting()
    deposit_forward_hours = _int_setting()
    flipper_hours = _int_setting()
    flipper_min_sold_pct = _decimal_setting()
    flipper_min_share_pct = _decimal_setting()
    holder_min_pct = _decimal_setting()
    accumulator_max_out_pct = _decimal_setting()
    accumulator_min_positions = _int_setting()
    early_buyer_min_explosions = _int_setting()
    refilter_after_days = _int_setting()

    class Meta:
        verbose_name = "réglages de qualification"
        verbose_name_plural = "réglages de qualification"
        constraints = [
            models.UniqueConstraint(
                fields=["chain"], name="wallets_one_settings_per_chain", nulls_distinct=False
            )
        ]

    def __str__(self):
        return f"Qualification {self.chain or 'globale'}"

    def clean(self):
        if self.chain_id is None:
            missing = [name for name in QUALIFICATION_FIELDS if getattr(self, name) is None]
            if missing:
                raise ValidationError(
                    {name: "Obligatoire pour les réglages globaux." for name in missing}
                )
