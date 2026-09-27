"""Qualification v2 : profils, transactions et mouvements Zerion, positions, liens, réglages."""

from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex
from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import Q

from apps.discovery.models import UINT256_DIGITS, Chain, Wallet

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
    "big_receive_pct",
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


def _usd(**kwargs):
    return models.DecimalField(max_digits=20, decimal_places=2, **kwargs)


class WalletProfile(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "En attente"
        PREFILTERED = "prefiltered", "Pré-filtré (historique en cours)"
        HISTORY_FETCHED = "history_fetched", "Historique complet"
        QUALIFIED = "qualified", "Qualifié"
        VALUED = "valued", "Wallet lié valorisé"
        FILTERED = "filtered", "Écarté"

    class Source(models.TextChoices):
        EARLY_BUYER = "early_buyer", "Early buyer"
        LINKED = "linked", "Wallet lié"

    wallet = models.OneToOneField(Wallet, on_delete=models.CASCADE, related_name="profile")
    source = models.CharField(max_length=16, choices=Source.choices, default=Source.EARLY_BUYER)
    depth = models.PositiveSmallIntegerField(default=0)
    status = models.CharField(max_length=24, choices=Status.choices, default=Status.PENDING)
    filter_reason = models.CharField(max_length=64, blank=True, default="")
    priority = models.FloatField(default=0)
    active_chains = ArrayField(models.CharField(max_length=64), default=list, blank=True)
    portfolio_value_usd = _usd(null=True, blank=True)
    linked_value_usd = _usd(null=True, blank=True)
    history_cursor = models.CharField(max_length=1024, blank=True, default="")
    history_complete = models.BooleanField(default=False)
    last_mined_at = models.DateTimeField(null=True, blank=True)
    metrics = models.JSONField(default=dict, blank=True)
    tags = ArrayField(models.CharField(max_length=32), default=list, blank=True)
    attempts = models.PositiveSmallIntegerField(default=0)
    analyzed_at = models.DateTimeField(null=True, blank=True)
    next_analysis_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["status", "priority"], name="wallets_profile_status_prio"),
            GinIndex(fields=["tags"], name="wallets_profile_tags"),
        ]

    def __str__(self):
        return str(self.wallet)


class WalletTransaction(models.Model):
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="transactions")
    zerion_id = models.CharField(max_length=64)
    chain = models.CharField(max_length=64)
    tx_hash = models.CharField(max_length=66)
    block = models.PositiveBigIntegerField(null=True, blank=True)
    mined_at = models.DateTimeField()
    operation_type = models.CharField(max_length=16)
    status = models.CharField(max_length=16)
    fee_usd = models.DecimalField(max_digits=20, decimal_places=6, null=True, blank=True)
    raw = models.JSONField(default=dict)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["wallet", "zerion_id"], name="wallets_unique_transaction"
            )
        ]
        indexes = [models.Index(fields=["wallet", "mined_at"], name="wallets_tx_wallet_date")]

    def __str__(self):
        return f"{self.operation_type} {self.tx_hash[:10]} ({self.chain})"


class TokenTrade(models.Model):
    class Kind(models.TextChoices):
        BUY = "buy", "Achat"
        SELL = "sell", "Vente"
        SEND = "send", "Envoi"
        RECEIVE = "receive", "Réception"

    transaction = models.ForeignKey(
        WalletTransaction, on_delete=models.CASCADE, related_name="trades"
    )
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="trades")
    transfer_index = models.PositiveSmallIntegerField()
    chain = models.CharField(max_length=64)
    token_address = models.CharField(max_length=66)
    token_symbol = models.CharField(max_length=64, blank=True, default="")
    token_decimals = models.PositiveSmallIntegerField(default=0)
    fungible_id = models.CharField(max_length=100, blank=True, default="")
    kind = models.CharField(max_length=8, choices=Kind.choices)
    direction = models.CharField(max_length=4)
    quantity = models.DecimalField(max_digits=60, decimal_places=18)
    amount = _amount()
    price_usd = models.DecimalField(max_digits=40, decimal_places=18, null=True, blank=True)
    value_usd = _usd(null=True, blank=True)
    counterparty = models.CharField(max_length=42, blank=True, default="")
    block = models.PositiveBigIntegerField(null=True, blank=True)
    mined_at = models.DateTimeField()
    is_internal = models.BooleanField(
        default=False, help_text="Mouvement entre deux wallets de la même entité."
    )

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["transaction", "transfer_index"], name="wallets_unique_transfer"
            )
        ]
        indexes = [
            models.Index(
                fields=["wallet", "chain", "token_address", "mined_at"], name="wallets_trade_token"
            )
        ]

    def __str__(self):
        return f"{self.get_kind_display()} {self.token_symbol or self.token_address}"


class TokenPosition(models.Model):
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="positions")
    chain = models.CharField(max_length=64)
    token_address = models.CharField(max_length=66)
    token_symbol = models.CharField(max_length=64, blank=True, default="")
    bought_amount = _amount(default=0)
    sold_amount = _amount(default=0)
    sent_amount = _amount(default=0)
    received_amount = _amount(default=0)
    bought_usd = _usd(default=0)
    sold_usd = _usd(default=0)
    buys = models.PositiveIntegerField(default=0)
    sells = models.PositiveIntegerField(default=0)
    first_at = models.DateTimeField()
    last_at = models.DateTimeField()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["wallet", "chain", "token_address"], name="wallets_unique_position"
            )
        ]

    def __str__(self):
        return f"{self.wallet} · {self.token_symbol or self.token_address}"

    @property
    def balance(self):
        return self.bought_amount + self.received_amount - self.sold_amount - self.sent_amount


class WalletLink(models.Model):
    class Kind(models.TextChoices):
        TRANSFER_AFTER_BUY = "transfer_after_buy", "Transfert après achat"
        BIG_RECEIVE = "big_receive", "Gros transfert reçu"
        TRANSFER_TO_VAULT = "transfer_to_vault", "Transfert vers un coffre (on-chain)"
        FUNDING = "funding", "Financement initial (information)"

    class LinkSource(models.TextChoices):
        HYPERSYNC = "hypersync", "HyperSync"
        ZERION = "zerion", "Zerion"

    from_wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="links_out")
    to_wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="links_in")
    kind = models.CharField(max_length=24, choices=Kind.choices)
    evidence = models.JSONField(default=dict, blank=True)
    source = models.CharField(max_length=16, choices=LinkSource.choices, default=LinkSource.ZERION)
    rejected = models.BooleanField(
        default=False, help_text="Rattachement refusé dans l'admin : jamais recréé."
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["from_wallet", "to_wallet", "kind"], name="wallets_unique_link"
            )
        ]

    def __str__(self):
        return f"{self.from_wallet} → {self.to_wallet} ({self.kind})"


STRONG_LINKS = (
    WalletLink.Kind.TRANSFER_AFTER_BUY,
    WalletLink.Kind.BIG_RECEIVE,
    WalletLink.Kind.TRANSFER_TO_VAULT,
)


class TokenInfo(models.Model):
    chain = models.CharField(max_length=64)
    address = models.CharField(max_length=66)
    fungible_id = models.CharField(max_length=100, blank=True, default="")
    symbol = models.CharField(max_length=64, blank=True, default="")
    name = models.CharField(max_length=200, blank=True, default="")
    decimals = models.PositiveSmallIntegerField(null=True, blank=True)
    total_supply = models.DecimalField(max_digits=60, decimal_places=18, null=True, blank=True)
    circulating_supply = models.DecimalField(
        max_digits=60, decimal_places=18, null=True, blank=True
    )
    verified = models.BooleanField(default=False)
    raw = models.JSONField(default=dict, blank=True)
    fetched_at = models.DateTimeField()

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["chain", "address"], name="wallets_unique_token_info")
        ]

    def __str__(self):
        return f"{self.symbol or self.address} ({self.chain})"


class PortfolioSnapshot(models.Model):
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="portfolios")
    fetched_at = models.DateTimeField()
    total_usd = _usd(null=True, blank=True)
    raw = models.JSONField(default=dict, blank=True)

    class Meta:
        indexes = [models.Index(fields=["wallet", "fetched_at"], name="wallets_portfolio_date")]

    def __str__(self):
        return f"{self.wallet} @ {self.fetched_at:%Y-%m-%d %H:%M}"


class PortfolioPosition(models.Model):
    snapshot = models.ForeignKey(
        PortfolioSnapshot, on_delete=models.CASCADE, related_name="positions"
    )
    chain = models.CharField(max_length=64)
    token_address = models.CharField(max_length=66)
    fungible_id = models.CharField(max_length=100, blank=True, default="")
    symbol = models.CharField(max_length=64, blank=True, default="")
    position_type = models.CharField(max_length=32, blank=True, default="")
    quantity = models.DecimalField(max_digits=60, decimal_places=18)
    price_usd = models.DecimalField(max_digits=40, decimal_places=18, null=True, blank=True)
    value_usd = _usd(null=True, blank=True)

    def __str__(self):
        return f"{self.symbol or self.token_address} ({self.chain})"


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


def _int_setting():
    return models.PositiveIntegerField(null=True, blank=True)


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
    big_receive_pct = _decimal_setting()
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
