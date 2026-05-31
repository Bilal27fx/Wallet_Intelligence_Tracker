"""Explosive tokens models."""
from django.db import models


class ExplosiveToken(models.Model):
    """Tokens detected via GeckoTerminal trending/new pools."""

    symbol = models.CharField(max_length=50)
    token_address = models.CharField(max_length=100)
    chain = models.CharField(max_length=50)
    pool_address = models.CharField(max_length=100, blank=True, null=True)
    pool_age_hours = models.FloatField(null=True, blank=True)
    price_change_24h = models.FloatField(null=True, blank=True)
    volume_24h = models.FloatField(null=True, blank=True)
    liquidity_usd = models.FloatField(null=True, blank=True)
    fdv = models.FloatField(null=True, blank=True)
    buys_ratio = models.FloatField(null=True, blank=True)
    detected_at = models.DateTimeField(auto_now_add=True)
    explosion_start_date = models.DateTimeField(null=True, blank=True)
    explosion_peak_date = models.DateTimeField(null=True, blank=True)
    explosion_pct = models.FloatField(null=True, blank=True)
    hours_gap = models.FloatField(null=True, blank=True)
    score = models.FloatField(null=True, blank=True)
    hours_since_now = models.FloatField(null=True, blank=True)
    traite = models.BooleanField(default=False)

    class Meta:
        db_table = 'explosive_tokens_detected'
        unique_together = [['token_address', 'chain']]
        indexes = [
            models.Index(fields=['chain']),
            models.Index(fields=['detected_at']),
            models.Index(fields=['traite']),
        ]
        app_label = 'wallets'

    def __str__(self):
        return f"{self.symbol} ({self.chain}) +{self.price_change_24h}%"


class TokenPriceHistory(models.Model):
    """OHLCV price history for explosive tokens."""

    token_address = models.CharField(max_length=100)
    chain = models.CharField(max_length=50)
    pool_address = models.CharField(max_length=100)
    date = models.DateTimeField()
    close = models.FloatField()
    volume = models.FloatField()

    class Meta:
        db_table = 'token_explosif_history_prices'
        unique_together = [['token_address', 'chain', 'date']]
        indexes = [
            models.Index(fields=['token_address', 'chain']),
            models.Index(fields=['date']),
        ]
        app_label = 'wallets'

    def __str__(self):
        return f"{self.token_address} @ {self.date}"
