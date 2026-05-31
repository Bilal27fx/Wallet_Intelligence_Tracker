"""Consensus models."""
from django.db import models
from .base import Wallet


class ConsensusSignal(models.Model):
    """Signal de consensus détecté."""
    token_symbol = models.CharField(max_length=50)
    contract_address = models.CharField(max_length=42)
    chain = models.CharField(max_length=50)
    nb_wallets = models.IntegerField()
    total_usd_invested = models.FloatField()
    market_cap = models.FloatField(null=True, blank=True)
    detected_at = models.DateTimeField(auto_now_add=True)
    wallets = models.ManyToManyField(Wallet, related_name='consensus_signals')

    class Meta:
        db_table = 'consensus_signals'
        ordering = ['-detected_at']

    def __str__(self):
        return f"{self.token_symbol} - {self.nb_wallets} wallets"
