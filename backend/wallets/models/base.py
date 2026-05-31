"""Base wallet models."""
from django.db import models


class Wallet(models.Model):
    """Wallet principal."""
    address = models.CharField(max_length=42, primary_key=True)
    period = models.CharField(max_length=20)  # 14d, 30d, 200d, 360d, manual
    total_portfolio_value = models.FloatField(null=True, blank=True)
    is_smart_wallet = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'wallets'
        ordering = ['-total_portfolio_value']

    def __str__(self):
        return f"{self.address[:10]}... ({self.period})"


class Token(models.Model):
    """Position token d'un wallet."""
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name='tokens')
    fungible_id = models.CharField(max_length=255)
    symbol = models.CharField(max_length=50)
    contract_address = models.CharField(max_length=100)
    chain = models.CharField(max_length=50)
    amount = models.FloatField()
    usd_value = models.FloatField()
    in_portfolio = models.BooleanField(default=True)

    class Meta:
        db_table = 'tokens'
        unique_together = [['wallet', 'fungible_id']]
        ordering = ['-usd_value']

    def __str__(self):
        return f"{self.symbol} ({self.wallet.address[:10]}...)"


class Transaction(models.Model):
    """Historique des transactions."""
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name='transactions')
    fungible_id = models.CharField(max_length=255)
    symbol = models.CharField(max_length=50)
    hash = models.CharField(max_length=66)
    date = models.DateTimeField()
    operation_type = models.CharField(max_length=50)
    action_type = models.CharField(max_length=20)  # buy, sell
    swap_description = models.TextField(blank=True)
    contract_address = models.CharField(max_length=100)
    quantity = models.FloatField()
    price_per_token = models.FloatField()
    total_value_usd = models.FloatField()
    direction = models.CharField(max_length=10)  # in, out
    recipient_address = models.CharField(max_length=42, blank=True)
    sender_address = models.CharField(max_length=42, blank=True)

    class Meta:
        db_table = 'transaction_history'
        unique_together = [['wallet', 'hash']]
        ordering = ['-date']
        indexes = [
            models.Index(fields=['wallet', 'date']),
            models.Index(fields=['fungible_id']),
        ]

    def __str__(self):
        return f"{self.action_type} {self.symbol} - {self.hash[:10]}..."


class WalletPositionChange(models.Model):
    """Changements de positions détectés (live tracking)."""
    session_id = models.CharField(max_length=100)
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name='position_changes')
    symbol = models.CharField(max_length=50)
    fungible_id = models.CharField(max_length=255)
    contract_address = models.CharField(max_length=100)
    change_type = models.CharField(max_length=20)  # NEW, ACCUMULATION, REDUCTION, EXIT
    old_amount = models.FloatField()
    new_amount = models.FloatField()
    usd_change = models.FloatField()
    detected_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'wallet_position_changes'
        ordering = ['-detected_at']

    def __str__(self):
        return f"{self.change_type} {self.symbol} - {self.wallet.address[:10]}..."
