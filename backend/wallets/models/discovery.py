"""Discovery models."""
from django.db import models


class WalletBrute(models.Model):
    """Wallets bruts issus du token discovery."""
    wallet_address = models.CharField(max_length=42)
    token_address = models.CharField(max_length=42)
    contract_address = models.CharField(max_length=42)
    chain = models.CharField(max_length=50)
    temporality = models.CharField(max_length=20)  # 14d, 30d, 200d, 360d

    class Meta:
        db_table = 'wallet_brute'
        unique_together = [['wallet_address', 'token_address', 'temporality']]

    def __str__(self):
        return f"{self.wallet_address[:10]}... - {self.temporality}"
