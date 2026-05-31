"""Analytics models."""
from django.db import models
from .base import Wallet


class TokenAnalytics(models.Model):
    """Résultats FIFO par wallet × token."""
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name='token_analytics')
    token_symbol = models.CharField(max_length=50)
    total_invested = models.FloatField()
    total_realized = models.FloatField()
    roi_percentage = models.FloatField()
    is_winning = models.BooleanField()
    status = models.CharField(max_length=20)  # GAGNANT, PERDANT, NEUTRE
    holding_days = models.IntegerField(default=0)
    in_portfolio = models.BooleanField(default=True)

    class Meta:
        db_table = 'token_analytics'
        unique_together = [['wallet', 'token_symbol']]
        indexes = [
            models.Index(fields=['wallet', 'roi_percentage']),
        ]

    def __str__(self):
        return f"{self.token_symbol} - {self.wallet.address[:10]}... ({self.roi_percentage:.1f}%)"


class WalletTierPerformance(models.Model):
    """Performance par palier d'investissement."""
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name='tier_performance')
    tier_usd = models.IntegerField()  # 3000, 6000, 9000, 12000
    roi_percentage = models.FloatField()
    winrate = models.FloatField()
    nb_trades = models.IntegerField()
    nb_gagnants = models.IntegerField()
    is_optimal_tier = models.BooleanField(default=False)

    class Meta:
        db_table = 'wallet_tier_performance'
        unique_together = [['wallet', 'tier_usd']]

    def __str__(self):
        return f"{self.wallet.address[:10]}... - ${self.tier_usd}"


class WalletQualified(models.Model):
    """Wallets qualifiés (ayant passé le filtre)."""
    wallet = models.OneToOneField(Wallet, on_delete=models.CASCADE, primary_key=True, related_name='qualification')
    final_score = models.FloatField()
    classification = models.CharField(max_length=20)  # ELITE, EXCELLENT, BON, MOYEN, FAIBLE
    weighted_roi = models.FloatField()
    taux_reussite = models.FloatField()
    nb_trades = models.IntegerField()

    class Meta:
        db_table = 'wallet_qualified'

    def __str__(self):
        return f"{self.wallet.address[:10]}... - {self.classification}"


class SmartWallet(models.Model):
    """Smart wallets sélectionnés."""
    wallet = models.OneToOneField(Wallet, on_delete=models.CASCADE, primary_key=True, related_name='smart_wallet_data')
    optimal_threshold_tier = models.IntegerField()
    quality_score = models.FloatField()
    threshold_status = models.CharField(max_length=20)  # EXCELLENT, GOOD, AVERAGE, POOR
    optimal_roi = models.FloatField()
    optimal_winrate = models.FloatField()
    global_roi = models.FloatField()
    global_winrate = models.FloatField()

    class Meta:
        db_table = 'smart_wallets'

    def __str__(self):
        return f"{self.wallet.address[:10]}... - {self.threshold_status}"
