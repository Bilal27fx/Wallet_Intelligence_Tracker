"""Wallet scoring and qualification service."""
from typing import Dict, Any, List
from wallets.models import (
    Wallet, TokenAnalytics, WalletQualified,
    WalletTierPerformance, SmartWallet
)
from django.db.models import Avg, Count, Q
from django.conf import settings


class WalletScorerService:
    """Service to score and qualify wallets."""

    def __init__(self):
        self.min_trades = getattr(settings, 'WALLET_MIN_TRADES', 5)
        self.min_roi = getattr(settings, 'WALLET_MIN_ROI', 20)
        self.min_winrate = getattr(settings, 'WALLET_MIN_WINRATE', 40)

    def score_wallet(self, wallet_address: str) -> Dict[str, Any]:
        """Calculate comprehensive score for a wallet."""
        wallet = Wallet.objects.get(address=wallet_address)

        analytics = TokenAnalytics.objects.filter(wallet=wallet)

        if not analytics.exists():
            return {
                'wallet_address': wallet_address,
                'error': 'No analytics data found'
            }

        nb_trades = analytics.count()
        nb_winning = analytics.filter(is_winning=True).count()
        winrate = (nb_winning / nb_trades * 100) if nb_trades > 0 else 0

        avg_roi = analytics.aggregate(Avg('roi_percentage'))['roi_percentage__avg'] or 0

        weighted_roi = self._calculate_weighted_roi(analytics)

        final_score = self._calculate_final_score(
            roi=weighted_roi,
            winrate=winrate,
            nb_trades=nb_trades
        )

        classification = self._classify_wallet(final_score)

        score_data = {
            'wallet': wallet,
            'final_score': final_score,
            'classification': classification,
            'weighted_roi': weighted_roi,
            'taux_reussite': winrate,
            'nb_trades': nb_trades
        }

        WalletQualified.objects.update_or_create(
            wallet=wallet,
            defaults=score_data
        )

        return {
            'wallet_address': wallet_address,
            'final_score': final_score,
            'classification': classification,
            'weighted_roi': weighted_roi,
            'winrate': winrate,
            'nb_trades': nb_trades
        }

    def _calculate_weighted_roi(self, analytics) -> float:
        """Calculate weighted ROI based on investment amounts."""
        total_invested = sum(a.total_invested for a in analytics)

        if total_invested == 0:
            return 0.0

        weighted_sum = sum(
            a.roi_percentage * (a.total_invested / total_invested)
            for a in analytics
        )

        return weighted_sum

    def _calculate_final_score(
        self,
        roi: float,
        winrate: float,
        nb_trades: int
    ) -> float:
        """Calculate final wallet score."""
        roi_score = min(roi / 2, 50)

        winrate_score = min(winrate / 2, 50)

        trade_volume_bonus = min(nb_trades / 2, 10)

        return roi_score + winrate_score + trade_volume_bonus

    def _classify_wallet(self, score: float) -> str:
        """Classify wallet based on score."""
        if score >= 80:
            return 'ELITE'
        elif score >= 60:
            return 'EXCELLENT'
        elif score >= 40:
            return 'BON'
        elif score >= 20:
            return 'MOYEN'
        else:
            return 'FAIBLE'

    def qualify_wallet(self, wallet_address: str) -> bool:
        """Check if wallet meets qualification criteria."""
        score_data = self.score_wallet(wallet_address)

        if 'error' in score_data:
            return False

        is_qualified = (
            score_data['nb_trades'] >= self.min_trades and
            score_data['weighted_roi'] >= self.min_roi and
            score_data['winrate'] >= self.min_winrate
        )

        return is_qualified

    def score_all_wallets(self) -> Dict[str, Any]:
        """Score all wallets in database."""
        wallets = Wallet.objects.all()

        results = {
            'total_wallets': 0,
            'qualified': 0,
            'elite': 0,
            'excellent': 0,
            'bon': 0,
            'moyen': 0,
            'faible': 0
        }

        for wallet in wallets:
            try:
                score_data = self.score_wallet(wallet.address)

                if 'error' not in score_data:
                    results['total_wallets'] += 1

                    if self.qualify_wallet(wallet.address):
                        results['qualified'] += 1

                    classification = score_data['classification']
                    results[classification.lower()] += 1

            except Exception:
                continue

        return results

    def get_top_wallets(self, limit: int = 10) -> List[WalletQualified]:
        """Get top wallets by score."""
        return list(
            WalletQualified.objects.all().order_by('-final_score')[:limit]
        )

    def get_qualified_wallets(self) -> List[WalletQualified]:
        """Get all qualified wallets."""
        return list(
            WalletQualified.objects.filter(
                nb_trades__gte=self.min_trades,
                weighted_roi__gte=self.min_roi,
                taux_reussite__gte=self.min_winrate
            ).order_by('-final_score')
        )
