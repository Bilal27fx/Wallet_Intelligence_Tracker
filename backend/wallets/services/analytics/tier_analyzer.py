"""Wallet tier performance analysis service."""
from typing import Dict, Any, List
from wallets.models import (
    Wallet, TokenAnalytics, WalletTierPerformance,
    SmartWallet
)
from django.db.models import Q
from django.conf import settings


class TierAnalyzerService:
    """Service to analyze wallet performance by investment tiers."""

    def __init__(self):
        self.tiers = getattr(
            settings,
            'INVESTMENT_TIERS',
            [3000, 6000, 9000, 12000]
        )
        self.min_quality_score = getattr(settings, 'MIN_QUALITY_SCORE', 0.6)

    def analyze_wallet_tiers(self, wallet_address: str) -> List[Dict[str, Any]]:
        """Analyze performance across all tiers for a wallet."""
        wallet = Wallet.objects.get(address=wallet_address)

        analytics = TokenAnalytics.objects.filter(wallet=wallet)

        if not analytics.exists():
            return []

        tier_results = []

        for tier_usd in self.tiers:
            tier_analytics = analytics.filter(total_invested__gte=tier_usd)

            if not tier_analytics.exists():
                continue

            nb_trades = tier_analytics.count()
            nb_winning = tier_analytics.filter(is_winning=True).count()
            winrate = (nb_winning / nb_trades * 100) if nb_trades > 0 else 0

            total_invested = sum(a.total_invested for a in tier_analytics)
            total_realized = sum(a.total_realized for a in tier_analytics)

            roi_percentage = (
                (total_realized - total_invested) / total_invested * 100
            ) if total_invested > 0 else 0

            tier_data = {
                'wallet': wallet,
                'tier_usd': tier_usd,
                'roi_percentage': roi_percentage,
                'winrate': winrate,
                'nb_trades': nb_trades,
                'nb_gagnants': nb_winning,
                'is_optimal_tier': False
            }

            tier_results.append(tier_data)

        if tier_results:
            optimal_tier = max(
                tier_results,
                key=lambda x: (x['winrate'], x['roi_percentage'])
            )
            optimal_tier['is_optimal_tier'] = True

        for tier_data in tier_results:
            WalletTierPerformance.objects.update_or_create(
                wallet=wallet,
                tier_usd=tier_data['tier_usd'],
                defaults=tier_data
            )

        return tier_results

    def identify_smart_wallet(self, wallet_address: str) -> Dict[str, Any]:
        """Identify if wallet qualifies as smart wallet."""
        wallet = Wallet.objects.get(address=wallet_address)

        tier_performances = WalletTierPerformance.objects.filter(wallet=wallet)

        if not tier_performances.exists():
            return {
                'wallet_address': wallet_address,
                'is_smart': False,
                'reason': 'No tier performance data'
            }

        optimal_tier = tier_performances.filter(is_optimal_tier=True).first()

        if not optimal_tier or optimal_tier.tier_usd == 0:
            return {
                'wallet_address': wallet_address,
                'is_smart': False,
                'reason': 'No optimal tier found'
            }

        all_analytics = TokenAnalytics.objects.filter(wallet=wallet)
        global_nb_trades = all_analytics.count()
        global_nb_winning = all_analytics.filter(is_winning=True).count()
        global_winrate = (
            (global_nb_winning / global_nb_trades * 100)
            if global_nb_trades > 0 else 0
        )

        total_invested = sum(a.total_invested for a in all_analytics)
        total_realized = sum(a.total_realized for a in all_analytics)
        global_roi = (
            (total_realized - total_invested) / total_invested * 100
        ) if total_invested > 0 else 0

        quality_score = self._calculate_quality_score(
            optimal_roi=optimal_tier.roi_percentage,
            optimal_winrate=optimal_tier.winrate,
            global_roi=global_roi,
            global_winrate=global_winrate,
            nb_trades=optimal_tier.nb_trades
        )

        threshold_status = self._classify_threshold(quality_score)

        is_smart = quality_score >= self.min_quality_score

        smart_wallet_data = {
            'wallet': wallet,
            'optimal_threshold_tier': optimal_tier.tier_usd if is_smart else 0,
            'quality_score': quality_score,
            'threshold_status': threshold_status,
            'optimal_roi': optimal_tier.roi_percentage,
            'optimal_winrate': optimal_tier.winrate,
            'global_roi': global_roi,
            'global_winrate': global_winrate
        }

        if is_smart:
            SmartWallet.objects.update_or_create(
                wallet=wallet,
                defaults=smart_wallet_data
            )

        return {
            'wallet_address': wallet_address,
            'is_smart': is_smart,
            'optimal_tier': optimal_tier.tier_usd,
            'quality_score': quality_score,
            'threshold_status': threshold_status,
            'optimal_roi': optimal_tier.roi_percentage,
            'optimal_winrate': optimal_tier.winrate
        }

    def _calculate_quality_score(
        self,
        optimal_roi: float,
        optimal_winrate: float,
        global_roi: float,
        global_winrate: float,
        nb_trades: int
    ) -> float:
        """Calculate quality score for smart wallet identification."""
        roi_improvement = (
            (optimal_roi - global_roi) / abs(global_roi)
            if global_roi != 0 else 0
        )

        winrate_improvement = (
            (optimal_winrate - global_winrate) / global_winrate
            if global_winrate > 0 else 0
        )

        trade_volume_factor = min(nb_trades / 10, 1.0)

        score = (
            (roi_improvement * 0.4) +
            (winrate_improvement * 0.4) +
            (trade_volume_factor * 0.2)
        )

        return max(0, min(1, score))

    def _classify_threshold(self, quality_score: float) -> str:
        """Classify threshold status based on quality score."""
        if quality_score >= 0.8:
            return 'EXCELLENT'
        elif quality_score >= 0.6:
            return 'GOOD'
        elif quality_score >= 0.4:
            return 'AVERAGE'
        else:
            return 'POOR'

    def analyze_all_wallets(self) -> Dict[str, Any]:
        """Analyze tiers for all wallets."""
        wallets = Wallet.objects.all()

        results = {
            'total_wallets': 0,
            'smart_wallets': 0,
            'tier_performances_calculated': 0
        }

        for wallet in wallets:
            try:
                tier_results = self.analyze_wallet_tiers(wallet.address)
                results['tier_performances_calculated'] += len(tier_results)

                smart_result = self.identify_smart_wallet(wallet.address)
                if smart_result.get('is_smart'):
                    results['smart_wallets'] += 1

                results['total_wallets'] += 1

            except Exception:
                continue

        return results

    def get_smart_wallets(self) -> List[SmartWallet]:
        """Get all identified smart wallets."""
        return list(
            SmartWallet.objects.filter(
                optimal_threshold_tier__gt=0
            ).order_by('-quality_score')
        )
