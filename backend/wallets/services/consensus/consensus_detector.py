"""Consensus detection service for smart wallet signals."""
from typing import List, Dict, Any
from datetime import datetime, timedelta
from wallets.models import (
    SmartWallet, WalletPositionChange, Token,
    ConsensusSignal
)
from django.db.models import Count, Q
from django.conf import settings


class ConsensusDetectorService:
    """Service to detect consensus signals from smart wallets."""

    def __init__(self):
        self.min_wallets = getattr(settings, 'CONSENSUS_MIN_WALLETS', 3)
        self.time_window_hours = getattr(settings, 'CONSENSUS_TIME_WINDOW', 24)
        self.min_confidence = getattr(settings, 'CONSENSUS_MIN_CONFIDENCE', 0.6)

    def detect_consensus_buys(
        self,
        hours: int = None
    ) -> List[Dict[str, Any]]:
        """Detect tokens with consensus buy signals."""
        hours = hours or self.time_window_hours
        since = datetime.now() - timedelta(hours=hours)

        smart_wallets = SmartWallet.objects.filter(
            optimal_threshold_tier__gt=0
        ).values_list('wallet__address', flat=True)

        new_positions = WalletPositionChange.objects.filter(
            wallet__address__in=smart_wallets,
            change_type='NEW',
            detected_at__gte=since
        )

        token_counts = new_positions.values(
            'contract_address', 'symbol'
        ).annotate(
            wallet_count=Count('wallet', distinct=True)
        ).filter(
            wallet_count__gte=self.min_wallets
        )

        consensus_signals = []

        for token_data in token_counts:
            contract_address = token_data['contract_address']
            symbol = token_data['symbol']
            wallet_count = token_data['wallet_count']

            wallet_addresses = list(
                new_positions.filter(
                    contract_address=contract_address
                ).values_list('wallet__address', flat=True).distinct()
            )

            confidence_score = self._calculate_confidence(
                wallet_count=wallet_count,
                total_smart_wallets=len(smart_wallets),
                wallet_addresses=wallet_addresses
            )

            if confidence_score >= self.min_confidence:
                signal_data = {
                    'token_address': contract_address,
                    'symbol': symbol,
                    'signal_type': 'BUY',
                    'wallet_count': wallet_count,
                    'confidence_score': confidence_score,
                    'detected_at': datetime.now(),
                    'smart_wallets': wallet_addresses
                }

                ConsensusSignal.objects.create(
                    token_address=contract_address,
                    symbol=symbol,
                    signal_type='BUY',
                    wallet_count=wallet_count,
                    confidence_score=confidence_score
                )

                consensus_signals.append(signal_data)

        return consensus_signals

    def detect_consensus_sells(
        self,
        hours: int = None
    ) -> List[Dict[str, Any]]:
        """Detect tokens with consensus sell signals."""
        hours = hours or self.time_window_hours
        since = datetime.now() - timedelta(hours=hours)

        smart_wallets = SmartWallet.objects.filter(
            optimal_threshold_tier__gt=0
        ).values_list('wallet__address', flat=True)

        exit_positions = WalletPositionChange.objects.filter(
            wallet__address__in=smart_wallets,
            change_type__in=['EXIT', 'REDUCTION'],
            detected_at__gte=since
        )

        token_counts = exit_positions.values(
            'contract_address', 'symbol'
        ).annotate(
            wallet_count=Count('wallet', distinct=True)
        ).filter(
            wallet_count__gte=self.min_wallets
        )

        consensus_signals = []

        for token_data in token_counts:
            contract_address = token_data['contract_address']
            symbol = token_data['symbol']
            wallet_count = token_data['wallet_count']

            wallet_addresses = list(
                exit_positions.filter(
                    contract_address=contract_address
                ).values_list('wallet__address', flat=True).distinct()
            )

            confidence_score = self._calculate_confidence(
                wallet_count=wallet_count,
                total_smart_wallets=len(smart_wallets),
                wallet_addresses=wallet_addresses
            )

            if confidence_score >= self.min_confidence:
                signal_data = {
                    'token_address': contract_address,
                    'symbol': symbol,
                    'signal_type': 'SELL',
                    'wallet_count': wallet_count,
                    'confidence_score': confidence_score,
                    'detected_at': datetime.now(),
                    'smart_wallets': wallet_addresses
                }

                ConsensusSignal.objects.create(
                    token_address=contract_address,
                    symbol=symbol,
                    signal_type='SELL',
                    wallet_count=wallet_count,
                    confidence_score=confidence_score
                )

                consensus_signals.append(signal_data)

        return consensus_signals

    def _calculate_confidence(
        self,
        wallet_count: int,
        total_smart_wallets: int,
        wallet_addresses: List[str]
    ) -> float:
        """Calculate confidence score for consensus signal."""
        participation_rate = wallet_count / total_smart_wallets

        smart_wallets = SmartWallet.objects.filter(
            wallet__address__in=wallet_addresses
        )

        avg_quality = sum(
            sw.quality_score for sw in smart_wallets
        ) / len(smart_wallets) if smart_wallets else 0

        confidence = (participation_rate * 0.5) + (avg_quality * 0.5)

        return min(1.0, confidence)

    def get_active_signals(
        self,
        hours: int = None
    ) -> List[ConsensusSignal]:
        """Get active consensus signals."""
        hours = hours or self.time_window_hours
        since = datetime.now() - timedelta(hours=hours)

        return list(
            ConsensusSignal.objects.filter(
                detected_at__gte=since
            ).order_by('-confidence_score', '-detected_at')
        )

    def run_consensus_detection(self) -> Dict[str, Any]:
        """Run full consensus detection."""
        buy_signals = self.detect_consensus_buys()
        sell_signals = self.detect_consensus_sells()

        return {
            'buy_signals': len(buy_signals),
            'sell_signals': len(sell_signals),
            'total_signals': len(buy_signals) + len(sell_signals),
            'detected_at': datetime.now().isoformat(),
            'signals': {
                'buys': buy_signals,
                'sells': sell_signals
            }
        }
