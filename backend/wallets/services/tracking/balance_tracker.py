"""Wallet balance change tracking service."""
from typing import List, Dict, Any
from datetime import datetime
from wallets.models import Wallet, Token, WalletPositionChange
from django.db.models import F
import uuid


class BalanceTrackerService:
    """Service to detect and track balance changes."""

    def __init__(self):
        self.session_id = str(uuid.uuid4())
        self.min_change_threshold = 0.01

    def detect_position_changes(self, wallet_address: str) -> List[Dict[str, Any]]:
        """Detect changes in wallet positions."""
        wallet = Wallet.objects.get(address=wallet_address)
        current_tokens = Token.objects.filter(wallet=wallet, in_portfolio=True)

        changes = []

        for token in current_tokens:
            change_type = self._determine_change_type(token)

            if change_type:
                change_data = {
                    'session_id': self.session_id,
                    'wallet': wallet,
                    'symbol': token.symbol,
                    'fungible_id': token.fungible_id,
                    'contract_address': token.contract_address,
                    'change_type': change_type,
                    'old_amount': token.previous_amount or 0,
                    'new_amount': token.current_amount,
                    'usd_change': self._calculate_usd_change(token),
                    'detected_at': datetime.now()
                }

                WalletPositionChange.objects.create(**change_data)
                changes.append(change_data)

                token.previous_amount = token.current_amount
                token.save(update_fields=['previous_amount'])

        return changes

    def _determine_change_type(self, token: Token) -> str:
        """Determine the type of position change."""
        old_amount = token.previous_amount or 0
        new_amount = token.current_amount

        if old_amount == 0 and new_amount > 0:
            return 'NEW'
        elif old_amount > 0 and new_amount == 0:
            return 'EXIT'
        elif new_amount > old_amount * (1 + self.min_change_threshold):
            return 'ACCUMULATION'
        elif new_amount < old_amount * (1 - self.min_change_threshold):
            return 'REDUCTION'

        return None

    def _calculate_usd_change(self, token: Token) -> float:
        """Calculate USD value change."""
        old_amount = token.previous_amount or 0
        new_amount = token.current_amount

        if old_amount == 0:
            return token.current_usd_value

        old_value = token.previous_usd_value or 0
        new_value = token.current_usd_value

        return new_value - old_value

    def get_recent_changes(
        self,
        wallet_address: str = None,
        limit: int = 100
    ) -> List[WalletPositionChange]:
        """Get recent position changes."""
        queryset = WalletPositionChange.objects.all()

        if wallet_address:
            queryset = queryset.filter(wallet__address=wallet_address)

        return list(queryset.order_by('-detected_at')[:limit])

    def clear_session_changes(self):
        """Clear changes from current session."""
        WalletPositionChange.objects.filter(session_id=self.session_id).delete()

    def analyze_change_patterns(
        self,
        wallet_address: str
    ) -> Dict[str, Any]:
        """Analyze position change patterns for a wallet."""
        changes = WalletPositionChange.objects.filter(
            wallet__address=wallet_address
        ).order_by('-detected_at')

        total_changes = changes.count()
        new_positions = changes.filter(change_type='NEW').count()
        exits = changes.filter(change_type='EXIT').count()
        accumulations = changes.filter(change_type='ACCUMULATION').count()
        reductions = changes.filter(change_type='REDUCTION').count()

        return {
            'wallet_address': wallet_address,
            'total_changes': total_changes,
            'new_positions': new_positions,
            'exits': exits,
            'accumulations': accumulations,
            'reductions': reductions,
            'activity_score': self._calculate_activity_score(
                new_positions, exits, accumulations, reductions
            )
        }

    def _calculate_activity_score(
        self,
        new: int,
        exits: int,
        accumulations: int,
        reductions: int
    ) -> float:
        """Calculate activity score based on change patterns."""
        total = new + exits + accumulations + reductions
        if total == 0:
            return 0.0

        new_score = (new / total) * 30
        exit_score = (exits / total) * 20
        accum_score = (accumulations / total) * 30
        reduce_score = (reductions / total) * 20

        return new_score + exit_score + accum_score + reduce_score
