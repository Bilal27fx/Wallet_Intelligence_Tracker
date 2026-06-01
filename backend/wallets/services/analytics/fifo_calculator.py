"""FIFO calculation service for token analytics."""
from typing import List, Dict, Any
from datetime import datetime
from wallets.models import Transaction, TokenAnalytics, Wallet
from django.db.models import Q


class FIFOCalculatorService:
    """Service to calculate FIFO-based token analytics."""

    def calculate_token_analytics(
        self,
        wallet_address: str,
        symbol: str = None
    ) -> List[Dict[str, Any]]:
        """Calculate FIFO analytics for wallet tokens."""
        wallet = Wallet.objects.get(address=wallet_address)

        if symbol:
            symbols = [symbol]
        else:
            symbols = Transaction.objects.filter(
                wallet=wallet
            ).values_list('symbol', flat=True).distinct()

        results = []
        for sym in symbols:
            analytics = self._calculate_fifo_for_token(wallet, sym)
            if analytics:
                results.append(analytics)

        return results

    def _calculate_fifo_for_token(
        self,
        wallet: Wallet,
        symbol: str
    ) -> Dict[str, Any]:
        """Calculate FIFO analytics for a specific token."""
        transactions = Transaction.objects.filter(
            wallet=wallet,
            symbol=symbol
        ).order_by('date')

        if not transactions.exists():
            return None

        buy_queue = []
        total_invested = 0.0
        total_realized = 0.0
        holding_days = 0

        for tx in transactions:
            if tx.action_type == 'buy':
                buy_queue.append({
                    'quantity': abs(tx.quantity),
                    'price': tx.price_per_token,
                    'date': tx.date,
                    'total_cost': abs(tx.quantity) * tx.price_per_token
                })
                total_invested += abs(tx.quantity) * tx.price_per_token

            elif tx.action_type == 'sell':
                sell_quantity = abs(tx.quantity)
                sell_proceeds = sell_quantity * tx.price_per_token

                while sell_quantity > 0 and buy_queue:
                    buy_entry = buy_queue[0]

                    if buy_entry['quantity'] <= sell_quantity:
                        cost_basis = buy_entry['total_cost']
                        proceeds = buy_entry['quantity'] * tx.price_per_token

                        total_realized += proceeds - cost_basis

                        if buy_entry['date'] and tx.date:
                            holding_days += (tx.date - buy_entry['date']).days

                        sell_quantity -= buy_entry['quantity']
                        buy_queue.pop(0)
                    else:
                        cost_per_unit = buy_entry['total_cost'] / buy_entry['quantity']
                        cost_basis = sell_quantity * cost_per_unit
                        proceeds = sell_quantity * tx.price_per_token

                        total_realized += proceeds - cost_basis

                        if buy_entry['date'] and tx.date:
                            holding_days += (tx.date - buy_entry['date']).days

                        buy_entry['quantity'] -= sell_quantity
                        buy_entry['total_cost'] -= cost_basis
                        sell_quantity = 0

        unrealized_value = sum(entry['total_cost'] for entry in buy_queue)
        total_return = total_realized + unrealized_value - total_invested

        roi_percentage = (total_return / total_invested * 100) if total_invested > 0 else 0
        is_winning = total_return > 0

        avg_holding_days = holding_days // len([tx for tx in transactions if tx.action_type == 'sell']) if transactions.filter(action_type='sell').exists() else 0

        analytics_data = {
            'wallet': wallet,
            'token_symbol': symbol,
            'total_invested': total_invested,
            'total_realized': total_realized,
            'roi_percentage': roi_percentage,
            'is_winning': is_winning,
            'status': 'GAGNANT' if is_winning else 'PERDANT',
            'holding_days': avg_holding_days,
            'in_portfolio': len(buy_queue) > 0
        }

        TokenAnalytics.objects.update_or_create(
            wallet=wallet,
            token_symbol=symbol,
            defaults=analytics_data
        )

        return analytics_data

    def recalculate_all_analytics(self, wallet_address: str = None):
        """Recalculate analytics for all wallets or specific wallet."""
        if wallet_address:
            wallets = Wallet.objects.filter(address=wallet_address)
        else:
            wallets = Wallet.objects.all()

        total_calculated = 0
        for wallet in wallets:
            results = self.calculate_token_analytics(wallet.address)
            total_calculated += len(results)

        return {
            'wallets_processed': wallets.count(),
            'tokens_calculated': total_calculated
        }

    def get_winning_tokens(self, wallet_address: str) -> List[TokenAnalytics]:
        """Get winning tokens for a wallet."""
        wallet = Wallet.objects.get(address=wallet_address)
        return list(
            TokenAnalytics.objects.filter(
                wallet=wallet,
                is_winning=True
            ).order_by('-roi_percentage')
        )

    def get_losing_tokens(self, wallet_address: str) -> List[TokenAnalytics]:
        """Get losing tokens for a wallet."""
        wallet = Wallet.objects.get(address=wallet_address)
        return list(
            TokenAnalytics.objects.filter(
                wallet=wallet,
                is_winning=False
            ).order_by('roi_percentage')
        )
