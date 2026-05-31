"""Zerion wallet tracking service."""
from typing import List, Dict, Any
from datetime import datetime
from common.api_clients import ZerionClient
from wallets.models import Wallet, Token, Transaction, WalletPositionChange
from django.db import transaction as db_transaction
import uuid


class ZerionTrackerService:
    """Service to track wallet portfolios using Zerion."""

    def __init__(self):
        self.client = ZerionClient()

    def sync_wallet_portfolio(self, wallet_address: str) -> Dict[str, Any]:
        """Sync wallet portfolio from Zerion."""
        try:
            portfolio = self.client.get_wallet_portfolio(wallet_address)
            total_value = portfolio.get('attributes', {}).get('total', {}).get('value', 0)

            wallet, _ = Wallet.objects.update_or_create(
                address=wallet_address,
                defaults={'total_portfolio_value': total_value}
            )

            return {
                'success': True,
                'wallet_address': wallet_address,
                'total_value': total_value
            }

        except Exception as e:
            return {
                'success': False,
                'wallet_address': wallet_address,
                'error': str(e)
            }

    def sync_wallet_positions(self, wallet_address: str) -> Dict[str, Any]:
        """Sync current token positions from Zerion."""
        try:
            positions = self.client.get_wallet_positions(wallet_address)

            wallet = Wallet.objects.get(address=wallet_address)

            Token.objects.filter(wallet=wallet).update(in_portfolio=False)

            synced_count = 0
            for position in positions:
                attributes = position.get('attributes', {})
                fungible_info = attributes.get('fungible_info', {})

                token, created = Token.objects.update_or_create(
                    wallet=wallet,
                    fungible_id=position.get('id', ''),
                    defaults={
                        'symbol': fungible_info.get('symbol', ''),
                        'contract_address': fungible_info.get('implementations', [{}])[0].get('address', ''),
                        'chain': fungible_info.get('implementations', [{}])[0].get('chain_id', ''),
                        'current_amount': attributes.get('quantity', {}).get('float', 0),
                        'current_usd_value': attributes.get('value', 0),
                        'in_portfolio': True
                    }
                )
                synced_count += 1

            return {
                'success': True,
                'wallet_address': wallet_address,
                'tokens_synced': synced_count
            }

        except Exception as e:
            return {
                'success': False,
                'wallet_address': wallet_address,
                'error': str(e)
            }

    def sync_wallet_transactions(
        self,
        wallet_address: str,
        fungible_id: str = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Sync transaction history from Zerion."""
        try:
            transactions = self.client.get_wallet_transactions(
                wallet_address,
                fungible_id=fungible_id,
                limit=limit
            )

            wallet = Wallet.objects.get(address=wallet_address)

            synced_count = 0
            for tx in transactions:
                attributes = tx.get('attributes', {})
                changes = attributes.get('changes', [])

                for change in changes:
                    asset = change.get('asset', {})
                    fungible = asset.get('fungible_info', {})

                    Transaction.objects.update_or_create(
                        wallet=wallet,
                        hash=attributes.get('hash', ''),
                        defaults={
                            'fungible_id': asset.get('asset_code', ''),
                            'symbol': fungible.get('symbol', ''),
                            'contract_address': fungible.get('implementations', [{}])[0].get('address', ''),
                            'date': attributes.get('mined_at'),
                            'operation_type': attributes.get('operation_type', ''),
                            'action_type': 'buy' if change.get('direction') == 'in' else 'sell',
                            'quantity': abs(change.get('value', 0)),
                            'price_per_token': change.get('price', 0),
                            'total_value_usd': change.get('value', 0) * change.get('price', 0),
                            'direction': change.get('direction', ''),
                        }
                    )
                    synced_count += 1

            return {
                'success': True,
                'wallet_address': wallet_address,
                'transactions_synced': synced_count
            }

        except Exception as e:
            return {
                'success': False,
                'wallet_address': wallet_address,
                'error': str(e)
            }

    def full_sync(self, wallet_address: str) -> Dict[str, Any]:
        """Full sync of wallet data."""
        results = {
            'wallet_address': wallet_address,
            'portfolio': None,
            'positions': None,
            'transactions': None
        }

        results['portfolio'] = self.sync_wallet_portfolio(wallet_address)
        results['positions'] = self.sync_wallet_positions(wallet_address)
        results['transactions'] = self.sync_wallet_transactions(wallet_address)

        return results
