"""Dune Analytics wallet discovery service."""
from typing import List, Dict, Any
from common.api_clients import DuneClient
from wallets.models import Wallet, Token
from django.conf import settings


class DuneDiscoveryService:
    """Service to discover wallets using Dune Analytics."""

    def __init__(self):
        self.client = DuneClient()
        self.min_holders = getattr(settings, 'DUNE_MIN_HOLDERS', 10)
        self.min_wallet_value = getattr(settings, 'DUNE_MIN_WALLET_VALUE', 1000)

    def discover_token_holders(
        self,
        token_address: str,
        chain: str = "ethereum",
        query_id: int = None
    ) -> List[str]:
        """Discover wallets holding a specific token."""
        params = {
            "token_address": token_address,
            "chain": chain,
            "min_value_usd": self.min_wallet_value
        }

        results = self.client.execute_and_wait(query_id, params)
        return [row['wallet_address'] for row in results if row.get('wallet_address')]

    def discover_profitable_wallets(
        self,
        token_address: str,
        min_roi: float = 2.0,
        query_id: int = None
    ) -> List[Dict[str, Any]]:
        """Discover wallets with profitable trades on a token."""
        params = {
            "token_address": token_address,
            "min_roi_multiplier": min_roi
        }

        results = self.client.execute_and_wait(query_id, params)

        wallets = []
        for row in results:
            wallets.append({
                'address': row['wallet_address'],
                'roi': row.get('roi_multiplier', 0),
                'profit_usd': row.get('profit_usd', 0),
                'first_buy_date': row.get('first_buy_date'),
                'last_sell_date': row.get('last_sell_date')
            })

        return wallets

    def discover_early_buyers(
        self,
        token_address: str,
        hours_after_launch: int = 24,
        query_id: int = None
    ) -> List[str]:
        """Discover wallets that bought within X hours of token launch."""
        params = {
            "token_address": token_address,
            "hours_after_launch": hours_after_launch
        }

        results = self.client.execute_and_wait(query_id, params)
        return [row['wallet_address'] for row in results if row.get('wallet_address')]

    def save_discovered_wallets(
        self,
        wallet_addresses: List[str],
        period: str,
        token_address: str = None,
        chain: str = "ethereum"
    ) -> int:
        """Save discovered wallets to database."""
        created_count = 0

        for address in wallet_addresses:
            wallet, created = Wallet.objects.get_or_create(
                address=address,
                defaults={'period': period}
            )

            if created:
                created_count += 1

                if token_address:
                    Token.objects.get_or_create(
                        wallet=wallet,
                        contract_address=token_address,
                        defaults={
                            'chain': chain,
                            'symbol': '',
                            'fungible_id': f'{chain}:{token_address}',
                            'in_portfolio': True
                        }
                    )

        return created_count
