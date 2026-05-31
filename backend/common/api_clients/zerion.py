"""Zerion API client."""
from django.conf import settings
from .base import BaseAPIClient
from typing import List, Dict, Any


class ZerionClient(BaseAPIClient):
    """Client for Zerion Wallet API."""

    def __init__(self):
        api_key = settings.ZERION_API_KEY if hasattr(settings, 'ZERION_API_KEY') else None
        super().__init__(api_key=api_key, base_url="https://api.zerion.io/v1/")

    def _get_headers(self) -> Dict[str, str]:
        """Override headers for Zerion API."""
        return {
            "accept": "application/json",
            "authorization": f"Basic {self.api_key}"
        }

    def get_wallet_portfolio(self, wallet_address: str, currency: str = "usd") -> Dict[str, Any]:
        """Get wallet portfolio from Zerion."""
        endpoint = f"wallets/{wallet_address}/portfolio"
        params = {
            "currency": currency,
            "filter[positions]": "only_simple",
            "sort": "value"
        }
        return self.get(endpoint, params=params)

    def get_wallet_positions(self, wallet_address: str) -> List[Dict]:
        """Get current token positions for a wallet."""
        endpoint = f"wallets/{wallet_address}/positions"
        params = {
            "filter[positions]": "only_simple",
            "filter[trash]": "only_non_trash",
            "currency": "usd",
            "sort": "value"
        }
        response = self.get(endpoint, params=params)
        return response.get('data', [])

    def get_wallet_transactions(
        self,
        wallet_address: str,
        fungible_id: str = None,
        limit: int = 100
    ) -> List[Dict]:
        """Get transaction history for a wallet."""
        endpoint = f"wallets/{wallet_address}/transactions"
        params = {
            "currency": "usd",
            "page[size]": limit
        }
        if fungible_id:
            params["filter[asset_id]"] = fungible_id

        response = self.get(endpoint, params=params)
        return response.get('data', [])
