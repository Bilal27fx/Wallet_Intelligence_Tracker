"""DexScreener API client."""
from .base import BaseAPIClient
from typing import Dict, Any, List


class DexScreenerClient(BaseAPIClient):
    """Client for DexScreener API."""

    def __init__(self):
        super().__init__(base_url="https://api.dexscreener.com/latest/")

    def _get_headers(self) -> Dict[str, str]:
        """DexScreener doesn't require API key."""
        return {"Content-Type": "application/json"}

    def get_token_profiles(self, chain_id: str, token_addresses: List[str]) -> List[Dict[str, Any]]:
        """Get token profiles by chain and addresses."""
        addresses = ",".join(token_addresses[:30])
        endpoint = f"dex/tokens/{chain_id}/{addresses}"
        response = self.get(endpoint)
        return response.get('pairs', [])

    def search_pairs(self, query: str) -> List[Dict[str, Any]]:
        """Search for trading pairs."""
        endpoint = f"dex/search/?q={query}"
        response = self.get(endpoint)
        return response.get('pairs', [])

    def get_token_pairs(self, token_address: str) -> List[Dict[str, Any]]:
        """Get all pairs for a token address."""
        endpoint = f"dex/tokens/{token_address}"
        response = self.get(endpoint)
        return response.get('pairs', [])

    def get_pair_by_address(self, chain_id: str, pair_address: str) -> Dict[str, Any]:
        """Get specific pair by chain and pair address."""
        endpoint = f"dex/pairs/{chain_id}/{pair_address}"
        response = self.get(endpoint)
        pairs = response.get('pairs', [])
        return pairs[0] if pairs else {}
