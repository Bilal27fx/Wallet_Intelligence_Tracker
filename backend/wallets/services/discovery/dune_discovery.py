"""Dune Analytics wallet discovery service."""
from typing import List, Dict, Any
import logging
from common.api_clients import DuneClient
from wallets.models import Wallet, Token, ExplosiveToken, WalletBrute
from django.conf import settings

logger = logging.getLogger(__name__)


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
        perf_hours: int = 24,
        early_hours: int = 48,
        query_id: int = None
    ) -> List[str]:
        """Discover wallets that bought within early window after token launch."""
        params = {
            "token_address": token_address,
            "perf_window": perf_hours,
            "early_window": early_hours
        }

        results = self.client.execute_and_wait(query_id, params)
        return [row.get('wallet') or row.get('wallet_address') for row in results if (row.get('wallet') or row.get('wallet_address'))]

    def save_discovered_wallets(
        self,
        wallet_addresses: List[str],
        period: str,
        token_address: str = None,
        chain: str = "ethereum"
    ) -> int:
        """Save discovered wallets to WalletBrute (discovery table)."""
        created_count = 0

        for address in wallet_addresses:
            if not address:
                continue

            # Save to WalletBrute (discovery table)
            _, created = WalletBrute.objects.get_or_create(
                wallet_address=address,
                token_address=token_address or '',
                temporality=period,
                defaults={
                    'chain': chain,
                    'contract_address': token_address or ''
                }
            )

            if created:
                created_count += 1

        return created_count

    def discover_all_wallets(self) -> Dict[str, Any]:
        """
        Discover wallets for all untreated explosive tokens.
        For each token with hours_since_now, query Dune for early buyers.
        """
        # Get untreated explosive tokens
        tokens = ExplosiveToken.objects.filter(
            hours_since_now__isnull=False,
            traite=False
        )

        if not tokens.exists():
            logger.warning("No untreated explosive tokens found")
            return {'total_tokens': 0, 'total_wallets': 0}

        logger.info(f"Discovering wallets for {tokens.count()} explosive tokens")
        total_wallets = 0
        processed_tokens = 0

        chain_mapping = {
            'base': 'base',
            'bsc': 'bnb',
            'ethereum': 'ethereum'
        }

        # Get Dune query IDs from settings
        query_ids = getattr(settings, 'DUNE_QUERY_IDS', {})

        # Type 3 extra hours (for long-term tokens)
        extra_hours = 720  # 30 days

        for token in tokens:
            chain_key = chain_mapping.get(token.chain)
            if not chain_key:
                logger.warning(f"Chain not supported: {token.chain}")
                continue

            # Get query ID for this chain
            query_id = query_ids.get(chain_key)
            if not query_id:
                logger.warning(f"No Dune query ID configured for chain: {chain_key}")
                continue

            # Use hours_since_now as performance window
            perf_hours = round(token.hours_since_now) if token.hours_since_now else 0
            early_hours = perf_hours + extra_hours

            logger.info(
                f"[{processed_tokens + 1}/{tokens.count()}] {token.symbol} ({token.chain}) | "
                f"perf={perf_hours}h | early={early_hours}h"
            )

            try:
                # Call Dune API to discover wallets
                # Using same parameter names as old system: perf_window + early_window
                params = {
                    "token_address": token.token_address,
                    "perf_window": perf_hours,
                    "early_window": early_hours
                }

                logger.info(f"Calling Dune query {query_id} for {token.symbol} ({token.token_address})")
                logger.info(f"  Params: perf_window={perf_hours}h, early_window={early_hours}h")

                wallet_addresses = self.discover_early_buyers(
                    token_address=token.token_address,
                    perf_hours=perf_hours,
                    early_hours=early_hours,
                    query_id=query_id
                )

                # Save discovered wallets
                wallets_created = self.save_discovered_wallets(
                    wallet_addresses=wallet_addresses,
                    period='discovery',
                    token_address=token.token_address,
                    chain=token.chain
                )

                # Mark token as treated
                token.traite = True
                token.save()

                total_wallets += wallets_created
                processed_tokens += 1

                logger.info(f"Processed {token.symbol}: {wallets_created} wallets discovered")

            except Exception as e:
                logger.error(f"Error processing {token.symbol}: {e}")
                continue

        logger.info(f"Discovery completed: {processed_tokens} tokens processed, {total_wallets} wallets discovered")
        return {
            'total_tokens': processed_tokens,
            'total_wallets': total_wallets
        }
