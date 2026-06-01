"""Wallet initialization service - initializes newly discovered wallets with filtering."""
from typing import Dict, Any, Tuple
import logging
from wallets.models import Wallet, WalletBrute
from wallets.services.tracking import ZerionTrackerService
from django.db import transaction
from django.conf import settings

logger = logging.getLogger(__name__)


class WalletInitializerService:
    """Service to initialize newly discovered wallets from wallet_brute with filtering."""

    def __init__(self):
        self.zerion = ZerionTrackerService()
        self.config = getattr(settings, 'WALLET_FILTER', {})

        # Filter criteria
        self.min_token_value = self.config.get('MIN_TOKEN_VALUE_USD', 500)
        self.min_wallet_value = self.config.get('MIN_WALLET_VALUE_USD', 50000)
        self.max_wallet_value = self.config.get('MAX_WALLET_VALUE_USD', 50000000)
        self.min_tokens = self.config.get('MIN_TOKENS_PER_WALLET', 2)
        self.max_tokens = self.config.get('MAX_TOKENS_PER_WALLET', 60)
        self.excluded_tokens = set(self.config.get('EXCLUDED_TOKENS', ()))

    def _wallet_tag(self, address: str) -> str:
        """Return short wallet identifier for logs."""
        if not address or len(address) < 16:
            return address or "unknown"
        return f"{address[:10]}...{address[-6:]}"

    def _fmt_usd(self, value: float) -> str:
        """Format USD amount for logs."""
        return f"${value:,.0f}"

    def _evaluate_wallet(self, wallet_address: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Evaluate if wallet meets filtering criteria.
        Returns: (should_initialize, decision_info)
        """
        # Check if already in database
        if Wallet.objects.filter(address=wallet_address).exists():
            return False, {
                'status': 'SKIP',
                'reason': 'already_in_db'
            }

        # Fetch positions from Zerion
        try:
            positions_data = self.zerion.get_wallet_positions(wallet_address)
        except Exception as e:
            logger.error(f"Error fetching positions for {self._wallet_tag(wallet_address)}: {e}")
            return False, {
                'status': 'ERROR',
                'reason': 'zerion_request_failed',
                'error': str(e)
            }

        if not positions_data:
            return False, {
                'status': 'SKIP',
                'reason': 'no_positions_data'
            }

        # Calculate total wallet value
        all_positions = positions_data.get('positions', [])
        total_value = sum(
            float(pos.get('usd_value', 0))
            for pos in all_positions
        )

        # Filter 1: Wallet value range
        if total_value < self.min_wallet_value:
            return False, {
                'status': 'SKIP',
                'reason': 'wallet_value_below_min',
                'wallet_value': self._fmt_usd(total_value),
                'min_required': self._fmt_usd(self.min_wallet_value)
            }

        if total_value > self.max_wallet_value:
            return False, {
                'status': 'SKIP',
                'reason': 'wallet_value_above_max',
                'wallet_value': self._fmt_usd(total_value),
                'max_allowed': self._fmt_usd(self.max_wallet_value)
            }

        # Filter 2: Token-level filtering
        valid_positions = []
        excluded_positions = []

        for pos in all_positions:
            symbol = pos.get('symbol', '').upper()
            usd_value = float(pos.get('usd_value', 0))

            # Skip tokens below minimum value
            if usd_value < self.min_token_value:
                continue

            # Separate excluded vs valid tokens
            if symbol in self.excluded_tokens:
                excluded_positions.append(pos)
            else:
                valid_positions.append(pos)

        # Filter 3: Valid token count
        if len(valid_positions) < self.min_tokens:
            return False, {
                'status': 'SKIP',
                'reason': 'valid_tokens_below_min',
                'wallet_value': self._fmt_usd(total_value),
                'valid_tokens': len(valid_positions),
                'min_required': self.min_tokens,
                'excluded_tokens': len(excluded_positions)
            }

        # Filter 4: Total token count (valid + excluded)
        total_tokens = len(valid_positions) + len(excluded_positions)
        if total_tokens > self.max_tokens:
            return False, {
                'status': 'SKIP',
                'reason': 'total_tokens_above_max',
                'wallet_value': self._fmt_usd(total_value),
                'total_tokens': total_tokens,
                'max_allowed': self.max_tokens,
                'excluded_tokens': len(excluded_positions)
            }

        # Wallet passes all filters
        return True, {
            'status': 'VALID',
            'wallet_value': self._fmt_usd(total_value),
            'valid_tokens': len(valid_positions),
            'excluded_tokens': len(excluded_positions),
            'total_tokens': total_tokens
        }

    def initialize_discovered_wallets(self) -> Dict[str, Any]:
        """
        Initialize all wallets from wallet_brute with filtering:
        1. Get wallets from wallet_brute
        2. For each wallet:
           - Fetch balances from Zerion
           - Apply filtering criteria
           - Initialize only if passes filters
        3. Clear wallet_brute
        """
        # Get all unique wallets from wallet_brute
        wallet_entries = WalletBrute.objects.values(
            'wallet_address', 'temporality'
        ).distinct()

        if not wallet_entries.exists():
            logger.warning("No wallets found in wallet_brute")
            return {
                'total_wallets': 0,
                'initialized': 0,
                'failed': 0,
                'skipped': 0,
                'wallet_brute_cleared': 0,
                'skip_reasons': {}
            }

        logger.info(f"[1/3] Evaluating {wallet_entries.count()} wallets from wallet_brute")

        initialized = 0
        failed = 0
        skipped = 0
        skip_reasons = {}

        for entry in wallet_entries:
            wallet_address = entry['wallet_address']
            period = entry['temporality'] or 'manual'
            tag = self._wallet_tag(wallet_address)

            # Evaluate wallet against filters
            should_init, decision = self._evaluate_wallet(wallet_address)

            if not should_init:
                status = decision.get('status', 'SKIP')
                reason = decision.get('reason', 'unknown')

                # Log skip/error
                if status == 'ERROR':
                    logger.error(f"[{tag}] ERROR | period={period} | reason={reason}")
                    failed += 1
                else:
                    # Build log message with decision details
                    details = {k: v for k, v in decision.items() if k not in ('status', 'reason')}
                    detail_str = ' | '.join(f"{k}={v}" for k, v in details.items())
                    logger.info(f"[{tag}] SKIP | period={period} | reason={reason} | {detail_str}")
                    skipped += 1
                    skip_reasons[reason] = skip_reasons.get(reason, 0) + 1

                continue

            # Wallet passes all filters - initialize it
            try:
                logger.info(
                    f"[{tag}] VALID | period={period} | "
                    f"wallet_value={decision.get('wallet_value')} | "
                    f"valid_tokens={decision.get('valid_tokens')} | "
                    f"excluded_tokens={decision.get('excluded_tokens')}"
                )

                # Create or get wallet
                Wallet.objects.get_or_create(
                    address=wallet_address,
                    defaults={'period': period}
                )

                # Full sync from Zerion (balances + transactions)
                self.zerion.full_sync(wallet_address)

                initialized += 1
                logger.info(f"[{tag}] INITIALIZED | period={period}")

            except Exception as e:
                logger.error(f"Failed to initialize wallet {tag}: {e}")
                failed += 1
                continue

        # Clear wallet_brute after processing
        logger.info("[3/3] Clearing wallet_brute table")
        deleted_count = WalletBrute.objects.all().delete()[0]
        logger.info(f"  {deleted_count} entries deleted from wallet_brute")

        # Log summary
        logger.info(
            f"Wallet initialization completed: "
            f"{initialized} initialized, {skipped} skipped, {failed} failed"
        )

        if skip_reasons:
            logger.info("Skip reasons breakdown:")
            for reason, count in sorted(skip_reasons.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"  {reason}: {count}")

        return {
            'total_wallets': wallet_entries.count(),
            'initialized': initialized,
            'skipped': skipped,
            'failed': failed,
            'wallet_brute_cleared': deleted_count,
            'skip_reasons': skip_reasons
        }

    def get_pending_wallets_count(self) -> int:
        """Get count of wallets pending initialization in wallet_brute."""
        return WalletBrute.objects.values('wallet_address').distinct().count()
