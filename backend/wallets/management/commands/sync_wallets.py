"""Sync wallet data from Zerion."""
from django.core.management.base import BaseCommand
from wallets.models import Wallet
from wallets.services import ZerionTrackerService, BalanceTrackerService


class Command(BaseCommand):
    help = 'Sync wallet data from Zerion API'

    def add_arguments(self, parser):
        parser.add_argument(
            '--wallet',
            type=str,
            help='Specific wallet address to sync'
        )
        parser.add_argument(
            '--full',
            action='store_true',
            help='Full sync including transactions'
        )
        parser.add_argument(
            '--detect-changes',
            action='store_true',
            help='Detect position changes after sync'
        )

    def handle(self, *args, **options):
        tracker = ZerionTrackerService()
        balance_tracker = BalanceTrackerService()

        wallet_address = options.get('wallet')
        full_sync = options.get('full', False)
        detect_changes = options.get('detect_changes', False)

        if wallet_address:
            wallets = [wallet_address]
        else:
            wallets = Wallet.objects.values_list('address', flat=True)

        self.stdout.write(f"Syncing {len(wallets)} wallets...")

        synced = 0
        errors = 0

        for address in wallets:
            try:
                if full_sync:
                    result = tracker.full_sync(address)
                    if all(r.get('success') for r in result.values() if r):
                        synced += 1
                        self.stdout.write(
                            self.style.SUCCESS(f"✓ Full sync: {address}")
                        )
                    else:
                        errors += 1
                        self.stdout.write(
                            self.style.ERROR(f"✗ Failed: {address}")
                        )
                else:
                    portfolio_result = tracker.sync_wallet_portfolio(address)
                    positions_result = tracker.sync_wallet_positions(address)

                    if portfolio_result['success'] and positions_result['success']:
                        synced += 1
                        self.stdout.write(
                            self.style.SUCCESS(
                                f"✓ {address}: {positions_result['tokens_synced']} tokens"
                            )
                        )
                    else:
                        errors += 1
                        self.stdout.write(
                            self.style.ERROR(f"✗ Failed: {address}")
                        )

                if detect_changes:
                    changes = balance_tracker.detect_position_changes(address)
                    if changes:
                        self.stdout.write(
                            f"  → {len(changes)} position changes detected"
                        )

            except Exception as e:
                errors += 1
                self.stdout.write(
                    self.style.ERROR(f"✗ Error {address}: {str(e)}")
                )

        self.stdout.write(
            self.style.SUCCESS(
                f"\nCompleted: {synced} synced, {errors} errors"
            )
        )
