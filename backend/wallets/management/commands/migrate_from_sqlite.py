"""Migrate data from SQLite to PostgreSQL."""
import sqlite3
from django.core.management.base import BaseCommand
from django.db import transaction
from wallets.models import (
    Wallet, Token, Transaction, WalletPositionChange,
    WalletBrute, TokenAnalytics, WalletTierPerformance,
    WalletQualified, SmartWallet
)


class Command(BaseCommand):
    help = 'Migrate data from SQLite to PostgreSQL'

    def add_arguments(self, parser):
        parser.add_argument(
            '--sqlite-path',
            type=str,
            default='data/db/wit_database.db',
            help='Path to SQLite database'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Run without committing changes'
        )

    def handle(self, *args, **options):
        sqlite_path = options['sqlite_path']
        dry_run = options['dry_run']

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN MODE - No changes will be committed"))

        self.stdout.write(f"Connecting to SQLite: {sqlite_path}")

        try:
            conn = sqlite3.connect(sqlite_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Failed to connect to SQLite: {e}"))
            return

        try:
            # Check which tables exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            existing_tables = {row['name'] for row in cursor.fetchall()}
            self.stdout.write(f"Found tables in SQLite: {', '.join(sorted(existing_tables))}\n")

            # Migrate wallets (required first - other tables have FK to wallets)
            if 'wallets' in existing_tables:
                self._migrate_wallets(cursor, dry_run)
            else:
                self.stdout.write(self.style.ERROR("No 'wallets' table found - cannot proceed"))
                return

            # Migrate tokens (requires wallets)
            if 'tokens' in existing_tables:
                self._migrate_tokens(cursor, dry_run)

            # Migrate transactions (requires wallets)
            if 'transaction_history' in existing_tables:
                self._migrate_transactions(cursor, dry_run)

            # Migrate token analytics (requires wallets)
            if 'token_analytics' in existing_tables:
                self._migrate_token_analytics(cursor, dry_run)

            # Migrate wallet tier performance (requires wallets)
            if 'wallet_tier_performance' in existing_tables:
                self._migrate_wallet_tier_performance(cursor, dry_run)

            # Migrate wallet qualified (requires wallets)
            if 'wallet_qualified' in existing_tables:
                self._migrate_wallet_qualified(cursor, dry_run)

            # Migrate smart wallets (requires wallets)
            if 'smart_wallets' in existing_tables:
                self._migrate_smart_wallets(cursor, dry_run)

            # Migrate wallet brute (independent)
            if 'wallet_brute' in existing_tables:
                self._migrate_wallet_brute(cursor, dry_run)

            # Migrate wallet position changes (requires wallets)
            if 'wallet_position_changes' in existing_tables:
                self._migrate_wallet_position_changes(cursor, dry_run)

            conn.close()

            if dry_run:
                self.stdout.write(self.style.WARNING("\nDRY RUN COMPLETED - No changes were committed"))
            else:
                self.stdout.write(self.style.SUCCESS("\n🎉 Migration completed successfully!"))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\nMigration failed: {e}"))
            import traceback
            traceback.print_exc()

    @transaction.atomic
    def _migrate_wallets(self, cursor, dry_run):
        self.stdout.write("Migrating wallets...")
        cursor.execute("SELECT * FROM wallets")
        wallets_count = 0

        for row in cursor.fetchall():
            if not dry_run:
                Wallet.objects.update_or_create(
                    address=row['wallet_address'],
                    defaults={
                        'period': row['period'] or '14d',
                        'total_portfolio_value': row['total_portfolio_value'],
                    }
                )
            wallets_count += 1

        self.stdout.write(self.style.SUCCESS(f"✓ Migrated {wallets_count} wallets"))

    @transaction.atomic
    def _migrate_tokens(self, cursor, dry_run):
        self.stdout.write("Migrating tokens...")
        cursor.execute("SELECT * FROM tokens")

        # Preload all wallets into memory
        wallet_dict = {w.address: w for w in Wallet.objects.all()}

        tokens_to_create = []
        tokens_count = 0
        batch_size = 1000

        for row in cursor.fetchall():
            wallet = wallet_dict.get(row['wallet_address'])
            if not wallet:
                continue

            if not dry_run:
                tokens_to_create.append(Token(
                    wallet=wallet,
                    fungible_id=row['fungible_id'],
                    symbol=row['symbol'] or '',
                    contract_address=row['contract_address'] or '',
                    chain=row['chain'] or '',
                    amount=row['current_amount'] or 0,
                    usd_value=row['current_usd_value'] or 0,
                    in_portfolio=bool(row['in_portfolio']),
                ))

                if len(tokens_to_create) >= batch_size:
                    Token.objects.bulk_create(tokens_to_create, ignore_conflicts=True)
                    tokens_count += len(tokens_to_create)
                    self.stdout.write(f"  {tokens_count} tokens migrated...")
                    tokens_to_create = []
            else:
                tokens_count += 1

        # Insert remaining
        if tokens_to_create and not dry_run:
            Token.objects.bulk_create(tokens_to_create, ignore_conflicts=True)
            tokens_count += len(tokens_to_create)

        self.stdout.write(self.style.SUCCESS(f"✓ Migrated {tokens_count} tokens"))

    @transaction.atomic
    def _migrate_transactions(self, cursor, dry_run):
        self.stdout.write("Migrating transactions (this may take a few minutes)...")
        cursor.execute("SELECT * FROM transaction_history")

        # Preload all wallets into memory
        wallet_dict = {w.address: w for w in Wallet.objects.all()}

        transactions_to_create = []
        tx_count = 0
        batch_size = 5000

        for row in cursor.fetchall():
            wallet = wallet_dict.get(row['wallet_address'])
            if not wallet:
                continue

            if not dry_run:
                transactions_to_create.append(Transaction(
                    wallet=wallet,
                    hash=row['hash'],
                    fungible_id=row['fungible_id'] or '',
                    symbol=row['symbol'] or '',
                    date=row['date'],
                    operation_type=row['operation_type'] or '',
                    action_type=row['action_type'] or '',
                    swap_description=row['swap_description'] or '',
                    contract_address=row['contract_address'] or '',
                    quantity=row['quantity'] or 0,
                    price_per_token=row['price_per_token'] or 0,
                    total_value_usd=row['total_value_usd'] or 0,
                    direction=row['direction'] or '',
                    recipient_address=row['recipient_address'] or '',
                    sender_address=row['sender_address'] or '',
                ))

                if len(transactions_to_create) >= batch_size:
                    Transaction.objects.bulk_create(transactions_to_create, ignore_conflicts=True)
                    tx_count += len(transactions_to_create)
                    self.stdout.write(f"  {tx_count} transactions migrated...")
                    transactions_to_create = []
            else:
                tx_count += 1

        # Insert remaining
        if transactions_to_create and not dry_run:
            Transaction.objects.bulk_create(transactions_to_create, ignore_conflicts=True)
            tx_count += len(transactions_to_create)

        self.stdout.write(self.style.SUCCESS(f"✓ Migrated {tx_count} transactions"))

    @transaction.atomic
    def _migrate_token_analytics(self, cursor, dry_run):
        self.stdout.write("Migrating token analytics...")
        cursor.execute("SELECT * FROM token_analytics")

        wallet_dict = {w.address: w for w in Wallet.objects.all()}
        analytics_to_create = []
        analytics_count = 0
        batch_size = 1000

        for row in cursor.fetchall():
            wallet = wallet_dict.get(row['wallet_address'])
            if not wallet:
                continue

            if not dry_run:
                roi = row['roi_percentage'] or 0
                is_winning = roi > 0
                status = 'GAGNANT' if roi > 0 else ('PERDANT' if roi < 0 else 'NEUTRE')

                # Calculate holding days from first/last transaction dates
                holding_days = 0
                try:
                    if row['first_transaction_date'] and row['last_transaction_date']:
                        from datetime import datetime
                        first = datetime.fromisoformat(row['first_transaction_date'])
                        last = datetime.fromisoformat(row['last_transaction_date'])
                        holding_days = (last - first).days
                except (KeyError, ValueError, TypeError):
                    holding_days = 0

                try:
                    in_portfolio = bool(row['in_portfolio'])
                except (KeyError, TypeError):
                    in_portfolio = True

                analytics_to_create.append(TokenAnalytics(
                    wallet=wallet,
                    token_symbol=row['token_symbol'],
                    total_invested=row['total_invested'] or 0,
                    total_realized=row['total_realized'] or 0,
                    roi_percentage=roi,
                    is_winning=is_winning,
                    status=status,
                    holding_days=holding_days,
                    in_portfolio=in_portfolio,
                ))

                if len(analytics_to_create) >= batch_size:
                    TokenAnalytics.objects.bulk_create(analytics_to_create, ignore_conflicts=True)
                    analytics_count += len(analytics_to_create)
                    analytics_to_create = []
            else:
                analytics_count += 1

        # Insert remaining
        if analytics_to_create and not dry_run:
            TokenAnalytics.objects.bulk_create(analytics_to_create, ignore_conflicts=True)
            analytics_count += len(analytics_to_create)

        self.stdout.write(self.style.SUCCESS(f"✓ Migrated {analytics_count} token analytics"))

    @transaction.atomic
    def _migrate_wallet_tier_performance(self, cursor, dry_run):
        self.stdout.write("Migrating wallet tier performance...")
        cursor.execute("SELECT * FROM wallet_tier_performance")
        tier_count = 0

        for row in cursor.fetchall():
            if not dry_run:
                try:
                    wallet = Wallet.objects.get(address=row['wallet_address'])
                    WalletTierPerformance.objects.update_or_create(
                        wallet=wallet,
                        tier_usd=row['tier_usd'],
                        defaults={
                            'roi_percentage': row['roi_percentage'] or 0,
                            'winrate': row['winrate'] or 0,
                            'nb_trades': row['nb_trades'] or 0,
                            'nb_gagnants': row['nb_gagnants'] or 0,
                            'is_optimal_tier': bool(row['is_optimal_tier']),
                        }
                    )
                except Wallet.DoesNotExist:
                    self.stdout.write(self.style.WARNING(
                        f"Skipping tier performance for missing wallet: {row['wallet_address']}"
                    ))
                    continue
            tier_count += 1

        self.stdout.write(self.style.SUCCESS(f"✓ Migrated {tier_count} tier performances"))

    @transaction.atomic
    def _migrate_wallet_qualified(self, cursor, dry_run):
        self.stdout.write("Migrating wallet qualified...")
        cursor.execute("SELECT * FROM wallet_qualified")
        qualified_count = 0

        for row in cursor.fetchall():
            if not dry_run:
                try:
                    wallet = Wallet.objects.get(address=row['wallet_address'])
                    WalletQualified.objects.update_or_create(
                        wallet=wallet,
                        defaults={
                            'final_score': row['final_score'] or 0,
                            'classification': row['classification'] or '',
                            'weighted_roi': row['weighted_roi'] or 0,
                            'taux_reussite': row['taux_reussite'] or 0,
                            'nb_trades': row['nb_trades'] or 0,
                        }
                    )
                except Wallet.DoesNotExist:
                    self.stdout.write(self.style.WARNING(
                        f"Skipping qualified wallet for missing wallet: {row['wallet_address']}"
                    ))
                    continue
            qualified_count += 1

        self.stdout.write(self.style.SUCCESS(f"✓ Migrated {qualified_count} qualified wallets"))

    @transaction.atomic
    def _migrate_smart_wallets(self, cursor, dry_run):
        self.stdout.write("Migrating smart wallets...")
        cursor.execute("SELECT * FROM smart_wallets")
        smart_count = 0

        for row in cursor.fetchall():
            if not dry_run:
                try:
                    wallet = Wallet.objects.get(address=row['wallet_address'])
                    wallet.is_smart_wallet = True
                    wallet.save()

                    SmartWallet.objects.update_or_create(
                        wallet=wallet,
                        defaults={
                            'optimal_threshold_tier': row['optimal_threshold_tier'] or 0,
                            'quality_score': row['quality_score'] or 0,
                            'threshold_status': row['threshold_status'] or '',
                            'optimal_roi': row['optimal_roi'] or 0,
                            'optimal_winrate': row['optimal_winrate'] or 0,
                            'global_roi': row['global_roi'] or 0,
                            'global_winrate': row['global_winrate'] or 0,
                        }
                    )
                except Wallet.DoesNotExist:
                    self.stdout.write(self.style.WARNING(
                        f"Skipping smart wallet for missing wallet: {row['wallet_address']}"
                    ))
                    continue
            smart_count += 1

        self.stdout.write(self.style.SUCCESS(f"✓ Migrated {smart_count} smart wallets"))

    @transaction.atomic
    def _migrate_wallet_brute(self, cursor, dry_run):
        self.stdout.write("Migrating wallet brute...")
        cursor.execute("SELECT * FROM wallet_brute")
        brute_count = 0

        for row in cursor.fetchall():
            if not dry_run:
                WalletBrute.objects.update_or_create(
                    wallet_address=row['wallet_address'],
                    token_address=row['token_address'],
                    temporality=row['temporality'],
                    defaults={
                        'contract_address': row['contract_address'] or '',
                        'chain': row['chain'] or '',
                    }
                )
            brute_count += 1

        self.stdout.write(self.style.SUCCESS(f"✓ Migrated {brute_count} wallet brute"))

    @transaction.atomic
    def _migrate_wallet_position_changes(self, cursor, dry_run):
        self.stdout.write("Migrating wallet position changes...")
        cursor.execute("SELECT * FROM wallet_position_changes")
        changes_count = 0

        for row in cursor.fetchall():
            if not dry_run:
                try:
                    wallet = Wallet.objects.get(address=row['wallet_address'])
                    WalletPositionChange.objects.update_or_create(
                        session_id=row['session_id'],
                        wallet=wallet,
                        symbol=row['symbol'],
                        fungible_id=row['fungible_id'] or '',
                        defaults={
                            'contract_address': row['contract_address'] or '',
                            'change_type': row['change_type'] or '',
                            'old_amount': row['old_amount'] or 0,
                            'new_amount': row['new_amount'] or 0,
                            'usd_change': row['usd_change'] or 0,
                        }
                    )
                except Wallet.DoesNotExist:
                    self.stdout.write(self.style.WARNING(
                        f"Skipping position change for missing wallet: {row['wallet_address']}"
                    ))
                    continue
            changes_count += 1

        self.stdout.write(self.style.SUCCESS(f"✓ Migrated {changes_count} wallet position changes"))
