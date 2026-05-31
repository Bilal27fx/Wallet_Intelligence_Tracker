"""Calculate FIFO analytics for wallets."""
from django.core.management.base import BaseCommand
from wallets.services import FIFOCalculatorService


class Command(BaseCommand):
    help = 'Calculate FIFO analytics for wallet tokens'

    def add_arguments(self, parser):
        parser.add_argument(
            '--wallet',
            type=str,
            help='Specific wallet address to calculate'
        )
        parser.add_argument(
            '--symbol',
            type=str,
            help='Specific token symbol to calculate'
        )
        parser.add_argument(
            '--recalculate-all',
            action='store_true',
            help='Recalculate analytics for all wallets'
        )

    def handle(self, *args, **options):
        calculator = FIFOCalculatorService()

        wallet_address = options.get('wallet')
        symbol = options.get('symbol')
        recalculate_all = options.get('recalculate_all', False)

        if recalculate_all:
            self.stdout.write("Recalculating analytics for all wallets...")
            result = calculator.recalculate_all_analytics()
            self.stdout.write(
                self.style.SUCCESS(
                    f"✓ Processed {result['wallets_processed']} wallets, "
                    f"{result['tokens_calculated']} tokens calculated"
                )
            )

        elif wallet_address:
            self.stdout.write(f"Calculating analytics for {wallet_address}...")

            results = calculator.calculate_token_analytics(wallet_address, symbol)

            if results:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"✓ Calculated {len(results)} tokens"
                    )
                )

                for result in results[:5]:
                    status_icon = "📈" if result['is_winning'] else "📉"
                    self.stdout.write(
                        f"  {status_icon} {result['token_symbol']}: "
                        f"ROI {result['roi_percentage']:.2f}% "
                        f"(Invested: ${result['total_invested']:.2f})"
                    )

                if len(results) > 5:
                    self.stdout.write(f"  ... and {len(results) - 5} more")

                winning = calculator.get_winning_tokens(wallet_address)
                losing = calculator.get_losing_tokens(wallet_address)

                self.stdout.write(
                    f"\n  Winning: {len(winning)} | Losing: {len(losing)}"
                )
            else:
                self.stdout.write(
                    self.style.WARNING("No analytics data calculated")
                )

        else:
            self.stdout.write(
                self.style.ERROR(
                    "Please provide --wallet or use --recalculate-all"
                )
            )
