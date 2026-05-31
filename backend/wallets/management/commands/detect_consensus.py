"""Detect consensus signals from smart wallets."""
from django.core.management.base import BaseCommand
from wallets.services import ConsensusDetectorService


class Command(BaseCommand):
    help = 'Detect consensus buy/sell signals from smart wallets'

    def add_arguments(self, parser):
        parser.add_argument(
            '--hours',
            type=int,
            default=24,
            help='Time window in hours (default: 24)'
        )
        parser.add_argument(
            '--type',
            type=str,
            choices=['buy', 'sell', 'all'],
            default='all',
            help='Signal type to detect (default: all)'
        )
        parser.add_argument(
            '--show-active',
            action='store_true',
            help='Show active signals only'
        )

    def handle(self, *args, **options):
        detector = ConsensusDetectorService()

        hours = options.get('hours', 24)
        signal_type = options.get('type', 'all')
        show_active = options.get('show_active', False)

        if show_active:
            self.stdout.write(f"Fetching active signals from last {hours} hours...")
            signals = detector.get_active_signals(hours)

            if signals:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"\n✓ Found {len(signals)} active signals"
                    )
                )

                for signal in signals:
                    signal_icon = "🟢" if signal.signal_type == 'BUY' else "🔴"
                    self.stdout.write(
                        f"\n{signal_icon} {signal.symbol} ({signal.token_address[:10]}...)\n"
                        f"  Type: {signal.signal_type}\n"
                        f"  Wallets: {signal.wallet_count}\n"
                        f"  Confidence: {signal.confidence_score:.2%}\n"
                        f"  Detected: {signal.detected_at}"
                    )
            else:
                self.stdout.write(
                    self.style.WARNING("No active signals found")
                )

        else:
            self.stdout.write(
                f"Detecting consensus signals (last {hours} hours)..."
            )

            if signal_type in ['buy', 'all']:
                self.stdout.write("\nDetecting BUY signals...")
                buy_signals = detector.detect_consensus_buys(hours)

                if buy_signals:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"✓ Found {len(buy_signals)} BUY signals"
                        )
                    )

                    for signal in buy_signals:
                        self.stdout.write(
                            f"\n🟢 {signal['symbol']} ({signal['token_address'][:10]}...)\n"
                            f"  Wallets: {signal['wallet_count']}\n"
                            f"  Confidence: {signal['confidence_score']:.2%}\n"
                            f"  Smart wallets: {', '.join(w[:10] + '...' for w in signal['smart_wallets'][:3])}"
                        )
                        if len(signal['smart_wallets']) > 3:
                            self.stdout.write(
                                f"  ... and {len(signal['smart_wallets']) - 3} more"
                            )
                else:
                    self.stdout.write("  No BUY signals detected")

            if signal_type in ['sell', 'all']:
                self.stdout.write("\nDetecting SELL signals...")
                sell_signals = detector.detect_consensus_sells(hours)

                if sell_signals:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"✓ Found {len(sell_signals)} SELL signals"
                        )
                    )

                    for signal in sell_signals:
                        self.stdout.write(
                            f"\n🔴 {signal['symbol']} ({signal['token_address'][:10]}...)\n"
                            f"  Wallets: {signal['wallet_count']}\n"
                            f"  Confidence: {signal['confidence_score']:.2%}\n"
                            f"  Smart wallets: {', '.join(w[:10] + '...' for w in signal['smart_wallets'][:3])}"
                        )
                        if len(signal['smart_wallets']) > 3:
                            self.stdout.write(
                                f"  ... and {len(signal['smart_wallets']) - 3} more"
                            )
                else:
                    self.stdout.write("  No SELL signals detected")

            if signal_type == 'all':
                result = detector.run_consensus_detection()
                self.stdout.write(
                    self.style.SUCCESS(
                        f"\n{'='*50}\n"
                        f"Total signals detected: {result['total_signals']}\n"
                        f"  BUY: {result['buy_signals']}\n"
                        f"  SELL: {result['sell_signals']}"
                    )
                )
