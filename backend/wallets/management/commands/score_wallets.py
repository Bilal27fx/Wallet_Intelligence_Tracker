"""Score and qualify wallets."""
from django.core.management.base import BaseCommand
from wallets.services import WalletScorerService, TierAnalyzerService


class Command(BaseCommand):
    help = 'Score wallets and identify smart wallets'

    def add_arguments(self, parser):
        parser.add_argument(
            '--wallet',
            type=str,
            help='Specific wallet address to score'
        )
        parser.add_argument(
            '--score-all',
            action='store_true',
            help='Score all wallets'
        )
        parser.add_argument(
            '--analyze-tiers',
            action='store_true',
            help='Analyze tier performance'
        )
        parser.add_argument(
            '--identify-smart',
            action='store_true',
            help='Identify smart wallets'
        )
        parser.add_argument(
            '--top',
            type=int,
            default=10,
            help='Show top N wallets (default: 10)'
        )

    def handle(self, *args, **options):
        scorer = WalletScorerService()
        tier_analyzer = TierAnalyzerService()

        wallet_address = options.get('wallet')
        score_all = options.get('score_all', False)
        analyze_tiers = options.get('analyze_tiers', False)
        identify_smart = options.get('identify_smart', False)
        top_n = options.get('top', 10)

        if score_all:
            self.stdout.write("Scoring all wallets...")
            result = scorer.score_all_wallets()

            self.stdout.write(
                self.style.SUCCESS(
                    f"\n✓ Scored {result['total_wallets']} wallets"
                )
            )
            self.stdout.write(
                f"  Qualified: {result['qualified']}\n"
                f"  ELITE: {result['elite']}\n"
                f"  EXCELLENT: {result['excellent']}\n"
                f"  BON: {result['bon']}\n"
                f"  MOYEN: {result['moyen']}\n"
                f"  FAIBLE: {result['faible']}"
            )

            top_wallets = scorer.get_top_wallets(top_n)
            self.stdout.write(f"\nTop {top_n} wallets:")
            for i, wq in enumerate(top_wallets, 1):
                self.stdout.write(
                    f"  {i}. {wq.wallet.address[:10]}... "
                    f"Score: {wq.final_score:.2f} "
                    f"({wq.classification}) "
                    f"ROI: {wq.weighted_roi:.2f}% "
                    f"Winrate: {wq.taux_reussite:.2f}%"
                )

        elif wallet_address:
            self.stdout.write(f"Scoring wallet {wallet_address}...")

            score_data = scorer.score_wallet(wallet_address)

            if 'error' in score_data:
                self.stdout.write(
                    self.style.ERROR(f"✗ {score_data['error']}")
                )
                return

            self.stdout.write(
                self.style.SUCCESS(
                    f"\n✓ Wallet: {wallet_address}\n"
                    f"  Score: {score_data['final_score']:.2f}\n"
                    f"  Classification: {score_data['classification']}\n"
                    f"  Weighted ROI: {score_data['weighted_roi']:.2f}%\n"
                    f"  Winrate: {score_data['winrate']:.2f}%\n"
                    f"  Trades: {score_data['nb_trades']}"
                )
            )

            is_qualified = scorer.qualify_wallet(wallet_address)
            if is_qualified:
                self.stdout.write(
                    self.style.SUCCESS("  ✓ QUALIFIED")
                )
            else:
                self.stdout.write(
                    self.style.WARNING("  ✗ Not qualified")
                )

            if analyze_tiers:
                self.stdout.write("\nAnalyzing tier performance...")
                tiers = tier_analyzer.analyze_wallet_tiers(wallet_address)

                for tier in tiers:
                    optimal = "⭐ OPTIMAL" if tier['is_optimal_tier'] else ""
                    self.stdout.write(
                        f"  Tier ${tier['tier_usd']:,}: "
                        f"ROI {tier['roi_percentage']:.2f}% "
                        f"Winrate {tier['winrate']:.2f}% "
                        f"({tier['nb_trades']} trades) {optimal}"
                    )

            if identify_smart:
                self.stdout.write("\nIdentifying smart wallet...")
                smart_data = tier_analyzer.identify_smart_wallet(wallet_address)

                if smart_data['is_smart']:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"  ✓ SMART WALLET\n"
                            f"  Optimal Tier: ${smart_data['optimal_tier']:,}\n"
                            f"  Quality Score: {smart_data['quality_score']:.2f}\n"
                            f"  Status: {smart_data['threshold_status']}\n"
                            f"  Optimal ROI: {smart_data['optimal_roi']:.2f}%\n"
                            f"  Optimal Winrate: {smart_data['optimal_winrate']:.2f}%"
                        )
                    )
                else:
                    self.stdout.write(
                        self.style.WARNING(
                            f"  ✗ Not a smart wallet: {smart_data.get('reason', 'Unknown')}"
                        )
                    )

        elif analyze_tiers:
            self.stdout.write("Analyzing tiers for all wallets...")
            result = tier_analyzer.analyze_all_wallets()

            self.stdout.write(
                self.style.SUCCESS(
                    f"\n✓ Analyzed {result['total_wallets']} wallets\n"
                    f"  Smart wallets identified: {result['smart_wallets']}\n"
                    f"  Tier performances calculated: {result['tier_performances_calculated']}"
                )
            )

            smart_wallets = tier_analyzer.get_smart_wallets()
            self.stdout.write(f"\nTop {min(top_n, len(smart_wallets))} smart wallets:")
            for i, sw in enumerate(smart_wallets[:top_n], 1):
                self.stdout.write(
                    f"  {i}. {sw.wallet.address[:10]}... "
                    f"Tier: ${sw.optimal_threshold_tier:,} "
                    f"Quality: {sw.quality_score:.2f} "
                    f"({sw.threshold_status})"
                )

        else:
            self.stdout.write(
                self.style.ERROR(
                    "Please provide --wallet, --score-all, or --analyze-tiers"
                )
            )
