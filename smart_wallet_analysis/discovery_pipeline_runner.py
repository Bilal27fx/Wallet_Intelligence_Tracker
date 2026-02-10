#!/usr/bin/env python3
"""
DISCOVERY PIPELINE RUNNER - CONSTRUCTION DE LA BASE DE DONNÉES
Exécute séquentiellement les 3 modules de découverte et construction:

1. Token Discovery Manual    - Découverte des tokens explosifs via Dune Analytics
2. Wallet Tracker            - Extraction wallets, balances et historiques
3. Score Engine              - Analyse FIFO, scoring et classification complète

Objectif: Faire GROSSIR la base de données avec de nouveaux wallets et les analyser
Base de données: wit_database.db
"""

import sys
import time
from pathlib import Path
from datetime import datetime
import argparse

# Configuration des paths
ROOT_DIR = Path(__file__).parent.parent.parent
SMART_WALLET_DIR = Path(__file__).parent

# Ajouter les modules au path
sys.path.insert(0, str(SMART_WALLET_DIR / "token_discovery_manual"))
sys.path.insert(0, str(SMART_WALLET_DIR / "wallet_tracker"))
sys.path.insert(0, str(SMART_WALLET_DIR / "score_engine"))

# Imports des runners
from token_discovery_manual.dune_api_loop_manual import run_manual_token_discovery
from wallet_tracker.wallet_tracker_runner import main as run_wallet_tracker
from score_engine.score_engine_runner import run_score_engine_pipeline


class PipelineStats:
    """Statistiques d'exécution du pipeline"""
    def __init__(self):
        self.start_time = None
        self.steps = {}

    def start(self):
        self.start_time = datetime.now()

    def record_step(self, step_name, duration, success, error=None):
        self.steps[step_name] = {
            'duration': duration,
            'success': success,
            'error': error
        }

    def print_summary(self):
        """Affiche un résumé de l'exécution"""
        total_duration = (datetime.now() - self.start_time).total_seconds()

        print("\n" + "="*80)
        print("📊 RÉSUMÉ DU PIPELINE")
        print("="*80)
        print(f"⏱️  Durée totale: {self._format_duration(total_duration)}")
        print(f"📅 Démarré: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🏁 Terminé: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        success_count = sum(1 for s in self.steps.values() if s['success'])
        total_steps = len(self.steps)

        print(f"✅ Étapes réussies: {success_count}/{total_steps}")
        print()

        for i, (step_name, stats) in enumerate(self.steps.items(), 1):
            status = "✅" if stats['success'] else "❌"
            duration = self._format_duration(stats['duration'])
            print(f"{status} [{i}/4] {step_name:<30} {duration}")
            if stats['error']:
                print(f"     └─ Erreur: {stats['error']}")

        print("="*80 + "\n")

        return success_count == total_steps

    @staticmethod
    def _format_duration(seconds):
        """Formate une durée en secondes"""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}min"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}h"


def print_separator(title="", subtitle=""):
    """Affiche un séparateur visuel"""
    print("\n" + "="*80)
    if title:
        print(f"  {title}")
        if subtitle:
            print(f"  {subtitle}")
        print("="*80)
    print()


def run_discovery_pipeline(
    skip_token_discovery=False,
    skip_wallet_tracker=False,
    skip_score_engine=False,
    quality_filter=0.0
):
    """
    Exécute le pipeline de découverte et construction

    Args:
        skip_token_discovery (bool): Skip l'étape 1 (token discovery)
        skip_wallet_tracker (bool): Skip l'étape 2 (wallet tracker)
        skip_score_engine (bool): Skip l'étape 3 (score engine)
        quality_filter (float): Filtre qualité pour optimal_threshold (0.0-1.0)

    Returns:
        bool: True si succès complet, False sinon
    """

    stats = PipelineStats()
    stats.start()

    print_separator(
        "🚀 DISCOVERY PIPELINE - CONSTRUCTION DE LA BASE",
        f"Démarré le {stats.start_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    print("📋 OBJECTIF: Découvrir de nouveaux tokens et wallets, puis les analyser")
    print()
    print("📊 CONFIGURATION:")
    print(f"   • Token Discovery:  {'⏩ SKIP' if skip_token_discovery else '✅ Activé'}")
    print(f"   • Wallet Tracker:   {'⏩ SKIP' if skip_wallet_tracker else '✅ Activé'}")
    print(f"   • Score Engine:     {'⏩ SKIP' if skip_score_engine else '✅ Activé'} (qualité ≥ {quality_filter})")
    print()

    errors = []

    # === ÉTAPE 1: TOKEN DISCOVERY ===
    if not skip_token_discovery:
        try:
            print_separator("🎯 ÉTAPE 1/3 - TOKEN DISCOVERY MANUAL", "Découverte des tokens explosifs via Dune Analytics")

            step_start = time.time()
            run_manual_token_discovery()
            step_duration = time.time() - step_start

            stats.record_step("Token Discovery", step_duration, True)
            print(f"\n✅ Token Discovery terminé ({stats._format_duration(step_duration)})")

        except Exception as e:
            step_duration = time.time() - step_start
            stats.record_step("Token Discovery", step_duration, False, str(e))
            print(f"\n❌ Erreur Token Discovery: {e}")
            print("⚠️  Le pipeline continue avec les données existantes...")
    else:
        print("⏩ [1/3] Token Discovery - SKIP\n")

    # === ÉTAPE 2: WALLET TRACKER ===
    if not skip_wallet_tracker:
        try:
            print_separator("💼 ÉTAPE 2/3 - WALLET TRACKER", "Extraction des wallets, balances et historiques")

            step_start = time.time()
            success = run_wallet_tracker()
            step_duration = time.time() - step_start

            if success:
                stats.record_step("Wallet Tracker", step_duration, True)
                print(f"\n✅ Wallet Tracker terminé ({stats._format_duration(step_duration)})")
            else:
                raise Exception("Wallet Tracker a retourné False")

        except Exception as e:
            step_duration = time.time() - step_start
            stats.record_step("Wallet Tracker", step_duration, False, str(e))
            print(f"\n❌ Erreur Wallet Tracker: {e}")
            print("⚠️  Impossible de continuer sans données wallet")
            stats.print_summary()
            return False
    else:
        print("⏩ [2/3] Wallet Tracker - SKIP\n")

    # === ÉTAPE 3: SCORE ENGINE ===
    if not skip_score_engine:
        try:
            print_separator("⭐ ÉTAPE 3/3 - SCORE ENGINE", "Analyse FIFO, scoring et classification des wallets")

            step_start = time.time()
            success = run_score_engine_pipeline(
                quality_filter=quality_filter,
                show_stats=True
            )
            step_duration = time.time() - step_start

            if not success:
                raise Exception("Score Engine pipeline a retourné False")

            stats.record_step("Score Engine", step_duration, True)
            print(f"\n✅ Score Engine terminé ({stats._format_duration(step_duration)})")

        except Exception as e:
            step_duration = time.time() - step_start
            stats.record_step("Score Engine", step_duration, False, str(e))
            print(f"\n❌ Erreur Score Engine: {e}")
            print("⚠️  Le pipeline continue sans analyse avancée...")
    else:
        print("⏩ [3/3] Score Engine - SKIP\n")

    # === RÉSUMÉ FINAL ===
    success = stats.print_summary()

    if success:
        print("🎉 DISCOVERY PIPELINE TERMINÉ AVEC SUCCÈS!")
        print()
        print("📊 NOUVELLES DONNÉES AJOUTÉES DANS: data/db/wit_database.db")
        print("   • wallet_brute          (nouveaux wallets découverts)")
        print("   • wallets               (profils enrichis)")
        print("   • tokens                (positions détaillées)")
        print("   • transaction_history   (historiques complets)")
        print("   • token_analytics       (métriques FIFO)")
        print("   • wallet_qualified      (wallets qualifiés)")
        print("   • wallet_profiles       (analyse par paliers)")
        print("   • smart_wallets         (seuils optimaux)")
        print()
        print("➡️  Prochaine étape: Lancer le Smart Wallets Update Pipeline")
        print("   python smart_wallet_analysis/run_smartwallets_pipeline.py")
        print()
    else:
        print("⚠️  PIPELINE TERMINÉ AVEC DES ERREURS")
        print("     Consultez les logs ci-dessus pour plus de détails")
        print()

    return success


def main():
    """Point d'entrée principal"""

    parser = argparse.ArgumentParser(
        description='🚀 Discovery Pipeline - Construction de la base de données',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:

  # Pipeline complet (toutes les étapes)
  python discovery_pipeline_runner.py

  # Skip certaines étapes (utiliser données existantes)
  python discovery_pipeline_runner.py --skip-token-discovery
  python discovery_pipeline_runner.py --skip-wallet-tracker --skip-token-discovery

  # Exécuter seulement certaines étapes
  python discovery_pipeline_runner.py --only-token-discovery
  python discovery_pipeline_runner.py --only-score-engine --quality 0.9

  # Options avancées
  python discovery_pipeline_runner.py --quality 0.9
        """
    )

    # Options de skip
    skip_group = parser.add_argument_group('Options de skip (étapes à ignorer)')
    skip_group.add_argument('--skip-token-discovery', action='store_true',
                           help='Skip Token Discovery (utilise données existantes)')
    skip_group.add_argument('--skip-wallet-tracker', action='store_true',
                           help='Skip Wallet Tracker (utilise données existantes)')
    skip_group.add_argument('--skip-score-engine', action='store_true',
                           help='Skip Score Engine (utilise données existantes)')

    # Options "only" (exécuter seulement une étape)
    only_group = parser.add_argument_group('Options "only" (exécuter une seule étape)')
    only_exclusive = only_group.add_mutually_exclusive_group()
    only_exclusive.add_argument('--only-token-discovery', action='store_true',
                                help='Exécuter seulement Token Discovery')
    only_exclusive.add_argument('--only-wallet-tracker', action='store_true',
                                help='Exécuter seulement Wallet Tracker')
    only_exclusive.add_argument('--only-score-engine', action='store_true',
                                help='Exécuter seulement Score Engine')

    # Configuration Score Engine
    score_group = parser.add_argument_group('Configuration Score Engine')
    score_group.add_argument('--quality', type=float, default=0.0,
                            help='Filtre qualité minimum (0.0-1.0, défaut: 0.0)')

    args = parser.parse_args()

    # Validation
    if args.quality < 0 or args.quality > 1:
        print("❌ Erreur: --quality doit être entre 0.0 et 1.0")
        sys.exit(1)

    # Gérer les options "only"
    if args.only_token_discovery:
        args.skip_wallet_tracker = True
        args.skip_score_engine = True
    elif args.only_wallet_tracker:
        args.skip_token_discovery = True
        args.skip_score_engine = True
    elif args.only_score_engine:
        args.skip_token_discovery = True
        args.skip_wallet_tracker = True

    # Exécution
    try:
        success = run_discovery_pipeline(
            skip_token_discovery=args.skip_token_discovery,
            skip_wallet_tracker=args.skip_wallet_tracker,
            skip_score_engine=args.skip_score_engine,
            quality_filter=args.quality
        )

        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        print("\n⚠️  Pipeline interrompu par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Erreur fatale: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
