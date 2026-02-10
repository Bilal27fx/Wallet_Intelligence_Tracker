#!/usr/bin/env python3
"""
RUNNER PRINCIPAL - SCORE ENGINE
Exécute séquentiellement les 4 modules d'analyse des smart wallets :
1. FIFO Analysis (calcul métriques par token)
2. Wallet Scoring (scoring et qualification)
3. Simple Wallet Analyzer (analyse par paliers)
4. Optimal Threshold (calcul seuils optimaux)
"""

import sys
from pathlib import Path
from datetime import datetime

# Ajouter le répertoire score_engine au path
SCORE_ENGINE_DIR = Path(__file__).parent
sys.path.insert(0, str(SCORE_ENGINE_DIR))

# Imports des modules
from fifo_clean_simple import run_fifo_analysis
from wallet_scoring_system import score_all_wallets, display_top_wallets, save_qualified_wallets, get_qualified_wallets_stats
from simple_wallet_analyzer import analyze_qualified_wallets
from optimal_threshold_analyzer import OptimalThresholdAnalyzer


def print_separator(title=""):
    """Affiche un séparateur visuel"""
    print(f"\n{'='*80}")
    if title:
        print(f"  {title}")
        print(f"{'='*80}")
    print()


def run_score_engine_pipeline(quality_filter: float = 0.0, show_stats: bool = True):
    """
    Exécute le pipeline complet d'analyse des smart wallets

    Args:
        quality_filter: Filtre qualité minimum pour optimal_threshold (0.0 = tous)
        show_stats: Afficher les statistiques détaillées
    """

    start_time = datetime.now()

    print_separator("🚀 DÉMARRAGE DU PIPELINE SCORE ENGINE")
    print(f"📅 Date: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Filtre qualité: {quality_filter}")
    print(f"📊 Statistiques: {'Activées' if show_stats else 'Désactivées'}")

    errors = []

    # === ÉTAPE 1: FIFO ANALYSIS ===
    try:
        print_separator("📊 ÉTAPE 1/4 - ANALYSE FIFO")
        print("🔍 Calcul des métriques token par wallet...")
        print("⏳ Cette étape peut prendre plusieurs minutes...\n")

        run_fifo_analysis()

        print("\n✅ FIFO Analysis terminée avec succès")

    except Exception as e:
        error_msg = f"❌ Erreur FIFO Analysis: {e}"
        print(error_msg)
        errors.append(("FIFO Analysis", str(e)))
        return False

    # === ÉTAPE 2: WALLET SCORING ===
    try:
        print_separator("⭐ ÉTAPE 2/4 - SCORING DES WALLETS")
        print("🎯 Calcul des scores et qualification des wallets...\n")

        # Scorer tous les wallets (min_score=20)
        scored_wallets = score_all_wallets(min_score=20)

        if not scored_wallets:
            print("⚠️ Aucun wallet qualifié trouvé")
            return False

        # Afficher le top 20
        display_top_wallets(scored_wallets, top_n=20)

        # Sauvegarder dans wallet_qualified
        save_qualified_wallets(scored_wallets)

        # Afficher les stats
        if show_stats:
            get_qualified_wallets_stats()

        print(f"\n✅ Scoring terminé - {len(scored_wallets)} wallets qualifiés")

    except Exception as e:
        error_msg = f"❌ Erreur Wallet Scoring: {e}"
        print(error_msg)
        errors.append(("Wallet Scoring", str(e)))
        return False

    # === ÉTAPE 3: SIMPLE WALLET ANALYZER ===
    try:
        print_separator("📈 ÉTAPE 3/4 - ANALYSE PAR PALIERS")
        print("🔍 Analyse détaillée par paliers d'investissement (3K-12K)...\n")

        analyze_qualified_wallets()

        print("\n✅ Analyse par paliers terminée")

    except Exception as e:
        error_msg = f"❌ Erreur Simple Wallet Analyzer: {e}"
        print(error_msg)
        errors.append(("Simple Wallet Analyzer", str(e)))
        return False

    # === ÉTAPE 4: OPTIMAL THRESHOLD ===
    try:
        print_separator("🎯 ÉTAPE 4/4 - CALCUL SEUILS OPTIMAUX")
        print(f"🔬 Analyse des seuils optimaux (qualité ≥ {quality_filter})...\n")

        analyzer = OptimalThresholdAnalyzer()
        results = analyzer.analyze_all_qualified_wallets(quality_filter=quality_filter)

        if show_stats:
            analyzer.get_smart_wallets_threshold_stats()

        # Résumé des résultats
        if quality_filter > 0:
            print(f"\n🎯 {len(results)} wallets exceptionnels (qualité ≥ {quality_filter})")
            if results:
                print("\n🏆 TOP 5 WALLETS EXCEPTIONNELS:")
                for i, result in enumerate(results[:5], 1):
                    threshold_str = f"{result['optimal_threshold']}K" if result['optimal_threshold'] else "N/A"
                    print(f"   {i}. {result['wallet_address'][:10]}... | Seuil: {threshold_str} | Qualité: {result['quality']:.3f}")
        else:
            print(f"\n🎯 {len(results)} wallets analysés au total")

        print("\n✅ Calcul des seuils optimaux terminé")

    except Exception as e:
        error_msg = f"❌ Erreur Optimal Threshold: {e}"
        print(error_msg)
        errors.append(("Optimal Threshold", str(e)))
        return False

    # === RÉSUMÉ FINAL ===
    end_time = datetime.now()
    duration = end_time - start_time

    print_separator("✅ PIPELINE TERMINÉ AVEC SUCCÈS")
    print(f"⏱️  Durée totale: {duration}")
    print(f"📊 Résultats disponibles dans: data/db/wit_database.db")
    print(f"📋 Tables créées/mises à jour:")
    print(f"   • token_analytics (métriques FIFO)")
    print(f"   • wallet_qualified (wallets qualifiés avec scores)")
    print(f"   • wallet_profiles (analyse par paliers)")
    print(f"   • smart_wallets (seuils optimaux et qualité)")

    if errors:
        print(f"\n⚠️  {len(errors)} erreur(s) détectée(s):")
        for step, error in errors:
            print(f"   • {step}: {error}")
        return False

    print(f"\n{'='*80}\n")
    return True


def main():
    """Point d'entrée principal"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Runner principal du Score Engine',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  python score_engine_runner.py                    # Pipeline complet
  python score_engine_runner.py --quality 0.9      # Avec filtre qualité
  python score_engine_runner.py --no-stats         # Sans statistiques
        """
    )

    parser.add_argument(
        '--quality',
        type=float,
        default=0.0,
        help='Filtre qualité minimum pour optimal_threshold (0.0-1.0, défaut: 0.0)'
    )

    parser.add_argument(
        '--no-stats',
        action='store_true',
        help='Désactiver l\'affichage des statistiques détaillées'
    )

    args = parser.parse_args()

    # Validation
    if args.quality < 0 or args.quality > 1:
        print("❌ Erreur: --quality doit être entre 0.0 et 1.0")
        sys.exit(1)

    # Exécution
    success = run_score_engine_pipeline(
        quality_filter=args.quality,
        show_stats=not args.no_stats
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
