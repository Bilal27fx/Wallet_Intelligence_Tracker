#!/usr/bin/env python3
"""
Runner orchestrateur: Pipeline complet d'analyse des Smart Wallets
Exécute séquentiellement:
1. Tracking live des smart wallets
2. Analyse FIFO des smart wallets
3. Scoring des wallets (avec filtre 150k si ROI < 50%)
4. Analyse simple par paliers (3K-12K)
5. Analyse des seuils optimaux
"""

import sys
import time
from pathlib import Path
from datetime import datetime

# Configuration des paths
ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT / "module" / "smart_wallet_analysis" / "tracking_live"))
sys.path.insert(0, str(ROOT / "module" / "smart_wallet_analysis" / "score_engine"))

# Imports
from tracking_live.run import run_complete_live_tracking
from score_engine.fifo_clean_simple import run_smart_wallets_fifo
from score_engine.wallet_scoring_system import score_all_wallets, save_qualified_wallets
from score_engine.simple_wallet_analyzer import analyze_qualified_wallets
from score_engine.optimal_threshold_analyzer import OptimalThresholdAnalyzer


def run_tracking_and_fifo_pipeline():
    """Pipeline complet: Tracking → FIFO → Scoring → Analyses"""

    print("\n" + "=" * 80)
    print("🎯 PIPELINE COMPLET D'ANALYSE SMART WALLETS")
    print("=" * 80)
    print(f"⏰ Démarrage: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80 + "\n")

    start_time = time.time()
    step_times = {}

    # ==========================================
    # ÉTAPE 1: TRACKING LIVE
    # ==========================================
    print("=" * 80)
    print("📡 ÉTAPE 1/5: TRACKING LIVE DES SMART WALLETS")
    print("=" * 80 + "\n")

    step1_start = time.time()
    try:
        run_complete_live_tracking(
            enable_transaction_tracking=True,
            min_usd=500,
            hours_lookback=24
        )
        step_times['tracking'] = time.time() - step1_start
        print(f"\n✅ Étape 1 terminée en {step_times['tracking']:.2f}s ({step_times['tracking']/60:.1f} min)")
    except Exception as e:
        print(f"\n❌ Erreur lors du tracking live: {e}")
        return False

    print("\n⏸️ Pause de 5 secondes...\n")
    time.sleep(5)

    # ==========================================
    # ÉTAPE 2: FIFO SMART WALLETS
    # ==========================================
    print("=" * 80)
    print("🧮 ÉTAPE 2/5: ANALYSE FIFO SMART WALLETS")
    print("=" * 80 + "\n")

    step2_start = time.time()
    try:
        result = run_smart_wallets_fifo()
        step_times['fifo'] = time.time() - step2_start

        if result:
            print(f"\n✅ Étape 2 terminée en {step_times['fifo']:.2f}s ({step_times['fifo']/60:.1f} min)")
        else:
            print(f"\n⚠️ Étape 2 terminée avec des avertissements")
    except Exception as e:
        print(f"\n❌ Erreur lors de l'analyse FIFO: {e}")
        return False

    print("\n⏸️ Pause de 3 secondes...\n")
    time.sleep(3)

    # ==========================================
    # ÉTAPE 3: SCORING DES WALLETS
    # ==========================================
    print("=" * 80)
    print("⭐ ÉTAPE 3/5: SCORING DES WALLETS")
    print("=" * 80 + "\n")

    step3_start = time.time()
    try:
        scored_wallets = score_all_wallets(min_score=20)

        if scored_wallets:
            save_qualified_wallets(scored_wallets)
            step_times['scoring'] = time.time() - step3_start
            print(f"\n✅ Étape 3 terminée en {step_times['scoring']:.2f}s")
            print(f"   {len(scored_wallets)} wallets qualifiés")
        else:
            print(f"\n⚠️ Aucun wallet qualifié")
            step_times['scoring'] = time.time() - step3_start
    except Exception as e:
        print(f"\n❌ Erreur lors du scoring: {e}")
        return False

    print("\n⏸️ Pause de 3 secondes...\n")
    time.sleep(3)

    # ==========================================
    # ÉTAPE 4: ANALYSE SIMPLE PAR PALIERS
    # ==========================================
    print("=" * 80)
    print("📊 ÉTAPE 4/5: ANALYSE PAR PALIERS (3K-12K)")
    print("=" * 80 + "\n")

    step4_start = time.time()
    try:
        analyze_qualified_wallets()
        step_times['paliers'] = time.time() - step4_start
        print(f"\n✅ Étape 4 terminée en {step_times['paliers']:.2f}s ({step_times['paliers']/60:.1f} min)")
    except Exception as e:
        print(f"\n❌ Erreur lors de l'analyse par paliers: {e}")
        return False

    print("\n⏸️ Pause de 3 secondes...\n")
    time.sleep(3)

    # ==========================================
    # ÉTAPE 5: ANALYSE SEUILS OPTIMAUX
    # ==========================================
    print("=" * 80)
    print("🎯 ÉTAPE 5/5: ANALYSE DES SEUILS OPTIMAUX")
    print("=" * 80 + "\n")

    step5_start = time.time()
    try:
        analyzer = OptimalThresholdAnalyzer()
        results = analyzer.analyze_all_qualified_wallets(quality_filter=0.0)
        step_times['seuils'] = time.time() - step5_start
        print(f"\n✅ Étape 5 terminée en {step_times['seuils']:.2f}s ({step_times['seuils']/60:.1f} min)")
    except Exception as e:
        print(f"\n❌ Erreur lors de l'analyse des seuils: {e}")
        return False

    # ==========================================
    # RÉSUMÉ FINAL
    # ==========================================
    total_duration = time.time() - start_time

    print("\n" + "=" * 80)
    print("🏆 PIPELINE COMPLET TERMINÉ")
    print("=" * 80)
    print(f"⏱️ Durée totale: {total_duration:.2f}s ({total_duration/60:.1f} min)")
    print(f"\n📋 Détail des étapes:")
    print(f"   • Étape 1 (Tracking):  {step_times['tracking']:.2f}s ({step_times['tracking']/60:.1f} min)")
    print(f"   • Étape 2 (FIFO):      {step_times['fifo']:.2f}s ({step_times['fifo']/60:.1f} min)")
    print(f"   • Étape 3 (Scoring):   {step_times['scoring']:.2f}s")
    print(f"   • Étape 4 (Paliers):   {step_times['paliers']:.2f}s ({step_times['paliers']/60:.1f} min)")
    print(f"   • Étape 5 (Seuils):    {step_times['seuils']:.2f}s ({step_times['seuils']/60:.1f} min)")
    print(f"\n⏰ Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80 + "\n")

    return True


if __name__ == "__main__":
    try:
        success = run_tracking_and_fifo_pipeline()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️ Pipeline interrompu par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Erreur fatale: {e}")
        sys.exit(1)
