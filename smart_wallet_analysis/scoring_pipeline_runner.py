#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline 2: Re-scoring Pipeline
Exécuté quotidiennement pour mettre à jour les smart wallets

Workflow:
1. Récupère tous les wallets de transaction_history (déjà filtrés)
2. Met à jour leurs transactions via tracking_live (incrémental, optimisé API)
3. Re-score tous les wallets via score_engine
4. Génère la nouvelle liste de smart wallets
"""

import sys
import time
import sqlite3
from pathlib import Path
from datetime import datetime

# Ajouter le répertoire parent au path
ROOT = Path(__file__).parent.parent
sys.path.append(str(ROOT))

# Imports des modules
from smart_wallet_analysis.tracking_live.run import run_rescoring_transaction_update
from smart_wallet_analysis.score_engine.fifo_clean_simple import SimpleFIFOAnalyzer
from smart_wallet_analysis.score_engine.wallet_scoring_system import score_all_wallets
from smart_wallet_analysis.score_engine.simple_wallet_analyzer import analyze_qualified_wallets
from smart_wallet_analysis.score_engine.optimal_threshold_analyzer import OptimalThresholdAnalyzer
from smart_wallet_analysis.consensus_live.consensus_live_detector import run_live_consensus_detection
from smart_wallet_analysis.wallet_tracker.wallet_token_history_simple import extract_wallet_simple_history

DB_PATH = ROOT / "data" / "db" / "wit_database.db"


def get_wallets_to_rescore():
    """
    Récupère tous les wallets de transaction_history
    Ces wallets ont déjà été filtrés par le Discovery Pipeline:
    - MIN_TOKENS_PER_WALLET = 3
    - MIN_TOKEN_VOLUME_THRESHOLD = $500 par token
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT DISTINCT wallet_address
            FROM transaction_history
            ORDER BY wallet_address
        """)

        wallets = [row[0] for row in cursor.fetchall()]
        conn.close()

        print(f"📊 {len(wallets)} wallets dans transaction_history")
        return wallets

    except Exception as e:
        print(f"❌ Erreur récupération wallets: {e}")
        return []


def update_transaction_histories(wallets_list):
    """
    Étape 1: Mise à jour incrémentale des transactions
    Utilise tracking_live (optimisé) au lieu de wallet_token_history (lourd)

    Avantages:
    - Détecte seulement les changements récents (24h)
    - Met à jour uniquement les tokens modifiés
    - Économise le quota API Zerion
    """
    print("\n" + "="*70)
    print("📊 ÉTAPE 1: MISE À JOUR DES TRANSACTIONS")
    print("="*70 + "\n")

    if not wallets_list:
        print("⚠️ Aucun wallet à mettre à jour")
        return 0

    # Utilise tracking_live en mode re-scoring (sans filtre smart_wallets)
    changes_count = run_rescoring_transaction_update(
        wallet_list=wallets_list,
        min_usd=500,
        hours_lookback=24
    )

    print(f"\n✅ Mise à jour terminée: {changes_count} wallets avec changements\n")
    return changes_count


def run_fifo_analysis_full():
    """
    Étape 2: Analyse FIFO complète
    Traite TOUS les wallets (pas juste les nouveaux)
    """
    print("\n" + "="*70)
    print("📊 ÉTAPE 2: ANALYSE FIFO (TOUS LES WALLETS)")
    print("="*70 + "\n")

    try:
        # Vider token_analytics pour forcer recalcul complet
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(DISTINCT wallet_address) FROM transaction_history")
        total_wallets = cursor.fetchone()[0]
        print(f"📊 {total_wallets} wallets à analyser")

        print("🗑️ Suppression de l'ancienne analyse FIFO...")
        cursor.execute("DELETE FROM token_analytics")
        conn.commit()
        conn.close()

        # Lancer l'analyse FIFO (qui va maintenant tout recalculer)
        print("🔄 Lancement de l'analyse FIFO...\n")
        analyzer = SimpleFIFOAnalyzer()
        analyzer.analyze_all_wallets()

        print("\n✅ Analyse FIFO terminée\n")
        return True

    except Exception as e:
        print(f"❌ Erreur FIFO analysis: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_wallet_scoring_full():
    """
    Étape 3: Scoring des wallets
    """
    print("\n" + "="*70)
    print("📊 ÉTAPE 3: SCORING DES WALLETS")
    print("="*70 + "\n")

    try:
        score_all_wallets(min_score=0)

        print("\n✅ Scoring terminé\n")
        return True

    except Exception as e:
        print(f"❌ Erreur wallet scoring: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_simple_analysis():
    """
    Étape 4: Analyse simple par tiers
    """
    print("\n" + "="*70)
    print("📊 ÉTAPE 4: ANALYSE PAR TIERS D'INVESTISSEMENT")
    print("="*70 + "\n")

    try:
        analyze_qualified_wallets()

        print("\n✅ Analyse par tiers terminée\n")
        return True

    except Exception as e:
        print(f"❌ Erreur simple analysis: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_optimal_threshold():
    """
    Étape 5: Calcul des seuils optimaux et sélection smart wallets
    """
    print("\n" + "="*70)
    print("📊 ÉTAPE 5: SÉLECTION DES SMART WALLETS")
    print("="*70 + "\n")

    try:
        optimizer = OptimalThresholdAnalyzer()
        optimizer.analyze_all_qualified_wallets()

        print("\n✅ Sélection smart wallets terminée\n")
        return True

    except Exception as e:
        print(f"❌ Erreur optimal threshold: {e}")
        import traceback
        traceback.print_exc()
        return False


def get_final_stats():
    """Affiche les statistiques finales"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Smart wallets
        cursor.execute("SELECT COUNT(*) FROM smart_wallets")
        smart_count = cursor.fetchone()[0]

        # Wallets qualifiés
        cursor.execute("SELECT COUNT(*) FROM wallet_qualified")
        qualified_count = cursor.fetchone()[0]

        # Total wallets analysés
        cursor.execute("SELECT COUNT(DISTINCT wallet_address) FROM token_analytics")
        analyzed_count = cursor.fetchone()[0]

        conn.close()

        return {
            'smart_wallets': smart_count,
            'qualified_wallets': qualified_count,
            'analyzed_wallets': analyzed_count
        }

    except Exception as e:
        print(f"❌ Erreur récupération stats: {e}")
        return {}


def run_analysis_and_selection_only():
    """
    Lance uniquement les étapes 4 et 5 (analyse + sélection smart wallets)
    Utile après avoir déjà fait FIFO + Scoring
    """
    start_time = time.time()

    print("\n" + "="*80)
    print("🎯 ÉTAPES 4-5: ANALYSE & SÉLECTION")
    print("="*80 + "\n")

    # Étape 4: Analyse simple
    if not run_simple_analysis():
        print("❌ Erreur lors de l'analyse simple")
        return False

    # Étape 5: Optimal threshold
    if not run_optimal_threshold():
        print("❌ Erreur lors de la sélection smart wallets")
        return False

    # Stats finales
    elapsed = time.time() - start_time
    stats = get_final_stats()

    print("\n" + "="*80)
    print("✅ ANALYSE & SÉLECTION TERMINÉES")
    print("="*80)
    print(f"⏱️ Durée: {elapsed:.1f} secondes")
    print(f"📊 Wallets analysés: {stats.get('analyzed_wallets', 0)}")
    print(f"🎯 Wallets qualifiés: {stats.get('qualified_wallets', 0)}")
    print(f"⭐ Smart wallets: {stats.get('smart_wallets', 0)}")
    print("="*80 + "\n")

    return True


def run_complete_scoring_pipeline():
    """
    Pipeline complet de re-scoring quotidien

    Workflow:
    1. Récupération liste wallets (transaction_history)
    2. Mise à jour transactions (tracking_live optimisé)
    3. Analyse FIFO (tous les wallets)
    4. Scoring wallets
    5. Analyse simple
    6. Sélection smart wallets
    """
    start_time = time.time()

    print("\n" + "="*80)
    print("🎯 PIPELINE 2: RE-SCORING QUOTIDIEN")
    print("="*80)
    print(f"⏰ Démarrage: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")

    # Étape 0: Récupérer la liste des wallets
    wallets_to_rescore = get_wallets_to_rescore()

    if not wallets_to_rescore:
        print("❌ Aucun wallet à re-scorer")
        return False

    # Étape 1: Mise à jour des transactions
    changes = update_transaction_histories(wallets_to_rescore)

    # Étape 2: Analyse FIFO
    if not run_fifo_analysis_full():
        print("❌ Erreur lors de l'analyse FIFO")
        return False

    # Étape 3: Scoring
    if not run_wallet_scoring_full():
        print("❌ Erreur lors du scoring")
        return False

    # Étape 4: Analyse simple
    if not run_simple_analysis():
        print("❌ Erreur lors de l'analyse simple")
        return False

    # Étape 5: Optimal threshold
    if not run_optimal_threshold():
        print("❌ Erreur lors de la sélection smart wallets")
        return False

    # Stats finales
    elapsed = time.time() - start_time
    stats = get_final_stats()

    print("\n" + "="*80)
    print("✅ PIPELINE 2 TERMINÉ AVEC SUCCÈS")
    print("="*80)
    print(f"⏱️ Durée totale: {elapsed/60:.1f} minutes")
    print(f"📊 Wallets analysés: {stats.get('analyzed_wallets', 0)}")
    print(f"🎯 Wallets qualifiés: {stats.get('qualified_wallets', 0)}")
    print(f"⭐ Smart wallets: {stats.get('smart_wallets', 0)}")
    print(f"🔄 Wallets avec changements: {changes}")
    print(f"🏁 Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")

    return True


if __name__ == "__main__":
    try:
        success = run_complete_scoring_pipeline()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⚠️ Pipeline interrompu par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Erreur fatale: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
