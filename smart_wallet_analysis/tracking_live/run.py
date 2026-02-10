#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Runner principal pour le module tracking_live
1. Detection des changements de positions + mise à jour temps réel
2. Récupération de l'historique par token pour les tokens modifiés
3. Interface en ligne de commande avec options configurables
"""

import sys
import argparse
import time
from pathlib import Path
from datetime import datetime

# Ajouter le répertoire parent au PYTHONPATH
sys.path.append(str(Path(__file__).parent))

# Imports des modules de tracking
from live_wallet_balances_extractor_zerion import (
    run_live_wallet_changes_tracker,
    get_existing_wallet_tokens,
    get_token_balances_zerion,
    detect_position_changes_sql
)
from live_wallet_transaction_tracker_extractor_zerion import run_optimized_transaction_tracking
from wallet_migration_detector import run_migration_detection
import sqlite3
import uuid

def run_complete_live_tracking(enable_transaction_tracking=True, min_usd=500, hours_lookback=24, enable_migration_detection=True):
    """Lance le tracking live complet en 3 phases

    Args:
        enable_transaction_tracking (bool): Activer la phase 2 (historique des transactions)
        min_usd (int): Seuil minimum USD pour le tracking des transactions
        hours_lookback (int): Nombre d'heures à analyser pour les changements récents
        enable_migration_detection (bool): Activer la phase 3 (détection des migrations de wallets)
    """
    
    print("=" * 80)
    print("🚀 TRACKING LIVE COMPLET - WIT V1")
    print("=" * 80)
    print(f"⏰ Démarrage: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔧 Configuration:")
    print(f"   • Transaction tracking: {'✅ Activé' if enable_transaction_tracking else '❌ Désactivé'}")
    print(f"   • Seuil minimum: ${min_usd:,}")
    print(f"   • Analyse des dernières: {hours_lookback}h")
    print()
    
    start_time = time.time()
    
    try:
        # === PHASE 1: Détection changements + Mise à jour positions ===
        print("=" * 60)
        print("🔍 PHASE 1: DÉTECTION CHANGEMENTS & MISE À JOUR POSITIONS")
        print("=" * 60)
        
        phase1_start = time.time()
        success_phase1 = run_live_wallet_changes_tracker()
        phase1_duration = time.time() - phase1_start
        
        if not success_phase1:
            print("❌ Erreur Phase 1 - Arrêt du tracking")
            return False
        
        print(f"✅ Phase 1 terminée avec succès! ({phase1_duration:.1f}s)")
        print("   🔄 Changements détectés et positions mises à jour")
        print("   📊 Tables mises à jour: wallet_position_changes, tokens, wallets")
        
        if not enable_transaction_tracking:
            print("\n🏁 Tracking terminé (Phase 2 désactivée)")
            return True
        
        # === PHASE 2: Récupération historique par token ===
        print("\n" + "=" * 60)
        print("📈 PHASE 2: REMPLACEMENT HISTORIQUE TOKENS MODIFIÉS")
        print("=" * 60)
        
        phase2_start = time.time()
        success_phase2 = run_optimized_transaction_tracking(min_usd=min_usd, hours_lookback=hours_lookback)
        phase2_duration = time.time() - phase2_start
        
        if not success_phase2:
            print("⚠️ Erreur Phase 2 - Historiques partiellement mis à jour")
            return False
        
        print(f"✅ Phase 2 terminée avec succès! ({phase2_duration:.1f}s)")
        print("   📚 Historiques complets remplacés pour tokens modifiés")
        print("   📊 Table mise à jour: transaction_history")

        # === PHASE 3: Détection des migrations de wallets ===
        if enable_migration_detection:
            print("\n" + "=" * 60)
            print("🔄 PHASE 3: DÉTECTION DES MIGRATIONS DE WALLETS")
            print("=" * 60)

            phase3_start = time.time()
            try:
                migrations = run_migration_detection(
                    hours_lookback=168,           # fenêtre fixe 7 jours pour les migrations
                    min_transfer_percentage=70    # 70% du portefeuille transféré = migration
                )
                phase3_duration = time.time() - phase3_start

                if migrations:
                    print(f"✅ Phase 3 terminée: {len(migrations)} migrations détectées! ({phase3_duration:.1f}s)")
                    print("   🔗 Liens de migration créés avec héritage des prix d'achat")
                    print("   📊 Tables mises à jour: wallet_migrations, transaction_history")
                else:
                    print(f"✅ Phase 3 terminée: Aucune migration détectée ({phase3_duration:.1f}s)")
            except Exception as migration_error:
                print(f"⚠️ Erreur Phase 3 (non critique): {migration_error}")
                print("   → Le tracking continue sans détection de migration")

    except Exception as e:
        print(f"❌ Erreur critique pendant le tracking: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # === RÉSUMÉ FINAL ===
    total_duration = time.time() - start_time
    print("\n" + "=" * 60)
    print("🎉 TRACKING LIVE COMPLET TERMINÉ")
    print("=" * 60)
    print("✅ Phase 1: Détection changements + Mise à jour positions")
    if enable_transaction_tracking:
        print("✅ Phase 2: Remplacement historique complet")
        if enable_migration_detection:
            print("✅ Phase 3: Détection migrations + Héritage prix")
    print(f"⏱️ Durée totale: {total_duration:.1f}s")
    print(f"🏁 Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("📊 Base de données mise à jour:")
    print("   • wallet_position_changes   (changements détectés)")
    print("   • tokens                    (positions actuelles avec in_portfolio)")
    print("   • wallets                   (valeurs de portefeuille)")
    if enable_transaction_tracking:
        print("   • transaction_history       (historiques complets)")
        if enable_migration_detection:
            print("   • wallet_migrations         (liens de migration + prix hérités)")
    print("=" * 80)
    
    return True

def run_balance_tracking_only():
    """Lance uniquement le tracking des changements de balance (Phase 1)"""
    
    print("🔍 TRACKING BALANCES UNIQUEMENT")
    print("=" * 50)
    
    try:
        success = run_live_wallet_changes_tracker()
        if success:
            print("✅ Tracking des balances terminé avec succès!")
        else:
            print("❌ Erreur lors du tracking des balances")
        return success
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return False

def run_transaction_tracking_only(min_usd=500, hours_lookback=24):
    """Lance uniquement le tracking des transactions (Phase 2)"""

    print(f"📈 TRACKING TRANSACTIONS UNIQUEMENT")
    print("=" * 50)

    try:
        success = run_optimized_transaction_tracking(min_usd=min_usd, hours_lookback=hours_lookback)
        if success:
            print("✅ Tracking des transactions terminé avec succès!")
        else:
            print("❌ Erreur lors du tracking des transactions")
        return success
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return False

def run_rescoring_transaction_update(wallet_list, min_usd=500, hours_lookback=24):
    """
    Version spéciale pour le Pipeline 2 de re-scoring
    Traite une liste de wallets (pas juste smart wallets)

    Args:
        wallet_list: Liste des wallets à mettre à jour
        min_usd: Seuil minimum USD pour le tracking
        hours_lookback: Nombre d'heures à analyser

    Returns:
        int: Nombre de wallets avec changements détectés
    """
    print("=" * 80)
    print("🔄 MISE À JOUR TRANSACTIONS POUR RE-SCORING")
    print("=" * 80)
    print(f"📊 {len(wallet_list)} wallets à traiter")
    print(f"💰 Seuil minimum: ${min_usd}")
    print(f"⏰ Fenêtre: {hours_lookback}h")
    print()

    start_time = time.time()
    session_id = str(uuid.uuid4())[:8]
    changes_detected = 0
    errors = 0

    # Phase 1: Détection des changements pour chaque wallet
    print("=" * 60)
    print("🔍 PHASE 1: DÉTECTION DES CHANGEMENTS")
    print("=" * 60)

    for i, wallet in enumerate(wallet_list, 1):
        try:
            print(f"\n[{i}/{len(wallet_list)}] 🔍 {wallet[:12]}...")

            # Récupérer positions actuelles (SANS filtre smart_wallets)
            db_positions = get_existing_wallet_tokens(
                wallet,
                filter_smart_wallets=False  # Mode re-scoring
            )

            if not db_positions:
                print(f"  ⚠️ Aucune position en base")
                continue

            # Récupérer positions live depuis Zerion
            live_positions_df = get_token_balances_zerion(wallet)

            if live_positions_df is None or live_positions_df.empty:
                print(f"  ⚠️ Aucune position live trouvée")
                continue

            # Convertir le DataFrame en liste de dictionnaires (format attendu par detect_position_changes_sql)
            live_positions = []
            for _, row in live_positions_df.iterrows():
                live_positions.append({
                    'token': row.get('token', row.get('symbol', 'UNKNOWN')),
                    'amount': row['amount'],
                    'usd_value': row['usd_value'],
                    'contract_address': row['contract_address'],
                    'chain': row['chain'],
                    'fungible_id': row['fungible_id']
                })

            # Comparer et détecter changements
            changes = detect_position_changes_sql(wallet, live_positions, session_id)

            if changes:
                # Compter le nombre total de changements
                total_changes = (len(changes.get('new_tokens', [])) +
                               len(changes.get('accumulations', [])) +
                               len(changes.get('reductions', [])) +
                               len(changes.get('exits', [])))

                if total_changes > 0:
                    changes_detected += 1
                    print(f"  ✅ {total_changes} changements détectés")
                else:
                    print(f"  ℹ️ Aucun changement")
            else:
                print(f"  ℹ️ Aucun changement")

        except Exception as e:
            print(f"  ❌ Erreur: {e}")
            errors += 1

        # Rate limiting léger
        if i % 10 == 0:
            time.sleep(2)

    print(f"\n✅ Phase 1 terminée: {changes_detected} wallets avec changements, {errors} erreurs")

    # Phase 2: Mise à jour historique seulement pour les tokens modifiés
    if changes_detected > 0:
        print("\n" + "=" * 60)
        print("📈 PHASE 2: MISE À JOUR DES HISTORIQUES")
        print("=" * 60)

        success = run_optimized_transaction_tracking(
            min_usd=min_usd,
            hours_lookback=hours_lookback
        )

        if not success:
            print("⚠️ Phase 2 terminée avec des erreurs")
    else:
        print("\nℹ️ Aucun changement détecté - Phase 2 non nécessaire")

    # Résumé
    duration = time.time() - start_time
    print("\n" + "=" * 80)
    print("✅ MISE À JOUR RE-SCORING TERMINÉE")
    print("=" * 80)
    print(f"📊 Wallets traités: {len(wallet_list)}")
    print(f"🔄 Wallets avec changements: {changes_detected}")
    print(f"❌ Erreurs: {errors}")
    print(f"⏱️ Durée: {duration/60:.1f} minutes")
    print("=" * 80)

    return changes_detected

def main():
    """Interface en ligne de commande pour le tracking live"""
    
    parser = argparse.ArgumentParser(
        description='🚀 Runner pour le module tracking_live - WIT V1',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  python run.py                           # Tracking complet (balance + transactions)
  python run.py --balance-only             # Uniquement les changements de balances
  python run.py --transactions-only        # Uniquement l'historique des transactions
  python run.py --no-transactions          # Balance seulement (équivalent à --balance-only)
  python run.py --min-usd 1000            # Seuil minimum 1000$ pour les transactions
  python run.py --hours-lookback 48       # Analyser les 48 dernières heures
        """
    )
    
    # Modes d'exécution
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        '--balance-only', 
        action='store_true',
        help='Lance uniquement la détection des changements de balances (Phase 1)'
    )
    mode_group.add_argument(
        '--transactions-only', 
        action='store_true',
        help='Lance uniquement le tracking des transactions (Phase 2)'
    )
    mode_group.add_argument(
        '--no-transactions', 
        action='store_true',
        help='Désactive le tracking des transactions (équivalent à --balance-only)'
    )
    
    # Configuration
    parser.add_argument(
        '--min-usd', 
        type=int, 
        default=500,
        help='Seuil minimum USD pour le tracking des transactions (défaut: 500)'
    )
    parser.add_argument(
        '--hours-lookback', 
        type=int, 
        default=24,
        help='Nombre d\'heures à analyser pour les changements récents (défaut: 24)'
    )
    
    # Options de debug
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Mode verbeux (plus de détails)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Mode simulation (affiche la configuration sans exécuter)'
    )
    
    args = parser.parse_args()
    
    # Affichage de la configuration
    print("🔧 CONFIGURATION:")
    print(f"   • Mode: ", end="")
    if args.balance_only or args.no_transactions:
        print("Balances uniquement")
    elif args.transactions_only:
        print("Transactions uniquement")
    else:
        print("Complet (balances + transactions)")
    
    print(f"   • Seuil minimum: ${args.min_usd:,}")
    print(f"   • Analyse des dernières: {args.hours_lookback}h")
    print(f"   • Mode verbeux: {'✅' if args.verbose else '❌'}")
    print(f"   • Simulation: {'✅' if args.dry_run else '❌'}")
    print()
    
    if args.dry_run:
        print("🧪 MODE SIMULATION - Aucune exécution réelle")
        return
    
    # Exécution selon le mode choisi
    try:
        if args.balance_only or args.no_transactions:
            success = run_balance_tracking_only()
        elif args.transactions_only:
            success = run_transaction_tracking_only(
                min_usd=args.min_usd,
                hours_lookback=args.hours_lookback
            )
        else:
            success = run_complete_live_tracking(
                enable_transaction_tracking=True,
                min_usd=args.min_usd,
                hours_lookback=args.hours_lookback
            )
        
        if success:
            print("\n🎉 Tracking terminé avec succès!")
            sys.exit(0)
        else:
            print("\n❌ Tracking terminé avec des erreurs")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️ Tracking interrompu par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Erreur fatale: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()