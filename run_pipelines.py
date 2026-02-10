#!/usr/bin/env python3
"""
WIT PIPELINES ORCHESTRATOR
Gère l'exécution automatique et manuelle des pipelines d'analyse

PIPELINES:
1. Discovery Pipeline    - Construction de la BDD (nouveaux wallets)
2. Scoring Pipeline      - Re-scoring quotidien de tous les wallets
3. Smart Wallets Live    - Mise à jour live des smart wallets (toutes les 2h)

SCHEDULER:
- Discovery Pipeline:     1x par semaine (lundi 02:00)
- Scoring Pipeline:       1x par jour (tous les jours à 04:00)
- Smart Wallets Live:     Toutes les 2 heures

IMPORTANT:
Avant de lancer le Discovery Pipeline, vous DEVEZ remplir le fichier:
📄 /data/raw/json/explosive_tokens_manual.json

Format attendu:
[
  {
    "token_address": "0x...",
    "symbol": "PEPE",
    "chain": "ethereum",
    "perf_window": "250j",
    "type": 1
  }
]
"""

import sys
import time
import schedule
from pathlib import Path
from datetime import datetime

# Configuration des paths
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT / "smart_wallet_analysis"))

# Import des pipelines
from smart_wallet_analysis.discovery_pipeline_runner import run_discovery_pipeline
from smart_wallet_analysis.scoring_pipeline_runner import run_complete_scoring_pipeline
from smart_wallet_analysis.run_smartwallets_pipeline import run_tracking_and_fifo_pipeline


def check_explosive_tokens_file():
    """Vérifie que le fichier explosive_tokens_manual.json existe et n'est pas vide"""
    tokens_file = ROOT / "data" / "raw" / "json" / "explosive_tokens_manual.json"

    if not tokens_file.exists():
        print("\n" + "="*80)
        print("⚠️  ATTENTION: Fichier explosive_tokens_manual.json introuvable!")
        print("="*80)
        print(f"📄 Chemin attendu: {tokens_file}")
        print()
        print("📝 Créez ce fichier avec le format suivant:")
        print("""
[
  {
    "token_address": "0x...",
    "symbol": "PEPE",
    "chain": "ethereum",
    "perf_window": "250j",
    "type": 1
  }
]
        """)
        print("="*80 + "\n")
        return False

    import json
    try:
        with open(tokens_file, 'r') as f:
            tokens = json.load(f)
            if not tokens or len(tokens) == 0:
                print("\n" + "="*80)
                print("⚠️  ATTENTION: Le fichier explosive_tokens_manual.json est vide!")
                print("="*80)
                print(f"📄 Fichier: {tokens_file}")
                print()
                print("📝 Ajoutez des tokens explosifs avec le format suivant:")
                print("""
[
  {
    "token_address": "0x...",
    "symbol": "PEPE",
    "chain": "ethereum",
    "perf_window": "250j",
    "type": 1
  }
]
                """)
                print("="*80 + "\n")
                return False

            print(f"✅ Fichier explosive_tokens_manual.json trouvé: {len(tokens)} token(s)")
            return True

    except json.JSONDecodeError:
        print(f"\n❌ Erreur: Le fichier {tokens_file} contient du JSON invalide")
        return False


def print_banner():
    """Affiche la bannière du système"""
    print("\n" + "="*80)
    print("🚀 WIT PIPELINES ORCHESTRATOR")
    print("="*80)
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")


def run_discovery():
    """Exécute le Discovery Pipeline avec vérification préalable"""
    print_banner()
    print("📋 LANCEMENT: Discovery Pipeline (Construction de la BDD)")
    print()

    # Vérifier le fichier explosive_tokens_manual.json
    if not check_explosive_tokens_file():
        print("❌ Discovery Pipeline annulé: fichier explosive_tokens_manual.json manquant ou vide")
        print()
        print("➡️  Remplissez le fichier puis relancez:")
        print("    python run_pipelines.py --discovery")
        print()
        return False

    print()
    try:
        success = run_discovery_pipeline(
            skip_token_discovery=False,
            skip_wallet_tracker=False,
            skip_score_engine=False,
            quality_filter=0.0
        )
        return success
    except Exception as e:
        print(f"\n❌ Erreur lors du Discovery Pipeline: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_scoring():
    """Exécute le Scoring Pipeline (re-scoring quotidien)"""
    print_banner()
    print("📋 LANCEMENT: Scoring Pipeline (Re-scoring quotidien)")
    print()

    try:
        success = run_complete_scoring_pipeline()
        return success
    except Exception as e:
        print(f"\n❌ Erreur lors du Scoring Pipeline: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_smartwallets_live():
    """Exécute le Smart Wallets Live Pipeline (tracking temps réel)"""
    print_banner()
    print("📋 LANCEMENT: Smart Wallets Live Pipeline (Tracking temps réel)")
    print()

    try:
        success = run_tracking_and_fifo_pipeline()
        return success
    except Exception as e:
        print(f"\n❌ Erreur lors du Smart Wallets Live Pipeline: {e}")
        import traceback
        traceback.print_exc()
        return False


def scheduled_discovery():
    """Tâche planifiée: Discovery Pipeline (hebdomadaire)"""
    print("\n" + "🕐 "*40)
    print("⏰ TÂCHE PLANIFIÉE: Discovery Pipeline (hebdomadaire)")
    print("🕐 "*40 + "\n")
    run_discovery()


def scheduled_scoring():
    """Tâche planifiée: Scoring Pipeline (quotidien)"""
    print("\n" + "🕐 "*40)
    print("⏰ TÂCHE PLANIFIÉE: Scoring Pipeline (quotidien)")
    print("🕐 "*40 + "\n")
    run_scoring()


def scheduled_smartwallets_live():
    """Tâche planifiée: Smart Wallets Live (toutes les 2h)"""
    print("\n" + "🕐 "*40)
    print("⏰ TÂCHE PLANIFIÉE: Smart Wallets Live (2h)")
    print("🕐 "*40 + "\n")
    run_smartwallets_live()


def run_scheduler():
    """Lance le scheduler automatique"""
    print_banner()
    print("🤖 MODE SCHEDULER AUTOMATIQUE")
    print()
    print("📅 PLANIFICATION:")
    print("   • Discovery Pipeline:      Tous les lundis à 02:00")
    print("   • Scoring Pipeline:        Tous les jours à 04:00")
    print("   • Smart Wallets Live:      Toutes les 2 heures")
    print()
    print("⚠️  IMPORTANT: Avant le prochain Discovery Pipeline, remplissez:")
    print(f"   📄 {ROOT / 'data' / 'raw' / 'json' / 'explosive_tokens_manual.json'}")
    print()
    print("💡 TIP: Ctrl+C pour arrêter le scheduler")
    print("="*80 + "\n")

    # Configuration des tâches planifiées
    schedule.every().monday.at("02:00").do(scheduled_discovery)
    schedule.every().day.at("04:00").do(scheduled_scoring)
    schedule.every(2).hours.do(scheduled_smartwallets_live)

    print("✅ Scheduler démarré. En attente des prochaines tâches planifiées...")
    print()

    # Afficher les prochaines exécutions
    jobs = schedule.get_jobs()
    print("📋 PROCHAINES EXÉCUTIONS:")
    for job in jobs:
        print(f"   • {job.next_run.strftime('%Y-%m-%d %H:%M:%S')} - {job.job_func.__name__}")
    print()

    # Boucle principale du scheduler
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Vérifier toutes les minutes
    except KeyboardInterrupt:
        print("\n\n⚠️  Scheduler arrêté par l'utilisateur")
        sys.exit(0)


def main():
    """Point d'entrée principal - Lance directement le scheduler"""

    try:
        # Lancer directement le scheduler automatique
        run_scheduler()

    except KeyboardInterrupt:
        print("\n\n⚠️  Scheduler arrêté par l'utilisateur")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n❌ Erreur fatale: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
