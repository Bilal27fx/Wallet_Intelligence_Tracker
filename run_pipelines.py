#!/usr/bin/env python3
"""Orchestrateur des pipelines WIT."""

import sys
import time
import argparse
from datetime import datetime

import schedule

from smart_wallet_analysis.logger import get_logger

logger = get_logger("pipelines.orchestrator")


def _log_section(title, width=80):
    """Affiche un en-tete de section."""
    line = "=" * width
    logger.info("")
    logger.info("%s", line)
    logger.info("%s", title)
    logger.info("%s", line)
    logger.info("")


def print_banner():
    """Affiche la banniere du systeme."""
    _log_section("WIT PIPELINES ORCHESTRATOR")
    logger.info("Demarrage: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


def run_discovery():
    """Execute le Discovery Pipeline."""
    print_banner()
    logger.info("LANCEMENT: Discovery Pipeline")

    try:
        from smart_wallet_analysis.discovery_pipeline_runner import run_discovery_pipeline
        return run_discovery_pipeline(
            skip_token_discovery=False,
            skip_wallet_tracker=False,
            skip_score_engine=False,
            quality_filter=0.0
        )
    except Exception as e:
        logger.error("Erreur Discovery Pipeline: %s", e)
        return False


def run_scoring():
    """Execute le Scoring Pipeline."""
    print_banner()
    logger.info("LANCEMENT: Scoring Pipeline")

    try:
        from smart_wallet_analysis.scoring_pipeline_runner import run_complete_scoring_pipeline
        return run_complete_scoring_pipeline()
    except Exception as e:
        logger.error("Erreur Scoring Pipeline: %s", e)
        return False


def run_smartwallets_live():
    """Execute le Smart Wallets Live Pipeline."""
    print_banner()
    logger.info("LANCEMENT: Smart Wallets Live Pipeline")

    try:
        from smart_wallet_analysis.run_smartwallets_pipeline import (
            run_tracking_and_fifo_pipeline,
        )
        return run_tracking_and_fifo_pipeline()
    except Exception as e:
        logger.error("Erreur Smart Wallets Live Pipeline: %s", e)
        return False


def run_consensus_live():
    """Execute la detection de consensus live."""
    print_banner()
    logger.info("LANCEMENT: Consensus Live Detector")
    try:
        from smart_wallet_analysis.consensus_live.consensus_live_detector import (
            run_live_consensus_detection,
        )
        signals = run_live_consensus_detection()
        logger.info("Consensus detectes: %s", len(signals))
        return True
    except Exception as e:
        logger.error("Erreur Consensus Live Detector: %s", e)
        return False


def run_backtesting_simple():
    """Execute le backtesting consensus simple."""
    print_banner()
    logger.info("LANCEMENT: Backtesting Consensus Simple")
    try:
        from smart_wallet_analysis.backtesting_engine.consensus_backtesting_simple import (
            run_simple_backtesting,
            export_simple_results,
        )
        all_consensus, period_results = run_simple_backtesting()
        if all_consensus:
            export_simple_results(all_consensus, period_results)
            logger.info("Backtesting termine: %s consensus exportes", len(all_consensus))
        else:
            logger.info("Backtesting termine: aucun consensus detecte")
        return True
    except Exception as e:
        logger.error("Erreur Backtesting Consensus Simple: %s", e)
        return False


def scheduled_discovery():
    """Tache planifiee: Discovery quotidien."""
    _log_section("TACHE PLANIFIEE: Discovery Pipeline")
    run_discovery()


def scheduled_scoring():
    """Tache planifiee: Scoring quotidien."""
    _log_section("TACHE PLANIFIEE: Scoring Pipeline")
    run_scoring()


def scheduled_smartwallets_live():
    """Tache planifiee: Smart Wallets Live toutes les 2h."""
    _log_section("TACHE PLANIFIEE: Smart Wallets Live (2h)")
    run_smartwallets_live()


def run_scheduler():
    """Lance le scheduler automatique."""
    print_banner()
    logger.info("MODE SCHEDULER AUTOMATIQUE")
    logger.info("Planification:")
    logger.info("Discovery Pipeline: tous les jours a 02:00")
    logger.info("Scoring Pipeline: tous les 2 jours a 04:00")
    logger.info("Smart Wallets Live: toutes les 2 heures")

    schedule.every().day.at("02:00").do(scheduled_discovery)
    schedule.every(2).days.at("04:00").do(scheduled_scoring)
    schedule.every(2).hours.do(scheduled_smartwallets_live)

    logger.info("Scheduler demarre. En attente des prochaines taches...")
    logger.info("Prochaines executions:")
    for job in schedule.get_jobs():
        logger.info("%s - %s", job.next_run.strftime("%Y-%m-%d %H:%M:%S"), job.job_func.__name__)

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        logger.warning("Scheduler arrete par l'utilisateur")
        sys.exit(0)


def _menu_actions():
    """Retourne le mapping des actions disponibles."""
    return {
        "1": ("Discovery Pipeline", run_discovery),
        "2": ("Scoring Pipeline", run_scoring),
        "3": ("Smart Wallets Live Pipeline", run_smartwallets_live),
        "4": ("Consensus Live Detector", run_consensus_live),
        "5": ("Backtesting Consensus Simple", run_backtesting_simple),
        "6": ("Scheduler Automatique", run_scheduler),
        "0": ("Quitter", None),
    }


def _show_menu():
    """Affiche le menu interactif."""
    _log_section("MENU WIT PIPELINES")
    logger.info("Choisis une option:")
    for key, (label, _) in _menu_actions().items():
        logger.info("  %s) %s", key, label)


def run_interactive_menu():
    """Lance le menu interactif."""
    while True:
        _show_menu()
        try:
            choice = input("\nOption > ").strip()
        except EOFError:
            logger.warning("Entrée fermée, arrêt du menu")
            return 0

        actions = _menu_actions()
        if choice not in actions:
            logger.warning("Option invalide: %s", choice)
            continue

        label, action = actions[choice]
        if action is None:
            logger.info("Sortie du menu")
            return 0

        logger.info("Execution: %s", label)
        success = action()
        logger.info("Resultat: %s", "SUCCES" if success else "ECHEC")
        if choice == "6":
            return 0


def _parse_args():
    """Parse les arguments CLI."""
    parser = argparse.ArgumentParser(description="Orchestrateur des pipelines WIT")
    parser.add_argument(
        "command",
        nargs="?",
        default="menu",
        choices=[
            "menu",
            "scheduler",
            "discovery",
            "scoring",
            "smartwallets",
            "consensus",
            "backtest",
        ],
        help="Commande a executer (defaut: menu)",
    )
    return parser.parse_args()


def main():
    """Point d'entree principal."""
    try:
        args = _parse_args()
        dispatch = {
            "menu": run_interactive_menu,
            "scheduler": run_scheduler,
            "discovery": run_discovery,
            "scoring": run_scoring,
            "smartwallets": run_smartwallets_live,
            "consensus": run_consensus_live,
            "backtest": run_backtesting_simple,
        }
        result = dispatch[args.command]()
        if isinstance(result, bool):
            sys.exit(0 if result else 1)
        sys.exit(0)
    except KeyboardInterrupt:
        logger.warning("Execution arretee par l'utilisateur")
        sys.exit(0)
    except Exception as e:
        logger.error("Erreur fatale: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
