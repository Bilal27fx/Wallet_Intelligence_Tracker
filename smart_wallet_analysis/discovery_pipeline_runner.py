#!/usr/bin/env python3
"""Discovery pipeline: token discovery, wallet tracker, score engine."""

import sys
import time
import argparse
from datetime import datetime

from smart_wallet_analysis.logger import get_logger
from smart_wallet_analysis.token_discovery_manual.runner import run_token_discovery_pipeline
from smart_wallet_analysis.wallet_tracker.wallet_tracker_runner import main as run_wallet_tracker
from smart_wallet_analysis.score_engine.score_engine_runner import run_score_engine_pipeline

logger = get_logger("discovery_pipeline.runner")


def _fmt(seconds):
    """Formate une durée en secondes."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        return f"{seconds/60:.1f}min"
    return f"{seconds/3600:.1f}h"


def _log_section(title, width=70):
    """Affiche un en-tete de section."""
    line = "=" * width
    logger.info("")
    logger.info("%s", line)
    logger.info("%s", title)
    logger.info("%s", line)


def run_discovery_pipeline(skip_token_discovery=False, skip_wallet_tracker=False, skip_score_engine=False, quality_filter=0.0):
    """Pipeline de découverte : tokens → wallets → scoring."""
    start = datetime.now()
    steps = []

    _log_section(f"🚀 DISCOVERY PIPELINE — {start.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(
        "Token Discovery: %s | Wallet Tracker: %s | Score Engine: %s (qualite >= %s)",
        "SKIP" if skip_token_discovery else "ON",
        "SKIP" if skip_wallet_tracker else "ON",
        "SKIP" if skip_score_engine else "ON",
        quality_filter,
    )

    if not skip_token_discovery:
        _log_section("🎯 [1/3] TOKEN DISCOVERY", width=50)
        t = time.time()
        try:
            run_token_discovery_pipeline()
            d = time.time() - t
            steps.append(("Token Discovery", d, True, None))
            logger.info("✅ Token Discovery termine (%s)", _fmt(d))
        except Exception as e:
            d = time.time() - t
            steps.append(("Token Discovery", d, False, str(e)))
            logger.error("❌ Erreur Token Discovery: %s", e)
            logger.warning("⚠️ Pipeline continue avec donnees existantes...")
    else:
        logger.info("⏩ [1/3] Token Discovery — SKIP")

    if not skip_wallet_tracker:
        _log_section("💼 [2/3] WALLET TRACKER", width=50)
        t = time.time()
        try:
            success = run_wallet_tracker()
            d = time.time() - t
            if not success:
                raise Exception("Wallet Tracker a retourné False")
            steps.append(("Wallet Tracker", d, True, None))
            logger.info("✅ Wallet Tracker termine (%s)", _fmt(d))
        except Exception as e:
            d = time.time() - t
            steps.append(("Wallet Tracker", d, False, str(e)))
            logger.error("❌ Erreur Wallet Tracker: %s", e)
            _print_summary(start, steps)
            return False
    else:
        logger.info("⏩ [2/3] Wallet Tracker — SKIP")

    if not skip_score_engine:
        _log_section("⭐ [3/3] SCORE ENGINE", width=50)
        t = time.time()
        try:
            success = run_score_engine_pipeline(quality_filter=quality_filter, show_stats=True)
            d = time.time() - t
            if not success:
                raise Exception("Score Engine a retourné False")
            steps.append(("Score Engine", d, True, None))
            logger.info("✅ Score Engine termine (%s)", _fmt(d))
        except Exception as e:
            d = time.time() - t
            steps.append(("Score Engine", d, False, str(e)))
            logger.error("❌ Erreur Score Engine: %s", e)
    else:
        logger.info("⏩ [3/3] Score Engine — SKIP")

    return _print_summary(start, steps)


def _print_summary(start, steps):
    """Affiche le résumé et retourne True si toutes les étapes ont réussi."""
    total = (datetime.now() - start).total_seconds()
    success_count = sum(1 for _, _, ok, _ in steps if ok)
    _log_section(f"📊 RESUME — duree: {_fmt(total)} | {success_count}/{len(steps)} etapes reussies")
    for i, (name, d, ok, err) in enumerate(steps, 1):
        logger.info("%s [%s] %-25s %s", "✅" if ok else "❌", i, name, _fmt(d))
        if err:
            logger.info("   -> %s", err)
    return success_count == len(steps)


def main():
    """Point d'entrée CLI."""
    parser = argparse.ArgumentParser(description='Discovery Pipeline — Construction de la base de données')

    skip = parser.add_argument_group('Skip')
    skip.add_argument('--skip-token-discovery', action='store_true')
    skip.add_argument('--skip-wallet-tracker', action='store_true')
    skip.add_argument('--skip-score-engine', action='store_true')

    only = parser.add_argument_group('Only').add_mutually_exclusive_group()
    only.add_argument('--only-token-discovery', action='store_true')
    only.add_argument('--only-wallet-tracker', action='store_true')
    only.add_argument('--only-score-engine', action='store_true')

    parser.add_argument('--quality', type=float, default=0.0, help='Filtre qualité Score Engine (0.0-1.0)')
    args = parser.parse_args()

    if not (0.0 <= args.quality <= 1.0):
        logger.error("❌ --quality doit etre entre 0.0 et 1.0")
        sys.exit(1)

    if args.only_token_discovery:
        args.skip_wallet_tracker = args.skip_score_engine = True
    elif args.only_wallet_tracker:
        args.skip_token_discovery = args.skip_score_engine = True
    elif args.only_score_engine:
        args.skip_token_discovery = args.skip_wallet_tracker = True

    try:
        success = run_discovery_pipeline(
            skip_token_discovery=args.skip_token_discovery,
            skip_wallet_tracker=args.skip_wallet_tracker,
            skip_score_engine=args.skip_score_engine,
            quality_filter=args.quality
        )
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.warning("⚠️ Pipeline interrompu")
        sys.exit(1)
    except Exception as e:
        logger.error("💥 Erreur fatale: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
