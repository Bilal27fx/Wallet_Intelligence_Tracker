#!/usr/bin/env python3
"""Runner tracking_live: balances + transactions."""

import sys
import argparse
import time
import uuid

from smart_wallet_analysis.config import TRACKING_LIVE
from smart_wallet_analysis.logger import get_logger
from smart_wallet_analysis.tracking_live.live_wallet_balances_extractor_zerion import (
    run_live_wallet_changes_tracker,
    get_existing_wallet_tokens,
    get_token_balances_zerion,
    detect_position_changes_sql
)
from smart_wallet_analysis.tracking_live.live_wallet_transaction_tracker_extractor_zerion import run_optimized_transaction_tracking

logger = get_logger("tracking_live.runner")

_TL = TRACKING_LIVE


def run_complete_live_tracking(enable_transaction_tracking=True, min_usd=None, hours_lookback=None):
    """Lance le tracking live complet."""
    min_usd = _TL["MIN_TOKEN_VALUE_USD"] if min_usd is None else min_usd
    hours_lookback = _TL["HOURS_LOOKBACK_DEFAULT"] if hours_lookback is None else hours_lookback
    logger.info("TRACKING LIVE — démarrage")
    start_time = time.time()

    try:
        success = run_live_wallet_changes_tracker()
        if not success:
            logger.error("Phase 1 échouée — arrêt")
            return False
        logger.info("Phase 1 terminée (changements détectés, positions mises à jour)")

        if not enable_transaction_tracking:
            return True

        success = run_optimized_transaction_tracking(min_usd=min_usd, hours_lookback=hours_lookback)
        if not success:
            logger.warning("Phase 2 échouée — historiques partiellement mis à jour")
            return False
        logger.info("Phase 2 terminée (transaction_history mis à jour)")

    except Exception as e:
        logger.error(f"Erreur critique: {e}", exc_info=True)
        return False

    logger.info(f"Tracking terminé ({time.time() - start_time:.1f}s)")
    return True


def run_balance_tracking_only():
    """Lance uniquement la Phase 1."""
    try:
        success = run_live_wallet_changes_tracker()
        logger.info("Tracking balances terminé" if success else "Erreur tracking balances")
        return success
    except Exception as e:
        logger.error(f"Erreur: {e}")
        return False


def run_transaction_tracking_only(min_usd=None, hours_lookback=None):
    """Lance uniquement la Phase 2."""
    min_usd = _TL["MIN_TOKEN_VALUE_USD"] if min_usd is None else min_usd
    hours_lookback = _TL["HOURS_LOOKBACK_DEFAULT"] if hours_lookback is None else hours_lookback
    try:
        success = run_optimized_transaction_tracking(min_usd=min_usd, hours_lookback=hours_lookback)
        logger.info("Tracking transactions terminé" if success else "Erreur tracking transactions")
        return success
    except Exception as e:
        logger.error(f"Erreur: {e}")
        return False


def run_rescoring_transaction_update(wallet_list, min_usd=None, hours_lookback=None):
    """Mise à jour transactions pour re-scoring."""
    min_usd = _TL["MIN_TOKEN_VALUE_USD"] if min_usd is None else min_usd
    hours_lookback = _TL["HOURS_LOOKBACK_DEFAULT"] if hours_lookback is None else hours_lookback
    logger.info(f"MISE À JOUR RE-SCORING — {len(wallet_list)} wallets")
    start_time = time.time()
    session_id = str(uuid.uuid4())[:8]
    changes_detected = errors = 0

    for i, wallet in enumerate(wallet_list, 1):
        try:
            logger.info(f"[{i}/{len(wallet_list)}] {wallet[:12]}...")

            db_positions = get_existing_wallet_tokens(wallet, filter_smart_wallets=False)
            if not db_positions:
                logger.warning(f"  Aucune position en base — {wallet[:12]}...")
                continue

            live_df = get_token_balances_zerion(wallet)
            if live_df is None or live_df.empty:
                logger.warning(f"  Aucune position live — {wallet[:12]}...")
                continue

            live_positions = [
                {
                    'token': row.get('token', row.get('symbol', 'UNKNOWN')),
                    'amount': row['amount'],
                    'usd_value': row['usd_value'],
                    'contract_address': row['contract_address'],
                    'chain': row['chain'],
                    'fungible_id': row['fungible_id']
                }
                for _, row in live_df.iterrows()
            ]

            changes = detect_position_changes_sql(wallet, live_positions, session_id)
            total = sum(len(changes.get(k, [])) for k in ('new_tokens', 'accumulations', 'reductions', 'exits')) if changes else 0
            if total > 0:
                changes_detected += 1
                logger.info(f"  {total} changements")
            else:
                logger.info(f"  Aucun changement")

        except Exception as e:
            logger.error(f"  Erreur {wallet[:12]}...: {e}")
            errors += 1

        if i % 10 == 0:
            time.sleep(2)

    logger.info(f"Détection: {changes_detected} wallets avec changements, {errors} erreurs")

    if changes_detected > 0:
        run_optimized_transaction_tracking(min_usd=min_usd, hours_lookback=hours_lookback)

    logger.info(f"Re-scoring terminé ({(time.time() - start_time)/60:.1f}min)")
    return changes_detected


def main():
    """Interface CLI pour le tracking live."""
    parser = argparse.ArgumentParser(description='Runner tracking_live — WIT V1')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--balance-only', action='store_true', help='Phase 1 uniquement (balances)')
    mode.add_argument('--transactions-only', action='store_true', help='Phase 2 uniquement (transactions)')
    mode.add_argument('--no-transactions', action='store_true', help='Désactive Phase 2')
    parser.add_argument(
        '--min-usd',
        type=int,
        default=_TL["MIN_TOKEN_VALUE_USD"],
        help=f'Seuil minimum USD (défaut: {_TL["MIN_TOKEN_VALUE_USD"]})'
    )
    parser.add_argument(
        '--hours-lookback',
        type=int,
        default=_TL["HOURS_LOOKBACK_DEFAULT"],
        help=f'Heures à analyser (défaut: {_TL["HOURS_LOOKBACK_DEFAULT"]})'
    )
    args = parser.parse_args()

    try:
        if args.balance_only or args.no_transactions:
            success = run_balance_tracking_only()
        elif args.transactions_only:
            success = run_transaction_tracking_only(min_usd=args.min_usd, hours_lookback=args.hours_lookback)
        else:
            success = run_complete_live_tracking(
                enable_transaction_tracking=True,
                min_usd=args.min_usd,
                hours_lookback=args.hours_lookback
            )
        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        logger.warning("Tracking interrompu")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erreur fatale: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
