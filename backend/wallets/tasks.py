"""Celery tasks for WIT pipelines."""
from celery import shared_task
from celery.utils.log import get_task_logger
from wallets.services import (
    FIFOCalculatorService,
    WalletScorerService,
    TierAnalyzerService,
    ConsensusDetectorService
)

logger = get_task_logger(__name__)


@shared_task(bind=True, max_retries=3)
def run_scoring_pipeline(self):
    """
    Run scoring pipeline (FIFO + wallet scoring + tier analysis).
    Uses Django services created in Phase 3.
    """
    try:
        logger.info("Starting scoring pipeline...")

        # Step 1: Calculate FIFO analytics for all wallets
        logger.info("Step 1/3: Calculating FIFO analytics...")
        calculator = FIFOCalculatorService()
        fifo_result = calculator.recalculate_all_analytics()
        logger.info(f"FIFO: {fifo_result['wallets_processed']} wallets, {fifo_result['tokens_calculated']} tokens")

        # Step 2: Score all wallets
        logger.info("Step 2/3: Scoring wallets...")
        scorer = WalletScorerService()
        score_result = scorer.score_all_wallets()
        logger.info(f"Scoring: {score_result['total_wallets']} wallets scored, {score_result['qualified']} qualified")

        # Step 3: Analyze tiers and identify smart wallets
        logger.info("Step 3/3: Analyzing tiers...")
        tier_analyzer = TierAnalyzerService()
        tier_result = tier_analyzer.analyze_all_wallets()
        logger.info(f"Tiers: {tier_result['smart_wallets']} smart wallets identified")

        logger.info("Scoring pipeline completed successfully")
        return {
            'status': 'success',
            'fifo': fifo_result,
            'scoring': score_result,
            'tiers': tier_result
        }

    except Exception as exc:
        logger.error(f"Scoring pipeline failed: {exc}")
        self.retry(exc=exc, countdown=300)  # Retry after 5min


@shared_task(bind=True, max_retries=3)
def run_consensus_detection(self):
    """
    Run consensus detection from smart wallets.
    Uses Django services created in Phase 3.
    """
    try:
        logger.info("Starting consensus detection...")

        detector = ConsensusDetectorService()
        result = detector.run_consensus_detection()

        logger.info(
            f"Consensus detection completed: "
            f"{result['buy_signals']} buy signals, "
            f"{result['sell_signals']} sell signals"
        )

        # Trigger Telegram alerts for each signal
        from .tasks import send_telegram_alert
        for signal in result['signals']['buys']:
            if signal.get('token_address'):
                logger.info(f"Triggering alert for BUY signal: {signal['symbol']}")
                # TODO: Get signal ID and trigger alert

        for signal in result['signals']['sells']:
            if signal.get('token_address'):
                logger.info(f"Triggering alert for SELL signal: {signal['symbol']}")
                # TODO: Get signal ID and trigger alert

        return {
            'status': 'success',
            'buy_signals': result['buy_signals'],
            'sell_signals': result['sell_signals']
        }

    except Exception as exc:
        logger.error(f"Consensus detection failed: {exc}")
        self.retry(exc=exc, countdown=300)  # Retry after 5min


@shared_task
def send_telegram_alert(signal_data: dict):
    """Send Telegram alert for consensus signal."""
    try:
        logger.info(f"Sending Telegram alert for {signal_data.get('symbol')}")

        # TODO: Implement Telegram service
        logger.warning("Telegram service not yet implemented")

        return {'status': 'success', 'signal': signal_data}

    except Exception as exc:
        logger.error(f"Failed to send Telegram alert: {exc}")
        raise


@shared_task
def sync_wallet_data(wallet_address: str):
    """Sync data for a specific wallet from Zerion."""
    from wallets.services import ZerionTrackerService

    try:
        logger.info(f"Syncing wallet {wallet_address}...")

        tracker = ZerionTrackerService()
        result = tracker.full_sync(wallet_address)

        logger.info(f"Wallet {wallet_address} synced successfully")
        return {'status': 'success', 'wallet': wallet_address, 'result': result}

    except Exception as exc:
        logger.error(f"Failed to sync wallet {wallet_address}: {exc}")
        raise


@shared_task
def detect_position_changes(wallet_address: str):
    """Detect position changes for a specific wallet."""
    from wallets.services import BalanceTrackerService

    try:
        logger.info(f"Detecting position changes for {wallet_address}...")

        tracker = BalanceTrackerService()
        changes = tracker.detect_position_changes(wallet_address)

        logger.info(f"Detected {len(changes)} position changes for {wallet_address}")
        return {'status': 'success', 'wallet': wallet_address, 'changes': len(changes)}

    except Exception as exc:
        logger.error(f"Failed to detect position changes for {wallet_address}: {exc}")
        raise
