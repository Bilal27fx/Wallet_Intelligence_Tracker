#!/usr/bin/env python3
"""Runner consensus live."""

from datetime import datetime
from numbers import Number

from smart_wallet_analysis.config import CONSENSUS_LIVE
from smart_wallet_analysis.logger import get_logger
from smart_wallet_analysis.consensus_live.data import (
    get_smart_wallets as _get_smart_wallets,
    get_recent_transactions_live,
    get_existing_consensus_from_db,
    save_live_consensus_to_db,
)
from smart_wallet_analysis.consensus_live.logic import (
    detect_live_consensus,
    calculate_live_performance,
)

logger = get_logger("consensus_live.runner")

def get_smart_wallets():
    """Récupère les wallets qualifiés."""
    return _get_smart_wallets()

def _fmt_datetime(value):
    """Formate une date pour les logs."""
    if value is None:
        return "N/A"
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        if value.tzinfo:
            return value.strftime("%Y-%m-%d %H:%M:%S %Z")
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)

def _fmt_money(value):
    """Formate un montant USD."""
    return f"{value:,.0f}" if isinstance(value, Number) else "N/A"

def _fmt_float(value, decimals=2):
    """Formate un nombre décimal."""
    if not isinstance(value, Number):
        return "N/A"
    return f"{value:.{decimals}f}"

def _fmt_pct(value):
    """Formate un pourcentage signé."""
    return f"{value:+.1f}%" if isinstance(value, Number) else "N/A"

def _log_signal(signal):
    """Log un signal de consensus."""
    perf = signal.get("performance", {})
    token_info = signal.get("token_info", {})
    whale_details = signal.get("whale_details", [])

    logger.info("%s (%s)", signal["symbol"], signal["signal_type"])
    logger.info("contract: %s", signal.get("contract_address", "N/A"))
    logger.info(
        "detection: %s (formation du consensus >=%s whales)",
        _fmt_datetime(signal.get("detection_date")),
        CONSENSUS_LIVE["MIN_WHALES_CONSENSUS"]
    )
    logger.info(
        "whales: %s (exceptionnels: %s, normaux: %s)",
        signal["whale_count"],
        signal["exceptional_count"],
        signal["normal_count"]
    )
    logger.info("investi: $%s", _fmt_money(signal.get("total_investment")))
    logger.info(
        "période: %s -> %s",
        _fmt_datetime(signal.get("period_start")),
        _fmt_datetime(signal.get("period_end"))
    )

    if token_info:
        logger.info(
            "market cap: $%s | volume 24h: $%s",
            f"{token_info.get('market_cap', 0):,.0f}",
            f"{token_info.get('volume_24h', 0):,.0f}"
        )
        logger.info(
            "variation 24h: %+0.1f%% | liquidité: $%s",
            token_info.get("price_change_24h", 0),
            f"{token_info.get('liquidity_usd', 0):,.0f}"
        )

        buys_24h = token_info.get("txns_24h_buys", 0)
        sells_24h = token_info.get("txns_24h_sells", 0)
        if buys_24h > 0 or sells_24h > 0:
            total_txns = buys_24h + sells_24h
            buy_ratio = (buys_24h / total_txns * 100) if total_txns > 0 else 0
            logger.info(
                "txns 24h: %s achats | %s ventes | ratio achat: %.1f%%",
                buys_24h,
                sells_24h,
                buy_ratio
            )

    if perf.get("performance_pct") is not None:
        logger.info(
            "performance: %+0.1f%% (%sj) - %s",
            perf["performance_pct"],
            perf["days_held"],
            perf["status"]
        )
        if perf.get("current_price") is not None:
            logger.info(
                "prix: $%.8f -> $%.8f",
                perf["entry_price"],
                perf["current_price"]
            )
    else:
        logger.info("%s", perf.get("status"))

    if whale_details:
        logger.info("wallets du consensus:")
        for idx, wallet in enumerate(whale_details, 1):
            logger.info(
                "%s) %s | profil=%s | quality=%s | tier=%sk | roi=%s | winrate=%s | investi=$%s | tx=%s",
                idx,
                wallet.get("address", "N/A"),
                wallet.get("threshold_status", "N/A"),
                _fmt_float(wallet.get("quality_score"), 2),
                _fmt_float(wallet.get("optimal_threshold_tier"), 0),
                _fmt_pct(wallet.get("optimal_roi")),
                _fmt_pct(wallet.get("optimal_winrate")),
                _fmt_money(wallet.get("investment_usd")),
                wallet.get("transaction_count", "N/A")
            )
            logger.info(
                "   first_buy=%s | last_buy=%s",
                _fmt_datetime(wallet.get("first_buy_date")),
                _fmt_datetime(wallet.get("last_buy_date"))
            )

def run_live_consensus_detection():
    """Lance la détection de consensus en temps réel."""
    logger.info("CONSENSUS LIVE DETECTOR")
    logger.info("Analyse des %sj derniers jours", CONSENSUS_LIVE["PERIOD_DAYS"])
    logger.info("Consensus minimum: >=%s wallets", CONSENSUS_LIVE["MIN_WHALES_CONSENSUS"])

    smart_wallets = get_smart_wallets()
    if not smart_wallets:
        logger.warning("Aucun smart wallet trouvé")
        return []

    logger.info("%s smart wallets chargés", len(smart_wallets))
    logger.info("Récupération des transactions des %sj derniers jours...", CONSENSUS_LIVE["PERIOD_DAYS"])
    df_transactions = get_recent_transactions_live(smart_wallets)

    if df_transactions.empty:
        logger.warning("Aucune transaction qualifiée trouvée")
        return []

    logger.info("%s transactions qualifiées", len(df_transactions))
    logger.info("%s wallets actifs", df_transactions["wallet_address"].nunique())
    logger.info("%s tokens uniques", df_transactions["symbol"].nunique())

    existing = get_existing_consensus_from_db()
    if existing:
        logger.info("%s consensus déjà en BDD (ignorés)", len(existing))

    consensus_signals = detect_live_consensus(df_transactions, existing_consensus=existing)
    if not consensus_signals:
        logger.info(
            "Aucun consensus détecté sur %s tokens analysés",
            df_transactions["symbol"].nunique()
        )
        return []

    logger.info("%s consensus LIVE détectés", len(consensus_signals))

    for signal in consensus_signals:
        signal["performance"] = calculate_live_performance(signal)
        _log_signal(signal)

    save_live_consensus_to_db(consensus_signals)
    return consensus_signals

def main():
    """Point d'entrée principal."""
    logger.info("Lancement du Consensus Live Detector")
    logger.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    consensus_signals = run_live_consensus_detection()

    if consensus_signals:
        logger.info("Détection terminée avec succès")
        logger.info("%s consensus actifs détectés", len(consensus_signals))

        positive_signals = [
            s for s in consensus_signals
            if s.get("performance", {}).get("performance_pct", 0) > 0
        ]
        if positive_signals:
            avg_perf = sum(
                s["performance"]["performance_pct"] for s in positive_signals
            ) / len(positive_signals)
            logger.info(
                "%s/%s signaux positifs (moyenne: %+0.1f%%)",
                len(positive_signals),
                len(consensus_signals),
                avg_perf
            )
    else:
        logger.info("Aucun consensus actif détecté pour le moment")
        logger.info(
            "Prochaine vérification dans %sh",
            CONSENSUS_LIVE["UPDATE_INTERVAL_HOURS"]
        )

if __name__ == "__main__":
    main()
