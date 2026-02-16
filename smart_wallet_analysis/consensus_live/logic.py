#!/usr/bin/env python3
"""Logique de consensus live."""

import time
from datetime import datetime, timezone
from smart_wallet_analysis.config import CONSENSUS_LIVE
from smart_wallet_analysis.consensus_live.io import (
    get_token_info_dexscreener,
    get_current_price_dexscreener,
)

def _is_exceptional_status(status):
    """Retourne True si le statut wallet est excellent/exceptionnel."""
    normalized = str(status or "").strip().upper()
    return normalized in {"EXCEPTIONAL", "EXCELLENT"} or "EXCEPTIONAL" in normalized or "EXCELLENT" in normalized

def _get_signal_type(exceptional_count, normal_count):
    """Détermine le type de consensus."""
    if exceptional_count >= 1 and normal_count >= 1:
        return "MIXED_CONSENSUS"
    if exceptional_count >= 1:
        return "EXCEPTIONAL_CONSENSUS"
    return "INVALID_CONSENSUS"

def _build_whale_details(token_group, wallet_sums):
    """Construit les détails des whales."""
    details = []
    for wallet_addr, wallet_data in wallet_sums.iterrows():
        wallet_txs = token_group[token_group["wallet_address"] == wallet_addr]
        details.append({
            "address": wallet_addr,
            "optimal_threshold_tier": wallet_data["optimal_threshold_tier"],
            "quality_score": wallet_data["quality_score"],
            "threshold_status": wallet_data["threshold_status"],
            "optimal_roi": wallet_data["optimal_roi"],
            "optimal_winrate": wallet_data["optimal_winrate"],
            "investment_usd": wallet_txs["investment_usd"].sum(),
            "transaction_count": len(wallet_txs),
            "first_buy_date": wallet_txs["date"].min(),
            "last_buy_date": wallet_txs["date"].max()
        })

    details.sort(key=lambda x: (not _is_exceptional_status(x["threshold_status"]), -x["investment_usd"]))
    return details

def _build_detection_context(token_group, wallet_sums, min_whales):
    """Construit le contexte de formation du consensus."""
    timeline = token_group.sort_values("date")[["wallet_address", "date"]]
    wallet_first_seen = {}
    formation_log = []
    detection_date = None
    detection_trigger_wallet = None
    detection_wallets = []

    for _, row in timeline.iterrows():
        wallet_address = row["wallet_address"]
        tx_date = row["date"]
        if wallet_address in wallet_first_seen:
            continue

        wallet_first_seen[wallet_address] = tx_date
        wallet_data = wallet_sums.loc[wallet_address]
        wallet_rank = len(wallet_first_seen)
        is_detection_step = wallet_rank == min_whales and detection_date is None

        if is_detection_step:
            detection_date = tx_date
            detection_trigger_wallet = wallet_address
            detection_wallets = list(wallet_first_seen.keys())[:min_whales]

        formation_log.append({
            "rank": wallet_rank,
            "wallet_address": wallet_address,
            "first_buy_date": tx_date,
            "optimal_threshold_tier": wallet_data["optimal_threshold_tier"],
            "threshold_status": wallet_data["threshold_status"],
            "quality_score": wallet_data["quality_score"],
            "optimal_roi": wallet_data["optimal_roi"],
            "optimal_winrate": wallet_data["optimal_winrate"],
            "investment_usd": wallet_data["investment_usd"],
            "is_detection_step": is_detection_step
        })

    if detection_date is None:
        detection_date = timeline["date"].max()
        detection_wallets = list(wallet_first_seen.keys())[:min_whales]
        if detection_wallets:
            detection_trigger_wallet = detection_wallets[-1]

    return {
        "detection_date": detection_date,
        "detection_wallets": detection_wallets,
        "detection_trigger_wallet": detection_trigger_wallet,
        "formation_log": formation_log
    }

def detect_live_consensus(df_transactions, existing_consensus=None):
    """Détecte les consensus actuels dans la période."""
    if df_transactions.empty:
        return []

    existing_consensus = existing_consensus or set()
    signals_detected = []

    for symbol, token_group in df_transactions.groupby("symbol"):
        token_group = token_group.sort_values("date")
        contract_address = token_group["contract_address"].iloc[0]

        if (symbol, contract_address) in existing_consensus:
            continue

        token_info = get_token_info_dexscreener(contract_address)
        if not token_info:
            continue

        market_cap = token_info.get("market_cap", 0)
        if market_cap < CONSENSUS_LIVE["MIN_MARKET_CAP"] or market_cap > CONSENSUS_LIVE["MAX_MARKET_CAP"]:
            continue

        wallet_sums = token_group.groupby("wallet_address").agg({
            "investment_usd": "sum",
            "optimal_threshold_tier": "first",
            "quality_score": "first",
            "threshold_status": "first",
            "optimal_roi": "first",
            "optimal_winrate": "first"
        })

        thresholds = wallet_sums["optimal_threshold_tier"] * 1000
        qualified_wallets = wallet_sums[wallet_sums["investment_usd"] >= thresholds]

        status_series = qualified_wallets["threshold_status"].astype(str).str.upper()
        exceptional_count = status_series.apply(_is_exceptional_status).sum()
        normal_count = len(qualified_wallets) - exceptional_count
        unique_whales = len(qualified_wallets)

        if unique_whales < CONSENSUS_LIVE["MIN_WHALES_CONSENSUS"]:
            continue
        if exceptional_count < 1:
            continue

        qualified_addresses = qualified_wallets.index.tolist()
        qualified_token_group = token_group[token_group["wallet_address"].isin(qualified_addresses)]
        if qualified_token_group.empty:
            continue

        signal_type = _get_signal_type(exceptional_count, normal_count)
        avg_entry_price = (
            (qualified_token_group["investment_usd"] * qualified_token_group["price_per_token"]).sum()
            / qualified_token_group["investment_usd"].sum()
        )
        detection_context = _build_detection_context(
            qualified_token_group,
            qualified_wallets,
            CONSENSUS_LIVE["MIN_WHALES_CONSENSUS"]
        )

        signal_data = {
            "symbol": symbol,
            "contract_address": contract_address,
            "detection_date": detection_context["detection_date"],
            "period_start": qualified_token_group["date"].min(),
            "period_end": qualified_token_group["date"].max(),
            "whale_count": unique_whales,
            "exceptional_count": exceptional_count,
            "normal_count": normal_count,
            "signal_type": signal_type,
            "total_investment": qualified_wallets["investment_usd"].sum(),
            "avg_entry_price": avg_entry_price,
            "transactions": qualified_token_group,
            "whale_details": _build_whale_details(qualified_token_group, qualified_wallets),
            "detection_wallets": detection_context["detection_wallets"],
            "detection_trigger_wallet": detection_context["detection_trigger_wallet"],
            "formation_log": detection_context["formation_log"],
            "token_info": token_info
        }

        signals_detected.append(signal_data)
        time.sleep(CONSENSUS_LIVE["PRICE_CHECK_DELAY"])

    return signals_detected

def _performance_status(performance_pct):
    """Retourne le statut en fonction de la performance."""
    thresholds = CONSENSUS_LIVE["PERFORMANCE_THRESHOLDS"]
    if performance_pct >= thresholds["MOON_SHOT"]:
        return "🚀 MOON SHOT"
    if performance_pct >= thresholds["EXCELLENT"]:
        return "🌟 EXCELLENT"
    if performance_pct >= thresholds["TRES_BON"]:
        return "💚 TRÈS BON"
    if performance_pct >= thresholds["BON"]:
        return "📈 BON"
    if performance_pct >= thresholds["POSITIF"]:
        return "🟡 POSITIF"
    if performance_pct >= thresholds["NEGATIF"]:
        return "📉 NÉGATIF"
    return "🔴 TRÈS NÉGATIF"

def calculate_live_performance(consensus_data):
    """Calcule la performance actuelle d'un consensus."""
    symbol = consensus_data["symbol"]
    contract_address = consensus_data["contract_address"]
    avg_entry_price = consensus_data["avg_entry_price"]
    consensus_formation_date = consensus_data["detection_date"]

    if not contract_address or avg_entry_price <= 0:
        return {
            "symbol": symbol,
            "entry_price": avg_entry_price,
            "current_price": None,
            "performance_pct": None,
            "days_held": (datetime.now(timezone.utc) - consensus_formation_date).days,
            "status": "DONNÉES_INSUFFISANTES"
        }

    current_price = get_current_price_dexscreener(contract_address)
    days_held = (datetime.now(timezone.utc) - consensus_formation_date).days

    if current_price:
        performance_pct = ((current_price - avg_entry_price) / avg_entry_price) * 100
        return {
            "symbol": symbol,
            "entry_price": avg_entry_price,
            "current_price": current_price,
            "performance_pct": performance_pct,
            "days_held": days_held,
            "status": _performance_status(performance_pct),
            "annualized_return": performance_pct / max(days_held, 1) * 365
        }

    return {
        "symbol": symbol,
        "entry_price": avg_entry_price,
        "current_price": None,
        "performance_pct": None,
        "days_held": days_held,
        "status": "PRIX_NON_DISPONIBLE"
    }
