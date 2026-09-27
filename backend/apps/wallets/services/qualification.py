"""Qualification v2 : pré-filtre HyperSync multi-chaînes, historique Zerion, décision par wallet."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

from apps.discovery.models import Chain, EarlyBuyer
from apps.wallets.models import KnownAddress, WalletProfile
from apps.wallets.services.blocks import block_at
from apps.wallets.services.classify import classify_all
from apps.wallets.services.filters import farmer_reason, mev_ratio, prefilter_reason
from apps.wallets.services.positions import TradeRecord
from apps.wallets.services.settings import qualification_thresholds

Status = WalletProfile.Status
Source = WalletProfile.Source
DAY = 86_400


@dataclass
class Clients:
    hypersync_for: Callable[[Chain], object]
    zerion: object


def filter_out(profile: WalletProfile, reason: str, now: datetime) -> str:
    profile.status = Status.FILTERED
    profile.filter_reason = reason
    profile.analyzed_at = now
    profile.next_analysis_at = now + timedelta(days=qualification_thresholds().refilter_after_days)
    profile.save()
    return profile.status


def prefilter_chains(profile: WalletProfile, cfg) -> list[Chain]:
    spotted = EarlyBuyer.objects.filter(wallet=profile.wallet).values_list(
        "explosion__candidate__token__chain__gt_id", flat=True
    )
    wanted = set(cfg.prefilter_chains) | set(spotted)
    return list(Chain.objects.active().filter(gt_id__in=wanted).order_by("gt_id"))


def prefilter_step(profile: WalletProfile, clients: Clients, now: datetime, cfg) -> str:
    """Bot, inactif, farmer, MEV et exchange, mesurés sur plusieurs chaînes via HyperSync."""
    wallet = profile.wallet
    t = qualification_thresholds()
    chains = prefilter_chains(profile, cfg)
    if not chains:
        return filter_out(profile, "no_chain", now)
    if KnownAddress.objects.blocking_for(wallet.address, [c.pk for c in chains]).exists():
        return filter_out(profile, "exchange", now)

    ts = int(now.timestamp())
    total_7d = total_active = 0
    received: set[tuple[str, str]] = set()
    records: list[TradeRecord] = []
    measures: dict[str, dict] = {}
    active: list[str] = []
    for chain in chains:
        hypersync = clients.hypersync_for(chain)
        height = hypersync.height()
        week = block_at(hypersync, chain, ts - 7 * DAY, height)
        recent = block_at(hypersync, chain, ts - t.inactive_days * DAY, height)
        start = block_at(hypersync, chain, ts - t.history_days * DAY, height)
        txs_7d = hypersync.wallet_tx_count(
            wallet.address, week, height, cap=t.max_txs_per_day * 7 + 1
        )
        txs_active = hypersync.wallet_tx_count(wallet.address, recent, height, cap=t.min_txs_active)
        total_7d += txs_7d
        total_active += txs_active
        measures[chain.gt_id] = {"txs_7d": txs_7d, "txs_active": txs_active}
        if total_7d > t.max_txs_per_day * 7:
            break
        transfers = hypersync.wallet_transfers(wallet.address, start, height)
        received |= {(chain.gt_id, x.token) for x in transfers if x.recipient == wallet.address}
        records += [
            TradeRecord(
                chain.gt_id,
                trade.transfer.token,
                trade.kind,
                trade.transfer.amount,
                None,
                trade.transfer.timestamp,
                trade.transfer.block,
                trade.counterparty,
            )
            for trade in classify_all(transfers, wallet.address)
        ]
        if txs_active or transfers:
            active.append(chain.gt_id)

    profile.metrics = {**profile.metrics, "prefilter": measures, "distinct_received": len(received)}
    profile.active_chains = active
    reason = (
        prefilter_reason(total_7d, total_active, t, profile.source)
        or farmer_reason(len(received), t)
        or ("bot_mev" if mev_ratio(records, set()) > t.max_mev_ratio else None)
    )
    if reason:
        return filter_out(profile, reason, now)
    profile.status = Status.PREFILTERED
    profile.save(update_fields=["status", "metrics", "active_chains"])
    return profile.status
