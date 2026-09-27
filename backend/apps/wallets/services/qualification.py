"""Qualification d'un wallet par étapes idempotentes : filtres, historique, entités, tags."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from django.db import transaction

from apps.discovery.models import Chain, Token
from apps.discovery.services.blocks import find_block_at
from apps.wallets.models import KnownAddress, TokenPosition, TokenTrade, WalletProfile
from apps.wallets.services.classify import Trade, classify_all
from apps.wallets.services.filters import (
    ChainActivity,
    farmer_reason,
    history_reason,
    prefilter_reason,
)
from apps.wallets.services.positions import TradeRecord, aggregate_positions
from apps.wallets.services.pricing import (
    native_price_lookup,
    price_trades,
    quote_assets,
    quote_tokens,
)
from apps.wallets.services.settings import qualification_thresholds

Status = WalletProfile.Status
BATCH_SIZE = 1000
DAY = 86_400


@dataclass
class Clients:
    hypersync_for: Callable[[Chain], object]
    zerion: object
    rpc_for: Callable[[Chain], object]


def _dt(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, UTC)


def _decimal(value: float | None) -> Decimal | None:
    return Decimal(str(value)) if value is not None else None


def profile_chains(profile: WalletProfile) -> list[Chain]:
    return list(Chain.objects.active().filter(pk__in=profile.chains))


def block_at(hypersync, ts: int, height: int) -> int:
    return find_block_at(ts, 0, height, hypersync.block_timestamp)


def filter_out(profile: WalletProfile, reason: str, now: datetime) -> str:
    profile.status = Status.FILTERED
    profile.filter_reason = reason
    profile.analyzed_at = now
    profile.next_analysis_at = now + timedelta(days=qualification_thresholds().refilter_after_days)
    profile.save()
    return profile.status


def records_for(wallet) -> list[TradeRecord]:
    return [
        TradeRecord(
            chain_id=trade.token.chain_id,
            token=trade.token.address,
            kind=trade.kind,
            amount=int(trade.amount),
            usd=float(trade.usd) if trade.usd is not None else None,
            ts=int(trade.at.timestamp()),
            block=trade.block,
            counterparty=trade.counterparty,
        )
        for trade in TokenTrade.objects.filter(wallet=wallet).select_related("token")
    ]


def _tokens(chain: Chain, addresses: set[str]) -> dict[str, Token]:
    existing = set(
        Token.objects.filter(chain=chain, address__in=addresses).values_list("address", flat=True)
    )
    Token.objects.bulk_create(
        [Token(chain=chain, address=a) for a in addresses - existing],
        ignore_conflicts=True,
        batch_size=BATCH_SIZE,
    )
    return {t.address: t for t in Token.objects.filter(chain=chain, address__in=addresses)}


def save_trades(wallet, chain: Chain, trades: list[Trade], usd: dict) -> None:
    tokens = _tokens(chain, {trade.transfer.token for trade in trades})
    TokenTrade.objects.bulk_create(
        [
            TokenTrade(
                wallet=wallet,
                token=tokens[trade.transfer.token],
                kind=trade.kind,
                amount=Decimal(trade.transfer.amount),
                usd=_decimal(usd.get((trade.transfer.tx_hash, trade.transfer.log_index))),
                counterparty=trade.counterparty,
                block=trade.transfer.block,
                at=_dt(trade.transfer.timestamp),
                tx_hash=trade.transfer.tx_hash,
                log_index=trade.transfer.log_index,
            )
            for trade in trades
        ],
        ignore_conflicts=True,
        batch_size=BATCH_SIZE,
    )


def recompute_positions(wallet) -> None:
    stats = aggregate_positions(records_for(wallet))
    tokens = {
        (t.chain_id, t.address): t for t in Token.objects.filter(trades__wallet=wallet).distinct()
    }
    with transaction.atomic():
        TokenPosition.objects.filter(wallet=wallet).delete()
        TokenPosition.objects.bulk_create(
            [
                TokenPosition(
                    wallet=wallet,
                    token=tokens[key],
                    bought_amount=Decimal(s.bought),
                    sold_amount=Decimal(s.sold),
                    sent_amount=Decimal(s.sent),
                    received_amount=Decimal(s.received),
                    bought_usd=Decimal(str(round(s.bought_usd, 2))),
                    sold_usd=Decimal(str(round(s.sold_usd, 2))),
                    buys=s.buys,
                    sells=s.sells,
                    first_at=_dt(s.first_ts),
                    last_at=_dt(s.last_ts),
                )
                for key, s in stats.items()
            ],
            batch_size=BATCH_SIZE,
        )


def prefilter_step(profile: WalletProfile, clients: Clients, now: datetime) -> str:
    wallet = profile.wallet
    chains = profile_chains(profile)
    if not chains:
        return filter_out(profile, "no_chain", now)
    if KnownAddress.objects.blocking_for(wallet.address, [c.pk for c in chains]).exists():
        return filter_out(profile, "exchange", now)
    ts_now = int(now.timestamp())
    checks = []
    measures = {}
    for chain in chains:
        t = qualification_thresholds(chain)
        hypersync = clients.hypersync_for(chain)
        height = hypersync.height()
        week = block_at(hypersync, ts_now - 7 * DAY, height)
        active = block_at(hypersync, ts_now - t.inactive_days * DAY, height)
        activity = ChainActivity(
            txs_7d=hypersync.wallet_tx_count(
                wallet.address, week, height, cap=t.max_txs_per_day * 7 + 1
            ),
            txs_active=hypersync.wallet_tx_count(
                wallet.address, active, height, cap=t.min_txs_active
            ),
        )
        checks.append((activity, t))
        measures[chain.gt_id] = {"txs_7d": activity.txs_7d, "txs_active": activity.txs_active}
    profile.metrics = {**profile.metrics, "prefilter": measures}
    reason = prefilter_reason(checks, profile.source)
    if reason:
        return filter_out(profile, reason, now)
    profile.status = Status.PREFILTERED
    profile.save(update_fields=["status", "metrics"])
    return profile.status


def history_step(profile: WalletProfile, clients: Clients, now: datetime) -> str:
    wallet = profile.wallet
    chains = profile_chains(profile)
    distinct: dict[str, int] = {}
    for chain in chains:
        t = qualification_thresholds(chain)
        hypersync = clients.hypersync_for(chain)
        height = hypersync.height()
        start = block_at(hypersync, int(now.timestamp()) - t.history_days * DAY, height)
        transfers = hypersync.wallet_transfers(wallet.address, start, height)
        distinct[chain.gt_id] = len({x.token for x in transfers if x.recipient == wallet.address})
        reason = farmer_reason(distinct[chain.gt_id], t)
        if reason:
            profile.metrics = {**profile.metrics, "distinct_tokens": distinct}
            return filter_out(profile, reason, now)
        trades = classify_all(transfers, wallet.address)
        usd = price_trades(
            trades,
            wallet.address,
            quote_assets(chain),
            native_price_lookup(chain, clients.zerion, now),
        )
        save_trades(wallet, chain, trades, usd)
    recompute_positions(wallet)
    profile.metrics = {**profile.metrics, "distinct_tokens": distinct}
    reason = history_reason(
        records_for(wallet), quote_tokens(chains), qualification_thresholds(), profile.source
    )
    if reason:
        return filter_out(profile, reason, now)
    profile.status = Status.HISTORY_FETCHED
    profile.save(update_fields=["status", "metrics"])
    return profile.status
