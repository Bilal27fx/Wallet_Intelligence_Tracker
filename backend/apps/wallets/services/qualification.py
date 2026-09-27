"""Qualification v2 : pré-filtre HyperSync multi-chaînes, historique Zerion, décision par wallet."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from django.db import transaction as db_transaction
from django.db.models import Max

from apps.discovery.models import Chain, EarlyBuyer
from apps.wallets.models import (
    KnownAddress,
    TokenPosition,
    TokenTrade,
    WalletLink,
    WalletProfile,
    WalletTransaction,
)
from apps.wallets.services.blocks import block_at
from apps.wallets.services.classify import classify_all
from apps.wallets.services.entities import (
    ZERO_ADDRESS,
    add_link,
    big_receive_targets,
    ensure_linked_profile,
    transfer_after_buy_targets,
)
from apps.wallets.services.exchanges import detect_exchange
from apps.wallets.services.filters import farmer_reason, mev_ratio, prefilter_reason
from apps.wallets.services.positions import TradeRecord, aggregate_positions
from apps.wallets.services.settings import qualification_thresholds
from apps.wallets.services.zerion_history import counterparty, movements

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


BATCH_SIZE = 1000


def _usd(value: float | None, places: int = 2) -> Decimal | None:
    return Decimal(str(round(value, places))) if value is not None else None


def save_transactions(wallet, transactions) -> None:
    """Transactions Zerion et leurs mouvements, mis à jour à la relance."""
    kept = [(tx, rows) for tx in transactions if (rows := movements(tx))]
    if not kept:
        return
    WalletTransaction.objects.bulk_create(
        [
            WalletTransaction(
                wallet=wallet,
                zerion_id=tx.zerion_id,
                chain=tx.chain,
                tx_hash=tx.tx_hash,
                block=tx.block,
                mined_at=tx.mined_at,
                operation_type=tx.operation_type,
                status=tx.status,
                fee_usd=_usd(tx.fee_usd, 6),
                raw=tx.raw,
            )
            for tx, _ in kept
        ],
        update_conflicts=True,
        unique_fields=["wallet", "zerion_id"],
        update_fields=[
            "chain",
            "tx_hash",
            "block",
            "mined_at",
            "operation_type",
            "status",
            "fee_usd",
            "raw",
        ],
        batch_size=BATCH_SIZE,
    )
    ids = dict(
        WalletTransaction.objects.filter(
            wallet=wallet, zerion_id__in=[tx.zerion_id for tx, _ in kept]
        ).values_list("zerion_id", "id")
    )
    TokenTrade.objects.bulk_create(
        [
            TokenTrade(
                transaction_id=ids[tx.zerion_id],
                wallet=wallet,
                transfer_index=t.index,
                chain=t.chain,
                token_address=t.token_address,
                token_symbol=t.token_symbol[:64],
                token_decimals=t.token_decimals,
                fungible_id=t.fungible_id[:100],
                kind=kind,
                direction=t.direction,
                quantity=t.quantity,
                amount=Decimal(t.amount),
                price_usd=Decimal(str(t.price_usd)) if t.price_usd is not None else None,
                value_usd=_usd(t.value_usd),
                counterparty=counterparty(t)[:42],
                block=tx.block,
                mined_at=tx.mined_at,
            )
            for tx, rows in kept
            for t, kind in rows
        ],
        update_conflicts=True,
        unique_fields=["transaction", "transfer_index"],
        update_fields=["kind", "quantity", "amount", "price_usd", "value_usd", "counterparty"],
        batch_size=BATCH_SIZE,
    )


def records_for(wallet) -> list[TradeRecord]:
    return [
        TradeRecord(
            chain_id=row.chain,
            token=row.token_address,
            kind=row.kind,
            amount=int(row.amount),
            usd=float(row.value_usd) if row.value_usd is not None else None,
            ts=int(row.mined_at.timestamp()),
            block=row.block or 0,
            counterparty=row.counterparty,
        )
        for row in TokenTrade.objects.filter(wallet=wallet)
    ]


def recompute_positions(wallet) -> None:
    stats = aggregate_positions(records_for(wallet))
    symbols = dict(
        TokenTrade.objects.filter(wallet=wallet)
        .values_list("token_address", "token_symbol")
        .distinct()
    )
    with db_transaction.atomic():
        TokenPosition.objects.filter(wallet=wallet).delete()
        TokenPosition.objects.bulk_create(
            [
                TokenPosition(
                    wallet=wallet,
                    chain=chain,
                    token_address=token,
                    token_symbol=symbols.get(token, ""),
                    bought_amount=Decimal(s.bought),
                    sold_amount=Decimal(s.sold),
                    sent_amount=Decimal(s.sent),
                    received_amount=Decimal(s.received),
                    bought_usd=_usd(s.bought_usd),
                    sold_usd=_usd(s.sold_usd),
                    buys=s.buys,
                    sells=s.sells,
                    first_at=datetime.fromtimestamp(s.first_ts, UTC),
                    last_at=datetime.fromtimestamp(s.last_ts, UTC),
                )
                for (chain, token), s in stats.items()
            ],
            batch_size=BATCH_SIZE,
        )


def history_step(profile: WalletProfile, clients: Clients, now: datetime) -> str:
    """Récupère l'historique Zerion jusqu'au bout ; le curseur est enregistré après chaque page."""
    wallet = profile.wallet
    t = qualification_thresholds()
    if profile.history_complete and profile.last_mined_at:
        since = profile.last_mined_at
    else:
        since = now - timedelta(days=t.history_days)
    cursor = profile.history_cursor or None
    while True:
        page = clients.zerion.transactions(wallet.address, since, cursor)
        save_transactions(wallet, page.transactions)
        cursor = page.next_cursor
        profile.history_cursor = cursor or ""
        profile.save(update_fields=["history_cursor"])
        if cursor is None:
            break
    profile.history_complete = True
    profile.last_mined_at = WalletTransaction.objects.filter(wallet=wallet).aggregate(
        latest=Max("mined_at")
    )["latest"]
    recompute_positions(wallet)
    profile.status = Status.HISTORY_FETCHED
    profile.save(update_fields=["history_complete", "last_mined_at", "status"])
    return profile.status


def link_step(profile: WalletProfile, clients: Clients, now: datetime, records) -> None:
    """Liens forts vers les wallets liés directs (après anti-exchange) + financement informatif."""
    t = qualification_thresholds()
    wallet = profile.wallet
    candidates = [
        (key, WalletLink.Kind.TRANSFER_AFTER_BUY, evidence, True)
        for key, evidence in transfer_after_buy_targets(records, t.transfer_after_buy_pct).items()
    ] + [
        (key, WalletLink.Kind.BIG_RECEIVE, evidence, False)
        for key, evidence in big_receive_targets(records, t.big_receive_pct).items()
    ]
    contexts: dict[int, tuple] = {}

    def context(chain):
        if chain.pk not in contexts:
            hypersync = clients.hypersync_for(chain)
            contexts[chain.pk] = (hypersync, hypersync.height())
        return contexts[chain.pk]

    for (chain_id, other), kind, evidence, outgoing in candidates:
        if other in ("", ZERO_ADDRESS, wallet.address):
            continue
        chain = Chain.objects.active().filter(zerion_id=chain_id).first()
        if chain is None:
            continue
        hypersync, height = context(chain)
        if detect_exchange(other, chain, hypersync, t, now, height):
            continue
        source, target = (wallet.address, other) if outgoing else (other, wallet.address)
        add_link(source, target, kind, {**evidence, "chain": chain_id})
        ensure_linked_profile(other)

    spotted = Chain.objects.active().filter(
        gt_id__in=EarlyBuyer.objects.filter(wallet=wallet).values_list(
            "explosion__candidate__token__chain__gt_id", flat=True
        )
    )
    for chain in spotted:
        hypersync, height = context(chain)
        funding = hypersync.first_funding(wallet.address, height)
        if funding and not KnownAddress.objects.blocking_for(funding.funder, [chain.pk]).exists():
            add_link(
                funding.funder,
                wallet.address,
                WalletLink.Kind.FUNDING,
                {"chain": chain.gt_id, "block": funding.block, "value": str(funding.value)},
            )
