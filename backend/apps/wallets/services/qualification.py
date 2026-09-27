"""Qualification d'un wallet par étapes idempotentes : filtres, historique, entités, tags."""

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from django.db import transaction

from apps.discovery.models import Chain, EarlyBuyer, Token
from apps.wallets.models import (
    Entity,
    KnownAddress,
    TokenPosition,
    TokenTrade,
    WalletLink,
    WalletProfile,
)
from apps.wallets.services.blocks import block_at
from apps.wallets.services.classify import Trade, classify_all
from apps.wallets.services.entities import (
    add_link,
    follow,
    funder_is_service,
    refresh_entity,
    transfer_after_buy_targets,
)
from apps.wallets.services.exchanges import detect_exchange
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
    wallet_value,
)
from apps.wallets.services.settings import qualification_thresholds
from apps.wallets.services.tags import EarlyBuy, entity_tags, wallet_tags

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
        week = block_at(hypersync, chain, ts_now - 7 * DAY, height)
        active = block_at(hypersync, chain, ts_now - t.inactive_days * DAY, height)
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
        start = block_at(hypersync, chain, int(now.timestamp()) - t.history_days * DAY, height)
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


PORTFOLIO_REASONS = ("portfolio_too_small", "portfolio_too_large")


def link_step(profile: WalletProfile, clients: Clients, now: datetime) -> None:
    t = qualification_thresholds()
    wallet = profile.wallet
    chains = {chain.pk: chain for chain in profile_chains(profile)}
    contexts: dict[int, tuple] = {}

    def context(chain):
        if chain.pk not in contexts:
            hypersync = clients.hypersync_for(chain)
            contexts[chain.pk] = (hypersync, hypersync.height())
        return contexts[chain.pk]

    targets = transfer_after_buy_targets(records_for(wallet), t.transfer_after_buy_pct)
    for (chain_id, destination), evidence in targets.items():
        chain = chains.get(chain_id)
        if chain is None:
            continue
        hypersync, height = context(chain)
        if detect_exchange(destination, chain, hypersync, t, now, height):
            continue
        add_link(
            wallet.address,
            destination,
            WalletLink.Kind.TRANSFER_AFTER_BUY,
            {**evidence, "chain": chain.gt_id},
        )
        follow(destination, profile, chain, t)

    for chain in chains.values():
        hypersync, height = context(chain)
        funding = hypersync.first_funding(wallet.address, height)
        if funding is None:
            continue
        if funder_is_service(funding.funder, chain, t) or detect_exchange(
            funding.funder, chain, hypersync, t, now, height
        ):
            continue
        add_link(
            funding.funder,
            wallet.address,
            WalletLink.Kind.FUNDING,
            {"chain": chain.gt_id, "block": funding.block, "value": str(funding.value)},
        )
        follow(funding.funder, profile, chain, t)


def early_buys(wallet) -> list[EarlyBuy]:
    return [
        EarlyBuy(
            token=e.explosion.candidate.token.address,
            chain_id=e.explosion.candidate.token.chain_id,
            is_sniper=e.is_sniper,
            bought=int(e.bought_amount),
            sold_before_peak=int(e.sold_amount),
        )
        for e in EarlyBuyer.objects.filter(wallet=wallet).select_related(
            "explosion__candidate__token"
        )
    ]


def evaluate_entity(entity: Entity) -> None:
    """Valeur et tags de l'entité, puis statut de ses wallets déjà valorisés."""
    t = qualification_thresholds()
    profiles = list(entity.profiles.all())
    value = sum(float(p.portfolio_value_usd) for p in profiles if p.portfolio_value_usd is not None)
    members = {p.wallet_id for p in profiles}
    explosive = list(
        EarlyBuyer.objects.filter(wallet_id__in=members).values_list(
            "explosion__candidate__token__address", flat=True
        )
    )
    internal_holding = WalletLink.objects.filter(
        kind=WalletLink.Kind.TRANSFER_AFTER_BUY,
        from_wallet_id__in=members,
        to_wallet_id__in=members,
        evidence__token__in=explosive,
    ).exists()
    entity.portfolio_value_usd = Decimal(str(round(value, 2)))
    entity.tags = entity_tags([p.tags for p in profiles], internal_holding)
    entity.save()

    if value < t.min_portfolio_usd:
        status, reason = Status.FILTERED, "portfolio_too_small"
    elif value > t.max_portfolio_usd:
        status, reason = Status.FILTERED, "portfolio_too_large"
    else:
        status, reason = Status.QUALIFIED, ""
    for p in profiles:
        judged = p.portfolio_value_usd is not None and (
            p.status in (Status.HISTORY_FETCHED, Status.QUALIFIED)
            or (p.status == Status.FILTERED and p.filter_reason in PORTFOLIO_REASONS)
        )
        if judged and (p.status, p.filter_reason) != (status, reason):
            p.status = status
            p.filter_reason = reason
            p.save(update_fields=["status", "filter_reason"])


def complete_step(profile: WalletProfile, clients: Clients, now: datetime) -> str:
    link_step(profile, clients, now)
    chains = profile_chains(profile)
    total, details = wallet_value(profile.wallet, chains, clients.zerion, clients.rpc_for, now)
    profile.portfolio_value_usd = Decimal(str(total))
    profile.metrics = {**profile.metrics, "value": details}
    profile.tags = wallet_tags(
        records_for(profile.wallet),
        early_buys(profile.wallet),
        quote_tokens(chains),
        qualification_thresholds(),
    )
    profile.analyzed_at = now
    profile.attempts = 0
    profile.save()
    evaluate_entity(refresh_entity(profile.wallet))
    profile.refresh_from_db()
    return profile.status


def qualify_wallet(profile: WalletProfile, clients: Clients, now: datetime) -> str:
    if profile.status == Status.PENDING:
        prefilter_step(profile, clients, now)
    if profile.status == Status.PREFILTERED:
        history_step(profile, clients, now)
    if profile.status == Status.HISTORY_FETCHED:
        complete_step(profile, clients, now)
    return profile.status


def enqueue_profiles(now: datetime, cfg) -> int:
    """Profils des nouveaux early buyers ; rouvre les filtrés ayant une nouvelle explosion."""
    extra = set(
        Chain.objects.active().filter(gt_id__in=cfg.extra_chains).values_list("pk", flat=True)
    )
    by_wallet: dict[int, set[int]] = defaultdict(set)
    rows = EarlyBuyer.objects.filter(wallet__profile__isnull=True).values_list(
        "wallet_id", "explosion__candidate__token__chain_id"
    )
    for wallet_id, chain_id in rows:
        by_wallet[wallet_id].add(chain_id)
    created = 0
    for wallet_id, chain_ids in by_wallet.items():
        _, was_created = WalletProfile.objects.get_or_create(
            wallet_id=wallet_id, defaults={"chains": sorted(chain_ids | extra)}
        )
        created += int(was_created)

    due = WalletProfile.objects.filter(
        status=Status.FILTERED, source=WalletProfile.Source.EARLY_BUYER, next_analysis_at__lte=now
    )
    for profile in due:
        fresh = set(
            EarlyBuyer.objects.filter(
                wallet_id=profile.wallet_id,
                explosion__candidate__updated_at__gt=profile.analyzed_at,
            ).values_list("explosion__candidate__token__chain_id", flat=True)
        )
        if fresh:
            profile.status = Status.PENDING
            profile.filter_reason = ""
            profile.attempts = 0
            profile.chains = sorted(set(profile.chains) | fresh | extra)
            profile.save()
    return created
