"""Analyse d'un candidat : historique de prix, verdict d'explosion, conversion en blocs."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from apps.discovery.models import Candidate, Chain, Explosion, Token
from apps.discovery.services.blocks import find_block_at
from apps.discovery.services.candidates import upsert_token_and_pools
from apps.discovery.services.explosion import REJECTED, WAITING, choose_resolution, detect_explosion
from apps.discovery.services.settings import thresholds_for
from integrations.geckoterminal import Candle, GtPool


@dataclass(frozen=True)
class PriceHistory:
    pools: list[GtPool]
    main: GtPool
    candles: list[Candle]


def _datetime(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=UTC)


def fetch_price_history(gt, chain: Chain, token: Token, now: datetime) -> PriceHistory:
    info = gt.token(chain.gt_id, token.address)
    token.symbol = info.symbol[:64]
    token.decimals = info.decimals
    token.save(update_fields=["symbol", "decimals"])
    pools = gt.token_pools(chain.gt_id, token.address)
    if not pools:
        raise ValueError("no_pool")
    main = max(pools, key=lambda pool: pool.liquidity_usd)
    upsert_token_and_pools(chain, [main] + [p for p in pools if p is not main])
    created_at = main.created_at or now - timedelta(days=3650)
    timeframe, aggregate = choose_resolution((now - created_at).total_seconds() / 3600)
    candles = gt.ohlcv(chain.gt_id, main.address, timeframe, aggregate)
    return PriceHistory(pools, main, candles)


def reject(candidate: Candidate, reason: str) -> str:
    candidate.status = Candidate.Status.REJECTED
    candidate.rejection_reason = reason
    candidate.save(update_fields=["status", "rejection_reason", "updated_at"])
    return candidate.status


def analyze_candidate(
    candidate: Candidate, *, gt, hypersync_for: Callable[[Chain], object], now: datetime
) -> str:
    token = candidate.token
    chain = token.chain
    if not chain.is_active:
        return reject(candidate, "chain_inactive")
    thresholds = thresholds_for(chain)
    timeout = candidate.created_at + timedelta(hours=thresholds.confirmation_timeout_hours)
    if candidate.status == Candidate.Status.WAITING_CONFIRMATION and timeout < now:
        return reject(candidate, "confirmation_timeout")

    try:
        history = fetch_price_history(gt, chain, token, now)
    except ValueError:
        return reject(candidate, "no_pool")

    verdict = detect_explosion(
        history.candles,
        now_ts=int(now.timestamp()),
        current_liquidity_usd=sum(pool.liquidity_usd for pool in history.pools),
        thresholds=thresholds,
    )
    if verdict.status == REJECTED:
        return reject(candidate, verdict.reason)

    signal = verdict.signal
    previous = (
        Explosion.objects.filter(candidate__token=token)
        .exclude(candidate=candidate)
        .order_by("-peak_at")
        .first()
    )
    if previous and previous.peak_at >= _datetime(signal.peak_ts):
        return reject(candidate, "already_extracted")

    if verdict.status == WAITING:
        candidate.status = Candidate.Status.WAITING_CONFIRMATION
        candidate.next_check_at = _datetime(verdict.next_check_ts)
        candidate.save(update_fields=["status", "next_check_at", "updated_at"])
        return candidate.status

    hypersync = hypersync_for(chain)
    height = hypersync.height()
    low_block = find_block_at(signal.low_ts, 0, height, hypersync.block_timestamp)
    peak_block = find_block_at(signal.peak_ts, low_block, height, hypersync.block_timestamp)
    created_by_address = {pool.address: pool.created_at for pool in history.pools}
    for pool in token.pools.filter(created_block__isnull=True):
        created_at = created_by_address.get(pool.address)
        if created_at is not None:
            pool.created_block = find_block_at(
                int(created_at.timestamp()), 0, low_block, hypersync.block_timestamp
            )
            pool.save(update_fields=["created_block"])

    Explosion.objects.update_or_create(
        candidate=candidate,
        defaults={
            "low_block": low_block,
            "low_at": _datetime(signal.low_ts),
            "peak_block": peak_block,
            "peak_at": _datetime(signal.peak_ts),
            "multiplier": signal.multiplier,
            "retention_pct": signal.retention_pct,
        },
    )
    candidate.status = Candidate.Status.CONFIRMED
    candidate.next_check_at = None
    candidate.save(update_fields=["status", "next_check_at", "updated_at"])
    return candidate.status
