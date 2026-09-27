"""Analyse d'un candidat : historique de prix, verdict d'explosion, conversion en blocs."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from apps.discovery.models import Candidate, Chain, Explosion, Token
from apps.discovery.services.blocks import find_block_at
from apps.discovery.services.candidates import SOURCE_MANUAL, upsert_token_and_pools
from apps.discovery.services.explosion import (
    REJECTED,
    choose_resolution,
    detect_explosion,
    measure_retention,
)
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

    try:
        history = fetch_price_history(gt, chain, token, now)
    except ValueError:
        return reject(candidate, "no_pool")

    created_at = history.main.created_at
    if created_at is not None:
        pool_created_ts = int(created_at.timestamp())
    else:
        pool_created_ts = history.candles[0].ts if history.candles else 0
    manual = SOURCE_MANUAL in candidate.sources
    verdict = detect_explosion(
        history.candles,
        now_ts=int(now.timestamp()),
        pool_created_ts=pool_created_ts,
        current_liquidity_usd=sum(pool.liquidity_usd for pool in history.pools),
        thresholds=thresholds,
        window_hours=None if manual else thresholds.explosion_window_hours,
    )
    if verdict.status == REJECTED:
        return reject(candidate, verdict.reason)

    wave = verdict.wave
    previous = (
        Explosion.objects.filter(candidate__token=token)
        .exclude(candidate=candidate)
        .order_by("-peak_at")
        .first()
    )
    if previous and previous.peak_at >= _datetime(wave.peak.ts):
        return reject(candidate, "already_extracted")

    hypersync = hypersync_for(chain)
    height = hypersync.height()
    trough_block = find_block_at(wave.trough.ts, 0, height, hypersync.block_timestamp)
    peak_block = find_block_at(wave.peak.ts, trough_block, height, hypersync.block_timestamp)
    created_by_address = {pool.address: pool.created_at for pool in history.pools}
    for pool in token.pools.filter(created_block__isnull=True):
        created_at = created_by_address.get(pool.address)
        if created_at is not None:
            pool.created_block = find_block_at(
                int(created_at.timestamp()), 0, trough_block, hypersync.block_timestamp
            )
            pool.save(update_fields=["created_block"])

    explosion, _ = Explosion.objects.update_or_create(
        candidate=candidate,
        defaults={
            "trough_block": trough_block,
            "trough_at": _datetime(wave.trough.ts),
            "peak_block": peak_block,
            "peak_at": _datetime(wave.peak.ts),
            "peak_price": wave.peak.close,
            "multiplier": wave.multiplier,
            "score": wave.score,
            "retention_pct": None,
            "retention_status": Explosion.Retention.PENDING,
        },
    )
    apply_retention(explosion, history.candles, thresholds, now)
    candidate.status = Candidate.Status.CONFIRMED
    candidate.next_check_at = None
    candidate.save(update_fields=["status", "next_check_at", "updated_at"])
    return candidate.status


def apply_retention(explosion: Explosion, candles: list[Candle], thresholds, now: datetime) -> str:
    retention, status = measure_retention(
        candles,
        peak_ts=int(explosion.peak_at.timestamp()),
        peak_price=explosion.peak_price or 0.0,
        now_ts=int(now.timestamp()),
        thresholds=thresholds,
    )
    explosion.retention_pct = retention
    explosion.retention_status = status
    explosion.save(update_fields=["retention_pct", "retention_status"])
    return status


def update_retention(explosion: Explosion, *, gt, now: datetime) -> str:
    """Mesure la rétention d'une explosion `pending` une fois le délai écoulé."""
    token = explosion.candidate.token
    thresholds = thresholds_for(token.chain)
    if now < explosion.peak_at + timedelta(hours=thresholds.confirmation_hours):
        return explosion.retention_status
    history = fetch_price_history(gt, token.chain, token, now)
    return apply_retention(explosion, history.candles, thresholds, now)
