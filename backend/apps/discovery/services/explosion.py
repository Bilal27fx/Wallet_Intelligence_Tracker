"""Détection d'explosion sur des bougies OHLCV. Fonctions pures, sans base ni réseau."""

from dataclasses import dataclass

from apps.discovery.services.settings import Thresholds
from integrations.geckoterminal import MAX_OHLCV_CANDLES, Candle

CONFIRMED = "confirmed"
WAITING = "waiting"
REJECTED = "rejected"

RESOLUTIONS = (("hour", 1), ("hour", 4), ("hour", 12))


@dataclass(frozen=True)
class ExplosionSignal:
    low_ts: int
    peak_ts: int
    multiplier: float
    retention_pct: float | None


@dataclass(frozen=True)
class Verdict:
    status: str
    reason: str = ""
    signal: ExplosionSignal | None = None
    next_check_ts: int | None = None


def choose_resolution(pool_age_hours: float) -> tuple[str, int]:
    """Plus petite taille de bougie qui couvre toute la vie du pool en ≤ 1 000 bougies."""
    for timeframe, aggregate in RESOLUTIONS:
        if pool_age_hours <= aggregate * MAX_OHLCV_CANDLES:
            return timeframe, aggregate
    return "day", 1


def find_best_run(candles: list[Candle]) -> tuple[Candle, Candle, float] | None:
    """Meilleur rapport clôture du pic / plus bas précédent, en un seul passage."""
    best = None
    low = None
    for candle in candles:
        if candle.close <= 0:
            continue
        if low is None or candle.close < low.close:
            low = candle
            continue
        ratio = candle.close / low.close
        if best is None or ratio > best[2]:
            best = (low, candle, ratio)
    return best


def detect_explosion(
    candles: list[Candle], *, now_ts: int, current_liquidity_usd: float, thresholds: Thresholds
) -> Verdict:
    run = find_best_run(candles)
    if run is None or run[2] < thresholds.min_multiplier:
        return Verdict(REJECTED, "no_explosion")
    low, peak, multiplier = run

    half_window = thresholds.peak_volume_window_hours * 3600 / 2
    volume = sum(c.volume for c in candles if abs(c.ts - peak.ts) <= half_window)
    if volume < thresholds.min_volume_usd:
        return Verdict(REJECTED, "low_volume")
    if current_liquidity_usd < thresholds.min_liquidity_usd:
        return Verdict(REJECTED, "low_liquidity")

    confirm_ts = peak.ts + thresholds.confirmation_hours * 3600
    if now_ts < confirm_ts:
        signal = ExplosionSignal(low.ts, peak.ts, round(multiplier, 2), None)
        return Verdict(WAITING, signal=signal, next_check_ts=confirm_ts)

    later = [c for c in candles if c.ts >= confirm_ts]
    reference = later[0] if later else candles[-1]
    retention = round(reference.close / peak.close * 100, 2)
    signal = ExplosionSignal(low.ts, peak.ts, round(multiplier, 2), retention)
    if retention < thresholds.min_retention_pct:
        return Verdict(REJECTED, "rug", signal=signal)
    return Verdict(CONFIRMED, signal=signal)
