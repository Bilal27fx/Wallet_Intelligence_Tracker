"""Détection d'explosion sur des bougies OHLCV. Fonctions pures, sans base ni réseau."""

from dataclasses import dataclass

from apps.discovery.services.settings import Thresholds
from integrations.geckoterminal import MAX_OHLCV_CANDLES, Candle

CONFIRMED = "confirmed"
REJECTED = "rejected"

PENDING = "pending"
HELD = "held"
RUG = "rug"

RESOLUTIONS = (("hour", 1), ("hour", 4), ("hour", 12))


@dataclass(frozen=True)
class Wave:
    trough: Candle
    peak: Candle
    multiplier: float
    score: float


@dataclass(frozen=True)
class Verdict:
    status: str
    reason: str = ""
    wave: Wave | None = None


def choose_resolution(pool_age_hours: float) -> tuple[str, int]:
    """Plus petite taille de bougie qui couvre toute la vie du pool en ≤ 1 000 bougies."""
    for timeframe, aggregate in RESOLUTIONS:
        if pool_age_hours <= aggregate * MAX_OHLCV_CANDLES:
            return timeframe, aggregate
    return "day", 1


def is_peak(closes: list[float], index: int) -> bool:
    """Début d'un sommet local : monte depuis la bougie précédente, ne remonte pas ensuite."""
    last = index == len(closes) - 1
    return (
        index > 0
        and closes[index] > closes[index - 1]
        and (last or closes[index] >= closes[index + 1])
    )


def find_trough(closes: list[float], peak_index: int, breakout_multiplier: float) -> int:
    """Dernier creux avant la montée finale vers le pic.

    En remontant depuis le pic, le creux recule vers chaque clôture plus basse, sauf si le prix
    a dépassé `breakout_multiplier` × cette clôture entre-temps : c'est alors une vague
    précédente, retombée, qui n'appartient pas à la montée finale.
    """
    trough = peak_index
    highest = 0.0
    for index in range(peak_index - 1, -1, -1):
        close = closes[index]
        if close <= 0:
            continue
        if close < closes[trough]:
            if highest > breakout_multiplier * close:
                break
            trough, highest = index, 0.0
        else:
            highest = max(highest, close)
    return trough


def maturity(age_seconds: int, maturity_hours: int) -> float:
    if maturity_hours <= 0:
        return 1.0
    return min(1.0, max(age_seconds, 0) / (maturity_hours * 3600))


def find_waves(
    candles: list[Candle], *, pool_created_ts: int, since_ts: int | None, thresholds: Thresholds
) -> list[Wave]:
    closes = [candle.close for candle in candles]
    waves = []
    for index, peak in enumerate(candles):
        if (since_ts is not None and peak.ts < since_ts) or not is_peak(closes, index):
            continue
        trough = candles[find_trough(closes, index, thresholds.breakout_multiplier)]
        if trough is peak or trough.close <= 0:
            continue
        multiplier = peak.close / trough.close
        weight = maturity(trough.ts - pool_created_ts, thresholds.maturity_hours)
        waves.append(Wave(trough, peak, round(multiplier, 2), round(multiplier * weight, 2)))
    return waves


def detect_explosion(
    candles: list[Candle],
    *,
    now_ts: int,
    pool_created_ts: int,
    current_liquidity_usd: float,
    thresholds: Thresholds,
    window_hours: int | None,
) -> Verdict:
    """Meilleure vague (score = multiplicateur × maturité) dont le pic est dans la fenêtre."""
    since_ts = None if window_hours is None else now_ts - window_hours * 3600
    waves = [
        wave
        for wave in find_waves(
            candles, pool_created_ts=pool_created_ts, since_ts=since_ts, thresholds=thresholds
        )
        if wave.multiplier >= thresholds.min_multiplier
        and (thresholds.max_multiplier <= 0 or wave.multiplier <= thresholds.max_multiplier)
    ]
    if not waves:
        return Verdict(REJECTED, "no_explosion")
    waves = [wave for wave in waves if wave.score >= thresholds.min_score]
    if not waves:
        return Verdict(REJECTED, "low_score")

    half_window = thresholds.peak_volume_window_hours * 3600 / 2
    waves = [
        wave
        for wave in waves
        if sum(c.volume for c in candles if abs(c.ts - wave.peak.ts) <= half_window)
        >= thresholds.min_volume_usd
    ]
    if not waves:
        return Verdict(REJECTED, "low_volume")
    if current_liquidity_usd < thresholds.min_liquidity_usd:
        return Verdict(REJECTED, "low_liquidity")
    return Verdict(CONFIRMED, wave=max(waves, key=lambda wave: (wave.score, wave.peak.ts)))


def measure_retention(
    candles: list[Candle], *, peak_ts: int, peak_price: float, now_ts: int, thresholds: Thresholds
) -> tuple[float | None, str]:
    """Clôture à pic + `confirmation_hours` rapportée au pic ; `pending` avant ce délai."""
    confirm_ts = peak_ts + thresholds.confirmation_hours * 3600
    if now_ts < confirm_ts or not candles or peak_price <= 0:
        return None, PENDING
    later = [candle for candle in candles if candle.ts >= confirm_ts]
    reference = later[0] if later else candles[-1]
    retention = round(reference.close / peak_price * 100, 2)
    return retention, HELD if retention >= thresholds.min_retention_pct else RUG
