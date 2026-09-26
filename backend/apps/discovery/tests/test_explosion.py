from dataclasses import replace

import pytest

from apps.discovery.services.explosion import (
    CONFIRMED,
    REJECTED,
    WAITING,
    choose_resolution,
    detect_explosion,
    find_best_run,
)
from apps.discovery.services.settings import Thresholds
from integrations.geckoterminal import Candle

HOUR = 3600
T = Thresholds(
    min_change_24h_pct=50,
    min_liquidity_usd=10_000,
    min_volume_usd=50_000,
    peak_volume_window_hours=24,
    min_fdv_usd=100_000,
    max_fdv_usd=100_000_000,
    max_pool_age_hours=720,
    min_multiplier=5,
    min_retention_pct=30,
    confirmation_hours=24,
    confirmation_timeout_hours=168,
    sniper_blocks=3,
    min_buy_usd=500,
    max_buyers=300,
)


def candles(closes: list[float], volume: float = 10_000) -> list[Candle]:
    return [Candle(i * HOUR, c, c, c, c, volume) for i, c in enumerate(closes)]


# Bas 0.5 à l'heure 2, pic 5.0 à l'heure 4 (×10), puis 30 heures à 3.0 (rétention 60 %).
EXPLOSIVE = [1.0, 0.8, 0.5, 2.0, 5.0] + [3.0] * 30
AFTER_ALL = 100 * HOUR


@pytest.mark.parametrize(
    ("age", "expected"),
    [
        (10, ("hour", 1)),
        (1000, ("hour", 1)),
        (1001, ("hour", 4)),
        (4001, ("hour", 12)),
        (12001, ("day", 1)),
    ],
)
def test_choose_resolution(age, expected):
    assert choose_resolution(age) == expected


def test_best_run_uses_lowest_close_before_peak():
    low, peak, multiplier = find_best_run(candles([2.0, 1.0, 4.0, 0.5, 1.5]))
    assert (low.close, peak.close, multiplier) == (1.0, 4.0, 4.0)


def test_best_run_none_when_price_only_falls():
    assert find_best_run(candles([5.0, 4.0, 3.0])) is None


def test_confirmed_explosion():
    verdict = detect_explosion(
        candles(EXPLOSIVE), now_ts=AFTER_ALL, current_liquidity_usd=50_000, thresholds=T
    )
    assert verdict.status == CONFIRMED
    assert verdict.signal.low_ts == 2 * HOUR
    assert verdict.signal.peak_ts == 4 * HOUR
    assert verdict.signal.multiplier == 10.0
    assert verdict.signal.retention_pct == 60.0


def test_rejects_small_move():
    verdict = detect_explosion(
        candles([1.0, 2.0, 3.0]), now_ts=AFTER_ALL, current_liquidity_usd=50_000, thresholds=T
    )
    assert (verdict.status, verdict.reason) == (REJECTED, "no_explosion")


def test_multiplier_exactly_at_threshold_passes():
    closes = [1.0, 5.0] + [5.0] * 30
    verdict = detect_explosion(
        candles(closes), now_ts=AFTER_ALL, current_liquidity_usd=50_000, thresholds=T
    )
    assert verdict.status == CONFIRMED


def test_rejects_low_volume_around_peak():
    verdict = detect_explosion(
        candles(EXPLOSIVE, volume=100), now_ts=AFTER_ALL, current_liquidity_usd=50_000, thresholds=T
    )
    assert (verdict.status, verdict.reason) == (REJECTED, "low_volume")


def test_rejects_drained_pool():
    verdict = detect_explosion(
        candles(EXPLOSIVE), now_ts=AFTER_ALL, current_liquidity_usd=500, thresholds=T
    )
    assert (verdict.status, verdict.reason) == (REJECTED, "low_liquidity")


def test_rejects_rug_after_peak():
    closes = [1.0, 0.5, 5.0] + [0.6] * 30
    verdict = detect_explosion(
        candles(closes), now_ts=AFTER_ALL, current_liquidity_usd=50_000, thresholds=T
    )
    assert (verdict.status, verdict.reason) == (REJECTED, "rug")
    assert verdict.signal.retention_pct == 12.0


def test_waits_when_peak_is_too_recent():
    verdict = detect_explosion(
        candles(EXPLOSIVE[:6]), now_ts=6 * HOUR, current_liquidity_usd=50_000, thresholds=T
    )
    assert verdict.status == WAITING
    assert verdict.next_check_ts == 4 * HOUR + 24 * HOUR
    assert verdict.signal.retention_pct is None


def test_threshold_override_changes_verdict():
    verdict = detect_explosion(
        candles([1.0, 3.0] + [3.0] * 30),
        now_ts=AFTER_ALL,
        current_liquidity_usd=50_000,
        thresholds=replace(T, min_multiplier=2),
    )
    assert verdict.status == CONFIRMED
