from dataclasses import replace

import pytest

from apps.discovery.services.explosion import (
    CONFIRMED,
    HELD,
    PENDING,
    REJECTED,
    RUG,
    choose_resolution,
    detect_explosion,
    find_trough,
    measure_retention,
)
from apps.discovery.services.settings import Thresholds
from integrations.geckoterminal import Candle

HOUR = 3600
DAY = 24 * HOUR
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
    sniper_blocks=3,
    min_buy_usd=500,
    max_buyers=300,
    explosion_window_hours=1000,
    maturity_hours=0,
    breakout_multiplier=2,
    buyer_window_hours=0,
)


def candles(closes: list[float], volume: float = 10_000, step: int = HOUR) -> list[Candle]:
    return [Candle(i * step, c, c, c, c, volume) for i, c in enumerate(closes)]


def detect(
    closes,
    now_ts=100 * HOUR,
    liquidity=50_000,
    window_hours=None,
    step=HOUR,
    volume=10_000,
    **overrides,
):
    return detect_explosion(
        candles(closes, volume, step),
        now_ts=now_ts,
        pool_created_ts=0,
        current_liquidity_usd=liquidity,
        thresholds=replace(T, **overrides),
        window_hours=window_hours,
    )


# Bas 0.5 à l'heure 2, pic 5.0 à l'heure 4 (×10), puis 30 heures à 3.0 (rétention 60 %).
EXPLOSIVE = [1.0, 0.8, 0.5, 2.0, 5.0] + [3.0] * 30


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


def test_trough_skips_back_over_small_rebound():
    # 0.2 → rebond à 0.35 (< ×2) → 0.3 → montée : le creux reste 0.2.
    assert find_trough([1.0, 0.2, 0.35, 0.3, 1.0, 2.0], 5, 2) == 1


def test_trough_stops_at_previous_wave():
    # 0.2 → vague à 0.6 (×3) retombée à 0.3 → montée : le creux est 0.3 (cas AI).
    assert find_trough([1.0, 1.0, 0.2, 0.6, 0.4, 0.3, 1.0, 2.0, 3.0], 8, 2) == 5


def test_confirmed_explosion():
    verdict = detect(EXPLOSIVE)
    assert verdict.status == CONFIRMED
    wave = verdict.wave
    assert (wave.trough.ts, wave.peak.ts, wave.multiplier, wave.score) == (
        2 * HOUR,
        4 * HOUR,
        10.0,
        10.0,
    )


def test_last_trough_before_final_rise_wins():
    closes = [1.0, 1.0, 0.2, 0.6, 0.4, 0.3, 1.0, 2.0, 3.0] + [2.5] * 30
    wave = detect(closes).wave
    assert (wave.trough.ts, wave.multiplier) == (5 * HOUR, 10.0)


def test_mature_wave_beats_bigger_launch_wave():
    # ×50 au jour 2, puis ×10 au jour 33 ; maturité 14 jours.
    closes = [1.0, 1.0, 0.1, 5.0] + [2.0] * 29 + [1.0, 10.0] + [8.0] * 3
    verdict = detect(closes, now_ts=40 * DAY, step=DAY, volume=100_000, maturity_hours=336)
    assert (verdict.wave.trough.ts, verdict.wave.multiplier, verdict.wave.score) == (
        33 * DAY,
        10.0,
        10.0,
    )
    unweighted = detect(closes, now_ts=40 * DAY, step=DAY, volume=100_000)
    assert unweighted.wave.multiplier == 50.0


def test_rejects_small_move():
    verdict = detect([1.0, 2.0, 3.0])
    assert (verdict.status, verdict.reason) == (REJECTED, "no_explosion")


def test_multiplier_exactly_at_threshold_passes():
    assert detect([1.0, 5.0] + [5.0] * 30).status == CONFIRMED


def test_rejects_low_volume_around_peak():
    assert detect(EXPLOSIVE, volume=100).reason == "low_volume"


def test_rejects_drained_pool():
    assert detect(EXPLOSIVE, liquidity=500).reason == "low_liquidity"


def test_threshold_override_changes_verdict():
    assert detect([1.0, 3.0] + [3.0] * 30, min_multiplier=2).status == CONFIRMED


def test_peak_outside_window_is_rejected():
    closes = [1.0, 0.5, 5.0] + [3.0] * 200
    verdict = detect(closes, now_ts=203 * HOUR, window_hours=72)
    assert (verdict.status, verdict.reason) == (REJECTED, "no_explosion")


def test_manual_candidate_has_no_window():
    closes = [1.0, 0.5, 5.0] + [3.0] * 200
    assert detect(closes, now_ts=203 * HOUR, window_hours=None).wave.multiplier == 10.0


def test_recent_explosion_wins_over_bigger_old_one():
    # Vieille explosion ×20, puis explosion récente ×6 dans les 72 dernières heures.
    closes = [1.0, 0.25, 5.0] + [1.0] * 100 + [0.5, 3.0] + [2.0] * 30
    verdict = detect(closes, now_ts=len(closes) * HOUR, window_hours=72)
    assert (verdict.wave.multiplier, verdict.wave.trough.ts) == (6.0, 103 * HOUR)


def test_retention_pending_before_confirmation_delay():
    retention = measure_retention(
        candles(EXPLOSIVE), peak_ts=4 * HOUR, peak_price=5.0, now_ts=10 * HOUR, thresholds=T
    )
    assert retention == (None, PENDING)


def test_retention_held_and_rug():
    held = measure_retention(
        candles(EXPLOSIVE), peak_ts=4 * HOUR, peak_price=5.0, now_ts=100 * HOUR, thresholds=T
    )
    rug = measure_retention(
        candles([1.0, 0.5, 5.0] + [0.6] * 30),
        peak_ts=2 * HOUR,
        peak_price=5.0,
        now_ts=100 * HOUR,
        thresholds=T,
    )
    assert held == (60.0, HELD)
    assert rug == (12.0, RUG)
