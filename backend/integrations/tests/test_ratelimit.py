import uuid

import pytest
import redis
from django.conf import settings

from integrations.errors import BudgetExhausted
from integrations.ratelimit import DailyBudget, NoBudget, NoopLimiter, RateLimiter


class FakeClock:
    def __init__(self, now: float):
        self.now = now
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def make_limiter(per_minute: int, clock: FakeClock) -> RateLimiter:
    client = redis.Redis.from_url(settings.REDIS_URL)
    return RateLimiter(
        client, f"test-{uuid.uuid4()}", per_minute, sleep=clock.sleep, clock=clock.time
    )


def test_first_request_does_not_wait():
    clock = FakeClock(now=600.0)
    make_limiter(30, clock).acquire()
    assert clock.sleeps == []


def test_requests_are_evenly_spaced():
    clock = FakeClock(now=610.0)
    limiter = make_limiter(2, clock)
    for _ in range(3):
        limiter.acquire()
    assert clock.sleeps == [30.0, 30.0]


def test_no_burst_across_minute_boundary():
    # Une limite par minute fixe laissait passer 2 × la limite autour du changement de minute.
    clock = FakeClock(now=659.0)
    limiter = make_limiter(2, clock)
    limiter.acquire()
    clock.now = 660.5
    limiter.acquire()
    assert clock.sleeps == [28.5]


def test_limiters_with_same_name_share_the_pace():
    clock = FakeClock(now=700.0)
    client = redis.Redis.from_url(settings.REDIS_URL)
    name = f"test-{uuid.uuid4()}"
    first = RateLimiter(client, name, 60, sleep=clock.sleep, clock=clock.time)
    second = RateLimiter(client, name, 60, sleep=clock.sleep, clock=clock.time)
    first.acquire()
    second.acquire()
    assert clock.sleeps == [1.0]


def test_noop_limiter_never_waits():
    NoopLimiter().acquire()


def make_budget(per_day: int, clock: FakeClock) -> DailyBudget:
    client = redis.Redis.from_url(settings.REDIS_URL)
    return DailyBudget(client, f"test-{uuid.uuid4()}", per_day, clock=clock.time)


def test_budget_allows_up_to_limit_then_raises():
    clock = FakeClock(now=86_400 * 100 + 10)
    budget = make_budget(2, clock)
    budget.consume()
    budget.consume()
    assert budget.remaining() == 0
    with pytest.raises(BudgetExhausted):
        budget.consume()


def test_budget_resets_the_next_day():
    clock = FakeClock(now=86_400 * 200 + 10)
    budget = make_budget(1, clock)
    budget.consume()
    clock.now += 86_400
    budget.consume()
    assert budget.remaining() == 0


def test_no_budget_never_raises():
    for _ in range(5):
        NoBudget().consume()
