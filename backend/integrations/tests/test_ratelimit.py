import uuid

import redis
from django.conf import settings

from integrations.ratelimit import NoopLimiter, RateLimiter


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


def test_allows_requests_under_the_limit():
    clock = FakeClock(now=600.0)
    limiter = make_limiter(2, clock)
    limiter.acquire()
    limiter.acquire()
    assert clock.sleeps == []


def test_waits_for_next_window_when_limit_reached():
    clock = FakeClock(now=610.0)
    limiter = make_limiter(2, clock)
    limiter.acquire()
    limiter.acquire()
    limiter.acquire()
    assert clock.sleeps == [50.0]


def test_noop_limiter_never_waits():
    NoopLimiter().acquire()
