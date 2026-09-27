"""Limiteur de débit partagé entre workers : requêtes espacées régulièrement via Redis."""

import time

import redis

from integrations.errors import BudgetExhausted

# Réserve atomiquement le prochain créneau libre et renvoie l'attente nécessaire (secondes).
_RESERVE_SLOT = """
local now = tonumber(ARGV[1])
local interval = tonumber(ARGV[2])
local next_free = tonumber(redis.call('GET', KEYS[1]) or '0')
local slot = math.max(now, next_free)
redis.call('SET', KEYS[1], tostring(slot + interval), 'EX', 3600)
return tostring(slot - now)
"""


class RateLimiter:
    def __init__(
        self,
        client: redis.Redis,
        name: str,
        per_minute: int,
        sleep=time.sleep,
        clock=time.time,
    ):
        self._reserve = client.register_script(_RESERVE_SLOT)
        self._key = f"ratelimit:{name}"
        self._interval = 60 / per_minute
        self._sleep = sleep
        self._clock = clock

    def acquire(self) -> None:
        """Attend son créneau : au plus `per_minute` requêtes par minute glissante, sans rafale."""
        wait = float(self._reserve(keys=[self._key], args=[self._clock(), self._interval]))
        if wait > 0:
            self._sleep(wait)


class NoopLimiter:
    def acquire(self) -> None:
        return None


class DailyBudget:
    """Nombre maximal de requêtes par jour UTC, partagé entre workers (compteur Redis)."""

    def __init__(self, client: redis.Redis, name: str, per_day: int, clock=time.time):
        self._client = client
        self._name = name
        self._per_day = per_day
        self._clock = clock

    def _key(self) -> str:
        return f"budget:{self._name}:{int(self._clock() // 86_400)}"

    def consume(self) -> None:
        key = self._key()
        used = self._client.incr(key)
        if used == 1:
            self._client.expire(key, 2 * 86_400)
        if used > self._per_day:
            raise BudgetExhausted(f"{self._name} : budget de {self._per_day} requêtes/jour atteint")

    def remaining(self) -> int:
        used = int(self._client.get(self._key()) or 0)
        return max(self._per_day - used, 0)


class NoBudget:
    def consume(self) -> None:
        return None
