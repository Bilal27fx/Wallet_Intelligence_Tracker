"""Limiteur de débit partagé entre workers : requêtes espacées régulièrement via Redis."""

import time

import redis

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
