"""Limiteur de débit partagé entre workers : fenêtre fixe d'une minute dans Redis."""

import time

import redis


class RateLimiter:
    def __init__(
        self,
        client: redis.Redis,
        name: str,
        per_minute: int,
        sleep=time.sleep,
        clock=time.time,
    ):
        self._client = client
        self._name = name
        self._per_minute = per_minute
        self._sleep = sleep
        self._clock = clock

    def acquire(self) -> None:
        """Bloque jusqu'à ce qu'une requête soit autorisée dans la fenêtre courante."""
        while True:
            now = self._clock()
            window = int(now // 60)
            key = f"ratelimit:{self._name}:{window}"
            count = self._client.incr(key)
            if count == 1:
                self._client.expire(key, 61)
            if count <= self._per_minute:
                return
            self._sleep((window + 1) * 60 - now)


class NoopLimiter:
    def acquire(self) -> None:
        return None
