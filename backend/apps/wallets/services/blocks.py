"""Date → bloc avec cache Redis partagé entre workers : un bloc passé ne change jamais."""

import redis
from django.conf import settings

from apps.discovery.services.blocks import find_block_near

HOUR = 3600
CACHE_TTL = 7 * 86_400


def block_at(hypersync, chain, ts: int, height: int) -> int:
    """Premier bloc à l'heure arrondie de `ts` (précision largement suffisante pour des fenêtres
    de plusieurs jours), calculé une seule fois par chaîne et par heure."""
    ts -= ts % HOUR
    key = f"blockat:{chain.pk}:{ts}"
    client = redis.Redis.from_url(settings.REDIS_URL)
    cached = client.get(key)
    if cached is not None:
        return min(int(cached), height)
    block = find_block_near(ts, height, hypersync.block_timestamp)
    client.set(key, block, ex=CACHE_TTL)
    return block
