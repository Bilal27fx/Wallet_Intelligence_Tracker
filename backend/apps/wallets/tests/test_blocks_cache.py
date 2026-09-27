import uuid

import pytest
import redis
from django.conf import settings

from apps.discovery.tests.factories import make_chain
from apps.wallets.services.blocks import block_at

pytestmark = pytest.mark.django_db


class CountingHyperSync:
    def __init__(self):
        self.calls = 0

    def block_timestamp(self, number):
        self.calls += 1
        return 1_600_000_000 + number * 2


def test_block_lookup_is_cached_per_chain_and_hour():
    chain = make_chain(gt_id=f"test-{uuid.uuid4().hex[:8]}")
    client = redis.Redis.from_url(settings.REDIS_URL)
    for key in client.scan_iter(f"blockat:{chain.pk}:*"):
        client.delete(key)
    hs = CountingHyperSync()
    height = 50_000_000
    target = 1_600_000_000 + 40_000_000 * 2 + 1234
    first = block_at(hs, chain, target, height)
    assert first == (target - target % 3600 - 1_600_000_000) // 2
    calls = hs.calls
    assert block_at(hs, chain, target + 60, height) == first
    assert hs.calls == calls
