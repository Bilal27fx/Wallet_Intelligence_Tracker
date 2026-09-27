import pytest
import redis
from rest_framework.test import APIClient


@pytest.fixture
def api_client():
    return APIClient()


@pytest.fixture(autouse=True)
def isolated_redis(settings):
    """Base Redis dédiée aux tests (15), vidée à chaque test : jamais le cache réel."""
    url = settings.REDIS_URL.rsplit("/", 1)[0] + "/15"
    settings.REDIS_URL = url
    redis.Redis.from_url(url).flushdb()
