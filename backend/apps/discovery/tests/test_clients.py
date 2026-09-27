import pytest
from django.core.exceptions import ImproperlyConfigured

from apps.discovery.models import PipelineSettings
from apps.discovery.services import clients
from apps.discovery.tests.factories import make_chain
from integrations.ratelimit import DailyBudget, RateLimiter

pytestmark = pytest.mark.django_db


def test_zerion_requires_api_key(settings):
    settings.ZERION_API_KEY = ""
    with pytest.raises(ImproperlyConfigured):
        clients.zerion(PipelineSettings.load())


def test_hypersync_requires_api_token(settings):
    settings.ENVIO_API_TOKEN = ""
    with pytest.raises(ImproperlyConfigured):
        clients.hypersync(make_chain(), PipelineSettings.load())


def test_geckoterminal_client_is_built_from_settings():
    assert clients.geckoterminal(PipelineSettings.load()) is not None


def test_http_backoff_comes_from_pipeline_settings():
    cfg = PipelineSettings.load()
    cfg.http_backoff_seconds = 7
    cfg.save()
    client = clients.geckoterminal(PipelineSettings.load())
    assert client._http._backoff == 7


def test_zerion_client_has_rate_limit_and_daily_budget(settings):
    settings.ZERION_API_KEY = "k"
    http = clients.zerion(PipelineSettings.load())._http
    assert isinstance(http._limiter, RateLimiter)
    assert isinstance(http._budget, DailyBudget)
    assert http._budget._per_day == 1800


def test_rpc_requires_public_url():
    with pytest.raises(ImproperlyConfigured):
        clients.rpc(make_chain(rpc_url=""), PipelineSettings.load())


def test_rpc_client_uses_chain_url():
    rpc = clients.rpc(make_chain(rpc_url="https://rpc.test/"), PipelineSettings.load())
    assert str(rpc._http._client.base_url) == "https://rpc.test/"
