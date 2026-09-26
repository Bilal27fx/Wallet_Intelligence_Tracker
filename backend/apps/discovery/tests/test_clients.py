import pytest
from django.core.exceptions import ImproperlyConfigured

from apps.discovery.models import PipelineSettings
from apps.discovery.services import clients
from apps.discovery.tests.factories import make_chain

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
