from django.conf import settings


def test_discovery_apps_installed():
    assert "django_celery_beat" in settings.INSTALLED_APPS
    assert "apps.discovery" in settings.INSTALLED_APPS


def test_beat_uses_database_scheduler():
    assert settings.CELERY_BEAT_SCHEDULER == "django_celery_beat.schedulers:DatabaseScheduler"


def test_api_keys_default_to_empty_strings():
    assert isinstance(settings.ENVIO_API_TOKEN, str)
    assert isinstance(settings.ZERION_API_KEY, str)
    assert isinstance(settings.COINGECKO_API_KEY, str)
