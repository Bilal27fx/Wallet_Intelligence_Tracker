import pytest

from apps.discovery.models import DetectionSettings


@pytest.fixture
def young_waves(db):
    """Le faux token explose 10 h après son lancement : on désactive le score minimum."""
    DetectionSettings.objects.filter(chain=None).update(min_score=0)
