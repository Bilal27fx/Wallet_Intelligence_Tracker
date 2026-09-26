import pytest
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction

from apps.discovery.models import Candidate, Chain, DetectionSettings, PipelineSettings
from apps.discovery.tests.factories import make_candidate, make_chain, make_token

pytestmark = pytest.mark.django_db


def test_active_chains_require_every_service():
    make_chain(gt_id="ok")
    make_chain(gt_id="disabled", is_enabled=False)
    make_chain(gt_id="no-hypersync", hypersync_supported=False)
    make_chain(gt_id="no-zerion", zerion_id="")
    make_chain(gt_id="no-evm", evm_id=None)
    assert [chain.gt_id for chain in Chain.objects.active()] == ["ok"]
    assert Chain.objects.get(gt_id="ok").is_active
    assert not Chain.objects.get(gt_id="no-zerion").is_active


def test_only_one_global_detection_settings_row():
    DetectionSettings.objects.filter(chain=None).delete()
    DetectionSettings.objects.create(chain=None)
    with pytest.raises(IntegrityError), transaction.atomic():
        DetectionSettings.objects.create(chain=None)


def test_global_settings_require_every_threshold():
    with pytest.raises(ValidationError):
        DetectionSettings(chain=None, min_multiplier=5).clean()


def test_chain_settings_may_leave_thresholds_empty():
    DetectionSettings(chain=make_chain(), min_multiplier=3).clean()


def test_pipeline_settings_is_a_singleton():
    first = PipelineSettings.load()
    PipelineSettings(trending_pages=2).save()
    assert PipelineSettings.objects.count() == 1
    assert PipelineSettings.load().pk == first.pk
    assert PipelineSettings.load().trending_pages == 2


def test_one_open_candidate_per_token():
    token = make_token()
    make_candidate(token)
    with pytest.raises(IntegrityError), transaction.atomic():
        make_candidate(token)


def test_closed_candidates_do_not_block_a_new_one():
    token = make_token()
    make_candidate(token, status=Candidate.Status.REJECTED)
    make_candidate(token, status=Candidate.Status.BUYERS_EXTRACTED)
    make_candidate(token)
    assert Candidate.objects.open().count() == 1
