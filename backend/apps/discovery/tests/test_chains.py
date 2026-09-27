import pytest

from apps.discovery.models import Chain, PipelineSettings
from apps.discovery.services.chains import sync_chains
from apps.discovery.tests.fakes import FakeCoinGecko, FakeDirectory, FakeGeckoTerminal, FakeZerion

pytestmark = pytest.mark.django_db


def run_sync() -> int:
    return sync_chains(FakeGeckoTerminal(), FakeCoinGecko(), FakeDirectory(), FakeZerion())


def test_sync_creates_chains_with_support_flags():
    assert run_sync() == 2
    base = Chain.objects.get(gt_id="base")
    assert (base.evm_id, base.zerion_id, base.hypersync_supported) == (8453, "base", True)
    assert base.is_active
    solana = Chain.objects.get(gt_id="solana")
    assert (solana.evm_id, solana.zerion_id, solana.hypersync_supported) == (None, "", False)
    assert not solana.is_active


def test_sync_never_touches_is_enabled():
    run_sync()
    Chain.objects.filter(gt_id="base").update(is_enabled=False)
    run_sync()
    assert not Chain.objects.get(gt_id="base").is_enabled


def test_sync_is_idempotent():
    run_sync()
    run_sync()
    assert Chain.objects.count() == 2


def test_sync_stores_rpc_and_native_assets():
    run_sync()
    base = Chain.objects.get(gt_id="base")
    assert base.rpc_url == "https://mainnet.base.org/"
    assert (base.native_fungible_id, base.wrapped_fungible_id) == ("eth", "0xweth")
    assert Chain.objects.get(gt_id="solana").rpc_url == ""


def test_pipeline_settings_qualification_defaults():
    cfg = PipelineSettings.load()
    assert cfg.zerion_daily_budget == 250
    assert cfg.zerion_requests_per_min == 50
    assert cfg.stablecoin_symbols == ["USDC", "USDT", "DAI"]
    assert cfg.extra_chains == []
    assert cfg.qualification_batch_size == 100
