import pytest
from django.core.exceptions import ImproperlyConfigured

from apps.discovery.models import PipelineSettings, Token, Wallet
from apps.discovery.tests.factories import make_chain, make_token
from apps.wallets.models import KnownAddress, TokenPosition
from apps.wallets.services.pricing import (
    STABLE,
    WRAPPED,
    QuoteAsset,
    native_price_lookup,
    quote_assets,
    sync_quote_assets,
    wallet_value,
)
from apps.wallets.tests.fakes import BUYER, NOW, TOKEN_A, UNIT, USDC, WETH, FakeRpc, FakeZerion
from integrations.errors import BudgetExhausted

pytestmark = pytest.mark.django_db


@pytest.fixture
def chain():
    return make_chain(
        native_fungible_id="eth", wrapped_fungible_id="0xweth-id", rpc_url="https://rpc.test/"
    )


def test_sync_quote_assets_registers_wrapped_and_stables(chain):
    assert sync_quote_assets(FakeZerion(), PipelineSettings.load()) == 2
    assert KnownAddress.objects.get(address=USDC).kind == "stablecoin"
    assert Token.objects.get(address=USDC).decimals == 6
    assert KnownAddress.objects.get(address=WETH).kind == "wrapped_native"
    assert quote_assets(chain) == {USDC: QuoteAsset(STABLE, 6), WETH: QuoteAsset(WRAPPED, 18)}


def test_wrapped_asset_only_registered_on_chains_using_it(chain):
    sync_quote_assets(FakeZerion(), PipelineSettings.load())
    assert not KnownAddress.objects.filter(address="0x" + "7" * 40).exists()


def test_native_price_lookup_fetches_chart_once(chain):
    zerion = FakeZerion()
    lookup = native_price_lookup(chain, zerion, NOW)
    assert lookup(int(NOW.timestamp())) == 2000.0
    assert lookup(0) is None
    native_price_lookup(chain, zerion, NOW)
    assert zerion.calls["chart"] == 1


def test_wallet_value_sums_tokens_and_native(chain):
    wallet = Wallet.objects.create(address=BUYER)
    token = make_token(chain, address=TOKEN_A)
    TokenPosition.objects.create(
        wallet=wallet, token=token, bought_amount=50 * UNIT, first_at=NOW, last_at=NOW
    )
    total, details = wallet_value(
        wallet, [chain], FakeZerion(), lambda c: FakeRpc({BUYER: UNIT}), NOW
    )
    assert total == 2150.0
    assert details["base"] == {"tokens_usd": 150.0, "native_usd": 2000.0}


def test_wallet_value_without_rpc_counts_native_as_zero(chain):
    wallet = Wallet.objects.create(address=BUYER)

    def no_rpc(c):
        raise ImproperlyConfigured("pas de RPC")

    total, details = wallet_value(wallet, [chain], FakeZerion(), no_rpc, NOW)
    assert total == 0.0
    assert details["base"]["native_error"] is True


def test_wallet_value_propagates_budget_exhaustion(chain):
    wallet = Wallet.objects.create(address=BUYER)
    TokenPosition.objects.create(
        wallet=wallet,
        token=make_token(chain, address=TOKEN_A),
        bought_amount=UNIT,
        first_at=NOW,
        last_at=NOW,
    )
    with pytest.raises(BudgetExhausted):
        wallet_value(wallet, [chain], FakeZerion(exhausted=True), lambda c: FakeRpc({}), NOW)
