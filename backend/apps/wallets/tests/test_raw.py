from decimal import Decimal

import pytest

from apps.discovery.models import PipelineSettings, Wallet
from apps.wallets.models import TokenInfo
from apps.wallets.services.raw import refresh_token_info, save_portfolio
from apps.wallets.tests.fakes import NOW, TOKEN_A, FakeZerion, meta_for
from integrations.zerion import Portfolio, PortfolioItem

pytestmark = pytest.mark.django_db


def test_save_portfolio_keeps_each_token():
    wallet = Wallet.objects.create(address="0x" + "1" * 40)
    item = PortfolioItem("base", "native", "eth", "ETH", "wallet", Decimal("0.5"), 2600.0, 1300.0)
    snapshot = save_portfolio(wallet, Portfolio(1300.0, {"base": 1300.0}, [item], {"p": 1}), NOW)
    [position] = snapshot.positions.all()
    assert (position.symbol, position.quantity) == ("ETH", Decimal("0.5"))
    assert (float(position.value_usd), float(snapshot.total_usd)) == (1300.0, 1300.0)
    assert snapshot.raw == {"p": 1}


def test_refresh_token_info_fetches_unknown_tokens_once(make_trade):
    wallet = Wallet.objects.create(address="0x" + "1" * 40)
    make_trade(wallet, kind="buy", counterparty="0x" + "9" * 40)
    zerion = FakeZerion(metas={("base", TOKEN_A): meta_for(TOKEN_A)})
    cfg = PipelineSettings.load()
    assert refresh_token_info(zerion, NOW, cfg) == 1
    info = TokenInfo.objects.get()
    assert (info.fungible_id, float(info.total_supply), info.decimals) == ("fid", 1_000_000.0, 18)
    assert refresh_token_info(zerion, NOW, cfg) == 0
    assert zerion.calls["token_metadata"] == 1


def test_unknown_token_is_stored_empty_and_not_refetched(make_trade):
    wallet = Wallet.objects.create(address="0x" + "1" * 40)
    make_trade(wallet, kind="buy", counterparty="0x" + "9" * 40)
    zerion = FakeZerion()
    cfg = PipelineSettings.load()
    assert refresh_token_info(zerion, NOW, cfg) == 1
    assert TokenInfo.objects.get().fungible_id == ""
    assert refresh_token_info(zerion, NOW, cfg) == 0
