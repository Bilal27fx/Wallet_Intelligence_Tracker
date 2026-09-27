from decimal import Decimal

import pytest

from apps.discovery.models import Wallet
from apps.wallets.services.raw import save_portfolio
from apps.wallets.tests.fakes import NOW
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
