"""Couche brute Zerion : portefeuille par token, métadonnées de tokens."""

from decimal import Decimal

from apps.wallets.models import PortfolioPosition, PortfolioSnapshot


def _money(value: float | None) -> Decimal | None:
    return Decimal(str(round(value, 2))) if value is not None else None


def save_portfolio(wallet, portfolio, now) -> PortfolioSnapshot:
    """Photo du portefeuille : une ligne par token, avec la réponse brute."""
    snapshot = PortfolioSnapshot.objects.create(
        wallet=wallet, fetched_at=now, total_usd=_money(portfolio.total_usd), raw=portfolio.raw
    )
    PortfolioPosition.objects.bulk_create(
        [
            PortfolioPosition(
                snapshot=snapshot,
                chain=item.chain,
                token_address=item.token_address,
                fungible_id=item.fungible_id[:100],
                symbol=item.symbol[:64],
                position_type=item.position_type[:32],
                quantity=item.quantity,
                price_usd=Decimal(str(item.price_usd)) if item.price_usd is not None else None,
                value_usd=_money(item.value_usd),
            )
            for item in portfolio.positions
        ]
    )
    return snapshot
