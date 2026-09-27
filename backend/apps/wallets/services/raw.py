"""Couche brute Zerion : portefeuille par token, métadonnées de tokens."""

from datetime import timedelta
from decimal import Decimal

from apps.wallets.models import PortfolioPosition, PortfolioSnapshot, TokenInfo, TokenTrade
from integrations.errors import BudgetExhausted
from integrations.zerion import MAX_IMPLEMENTATIONS_PER_CALL, NATIVE


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


def _supply(value: float | None) -> Decimal | None:
    return Decimal(str(value)) if value is not None else None


def refresh_token_info(zerion, now, cfg) -> int:
    """Métadonnées Zerion des tokens vus dans l'historique, absents ou trop anciens.

    Un token que Zerion ne connaît pas est enregistré vide : il n'est pas redemandé avant
    `token_info_refresh_days`. S'arrête proprement quand le budget Zerion du jour est épuisé.
    """
    fresh = now - timedelta(days=cfg.token_info_refresh_days)
    known = set(TokenInfo.objects.filter(fetched_at__gte=fresh).values_list("chain", "address"))
    pairs = TokenTrade.objects.exclude(token_address=NATIVE).values_list("chain", "token_address")
    wanted = sorted({pair for pair in pairs.distinct() if pair not in known})
    saved = 0
    for start in range(0, len(wanted), MAX_IMPLEMENTATIONS_PER_CALL):
        batch = wanted[start : start + MAX_IMPLEMENTATIONS_PER_CALL]
        try:
            metas = zerion.token_metadata(batch)
        except BudgetExhausted:
            break
        found = {
            (chain, address): (meta, decimals)
            for meta in metas
            for chain, (address, decimals) in meta.implementations.items()
        }
        for chain, address in batch:
            meta, decimals = found.get((chain, address), (None, None))
            TokenInfo.objects.update_or_create(
                chain=chain,
                address=address,
                defaults={
                    "fungible_id": meta.fungible_id[:100] if meta else "",
                    "symbol": meta.symbol[:64] if meta else "",
                    "name": meta.name[:200] if meta else "",
                    "decimals": decimals,
                    "total_supply": _supply(meta.total_supply) if meta else None,
                    "circulating_supply": _supply(meta.circulating_supply) if meta else None,
                    "verified": meta.verified if meta else False,
                    "raw": meta.raw if meta else {},
                    "fetched_at": now,
                },
            )
            saved += 1
    return saved
