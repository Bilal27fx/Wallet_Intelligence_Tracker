"""Résolution des seuils : valeur de la chaîne si renseignée, sinon valeur globale."""

from dataclasses import dataclass, fields
from decimal import Decimal

from django.db.models import Q

from apps.discovery.models import Chain, DetectionSettings


@dataclass(frozen=True)
class Thresholds:
    min_change_24h_pct: float
    min_liquidity_usd: float
    min_volume_usd: float
    peak_volume_window_hours: int
    min_fdv_usd: float
    max_fdv_usd: float
    max_pool_age_hours: int
    min_multiplier: float
    min_retention_pct: float
    confirmation_hours: int
    sniper_blocks: int
    min_buy_usd: float
    max_buyers: int
    explosion_window_hours: int
    maturity_hours: int
    breakout_multiplier: float
    buyer_window_hours: int


def thresholds_for(chain: Chain) -> Thresholds:
    rows = {
        row.chain_id: row
        for row in DetectionSettings.objects.filter(Q(chain__isnull=True) | Q(chain=chain))
    }
    base = rows[None]
    override = rows.get(chain.pk)
    values = {}
    for field in fields(Thresholds):
        value = getattr(override, field.name) if override else None
        if value is None:
            value = getattr(base, field.name)
        values[field.name] = float(value) if isinstance(value, Decimal) else value
    return Thresholds(**values)
