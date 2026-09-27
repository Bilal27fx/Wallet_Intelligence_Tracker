"""Résolution des seuils de qualification : valeur de la chaîne si renseignée, sinon globale."""

from dataclasses import dataclass, fields
from decimal import Decimal

from django.db.models import Q

from apps.discovery.models import Chain
from apps.wallets.models import QualificationSettings


@dataclass(frozen=True)
class QualificationThresholds:
    max_txs_per_day: int
    max_distinct_tokens: int
    inactive_days: int
    min_txs_active: int
    history_days: int
    max_mev_ratio: float
    min_distinct_buys: int
    min_portfolio_usd: float
    max_portfolio_usd: float
    transfer_after_buy_pct: float
    big_receive_pct: float
    hot_wallet_min_counterparties: int
    deposit_forward_pct: float
    deposit_forward_hours: int
    flipper_hours: int
    flipper_min_sold_pct: float
    flipper_min_share_pct: float
    holder_min_pct: float
    accumulator_max_out_pct: float
    accumulator_min_positions: int
    early_buyer_min_explosions: int
    refilter_after_days: int


def qualification_thresholds(chain: Chain | None = None) -> QualificationThresholds:
    condition = Q(chain__isnull=True) | Q(chain=chain) if chain else Q(chain__isnull=True)
    rows = {row.chain_id: row for row in QualificationSettings.objects.filter(condition)}
    base = rows[None]
    override = rows.get(chain.pk) if chain else None
    values = {}
    for field in fields(QualificationThresholds):
        value = getattr(override, field.name) if override else None
        if value is None:
            value = getattr(base, field.name)
        values[field.name] = float(value) if isinstance(value, Decimal) else value
    return QualificationThresholds(**values)
