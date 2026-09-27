"""Filtres anti-bruit. Fonctions pures."""

from apps.wallets.services.classify import BUY, SELL
from apps.wallets.services.positions import TradeRecord
from apps.wallets.services.settings import QualificationThresholds

EARLY_BUYER = "early_buyer"


def prefilter_reason(
    txs_7d: int, txs_active: int, t: QualificationThresholds, source: str
) -> str | None:
    """Mesures additionnées sur les chaînes du pré-filtre."""
    if txs_7d > t.max_txs_per_day * 7:
        return "bot_frequency"
    if source == EARLY_BUYER and txs_active < t.min_txs_active:
        return "inactive"
    return None


def farmer_reason(distinct_received: int, t: QualificationThresholds) -> str | None:
    return "farmer" if distinct_received > t.max_distinct_tokens else None


def mev_ratio(records: list[TradeRecord], quote_tokens: set[str]) -> float:
    buys = [r for r in records if r.kind == BUY and r.token not in quote_tokens]
    if not buys:
        return 0.0
    sells = {(r.token, r.block) for r in records if r.kind == SELL}
    same_block = sum(1 for r in buys if (r.token, r.block) in sells)
    return same_block * 100 / len(buys)


def distinct_buys(records: list[TradeRecord], quote_tokens: set[str]) -> int:
    return len({r.token for r in records if r.kind == BUY and r.token not in quote_tokens})


def history_reason(
    records: list[TradeRecord], quote_tokens: set[str], t: QualificationThresholds, source: str
) -> str | None:
    if source != EARLY_BUYER:
        return None
    if mev_ratio(records, quote_tokens) > t.max_mev_ratio:
        return "bot_mev"
    if distinct_buys(records, quote_tokens) < t.min_distinct_buys:
        return "too_few_trades"
    return None
