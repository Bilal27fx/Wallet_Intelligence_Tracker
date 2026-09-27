"""Tags de comportement d'un wallet et d'une entité. Fonctions pures."""

from collections import defaultdict
from dataclasses import dataclass

from apps.wallets.services.classify import BUY, RECEIVE, SELL, SEND
from apps.wallets.services.positions import TradeRecord
from apps.wallets.services.settings import QualificationThresholds

SNIPER = "SNIPER"
EARLY_BUYER = "EARLY_BUYER"
ACCUMULATEUR = "ACCUMULATEUR"
FLIPPER = "FLIPPER"
HOLDER = "HOLDER"


@dataclass(frozen=True)
class EarlyBuy:
    token: str
    chain_id: int
    is_sniper: bool
    bought: int
    sold_before_peak: int


def _flipped(records: list[TradeRecord], t: QualificationThresholds) -> bool:
    buys = [r for r in records if r.kind == BUY]
    bought = sum(r.amount for r in buys)
    first_buy = min(r.ts for r in buys)
    sold = 0
    for record in sorted(records, key=lambda r: r.ts):
        if record.kind != SELL:
            continue
        sold += record.amount
        if sold * 100 >= t.flipper_min_sold_pct * bought:
            return record.ts - first_buy <= t.flipper_hours * 3600
    return False


def _accumulating(records: list[TradeRecord], t: QualificationThresholds) -> bool:
    buys = [r for r in records if r.kind == BUY]
    inflow = sum(r.amount for r in records if r.kind in (BUY, RECEIVE))
    out = sum(r.amount for r in records if r.kind in (SELL, SEND))
    return len(buys) >= 2 and out * 100 < t.accumulator_max_out_pct * inflow


def wallet_tags(
    records: list[TradeRecord],
    early_buys: list[EarlyBuy],
    quote_tokens: set[str],
    t: QualificationThresholds,
) -> list[str]:
    tags: set[str] = set()
    if any(e.is_sniper for e in early_buys):
        tags.add(SNIPER)
    if len(early_buys) >= t.early_buyer_min_explosions:
        tags.add(EARLY_BUYER)
    if any(
        e.bought > 0 and (e.bought - e.sold_before_peak) * 100 >= t.holder_min_pct * e.bought
        for e in early_buys
    ):
        tags.add(HOLDER)

    per_position: dict[tuple[int, str], list[TradeRecord]] = defaultdict(list)
    for record in records:
        if record.token not in quote_tokens:
            per_position[(record.chain_id, record.token)].append(record)
    bought_positions = [rs for rs in per_position.values() if any(r.kind == BUY for r in rs)]
    if bought_positions:
        flipped = sum(1 for rs in bought_positions if _flipped(rs, t))
        if flipped * 100 > t.flipper_min_share_pct * len(bought_positions):
            tags.add(FLIPPER)
        if sum(1 for rs in bought_positions if _accumulating(rs, t)) >= t.accumulator_min_positions:
            tags.add(ACCUMULATEUR)
    return sorted(tags)


def entity_tags(member_tags: list[list[str]], internal_holding: bool) -> list[str]:
    tags = {tag for member in member_tags for tag in member}
    if internal_holding:
        tags.add(HOLDER)
    return sorted(tags)
