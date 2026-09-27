"""Position des entités par explosion, agrégée depuis les early buyers de leurs wallets."""

from collections import defaultdict
from decimal import Decimal

from apps.discovery.models import EarlyBuyer, EntityEarlyBuy, Explosion


def refresh_entity_buys(explosion: Explosion) -> int:
    """Recalcule les lignes par entité d'une explosion et leur rang (position au creux)."""
    totals: dict[int, dict] = defaultdict(
        lambda: {"amount": Decimal(0), "usd": Decimal(0), "sold": Decimal(0), "first": None}
    )
    for row in EarlyBuyer.objects.filter(explosion=explosion, entity__isnull=False):
        total = totals[row.entity_id]
        total["amount"] += row.held_amount
        total["usd"] += row.held_usd
        total["sold"] += row.sold_amount
        if total["first"] is None or row.first_buy_at < total["first"]:
            total["first"] = row.first_buy_at
    EntityEarlyBuy.objects.filter(explosion=explosion).delete()
    ranked = sorted(totals.items(), key=lambda item: item[1]["usd"], reverse=True)
    EntityEarlyBuy.objects.bulk_create(
        [
            EntityEarlyBuy(
                entity_id=entity_id,
                explosion=explosion,
                held_amount=total["amount"],
                held_usd=total["usd"],
                first_buy_at=total["first"],
                sold_during_rise_pct=(
                    round(total["sold"] * 100 / total["amount"], 2) if total["amount"] else 0
                ),
                rank=rank,
            )
            for rank, (entity_id, total) in enumerate(ranked, start=1)
        ]
    )
    return len(ranked)
