"""Agrégation des mouvements par (chaîne, token). Fonction pure."""

from dataclasses import dataclass

from apps.wallets.services.classify import BUY, RECEIVE, SELL, SEND


@dataclass(frozen=True)
class TradeRecord:
    chain_id: int
    token: str
    kind: str
    amount: int
    usd: float | None
    ts: int
    block: int
    counterparty: str


@dataclass
class PositionStats:
    bought: int = 0
    sold: int = 0
    sent: int = 0
    received: int = 0
    bought_usd: float = 0.0
    sold_usd: float = 0.0
    buys: int = 0
    sells: int = 0
    first_ts: int = 0
    last_ts: int = 0


def aggregate_positions(records: list[TradeRecord]) -> dict[tuple[int, str], PositionStats]:
    stats: dict[tuple[int, str], PositionStats] = {}
    for record in sorted(records, key=lambda r: r.ts):
        position = stats.setdefault(
            (record.chain_id, record.token), PositionStats(first_ts=record.ts, last_ts=record.ts)
        )
        position.last_ts = record.ts
        if record.kind == BUY:
            position.bought += record.amount
            position.buys += 1
            position.bought_usd += record.usd or 0.0
        elif record.kind == SELL:
            position.sold += record.amount
            position.sells += 1
            position.sold_usd += record.usd or 0.0
        elif record.kind == SEND:
            position.sent += record.amount
        elif record.kind == RECEIVE:
            position.received += record.amount
    return stats
