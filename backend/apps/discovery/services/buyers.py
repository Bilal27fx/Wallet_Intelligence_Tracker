"""Agrégation des early buyers depuis les transferts ERC-20. Fonction pure.

Un achat = le signataire de la transaction reçoit les tokens. Seul un EOA signe une
transaction : les contrats (bots, smart accounts, routers) sont exclus par construction.
"""

from bisect import bisect_right
from dataclasses import dataclass

from integrations.geckoterminal import Candle
from integrations.hypersync import Transfer

ZERO_ADDRESS = "0x" + "0" * 40


@dataclass
class BuyerStats:
    wallet: str
    first_buy_block: int
    first_buy_ts: int
    bought_amount: int = 0
    bought_usd: float = 0.0
    sold_amount: int = 0
    is_sniper: bool = False


def aggregate_buyers(
    transfers: list[Transfer],
    *,
    low_block: int,
    peak_block: int,
    pool_created_block: int,
    candles: list[Candle],
    decimals: int,
    sniper_blocks: int,
    min_buy_usd: float,
    max_buyers: int,
) -> list[BuyerStats]:
    candle_times = [candle.ts for candle in candles]

    def price_at(ts: int) -> float:
        index = bisect_right(candle_times, ts) - 1
        return candles[max(index, 0)].close

    scale = 10**decimals
    stats: dict[str, BuyerStats] = {}
    for transfer in sorted(transfers, key=lambda t: t.block):
        if transfer.block > peak_block:
            continue
        signer = transfer.tx_from
        is_buy = transfer.recipient == signer and transfer.sender not in (signer, ZERO_ADDRESS)
        if is_buy and transfer.block <= low_block:
            buyer = stats.get(signer)
            if buyer is None:
                buyer = stats[signer] = BuyerStats(signer, transfer.block, transfer.timestamp)
            buyer.bought_amount += transfer.amount
            buyer.bought_usd += transfer.amount / scale * price_at(transfer.timestamp)
        elif transfer.sender == signer and transfer.recipient != signer and signer in stats:
            stats[signer].sold_amount += transfer.amount

    kept = [buyer for buyer in stats.values() if buyer.bought_usd >= min_buy_usd]
    for buyer in kept:
        buyer.bought_usd = round(buyer.bought_usd, 2)
        buyer.is_sniper = buyer.first_buy_block - pool_created_block <= sniper_blocks
    kept.sort(key=lambda buyer: buyer.bought_usd, reverse=True)
    return kept[:max_buyers] if max_buyers > 0 else kept
