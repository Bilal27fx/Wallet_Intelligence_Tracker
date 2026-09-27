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


class BuyerAggregator:
    """Totaux par acheteur alimentés page par page : la mémoire suit le nombre d'acheteurs."""

    def __init__(self, *, candles: list[Candle], decimals: int):
        self._candles = candles
        self._times = [candle.ts for candle in candles]
        self._scale = 10**decimals
        self.stats: dict[str, BuyerStats] = {}

    def _price_at(self, ts: int) -> float:
        index = bisect_right(self._times, ts) - 1
        return self._candles[max(index, 0)].close

    def add(self, transfers: list[Transfer]) -> None:
        """Passe 1 (fenêtre d'achat → creux) : achats et ventes."""
        for transfer in sorted(transfers, key=lambda t: t.block):
            signer = transfer.tx_from
            if transfer.recipient == signer and transfer.sender not in (signer, ZERO_ADDRESS):
                buyer = self.stats.get(signer)
                if buyer is None:
                    buyer = self.stats[signer] = BuyerStats(
                        signer, transfer.block, transfer.timestamp
                    )
                buyer.bought_amount += transfer.amount
                price = self._price_at(transfer.timestamp)
                buyer.bought_usd += transfer.amount / self._scale * price
            else:
                self._count_sell(transfer)

    def add_sells(self, transfers: list[Transfer]) -> None:
        """Passe 2 (creux → pic) : ventes des acheteurs connus."""
        for transfer in transfers:
            self._count_sell(transfer)

    def _count_sell(self, transfer: Transfer) -> None:
        signer = transfer.tx_from
        if transfer.sender == signer and transfer.recipient != signer and signer in self.stats:
            self.stats[signer].sold_amount += transfer.amount

    def select(
        self, *, pool_created_block: int, sniper_blocks: int, min_buy_usd: float, max_buyers: int
    ) -> list[BuyerStats]:
        kept = [buyer for buyer in self.stats.values() if buyer.bought_usd >= min_buy_usd]
        for buyer in kept:
            buyer.bought_usd = round(buyer.bought_usd, 2)
            buyer.is_sniper = buyer.first_buy_block - pool_created_block <= sniper_blocks
        kept.sort(key=lambda buyer: buyer.bought_usd, reverse=True)
        return kept[:max_buyers] if max_buyers > 0 else kept
