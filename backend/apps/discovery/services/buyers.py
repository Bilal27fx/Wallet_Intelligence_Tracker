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
    sold_before_trough: int = 0
    held_amount: int = 0
    held_usd: float = 0.0
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
        """Passe 1 (fenêtre d'achat → creux) : achats, et ventes qui réduisent la position."""
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
            elif self._is_sell(transfer):
                self.stats[signer].sold_before_trough += transfer.amount

    def add_sells(self, transfers: list[Transfer]) -> None:
        """Passe 2 (creux → pic) : ventes des acheteurs retenus pendant la montée."""
        for transfer in transfers:
            if self._is_sell(transfer):
                self.stats[transfer.tx_from].sold_amount += transfer.amount

    def _is_sell(self, transfer: Transfer) -> bool:
        signer = transfer.tx_from
        return transfer.sender == signer and transfer.recipient != signer and signer in self.stats

    def select(
        self,
        *,
        pool_created_block: int,
        sniper_blocks: int,
        min_buy_usd: float,
        max_buyers: int,
        trough_ts: int,
    ) -> list[BuyerStats]:
        """Acheteurs classés par position détenue au creux, valorisée au prix du creux.

        Un trader qui a tout revendu avant le creux tombe à 0, quel que soit son volume.
        """
        trough_price = self._price_at(trough_ts)
        kept = []
        for buyer in self.stats.values():
            buyer.held_amount = max(buyer.bought_amount - buyer.sold_before_trough, 0)
            buyer.held_usd = round(buyer.held_amount / self._scale * trough_price, 2)
            if buyer.held_usd >= min_buy_usd:
                buyer.bought_usd = round(buyer.bought_usd, 2)
                buyer.is_sniper = buyer.first_buy_block - pool_created_block <= sniper_blocks
                kept.append(buyer)
        kept.sort(key=lambda buyer: buyer.held_usd, reverse=True)
        return kept[:max_buyers] if max_buyers > 0 else kept
