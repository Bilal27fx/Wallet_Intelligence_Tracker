"""Scénario v2 : BUYER achète A, B, C ; envoie 90 % de A à VAULT ; a reçu 5 000 USDC de SENDER.
Portefeuilles Zerion : BUYER 6 150 $, VAULT 11 350 $, SENDER 2 000 $ (total lié : 19 500 $)."""

from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from integrations.errors import BudgetExhausted
from integrations.hypersync import Funding, WalletTransfer
from integrations.zerion import (
    NATIVE,
    Portfolio,
    TransactionsPage,
    ZerionTransaction,
    ZerionTransfer,
)

NOW = datetime(2026, 9, 27, 12, tzinfo=UTC)
BLOCK_TIME = 2
HEIGHT = 50_000_000
GENESIS_TS = int(NOW.timestamp()) - HEIGHT * BLOCK_TIME
START = HEIGHT - 100_000
UNIT = 10**18

BUYER = "0x" + "a" * 40
VAULT = "0x" + "b" * 40
SENDER = "0x" + "f" * 40
POOL = "0x" + "2" * 40
ROUTER = "0x" + "9" * 40
TOKEN_A = "0x" + "1" * 40
TOKEN_B = "0x" + "3" * 40
TOKEN_C = "0x" + "4" * 40
USDC = "0x" + "c" * 40
DEPOSIT = "0x" + "5" * 40
HOT = "0x" + "e" * 40


def tr(block, tx, token, sender, recipient, amount, tx_from, tx_to, tx_value=0, log_index=0):
    return WalletTransfer(
        block=block,
        timestamp=GENESIS_TS + block * BLOCK_TIME,
        tx_hash=tx,
        log_index=log_index,
        token=token,
        sender=sender,
        recipient=recipient,
        amount=amount,
        tx_from=tx_from,
        tx_to=tx_to,
        tx_value=tx_value,
    )


def hs_history() -> list[WalletTransfer]:
    """Vue HyperSync de BUYER (pré-filtre) : 3 achats via un router."""
    return [
        tr(START, "0xh1", TOKEN_A, POOL, BUYER, 500 * UNIT, BUYER, ROUTER),
        tr(START + 10, "0xh2", TOKEN_B, POOL, BUYER, 200 * UNIT, BUYER, ROUTER),
        tr(START + 20, "0xh3", TOKEN_C, POOL, BUYER, 100 * UNIT, BUYER, ROUTER),
    ]


class FakeWalletHyperSync:
    def __init__(self, transfers=None, tx_counts=None, counterparties=None, fundings=None):
        self.transfers = transfers if transfers is not None else {BUYER: hs_history()}
        self.tx_counts = tx_counts if tx_counts is not None else {VAULT: 0}
        self.counterparties = counterparties or {}
        self.fundings = (
            fundings if fundings is not None else {BUYER: Funding(SENDER, START - 20, UNIT)}
        )

    def height(self):
        return HEIGHT

    def block_timestamp(self, number):
        return GENESIS_TS + number * BLOCK_TIME

    def wallet_tx_count(self, address, from_block, to_block, cap):
        return min(self.tx_counts.get(address, 10), cap)

    def distinct_counterparties(self, address, from_block, to_block, cap):
        return min(self.counterparties.get(address, 3), cap)

    def first_funding(self, address, to_block):
        return self.fundings.get(address)

    def wallet_transfers(self, address, from_block, to_block):
        return [t for t in self.transfers.get(address, []) if from_block <= t.block < to_block]


def ztransfer(index, direction, token, symbol, units, value, other, decimals=18):
    sender, recipient = (other, BUYER) if direction == "in" else (BUYER, other)
    return ZerionTransfer(
        index=index,
        chain="base",
        token_address=token,
        token_symbol=symbol,
        token_decimals=decimals,
        fungible_id=symbol.lower(),
        direction=direction,
        amount=int(units * 10**decimals),
        quantity=Decimal(str(units)),
        price_usd=(value / units) if value is not None else None,
        value_usd=value,
        sender=sender,
        recipient=recipient,
    )


def ztx(zerion_id, days_ago, operation_type, transfers, block):
    return ZerionTransaction(
        zerion_id=zerion_id,
        chain="base",
        tx_hash=f"0x{zerion_id}",
        block=block,
        mined_at=NOW - timedelta(days=days_ago),
        operation_type=operation_type,
        status="confirmed",
        fee_usd=0.05,
        transfers=transfers,
        raw={"id": zerion_id},
    )


def buyer_zerion_history(send_to: str = VAULT) -> list[ZerionTransaction]:
    """Du plus récent au plus ancien, comme Zerion."""
    return [
        ztx("z5", 1, "send", [ztransfer(0, "out", TOKEN_A, "A", 450, 1350.0, send_to)], 105),
        ztx(
            "z4",
            2,
            "trade",
            [
                ztransfer(0, "in", TOKEN_C, "C", 100, 300.0, POOL),
                ztransfer(1, "out", USDC, "USDC", 300, 300.0, ROUTER, decimals=6),
            ],
            104,
        ),
        ztx(
            "z3",
            3,
            "trade",
            [
                ztransfer(0, "in", TOKEN_C, "C", 100, 300.0, POOL),
                ztransfer(1, "out", USDC, "USDC", 300, 300.0, ROUTER, decimals=6),
            ],
            103,
        ),
        ztx(
            "z2",
            4,
            "trade",
            [
                ztransfer(0, "in", TOKEN_B, "B", 200, 1000.0, POOL),
                ztransfer(1, "out", NATIVE, "ETH", 0.5, 1000.0, ROUTER),
            ],
            102,
        ),
        ztx(
            "z1",
            5,
            "trade",
            [
                ztransfer(0, "in", TOKEN_A, "A", 500, 1000.0, POOL),
                ztransfer(1, "out", USDC, "USDC", 1000, 1000.0, ROUTER, decimals=6),
            ],
            101,
        ),
        ztx(
            "z0",
            6,
            "receive",
            [ztransfer(0, "in", USDC, "USDC", 5000, 5000.0, SENDER, decimals=6)],
            100,
        ),
    ]


class FakeZerion:
    def __init__(self, histories=None, portfolios=None, page_size=3, budget=None):
        self.histories = histories if histories is not None else {BUYER: buyer_zerion_history()}
        self.portfolios = (
            portfolios
            if portfolios is not None
            else {BUYER: 6150.0, VAULT: 11350.0, SENDER: 2000.0}
        )
        self.page_size = page_size
        self.budget = budget
        self.calls: Counter[str] = Counter()

    def _spend(self, name):
        if self.budget is not None and sum(self.calls.values()) >= self.budget:
            raise BudgetExhausted("budget")
        self.calls[name] += 1

    def transactions(self, address, since, cursor=None):
        self._spend("transactions")
        txs = [t for t in self.histories.get(address, []) if t.mined_at >= since]
        start = int(cursor or 0)
        end = start + self.page_size
        return TransactionsPage(txs[start:end], str(end) if end < len(txs) else None)

    def portfolio(self, address):
        self._spend("portfolio")
        total = self.portfolios.get(address, 0.0)
        return Portfolio(total, {"base": total})
