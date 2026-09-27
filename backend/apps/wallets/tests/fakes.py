"""Scénario : un wallet d'achat (BUYER) achète 3 tokens, envoie 90 % de A vers un coffre (VAULT).
FUNDER lui a envoyé son premier gaz. Prix : A = 3 $, B = 1 $, C = 2 $, ETH = 2 000 $."""

from collections import Counter
from datetime import UTC, datetime

from integrations.errors import BudgetExhausted
from integrations.hypersync import Funding, WalletTransfer
from integrations.zerion import Fungible

NOW = datetime(2026, 9, 27, 12, tzinfo=UTC)
BLOCK_TIME = 2
HEIGHT = 50_000_000
GENESIS_TS = int(NOW.timestamp()) - HEIGHT * BLOCK_TIME
UNIT = 10**18

BUYER = "0x" + "a" * 40
VAULT = "0x" + "b" * 40
FUNDER = "0x" + "f" * 40
POOL = "0x" + "2" * 40
ROUTER = "0x" + "9" * 40
TOKEN_A = "0x" + "1" * 40
TOKEN_B = "0x" + "3" * 40
TOKEN_C = "0x" + "4" * 40
USDC = "0x" + "c" * 40
WETH = "0x" + "d" * 40
DEPOSIT = "0x" + "5" * 40
HOT = "0x" + "e" * 40


def block_of(ts: int) -> int:
    return (ts - GENESIS_TS) // BLOCK_TIME


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


START = HEIGHT - 100_000  # environ 2,3 jours avant NOW


def buyer_history(send_to: str = VAULT) -> list[WalletTransfer]:
    return [
        tr(START - 10, "0xt0", USDC, FUNDER, BUYER, 5_000 * 10**6, FUNDER, USDC),
        tr(START, "0xt1", USDC, BUYER, POOL, 1_000 * 10**6, BUYER, ROUTER, log_index=0),
        tr(START, "0xt1", TOKEN_A, POOL, BUYER, 500 * UNIT, BUYER, ROUTER, log_index=1),
        tr(START + 10, "0xt2", TOKEN_B, POOL, BUYER, 200 * UNIT, BUYER, ROUTER, tx_value=UNIT // 2),
        tr(START + 20, "0xt3", USDC, BUYER, POOL, 300 * 10**6, BUYER, ROUTER, log_index=0),
        tr(START + 20, "0xt3", TOKEN_C, POOL, BUYER, 100 * UNIT, BUYER, ROUTER, log_index=1),
        tr(START + 30, "0xt4", USDC, BUYER, POOL, 300 * 10**6, BUYER, ROUTER, log_index=0),
        tr(START + 30, "0xt4", TOKEN_C, POOL, BUYER, 100 * UNIT, BUYER, ROUTER, log_index=1),
        tr(START + 40, "0xt5", TOKEN_A, BUYER, send_to, 450 * UNIT, BUYER, TOKEN_A),
    ]


def vault_history() -> list[WalletTransfer]:
    return [tr(START + 40, "0xt5", TOKEN_A, BUYER, VAULT, 450 * UNIT, BUYER, TOKEN_A)]


def deposit_history() -> list[WalletTransfer]:
    return [
        tr(START + 40, "0xt5", TOKEN_A, BUYER, DEPOSIT, 450 * UNIT, BUYER, TOKEN_A),
        tr(START + 45, "0xt6", TOKEN_A, DEPOSIT, HOT, 450 * UNIT, HOT, TOKEN_A),
    ]


class FakeWalletHyperSync:
    def __init__(self, transfers=None, tx_counts=None, counterparties=None, fundings=None):
        self.transfers = (
            transfers if transfers is not None else {BUYER: buyer_history(), VAULT: vault_history()}
        )
        self.tx_counts = tx_counts if tx_counts is not None else {VAULT: 0}
        self.counterparties = counterparties or {}
        self.fundings = (
            fundings if fundings is not None else {BUYER: Funding(FUNDER, START - 20, UNIT)}
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


class FakeZerion:
    PRICES = {TOKEN_A: 3.0, TOKEN_B: 1.0, TOKEN_C: 2.0, USDC: 1.0}

    def __init__(self, exhausted: bool = False):
        self.calls: Counter[str] = Counter()
        self.exhausted = exhausted

    def _call(self, name):
        if self.exhausted:
            raise BudgetExhausted("budget")
        self.calls[name] += 1

    def prices(self, implementations):
        self._call("prices")
        return {
            (chain, address): Fungible(
                address, "T", self.PRICES[address], {chain: (address, 6 if address == USDC else 18)}
            )
            for chain, address in implementations
            if address in self.PRICES
        }

    def fungible(self, fungible_id):
        self._call("fungible")
        return Fungible(
            fungible_id, "WETH", 2000.0, {"base": (WETH, 18), "bsc": ("0x" + "7" * 40, 18)}
        )

    def search(self, symbol):
        self._call("search")
        return Fungible("usdc", "USDC", 1.0, {"base": (USDC, 6)}) if symbol == "USDC" else None

    def price_chart(self, fungible_id):
        self._call("chart")
        start = int(NOW.timestamp()) - 400 * 86_400
        return [(start + i * 86_400, 2000.0) for i in range(401)]


class FakeRpc:
    def __init__(self, balances):
        self.balances = balances

    def native_balance(self, address):
        return self.balances.get(address, 0)
