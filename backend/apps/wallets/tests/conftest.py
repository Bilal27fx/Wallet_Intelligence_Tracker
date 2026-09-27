from decimal import Decimal
from itertools import count

import pytest

from apps.wallets.models import TokenTrade, WalletTransaction
from apps.wallets.tests.fakes import NOW, TOKEN_A


@pytest.fixture
def make_trade(db):
    """Un mouvement Zerion minimal (une transaction, un transfert) pour un wallet."""
    numbers = count(1)

    def make(wallet, *, kind, counterparty, token=TOKEN_A, chain="base"):
        n = next(numbers)
        tx = WalletTransaction.objects.create(
            wallet=wallet,
            zerion_id=f"z{n}",
            chain=chain,
            tx_hash=f"0x{n:064x}",
            mined_at=NOW,
            operation_type="trade" if kind in ("buy", "sell") else kind,
            status="confirmed",
        )
        return TokenTrade.objects.create(
            transaction=tx,
            wallet=wallet,
            transfer_index=0,
            chain=chain,
            token_address=token,
            kind=kind,
            direction="out" if kind in ("sell", "send") else "in",
            quantity=Decimal(1),
            amount=Decimal(10**18),
            counterparty=counterparty,
            mined_at=NOW,
        )

    return make
