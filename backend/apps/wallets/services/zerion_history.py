"""Transactions Zerion → mouvements (achat, vente, envoi, réception). Fonctions pures."""

from apps.wallets.services.classify import BUY, RECEIVE, SELL, SEND
from integrations.zerion import NATIVE, ZerionTransaction, ZerionTransfer


def trade_kind(operation_type: str, direction: str, has_in: bool, has_out: bool) -> str | None:
    """Un trade (ou un execute qui échange) donne achat / vente ; le reste envoi / réception."""
    if direction not in ("in", "out"):
        return None
    swap = operation_type == "trade" or (operation_type == "execute" and has_in and has_out)
    if swap:
        return BUY if direction == "in" else SELL
    return RECEIVE if direction == "in" else SEND


def movements(tx: ZerionTransaction) -> list[tuple[ZerionTransfer, str]]:
    if tx.status != "confirmed":
        return []
    directions = {t.direction for t in tx.transfers}
    has_in, has_out = "in" in directions, "out" in directions
    rows = []
    for transfer in tx.transfers:
        kind = trade_kind(tx.operation_type, transfer.direction, has_in, has_out)
        if kind:
            rows.append((transfer, kind))
    return rows


def counterparty(transfer: ZerionTransfer) -> str:
    return transfer.sender if transfer.direction == "in" else transfer.recipient


def is_quote(symbol: str, address: str, quote_symbols: list[str]) -> bool:
    """Monnaie de paiement : le natif, ou un symbole de la liste réglable."""
    return address == NATIVE or symbol.upper() in {s.upper() for s in quote_symbols}
