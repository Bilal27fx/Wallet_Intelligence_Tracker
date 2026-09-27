"""Classement des transferts d'un wallet. Fonction pure.

Achat : le wallet signe et reçoit via un contrat autre que le token (router, pool).
Vente : le wallet signe et envoie via un contrat autre que le token.
Envoi : appel direct à transfer() du token, ou tokens déplacés par un autre signataire.
Réception : le wallet reçoit sans avoir signé (transfert, airdrop).
"""

from dataclasses import dataclass

from integrations.hypersync import WalletTransfer

BUY = "buy"
SELL = "sell"
SEND = "send"
RECEIVE = "receive"


@dataclass(frozen=True)
class Trade:
    transfer: WalletTransfer
    kind: str
    counterparty: str


def classify(transfer: WalletTransfer, wallet: str) -> Trade | None:
    wallet = wallet.lower()
    if transfer.sender == wallet and transfer.recipient == wallet:
        return None
    via_contract = transfer.tx_from == wallet and transfer.tx_to != transfer.token
    if transfer.recipient == wallet:
        return Trade(transfer, BUY if via_contract else RECEIVE, transfer.sender)
    if transfer.sender == wallet:
        return Trade(transfer, SELL if via_contract else SEND, transfer.recipient)
    return None


def classify_all(transfers: list[WalletTransfer], wallet: str) -> list[Trade]:
    return [trade for trade in (classify(t, wallet) for t in transfers) if trade is not None]
