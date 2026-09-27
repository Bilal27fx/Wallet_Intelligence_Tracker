"""Entités : wallets d'une même personne, reliés par des transferts ou un financement."""

from collections import defaultdict

from apps.wallets.services.classify import BUY, RECEIVE, SEND
from apps.wallets.services.positions import TradeRecord


def transfer_after_buy_targets(
    records: list[TradeRecord], threshold_pct: float
) -> dict[tuple[int, str], dict]:
    """Destinataires ayant reçu ≥ threshold % d'un token entré dans le wallet. Fonction pure."""
    inflow: dict[tuple[int, str], int] = defaultdict(int)
    sent: dict[tuple[int, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for record in records:
        key = (record.chain_id, record.token)
        if record.kind in (BUY, RECEIVE):
            inflow[key] += record.amount
        elif record.kind == SEND:
            sent[key][record.counterparty] += record.amount
    targets: dict[tuple[int, str], dict] = {}
    for (chain_id, token), per_destination in sent.items():
        total = inflow[(chain_id, token)]
        if total <= 0:
            continue
        for destination, amount in per_destination.items():
            pct = round(amount * 100 / total, 2)
            current = targets.get((chain_id, destination))
            if pct >= threshold_pct and (current is None or pct > current["pct"]):
                targets[(chain_id, destination)] = {"token": token, "pct": pct}
    return targets
