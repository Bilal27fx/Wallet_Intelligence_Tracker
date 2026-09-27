"""Détection des exchanges : registre d'adresses, hot wallets, adresses de dépôt."""

from collections import Counter

from integrations.hypersync import WalletTransfer


def forwarding(
    transfers: list[WalletTransfer], address: str, hours: int, min_forward_pct: float
) -> tuple[float, str | None]:
    """Part (%) des réceptions renvoyées rapidement vers une même destination. Fonction pure."""
    address = address.lower()
    window = hours * 3600
    receptions = [t for t in transfers if t.recipient == address and t.sender != address]
    sends = [t for t in transfers if t.sender == address and t.recipient != address]
    if not receptions:
        return 0.0, None
    destinations: Counter[str] = Counter()
    for reception in receptions:
        out = [
            s
            for s in sends
            if s.token == reception.token
            and reception.timestamp <= s.timestamp <= reception.timestamp + window
        ]
        if out and sum(s.amount for s in out) * 100 >= reception.amount * min_forward_pct:
            destinations[max(out, key=lambda s: s.amount).recipient] += 1
    if not destinations:
        return 0.0, None
    destination, count = destinations.most_common(1)[0]
    return count * 100 / len(receptions), destination
