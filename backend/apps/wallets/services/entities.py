"""Wallets liés directs : liens forts (transfert après achat, gros transfert reçu)."""

from collections import defaultdict

from apps.discovery.models import Wallet
from apps.wallets.models import WalletProfile
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


ZERO_ADDRESS = "0x" + "0" * 40


def big_receive_targets(
    records: list[TradeRecord], threshold_pct: float
) -> dict[tuple[str, str], dict]:
    """Expéditeurs dont les envois représentent ≥ threshold % des entrées ($) du wallet. Pur."""
    inflow = sum(r.usd or 0.0 for r in records if r.kind in (BUY, RECEIVE))
    if inflow <= 0:
        return {}
    received: dict[str, float] = defaultdict(float)
    chain_of: dict[str, str] = {}
    for record in records:
        if record.kind == RECEIVE and record.usd and record.counterparty not in ("", ZERO_ADDRESS):
            received[record.counterparty] += record.usd
            chain_of.setdefault(record.counterparty, record.chain_id)
    return {
        (chain_of[sender], sender): {
            "pct": round(value * 100 / inflow, 2),
            "value_usd": round(value, 2),
        }
        for sender, value in received.items()
        if value * 100 >= threshold_pct * inflow
    }


def ensure_linked_profile(address: str) -> WalletProfile:
    """Profil d'un wallet lié direct : valorisé, jamais suivi plus loin."""
    wallet, _ = Wallet.objects.get_or_create(address=address.lower())
    profile, _ = WalletProfile.objects.get_or_create(
        wallet=wallet, defaults={"source": WalletProfile.Source.LINKED, "depth": 1}
    )
    return profile
