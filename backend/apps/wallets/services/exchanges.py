"""Détection des exchanges : registre d'adresses, hot wallets, adresses de dépôt."""

from collections import Counter

from apps.discovery.models import Chain
from apps.discovery.services.blocks import find_block_at
from apps.wallets.models import KnownAddress
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


WEEK = 7 * 86_400


def register(address: str, chain, kind: str, label: str = "") -> None:
    KnownAddress.objects.get_or_create(
        chain=chain,
        address=address.lower(),
        defaults={"kind": kind, "label": label[:128], "source": KnownAddress.Source.AUTO},
    )


def _is_hot(address: str, hypersync, t, from_block: int, height: int) -> bool:
    count = hypersync.distinct_counterparties(
        address, from_block, height, cap=t.hot_wallet_min_counterparties
    )
    return count >= t.hot_wallet_min_counterparties


def detect_exchange(address: str, chain, hypersync, t, now, height: int) -> bool:
    """Exchange, dépôt d'exchange ou service ? Enregistre l'adresse au passage (source auto)."""
    address = address.lower()
    if KnownAddress.objects.blocking_for(address, [chain.pk]).exists():
        return True
    week = find_block_at(int(now.timestamp()) - WEEK, 0, height, hypersync.block_timestamp)
    if _is_hot(address, hypersync, t, week, height):
        register(address, chain, KnownAddress.Kind.EXCHANGE, "hot wallet (auto)")
        return True
    transfers = hypersync.wallet_transfers(address, week, height)
    never_signed = hypersync.wallet_tx_count(address, 0, height, cap=1) == 0
    if never_signed and any(x.sender == address for x in transfers):
        register(address, chain, KnownAddress.Kind.CEX_DEPOSIT, "relais qui ne signe jamais (auto)")
        return True
    share, destination = forwarding(
        transfers, address, t.deposit_forward_hours, t.deposit_forward_pct
    )
    if (
        destination
        and share >= t.deposit_forward_pct
        and (
            KnownAddress.objects.blocking_for(destination, [chain.pk]).exists()
            or _is_hot(destination, hypersync, t, week, height)
        )
    ):
        register(destination, chain, KnownAddress.Kind.EXCHANGE, "hot wallet (auto)")
        register(address, chain, KnownAddress.Kind.CEX_DEPOSIT, f"dépôt vers {destination} (auto)")
        return True
    return False


def import_known_addresses(rows: list[dict]) -> int:
    """Import de listes d'adresses (exchanges, bridges…). Chaîne vide = toutes les chaînes."""
    chains = {chain.gt_id: chain for chain in Chain.objects.all()}
    imported = 0
    for row in rows:
        address = (row.get("address") or "").strip().lower()
        kind = (row.get("kind") or "").strip()
        chain_id = (row.get("chain") or "").strip()
        if (
            not address.startswith("0x")
            or len(address) != 42
            or kind not in KnownAddress.Kind.values
        ):
            continue
        if chain_id and chain_id not in chains:
            continue
        KnownAddress.objects.update_or_create(
            chain=chains.get(chain_id),
            address=address,
            defaults={
                "kind": kind,
                "label": (row.get("label") or "").strip()[:128],
                "source": KnownAddress.Source.IMPORT,
            },
        )
        imported += 1
    return imported
