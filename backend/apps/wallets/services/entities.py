"""Entités : wallets d'une même personne, reliés par des transferts ou un financement."""

from collections import defaultdict

from django.db.models import Q

from apps.discovery.models import Wallet
from apps.wallets.models import BLOCKING_KINDS, Entity, KnownAddress, WalletLink, WalletProfile
from apps.wallets.services.classify import BUY, RECEIVE, SEND
from apps.wallets.services.exchanges import register
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


def add_link(from_address: str, to_address: str, kind: str, evidence: dict) -> None:
    source, _ = Wallet.objects.get_or_create(address=from_address.lower())
    target, _ = Wallet.objects.get_or_create(address=to_address.lower())
    WalletLink.objects.get_or_create(
        from_wallet=source, to_wallet=target, kind=kind, defaults={"evidence": evidence}
    )


def follow(address: str, parent: WalletProfile, chain, t) -> None:
    if parent.depth + 1 > t.follow_depth:
        return
    wallet, _ = Wallet.objects.get_or_create(address=address.lower())
    profile, created = WalletProfile.objects.get_or_create(
        wallet=wallet,
        defaults={
            "source": WalletProfile.Source.LINKED,
            "depth": parent.depth + 1,
            "chains": [chain.pk],
        },
    )
    if not created and chain.pk not in profile.chains:
        profile.chains = [*profile.chains, chain.pk]
        profile.save(update_fields=["chains"])


def funder_is_service(funder: str, chain, t) -> bool:
    funded = WalletLink.objects.filter(
        from_wallet__address=funder, kind=WalletLink.Kind.FUNDING
    ).count()
    if funded >= t.funder_max_wallets:
        register(funder, chain, KnownAddress.Kind.SERVICE, "financeur massif (auto)")
        return True
    return False


def refresh_entity(wallet) -> Entity:
    """Regroupe les wallets reliés à `wallet` (liens hors adresses bloquantes) dans une entité."""
    blocked = KnownAddress.objects.filter(kind__in=BLOCKING_KINDS).values("address")
    links = WalletLink.objects.exclude(from_wallet__address__in=blocked).exclude(
        to_wallet__address__in=blocked
    )
    members = {wallet.pk}
    frontier = {wallet.pk}
    while frontier:
        pairs = links.filter(
            Q(from_wallet_id__in=frontier) | Q(to_wallet_id__in=frontier)
        ).values_list("from_wallet_id", "to_wallet_id")
        reached = {wallet_id for pair in pairs for wallet_id in pair}
        frontier = reached - members
        members |= reached
    profiles = WalletProfile.objects.filter(wallet_id__in=members)
    entity_ids = sorted({pk for pk in profiles.values_list("entity_id", flat=True) if pk})
    entity = Entity.objects.get(pk=entity_ids[0]) if entity_ids else Entity.objects.create()
    profiles.update(entity=entity)
    Entity.objects.filter(pk__in=entity_ids[1:]).delete()
    return entity
