"""Entités : wallets d'une même personne, regroupés par liens forts.

Un seul point d'entrée pour la découverte (HyperSync) et la qualification (Zerion) : un lien
fort crée l'entité, y rattache un wallet ou fusionne deux entités. Un rattachement refusé
dans l'admin (lien `rejected`) n'est jamais recréé.
"""

from django.db import transaction
from django.db.models import Q

from apps.discovery.models import EarlyBuyer, Entity, Explosion, Wallet
from apps.discovery.services.entity_buys import refresh_entity_buys
from apps.wallets.models import STRONG_LINKS, WalletLink


def ensure_entity(wallet: Wallet) -> Entity:
    if wallet.entity_id is None:
        wallet.entity = Entity.objects.create()
        wallet.save(update_fields=["entity"])
    return wallet.entity


def _refresh(wallet_filter: Q) -> None:
    """Reporte l'entité courante des wallets sur leurs early buys, puis recalcule les rangs."""
    explosions = set()
    for row in EarlyBuyer.objects.filter(wallet_filter).select_related("wallet"):
        explosions.add(row.explosion_id)
        if row.entity_id != row.wallet.entity_id:
            row.entity_id = row.wallet.entity_id
            row.save(update_fields=["entity"])
    for explosion in Explosion.objects.filter(pk__in=explosions):
        refresh_entity_buys(explosion)


def merge(keep: Entity, gone: Entity) -> Entity:
    if keep.pk == gone.pk:
        return keep
    Wallet.objects.filter(entity=gone).update(entity=keep)
    gone.merged_into = keep
    gone.save(update_fields=["merged_into"])
    _refresh(Q(wallet__entity=keep) | Q(entity=gone))
    return keep


def _join(a: Wallet, b: Wallet) -> None:
    a.refresh_from_db()
    b.refresh_from_db()
    if a.entity_id and b.entity_id:
        if a.entity_id != b.entity_id:
            keep, gone = sorted([a.entity, b.entity], key=lambda entity: entity.pk)
            merge(keep, gone)
        return
    entity_id = a.entity_id or b.entity_id or Entity.objects.create().pk
    Wallet.objects.filter(pk__in=[a.pk, b.pk]).update(entity_id=entity_id)
    _refresh(Q(wallet__entity_id=entity_id))


def link(from_address: str, to_address: str, kind: str, source: str, evidence: dict):
    a, _ = Wallet.objects.get_or_create(address=from_address.lower())
    b, _ = Wallet.objects.get_or_create(address=to_address.lower())
    pair = Q(from_wallet=a, to_wallet=b) | Q(from_wallet=b, to_wallet=a)
    if WalletLink.objects.filter(pair, rejected=True).exists():
        return None
    with transaction.atomic():
        created, _ = WalletLink.objects.get_or_create(
            from_wallet=a,
            to_wallet=b,
            kind=kind,
            defaults={"evidence": evidence, "source": source},
        )
        if kind in STRONG_LINKS:
            _join(a, b)
    return created


def detach(wallet: Wallet) -> None:
    """Sort le wallet de son entité (il redevient seul) ; ses liens forts ne seront plus recréés."""
    old = wallet.entity_id
    with transaction.atomic():
        WalletLink.objects.filter(
            Q(from_wallet=wallet) | Q(to_wallet=wallet), kind__in=STRONG_LINKS
        ).update(rejected=True)
        wallet.entity = Entity.objects.create()
        wallet.save(update_fields=["entity"])
        _refresh(Q(wallet=wallet) | Q(wallet__entity_id=old))
