"""Extraction des early buyers d'une explosion, par entité.

Passe 1 : tous les transferts de la fenêtre d'achat au creux, agrégés page par page (flux,
coffres, positions au creux). Sélection : top des entités, bots écartés. Passe entité :
transferts des wallets retenus jusqu'au pic (bruts enregistrés, ventes pendant la montée,
coffres suivis sur `vault_follow_depth` niveaux).
"""

import logging
import time
from datetime import UTC, datetime
from decimal import Decimal

import redis
from django.conf import settings
from django.db import transaction

from apps.discovery.models import (
    Candidate,
    EarlyBuyer,
    ExcludedBuyer,
    Explosion,
    PipelineSettings,
    Wallet,
)
from apps.discovery.services.analysis import fetch_price_history, reject
from apps.discovery.services.blocks import find_block_at, find_block_near
from apps.discovery.services.entity_buys import refresh_entity_buys
from apps.discovery.services.flows import (
    FlowScanner,
    Selection,
    classify_recipients,
    select_entities,
)
from apps.discovery.services.settings import Thresholds, thresholds_for
from apps.wallets.models import BLOCKING_KINDS, KnownAddress, WalletLink
from apps.wallets.services import entity_graph
from apps.wallets.services.settings import qualification_thresholds

BATCH_SIZE = 1000
LOG_EVERY_PAGES = 20

logger = logging.getLogger(__name__)
DAY = 86_400


def _at(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=UTC)


def _usd(value: float) -> Decimal:
    return Decimal(str(round(value, 2)))


def buy_window_start(
    explosion: Explosion, first_block: int, thresholds: Thresholds, hypersync
) -> int:
    """Début de la fenêtre d'achat : `buyer_window_hours` avant le creux, jamais avant le pool."""
    if thresholds.buyer_window_hours <= 0:
        return first_block
    start_ts = int(explosion.trough_at.timestamp()) - thresholds.buyer_window_hours * 3600
    return find_block_at(start_ts, first_block, explosion.trough_block, hypersync.block_timestamp)


def bot_checker(hypersync, chain, thresholds: Thresholds, q, now: datetime):
    """Activité actuelle : tx signées par jour sur `bot_window_days`, par lots, en cache."""
    days = thresholds.bot_window_days
    limit = q.max_txs_per_day * days
    height = hypersync.height()
    start = find_block_near(int(now.timestamp()) - days * DAY, height, hypersync.block_timestamp)
    cache = redis.Redis.from_url(settings.REDIS_URL)
    prefix = f"botrate:{chain.pk}:{now:%Y-%m-%d}:{days}:"

    def check(addresses: list[str]) -> dict[str, tuple[bool, float]]:
        cached = dict(zip(addresses, cache.mget([prefix + a for a in addresses]), strict=True))
        counts = {a: int(v) for a, v in cached.items() if v is not None}
        missing = [a for a, v in cached.items() if v is None]
        if missing:
            fresh = hypersync.tx_counts(missing, start, height + 1)
            counts.update(fresh)
            pipe = cache.pipeline()
            for address, count in fresh.items():
                pipe.set(prefix + address, count, ex=DAY)
            pipe.execute()
        return {a: (counts.get(a, 0) > limit, round(counts.get(a, 0) / days, 1)) for a in addresses}

    return check


def extract_buyers(
    candidate: Candidate, *, gt, hypersync, cfg: PipelineSettings, now: datetime
) -> int:
    """Retourne le nombre d'entités retenues."""
    token = candidate.token
    chain = token.chain
    explosion = candidate.explosion
    thresholds = thresholds_for(chain)
    q = qualification_thresholds()

    pools = list(token.pools.exclude(created_block__isnull=True))
    if not pools:
        reject(candidate, "no_pool")
        return 0
    first_block = min(pool.created_block for pool in pools)
    history = fetch_price_history(gt, chain, token, now)

    scanner = FlowScanner(
        candles=history.candles,
        decimals=token.decimals,
        pools={pool.address.lower() for pool in token.pools.all()},
        hub_min_senders=thresholds.hub_min_senders,
    )
    label = f"[extraction] {token.symbol or token.address}"
    status = Explosion.Extraction.COMPLETE
    seen = pages = 0
    started = time.monotonic()
    start = buy_window_start(explosion, first_block, thresholds, hypersync)
    logger.info("%s passe 1 : blocs %s → %s", label, start, explosion.trough_block)
    for page in hypersync.transfer_pages(token.address, start, explosion.trough_block + 1):
        scanner.add(page)
        seen += len(page)
        pages += 1
        if pages % LOG_EVERY_PAGES == 0:
            logger.info(
                "%s passe 1 : %s pages, %s transferts, %.0fs",
                label,
                pages,
                seen,
                time.monotonic() - started,
            )
        if seen >= cfg.max_transfers_per_token:
            status = Explosion.Extraction.PARTIAL
            break
    logger.info(
        "%s passe 1 terminée : %s transferts, %s acheteurs, %.0fs",
        label,
        seen,
        len(scanner.bought),
        time.monotonic() - started,
    )

    known = set(
        KnownAddress.objects.filter(kind__in=BLOCKING_KINDS).values_list("address", flat=True)
    )
    kinds = classify_recipients(
        scanner,
        known=known,
        deposit_forward_pct=q.deposit_forward_pct,
        deposit_forward_hours=q.deposit_forward_hours,
    )
    trough_price = scanner.price_at(int(explosion.trough_at.timestamp()))
    scale = 10**token.decimals
    started = time.monotonic()
    selection = select_entities(
        scanner,
        kinds,
        bot_check=bot_checker(hypersync, chain, thresholds, q, now),
        batch_size=max(cfg.sell_pass_batch_size, 1),
        big_pct=thresholds.vault_min_pct,
        trough_price=trough_price,
        scale=scale,
        min_usd=thresholds.min_buy_usd,
        max_entities=thresholds.max_buyers,
    )
    logger.info(
        "%s sélection : %s entités, %s bots écartés, %.0fs",
        label,
        len(selection.selected),
        len(selection.bots),
        time.monotonic() - started,
    )

    # Après le creux (ventes, nouveaux coffres), l'historique Zerion de la qualification prend
    # le relais : relire la montée ici coûte des millions de transferts pour peu d'information.
    members = sorted({w for candidate in selection.selected for w in candidate.wallets})
    started = time.monotonic()
    with transaction.atomic():
        _save_links(token, selection, members)
        _save_buyers(
            explosion, selection, members, scanner, first_block, thresholds, trough_price, scale
        )
        explosion.extraction_status = status
        explosion.save(update_fields=["extraction_status"])
        candidate.status = Candidate.Status.BUYERS_EXTRACTED
        candidate.save(update_fields=["status", "updated_at"])
    logger.info("%s enregistrement : %.0fs", label, time.monotonic() - started)
    return len(selection.selected)


def _save_links(token, selection: Selection, members: list[str]) -> None:
    retained = set(members)
    base = {"chain": token.chain.gt_id, "token": token.address}
    for link in selection.flows.links:
        if link.sender in retained:
            entity_graph.link(
                link.sender,
                link.recipient,
                WalletLink.Kind.TRANSFER_TO_VAULT,
                WalletLink.LinkSource.HYPERSYNC,
                {
                    **base,
                    "pct": link.pct,
                    "amount": str(link.amount),
                    "block": link.first_block,
                    "tx": link.tx_hash,
                },
            )


def _save_buyers(
    explosion, selection, members, scanner, first_block, thresholds, trough_price, scale
) -> None:
    holders = selection.flows.holders
    Wallet.objects.bulk_create([Wallet(address=a) for a in members], ignore_conflicts=True)
    wallets = {w.address: w for w in Wallet.objects.filter(address__in=members)}
    for wallet in wallets.values():
        entity_graph.ensure_entity(wallet)

    rows = []
    for address in members:
        holder = holders[address]
        own = scanner.first_buy.get(address)
        rows.append(
            EarlyBuyer(
                explosion=explosion,
                wallet=wallets[address],
                entity_id=wallets[address].entity_id,
                first_buy_block=holder.first_block or explosion.trough_block,
                first_buy_at=_at(holder.first_ts or int(explosion.trough_at.timestamp())),
                bought_amount=Decimal(holder.bought),
                bought_usd=_usd(holder.cost),
                held_amount=Decimal(holder.held),
                held_usd=_usd(holder.held / scale * trough_price),
                inherited_amount=Decimal(holder.inherited),
                inherited_usd=_usd(holder.inherited_cost),
                inherited_from=wallets.get(holder.inherited_from),
                is_sniper=own is not None and own[0] - first_block <= thresholds.sniper_blocks,
            )
        )
    EarlyBuyer.objects.bulk_create(rows, ignore_conflicts=True, batch_size=BATCH_SIZE)
    # Les liens ont pu fusionner des entités après la création des lignes : on relit.
    for row in EarlyBuyer.objects.filter(explosion=explosion).select_related("wallet"):
        if row.entity_id != row.wallet.entity_id:
            row.entity_id = row.wallet.entity_id
            row.save(update_fields=["entity"])
    refresh_entity_buys(explosion)

    bots = selection.bots
    Wallet.objects.bulk_create([Wallet(address=a) for a in bots], ignore_conflicts=True)
    bot_wallets = {w.address: w for w in Wallet.objects.filter(address__in=list(bots))}
    ExcludedBuyer.objects.bulk_create(
        [
            ExcludedBuyer(
                explosion=explosion,
                wallet=bot_wallets[address],
                reason="bot",
                txs_per_day=Decimal(str(per_day)),
                held_usd=_usd(usd),
            )
            for address, (per_day, usd) in bots.items()
        ],
        ignore_conflicts=True,
    )
