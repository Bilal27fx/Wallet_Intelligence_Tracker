"""Extraction des early buyers d'une explosion, page par page (mémoire ∝ nombre d'acheteurs)."""

from datetime import UTC, datetime
from decimal import Decimal

from django.db import transaction

from apps.discovery.models import Candidate, EarlyBuyer, Explosion, PipelineSettings, Wallet
from apps.discovery.services.analysis import fetch_price_history, reject
from apps.discovery.services.blocks import find_block_at
from apps.discovery.services.buyers import BuyerAggregator
from apps.discovery.services.settings import Thresholds, thresholds_for

BATCH_SIZE = 1000


def buy_window_start(
    explosion: Explosion, first_block: int, thresholds: Thresholds, hypersync
) -> int:
    """Début de la fenêtre d'achat : `buyer_window_hours` avant le creux, jamais avant le pool."""
    if thresholds.buyer_window_hours <= 0:
        return first_block
    start_ts = int(explosion.trough_at.timestamp()) - thresholds.buyer_window_hours * 3600
    return find_block_at(start_ts, first_block, explosion.trough_block, hypersync.block_timestamp)


def extract_buyers(
    candidate: Candidate, *, gt, hypersync, cfg: PipelineSettings, now: datetime
) -> int:
    token = candidate.token
    chain = token.chain
    explosion = candidate.explosion
    thresholds = thresholds_for(chain)

    pools = list(token.pools.exclude(created_block__isnull=True))
    if not pools:
        reject(candidate, "no_pool")
        return 0
    first_block = min(pool.created_block for pool in pools)

    history = fetch_price_history(gt, chain, token, now)
    aggregator = BuyerAggregator(candles=history.candles, decimals=token.decimals)

    # Passe 1 : achats (et ventes) de la fenêtre d'achat jusqu'au creux.
    status = Explosion.Extraction.COMPLETE
    seen = 0
    start = buy_window_start(explosion, first_block, thresholds, hypersync)
    for page in hypersync.transfer_pages(token.address, start, explosion.trough_block + 1):
        aggregator.add(page)
        seen += len(page)
        if seen >= cfg.max_transfers_per_token:
            status = Explosion.Extraction.PARTIAL
            break

    buyers = aggregator.select(
        pool_created_block=first_block,
        sniper_blocks=thresholds.sniper_blocks,
        min_buy_usd=thresholds.min_buy_usd,
        max_buyers=thresholds.max_buyers,
    )

    # Passe 2 : ventes des acheteurs retenus, du creux au pic.
    wallets = sorted(buyer.wallet for buyer in buyers)
    batch = max(cfg.sell_pass_batch_size, 1)
    for offset in range(0, len(wallets), batch):
        for page in hypersync.transfer_pages(
            token.address,
            explosion.trough_block + 1,
            explosion.peak_block + 1,
            senders=wallets[offset : offset + batch],
        ):
            aggregator.add_sells(page)

    with transaction.atomic():
        Wallet.objects.bulk_create(
            [Wallet(address=buyer.wallet) for buyer in buyers],
            ignore_conflicts=True,
            batch_size=BATCH_SIZE,
        )
        wallet_ids = dict(
            Wallet.objects.filter(address__in=[b.wallet for b in buyers]).values_list(
                "address", "id"
            )
        )
        EarlyBuyer.objects.bulk_create(
            [
                EarlyBuyer(
                    explosion=explosion,
                    wallet_id=wallet_ids[buyer.wallet],
                    first_buy_block=buyer.first_buy_block,
                    first_buy_at=datetime.fromtimestamp(buyer.first_buy_ts, tz=UTC),
                    bought_amount=Decimal(buyer.bought_amount),
                    bought_usd=Decimal(str(buyer.bought_usd)),
                    sold_amount=Decimal(buyer.sold_amount),
                    is_sniper=buyer.is_sniper,
                )
                for buyer in buyers
            ],
            ignore_conflicts=True,
            batch_size=BATCH_SIZE,
        )
        explosion.extraction_status = status
        explosion.save(update_fields=["extraction_status"])
        candidate.status = Candidate.Status.BUYERS_EXTRACTED
        candidate.save(update_fields=["status", "updated_at"])
    return len(buyers)
