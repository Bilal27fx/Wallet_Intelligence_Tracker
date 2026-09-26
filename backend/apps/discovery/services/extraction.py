"""Extraction des early buyers d'un candidat confirmé."""

from datetime import UTC, datetime
from decimal import Decimal

from django.db import transaction

from apps.discovery.models import Candidate, EarlyBuyer, PipelineSettings, Wallet
from apps.discovery.services.analysis import fetch_price_history, reject
from apps.discovery.services.buyers import aggregate_buyers
from apps.discovery.services.settings import thresholds_for
from integrations.errors import TooManyTransfers

BATCH_SIZE = 1000


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

    try:
        transfers = hypersync.transfers(
            token.address, first_block, explosion.peak_block + 1, cfg.max_transfers_per_token
        )
    except TooManyTransfers:
        reject(candidate, "too_many_transfers")
        return 0

    history = fetch_price_history(gt, chain, token, now)
    buyers = aggregate_buyers(
        transfers,
        low_block=explosion.low_block,
        peak_block=explosion.peak_block,
        pool_created_block=first_block,
        candles=history.candles,
        decimals=token.decimals,
        sniper_blocks=thresholds.sniper_blocks,
        min_buy_usd=thresholds.min_buy_usd,
        max_buyers=thresholds.max_buyers,
    )

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
        candidate.status = Candidate.Status.BUYERS_EXTRACTED
        candidate.save(update_fields=["status", "updated_at"])
    return len(buyers)
