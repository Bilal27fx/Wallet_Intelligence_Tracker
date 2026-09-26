"""Collecte des candidats (nos « top gainers ») et ajout manuel."""

from datetime import datetime, timedelta

from django.db import IntegrityError, transaction

from apps.discovery.models import CLOSED_STATUSES, Candidate, Chain, PipelineSettings, Pool, Token
from apps.discovery.services.settings import Thresholds, thresholds_for
from integrations.geckoterminal import GtPool

SOURCE_TRENDING = "trending"
SOURCE_VOLUME = "volume"
SOURCE_MANUAL = "manual"


def passes_prefilter(pool: GtPool, thresholds: Thresholds, now: datetime) -> bool:
    if pool.created_at is None:
        return False
    age_hours = (now - pool.created_at).total_seconds() / 3600
    return (
        pool.price_change_24h_pct >= thresholds.min_change_24h_pct
        and pool.liquidity_usd >= thresholds.min_liquidity_usd
        and pool.volume_24h_usd >= thresholds.min_volume_usd
        and thresholds.min_fdv_usd <= pool.fdv_usd <= thresholds.max_fdv_usd
        and age_hours <= thresholds.max_pool_age_hours
    )


def _metrics(pool: GtPool) -> dict:
    return {
        "price_change_24h_pct": pool.price_change_24h_pct,
        "volume_24h_usd": pool.volume_24h_usd,
        "liquidity_usd": pool.liquidity_usd,
        "fdv_usd": pool.fdv_usd,
    }


def upsert_token_and_pools(chain: Chain, pools: list[GtPool]) -> Token:
    first = pools[0]
    token, _ = Token.objects.get_or_create(
        chain=chain,
        address=first.token_address,
        defaults={"symbol": first.token_symbol[:64], "decimals": first.token_decimals or 18},
    )
    for pool in pools:
        Pool.objects.get_or_create(token=token, address=pool.address)
    return token


def _recently_closed(token: Token, cooldown_hours: int, now: datetime) -> bool:
    return token.candidates.filter(
        status__in=CLOSED_STATUSES, updated_at__gte=now - timedelta(hours=cooldown_hours)
    ).exists()


def _create_candidate(token: Token, sources: list[str], metrics: dict) -> Candidate | None:
    try:
        with transaction.atomic():
            return Candidate.objects.create(token=token, sources=sources, metrics=metrics)
    except IntegrityError:
        return None


def collect_candidates(gt, cfg: PipelineSettings, now: datetime) -> int:
    chains = {chain.gt_id: chain for chain in Chain.objects.active()}
    found: dict[tuple[str, str], dict] = {}

    def add(pool: GtPool, source: str) -> None:
        if pool.network not in chains:
            return
        entry = found.setdefault(
            (pool.network, pool.token_address), {"best": pool, "pools": {}, "sources": set()}
        )
        entry["pools"][pool.address] = pool
        entry["sources"].add(source)
        if pool.liquidity_usd > entry["best"].liquidity_usd:
            entry["best"] = pool

    for page in range(1, cfg.trending_pages + 1):
        pools = gt.trending_pools(page)
        if not pools:
            break
        for pool in pools:
            add(pool, SOURCE_TRENDING)
    for gt_id in chains:
        for page in range(1, cfg.volume_pages_per_chain + 1):
            pools = gt.top_volume_pools(gt_id, page)
            if not pools:
                break
            for pool in pools:
                add(pool, SOURCE_VOLUME)

    thresholds = {gt_id: thresholds_for(chain) for gt_id, chain in chains.items()}
    kept = [
        entry
        for entry in found.values()
        if passes_prefilter(entry["best"], thresholds[entry["best"].network], now)
    ]
    kept.sort(key=lambda entry: entry["best"].price_change_24h_pct, reverse=True)

    created = 0
    for entry in kept:
        best = entry["best"]
        pools = [best] + [p for a, p in entry["pools"].items() if a != best.address]
        token = upsert_token_and_pools(chains[best.network], pools)
        if _recently_closed(token, cfg.candidate_cooldown_hours, now):
            continue
        if _create_candidate(token, sorted(entry["sources"]), _metrics(best)):
            created += 1
    return created


def add_manual_candidate(chain: Chain, token_address: str, gt) -> Candidate:
    address = token_address.strip().lower()
    pools = gt.token_pools(chain.gt_id, address)
    if not pools:
        raise ValueError(f"Aucun pool trouvé pour {address} sur {chain.gt_id}")
    best = max(pools, key=lambda pool: pool.liquidity_usd)
    token = upsert_token_and_pools(chain, [best] + [p for p in pools if p is not best])
    candidate = _create_candidate(token, [SOURCE_MANUAL], _metrics(best))
    if candidate is None:
        raise ValueError(f"{address} a déjà un candidat en cours")
    return candidate
