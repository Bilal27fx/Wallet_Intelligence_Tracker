"""Construction des clients externes depuis les réglages du pipeline et les clés d'environnement."""

import redis
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from apps.discovery.models import Chain, PipelineSettings
from integrations import coingecko as cg
from integrations import geckoterminal as gt
from integrations import zerion as zr
from integrations.http import JsonHttpClient
from integrations.hypersync import CHAINS_URL, HyperSyncClient, HyperSyncDirectory
from integrations.ratelimit import DailyBudget, NoopLimiter, RateLimiter
from integrations.rpc import RpcClient


def _redis() -> redis.Redis:
    return redis.Redis.from_url(settings.REDIS_URL)


def _http(base_url: str, cfg: PipelineSettings, limiter=None, **kwargs) -> JsonHttpClient:
    return JsonHttpClient(
        base_url,
        limiter=limiter or NoopLimiter(),
        timeout=cfg.http_timeout_seconds,
        max_retries=cfg.http_max_retries,
        backoff_seconds=cfg.http_backoff_seconds,
        **kwargs,
    )


def geckoterminal(cfg: PipelineSettings) -> gt.GeckoTerminalClient:
    limiter = RateLimiter(_redis(), "geckoterminal", cfg.gecko_requests_per_min)
    return gt.GeckoTerminalClient(_http(gt.BASE_URL, cfg, limiter))


def coingecko(cfg: PipelineSettings) -> cg.CoinGeckoClient:
    headers = (
        {"x-cg-demo-api-key": settings.COINGECKO_API_KEY} if settings.COINGECKO_API_KEY else None
    )
    return cg.CoinGeckoClient(_http(cg.BASE_URL, cfg, headers=headers))


def zerion(cfg: PipelineSettings) -> zr.ZerionClient:
    if not settings.ZERION_API_KEY:
        raise ImproperlyConfigured("ZERION_API_KEY manquante")
    limiter = RateLimiter(_redis(), "zerion", cfg.zerion_requests_per_min)
    budget = DailyBudget(_redis(), "zerion", cfg.zerion_daily_budget)
    return zr.ZerionClient(
        _http(zr.BASE_URL, cfg, limiter, auth=(settings.ZERION_API_KEY, ""), budget=budget)
    )


def rpc(chain: Chain, cfg: PipelineSettings) -> RpcClient:
    if not chain.rpc_url:
        raise ImproperlyConfigured(f"{chain.gt_id} : aucun RPC public connu")
    return RpcClient(_http(chain.rpc_url, cfg))


def hypersync_directory(cfg: PipelineSettings) -> HyperSyncDirectory:
    return HyperSyncDirectory(_http(CHAINS_URL, cfg))


def hypersync(chain: Chain, cfg: PipelineSettings) -> HyperSyncClient:
    if not settings.ENVIO_API_TOKEN:
        raise ImproperlyConfigured("ENVIO_API_TOKEN manquant")
    limiter = RateLimiter(_redis(), "hypersync", cfg.hypersync_requests_per_min)
    return HyperSyncClient(
        chain.evm_id, settings.ENVIO_API_TOKEN, limiter, max_retries=cfg.http_max_retries
    )
