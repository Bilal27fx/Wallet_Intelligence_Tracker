"""Fabriques de test de la qualification."""

from apps.discovery.models import Candidate, EarlyBuyer, Explosion, Token
from apps.wallets.services.settings import QualificationThresholds
from apps.wallets.tests.fakes import NOW

DEFAULT_THRESHOLDS = dict(
    max_txs_per_day=200,
    max_distinct_tokens=300,
    inactive_days=90,
    min_txs_active=5,
    history_days=365,
    max_mev_ratio=30.0,
    min_distinct_buys=3,
    min_portfolio_usd=10_000.0,
    max_portfolio_usd=50_000_000.0,
    transfer_after_buy_pct=70.0,
    follow_depth=1,
    funder_max_wallets=50,
    hot_wallet_min_counterparties=1000,
    deposit_forward_pct=90.0,
    deposit_forward_hours=24,
    flipper_hours=24,
    flipper_min_sold_pct=80.0,
    flipper_min_share_pct=50.0,
    holder_min_pct=50.0,
    accumulator_max_out_pct=20.0,
    accumulator_min_positions=2,
    early_buyer_min_explosions=2,
    refilter_after_days=30,
)


def make_thresholds(**overrides) -> QualificationThresholds:
    return QualificationThresholds(**{**DEFAULT_THRESHOLDS, **overrides})


def make_early_buy(wallet, chain, token_address, is_sniper=False, bought=100, sold=0) -> EarlyBuyer:
    token, _ = Token.objects.get_or_create(chain=chain, address=token_address)
    candidate = Candidate.objects.create(token=token, status="buyers_extracted")
    explosion = Explosion.objects.create(
        candidate=candidate,
        low_block=1,
        low_at=NOW,
        peak_block=2,
        peak_at=NOW,
        multiplier=10,
        retention_pct=50,
    )
    return EarlyBuyer.objects.create(
        explosion=explosion,
        wallet=wallet,
        first_buy_block=1,
        first_buy_at=NOW,
        bought_amount=bought,
        bought_usd=1000,
        sold_amount=sold,
        is_sniper=is_sniper,
    )
