"""Fabriques de test de la qualification."""

from apps.wallets.services.settings import QualificationThresholds

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
