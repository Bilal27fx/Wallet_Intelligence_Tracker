"""Prix : mouvements (par la contrepartie), actifs de cotation, prix natifs, valeur d'un wallet.

Zerion ne sert qu'aux prix : lots de prix actuels, courbes quotidiennes, adresses des stablecoins.
"""

from bisect import bisect_right
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from django.core.exceptions import ImproperlyConfigured

from apps.discovery.models import Chain, Token
from apps.wallets.models import DailyPrice, KnownAddress, TokenPosition
from apps.wallets.services.classify import BUY, SELL, Trade
from integrations.errors import BudgetExhausted, IntegrationError

STABLE = "stable"
WRAPPED = "wrapped"
NATIVE_DECIMALS = 18


@dataclass(frozen=True)
class QuoteAsset:
    kind: str
    decimals: int


def price_trades(
    trades: list[Trade],
    wallet: str,
    quotes: dict[str, QuoteAsset],
    native_price: Callable[[int], float | None],
) -> dict[tuple[str, int], float | None]:
    """Prix en $ de chaque mouvement, déduit de la contrepartie dans la même transaction. Pur."""
    wallet = wallet.lower()
    usd: dict[tuple[str, int], float | None] = {}
    by_tx: dict[str, list[Trade]] = defaultdict(list)
    for trade in trades:
        by_tx[trade.transfer.tx_hash].append(trade)

    for legs in by_tx.values():
        first = legs[0].transfer
        price = native_price(first.timestamp)
        paid = 0.0
        received = 0.0
        for leg in legs:
            quote = quotes.get(leg.transfer.token)
            if quote is None:
                continue
            amount = leg.transfer.amount / 10**quote.decimals
            value = (
                amount if quote.kind == STABLE else (amount * price if price is not None else None)
            )
            usd[(leg.transfer.tx_hash, leg.transfer.log_index)] = (
                round(value, 2) if value is not None else None
            )
            if value is None:
                continue
            if leg.kind == SELL:
                paid += value
            elif leg.kind == BUY:
                received += value
        if first.tx_from == wallet and first.tx_value > 0 and price is not None:
            paid += first.tx_value / 10**NATIVE_DECIMALS * price

        for kind, total in ((BUY, paid), (SELL, received)):
            targets = [leg for leg in legs if leg.kind == kind and leg.transfer.token not in quotes]
            if targets and total > 0 and len({leg.transfer.token for leg in targets}) == 1:
                amount = sum(leg.transfer.amount for leg in targets)
                for leg in targets:
                    usd[(leg.transfer.tx_hash, leg.transfer.log_index)] = round(
                        total * leg.transfer.amount / amount, 2
                    )
        for leg in legs:
            usd.setdefault((leg.transfer.tx_hash, leg.transfer.log_index), None)
    return usd


def _register_quote(fungible, chains_by_zerion: dict[str, Chain], kind: str) -> int:
    registered = 0
    for zerion_id, (address, decimals) in fungible.implementations.items():
        chain = chains_by_zerion.get(zerion_id)
        if chain is None:
            continue
        Token.objects.update_or_create(
            chain=chain,
            address=address,
            defaults={"symbol": fungible.symbol[:64], "decimals": decimals},
        )
        KnownAddress.objects.update_or_create(
            chain=chain,
            address=address,
            defaults={
                "kind": kind,
                "label": fungible.symbol[:128],
                "source": KnownAddress.Source.AUTO,
            },
        )
        registered += 1
    return registered


def sync_quote_assets(zerion, cfg) -> int:
    """Natifs wrappés (chaînes qui les utilisent) et stablecoins, adresses récupérées sur Zerion."""
    chains = [chain for chain in Chain.objects.active() if chain.zerion_id]
    registered = 0
    wrapped_ids = sorted(
        {chain.wrapped_fungible_id for chain in chains if chain.wrapped_fungible_id}
    )
    for fungible_id in wrapped_ids:
        users = {c.zerion_id: c for c in chains if c.wrapped_fungible_id == fungible_id}
        registered += _register_quote(
            zerion.fungible(fungible_id), users, KnownAddress.Kind.WRAPPED_NATIVE
        )
    all_chains = {c.zerion_id: c for c in chains}
    for symbol in cfg.stablecoin_symbols:
        found = zerion.search(symbol)
        if found is not None:
            registered += _register_quote(found, all_chains, KnownAddress.Kind.STABLECOIN)
    return registered


def quote_assets(chain: Chain) -> dict[str, QuoteAsset]:
    known = list(
        KnownAddress.objects.filter(
            chain=chain, kind__in=[KnownAddress.Kind.STABLECOIN, KnownAddress.Kind.WRAPPED_NATIVE]
        )
    )
    decimals = dict(
        Token.objects.filter(chain=chain, address__in=[k.address for k in known]).values_list(
            "address", "decimals"
        )
    )
    return {
        k.address: QuoteAsset(
            STABLE if k.kind == KnownAddress.Kind.STABLECOIN else WRAPPED,
            decimals.get(k.address, 18),
        )
        for k in known
    }


def quote_tokens(chains: list[Chain]) -> set[str]:
    return set(
        KnownAddress.objects.filter(
            chain__in=chains,
            kind__in=[KnownAddress.Kind.STABLECOIN, KnownAddress.Kind.WRAPPED_NATIVE],
        ).values_list("address", flat=True)
    )


def native_price_lookup(chain: Chain, zerion, now: datetime) -> Callable[[int], float | None]:
    """Prix quotidien de l'actif natif. Courbe Zerion (1 an) récupérée au plus une fois par jour."""
    fungible_id = chain.native_fungible_id
    if not fungible_id:
        return lambda ts: None
    latest = DailyPrice.objects.filter(fungible_id=fungible_id).order_by("-day").first()
    if latest is None or latest.day < now.date() - timedelta(days=1):
        by_day = {
            datetime.fromtimestamp(ts, UTC).date(): price
            for ts, price in zerion.price_chart(fungible_id)
        }
        DailyPrice.objects.bulk_create(
            [DailyPrice(fungible_id=fungible_id, day=day, usd=usd) for day, usd in by_day.items()],
            update_conflicts=True,
            unique_fields=["fungible_id", "day"],
            update_fields=["usd"],
        )
    prices = dict(DailyPrice.objects.filter(fungible_id=fungible_id).values_list("day", "usd"))
    days = sorted(prices)

    def lookup(ts: int) -> float | None:
        index = bisect_right(days, datetime.fromtimestamp(ts, UTC).date()) - 1
        return prices[days[index]] if index >= 0 else None

    return lookup


def wallet_value(wallet, chains: list[Chain], zerion, rpc_for, now: datetime) -> tuple[float, dict]:
    """Soldes de tokens × prix Zerion + solde natif (RPC public). Lève BudgetExhausted."""
    total = 0.0
    details: dict = {}
    for chain in chains:
        positions = [
            p
            for p in TokenPosition.objects.filter(wallet=wallet, token__chain=chain).select_related(
                "token"
            )
            if p.balance > 0
        ]
        prices = (
            zerion.prices([(chain.zerion_id, p.token.address) for p in positions])
            if positions
            else {}
        )
        tokens_usd = 0.0
        for position in positions:
            fungible = prices.get((chain.zerion_id, position.token.address))
            if fungible is None or fungible.price is None:
                continue
            decimals = fungible.implementations[chain.zerion_id][1]
            tokens_usd += float(position.balance) / 10**decimals * fungible.price
        native_price = native_price_lookup(chain, zerion, now)(int(now.timestamp()))
        entry = {"tokens_usd": round(tokens_usd, 2), "native_usd": 0.0}
        try:
            balance = rpc_for(chain).native_balance(wallet.address)
        except BudgetExhausted:
            raise
        except (IntegrationError, ImproperlyConfigured):
            entry["native_error"] = True
        else:
            entry["native_usd"] = round(balance / 10**NATIVE_DECIMALS * (native_price or 0.0), 2)
        details[chain.gt_id] = entry
        total += entry["tokens_usd"] + entry["native_usd"]
    return round(total, 2), details
