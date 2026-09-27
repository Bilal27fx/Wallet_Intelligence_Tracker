# Qualification des wallets v2 — Plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remplacer l'historique HyperSync à prix reconstruits par l'historique Zerion (6 mois, toutes chaînes EVM, prix et type de chaque mouvement), pré-filtrer le bruit gratuitement via HyperSync sur plusieurs chaînes, et juger chaque wallet avec ses wallets liés directs.

**Architecture:** Le pré-filtre HyperSync (bot, inactif, farmer, MEV, exchange) additionne les mesures sur `prefilter_chains` + la chaîne où le wallet a été repéré. Les survivants récupèrent leurs 6 mois Zerion page par page (curseur enregistré, reprise le lendemain si le budget est atteint). Les décisions (filtres Zerion, liens forts, valeur `/portfolio` du wallet + wallets liés, tags) ne sont prises que sur historique complet. Les entités en chaîne et la reconstruction des prix sont supprimées.

**Tech Stack:** Python 3.12, Django 5.2, PostgreSQL 16, Celery 5.5, Redis 7, httpx, hypersync 1.2, respx, pytest-django, ruff.

**Spec:** `docs/superpowers/specs/2026-09-27-qualification-v2-design.md`

## Global Constraints

- Zerion : **jamais** pour la découverte ni pour le pré-filtre. Uniquement `/wallets/{address}/transactions/` et `/wallets/{address}/portfolio` (et `/chains/` pour la synchro des chaînes).
- Chaque mouvement stocké porte les données Zerion : `operation_type`, `direction`, `quantity`, `amount`, `price_usd`, `value_usd`, `counterparty`, token (chaîne, adresse, symbole, décimales, id Zerion). Réponse brute conservée par transaction.
- Aucune valeur réglable en dur : seuils dans `QualificationSettings` (ligne globale), budgets et listes dans `PipelineSettings`.
- Budget Zerion quotidien respecté (`DailyBudget`) ; budget épuisé = reprise au prochain passage, jamais une erreur.
- Chaînes des mouvements = identifiant Zerion en texte (`base`, `robinhood`…) ; le natif est stocké avec l'adresse `native`.
- Seuils en **pourcentage** pour les liens (jamais en $).
- Tests sans réseau sauf `@pytest.mark.live` ; Redis de test isolé (fixture existante).
- Commandes via le `Makefile` dans le conteneur `web`.
- Chaque commit se termine par `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Écarts assumés avec la spec

1. **Statut `valued`** ajouté pour les wallets liés (valorisés, sans historique ni décision propre) : plus lisible qu'un `qualified` trompeur.
2. **Seuils globaux uniquement** : la surcharge par chaîne de `QualificationSettings` n'est plus utilisée (les mesures sont additionnées sur plusieurs chaînes). Le mécanisme reste en place.
3. **`quote_symbols`** (réglage, défaut ETH, WETH, BNB, WBNB, USDC, USDT, DAI, USDC.E, USDBC, FDUSD) : les jambes « monnaie de paiement » d'un trade ne comptent pas comme tokens achetés (filtres et tags). Le natif (`native`) est toujours une monnaie de paiement.
4. **MEV au pré-filtre** calculé sans exclure les monnaies de paiement : un aller-retour dans le même bloc sur ETH/USDC est lui aussi typique d'un bot.
5. **`history_refresh_days`** (7) : fréquence de la mise à jour incrémentale des wallets qualifiés.
6. Le client `integrations/rpc.py` reste (testé, inutilisé par la v2) ; `clients.rpc` et `Chain.rpc_url` restent également.
7. **Données de l'essai v1 remises à zéro** par migration (mouvements, positions, entités, liens, wallets liés) ; les profils d'early buyers repassent en `pending`.

## Carte des fichiers

| Fichier | Action |
|---|---|
| `backend/integrations/zerion.py` | + `ZerionTransfer`, `ZerionTransaction`, `TransactionsPage`, `Portfolio`, `transactions()`, `portfolio()` |
| `backend/apps/discovery/models.py` | `PipelineSettings` : + `prefilter_chains`, `quote_symbols`, `history_refresh_days` ; − `extra_chains`, `stablecoin_symbols` |
| `backend/apps/wallets/models.py` | Réécrit : `WalletProfile`, `WalletTransaction`, `TokenTrade`, `TokenPosition`, `WalletLink`, `KnownAddress`, `QualificationSettings` |
| `backend/apps/wallets/services/settings.py` | Champs de seuils v2 |
| `backend/apps/wallets/services/zerion_history.py` | Nouveau, pur : type de mouvement, lignes d'une transaction, monnaies de paiement |
| `backend/apps/wallets/services/filters.py` | `prefilter_reason` sur totaux |
| `backend/apps/wallets/services/entities.py` | Réécrit : liens forts (transfert après achat, gros transfert reçu), `add_link`, `ensure_linked_profile` |
| `backend/apps/wallets/services/tags.py` | − `entity_tags` |
| `backend/apps/wallets/services/qualification.py` | Réécrit : pré-filtre multi-chaînes, historique Zerion, décision, wallets liés, mise à jour |
| `backend/apps/wallets/tasks.py` | Réécrit : ordre de traitement, budget |
| `backend/apps/wallets/admin.py` | Réécrit |
| `backend/apps/wallets/services/pricing.py`, `forms.py`, `templates/` + tests v1 | Supprimés (formulaire et templates recréés à la tâche 9) |
| `backend/apps/wallets/tests/fakes.py` | Réécrit : scénario Zerion |

---

### Task 1: Client Zerion — historique et portfolio

**Files:**
- Modify: `backend/integrations/zerion.py`
- Create: `backend/integrations/tests/test_zerion_history.py`

**Interfaces:**
- Produces (dans `integrations.zerion`) :
  - `NATIVE = "native"`, `OPERATION_TYPES = "trade,send,receive,execute,mint,burn,claim"`.
  - `ZerionTransfer(index: int, chain: str, token_address: str, token_symbol: str, token_decimals: int, fungible_id: str, direction: str, amount: int, quantity: Decimal, price_usd: float | None, value_usd: float | None, sender: str, recipient: str)`.
  - `ZerionTransaction(zerion_id: str, chain: str, tx_hash: str, block: int | None, mined_at: datetime, operation_type: str, status: str, fee_usd: float | None, transfers: list[ZerionTransfer], raw: dict)`.
  - `TransactionsPage(transactions: list[ZerionTransaction], next_cursor: str | None)`.
  - `Portfolio(total_usd: float, by_chain: dict[str, float])`.
  - `ZerionClient.transactions(address: str, since: datetime, cursor: str | None = None) -> TransactionsPage` ; `ZerionClient.portfolio(address: str) -> Portfolio`.

- [ ] **Step 1: Écrire les tests**

`backend/integrations/tests/test_zerion_history.py` :

```python
from datetime import UTC, datetime
from decimal import Decimal

import respx

from integrations.http import JsonHttpClient
from integrations.ratelimit import NoopLimiter
from integrations.zerion import BASE_URL, NATIVE, OPERATION_TYPES, ZerionClient

W = "0x11edfaca715703cb91c384d84cd2551122ab3019"
TOKEN = "0xca7a1e31b36779cf32acb18714ab26982cf36b05"


def client() -> ZerionClient:
    return ZerionClient(JsonHttpClient(BASE_URL, limiter=NoopLimiter(), max_retries=0))


def transfer(direction, fungible_id, symbol, address, qty_int, numeric, value, price, sender, recipient):
    return {
        "direction": direction,
        "quantity": {"int": qty_int, "decimals": 18, "float": float(numeric), "numeric": numeric},
        "value": value,
        "price": price,
        "sender": sender,
        "recipient": recipient,
        "fungible_info": {
            "id": fungible_id,
            "symbol": symbol,
            "implementations": [{"chain_id": "base", "address": address, "decimals": 18}],
        },
    }


TRADE = {
    "id": "1e7eb8fb",
    "attributes": {
        "operation_type": "trade",
        "hash": "0x5fa7",
        "mined_at_block": 51740628,
        "mined_at": "2026-09-24T17:23:23Z",
        "status": "confirmed",
        "fee": {"value": 0.0375},
        "transfers": [
            transfer("in", "cat-id", "CATALYST", TOKEN.upper().replace("0X", "0x"), "743365907836862550998",
                     "743.365907836862550998", 1312.84, 1.766, "0xPOOL", W),
            transfer("out", "eth", "ETH", "", "500000000000000000", "0.5", 1339.82, 2679.65, W, "0xROUTER"),
        ],
    },
    "relationships": {"chain": {"data": {"id": "base"}}},
}

NEXT = (
    f"{BASE_URL}/wallets/{W}/transactions/?currency=usd&page%5Bafter%5D=WyIyMDI2Il0%3D&page%5Bsize%5D=100"
)


@respx.mock
def test_transactions_parses_transfers_and_cursor():
    route = respx.get(f"{BASE_URL}/wallets/{W}/transactions/").respond(
        json={"data": [TRADE], "links": {"next": NEXT}}
    )
    page = client().transactions(W, datetime(2026, 3, 1, tzinfo=UTC))
    [tx] = page.transactions
    assert (tx.zerion_id, tx.chain, tx.tx_hash, tx.block) == ("1e7eb8fb", "base", "0x5fa7", 51740628)
    assert (tx.operation_type, tx.status, tx.fee_usd) == ("trade", "confirmed", 0.0375)
    assert tx.mined_at == datetime(2026, 9, 24, 17, 23, 23, tzinfo=UTC)
    buy, pay = tx.transfers
    assert (buy.index, buy.direction, buy.token_address, buy.token_symbol) == (0, "in", TOKEN, "CATALYST")
    assert buy.amount == 743365907836862550998
    assert buy.quantity == Decimal("743.365907836862550998")
    assert (buy.price_usd, buy.value_usd, buy.sender, buy.recipient) == (1.766, 1312.84, "0xpool", W)
    assert (pay.index, pay.token_address, pay.fungible_id) == (1, NATIVE, "eth")
    assert tx.raw == TRADE
    assert page.next_cursor == "WyIyMDI2Il0="
    params = route.calls.last.request.url.params
    assert params["filter[operation_types]"] == OPERATION_TYPES
    assert params["filter[trash]"] == "only_non_trash"
    assert params["filter[min_mined_at]"] == str(int(datetime(2026, 3, 1, tzinfo=UTC).timestamp() * 1000))
    assert "page[after]" not in params


@respx.mock
def test_transactions_sends_cursor_and_ends_without_next():
    route = respx.get(f"{BASE_URL}/wallets/{W}/transactions/").respond(json={"data": [], "links": {}})
    page = client().transactions(W, datetime(2026, 3, 1, tzinfo=UTC), cursor="abc")
    assert page.transactions == [] and page.next_cursor is None
    assert route.calls.last.request.url.params["page[after]"] == "abc"


@respx.mock
def test_transfer_without_price():
    tx = {**TRADE, "attributes": {**TRADE["attributes"], "fee": None, "transfers": [
        transfer("in", "x", "OBSCURE", "0xabc", "5", "0.000000000000000005", None, None, "0xa", W)
    ]}}
    respx.get(f"{BASE_URL}/wallets/{W}/transactions/").respond(json={"data": [tx], "links": {}})
    [parsed] = client().transactions(W, datetime(2026, 3, 1, tzinfo=UTC)).transactions
    assert parsed.fee_usd is None
    assert (parsed.transfers[0].price_usd, parsed.transfers[0].value_usd) == (None, None)


@respx.mock
def test_portfolio():
    respx.get(f"{BASE_URL}/wallets/{W}/portfolio").respond(
        json={"data": {"attributes": {
            "total": {"positions": 89108.31},
            "positions_distribution_by_chain": {"base": 1447.2, "robinhood": 87615.1, "ethereum": 46.0},
        }}}
    )
    portfolio = client().portfolio(W)
    assert portfolio.total_usd == 89108.31
    assert portfolio.by_chain["robinhood"] == 87615.1
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="integrations/tests/test_zerion_history.py -q"`
Expected: FAIL (`ImportError: cannot import name 'NATIVE'`).

- [ ] **Step 3: Implémenter**

Ajouter à `backend/integrations/zerion.py` (imports `from datetime import datetime`, `from decimal import Decimal`, `from urllib.parse import parse_qs, urlparse` en tête) :

```python
NATIVE = "native"
OPERATION_TYPES = "trade,send,receive,execute,mint,burn,claim"


@dataclass(frozen=True)
class ZerionTransfer:
    index: int
    chain: str
    token_address: str
    token_symbol: str
    token_decimals: int
    fungible_id: str
    direction: str
    amount: int
    quantity: Decimal
    price_usd: float | None
    value_usd: float | None
    sender: str
    recipient: str


@dataclass(frozen=True)
class ZerionTransaction:
    zerion_id: str
    chain: str
    tx_hash: str
    block: int | None
    mined_at: datetime
    operation_type: str
    status: str
    fee_usd: float | None
    transfers: list[ZerionTransfer]
    raw: dict


@dataclass(frozen=True)
class TransactionsPage:
    transactions: list[ZerionTransaction]
    next_cursor: str | None


@dataclass(frozen=True)
class Portfolio:
    total_usd: float
    by_chain: dict[str, float]


def _optional_float(value) -> float | None:
    return float(value) if value is not None else None


def _transfer(index: int, chain: str, item: dict) -> ZerionTransfer:
    info = item.get("fungible_info") or {}
    implementation = next(
        (i for i in info.get("implementations") or [] if i.get("chain_id") == chain), {}
    )
    quantity = item.get("quantity") or {}
    return ZerionTransfer(
        index=index,
        chain=chain,
        token_address=(implementation.get("address") or NATIVE).lower(),
        token_symbol=info.get("symbol") or "",
        token_decimals=int(quantity.get("decimals") or implementation.get("decimals") or 0),
        fungible_id=info.get("id") or "",
        direction=item.get("direction") or "",
        amount=int(quantity.get("int") or 0),
        quantity=Decimal(quantity.get("numeric") or "0"),
        price_usd=_optional_float(item.get("price")),
        value_usd=_optional_float(item.get("value")),
        sender=(item.get("sender") or "").lower(),
        recipient=(item.get("recipient") or "").lower(),
    )


def _transaction(item: dict) -> ZerionTransaction:
    attributes = item["attributes"]
    chain = item["relationships"]["chain"]["data"]["id"]
    return ZerionTransaction(
        zerion_id=item["id"],
        chain=chain,
        tx_hash=attributes.get("hash") or "",
        block=attributes.get("mined_at_block"),
        mined_at=datetime.fromisoformat(attributes["mined_at"].replace("Z", "+00:00")),
        operation_type=attributes.get("operation_type") or "",
        status=attributes.get("status") or "",
        fee_usd=_optional_float((attributes.get("fee") or {}).get("value")),
        transfers=[_transfer(i, chain, t) for i, t in enumerate(attributes.get("transfers") or [])],
        raw=item,
    )


def _cursor(next_url: str | None) -> str | None:
    if not next_url:
        return None
    return parse_qs(urlparse(next_url).query).get("page[after]", [None])[0]
```

et ajouter à `ZerionClient` :

```python
    def transactions(
        self, address: str, since: datetime, cursor: str | None = None
    ) -> TransactionsPage:
        params = {
            "currency": "usd",
            "page[size]": 100,
            "filter[trash]": "only_non_trash",
            "filter[operation_types]": OPERATION_TYPES,
            "filter[min_mined_at]": int(since.timestamp() * 1000),
        }
        if cursor:
            params["page[after]"] = cursor
        payload = self._http.get(f"/wallets/{address}/transactions/", params=params)
        return TransactionsPage(
            [_transaction(item) for item in payload.get("data", [])],
            _cursor((payload.get("links") or {}).get("next")),
        )

    def portfolio(self, address: str) -> Portfolio:
        attributes = self._http.get(f"/wallets/{address}/portfolio", params={"currency": "usd"})[
            "data"
        ]["attributes"]
        return Portfolio(
            total_usd=float((attributes.get("total") or {}).get("positions") or 0.0),
            by_chain={k: float(v) for k, v in (attributes.get("positions_distribution_by_chain") or {}).items()},
        )
```

- [ ] **Step 4: Lancer les tests**

Run: `make test args="integrations -q"`
Expected: PASS.

- [ ] **Step 5: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend/integrations
git commit -m "feat(integrations): historique Zerion par pages (curseur) et portfolio multi-chaînes

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Refonte du modèle de données et retrait des pièces v1

**Files:**
- Delete: `backend/apps/wallets/services/pricing.py`, `backend/apps/wallets/forms.py`, `backend/apps/wallets/templates/`, tests `test_pricing.py`, `test_pricing_db.py`, `test_qualification_history.py`, `test_qualification_entities.py`, `test_tasks.py`
- Create: migrations wallets `reset_v1_data`, `qualification_v2`, `v2_defaults` ; migration discovery `pipeline_v2`
- Rewrite: `backend/apps/wallets/models.py`, `services/entities.py`, `services/qualification.py`, `tasks.py`, `admin.py`, `tests/fakes.py`
- Modify: `backend/apps/discovery/models.py`, `services/settings.py`, `services/tags.py`, `tests/factories.py`, `tests/test_models.py`, `tests/test_tags.py`, `tests/test_admin.py`

**Interfaces:**
- Produces (`apps.wallets.models`) : `WalletProfile` (+ `Status.VALUED`, `priority`, `active_chains`, `linked_value_usd`, `history_cursor`, `history_complete`, `last_mined_at` ; sans `entity` ni `chains`), `WalletTransaction`, `TokenTrade` v2, `TokenPosition` v2, `WalletLink` (`TRANSFER_AFTER_BUY`, `BIG_RECEIVE`, `FUNDING`), `STRONG_LINKS`, `KnownAddress`, `QualificationSettings` v2, `QUALIFICATION_FIELDS`, `BLOCKING_KINDS`.
- Produces (`apps.discovery.models`) : `PipelineSettings.prefilter_chains`, `quote_symbols`, `history_refresh_days` ; `default_prefilter_chains()`, `default_quote_symbols()`.
- Produces : `QualificationThresholds` v2 (sans `follow_depth`, `funder_max_wallets` ; avec `big_receive_pct`) ; `entities.transfer_after_buy_targets` et `entities.add_link` conservés.

À la fin de cette tâche, la qualification v2 n'est pas encore branchée : `qualification.py`, `tasks.py` et `admin.py` sont des versions minimales, complétées aux tâches 4 à 9.

- [ ] **Step 1: Migration de remise à zéro (avec les modèles v1 encore en place)**

```bash
docker compose exec web python manage.py makemigrations wallets --empty -n reset_v1_data
```

Contenu (le fichier généré dépend de `0002_defaults`) :

```python
from django.db import migrations


def reset(apps, schema_editor):
    apps.get_model("wallets", "WalletLink").objects.all().delete()
    apps.get_model("wallets", "TokenTrade").objects.all().delete()
    apps.get_model("wallets", "TokenPosition").objects.all().delete()
    Profile = apps.get_model("wallets", "WalletProfile")
    Profile.objects.filter(source="linked").delete()
    Profile.objects.update(
        status="pending", filter_reason="", tags=[], portfolio_value_usd=None, metrics={},
        attempts=0, analyzed_at=None, next_analysis_at=None, entity=None,
    )
    apps.get_model("wallets", "Entity").objects.all().delete()


class Migration(migrations.Migration):
    dependencies = [("wallets", "0002_defaults")]
    operations = [migrations.RunPython(reset, migrations.RunPython.noop)]
```

```bash
make migrate
```

- [ ] **Step 2: Retirer les pièces v1**

```bash
cd backend
git rm -r apps/wallets/services/pricing.py apps/wallets/forms.py apps/wallets/templates \
  apps/wallets/tests/test_pricing.py apps/wallets/tests/test_pricing_db.py \
  apps/wallets/tests/test_qualification_history.py apps/wallets/tests/test_qualification_entities.py \
  apps/wallets/tests/test_tasks.py
cd ..
```

- [ ] **Step 3: Réglages du pipeline (découverte)**

Dans `backend/apps/discovery/models.py`, remplacer `default_stablecoins()` par :

```python
def default_prefilter_chains() -> list[str]:
    return ["base", "robinhood", "bsc", "eth", "arc"]


def default_quote_symbols() -> list[str]:
    return ["ETH", "WETH", "BNB", "WBNB", "USDC", "USDT", "DAI", "USDC.E", "USDBC", "FDUSD"]
```

Dans `PipelineSettings`, remplacer les champs `stablecoin_symbols` et `extra_chains` par :

```python
    prefilter_chains = models.JSONField(
        default=default_prefilter_chains,
        help_text="gt_id des chaînes du pré-filtre HyperSync (+ la chaîne où le wallet est repéré).",
    )
    quote_symbols = models.JSONField(
        default=default_quote_symbols,
        help_text="Monnaies de paiement : leurs jambes de trade ne comptent pas comme achats.",
    )
    history_refresh_days = models.PositiveIntegerField(default=7)
```

et passer les défauts de `zerion_daily_budget` à `1800` et de `zerion_requests_per_min` à `300`.

- [ ] **Step 4: Réécrire les modèles de la qualification**

`backend/apps/wallets/models.py` :

```python
"""Qualification v2 : profils, transactions et mouvements Zerion, positions, liens, réglages."""

from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex
from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import Q

from apps.discovery.models import UINT256_DIGITS, Chain, Wallet

QUALIFICATION_FIELDS = (
    "max_txs_per_day",
    "max_distinct_tokens",
    "inactive_days",
    "min_txs_active",
    "history_days",
    "max_mev_ratio",
    "min_distinct_buys",
    "min_portfolio_usd",
    "max_portfolio_usd",
    "transfer_after_buy_pct",
    "big_receive_pct",
    "hot_wallet_min_counterparties",
    "deposit_forward_pct",
    "deposit_forward_hours",
    "flipper_hours",
    "flipper_min_sold_pct",
    "flipper_min_share_pct",
    "holder_min_pct",
    "accumulator_max_out_pct",
    "accumulator_min_positions",
    "early_buyer_min_explosions",
    "refilter_after_days",
)
BLOCKING_KINDS = ("exchange", "cex_deposit", "service", "bridge", "router", "mev")


def _amount(**kwargs):
    return models.DecimalField(max_digits=UINT256_DIGITS, decimal_places=0, **kwargs)


def _usd(**kwargs):
    return models.DecimalField(max_digits=20, decimal_places=2, **kwargs)


class WalletProfile(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "En attente"
        PREFILTERED = "prefiltered", "Pré-filtré (historique en cours)"
        HISTORY_FETCHED = "history_fetched", "Historique complet"
        QUALIFIED = "qualified", "Qualifié"
        VALUED = "valued", "Wallet lié valorisé"
        FILTERED = "filtered", "Écarté"

    class Source(models.TextChoices):
        EARLY_BUYER = "early_buyer", "Early buyer"
        LINKED = "linked", "Wallet lié"

    wallet = models.OneToOneField(Wallet, on_delete=models.CASCADE, related_name="profile")
    source = models.CharField(max_length=16, choices=Source.choices, default=Source.EARLY_BUYER)
    depth = models.PositiveSmallIntegerField(default=0)
    status = models.CharField(max_length=24, choices=Status.choices, default=Status.PENDING)
    filter_reason = models.CharField(max_length=64, blank=True, default="")
    priority = models.FloatField(default=0)
    active_chains = ArrayField(models.CharField(max_length=64), default=list, blank=True)
    portfolio_value_usd = _usd(null=True, blank=True)
    linked_value_usd = _usd(null=True, blank=True)
    history_cursor = models.CharField(max_length=1024, blank=True, default="")
    history_complete = models.BooleanField(default=False)
    last_mined_at = models.DateTimeField(null=True, blank=True)
    metrics = models.JSONField(default=dict, blank=True)
    tags = ArrayField(models.CharField(max_length=32), default=list, blank=True)
    attempts = models.PositiveSmallIntegerField(default=0)
    analyzed_at = models.DateTimeField(null=True, blank=True)
    next_analysis_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["status", "priority"], name="wallets_profile_status_prio"),
            GinIndex(fields=["tags"], name="wallets_profile_tags"),
        ]

    def __str__(self):
        return str(self.wallet)


class WalletTransaction(models.Model):
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="transactions")
    zerion_id = models.CharField(max_length=64)
    chain = models.CharField(max_length=64)
    tx_hash = models.CharField(max_length=66)
    block = models.PositiveBigIntegerField(null=True, blank=True)
    mined_at = models.DateTimeField()
    operation_type = models.CharField(max_length=16)
    status = models.CharField(max_length=16)
    fee_usd = models.DecimalField(max_digits=20, decimal_places=6, null=True, blank=True)
    raw = models.JSONField(default=dict)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["wallet", "zerion_id"], name="wallets_unique_transaction")
        ]
        indexes = [models.Index(fields=["wallet", "mined_at"], name="wallets_tx_wallet_date")]

    def __str__(self):
        return f"{self.operation_type} {self.tx_hash[:10]} ({self.chain})"


class TokenTrade(models.Model):
    class Kind(models.TextChoices):
        BUY = "buy", "Achat"
        SELL = "sell", "Vente"
        SEND = "send", "Envoi"
        RECEIVE = "receive", "Réception"

    transaction = models.ForeignKey(WalletTransaction, on_delete=models.CASCADE, related_name="trades")
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="trades")
    transfer_index = models.PositiveSmallIntegerField()
    chain = models.CharField(max_length=64)
    token_address = models.CharField(max_length=66)
    token_symbol = models.CharField(max_length=64, blank=True, default="")
    token_decimals = models.PositiveSmallIntegerField(default=0)
    fungible_id = models.CharField(max_length=100, blank=True, default="")
    kind = models.CharField(max_length=8, choices=Kind.choices)
    direction = models.CharField(max_length=4)
    quantity = models.DecimalField(max_digits=60, decimal_places=18)
    amount = _amount()
    price_usd = models.DecimalField(max_digits=40, decimal_places=18, null=True, blank=True)
    value_usd = _usd(null=True, blank=True)
    counterparty = models.CharField(max_length=42, blank=True, default="")
    block = models.PositiveBigIntegerField(null=True, blank=True)
    mined_at = models.DateTimeField()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["transaction", "transfer_index"], name="wallets_unique_transfer"
            )
        ]
        indexes = [
            models.Index(
                fields=["wallet", "chain", "token_address", "mined_at"], name="wallets_trade_token"
            )
        ]

    def __str__(self):
        return f"{self.get_kind_display()} {self.token_symbol or self.token_address}"


class TokenPosition(models.Model):
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="positions")
    chain = models.CharField(max_length=64)
    token_address = models.CharField(max_length=66)
    token_symbol = models.CharField(max_length=64, blank=True, default="")
    bought_amount = _amount(default=0)
    sold_amount = _amount(default=0)
    sent_amount = _amount(default=0)
    received_amount = _amount(default=0)
    bought_usd = _usd(default=0)
    sold_usd = _usd(default=0)
    buys = models.PositiveIntegerField(default=0)
    sells = models.PositiveIntegerField(default=0)
    first_at = models.DateTimeField()
    last_at = models.DateTimeField()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["wallet", "chain", "token_address"], name="wallets_unique_position"
            )
        ]

    def __str__(self):
        return f"{self.wallet} · {self.token_symbol or self.token_address}"

    @property
    def balance(self):
        return self.bought_amount + self.received_amount - self.sold_amount - self.sent_amount


class WalletLink(models.Model):
    class Kind(models.TextChoices):
        TRANSFER_AFTER_BUY = "transfer_after_buy", "Transfert après achat"
        BIG_RECEIVE = "big_receive", "Gros transfert reçu"
        FUNDING = "funding", "Financement initial (information)"

    from_wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="links_out")
    to_wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="links_in")
    kind = models.CharField(max_length=24, choices=Kind.choices)
    evidence = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["from_wallet", "to_wallet", "kind"], name="wallets_unique_link"
            )
        ]

    def __str__(self):
        return f"{self.from_wallet} → {self.to_wallet} ({self.kind})"


STRONG_LINKS = (WalletLink.Kind.TRANSFER_AFTER_BUY, WalletLink.Kind.BIG_RECEIVE)


class KnownAddressQuerySet(models.QuerySet):
    def blocking_for(self, address: str, chain_ids: list[int]):
        return self.filter(
            Q(chain__isnull=True) | Q(chain_id__in=chain_ids),
            address=address.lower(),
            kind__in=BLOCKING_KINDS,
        )


class KnownAddress(models.Model):
    class Kind(models.TextChoices):
        EXCHANGE = "exchange", "Exchange"
        CEX_DEPOSIT = "cex_deposit", "Dépôt d'exchange"
        SERVICE = "service", "Service"
        BRIDGE = "bridge", "Bridge"
        ROUTER = "router", "Router"
        MEV = "mev", "Bot MEV"
        STABLECOIN = "stablecoin", "Stablecoin"
        WRAPPED_NATIVE = "wrapped_native", "Natif wrappé"

    class Source(models.TextChoices):
        IMPORT = "import", "Import"
        MANUAL = "manual", "Manuel"
        AUTO = "auto", "Automatique"

    chain = models.ForeignKey(
        Chain, null=True, blank=True, on_delete=models.CASCADE, related_name="known_addresses"
    )
    address = models.CharField(max_length=42)
    kind = models.CharField(max_length=16, choices=Kind.choices)
    label = models.CharField(max_length=128, blank=True, default="")
    source = models.CharField(max_length=8, choices=Source.choices, default=Source.MANUAL)
    created_at = models.DateTimeField(auto_now_add=True)

    objects = KnownAddressQuerySet.as_manager()

    class Meta:
        verbose_name_plural = "known addresses"
        constraints = [
            models.UniqueConstraint(
                fields=["chain", "address"], name="wallets_unique_known", nulls_distinct=False
            )
        ]

    def __str__(self):
        return f"{self.label or self.address} ({self.kind})"


def _int_setting():
    return models.PositiveIntegerField(null=True, blank=True)


def _decimal_setting():
    return models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)


class QualificationSettings(models.Model):
    chain = models.ForeignKey(
        Chain, null=True, blank=True, on_delete=models.CASCADE, related_name="qualification_settings"
    )
    max_txs_per_day = _int_setting()
    max_distinct_tokens = _int_setting()
    inactive_days = _int_setting()
    min_txs_active = _int_setting()
    history_days = _int_setting()
    max_mev_ratio = _decimal_setting()
    min_distinct_buys = _int_setting()
    min_portfolio_usd = _decimal_setting()
    max_portfolio_usd = _decimal_setting()
    transfer_after_buy_pct = _decimal_setting()
    big_receive_pct = _decimal_setting()
    hot_wallet_min_counterparties = _int_setting()
    deposit_forward_pct = _decimal_setting()
    deposit_forward_hours = _int_setting()
    flipper_hours = _int_setting()
    flipper_min_sold_pct = _decimal_setting()
    flipper_min_share_pct = _decimal_setting()
    holder_min_pct = _decimal_setting()
    accumulator_max_out_pct = _decimal_setting()
    accumulator_min_positions = _int_setting()
    early_buyer_min_explosions = _int_setting()
    refilter_after_days = _int_setting()

    class Meta:
        verbose_name = "réglages de qualification"
        verbose_name_plural = "réglages de qualification"
        constraints = [
            models.UniqueConstraint(
                fields=["chain"], name="wallets_one_settings_per_chain", nulls_distinct=False
            )
        ]

    def __str__(self):
        return f"Qualification {self.chain or 'globale'}"

    def clean(self):
        if self.chain_id is None:
            missing = [name for name in QUALIFICATION_FIELDS if getattr(self, name) is None]
            if missing:
                raise ValidationError({name: "Obligatoire pour les réglages globaux." for name in missing})
```

`backend/apps/wallets/services/settings.py` : dans `QualificationThresholds`, retirer `follow_depth: int` et `funder_max_wallets: int`, ajouter `big_receive_pct: float` juste après `transfer_after_buy_pct: float`.

`backend/apps/wallets/services/tags.py` : supprimer `entity_tags`. Dans `tests/test_tags.py`, retirer `entity_tags` de l'import et le test `test_entity_tags_union_and_internal_holding`.

`backend/apps/wallets/tests/factories.py` :
- remplacer `from apps.wallets.tests.fakes import NOW` par `from datetime import UTC, datetime` et la constante `NOW = datetime(2026, 9, 27, 12, tzinfo=UTC)` ;
- dans `DEFAULT_THRESHOLDS` : retirer `follow_depth=1,` et `funder_max_wallets=50,`, ajouter `big_receive_pct=30.0,` après `transfer_after_buy_pct=70.0,`, passer `max_txs_per_day=50,` et `history_days=180,`.

- [ ] **Step 5: Versions minimales des services, tâches, admin et faux clients**

`backend/apps/wallets/services/entities.py` : garder uniquement `transfer_after_buy_targets` et `add_link` (supprimer `follow`, `funder_is_service`, `refresh_entity`) ; imports réduits à :

```python
"""Wallets liés directs : liens forts (transfert après achat, gros transfert reçu)."""

from collections import defaultdict

from apps.discovery.models import Wallet
from apps.wallets.models import WalletLink
from apps.wallets.services.classify import BUY, RECEIVE, SEND
from apps.wallets.services.positions import TradeRecord
```

`backend/apps/wallets/services/qualification.py` :

```python
"""Qualification v2 d'un wallet (complétée aux tâches 4 à 8)."""
```

`backend/apps/wallets/tasks.py` :

```python
"""Tâches Celery de la qualification des wallets."""

from celery import shared_task


@shared_task
def qualify_wallets_task() -> dict:
    return {}
```

`backend/apps/wallets/admin.py` :

```python
"""Admin de la qualification (complété à la tâche 9)."""

from django.contrib import admin

from apps.wallets.models import KnownAddress, QualificationSettings

admin.site.register(KnownAddress)
admin.site.register(QualificationSettings)
```

`backend/apps/wallets/tests/fakes.py` :

```python
"""Faux clients de la qualification v2 (complétés à la tâche 4)."""
```

`backend/apps/wallets/tests/test_admin.py` : ne garder que `admin_client` et le test des listes paramétré sur `["knownaddress", "qualificationsettings"]` (le reste revient à la tâche 9).

- [ ] **Step 6: Migrations de schéma et valeurs par défaut**

```bash
docker compose exec web python manage.py makemigrations wallets -n qualification_v2
docker compose exec web python manage.py makemigrations discovery -n pipeline_v2
docker compose exec web python manage.py makemigrations wallets --empty -n v2_defaults
```

Si Django demande une valeur par défaut pour un champ non nul, répondre `timezone.now` pour une date et `''` pour un texte (les tables concernées sont vides depuis l'étape 1).

Contenu de `v2_defaults` (ajuster les numéros de dépendances aux fichiers générés) :

```python
from django.db import migrations


def update_defaults(apps, schema_editor):
    apps.get_model("wallets", "QualificationSettings").objects.filter(chain=None).update(
        max_txs_per_day=50, history_days=180, big_receive_pct=30
    )
    apps.get_model("discovery", "PipelineSettings").objects.filter(pk=1).update(
        zerion_daily_budget=1800, zerion_requests_per_min=300
    )


class Migration(migrations.Migration):
    dependencies = [("wallets", "0004_qualification_v2"), ("discovery", "0007_pipeline_v2")]
    operations = [migrations.RunPython(update_defaults, migrations.RunPython.noop)]
```

```bash
make migrate
```

- [ ] **Step 7: Adapter les tests de modèles**

Dans `backend/apps/wallets/tests/test_models.py` :
- remplacer `test_global_defaults_from_migration` par :

```python
def test_global_defaults_from_migration():
    t = qualification_thresholds()
    assert (t.max_txs_per_day, t.history_days) == (50, 180)
    assert (t.big_receive_pct, t.transfer_after_buy_pct) == (30.0, 70.0)
```

- remplacer `test_trade_is_unique_per_log_and_wallet` par :

```python
def test_transfer_is_unique_per_transaction_index():
    wallet = Wallet.objects.create(address="0x" + "a" * 40)
    tx = WalletTransaction.objects.create(
        wallet=wallet, zerion_id="z1", chain="base", tx_hash="0xt", mined_at="2026-09-27T00:00Z",
        operation_type="trade", status="confirmed",
    )
    values = dict(
        transaction=tx, wallet=wallet, transfer_index=0, chain="base", token_address="native",
        kind="buy", direction="in", quantity=1, amount=1, mined_at="2026-09-27T00:00Z",
    )
    TokenTrade.objects.create(**values)
    with pytest.raises(IntegrityError), transaction.atomic():
        TokenTrade.objects.create(**values)
```

- dans `test_profile_defaults`, vérifier `(status, source, depth, tags, history_complete) == ("pending", "early_buyer", 0, [], False)` ;
- ajouter :

```python
def test_pipeline_v2_defaults():
    cfg = PipelineSettings.load()
    assert cfg.prefilter_chains == ["base", "robinhood", "bsc", "eth", "arc"]
    assert "USDC" in cfg.quote_symbols
    assert (cfg.zerion_daily_budget, cfg.zerion_requests_per_min, cfg.history_refresh_days) == (1800, 300, 7)
```

- mettre à jour les imports (`PipelineSettings`, `WalletTransaction` ; retirer `make_token` s'il n'est plus utilisé).

- [ ] **Step 8: Lancer les tests**

Run: `make test args="-q"`
Expected: PASS.

- [ ] **Step 9: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add -A backend
git commit -m "refactor(wallets): modèle de données v2 (transactions Zerion) et retrait des pièces v1

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Fonctions pures — mouvements Zerion, filtres sur totaux, gros transfert reçu

**Files:**
- Create: `backend/apps/wallets/services/zerion_history.py`
- Modify: `backend/apps/wallets/services/filters.py`, `backend/apps/wallets/services/entities.py`, `backend/apps/wallets/services/positions.py`
- Create: `backend/apps/wallets/tests/test_zerion_history.py`
- Modify: `backend/apps/wallets/tests/test_filters.py`, `backend/apps/wallets/tests/test_pure_entities.py`

**Interfaces:**
- Consumes: `ZerionTransaction`, `ZerionTransfer`, `NATIVE` (Task 1) ; `BUY`, `SELL`, `SEND`, `RECEIVE` (`classify`) ; `TradeRecord`.
- Produces:
  - `zerion_history.trade_kind(operation_type, direction, has_in, has_out) -> str | None` ; `movements(tx: ZerionTransaction) -> list[tuple[ZerionTransfer, str]]` ; `counterparty(transfer: ZerionTransfer) -> str` ; `is_quote(symbol: str, address: str, quote_symbols: list[str]) -> bool`.
  - `filters.prefilter_reason(txs_7d: int, txs_active: int, t, source) -> str | None` (totaux multi-chaînes ; `ChainActivity` supprimé).
  - `entities.ZERO_ADDRESS`, `entities.big_receive_targets(records, threshold_pct) -> dict[tuple[str, str], dict]` : (chaîne, expéditeur) → `{"pct", "value_usd"}`.
  - `TradeRecord.chain_id` devient une chaîne (identifiant Zerion ou gt_id) ; annotation `str`.

- [ ] **Step 1: Écrire les tests**

`backend/apps/wallets/tests/test_zerion_history.py` :

```python
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from apps.wallets.services.zerion_history import counterparty, is_quote, movements, trade_kind
from integrations.zerion import NATIVE, ZerionTransaction, ZerionTransfer

W = "0x" + "a" * 40


def zt(index, direction, token="0x" + "1" * 40, symbol="TOK", sender="0xs", recipient=W):
    return ZerionTransfer(
        index=index, chain="base", token_address=token, token_symbol=symbol, token_decimals=18,
        fungible_id="f", direction=direction, amount=10**18, quantity=Decimal(1), price_usd=1.0,
        value_usd=1.0, sender=sender, recipient=recipient,
    )


def tx(operation_type, transfers, status="confirmed"):
    return ZerionTransaction(
        zerion_id="z", chain="base", tx_hash="0xt", block=1, mined_at=datetime(2026, 9, 1, tzinfo=UTC),
        operation_type=operation_type, status=status, fee_usd=None, transfers=transfers, raw={},
    )


@pytest.mark.parametrize(
    ("operation", "direction", "has_in", "has_out", "kind"),
    [
        ("trade", "in", True, True, "buy"),
        ("trade", "out", True, True, "sell"),
        ("send", "out", False, True, "send"),
        ("receive", "in", True, False, "receive"),
        ("execute", "in", True, True, "buy"),
        ("execute", "out", False, True, "send"),
        ("mint", "in", True, False, "receive"),
        ("burn", "out", False, True, "send"),
        ("claim", "in", True, False, "receive"),
        ("trade", "self", True, True, None),
    ],
)
def test_trade_kind(operation, direction, has_in, has_out, kind):
    assert trade_kind(operation, direction, has_in, has_out) == kind


def test_movements_of_a_trade():
    rows = movements(tx("trade", [zt(0, "in"), zt(1, "out", token=NATIVE, symbol="ETH")]))
    assert [(t.index, kind) for t, kind in rows] == [(0, "buy"), (1, "sell")]


def test_failed_transactions_have_no_movement():
    assert movements(tx("trade", [zt(0, "in")], status="failed")) == []


def test_counterparty_depends_on_direction():
    assert counterparty(zt(0, "in", sender="0xfrom")) == "0xfrom"
    assert counterparty(zt(0, "out", recipient="0xto")) == "0xto"


def test_is_quote():
    assert is_quote("usdc", "0x1", ["USDC"])
    assert is_quote("ANY", NATIVE, [])
    assert not is_quote("PEPE", "0x1", ["USDC"])
```

Dans `backend/apps/wallets/tests/test_filters.py` : supprimer l'import de `ChainActivity` et remplacer les deux tests de pré-filtre (`test_prefilter` paramétré et `test_prefilter_uses_each_chain_threshold`) par :

```python
@pytest.mark.parametrize(
    ("txs_7d", "txs_active", "source", "reason"),
    [
        (10, 10, "early_buyer", None),
        (351, 10, "early_buyer", "bot_frequency"),
        (351, 10, "linked", "bot_frequency"),
        (3, 2, "early_buyer", "inactive"),
        (3, 2, "linked", None),
    ],
)
def test_prefilter_on_totals(txs_7d, txs_active, source, reason):
    assert prefilter_reason(txs_7d, txs_active, make_thresholds(), source) == reason
```

(`make_thresholds()` a `max_txs_per_day=50`, donc le seuil sur 7 jours est 350.)

Ajouter à `backend/apps/wallets/tests/test_pure_entities.py` :

```python
from apps.wallets.services.entities import ZERO_ADDRESS, big_receive_targets


def usd_rec(kind, usd, counterparty, token="0xt"):
    return TradeRecord("base", token, kind, 1, usd, 1, 1, counterparty)


def test_big_receive_is_a_share_of_all_inflows():
    records = [
        usd_rec("buy", 1000.0, "0xpool"),
        usd_rec("receive", 3000.0, "0xbig"),
        usd_rec("receive", 50.0, "0xdust"),
        usd_rec("receive", 900.0, ZERO_ADDRESS),
        usd_rec("receive", None, "0xunknown"),
    ]
    assert big_receive_targets(records, 30) == {("base", "0xbig"): {"pct": 60.24, "value_usd": 3000.0}}


def test_no_inflow_no_target():
    assert big_receive_targets([usd_rec("send", 10.0, "0xa")], 30) == {}
```

(inflows = 1000 + 3000 + 50 + 900 = 4 980 $ ; 3000 / 4980 = 60,24 %. Les réceptions du ZERO_ADDRESS (mint) comptent dans les entrées mais ne sont jamais des cibles.)

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/wallets/tests/test_zerion_history.py apps/wallets/tests/test_filters.py apps/wallets/tests/test_pure_entities.py -q"`
Expected: FAIL.

- [ ] **Step 3: Implémenter**

`backend/apps/wallets/services/zerion_history.py` :

```python
"""Transactions Zerion → mouvements (achat, vente, envoi, réception). Fonctions pures."""

from apps.wallets.services.classify import BUY, RECEIVE, SELL, SEND
from integrations.zerion import NATIVE, ZerionTransaction, ZerionTransfer


def trade_kind(operation_type: str, direction: str, has_in: bool, has_out: bool) -> str | None:
    """Un trade (ou un execute qui échange) donne achat / vente ; le reste envoi / réception."""
    if direction not in ("in", "out"):
        return None
    swap = operation_type == "trade" or (operation_type == "execute" and has_in and has_out)
    if swap:
        return BUY if direction == "in" else SELL
    return RECEIVE if direction == "in" else SEND


def movements(tx: ZerionTransaction) -> list[tuple[ZerionTransfer, str]]:
    if tx.status != "confirmed":
        return []
    directions = {t.direction for t in tx.transfers}
    has_in, has_out = "in" in directions, "out" in directions
    rows = []
    for transfer in tx.transfers:
        kind = trade_kind(tx.operation_type, transfer.direction, has_in, has_out)
        if kind:
            rows.append((transfer, kind))
    return rows


def counterparty(transfer: ZerionTransfer) -> str:
    return transfer.sender if transfer.direction == "in" else transfer.recipient


def is_quote(symbol: str, address: str, quote_symbols: list[str]) -> bool:
    """Monnaie de paiement : le natif, ou un symbole de la liste réglable."""
    return address == NATIVE or symbol.upper() in {s.upper() for s in quote_symbols}
```

Dans `backend/apps/wallets/services/filters.py`, supprimer `ChainActivity` (et l'import de `dataclass`) et remplacer `prefilter_reason` par :

```python
def prefilter_reason(
    txs_7d: int, txs_active: int, t: QualificationThresholds, source: str
) -> str | None:
    """Mesures additionnées sur les chaînes du pré-filtre."""
    if txs_7d > t.max_txs_per_day * 7:
        return "bot_frequency"
    if source == EARLY_BUYER and txs_active < t.min_txs_active:
        return "inactive"
    return None
```

Ajouter à `backend/apps/wallets/services/entities.py` (qui contient `transfer_after_buy_targets` et `add_link` depuis la tâche 2) :

```python
ZERO_ADDRESS = "0x" + "0" * 40


def big_receive_targets(
    records: list[TradeRecord], threshold_pct: float
) -> dict[tuple[str, str], dict]:
    """Expéditeurs dont les envois représentent ≥ threshold % des entrées ($) du wallet. Pur."""
    inflow = sum(r.usd or 0.0 for r in records if r.kind in (BUY, RECEIVE))
    if inflow <= 0:
        return {}
    received: dict[str, float] = defaultdict(float)
    chain_of: dict[str, str] = {}
    for record in records:
        if record.kind == RECEIVE and record.usd and record.counterparty not in ("", ZERO_ADDRESS):
            received[record.counterparty] += record.usd
            chain_of.setdefault(record.counterparty, record.chain_id)
    return {
        (chain_of[sender], sender): {"pct": round(value * 100 / inflow, 2), "value_usd": round(value, 2)}
        for sender, value in received.items()
        if value * 100 >= threshold_pct * inflow
    }
```

Dans `backend/apps/wallets/services/positions.py`, changer l'annotation `chain_id: int` en `chain_id: str`.

- [ ] **Step 4: Lancer les tests**

Run: `make test args="apps/wallets -q"`
Expected: PASS.

- [ ] **Step 5: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend/apps/wallets
git commit -m "feat(wallets): mouvements Zerion, pré-filtre sur totaux et gros transfert reçu (en %)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Faux clients et pré-filtre HyperSync multi-chaînes

**Files:**
- Rewrite: `backend/apps/wallets/tests/fakes.py`
- Modify: `backend/apps/wallets/services/qualification.py`
- Create: `backend/apps/wallets/tests/test_prefilter.py`

**Interfaces:**
- Consumes: `block_at` (cache Redis), `classify_all`, `TradeRecord`, `farmer_reason`, `mev_ratio`, `prefilter_reason`, `qualification_thresholds`, `KnownAddress.objects.blocking_for`.
- Produces (`qualification`) : `Clients(hypersync_for, zerion)` ; `DAY` ; `filter_out(profile, reason, now) -> str` ; `prefilter_chains(profile, cfg) -> list[Chain]` ; `prefilter_step(profile, clients, now, cfg) -> str`.
- Produces (`tests.fakes`) : `NOW`, `HEIGHT`, `GENESIS_TS`, `BLOCK_TIME`, `START`, `UNIT`, `BUYER`, `VAULT`, `SENDER`, `POOL`, `ROUTER`, `TOKEN_A`, `TOKEN_B`, `TOKEN_C`, `USDC`, `DEPOSIT`, `HOT`, `tr()`, `hs_history()`, `ztransfer()`, `ztx()`, `buyer_zerion_history(send_to=VAULT)`, `FakeWalletHyperSync`, `FakeZerion`.

- [ ] **Step 1: Écrire les faux clients**

`backend/apps/wallets/tests/fakes.py` :

```python
"""Scénario v2 : BUYER achète A, B, C ; envoie 90 % de A à VAULT ; a reçu 5 000 USDC de SENDER.
Portefeuilles Zerion : BUYER 6 150 $, VAULT 11 350 $, SENDER 2 000 $ (total lié : 19 500 $)."""

from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from integrations.errors import BudgetExhausted
from integrations.hypersync import Funding, WalletTransfer
from integrations.zerion import NATIVE, Portfolio, TransactionsPage, ZerionTransaction, ZerionTransfer

NOW = datetime(2026, 9, 27, 12, tzinfo=UTC)
BLOCK_TIME = 2
HEIGHT = 50_000_000
GENESIS_TS = int(NOW.timestamp()) - HEIGHT * BLOCK_TIME
START = HEIGHT - 100_000
UNIT = 10**18

BUYER = "0x" + "a" * 40
VAULT = "0x" + "b" * 40
SENDER = "0x" + "f" * 40
POOL = "0x" + "2" * 40
ROUTER = "0x" + "9" * 40
TOKEN_A = "0x" + "1" * 40
TOKEN_B = "0x" + "3" * 40
TOKEN_C = "0x" + "4" * 40
USDC = "0x" + "c" * 40
DEPOSIT = "0x" + "5" * 40
HOT = "0x" + "e" * 40


def tr(block, tx, token, sender, recipient, amount, tx_from, tx_to, tx_value=0, log_index=0):
    return WalletTransfer(
        block=block, timestamp=GENESIS_TS + block * BLOCK_TIME, tx_hash=tx, log_index=log_index,
        token=token, sender=sender, recipient=recipient, amount=amount, tx_from=tx_from,
        tx_to=tx_to, tx_value=tx_value,
    )


def hs_history() -> list[WalletTransfer]:
    """Vue HyperSync de BUYER (pré-filtre) : 3 achats via un router."""
    return [
        tr(START, "0xh1", TOKEN_A, POOL, BUYER, 500 * UNIT, BUYER, ROUTER),
        tr(START + 10, "0xh2", TOKEN_B, POOL, BUYER, 200 * UNIT, BUYER, ROUTER),
        tr(START + 20, "0xh3", TOKEN_C, POOL, BUYER, 100 * UNIT, BUYER, ROUTER),
    ]


class FakeWalletHyperSync:
    def __init__(self, transfers=None, tx_counts=None, counterparties=None, fundings=None):
        self.transfers = transfers if transfers is not None else {BUYER: hs_history()}
        self.tx_counts = tx_counts if tx_counts is not None else {VAULT: 0}
        self.counterparties = counterparties or {}
        self.fundings = fundings if fundings is not None else {BUYER: Funding(SENDER, START - 20, UNIT)}

    def height(self):
        return HEIGHT

    def block_timestamp(self, number):
        return GENESIS_TS + number * BLOCK_TIME

    def wallet_tx_count(self, address, from_block, to_block, cap):
        return min(self.tx_counts.get(address, 10), cap)

    def distinct_counterparties(self, address, from_block, to_block, cap):
        return min(self.counterparties.get(address, 3), cap)

    def first_funding(self, address, to_block):
        return self.fundings.get(address)

    def wallet_transfers(self, address, from_block, to_block):
        return [t for t in self.transfers.get(address, []) if from_block <= t.block < to_block]


def ztransfer(index, direction, token, symbol, units, value, other, decimals=18):
    sender, recipient = (other, BUYER) if direction == "in" else (BUYER, other)
    return ZerionTransfer(
        index=index, chain="base", token_address=token, token_symbol=symbol, token_decimals=decimals,
        fungible_id=symbol.lower(), direction=direction, amount=int(units * 10**decimals),
        quantity=Decimal(str(units)), price_usd=(value / units) if value is not None else None,
        value_usd=value, sender=sender, recipient=recipient,
    )


def ztx(zerion_id, days_ago, operation_type, transfers, block):
    return ZerionTransaction(
        zerion_id=zerion_id, chain="base", tx_hash=f"0x{zerion_id}", block=block,
        mined_at=NOW - timedelta(days=days_ago), operation_type=operation_type, status="confirmed",
        fee_usd=0.05, transfers=transfers, raw={"id": zerion_id},
    )


def buyer_zerion_history(send_to: str = VAULT) -> list[ZerionTransaction]:
    """Du plus récent au plus ancien, comme Zerion."""
    return [
        ztx("z5", 1, "send", [ztransfer(0, "out", TOKEN_A, "A", 450, 1350.0, send_to)], 105),
        ztx("z4", 2, "trade", [ztransfer(0, "in", TOKEN_C, "C", 100, 300.0, POOL),
                               ztransfer(1, "out", USDC, "USDC", 300, 300.0, ROUTER, decimals=6)], 104),
        ztx("z3", 3, "trade", [ztransfer(0, "in", TOKEN_C, "C", 100, 300.0, POOL),
                               ztransfer(1, "out", USDC, "USDC", 300, 300.0, ROUTER, decimals=6)], 103),
        ztx("z2", 4, "trade", [ztransfer(0, "in", TOKEN_B, "B", 200, 1000.0, POOL),
                               ztransfer(1, "out", NATIVE, "ETH", 0.5, 1000.0, ROUTER)], 102),
        ztx("z1", 5, "trade", [ztransfer(0, "in", TOKEN_A, "A", 500, 1000.0, POOL),
                               ztransfer(1, "out", USDC, "USDC", 1000, 1000.0, ROUTER, decimals=6)], 101),
        ztx("z0", 6, "receive", [ztransfer(0, "in", USDC, "USDC", 5000, 5000.0, SENDER, decimals=6)], 100),
    ]


class FakeZerion:
    def __init__(self, histories=None, portfolios=None, page_size=3, budget=None):
        self.histories = histories if histories is not None else {BUYER: buyer_zerion_history()}
        self.portfolios = portfolios if portfolios is not None else {
            BUYER: 6150.0, VAULT: 11350.0, SENDER: 2000.0
        }
        self.page_size = page_size
        self.budget = budget
        self.calls: Counter[str] = Counter()

    def _spend(self, name):
        if self.budget is not None and sum(self.calls.values()) >= self.budget:
            raise BudgetExhausted("budget")
        self.calls[name] += 1

    def transactions(self, address, since, cursor=None):
        self._spend("transactions")
        txs = [t for t in self.histories.get(address, []) if t.mined_at >= since]
        start = int(cursor or 0)
        end = start + self.page_size
        return TransactionsPage(txs[start:end], str(end) if end < len(txs) else None)

    def portfolio(self, address):
        self._spend("portfolio")
        total = self.portfolios.get(address, 0.0)
        return Portfolio(total, {"base": total})
```

- [ ] **Step 2: Écrire les tests du pré-filtre**

`backend/apps/wallets/tests/test_prefilter.py` :

```python
from datetime import timedelta

import pytest

from apps.discovery.models import PipelineSettings, Wallet
from apps.discovery.tests.factories import make_chain
from apps.wallets.models import KnownAddress, WalletProfile
from apps.wallets.services.qualification import Clients, prefilter_chains, prefilter_step
from apps.wallets.tests.factories import make_early_buy
from apps.wallets.tests.fakes import (
    BUYER, NOW, POOL, ROUTER, START, TOKEN_A, UNIT, FakeWalletHyperSync, FakeZerion, tr,
)

pytestmark = pytest.mark.django_db


@pytest.fixture
def chain():
    return make_chain()


def profile(address=BUYER, **kw):
    wallet, _ = Wallet.objects.get_or_create(address=address)
    return WalletProfile.objects.create(wallet=wallet, **kw)


def run(p, hs=None):
    return prefilter_step(p, Clients(lambda c: hs or FakeWalletHyperSync(), FakeZerion()), NOW, PipelineSettings.load())


def test_prefilter_chains_add_spotted_chain(chain):
    arb = make_chain(gt_id="arbitrum", evm_id=42161, zerion_id="arbitrum")
    make_chain(gt_id="unused", evm_id=99, zerion_id="unused")
    p = profile()
    make_early_buy(p.wallet, arb, TOKEN_A)
    assert sorted(c.gt_id for c in prefilter_chains(p, PipelineSettings.load())) == ["arbitrum", "base"]


def test_clean_wallet_passes_and_records_active_chains(chain):
    p = profile()
    assert run(p) == "prefiltered"
    assert p.active_chains == ["base"]
    assert p.metrics["prefilter"]["base"] == {"txs_7d": 10, "txs_active": 5}


def test_bot_on_totals(chain):
    p = profile()
    assert run(p, FakeWalletHyperSync(tx_counts={BUYER: 400})) == "filtered"
    assert p.filter_reason == "bot_frequency"
    assert p.next_analysis_at == NOW + timedelta(days=30)


def test_farmer(chain):
    many = [tr(START + i, f"0xf{i}", f"0x{i:040x}", POOL, BUYER, UNIT, POOL, f"0x{i:040x}") for i in range(301)]
    p = profile()
    run(p, FakeWalletHyperSync(transfers={BUYER: many}))
    assert p.filter_reason == "farmer"


def test_mev_bot(chain):
    round_trips = []
    for i in range(3):
        token = f"0x{i + 1:040x}"
        round_trips += [
            tr(START + i, f"0xb{i}", token, POOL, BUYER, UNIT, BUYER, ROUTER),
            tr(START + i, f"0xs{i}", token, BUYER, POOL, UNIT, BUYER, ROUTER),
        ]
    p = profile()
    run(p, FakeWalletHyperSync(transfers={BUYER: round_trips}))
    assert p.filter_reason == "bot_mev"


def test_known_exchange(chain):
    KnownAddress.objects.create(address=BUYER, kind="exchange")
    p = profile()
    run(p)
    assert p.filter_reason == "exchange"


def test_inactive_only_for_early_buyers(chain):
    quiet = FakeWalletHyperSync(tx_counts={BUYER: 1}, transfers={})
    assert run(profile(), quiet) == "filtered"
```

- [ ] **Step 3: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/wallets/tests/test_prefilter.py -q"`
Expected: FAIL (`ImportError: cannot import name 'Clients'`).

- [ ] **Step 4: Implémenter**

Remplacer `backend/apps/wallets/services/qualification.py` par :

```python
"""Qualification v2 : pré-filtre HyperSync multi-chaînes, historique Zerion, décision par wallet."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

from apps.discovery.models import Chain, EarlyBuyer
from apps.wallets.models import KnownAddress, WalletProfile
from apps.wallets.services.blocks import block_at
from apps.wallets.services.classify import classify_all
from apps.wallets.services.filters import farmer_reason, mev_ratio, prefilter_reason
from apps.wallets.services.positions import TradeRecord
from apps.wallets.services.settings import qualification_thresholds

Status = WalletProfile.Status
Source = WalletProfile.Source
DAY = 86_400


@dataclass
class Clients:
    hypersync_for: Callable[[Chain], object]
    zerion: object


def filter_out(profile: WalletProfile, reason: str, now: datetime) -> str:
    profile.status = Status.FILTERED
    profile.filter_reason = reason
    profile.analyzed_at = now
    profile.next_analysis_at = now + timedelta(days=qualification_thresholds().refilter_after_days)
    profile.save()
    return profile.status


def prefilter_chains(profile: WalletProfile, cfg) -> list[Chain]:
    spotted = EarlyBuyer.objects.filter(wallet=profile.wallet).values_list(
        "explosion__candidate__token__chain__gt_id", flat=True
    )
    wanted = set(cfg.prefilter_chains) | set(spotted)
    return list(Chain.objects.active().filter(gt_id__in=wanted).order_by("gt_id"))


def prefilter_step(profile: WalletProfile, clients: Clients, now: datetime, cfg) -> str:
    """Bot, inactif, farmer, MEV et exchange, mesurés sur plusieurs chaînes via HyperSync."""
    wallet = profile.wallet
    t = qualification_thresholds()
    chains = prefilter_chains(profile, cfg)
    if not chains:
        return filter_out(profile, "no_chain", now)
    if KnownAddress.objects.blocking_for(wallet.address, [c.pk for c in chains]).exists():
        return filter_out(profile, "exchange", now)

    ts = int(now.timestamp())
    total_7d = total_active = 0
    received: set[tuple[str, str]] = set()
    records: list[TradeRecord] = []
    measures: dict[str, dict] = {}
    active: list[str] = []
    for chain in chains:
        hypersync = clients.hypersync_for(chain)
        height = hypersync.height()
        week = block_at(hypersync, chain, ts - 7 * DAY, height)
        recent = block_at(hypersync, chain, ts - t.inactive_days * DAY, height)
        start = block_at(hypersync, chain, ts - t.history_days * DAY, height)
        txs_7d = hypersync.wallet_tx_count(wallet.address, week, height, cap=t.max_txs_per_day * 7 + 1)
        txs_active = hypersync.wallet_tx_count(wallet.address, recent, height, cap=t.min_txs_active)
        total_7d += txs_7d
        total_active += txs_active
        measures[chain.gt_id] = {"txs_7d": txs_7d, "txs_active": txs_active}
        if total_7d > t.max_txs_per_day * 7:
            break
        transfers = hypersync.wallet_transfers(wallet.address, start, height)
        received |= {(chain.gt_id, x.token) for x in transfers if x.recipient == wallet.address}
        records += [
            TradeRecord(
                chain.gt_id, trade.transfer.token, trade.kind, trade.transfer.amount, None,
                trade.transfer.timestamp, trade.transfer.block, trade.counterparty,
            )
            for trade in classify_all(transfers, wallet.address)
        ]
        if txs_active or transfers:
            active.append(chain.gt_id)

    profile.metrics = {**profile.metrics, "prefilter": measures, "distinct_received": len(received)}
    profile.active_chains = active
    reason = (
        prefilter_reason(total_7d, total_active, t, profile.source)
        or farmer_reason(len(received), t)
        or ("bot_mev" if mev_ratio(records, set()) > t.max_mev_ratio else None)
    )
    if reason:
        return filter_out(profile, reason, now)
    profile.status = Status.PREFILTERED
    profile.save(update_fields=["status", "metrics", "active_chains"])
    return profile.status
```

- [ ] **Step 5: Lancer les tests**

Run: `make test args="apps/wallets -q"`
Expected: PASS.

- [ ] **Step 6: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend/apps/wallets
git commit -m "feat(wallets): pré-filtre HyperSync additionné sur plusieurs chaînes EVM

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Historique Zerion complet (6 mois, reprise au curseur)

**Files:**
- Modify: `backend/apps/wallets/services/qualification.py`
- Create: `backend/apps/wallets/tests/test_history.py`

**Interfaces:**
- Consumes: `ZerionClient.transactions` (Task 1), `movements`, `counterparty` (Task 3), modèles v2 (Task 2), `aggregate_positions`.
- Produces : `save_transactions(wallet, transactions)`, `records_for(wallet) -> list[TradeRecord]`, `recompute_positions(wallet)`, `history_step(profile, clients, now) -> str` (lève `BudgetExhausted` ; curseur enregistré après chaque page).

- [ ] **Step 1: Écrire les tests**

`backend/apps/wallets/tests/test_history.py` :

```python
from decimal import Decimal

import pytest

from apps.discovery.models import Wallet
from apps.discovery.tests.factories import make_chain
from apps.wallets.models import TokenPosition, TokenTrade, WalletProfile, WalletTransaction
from apps.wallets.services.qualification import Clients, history_step
from apps.wallets.tests.fakes import BUYER, NOW, TOKEN_A, USDC, FakeWalletHyperSync, FakeZerion
from integrations.errors import BudgetExhausted
from integrations.zerion import NATIVE

pytestmark = pytest.mark.django_db


@pytest.fixture
def profile():
    make_chain()
    return WalletProfile.objects.create(wallet=Wallet.objects.create(address=BUYER), status="prefiltered")


def run(p, zerion):
    return history_step(p, Clients(lambda c: FakeWalletHyperSync(), zerion), NOW)


def test_full_history_is_stored_with_zerion_data(profile):
    zerion = FakeZerion()
    assert run(profile, zerion) == "history_fetched"
    assert zerion.calls["transactions"] == 2
    assert profile.history_complete and profile.history_cursor == ""
    assert WalletTransaction.objects.filter(wallet=profile.wallet).count() == 6
    buy = TokenTrade.objects.get(wallet=profile.wallet, token_address=TOKEN_A, kind="buy")
    assert (buy.direction, buy.token_symbol, buy.value_usd, buy.quantity) == ("in", "A", Decimal("1000.00"), Decimal("500"))
    assert buy.transaction.operation_type == "trade"
    eth = TokenTrade.objects.get(wallet=profile.wallet, token_address=NATIVE)
    assert (eth.kind, eth.value_usd) == ("sell", Decimal("1000.00"))
    assert TokenTrade.objects.get(wallet=profile.wallet, kind="send").counterparty != ""
    a = TokenPosition.objects.get(wallet=profile.wallet, token_address=TOKEN_A)
    assert (a.bought_amount, a.sent_amount, a.bought_usd) == (500 * 10**18, 450 * 10**18, Decimal("1000.00"))
    usdc = TokenPosition.objects.get(wallet=profile.wallet, token_address=USDC)
    assert usdc.received_amount == 5000 * 10**6
    assert profile.last_mined_at == WalletTransaction.objects.latest("mined_at").mined_at


def test_budget_exhaustion_keeps_cursor_and_resumes(profile):
    with pytest.raises(BudgetExhausted):
        run(profile, FakeZerion(budget=1))
    profile.refresh_from_db()
    assert (profile.status, profile.history_cursor, profile.history_complete) == ("prefiltered", "3", False)
    assert WalletTransaction.objects.count() == 3
    resumed = FakeZerion()
    run(profile, resumed)
    assert resumed.calls["transactions"] == 1
    assert WalletTransaction.objects.count() == 6


def test_history_is_idempotent(profile):
    run(profile, FakeZerion())
    profile.status = "prefiltered"
    profile.history_complete = False
    run(profile, FakeZerion())
    assert TokenTrade.objects.count() == 11
```

(6 transactions : 1 envoi, 4 trades de 2 transferts, 1 réception = 11 mouvements.)

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/wallets/tests/test_history.py -q"`
Expected: FAIL.

- [ ] **Step 3: Implémenter**

Ajouter à `backend/apps/wallets/services/qualification.py` (imports à regrouper en tête : `from datetime import UTC` et `from decimal import Decimal` ; `from django.db import transaction as db_transaction` ; `from django.db.models import Max` ; `from apps.wallets.models import TokenPosition, TokenTrade, WalletTransaction` ; `from apps.wallets.services.positions import aggregate_positions` ; `from apps.wallets.services.zerion_history import counterparty, movements`) :

```python
BATCH_SIZE = 1000


def _usd(value: float | None, places: int = 2) -> Decimal | None:
    return Decimal(str(round(value, places))) if value is not None else None


def save_transactions(wallet, transactions) -> None:
    """Transactions Zerion et leurs mouvements, mis à jour à la relance."""
    kept = [(tx, rows) for tx in transactions if (rows := movements(tx))]
    if not kept:
        return
    WalletTransaction.objects.bulk_create(
        [
            WalletTransaction(
                wallet=wallet, zerion_id=tx.zerion_id, chain=tx.chain, tx_hash=tx.tx_hash,
                block=tx.block, mined_at=tx.mined_at, operation_type=tx.operation_type,
                status=tx.status, fee_usd=_usd(tx.fee_usd, 6), raw=tx.raw,
            )
            for tx, _ in kept
        ],
        update_conflicts=True,
        unique_fields=["wallet", "zerion_id"],
        update_fields=["chain", "tx_hash", "block", "mined_at", "operation_type", "status", "fee_usd", "raw"],
        batch_size=BATCH_SIZE,
    )
    ids = dict(
        WalletTransaction.objects.filter(
            wallet=wallet, zerion_id__in=[tx.zerion_id for tx, _ in kept]
        ).values_list("zerion_id", "id")
    )
    TokenTrade.objects.bulk_create(
        [
            TokenTrade(
                transaction_id=ids[tx.zerion_id], wallet=wallet, transfer_index=t.index,
                chain=t.chain, token_address=t.token_address, token_symbol=t.token_symbol[:64],
                token_decimals=t.token_decimals, fungible_id=t.fungible_id[:100], kind=kind,
                direction=t.direction, quantity=t.quantity, amount=Decimal(t.amount),
                price_usd=Decimal(str(t.price_usd)) if t.price_usd is not None else None,
                value_usd=_usd(t.value_usd), counterparty=counterparty(t)[:42],
                block=tx.block, mined_at=tx.mined_at,
            )
            for tx, rows in kept
            for t, kind in rows
        ],
        update_conflicts=True,
        unique_fields=["transaction", "transfer_index"],
        update_fields=["kind", "quantity", "amount", "price_usd", "value_usd", "counterparty"],
        batch_size=BATCH_SIZE,
    )


def records_for(wallet) -> list[TradeRecord]:
    return [
        TradeRecord(
            chain_id=row.chain,
            token=row.token_address,
            kind=row.kind,
            amount=int(row.amount),
            usd=float(row.value_usd) if row.value_usd is not None else None,
            ts=int(row.mined_at.timestamp()),
            block=row.block or 0,
            counterparty=row.counterparty,
        )
        for row in TokenTrade.objects.filter(wallet=wallet)
    ]


def recompute_positions(wallet) -> None:
    stats = aggregate_positions(records_for(wallet))
    symbols = dict(
        TokenTrade.objects.filter(wallet=wallet)
        .values_list("token_address", "token_symbol")
        .distinct()
    )
    with db_transaction.atomic():
        TokenPosition.objects.filter(wallet=wallet).delete()
        TokenPosition.objects.bulk_create(
            [
                TokenPosition(
                    wallet=wallet, chain=chain, token_address=token,
                    token_symbol=symbols.get(token, ""),
                    bought_amount=Decimal(s.bought), sold_amount=Decimal(s.sold),
                    sent_amount=Decimal(s.sent), received_amount=Decimal(s.received),
                    bought_usd=_usd(s.bought_usd), sold_usd=_usd(s.sold_usd),
                    buys=s.buys, sells=s.sells,
                    first_at=datetime.fromtimestamp(s.first_ts, UTC),
                    last_at=datetime.fromtimestamp(s.last_ts, UTC),
                )
                for (chain, token), s in stats.items()
            ],
            batch_size=BATCH_SIZE,
        )


def history_step(profile: WalletProfile, clients: Clients, now: datetime) -> str:
    """Récupère l'historique Zerion jusqu'au bout ; le curseur est enregistré après chaque page."""
    wallet = profile.wallet
    t = qualification_thresholds()
    if profile.history_complete and profile.last_mined_at:
        since = profile.last_mined_at
    else:
        since = now - timedelta(days=t.history_days)
    cursor = profile.history_cursor or None
    while True:
        page = clients.zerion.transactions(wallet.address, since, cursor)
        save_transactions(wallet, page.transactions)
        cursor = page.next_cursor
        profile.history_cursor = cursor or ""
        profile.save(update_fields=["history_cursor"])
        if cursor is None:
            break
    profile.history_complete = True
    profile.last_mined_at = WalletTransaction.objects.filter(wallet=wallet).aggregate(
        latest=Max("mined_at")
    )["latest"]
    recompute_positions(wallet)
    profile.status = Status.HISTORY_FETCHED
    profile.save(update_fields=["history_complete", "last_mined_at", "status"])
    return profile.status
```

- [ ] **Step 4: Lancer les tests**

Run: `make test args="apps/wallets -q"`
Expected: PASS.

- [ ] **Step 5: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend/apps/wallets
git commit -m "feat(wallets): historique Zerion complet sur 6 mois, reprise au curseur

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Wallets liés directs (liens forts, anti-exchange) et financement informatif

**Files:**
- Modify: `backend/apps/wallets/services/entities.py`, `backend/apps/wallets/services/qualification.py`
- Create: `backend/apps/wallets/tests/test_links.py`

**Interfaces:**
- Consumes: `transfer_after_buy_targets`, `big_receive_targets`, `add_link` (entities), `detect_exchange` (exchanges), `records_for`.
- Produces : `entities.ensure_linked_profile(address) -> WalletProfile` ; `qualification.link_step(profile, clients, now, records) -> None`.

- [ ] **Step 1: Écrire les tests**

`backend/apps/wallets/tests/test_links.py` :

```python
import pytest

from apps.discovery.models import Wallet
from apps.discovery.tests.factories import make_chain
from apps.wallets.models import KnownAddress, WalletLink, WalletProfile
from apps.wallets.services.qualification import Clients, history_step, link_step, records_for
from apps.wallets.tests.factories import make_early_buy
from apps.wallets.tests.fakes import (
    BUYER, DEPOSIT, HOT, NOW, SENDER, START, TOKEN_A, VAULT, FakeWalletHyperSync, FakeZerion,
    buyer_zerion_history, tr,
)

pytestmark = pytest.mark.django_db


@pytest.fixture
def chain():
    return make_chain()


def fetched(hs=None, zerion=None):
    p = WalletProfile.objects.create(wallet=Wallet.objects.create(address=BUYER), status="prefiltered")
    clients = Clients(lambda c: hs or FakeWalletHyperSync(), zerion or FakeZerion())
    history_step(p, clients, NOW)
    return p, clients


def test_strong_links_and_linked_profiles(chain):
    p, clients = fetched()
    link_step(p, clients, NOW, records_for(p.wallet))
    assert WalletLink.objects.filter(from_wallet__address=BUYER, to_wallet__address=VAULT, kind="transfer_after_buy").exists()
    big = WalletLink.objects.get(from_wallet__address=SENDER, to_wallet__address=BUYER, kind="big_receive")
    assert big.evidence["pct"] == 65.79
    linked = WalletProfile.objects.filter(source="linked")
    assert sorted(x.wallet.address for x in linked) == [VAULT, SENDER] and all(x.depth == 1 for x in linked)


def test_funding_is_informative_link(chain):
    p, clients = fetched()
    make_early_buy(p.wallet, chain, TOKEN_A)
    link_step(p, clients, NOW, records_for(p.wallet))
    assert WalletLink.objects.filter(kind="funding", from_wallet__address=SENDER).exists()


def test_exchange_deposit_is_not_linked(chain):
    hs = FakeWalletHyperSync(
        transfers={DEPOSIT: [tr(START, "0xd1", TOKEN_A, BUYER, DEPOSIT, 1, BUYER, TOKEN_A),
                             tr(START + 1, "0xd2", TOKEN_A, DEPOSIT, HOT, 1, HOT, TOKEN_A)]},
        tx_counts={DEPOSIT: 0},
    )
    zerion = FakeZerion(histories={BUYER: buyer_zerion_history(send_to=DEPOSIT)})
    p, clients = fetched(hs, zerion)
    link_step(p, clients, NOW, records_for(p.wallet))
    assert not WalletLink.objects.filter(to_wallet__address=DEPOSIT).exists()
    assert KnownAddress.objects.get(address=DEPOSIT).kind == "cex_deposit"
    assert not WalletProfile.objects.filter(wallet__address=DEPOSIT).exists()


def test_link_step_is_idempotent(chain):
    p, clients = fetched()
    link_step(p, clients, NOW, records_for(p.wallet))
    link_step(p, clients, NOW, records_for(p.wallet))
    assert WalletLink.objects.filter(kind__in=["transfer_after_buy", "big_receive"]).count() == 2
```

(Entrées valorisées de BUYER : C 300 + C 300 + B 1 000 + A 1 000 + USDC 5 000 = 7 600 $ ; 5 000 / 7 600 = 65,79 %.)

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/wallets/tests/test_links.py -q"`
Expected: FAIL.

- [ ] **Step 3: Implémenter**

Ajouter à `backend/apps/wallets/services/entities.py` (import `from apps.wallets.models import WalletProfile` en tête) :

```python
def ensure_linked_profile(address: str) -> WalletProfile:
    """Profil d'un wallet lié direct : valorisé, jamais suivi plus loin."""
    wallet, _ = Wallet.objects.get_or_create(address=address.lower())
    profile, _ = WalletProfile.objects.get_or_create(
        wallet=wallet, defaults={"source": WalletProfile.Source.LINKED, "depth": 1}
    )
    return profile
```

Ajouter à `backend/apps/wallets/services/qualification.py` (imports : `WalletLink` depuis les modèles ; `from apps.wallets.services.entities import ZERO_ADDRESS, add_link, big_receive_targets, ensure_linked_profile, transfer_after_buy_targets` ; `from apps.wallets.services.exchanges import detect_exchange`) :

```python
def link_step(profile: WalletProfile, clients: Clients, now: datetime, records) -> None:
    """Liens forts vers les wallets liés directs (après contrôle anti-exchange) + financement informatif."""
    t = qualification_thresholds()
    wallet = profile.wallet
    candidates = [
        (key, WalletLink.Kind.TRANSFER_AFTER_BUY, evidence, True)
        for key, evidence in transfer_after_buy_targets(records, t.transfer_after_buy_pct).items()
    ] + [
        (key, WalletLink.Kind.BIG_RECEIVE, evidence, False)
        for key, evidence in big_receive_targets(records, t.big_receive_pct).items()
    ]
    contexts: dict[int, tuple] = {}

    def context(chain):
        if chain.pk not in contexts:
            hypersync = clients.hypersync_for(chain)
            contexts[chain.pk] = (hypersync, hypersync.height())
        return contexts[chain.pk]

    for (chain_id, other), kind, evidence, outgoing in candidates:
        if other in ("", ZERO_ADDRESS, wallet.address):
            continue
        chain = Chain.objects.active().filter(zerion_id=chain_id).first()
        if chain is None:
            continue
        hypersync, height = context(chain)
        if detect_exchange(other, chain, hypersync, t, now, height):
            continue
        source, target = (wallet.address, other) if outgoing else (other, wallet.address)
        add_link(source, target, kind, {**evidence, "chain": chain_id})
        ensure_linked_profile(other)

    spotted = Chain.objects.active().filter(
        gt_id__in=EarlyBuyer.objects.filter(wallet=wallet).values_list(
            "explosion__candidate__token__chain__gt_id", flat=True
        )
    )
    for chain in spotted:
        hypersync, height = context(chain)
        funding = hypersync.first_funding(wallet.address, height)
        if funding and not KnownAddress.objects.blocking_for(funding.funder, [chain.pk]).exists():
            add_link(
                funding.funder, wallet.address, WalletLink.Kind.FUNDING,
                {"chain": chain.gt_id, "block": funding.block, "value": str(funding.value)},
            )
```

- [ ] **Step 4: Lancer les tests**

Run: `make test args="apps/wallets -q"`
Expected: PASS.

- [ ] **Step 5: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend/apps/wallets
git commit -m "feat(wallets): wallets liés directs par liens forts, financement informatif

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Décision — filtres Zerion, valeur (wallet + wallets liés), tags

**Files:**
- Modify: `backend/apps/wallets/services/qualification.py`
- Create: `backend/apps/wallets/tests/test_decision.py`

**Interfaces:**
- Consumes: `ZerionClient.portfolio` (Task 1), `is_quote` (Task 3), `link_step` (Task 6), `records_for` (Task 5), `wallet_tags`, `EarlyBuy`, `farmer_reason`, `history_reason`.
- Produces : `PORTFOLIO_REASONS`, `quote_tokens(wallet, cfg) -> set[str]`, `early_buys(wallet) -> list[EarlyBuy]`, `linked_profiles(wallet) -> QuerySet[WalletProfile]`, `evaluate_wallet(profile) -> str`, `decide_step(profile, clients, now, cfg) -> str`, `value_linked_step(profile, clients, now) -> str`.

- [ ] **Step 1: Écrire les tests**

`backend/apps/wallets/tests/test_decision.py` :

```python
from decimal import Decimal

import pytest

from apps.discovery.models import PipelineSettings, Wallet
from apps.discovery.tests.factories import make_chain
from apps.wallets.models import QualificationSettings, WalletProfile
from apps.wallets.services.qualification import (
    Clients, decide_step, history_step, value_linked_step,
)
from apps.wallets.tests.factories import make_early_buy
from apps.wallets.tests.fakes import (
    BUYER, NOW, SENDER, TOKEN_A, VAULT, FakeWalletHyperSync, FakeZerion, buyer_zerion_history,
)
from integrations.errors import BudgetExhausted

pytestmark = pytest.mark.django_db


@pytest.fixture
def chain():
    return make_chain()


def decided(zerion=None, before_decide=None):
    zerion = zerion or FakeZerion()
    clients = Clients(lambda c: FakeWalletHyperSync(), zerion)
    p = WalletProfile.objects.create(wallet=Wallet.objects.create(address=BUYER), status="prefiltered")
    history_step(p, clients, NOW)
    if before_decide:
        before_decide(p)
    decide_step(p, clients, NOW, PipelineSettings.load())
    return p, clients


def test_buyer_is_judged_with_its_linked_wallets(chain):
    p, clients = decided()
    assert (p.status, p.filter_reason, p.portfolio_value_usd) == ("filtered", "portfolio_too_small", Decimal("6150.00"))
    vault = WalletProfile.objects.get(wallet__address=VAULT)
    assert value_linked_step(vault, clients, NOW) == "valued"
    p.refresh_from_db()
    assert (p.status, p.linked_value_usd) == ("qualified", Decimal("11350.00"))
    value_linked_step(WalletProfile.objects.get(wallet__address=SENDER), clients, NOW)
    p.refresh_from_db()
    assert (p.status, p.linked_value_usd) == ("qualified", Decimal("13350.00"))


def test_farmer_rechecked_on_zerion(chain):
    QualificationSettings.objects.filter(chain=None).update(max_distinct_tokens=3)
    p, _ = decided()
    assert p.filter_reason == "farmer"


def test_too_few_trades(chain):
    zerion = FakeZerion(histories={BUYER: buyer_zerion_history()[4:]})
    p, _ = decided(zerion)
    assert p.filter_reason == "too_few_trades"


def test_tags_use_early_buys(chain):
    p, _ = decided(before_decide=lambda p: make_early_buy(p.wallet, chain, TOKEN_A, is_sniper=True, sold=10))
    assert p.tags == ["HOLDER", "SNIPER"]


def test_portfolio_budget_exhaustion_keeps_status(chain):
    zerion = FakeZerion()
    clients = Clients(lambda c: FakeWalletHyperSync(), zerion)
    p = WalletProfile.objects.create(wallet=Wallet.objects.create(address=BUYER), status="prefiltered")
    history_step(p, clients, NOW)
    zerion.budget = sum(zerion.calls.values())
    with pytest.raises(BudgetExhausted):
        decide_step(p, clients, NOW, PipelineSettings.load())
    p.refresh_from_db()
    assert p.status == "history_fetched"


def test_linked_value_does_not_rescue_a_bot(chain):
    p, clients = decided()
    WalletProfile.objects.filter(pk=p.pk).update(filter_reason="bot_frequency")
    value_linked_step(WalletProfile.objects.get(wallet__address=VAULT), clients, NOW)
    p.refresh_from_db()
    assert (p.status, p.filter_reason) == ("filtered", "bot_frequency")
```

(`buyer_zerion_history()[4:]` = seulement l'achat de A et la réception d'USDC : 1 token acheté.)

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/wallets/tests/test_decision.py -q"`
Expected: FAIL.

- [ ] **Step 3: Implémenter**

Ajouter à `backend/apps/wallets/services/qualification.py` (imports : `STRONG_LINKS` depuis les modèles ; `from apps.wallets.services.classify import BUY, RECEIVE` ; `history_reason` depuis filters ; `from apps.wallets.services.tags import EarlyBuy, wallet_tags` ; `from apps.wallets.services.zerion_history import is_quote`) :

```python
PORTFOLIO_REASONS = ("portfolio_too_small", "portfolio_too_large")


def quote_tokens(wallet, cfg) -> set[str]:
    pairs = TokenTrade.objects.filter(wallet=wallet).values_list("token_symbol", "token_address").distinct()
    return {address for symbol, address in pairs if is_quote(symbol, address, cfg.quote_symbols)}


def early_buys(wallet) -> list[EarlyBuy]:
    return [
        EarlyBuy(
            token=e.explosion.candidate.token.address,
            chain_id=e.explosion.candidate.token.chain.zerion_id,
            is_sniper=e.is_sniper,
            bought=int(e.bought_amount),
            sold_before_peak=int(e.sold_amount),
        )
        for e in EarlyBuyer.objects.filter(wallet=wallet).select_related("explosion__candidate__token__chain")
    ]


def linked_profiles(wallet):
    outgoing = WalletLink.objects.filter(kind__in=STRONG_LINKS, from_wallet=wallet).values_list(
        "to_wallet_id", flat=True
    )
    incoming = WalletLink.objects.filter(kind__in=STRONG_LINKS, to_wallet=wallet).values_list(
        "from_wallet_id", flat=True
    )
    return WalletProfile.objects.filter(wallet_id__in=set(outgoing) | set(incoming))


def evaluate_wallet(profile: WalletProfile) -> str:
    """Valeur jugée = portefeuille du wallet + portefeuilles de ses wallets liés directs."""
    if profile.source != Source.EARLY_BUYER or profile.portfolio_value_usd is None:
        return profile.status
    judged = profile.status in (Status.HISTORY_FETCHED, Status.QUALIFIED) or (
        profile.status == Status.FILTERED and profile.filter_reason in PORTFOLIO_REASONS
    )
    if not judged:
        return profile.status
    t = qualification_thresholds()
    linked = sum(
        (p.portfolio_value_usd for p in linked_profiles(profile.wallet) if p.portfolio_value_usd is not None),
        Decimal(0),
    )
    total = profile.portfolio_value_usd + linked
    if total < Decimal(str(t.min_portfolio_usd)):
        status, reason = Status.FILTERED, "portfolio_too_small"
    elif total > Decimal(str(t.max_portfolio_usd)):
        status, reason = Status.FILTERED, "portfolio_too_large"
    else:
        status, reason = Status.QUALIFIED, ""
    profile.linked_value_usd = linked
    profile.status = status
    profile.filter_reason = reason
    profile.save(update_fields=["linked_value_usd", "status", "filter_reason"])
    return profile.status


def decide_step(profile: WalletProfile, clients: Clients, now: datetime, cfg) -> str:
    """Sur historique complet : filtres Zerion, liens, valeur, tags, puis décision."""
    t = qualification_thresholds()
    wallet = profile.wallet
    records = records_for(wallet)
    quotes = quote_tokens(wallet, cfg)
    distinct_in = len({(r.chain_id, r.token) for r in records if r.kind in (BUY, RECEIVE)})
    reason = farmer_reason(distinct_in, t) or history_reason(records, quotes, t, profile.source)
    if reason:
        return filter_out(profile, reason, now)
    link_step(profile, clients, now, records)
    portfolio = clients.zerion.portfolio(wallet.address)
    profile.portfolio_value_usd = _usd(portfolio.total_usd)
    profile.metrics = {
        **profile.metrics,
        "portfolio_by_chain": {k: round(v, 2) for k, v in portfolio.by_chain.items()},
    }
    profile.tags = wallet_tags(records, early_buys(wallet), quotes, t)
    profile.analyzed_at = now
    profile.attempts = 0
    profile.save()
    return evaluate_wallet(profile)


def value_linked_step(profile: WalletProfile, clients: Clients, now: datetime) -> str:
    """Wallet lié : 1 appel /portfolio, puis réévaluation des wallets qui lui sont liés."""
    portfolio = clients.zerion.portfolio(profile.wallet.address)
    profile.portfolio_value_usd = _usd(portfolio.total_usd)
    profile.metrics = {
        **profile.metrics,
        "portfolio_by_chain": {k: round(v, 2) for k, v in portfolio.by_chain.items()},
    }
    profile.status = Status.VALUED
    profile.analyzed_at = now
    profile.save()
    for other in linked_profiles(profile.wallet):
        evaluate_wallet(other)
    return profile.status
```

- [ ] **Step 4: Lancer les tests**

Run: `make test args="apps/wallets -q"`
Expected: PASS.

- [ ] **Step 5: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend/apps/wallets
git commit -m "feat(wallets): décision sur historique complet, valeur du wallet et de ses wallets liés

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Orchestration, priorité, mise à jour incrémentale et tâches Celery

**Files:**
- Modify: `backend/apps/wallets/services/qualification.py`
- Rewrite: `backend/apps/wallets/tasks.py`
- Create: `backend/apps/wallets/tests/test_tasks.py`

**Interfaces:**
- Consumes: étapes des tâches 4 à 7 ; `clients.hypersync`, `clients.zerion` ; `BudgetExhausted`.
- Produces :
  - `qualification.compute_priority(wallet) -> float`, `enqueue_profiles(now, cfg) -> int`, `qualify_wallet(profile, clients, now, cfg) -> str`, `refresh_wallet(profile, clients, now, cfg) -> str`.
  - `tasks.build_clients(cfg) -> Clients`, `record_failure(profile, exc, max_attempts, now)`, `scheduled_profiles(now, cfg) -> tuple[list[int], list[int]]`, tâches `qualify_wallets_task() -> dict`, `qualify_wallet_task(profile_id) -> str`, `refresh_wallet_task(profile_id) -> str`.

- [ ] **Step 1: Écrire les tests**

`backend/apps/wallets/tests/test_tasks.py` :

```python
from datetime import timedelta
from unittest.mock import patch

import pytest

from apps.discovery.models import PipelineSettings, Wallet
from apps.discovery.tests.factories import make_chain
from apps.wallets import tasks
from apps.wallets.models import WalletProfile
from apps.wallets.services.qualification import (
    Clients, compute_priority, enqueue_profiles, qualify_wallet, refresh_wallet,
)
from apps.wallets.tests.factories import make_early_buy
from apps.wallets.tests.fakes import (
    BUYER, NOW, SENDER, TOKEN_A, TOKEN_B, VAULT, FakeWalletHyperSync, FakeZerion,
    buyer_zerion_history, ztransfer, ztx, POOL,
)
from integrations.errors import BudgetExhausted

pytestmark = pytest.mark.django_db


@pytest.fixture
def chain():
    return make_chain()


def test_priority_prefers_more_explosions(chain):
    one = Wallet.objects.create(address="0x" + "1" * 40)
    two = Wallet.objects.create(address="0x" + "2" * 40)
    make_early_buy(one, chain, TOKEN_A)
    make_early_buy(two, chain, TOKEN_A)
    make_early_buy(two, chain, TOKEN_B)
    assert compute_priority(two) > compute_priority(one) > 0


def test_enqueue_creates_profiles_and_reopens_filtered(chain):
    wallet = Wallet.objects.create(address=BUYER)
    make_early_buy(wallet, chain, TOKEN_A)
    assert enqueue_profiles(NOW, PipelineSettings.load()) == 1
    profile = WalletProfile.objects.get()
    assert profile.priority > 0
    WalletProfile.objects.filter(pk=profile.pk).update(
        status="filtered", filter_reason="inactive",
        analyzed_at=NOW - timedelta(days=40), next_analysis_at=NOW - timedelta(days=10),
    )
    make_early_buy(wallet, chain, TOKEN_B)
    enqueue_profiles(NOW, PipelineSettings.load())
    profile.refresh_from_db()
    assert (profile.status, profile.history_complete) == ("pending", False)


def test_full_pipeline_for_buyer_then_linked_wallets(chain):
    clients = Clients(lambda c: FakeWalletHyperSync(), FakeZerion())
    cfg = PipelineSettings.load()
    buyer = WalletProfile.objects.create(wallet=Wallet.objects.create(address=BUYER))
    assert qualify_wallet(buyer, clients, NOW, cfg) == "filtered"
    for address in (VAULT, SENDER):
        linked = WalletProfile.objects.get(wallet__address=address)
        assert qualify_wallet(linked, clients, NOW, cfg) == "valued"
    buyer.refresh_from_db()
    assert buyer.status == "qualified"


def test_scheduling_order(chain):
    cfg = PipelineSettings.load()
    cfg.qualification_batch_size = 4
    cfg.save()
    mk = lambda a, **kw: WalletProfile.objects.create(wallet=Wallet.objects.create(address=a), **kw).pk
    new_low = mk("0x" + "1" * 40, priority=1)
    new_high = mk("0x" + "2" * 40, priority=5)
    in_progress = mk("0x" + "3" * 40, status="prefiltered", priority=0)
    linked = mk("0x" + "4" * 40, source="linked", depth=1)
    stale = mk("0x" + "5" * 40, status="qualified", history_complete=True, analyzed_at=NOW - timedelta(days=8))
    ids, refresh = tasks.scheduled_profiles(NOW, cfg)
    assert ids == [linked, in_progress, new_high, new_low]
    assert refresh == []
    cfg.qualification_batch_size = 10
    cfg.save()
    assert tasks.scheduled_profiles(NOW, cfg)[1] == [stale]


def test_refresh_fetches_only_new_transactions(chain):
    zerion = FakeZerion()
    clients = Clients(lambda c: FakeWalletHyperSync(), zerion)
    cfg = PipelineSettings.load()
    buyer = WalletProfile.objects.create(wallet=Wallet.objects.create(address=BUYER))
    qualify_wallet(buyer, clients, NOW, cfg)
    newer = ztx("z6", 0, "trade", [ztransfer(0, "in", TOKEN_B, "B", 10, 50.0, POOL)], 106)
    zerion.histories[BUYER] = [newer] + buyer_zerion_history()
    before = zerion.calls["transactions"]
    refresh_wallet(buyer, clients, NOW, cfg)
    assert zerion.calls["transactions"] - before == 1
    assert buyer.wallet.transactions.count() == 7


def test_task_failure_and_budget(chain):
    profile = WalletProfile.objects.create(wallet=Wallet.objects.create(address=BUYER))
    cfg = PipelineSettings.load()
    cfg.max_attempts = 2
    cfg.save()
    with patch.object(tasks, "build_clients"), patch.object(tasks, "qualify_wallet", side_effect=BudgetExhausted("x")):
        tasks.qualify_wallet_task.apply(args=[profile.pk])
    profile.refresh_from_db()
    assert (profile.status, profile.attempts) == ("pending", 0)
    with patch.object(tasks, "build_clients"), patch.object(tasks, "qualify_wallet", side_effect=RuntimeError("x")):
        tasks.qualify_wallet_task.apply(args=[profile.pk])
        tasks.qualify_wallet_task.apply(args=[profile.pk])
    profile.refresh_from_db()
    assert (profile.status, profile.filter_reason) == ("filtered", "error:RuntimeError")


def test_daily_task_schedules_subtasks(chain):
    WalletProfile.objects.create(wallet=Wallet.objects.create(address=BUYER))
    with patch.object(tasks.qualify_wallet_task, "delay") as delay, patch.object(tasks.refresh_wallet_task, "delay"):
        result = tasks.qualify_wallets_task.apply().get()
    assert result == {"new_profiles": 0, "scheduled": 1, "refresh": 0}
    delay.assert_called_once()


def test_build_clients_reuses_one_hypersync_client_per_chain(chain):
    with patch.object(tasks.clients, "zerion"), patch.object(tasks.clients, "hypersync", side_effect=lambda c, cfg: object()):
        built = tasks.build_clients(PipelineSettings.load())
        assert built.hypersync_for(chain) is built.hypersync_for(chain)
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/wallets/tests/test_tasks.py -q"`
Expected: FAIL.

- [ ] **Step 3: Implémenter l'orchestration**

Ajouter à `backend/apps/wallets/services/qualification.py` (import `from django.db.models import Count, Sum`) :

```python
def compute_priority(wallet) -> float:
    """Nombre d'explosions captées (poids fort), puis montant total des early buys."""
    stats = EarlyBuyer.objects.filter(wallet=wallet).aggregate(n=Count("id"), usd=Sum("bought_usd"))
    return stats["n"] * 1_000_000 + min(float(stats["usd"] or 0), 999_999.0)


def enqueue_profiles(now: datetime, cfg) -> int:
    created = 0
    new_wallets = (
        EarlyBuyer.objects.filter(wallet__profile__isnull=True).values_list("wallet_id", flat=True).distinct()
    )
    for wallet_id in new_wallets:
        _, was_created = WalletProfile.objects.get_or_create(wallet_id=wallet_id)
        created += int(was_created)

    waiting = WalletProfile.objects.filter(
        source=Source.EARLY_BUYER,
        status__in=[Status.PENDING, Status.PREFILTERED, Status.HISTORY_FETCHED],
    ).select_related("wallet")
    for profile in waiting:
        priority = compute_priority(profile.wallet)
        if priority != profile.priority:
            profile.priority = priority
            profile.save(update_fields=["priority"])

    due = WalletProfile.objects.filter(
        source=Source.EARLY_BUYER, status=Status.FILTERED, next_analysis_at__lte=now
    ).select_related("wallet")
    for profile in due:
        fresh = EarlyBuyer.objects.filter(
            wallet_id=profile.wallet_id, explosion__candidate__updated_at__gt=profile.analyzed_at
        ).exists()
        if fresh:
            profile.status = Status.PENDING
            profile.filter_reason = ""
            profile.attempts = 0
            profile.history_cursor = ""
            profile.history_complete = False
            profile.priority = compute_priority(profile.wallet)
            profile.save()
    return created


def qualify_wallet(profile: WalletProfile, clients: Clients, now: datetime, cfg) -> str:
    if profile.source == Source.LINKED:
        if profile.status == Status.PENDING:
            return value_linked_step(profile, clients, now)
        return profile.status
    if profile.status == Status.PENDING:
        prefilter_step(profile, clients, now, cfg)
    if profile.status == Status.PREFILTERED:
        history_step(profile, clients, now)
    if profile.status == Status.HISTORY_FETCHED:
        decide_step(profile, clients, now, cfg)
    return profile.status


def refresh_wallet(profile: WalletProfile, clients: Clients, now: datetime, cfg) -> str:
    """Mise à jour incrémentale d'un wallet qualifié : nouvelles transactions, nouvelle décision."""
    history_step(profile, clients, now)
    return decide_step(profile, clients, now, cfg)
```

- [ ] **Step 4: Réécrire les tâches**

`backend/apps/wallets/tasks.py` :

```python
"""Tâches Celery de la qualification. Planning : tâche périodique `qualification-daily` (admin)."""

import logging
from datetime import timedelta

from celery import shared_task
from django.utils import timezone

from apps.discovery.models import PipelineSettings
from apps.discovery.services import clients
from apps.wallets.models import WalletProfile
from apps.wallets.services.qualification import (
    Clients,
    enqueue_profiles,
    filter_out,
    qualify_wallet,
    refresh_wallet,
)
from integrations.errors import BudgetExhausted

logger = logging.getLogger(__name__)
Status = WalletProfile.Status
Source = WalletProfile.Source
QUALIFYING = (Status.PENDING, Status.PREFILTERED, Status.HISTORY_FETCHED)


def build_clients(cfg: PipelineSettings) -> Clients:
    # Un client HyperSync par chaîne : son cache de timestamps de blocs sert à toutes les étapes.
    hypersync_clients: dict[int, object] = {}

    def hypersync_for(chain):
        if chain.pk not in hypersync_clients:
            hypersync_clients[chain.pk] = clients.hypersync(chain, cfg)
        return hypersync_clients[chain.pk]

    return Clients(hypersync_for=hypersync_for, zerion=clients.zerion(cfg))


def record_failure(profile: WalletProfile, exc: Exception, max_attempts: int, now) -> None:
    logger.exception("Échec de qualification du wallet %s", profile.wallet_id, exc_info=exc)
    profile.attempts += 1
    if profile.attempts >= max_attempts:
        filter_out(profile, f"error:{type(exc).__name__}"[:64], now)
    else:
        profile.save(update_fields=["attempts"])


def scheduled_profiles(now, cfg) -> tuple[list[int], list[int]]:
    """Wallets liés d'abord, puis historiques en cours, puis nouveaux (par priorité), puis mises à jour."""
    profiles = WalletProfile.objects.values_list("id", flat=True)
    linked = list(profiles.filter(source=Source.LINKED, status=Status.PENDING).order_by("id"))
    early = profiles.filter(source=Source.EARLY_BUYER)
    in_progress = list(
        early.filter(status__in=[Status.PREFILTERED, Status.HISTORY_FETCHED]).order_by("-priority", "id")
    )
    new = list(early.filter(status=Status.PENDING).order_by("-priority", "id"))
    ids = (linked + in_progress + new)[: cfg.qualification_batch_size]
    room = cfg.qualification_batch_size - len(ids)
    refresh = list(
        early.filter(
            status=Status.QUALIFIED,
            history_complete=True,
            analyzed_at__lte=now - timedelta(days=cfg.history_refresh_days),
        ).order_by("analyzed_at")[: max(room, 0)]
    )
    return ids, refresh


@shared_task
def qualify_wallets_task() -> dict:
    cfg = PipelineSettings.load()
    now = timezone.now()
    created = enqueue_profiles(now, cfg)
    ids, refresh = scheduled_profiles(now, cfg)
    for profile_id in ids:
        qualify_wallet_task.delay(profile_id)
    for profile_id in refresh:
        refresh_wallet_task.delay(profile_id)
    logger.info("%s nouveaux profils, %s programmés, %s mises à jour", created, len(ids), len(refresh))
    return {"new_profiles": created, "scheduled": len(ids), "refresh": len(refresh)}


def _run(profile_id: int, action, allowed) -> str:
    cfg = PipelineSettings.load()
    now = timezone.now()
    profile = WalletProfile.objects.select_related("wallet").get(pk=profile_id)
    if profile.status not in allowed:
        return profile.status
    try:
        return action(profile, build_clients(cfg), now, cfg)
    except BudgetExhausted:
        logger.info("Budget Zerion épuisé : wallet %s repris au prochain passage", profile_id)
        return profile.status
    except Exception as exc:
        record_failure(profile, exc, cfg.max_attempts, now)
        return profile.status


@shared_task
def qualify_wallet_task(profile_id: int) -> str:
    return _run(profile_id, qualify_wallet, QUALIFYING)


@shared_task
def refresh_wallet_task(profile_id: int) -> str:
    return _run(profile_id, refresh_wallet, (Status.QUALIFIED,))
```

- [ ] **Step 5: Lancer les tests**

Run: `make test args="-q"`
Expected: PASS.

- [ ] **Step 6: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend/apps/wallets
git commit -m "feat(wallets): orchestration v2, priorité, mise à jour incrémentale et tâches Celery

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: Admin v2

**Files:**
- Rewrite: `backend/apps/wallets/admin.py`
- Create: `backend/apps/wallets/forms.py`, `backend/apps/wallets/templates/admin/wallets/knownaddress/change_list.html`, `import_csv.html`
- Rewrite: `backend/apps/wallets/tests/test_admin.py`

**Interfaces:**
- Consumes: modèles v2, `import_known_addresses` (exchanges).
- Produces : URL `admin:wallets_knownaddress_import` ; listes filtrables des profils, transactions, mouvements, positions et liens.

- [ ] **Step 1: Écrire les tests**

`backend/apps/wallets/tests/test_admin.py` :

```python
import pytest
from django.core.files.uploadedfile import SimpleUploadedFile
from django.urls import reverse

from apps.discovery.models import Wallet
from apps.wallets.models import KnownAddress, WalletProfile

pytestmark = pytest.mark.django_db


@pytest.fixture
def admin_client(client, django_user_model):
    client.force_login(django_user_model.objects.create_superuser("admin", "a@example.com", "pw"))
    return client


@pytest.mark.parametrize(
    "model",
    ["walletprofile", "wallettransaction", "tokentrade", "tokenposition", "walletlink", "knownaddress", "qualificationsettings"],
)
def test_changelists_load(admin_client, model):
    assert admin_client.get(reverse(f"admin:wallets_{model}_changelist")).status_code == 200


def test_profile_page_loads(admin_client):
    profile = WalletProfile.objects.create(wallet=Wallet.objects.create(address="0x" + "a" * 40))
    assert admin_client.get(reverse("admin:wallets_walletprofile_change", args=[profile.pk])).status_code == 200


def test_import_view(admin_client):
    url = reverse("admin:wallets_knownaddress_import")
    assert admin_client.get(url).status_code == 200
    csv = SimpleUploadedFile("a.csv", b"address,kind,label,chain\n0x" + b"4" * 40 + b",exchange,OKX,\n")
    assert admin_client.post(url, {"file": csv}).status_code == 302
    assert KnownAddress.objects.get(address="0x" + "4" * 40).source == "import"
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/wallets/tests/test_admin.py -q"`
Expected: FAIL.

- [ ] **Step 3: Formulaire et templates**

Recréer `backend/apps/wallets/forms.py`, `templates/admin/wallets/knownaddress/change_list.html` et `import_csv.html` à l'identique de la v1 (plan `2026-09-27-qualification-wallets.md`, tâche 12, étape 4) :

```python
"""Formulaires de l'admin de la qualification."""

from django import forms


class KnownAddressImportForm(forms.Form):
    file = forms.FileField(label="Fichier CSV (colonnes : address, kind, label, chain)")
```

```html
{% extends "admin/change_list.html" %}
{% block object-tools-items %}
  <li><a href="{% url 'admin:wallets_knownaddress_import' %}">Importer un CSV</a></li>
  {{ block.super }}
{% endblock %}
```

```html
{% extends "admin/base_site.html" %}
{% block content %}
  <h1>Importer des adresses connues</h1>
  <p>Colonnes : <code>address</code>, <code>kind</code> (exchange, cex_deposit, service, bridge, router, mev), <code>label</code>, <code>chain</code> (gt_id, vide = toutes les chaînes).</p>
  <form method="post" enctype="multipart/form-data">
    {% csrf_token %}
    {{ form.as_p }}
    <input type="submit" value="Importer" class="default">
  </form>
{% endblock %}
```

- [ ] **Step 4: Admin**

`backend/apps/wallets/admin.py` :

```python
"""Admin de la qualification v2 : profils, transactions et mouvements Zerion, liens, réglages."""

import csv
import io

from django.contrib import admin, messages
from django.shortcuts import redirect
from django.template.response import TemplateResponse
from django.urls import path, reverse

from apps.wallets.forms import KnownAddressImportForm
from apps.wallets.models import (
    KnownAddress,
    QualificationSettings,
    TokenPosition,
    TokenTrade,
    WalletLink,
    WalletProfile,
    WalletTransaction,
)
from apps.wallets.services.exchanges import import_known_addresses


class ReadOnlyAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(WalletProfile)
class WalletProfileAdmin(ReadOnlyAdmin):
    list_display = [
        "wallet", "status", "filter_reason", "source", "priority", "portfolio_value_usd",
        "linked_value_usd", "history_complete", "active_chains", "tags",
    ]
    list_filter = ["status", "filter_reason", "source", "history_complete"]
    search_fields = ["wallet__address"]
    ordering = ["-priority"]
    list_select_related = ["wallet"]


@admin.register(WalletTransaction)
class WalletTransactionAdmin(ReadOnlyAdmin):
    list_display = ["mined_at", "wallet", "chain", "operation_type", "tx_hash", "fee_usd"]
    list_filter = ["operation_type", "chain"]
    search_fields = ["wallet__address", "tx_hash"]
    ordering = ["-mined_at"]
    list_select_related = ["wallet"]


@admin.register(TokenTrade)
class TokenTradeAdmin(ReadOnlyAdmin):
    list_display = [
        "mined_at", "wallet", "chain", "token_symbol", "kind", "direction", "quantity",
        "price_usd", "value_usd", "counterparty",
    ]
    list_filter = ["kind", "chain"]
    search_fields = ["wallet__address", "token_address", "token_symbol", "transaction__tx_hash"]
    ordering = ["-mined_at"]
    list_select_related = ["wallet"]


@admin.register(TokenPosition)
class TokenPositionAdmin(ReadOnlyAdmin):
    list_display = ["wallet", "chain", "token_symbol", "buys", "sells", "bought_usd", "sold_usd", "first_at", "last_at"]
    list_filter = ["chain"]
    search_fields = ["wallet__address", "token_address", "token_symbol"]
    ordering = ["-bought_usd"]


@admin.register(WalletLink)
class WalletLinkAdmin(ReadOnlyAdmin):
    list_display = ["from_wallet", "to_wallet", "kind", "evidence", "created_at"]
    list_filter = ["kind"]
    search_fields = ["from_wallet__address", "to_wallet__address"]


@admin.register(KnownAddress)
class KnownAddressAdmin(admin.ModelAdmin):
    list_display = ["address", "chain", "kind", "label", "source", "created_at"]
    list_filter = ["kind", "source", "chain"]
    search_fields = ["address", "label"]

    def get_urls(self):
        custom = [path("import/", self.admin_site.admin_view(self.import_view), name="wallets_knownaddress_import")]
        return custom + super().get_urls()

    def import_view(self, request):
        form = KnownAddressImportForm(request.POST or None, request.FILES or None)
        if request.method == "POST" and form.is_valid():
            text = io.TextIOWrapper(form.cleaned_data["file"].file, encoding="utf-8")
            count = import_known_addresses(list(csv.DictReader(text)))
            messages.success(request, f"{count} adresses importées.")
            return redirect(reverse("admin:wallets_knownaddress_changelist"))
        context = {**self.admin_site.each_context(request), "form": form}
        return TemplateResponse(request, "admin/wallets/knownaddress/import_csv.html", context)


@admin.register(QualificationSettings)
class QualificationSettingsAdmin(admin.ModelAdmin):
    list_display = ["__str__", "min_portfolio_usd", "history_days", "max_txs_per_day", "big_receive_pct"]
```

- [ ] **Step 5: Lancer les tests**

Run: `make test args="-q"`
Expected: PASS.

- [ ] **Step 6: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend/apps/wallets
git commit -m "feat(wallets): admin v2 (profils, transactions et mouvements Zerion, liens)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Tests live, documentation et essai réel

**Files:**
- Modify: `backend/integrations/tests/test_live_wallets.py`, `README.md`

- [ ] **Step 1: Test live de l'historique et du portfolio**

Ajouter à `backend/integrations/tests/test_live_wallets.py` :

```python
from datetime import UTC, datetime, timedelta


@pytest.mark.skipif(not os.environ.get("ZERION_API_KEY"), reason="ZERION_API_KEY absente")
def test_zerion_history_page_and_portfolio():
    client = ZerionClient(JsonHttpClient(BASE_URL, limiter=NoopLimiter(), auth=(os.environ["ZERION_API_KEY"], "")))
    page = client.transactions("0x11edfaca715703cb91c384d84cd2551122ab3019", datetime.now(UTC) - timedelta(days=180))
    assert page.transactions and page.transactions[0].transfers
    assert any(t.value_usd is not None for tx in page.transactions for t in tx.transfers)
    time.sleep(1.5)
    assert client.portfolio("0x11edfaca715703cb91c384d84cd2551122ab3019").total_usd > 0
```

Run: `make test args="-m live integrations -v"`
Expected: PASS.

- [ ] **Step 2: Documentation**

Dans `README.md`, remplacer la section « Qualification des wallets » par :

```markdown
## Qualification des wallets (v2)

Chaque jour à 08:00 UTC (tâche `qualification-daily`) :

1. **Pré-filtre HyperSync (gratuit)** sur les chaînes `prefilter_chains` (Base, Robinhood, BSC, Ethereum, Arc) + la chaîne où le wallet a été repéré : bot, inactif, farmer, MEV, exchange — mesures additionnées.
2. **Historique Zerion 6 mois**, toutes chaînes EVM, pour les survivants : chaque mouvement avec son type, sa quantité, son prix et sa valeur au moment de la transaction. Reprise au curseur le lendemain si le budget est atteint.
3. **Décision sur historique complet** : farmer / MEV revérifiés, liens forts (transfert après achat, gros transfert reçu en %), valeur `/portfolio` du wallet + de ses wallets liés directs, tags.
4. **Mise à jour incrémentale** des wallets qualifiés tous les `history_refresh_days` jours.

Ordre de traitement : wallets liés (1 appel), historiques en cours, nouveaux wallets par priorité. Budget Zerion : `zerion_daily_budget` (1 800/jour, plan Developer).
```

- [ ] **Step 3: Vérification complète**

```bash
make test && make lint
docker compose restart worker beat
```

Expected : tous les tests passent ; le worker liste `qualify_wallets_task`, `qualify_wallet_task`, `refresh_wallet_task`.

- [ ] **Step 4: Essai réel limité**

Régler `qualification_batch_size` à 20 dans l'admin, puis lancer directement (sans passer par le worker, pour suivre chaque résultat) :

```bash
docker compose exec web python manage.py shell -c "
from django.utils import timezone
from apps.discovery.models import PipelineSettings
from apps.wallets.models import WalletProfile
from apps.wallets.services.qualification import enqueue_profiles, qualify_wallet
from apps.wallets.tasks import build_clients, scheduled_profiles
cfg = PipelineSettings.load(); clients = build_clients(cfg)
print('nouveaux profils', enqueue_profiles(timezone.now(), cfg))
ids, _ = scheduled_profiles(timezone.now(), cfg)
for p in WalletProfile.objects.filter(id__in=ids).select_related('wallet'):
    try:
        status = qualify_wallet(p, clients, timezone.now(), cfg)
    except Exception as exc:
        status = f'{type(exc).__name__}: {exc}'
    print(p.wallet.address, status, p.filter_reason, p.portfolio_value_usd, p.linked_value_usd, p.tags, p.active_chains)
"
```

Expected : raisons variées au pré-filtre sans appel Zerion ; pour les survivants, des mouvements avec `value_usd` renseigné dans l'admin (Token trades), des wallets liés créés, et le compteur `ratelimit-org-day-remaining` de Zerion cohérent avec le budget.

- [ ] **Step 5: Commit**

```bash
git add backend/integrations/tests/test_live_wallets.py README.md
git commit -m "docs(wallets): tests live et documentation de la qualification v2

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
