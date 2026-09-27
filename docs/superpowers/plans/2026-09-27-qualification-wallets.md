# Qualification des wallets — Plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Qualifier chaque early buyer : écarter le bruit à bas coût, stocker son historique par token, le regrouper en entité avec ses wallets liés, juger la valeur de l'entité et lui attribuer des tags de comportement.

**Architecture:** HyperSync fournit toutes les données on-chain (transactions, transferts, financement, contreparties). Zerion ne sert qu'aux prix (lots de 25 tokens, courbes quotidiennes des actifs natifs, adresses des stablecoins), sous un budget quotidien Redis. Une nouvelle app `apps/wallets` contient les modèles, des fonctions pures (classement, positions, filtres, exchanges, entités, tags, prix) et une orchestration par étapes idempotentes, lancée par une tâche Celery quotidienne.

**Tech Stack:** Python 3.12, Django 5.2, PostgreSQL 16 (ArrayField, GIN), Celery 5.5 + django-celery-beat, Redis 7, httpx, hypersync 1.2, respx, pytest-django, ruff.

**Spec:** `docs/superpowers/specs/2026-09-27-qualification-wallets-design.md`

## Global Constraints

- Zerion **uniquement pour les prix** : jamais d'endpoint `/wallets/...`.
- Aucun paramètre réglable codé en dur : seuils dans `QualificationSettings` (global + par chaîne), budgets et listes dans `discovery.PipelineSettings`, planning dans `django-celery-beat`.
- Budget Zerion quotidien respecté ; budget épuisé = attente du lendemain, jamais une erreur.
- Adresses en minuscules ; montants bruts en `DecimalField(78, 0)`.
- Fonctions pures sans base ni réseau pour : classement, positions, filtres, forwarding, composantes connexes, tags, prix des mouvements.
- Étapes idempotentes ; écritures en masse avec `ignore_conflicts`.
- Tests sans réseau sauf `@pytest.mark.live`.
- Commandes via le `Makefile` (`make test args="..."`, `make lint`) dans le conteneur `web`.
- Chaque commit se termine par `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Écarts assumés avec la spec

1. **`link_min_share_pct` supprimé.** Une seule règle de transfert : `transfer_after_buy_pct` appliquée à tout token entré dans le wallet (acheté ou reçu, stablecoins compris). Couvre « acheter puis envoyer au coffre » et « recevoir des USDC puis les faire suivre ».
2. **`common_funder` n'est pas une ligne en base.** Deux wallets financés par le même financeur sont reliés par leurs deux liens `funding` vers lui ; l'union-find les regroupe. Au-delà de `funder_max_wallets` liens, le financeur est enregistré `KnownAddress(kind=service)` et ignoré.
3. **Nouveaux modèles / champs** : `DailyPrice` (courbes de prix mises en cache, visibles dans l'admin), `WalletProfile.source` / `depth` / `chains` (un wallet lié n'est pas soumis aux filtres `inactive`, `bot_mev`, `too_few_trades` : un coffre est inactif par nature), `accumulator_min_positions` (2), `flipper_min_sold_pct` (80) et `flipper_min_share_pct` (50) pour que FLIPPER n'ait aucune valeur en dur, kinds `service`, `stablecoin`, `wrapped_native` dans `KnownAddress`, `Chain.wrapped_fungible_id`, `PipelineSettings.zerion_requests_per_min` (50 au lieu d'un débit par seconde).
4. **Seuils par chaîne** : appliqués au pré-filtre et à `history_days` (mesures faites chaîne par chaîne). Les décisions au niveau wallet/entité (historique, liens, valeur, tags) utilisent la ligne globale.
5. **HOLDER d'entité** : membre HOLDER, ou lien `transfer_after_buy` interne à l'entité sur un token explosif.

## Carte des fichiers

| Fichier | Responsabilité |
|---|---|
| `backend/integrations/errors.py` | + `BudgetExhausted` |
| `backend/integrations/ratelimit.py` | + `DailyBudget`, `NoBudget` |
| `backend/integrations/http.py` | + `post()`, budget par requête |
| `backend/integrations/zerion.py` | `chains()`, `prices()`, `fungible()`, `search()`, `price_chart()` |
| `backend/integrations/rpc.py` | `RpcClient.native_balance()` |
| `backend/integrations/hypersync.py` | + `WalletTransfer`, `Funding`, requêtes par wallet |
| `backend/apps/discovery/models.py` | + champs `Chain`, `PipelineSettings` |
| `backend/apps/discovery/services/chains.py` | Synchronise RPC et actifs natifs Zerion |
| `backend/apps/discovery/services/clients.py` | Zerion avec débit + budget ; `rpc()` |
| `backend/apps/wallets/models.py` | Profils, positions, mouvements, entités, liens, adresses connues, prix, réglages |
| `backend/apps/wallets/migrations/0002_defaults.py` | Seuils globaux + tâche quotidienne |
| `backend/apps/wallets/services/settings.py` | `qualification_thresholds()` |
| `backend/apps/wallets/services/classify.py` | Pur : buy / sell / send / receive |
| `backend/apps/wallets/services/positions.py` | Pur : agrégation par token |
| `backend/apps/wallets/services/filters.py` | Pur : pré-filtre, farmer, MEV, trop peu de trades |
| `backend/apps/wallets/services/exchanges.py` | Pur `forwarding()` + registre + détection |
| `backend/apps/wallets/services/entities.py` | Pur (cibles de transfert) + liens, suivi, regroupement en entités |
| `backend/apps/wallets/services/tags.py` | Pur : tags wallet et entité |
| `backend/apps/wallets/services/pricing.py` | Pur `price_trades()` + actifs de cotation, prix natifs, valeur |
| `backend/apps/wallets/services/qualification.py` | Orchestration par étapes |
| `backend/apps/wallets/tasks.py` | Tâches Celery |
| `backend/apps/wallets/admin.py`, `forms.py`, `templates/…` | Admin + import CSV d'adresses |
| `backend/apps/wallets/tests/fakes.py` | Scénario « wallet d'achat → coffre » |

---

### Task 1: Chaînes enrichies (RPC, actifs natifs) et réglages du pipeline

**Files:**
- Modify: `backend/integrations/zerion.py`
- Modify: `backend/apps/discovery/models.py`, `backend/apps/discovery/services/chains.py`
- Create: `backend/apps/discovery/migrations/0005_chain_rpc_and_qualification_settings.py` (générée)
- Modify: `backend/apps/discovery/tests/fakes.py`, `backend/apps/discovery/tests/test_chains.py`, `backend/integrations/tests/test_chain_directories.py`

**Interfaces:**
- Produces:
  - `integrations.zerion.ZerionChain(zerion_id: str, evm_id: int, rpc_url: str, native_fungible_id: str, wrapped_fungible_id: str)` ; `ZerionClient.chains() -> list[ZerionChain]` ; `ZerionClient.chain_ids() -> dict[int, str]` (dérivé de `chains()`).
  - `Chain.rpc_url`, `Chain.native_fungible_id`, `Chain.wrapped_fungible_id` (chaînes, `""` par défaut).
  - `PipelineSettings.zerion_daily_budget` (250), `zerion_requests_per_min` (50), `stablecoin_symbols` (`["USDC", "USDT", "DAI"]`), `extra_chains` (`[]`, liste de `gt_id`), `qualification_batch_size` (100) ; fonction `default_stablecoins()`.

- [ ] **Step 1: Écrire les tests**

Ajouter à `backend/integrations/tests/test_chain_directories.py` :

```python
@respx.mock
def test_zerion_chains_reads_rpc_and_native_assets():
    respx.get(f"{ZERION_URL}/chains/").respond(
        json={
            "data": [
                {
                    "id": "base",
                    "attributes": {
                        "external_id": "0x2105",
                        "rpc": {"public_servers_url": ["wss://ws.base", "https://mainnet.base.org/"]},
                    },
                    "relationships": {
                        "native_fungible": {"data": {"type": "fungibles", "id": "eth"}},
                        "wrapped_native_fungible": {"data": {"type": "fungibles", "id": "0xweth"}},
                    },
                },
                {"id": "solana", "attributes": {"external_id": None}},
                {"id": "bare", "attributes": {"external_id": "0x1", "rpc": None}, "relationships": {}},
            ]
        }
    )
    client = ZerionClient(http(ZERION_URL))
    [base, bare] = client.chains()
    assert (base.zerion_id, base.evm_id, base.rpc_url) == ("base", 8453, "https://mainnet.base.org/")
    assert (base.native_fungible_id, base.wrapped_fungible_id) == ("eth", "0xweth")
    assert (bare.rpc_url, bare.native_fungible_id) == ("", "")
    assert client.chain_ids() == {8453: "base", 1: "bare"}
```

Dans `backend/apps/discovery/tests/fakes.py`, remplacer `FakeZerion` par :

```python
class FakeZerion:
    def chains(self):
        return [ZerionChain("base", 8453, "https://mainnet.base.org/", "eth", "0xweth")]

    def chain_ids(self):
        return {chain.evm_id: chain.zerion_id for chain in self.chains()}
```

et ajouter `from integrations.zerion import ZerionChain` aux imports.

Ajouter à `backend/apps/discovery/tests/test_chains.py` :

```python
def test_sync_stores_rpc_and_native_assets():
    run_sync()
    base = Chain.objects.get(gt_id="base")
    assert base.rpc_url == "https://mainnet.base.org/"
    assert (base.native_fungible_id, base.wrapped_fungible_id) == ("eth", "0xweth")
    assert Chain.objects.get(gt_id="solana").rpc_url == ""


def test_pipeline_settings_qualification_defaults():
    cfg = PipelineSettings.load()
    assert cfg.zerion_daily_budget == 250
    assert cfg.zerion_requests_per_min == 50
    assert cfg.stablecoin_symbols == ["USDC", "USDT", "DAI"]
    assert cfg.extra_chains == []
    assert cfg.qualification_batch_size == 100
```

et `from apps.discovery.models import Chain, PipelineSettings` en tête du fichier (remplace l'import de `Chain` seul).

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="integrations/tests/test_chain_directories.py apps/discovery/tests/test_chains.py -q"`
Expected: FAIL (`ImportError: cannot import name 'ZerionChain'`).

- [ ] **Step 3: Réécrire le haut de `backend/integrations/zerion.py`**

Remplacer le fichier entier par :

```python
"""Client Zerion : chaînes et prix uniquement (aucune donnée de wallet)."""

from dataclasses import dataclass

BASE_URL = "https://api.zerion.io/v1"
MAX_IMPLEMENTATIONS_PER_CALL = 25


@dataclass(frozen=True)
class ZerionChain:
    zerion_id: str
    evm_id: int
    rpc_url: str
    native_fungible_id: str
    wrapped_fungible_id: str


def _https_rpc(urls) -> str:
    return next((url for url in urls or [] if url.startswith("https://")), "")


def _relation_id(item: dict, name: str) -> str:
    relation = (item.get("relationships") or {}).get(name) or {}
    return (relation.get("data") or {}).get("id") or ""


class ZerionClient:
    def __init__(self, http):
        self._http = http

    def chains(self) -> list[ZerionChain]:
        payload = self._http.get("/chains/")
        chains = []
        for item in payload.get("data", []):
            attributes = item["attributes"]
            if not attributes.get("external_id"):
                continue
            chains.append(
                ZerionChain(
                    zerion_id=item["id"],
                    evm_id=int(attributes["external_id"], 16),
                    rpc_url=_https_rpc((attributes.get("rpc") or {}).get("public_servers_url")),
                    native_fungible_id=_relation_id(item, "native_fungible"),
                    wrapped_fungible_id=_relation_id(item, "wrapped_native_fungible"),
                )
            )
        return chains

    def chain_ids(self) -> dict[int, str]:
        return {chain.evm_id: chain.zerion_id for chain in self.chains()}
```

- [ ] **Step 4: Ajouter les champs aux modèles de la découverte**

Dans `backend/apps/discovery/models.py`, dans `Chain`, après `hypersync_supported` :

```python
    rpc_url = models.URLField(max_length=300, blank=True, default="")
    native_fungible_id = models.CharField(max_length=100, blank=True, default="")
    wrapped_fungible_id = models.CharField(max_length=100, blank=True, default="")
```

Avant `class PipelineSettings`, ajouter :

```python
def default_stablecoins() -> list[str]:
    return ["USDC", "USDT", "DAI"]
```

Dans `PipelineSettings`, après `http_backoff_seconds` :

```python
    zerion_daily_budget = models.PositiveIntegerField(default=250)
    zerion_requests_per_min = models.PositiveIntegerField(default=50)
    stablecoin_symbols = models.JSONField(default=default_stablecoins)
    extra_chains = models.JSONField(
        default=list, blank=True, help_text="gt_id des chaînes analysées en plus pour chaque wallet."
    )
    qualification_batch_size = models.PositiveIntegerField(default=100)
```

- [ ] **Step 5: Synchroniser les nouveaux champs**

Remplacer `backend/apps/discovery/services/chains.py` par :

```python
"""Synchronisation des chaînes : GeckoTerminal → CoinGecko (chain id) → HyperSync, Zerion."""

from apps.discovery.models import Chain


def sync_chains(gt, coingecko, directory, zerion) -> int:
    platform_ids = coingecko.platform_chain_ids()
    hypersync_ids = directory.supported_chain_ids()
    zerion_chains = {chain.evm_id: chain for chain in zerion.chains()}
    count = 0
    for network in gt.networks():
        evm_id = (
            platform_ids.get(network.coingecko_platform_id)
            if network.coingecko_platform_id
            else None
        )
        zc = zerion_chains.get(evm_id) if evm_id else None
        Chain.objects.update_or_create(
            gt_id=network.gt_id,
            defaults={
                "name": network.name,
                "evm_id": evm_id,
                "zerion_id": zc.zerion_id if zc else "",
                "rpc_url": zc.rpc_url if zc else "",
                "native_fungible_id": zc.native_fungible_id if zc else "",
                "wrapped_fungible_id": zc.wrapped_fungible_id if zc else "",
                "hypersync_supported": evm_id in hypersync_ids if evm_id else False,
            },
        )
        count += 1
    return count
```

- [ ] **Step 6: Migration et tests**

```bash
docker compose exec web python manage.py makemigrations discovery -n chain_rpc_and_qualification_settings
make migrate
make test args="-q"
```

Expected: migration `0005` créée ; tous les tests PASS.

- [ ] **Step 7: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend
git commit -m "feat(discovery): RPC publics, actifs natifs et réglages de qualification

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Budget quotidien, POST HTTP, prix Zerion et client RPC

**Files:**
- Modify: `backend/integrations/errors.py`, `backend/integrations/ratelimit.py`, `backend/integrations/http.py`, `backend/integrations/zerion.py`
- Create: `backend/integrations/rpc.py`
- Modify: `backend/integrations/tests/test_ratelimit.py`, `backend/integrations/tests/test_http.py`
- Create: `backend/integrations/tests/test_zerion_prices.py`, `backend/integrations/tests/test_rpc.py`

**Interfaces:**
- Consumes: `JsonHttpClient` (existant), `ZerionClient` (Task 1).
- Produces:
  - `integrations.errors.BudgetExhausted(IntegrationError)`.
  - `DailyBudget(client, name: str, per_day: int, clock=time.time)` : `.consume() -> None` (lève `BudgetExhausted` au-delà), `.remaining() -> int` ; `NoBudget().consume()`.
  - `JsonHttpClient(..., budget=None)` : `.get(path, params=None)`, `.post(path, json: dict)` ; le budget est consommé à chaque tentative.
  - `integrations.zerion.Fungible(fungible_id: str, symbol: str, price: float | None, implementations: dict[str, tuple[str, int]])` (chaîne Zerion → (adresse, décimales)).
  - `ZerionClient.prices(implementations: list[tuple[str, str]]) -> dict[tuple[str, str], Fungible]` (lots de 25), `.fungible(fungible_id) -> Fungible`, `.search(symbol) -> Fungible | None`, `.price_chart(fungible_id) -> list[tuple[int, float]]` (1 an, points quotidiens).
  - `integrations.rpc.RpcClient(http).native_balance(address: str) -> int` (wei).

- [ ] **Step 1: Écrire les tests du budget et du POST**

Ajouter à `backend/integrations/tests/test_ratelimit.py` :

```python
import pytest

from integrations.errors import BudgetExhausted
from integrations.ratelimit import DailyBudget, NoBudget


def make_budget(per_day: int, clock: FakeClock) -> DailyBudget:
    client = redis.Redis.from_url(settings.REDIS_URL)
    return DailyBudget(client, f"test-{uuid.uuid4()}", per_day, clock=clock.time)


def test_budget_allows_up_to_limit_then_raises():
    clock = FakeClock(now=86_400 * 100 + 10)
    budget = make_budget(2, clock)
    budget.consume()
    budget.consume()
    assert budget.remaining() == 0
    with pytest.raises(BudgetExhausted):
        budget.consume()


def test_budget_resets_the_next_day():
    clock = FakeClock(now=86_400 * 200 + 10)
    budget = make_budget(1, clock)
    budget.consume()
    clock.now += 86_400
    budget.consume()
    assert budget.remaining() == 0


def test_no_budget_never_raises():
    for _ in range(5):
        NoBudget().consume()
```

(Déplacer `import pytest` en tête avec les autres imports si ruff le demande.)

Ajouter à `backend/integrations/tests/test_http.py` :

```python
from integrations.errors import BudgetExhausted


class CountingBudget:
    def __init__(self, allowed: int):
        self.allowed = allowed
        self.used = 0

    def consume(self):
        self.used += 1
        if self.used > self.allowed:
            raise BudgetExhausted("fini")


@respx.mock
def test_post_sends_json():
    route = respx.post(f"{BASE}/rpc").respond(json={"result": "0x1"})
    client, _ = make_client()
    assert client.post("/rpc", json={"method": "x"}) == {"result": "0x1"}
    assert route.calls.last.request.content == b'{"method":"x"}'


@respx.mock
def test_budget_is_consumed_per_attempt():
    respx.get(f"{BASE}/items").mock(side_effect=[httpx.Response(500), httpx.Response(200, json={})])
    budget = CountingBudget(allowed=10)
    client = JsonHttpClient(BASE, limiter=NoopLimiter(), budget=budget, sleep=lambda s: None)
    client.get("/items")
    assert budget.used == 2


@respx.mock
def test_exhausted_budget_stops_before_calling():
    route = respx.get(f"{BASE}/items").respond(json={})
    client = JsonHttpClient(BASE, limiter=NoopLimiter(), budget=CountingBudget(allowed=0))
    with pytest.raises(BudgetExhausted):
        client.get("/items")
    assert route.call_count == 0
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="integrations/tests/test_ratelimit.py integrations/tests/test_http.py -q"`
Expected: FAIL (`ImportError: cannot import name 'BudgetExhausted'`).

- [ ] **Step 3: Implémenter l'erreur, le budget et le POST**

Ajouter à `backend/integrations/errors.py` :

```python


class BudgetExhausted(IntegrationError):
    """Le budget quotidien de requêtes du service est épuisé."""
```

Ajouter à `backend/integrations/ratelimit.py` (et `from integrations.errors import BudgetExhausted` en tête) :

```python


class DailyBudget:
    """Nombre maximal de requêtes par jour UTC, partagé entre workers (compteur Redis)."""

    def __init__(self, client: redis.Redis, name: str, per_day: int, clock=time.time):
        self._client = client
        self._name = name
        self._per_day = per_day
        self._clock = clock

    def _key(self) -> str:
        return f"budget:{self._name}:{int(self._clock() // 86_400)}"

    def consume(self) -> None:
        key = self._key()
        used = self._client.incr(key)
        if used == 1:
            self._client.expire(key, 2 * 86_400)
        if used > self._per_day:
            raise BudgetExhausted(f"{self._name} : budget de {self._per_day} requêtes/jour atteint")

    def remaining(self) -> int:
        used = int(self._client.get(self._key()) or 0)
        return max(self._per_day - used, 0)


class NoBudget:
    def consume(self) -> None:
        return None
```

Dans `backend/integrations/http.py` :
- ajouter `from integrations.ratelimit import NoBudget` ;
- ajouter le paramètre `budget=None,` après `limiter,` dans `__init__`, et `self._budget = budget or NoBudget()` ;
- remplacer la méthode `get` par :

```python
    def get(self, path: str, params: dict | None = None) -> dict | list:
        return self._request("GET", path, params=params)

    def post(self, path: str, json: dict) -> dict | list:
        return self._request("POST", path, json=json)

    def _request(self, method: str, path: str, **kwargs) -> dict | list:
        attempt = 0
        while True:
            self._budget.consume()
            self._limiter.acquire()
            status: int | None = None
            error: Exception | None = None
            try:
                response = self._client.request(method, path, **kwargs)
            except httpx.TransportError as exc:
                error = exc
            else:
                status = response.status_code
                if status == 404:
                    raise NotFound(f"{method} {path} : introuvable")
                if status < 400:
                    return response.json()
                if status not in RETRYABLE_STATUSES:
                    raise UpstreamError(f"{method} {path} : HTTP {status}")
            if attempt >= self._max_retries:
                if status == 429:
                    raise RateLimited(f"{method} {path} : HTTP 429")
                raise UpstreamError(f"{method} {path} : {status or error}")
            self._sleep(self._backoff * 2**attempt)
            attempt += 1
```

- [ ] **Step 4: Lancer les tests**

Run: `make test args="integrations -q"`
Expected: PASS.

- [ ] **Step 5: Écrire les tests des prix Zerion et du RPC**

`backend/integrations/tests/test_zerion_prices.py` :

```python
import respx

from integrations.http import JsonHttpClient
from integrations.ratelimit import NoopLimiter
from integrations.zerion import BASE_URL, ZerionClient

USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
XL = "0x1cdb289befdfac8af945a288bcdccc382cb34d32"


def client() -> ZerionClient:
    return ZerionClient(JsonHttpClient(BASE_URL, limiter=NoopLimiter(), max_retries=0))


def fungible(fid, symbol, price, impls):
    return {
        "id": fid,
        "attributes": {
            "symbol": symbol,
            "market_data": {"price": price},
            "implementations": [
                {"chain_id": c, "address": a, "decimals": d} for c, a, d in impls
            ],
        },
    }


@respx.mock
def test_prices_batches_and_maps_back_to_requested_pairs():
    route = respx.get(f"{BASE_URL}/fungibles/").respond(
        json={
            "data": [
                fungible("usdc", "USDC", 0.9998, [("ethereum", "0xa0b8", 6), ("base", USDC_BASE.upper().replace("0X", "0x"), 6)]),
                fungible("xl-id", "XL", 0.00012, [("robinhood", XL, 18)]),
                fungible("noprice", "SPAM", None, [("base", "0xspam", 18)]),
            ]
        }
    )
    prices = client().prices([("base", USDC_BASE), ("robinhood", XL), ("base", "0xspam")])
    assert prices[("base", USDC_BASE)].price == 0.9998
    assert prices[("base", USDC_BASE)].implementations["base"] == (USDC_BASE, 6)
    assert prices[("robinhood", XL)].symbol == "XL"
    assert prices[("base", "0xspam")].price is None
    params = route.calls.last.request.url.params
    assert params["filter[fungible_implementations]"] == f"base:{USDC_BASE},base:0xspam,robinhood:{XL}"


@respx.mock
def test_prices_splits_in_batches_of_25():
    route = respx.get(f"{BASE_URL}/fungibles/").respond(json={"data": []})
    client().prices([("base", f"0x{i:040x}") for i in range(30)])
    assert route.call_count == 2


def test_prices_without_input_makes_no_call():
    assert client().prices([]) == {}


@respx.mock
def test_search_returns_top_market_cap():
    route = respx.get(f"{BASE_URL}/fungibles/").respond(
        json={"data": [fungible("usdc", "USDC", 1.0, [("base", USDC_BASE, 6)])]}
    )
    found = client().search("USDC")
    assert found.implementations == {"base": (USDC_BASE, 6)}
    params = route.calls.last.request.url.params
    assert params["filter[search_query]"] == "USDC"
    assert params["sort"] == "-market_data.market_cap"


@respx.mock
def test_search_without_result():
    respx.get(f"{BASE_URL}/fungibles/").respond(json={"data": []})
    assert client().search("NOPE") is None


@respx.mock
def test_fungible_by_id():
    respx.get(f"{BASE_URL}/fungibles/0xweth").respond(
        json={"data": fungible("0xweth", "WETH", 2000.0, [("base", "0x4200000000000000000000000000000000000006", 18)])}
    )
    assert client().fungible("0xweth").implementations["base"][1] == 18


@respx.mock
def test_price_chart_reads_year_points():
    respx.get(f"{BASE_URL}/fungibles/eth/charts/year").respond(
        json={"data": {"attributes": {"points": [[1759017600, 4018.26], [1759104000, 4140.23]]}}}
    )
    assert client().price_chart("eth") == [(1759017600, 4018.26), (1759104000, 4140.23)]
```

`backend/integrations/tests/test_rpc.py` :

```python
import pytest
import respx

from integrations.errors import UpstreamError
from integrations.http import JsonHttpClient
from integrations.ratelimit import NoopLimiter
from integrations.rpc import RpcClient

RPC = "https://rpc.test/"


def client() -> RpcClient:
    return RpcClient(JsonHttpClient(RPC, limiter=NoopLimiter(), max_retries=0))


@respx.mock
def test_native_balance():
    route = respx.post(RPC).respond(json={"jsonrpc": "2.0", "id": 1, "result": "0xde0b6b3a7640000"})
    assert client().native_balance("0xabc") == 10**18
    assert b'"eth_getBalance"' in route.calls.last.request.content


@respx.mock
def test_rpc_error_raises():
    respx.post(RPC).respond(json={"jsonrpc": "2.0", "id": 1, "error": {"message": "nope"}})
    with pytest.raises(UpstreamError):
        client().native_balance("0xabc")
```

- [ ] **Step 6: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="integrations/tests/test_zerion_prices.py integrations/tests/test_rpc.py -q"`
Expected: FAIL (`ImportError`).

- [ ] **Step 7: Implémenter les prix Zerion et le RPC**

Ajouter à `backend/integrations/zerion.py`, après `_relation_id` :

```python


@dataclass(frozen=True)
class Fungible:
    fungible_id: str
    symbol: str
    price: float | None
    implementations: dict[str, tuple[str, int]]


def _fungible(item: dict) -> Fungible:
    attributes = item["attributes"]
    implementations = {}
    for impl in attributes.get("implementations") or []:
        if impl.get("address"):
            implementations[impl["chain_id"]] = (impl["address"].lower(), int(impl.get("decimals") or 0))
    price = (attributes.get("market_data") or {}).get("price")
    return Fungible(
        item["id"],
        attributes.get("symbol") or "",
        float(price) if price is not None else None,
        implementations,
    )
```

et ajouter à `ZerionClient` :

```python
    def prices(self, implementations: list[tuple[str, str]]) -> dict[tuple[str, str], Fungible]:
        wanted = sorted({(chain, address.lower()) for chain, address in implementations})
        found: dict[tuple[str, str], Fungible] = {}
        for start in range(0, len(wanted), MAX_IMPLEMENTATIONS_PER_CALL):
            batch = wanted[start : start + MAX_IMPLEMENTATIONS_PER_CALL]
            requested = set(batch)
            payload = self._http.get(
                "/fungibles/",
                params={
                    "filter[fungible_implementations]": ",".join(f"{c}:{a}" for c, a in batch),
                    "currency": "usd",
                    "page[size]": 100,
                },
            )
            for item in payload.get("data", []):
                fungible = _fungible(item)
                for chain, (address, _) in fungible.implementations.items():
                    if (chain, address) in requested:
                        found[(chain, address)] = fungible
        return found

    def fungible(self, fungible_id: str) -> Fungible:
        payload = self._http.get(f"/fungibles/{fungible_id}", params={"currency": "usd"})
        return _fungible(payload["data"])

    def search(self, symbol: str) -> Fungible | None:
        payload = self._http.get(
            "/fungibles/",
            params={
                "filter[search_query]": symbol,
                "sort": "-market_data.market_cap",
                "currency": "usd",
                "page[size]": 1,
            },
        )
        data = payload.get("data", [])
        return _fungible(data[0]) if data else None

    def price_chart(self, fungible_id: str) -> list[tuple[int, float]]:
        payload = self._http.get(f"/fungibles/{fungible_id}/charts/year", params={"currency": "usd"})
        return [(int(ts), float(price)) for ts, price in payload["data"]["attributes"]["points"]]
```

`backend/integrations/rpc.py` :

```python
"""Client JSON-RPC minimal : solde natif d'une adresse sur le RPC public d'une chaîne."""

from integrations.errors import UpstreamError


class RpcClient:
    def __init__(self, http):
        self._http = http

    def native_balance(self, address: str) -> int:
        payload = self._http.post(
            "",
            json={"jsonrpc": "2.0", "id": 1, "method": "eth_getBalance", "params": [address, "latest"]},
        )
        if "result" not in payload:
            raise UpstreamError(f"eth_getBalance : {payload.get('error')}")
        return int(payload["result"], 16)
```

- [ ] **Step 8: Lancer les tests**

Run: `make test args="integrations -q"`
Expected: PASS.

- [ ] **Step 9: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend/integrations
git commit -m "feat(integrations): budget quotidien, prix Zerion par lots et solde natif RPC

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Requêtes HyperSync par wallet

**Files:**
- Modify: `backend/integrations/hypersync.py`
- Create: `backend/integrations/tests/test_hypersync_wallet.py`

**Interfaces:**
- Consumes: `HyperSyncClient._get`, `_int`, `_topic_address`, `TRANSFER_TOPIC` (existants).
- Produces:
  - `WalletTransfer(block, timestamp, tx_hash, log_index, token, sender, recipient, amount, tx_from, tx_to, tx_value)` (dataclass gelée, adresses en minuscules, entiers pour `amount` et `tx_value`).
  - `Funding(funder: str, block: int, value: int)`.
  - `address_topic(address: str) -> str`.
  - `HyperSyncClient.wallet_tx_count(address, from_block, to_block, cap) -> int` (arrêt dès `cap`), `.distinct_counterparties(address, from_block, to_block, cap) -> int` (arrêt dès `cap`), `.first_funding(address, to_block) -> Funding | None`, `.wallet_transfers(address, from_block, to_block) -> list[WalletTransfer]`.

- [ ] **Step 1: Écrire les tests**

`backend/integrations/tests/test_hypersync_wallet.py` :

```python
from types import SimpleNamespace

from integrations.hypersync import (
    TRANSFER_TOPIC,
    Funding,
    HyperSyncClient,
    WalletTransfer,
    address_topic,
)
from integrations.ratelimit import NoopLimiter

W = "0x" + "a" * 40
POOL = "0x" + "b" * 40
ROUTER = "0x" + "9" * 40
TOKEN = "0x" + "1" * 40


def page(next_block, logs=(), txs=(), blocks=()):
    return SimpleNamespace(
        next_block=next_block,
        data=SimpleNamespace(logs=list(logs), transactions=list(txs), blocks=list(blocks)),
    )


class FakeInner:
    def __init__(self, pages):
        self.pages = list(pages)
        self.queries = []

    async def get(self, query):
        self.queries.append(query)
        return self.pages.pop(0)

    async def get_height(self):
        return 1000


def tx(hash_="0xt", from_=W, to=ROUTER, value="0x0", block=10):
    return SimpleNamespace(hash=hash_, from_=from_, to=to, value=value, block_number=block)


def make(pages) -> tuple[HyperSyncClient, FakeInner]:
    inner = FakeInner(pages)
    return HyperSyncClient(1, "t", NoopLimiter(), inner=inner), inner


def test_address_topic():
    assert address_topic("0xABC" + "0" * 37) == "0x" + "0" * 24 + "abc" + "0" * 37


def test_tx_count_stops_at_cap():
    client, inner = make([page(100, txs=[tx()] * 3), page(200, txs=[tx()] * 3), page(300)])
    assert client.wallet_tx_count(W, 0, 300, cap=5) == 6
    assert len(inner.queries) == 2


def test_tx_count_reads_all_pages_under_cap():
    client, _ = make([page(100, txs=[tx()] * 2), page(300, txs=[tx()])])
    assert client.wallet_tx_count(W, 0, 300, cap=50) == 3


def test_distinct_counterparties_ignores_self_and_stops_at_cap():
    client, _ = make(
        [
            page(100, txs=[tx(to="0x1"), tx(to="0x1"), tx(from_="0x2", to=W)]),
            page(200, txs=[tx(to="0x3")]),
            page(300, txs=[tx(to="0x4")]),
        ]
    )
    assert client.distinct_counterparties(W, 0, 300, cap=3) == 3


def test_first_funding_is_first_transaction_with_value():
    client, _ = make(
        [
            page(
                500,
                txs=[
                    tx(from_="0xfunder2", to=W, value="0x5", block=40),
                    tx(from_="0xspam", to=W, value="0x0", block=20),
                    tx(from_="0xFUNDER", to=W, value="0xa", block=30),
                ],
            )
        ]
    )
    assert client.first_funding(W, 500) == Funding("0xfunder", 30, 10)


def test_first_funding_none():
    client, _ = make([page(500)])
    assert client.first_funding(W, 500) is None


def test_wallet_transfers_joins_transaction_and_block():
    log = SimpleNamespace(
        block_number=10,
        log_index=3,
        transaction_hash="0xt",
        address=TOKEN.upper().replace("0X", "0x"),
        data=hex(500),
        topics=[TRANSFER_TOPIC, address_topic(POOL), address_topic(W), None],
    )
    nft = SimpleNamespace(
        block_number=10,
        log_index=4,
        transaction_hash="0xt",
        address=TOKEN,
        data="0x",
        topics=[TRANSFER_TOPIC, address_topic(POOL), address_topic(W), address_topic(W)],
    )
    client, inner = make(
        [
            page(
                1000,
                logs=[log, nft],
                txs=[tx(value="0x10")],
                blocks=[SimpleNamespace(number=10, timestamp="0x64")],
            )
        ]
    )
    assert client.wallet_transfers(W, 0, 1000) == [
        WalletTransfer(
            block=10,
            timestamp=100,
            tx_hash="0xt",
            log_index=3,
            token=TOKEN,
            sender=POOL,
            recipient=W,
            amount=500,
            tx_from=W,
            tx_to=ROUTER,
            tx_value=16,
        )
    ]
    [selection_from, selection_to] = inner.queries[0].logs
    assert selection_from.topics == [[TRANSFER_TOPIC], [address_topic(W)]]
    assert selection_to.topics == [[TRANSFER_TOPIC], [], [address_topic(W)]]
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="integrations/tests/test_hypersync_wallet.py -q"`
Expected: FAIL (`ImportError: cannot import name 'Funding'`).

- [ ] **Step 3: Implémenter**

Dans `backend/integrations/hypersync.py` :
- ajouter `TransactionSelection` à l'import depuis `hypersync` ;
- après la dataclass `Transfer`, ajouter :

```python


@dataclass(frozen=True)
class WalletTransfer:
    block: int
    timestamp: int
    tx_hash: str
    log_index: int
    token: str
    sender: str
    recipient: str
    amount: int
    tx_from: str
    tx_to: str
    tx_value: int


@dataclass(frozen=True)
class Funding:
    funder: str
    block: int
    value: int


def address_topic(address: str) -> str:
    return "0x" + "0" * 24 + address[2:].lower()
```

- ajouter à `HyperSyncClient` (avant `_get`) :

```python
    def wallet_tx_count(self, address: str, from_block: int, to_block: int, cap: int) -> int:
        query = Query(
            from_block=from_block,
            to_block=to_block,
            transactions=[TransactionSelection(from_=[address])],
            field_selection=FieldSelection(transaction=[TransactionField.HASH]),
        )
        count = 0
        for data in self._pages(query, to_block):
            count += len(data.transactions)
            if count >= cap:
                break
        return count

    def distinct_counterparties(self, address: str, from_block: int, to_block: int, cap: int) -> int:
        address = address.lower()
        query = Query(
            from_block=from_block,
            to_block=to_block,
            transactions=[TransactionSelection(from_=[address]), TransactionSelection(to=[address])],
            field_selection=FieldSelection(transaction=[TransactionField.FROM, TransactionField.TO]),
        )
        seen: set[str] = set()
        for data in self._pages(query, to_block):
            for tx in data.transactions:
                for other in (tx.from_, tx.to):
                    if other and other.lower() != address:
                        seen.add(other.lower())
            if len(seen) >= cap:
                break
        return len(seen)

    def first_funding(self, address: str, to_block: int) -> Funding | None:
        query = Query(
            from_block=0,
            to_block=to_block,
            transactions=[TransactionSelection(to=[address])],
            field_selection=FieldSelection(
                transaction=[TransactionField.FROM, TransactionField.VALUE, TransactionField.BLOCK_NUMBER]
            ),
        )
        for data in self._pages(query, to_block):
            for tx in sorted(data.transactions, key=lambda t: t.block_number):
                if tx.from_ and _int(tx.value) > 0:
                    return Funding(tx.from_.lower(), tx.block_number, _int(tx.value))
        return None

    def wallet_transfers(self, address: str, from_block: int, to_block: int) -> list[WalletTransfer]:
        topic = address_topic(address)
        query = Query(
            from_block=from_block,
            to_block=to_block,
            logs=[
                LogSelection(topics=[[TRANSFER_TOPIC], [topic]]),
                LogSelection(topics=[[TRANSFER_TOPIC], [], [topic]]),
            ],
            field_selection=FieldSelection(
                block=[BlockField.NUMBER, BlockField.TIMESTAMP],
                transaction=[
                    TransactionField.HASH,
                    TransactionField.FROM,
                    TransactionField.TO,
                    TransactionField.VALUE,
                ],
                log=[
                    LogField.BLOCK_NUMBER,
                    LogField.LOG_INDEX,
                    LogField.TRANSACTION_HASH,
                    LogField.ADDRESS,
                    LogField.DATA,
                    LogField.TOPIC0,
                    LogField.TOPIC1,
                    LogField.TOPIC2,
                ],
            ),
        )
        transfers: list[WalletTransfer] = []
        for data in self._pages(query, to_block):
            timestamps = {block.number: _int(block.timestamp) for block in data.blocks}
            transactions = {tx.hash: tx for tx in data.transactions if tx.hash}
            for log in data.logs:
                # HyperSync complète toujours à 4 topics avec None ; ERC-721 = 4 topics réels.
                topics = [topic for topic in (log.topics or []) if topic]
                tx = transactions.get(log.transaction_hash)
                if len(topics) != 3 or not log.data or log.data == "0x" or tx is None or not tx.from_:
                    continue
                transfers.append(
                    WalletTransfer(
                        block=log.block_number,
                        timestamp=timestamps.get(log.block_number, 0),
                        tx_hash=log.transaction_hash,
                        log_index=log.log_index,
                        token=log.address.lower(),
                        sender=_topic_address(topics[1]),
                        recipient=_topic_address(topics[2]),
                        amount=_int(log.data),
                        tx_from=tx.from_.lower(),
                        tx_to=(tx.to or "").lower(),
                        tx_value=_int(tx.value),
                    )
                )
        return transfers

    def _pages(self, query: Query, to_block: int):
        while True:
            response = self._get(query)
            yield response.data
            if response.next_block >= to_block:
                return
            query.from_block = response.next_block
```

- [ ] **Step 4: Lancer les tests**

Run: `make test args="integrations -q"`
Expected: PASS.

- [ ] **Step 5: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend/integrations
git commit -m "feat(integrations): requêtes HyperSync par wallet (transferts, transactions, financement)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: App `wallets`, modèles, réglages et planning

**Files:**
- Create: `backend/apps/wallets/` (via `make startapp name=wallets`), `services/` en package
- Modify: `backend/apps/wallets/models.py`
- Create: `backend/apps/wallets/migrations/0001_initial.py` (générée), `backend/apps/wallets/migrations/0002_defaults.py`
- Create: `backend/apps/wallets/services/settings.py`
- Modify: `backend/config/settings/base.py`, `backend/config/urls.py`
- Create: `backend/apps/wallets/tests/test_models.py`

**Interfaces:**
- Consumes: `discovery.Chain`, `Token`, `Wallet`, `UINT256_DIGITS`.
- Produces (dans `apps.wallets.models`) : `QUALIFICATION_FIELDS`, `BLOCKING_KINDS`, `Entity`, `WalletProfile` (+ `Status`, `Source`), `TokenPosition` (+ propriété `balance`), `TokenTrade` (+ `Kind`), `WalletLink` (+ `Kind`), `KnownAddress` (+ `Kind`, `Source`, `KnownAddress.objects.blocking_for(address, chain_ids)`), `DailyPrice`, `QualificationSettings` (+ `clean()`).
- Produces (dans `apps.wallets.services.settings`) : `QualificationThresholds` (dataclass des 23 seuils) et `qualification_thresholds(chain: Chain | None = None) -> QualificationThresholds`.
- Tâche périodique `qualification-daily` → `apps.wallets.tasks.qualify_wallets_task`, 08:00 UTC.

- [ ] **Step 1: Créer l'app**

```bash
make startapp name=wallets
rm backend/apps/wallets/services.py
mkdir -p backend/apps/wallets/services backend/apps/wallets/tests
printf '"""Logique métier de la qualification des wallets."""\n' > backend/apps/wallets/services/__init__.py
test -f backend/apps/wallets/tests/__init__.py || touch backend/apps/wallets/tests/__init__.py
```

Ajouter `"django.contrib.postgres",` à `INSTALLED_APPS` (après `"django.contrib.staticfiles",`) et `"apps.wallets",` après `"apps.discovery",` dans `backend/config/settings/base.py`. Ajouter `path("api/wallets/", include("apps.wallets.urls")),` dans `backend/config/urls.py`.

- [ ] **Step 2: Écrire les tests des modèles et des réglages**

`backend/apps/wallets/tests/test_models.py` :

```python
import pytest
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django_celery_beat.models import PeriodicTask

from apps.discovery.models import Wallet
from apps.discovery.tests.factories import make_chain, make_token
from apps.wallets.models import (
    KnownAddress,
    QualificationSettings,
    TokenPosition,
    TokenTrade,
    WalletProfile,
)
from apps.wallets.services.settings import qualification_thresholds

pytestmark = pytest.mark.django_db


def test_global_defaults_from_migration():
    t = qualification_thresholds()
    assert t.max_txs_per_day == 200
    assert t.history_days == 365
    assert t.min_portfolio_usd == 10_000.0
    assert t.transfer_after_buy_pct == 70.0
    assert t.accumulator_min_positions == 2


def test_chain_override_and_inheritance():
    chain = make_chain()
    QualificationSettings.objects.create(chain=chain, history_days=90)
    t = qualification_thresholds(chain)
    assert t.history_days == 90
    assert t.max_txs_per_day == 200


def test_global_settings_require_every_threshold():
    with pytest.raises(ValidationError):
        QualificationSettings(chain=None, history_days=10).clean()


def test_daily_task_scheduled_at_8():
    task = PeriodicTask.objects.get(name="qualification-daily")
    assert task.task == "apps.wallets.tasks.qualify_wallets_task"
    assert (task.crontab.minute, task.crontab.hour) == ("0", "8")


def test_trade_is_unique_per_log_and_wallet():
    token = make_token()
    wallet = Wallet.objects.create(address="0x" + "a" * 40)
    values = dict(wallet=wallet, token=token, kind="buy", amount=1, block=1, at="2026-09-27T00:00Z", tx_hash="0xt", log_index=0)
    TokenTrade.objects.create(**values)
    with pytest.raises(IntegrityError), transaction.atomic():
        TokenTrade.objects.create(**values)


def test_position_balance():
    position = TokenPosition(bought_amount=10, received_amount=5, sold_amount=3, sent_amount=2)
    assert position.balance == 10


def test_blocking_known_addresses():
    chain = make_chain()
    KnownAddress.objects.create(chain=None, address="0x1", kind="exchange")
    KnownAddress.objects.create(chain=chain, address="0x2", kind="cex_deposit")
    KnownAddress.objects.create(chain=chain, address="0x3", kind="stablecoin")
    assert KnownAddress.objects.blocking_for("0x1", [chain.pk]).exists()
    assert KnownAddress.objects.blocking_for("0x2", [chain.pk]).exists()
    assert not KnownAddress.objects.blocking_for("0x3", [chain.pk]).exists()


def test_profile_defaults():
    profile = WalletProfile.objects.create(wallet=Wallet.objects.create(address="0x" + "b" * 40))
    assert (profile.status, profile.source, profile.depth, profile.tags) == ("pending", "early_buyer", 0, [])
```

- [ ] **Step 3: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/wallets -q"`
Expected: FAIL (`ImportError`).

- [ ] **Step 4: Écrire les modèles**

`backend/apps/wallets/models.py` :

```python
"""Qualification : profils, historique par token, entités, liens, adresses connues, prix, réglages."""

from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex
from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import Q

from apps.discovery.models import UINT256_DIGITS, Chain, Token, Wallet

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
    "follow_depth",
    "funder_max_wallets",
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


class Entity(models.Model):
    portfolio_value_usd = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    tags = ArrayField(models.CharField(max_length=32), default=list, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name_plural = "entities"
        indexes = [GinIndex(fields=["tags"], name="wallets_entity_tags")]

    def __str__(self):
        return f"Entité #{self.pk}"


class WalletProfile(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "En attente"
        PREFILTERED = "prefiltered", "Pré-filtré"
        HISTORY_FETCHED = "history_fetched", "Historique récupéré"
        QUALIFIED = "qualified", "Qualifié"
        FILTERED = "filtered", "Écarté"

    class Source(models.TextChoices):
        EARLY_BUYER = "early_buyer", "Early buyer"
        LINKED = "linked", "Wallet lié"

    wallet = models.OneToOneField(Wallet, on_delete=models.CASCADE, related_name="profile")
    source = models.CharField(max_length=16, choices=Source.choices, default=Source.EARLY_BUYER)
    depth = models.PositiveSmallIntegerField(default=0)
    chains = ArrayField(models.PositiveBigIntegerField(), default=list, blank=True)
    status = models.CharField(max_length=24, choices=Status.choices, default=Status.PENDING)
    filter_reason = models.CharField(max_length=64, blank=True, default="")
    entity = models.ForeignKey(
        Entity, null=True, blank=True, on_delete=models.SET_NULL, related_name="profiles"
    )
    portfolio_value_usd = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    metrics = models.JSONField(default=dict, blank=True)
    tags = ArrayField(models.CharField(max_length=32), default=list, blank=True)
    attempts = models.PositiveSmallIntegerField(default=0)
    analyzed_at = models.DateTimeField(null=True, blank=True)
    next_analysis_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["status", "next_analysis_at"], name="wallets_profile_status"),
            GinIndex(fields=["tags"], name="wallets_profile_tags"),
        ]

    def __str__(self):
        return str(self.wallet)


class TokenPosition(models.Model):
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="positions")
    token = models.ForeignKey(Token, on_delete=models.CASCADE, related_name="positions")
    bought_amount = _amount(default=0)
    sold_amount = _amount(default=0)
    sent_amount = _amount(default=0)
    received_amount = _amount(default=0)
    bought_usd = models.DecimalField(max_digits=20, decimal_places=2, default=0)
    sold_usd = models.DecimalField(max_digits=20, decimal_places=2, default=0)
    buys = models.PositiveIntegerField(default=0)
    sells = models.PositiveIntegerField(default=0)
    first_at = models.DateTimeField()
    last_at = models.DateTimeField()

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["wallet", "token"], name="wallets_unique_position")
        ]

    def __str__(self):
        return f"{self.wallet} · {self.token}"

    @property
    def balance(self):
        return self.bought_amount + self.received_amount - self.sold_amount - self.sent_amount


class TokenTrade(models.Model):
    class Kind(models.TextChoices):
        BUY = "buy", "Achat"
        SELL = "sell", "Vente"
        SEND = "send", "Envoi"
        RECEIVE = "receive", "Réception"

    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="trades")
    token = models.ForeignKey(Token, on_delete=models.CASCADE, related_name="trades")
    kind = models.CharField(max_length=8, choices=Kind.choices)
    amount = _amount()
    usd = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    counterparty = models.CharField(max_length=42, blank=True, default="")
    block = models.PositiveBigIntegerField()
    at = models.DateTimeField()
    tx_hash = models.CharField(max_length=66)
    log_index = models.PositiveIntegerField()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["tx_hash", "log_index", "wallet"], name="wallets_unique_trade"
            )
        ]
        indexes = [models.Index(fields=["wallet", "token", "at"], name="wallets_trade_wallet_token")]

    def __str__(self):
        return f"{self.get_kind_display()} {self.token} ({self.tx_hash[:10]})"


class WalletLink(models.Model):
    class Kind(models.TextChoices):
        TRANSFER_AFTER_BUY = "transfer_after_buy", "Transfert après achat"
        FUNDING = "funding", "Financement initial"

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


class DailyPrice(models.Model):
    fungible_id = models.CharField(max_length=100)
    day = models.DateField()
    usd = models.FloatField()

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["fungible_id", "day"], name="wallets_unique_price")
        ]

    def __str__(self):
        return f"{self.fungible_id} {self.day} {self.usd}"


def _int_setting(**kwargs):
    return models.PositiveIntegerField(null=True, blank=True, **kwargs)


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
    follow_depth = _int_setting()
    funder_max_wallets = _int_setting()
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

- [ ] **Step 5: Service de réglages**

`backend/apps/wallets/services/settings.py` :

```python
"""Résolution des seuils de qualification : valeur de la chaîne si renseignée, sinon globale."""

from dataclasses import dataclass, fields
from decimal import Decimal

from django.db.models import Q

from apps.discovery.models import Chain
from apps.wallets.models import QualificationSettings


@dataclass(frozen=True)
class QualificationThresholds:
    max_txs_per_day: int
    max_distinct_tokens: int
    inactive_days: int
    min_txs_active: int
    history_days: int
    max_mev_ratio: float
    min_distinct_buys: int
    min_portfolio_usd: float
    max_portfolio_usd: float
    transfer_after_buy_pct: float
    follow_depth: int
    funder_max_wallets: int
    hot_wallet_min_counterparties: int
    deposit_forward_pct: float
    deposit_forward_hours: int
    flipper_hours: int
    flipper_min_sold_pct: float
    flipper_min_share_pct: float
    holder_min_pct: float
    accumulator_max_out_pct: float
    accumulator_min_positions: int
    early_buyer_min_explosions: int
    refilter_after_days: int


def qualification_thresholds(chain: Chain | None = None) -> QualificationThresholds:
    condition = Q(chain__isnull=True) | Q(chain=chain) if chain else Q(chain__isnull=True)
    rows = {row.chain_id: row for row in QualificationSettings.objects.filter(condition)}
    base = rows[None]
    override = rows.get(chain.pk) if chain else None
    values = {}
    for field in fields(QualificationThresholds):
        value = getattr(override, field.name) if override else None
        if value is None:
            value = getattr(base, field.name)
        values[field.name] = float(value) if isinstance(value, Decimal) else value
    return QualificationThresholds(**values)
```

- [ ] **Step 6: Migrations (schéma + valeurs par défaut)**

```bash
docker compose exec web python manage.py makemigrations wallets
```

`backend/apps/wallets/migrations/0002_defaults.py` :

```python
"""Seuils globaux par défaut et tâche quotidienne de qualification. Ensuite, tout se gère dans l'admin."""

from django.db import migrations

DEFAULTS = {
    "max_txs_per_day": 200,
    "max_distinct_tokens": 300,
    "inactive_days": 90,
    "min_txs_active": 5,
    "history_days": 365,
    "max_mev_ratio": 30,
    "min_distinct_buys": 3,
    "min_portfolio_usd": 10_000,
    "max_portfolio_usd": 50_000_000,
    "transfer_after_buy_pct": 70,
    "follow_depth": 1,
    "funder_max_wallets": 50,
    "hot_wallet_min_counterparties": 1000,
    "deposit_forward_pct": 90,
    "deposit_forward_hours": 24,
    "flipper_hours": 24,
    "flipper_min_sold_pct": 80,
    "flipper_min_share_pct": 50,
    "holder_min_pct": 50,
    "accumulator_max_out_pct": 20,
    "accumulator_min_positions": 2,
    "early_buyer_min_explosions": 2,
    "refilter_after_days": 30,
}


def create_defaults(apps, schema_editor):
    Settings = apps.get_model("wallets", "QualificationSettings")
    CrontabSchedule = apps.get_model("django_celery_beat", "CrontabSchedule")
    PeriodicTask = apps.get_model("django_celery_beat", "PeriodicTask")
    Settings.objects.get_or_create(chain=None, defaults=DEFAULTS)
    schedule, _ = CrontabSchedule.objects.get_or_create(
        minute="0", hour="8", day_of_week="*", day_of_month="*", month_of_year="*", timezone="UTC"
    )
    PeriodicTask.objects.get_or_create(
        name="qualification-daily",
        defaults={"task": "apps.wallets.tasks.qualify_wallets_task", "crontab": schedule},
    )


def remove_defaults(apps, schema_editor):
    apps.get_model("django_celery_beat", "PeriodicTask").objects.filter(
        name="qualification-daily"
    ).delete()
    apps.get_model("wallets", "QualificationSettings").objects.filter(chain=None).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("wallets", "0001_initial"),
        ("django_celery_beat", "0019_alter_periodictasks_options"),
    ]

    operations = [migrations.RunPython(create_defaults, remove_defaults)]
```

Créer aussi un `backend/apps/wallets/tasks.py` minimal pour que la tâche existe dès maintenant (complété à la Task 11) :

```python
"""Tâches Celery de la qualification des wallets."""

from celery import shared_task


@shared_task
def qualify_wallets_task() -> dict:
    return {}
```

- [ ] **Step 7: Migrer et tester**

```bash
make migrate
make test args="apps/wallets -q"
```

Expected: PASS.

- [ ] **Step 8: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend
git commit -m "feat(wallets): app, modèles de qualification, réglages et planning quotidien

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Classement, positions et filtres (fonctions pures)

**Files:**
- Create: `backend/apps/wallets/services/classify.py`, `positions.py`, `filters.py`
- Create: `backend/apps/wallets/tests/factories.py`
- Create: `backend/apps/wallets/tests/test_classify.py`, `test_positions.py`, `test_filters.py`

**Interfaces:**
- Consumes: `WalletTransfer` (Task 3), `QualificationThresholds` (Task 4).
- Produces:
  - `classify` : constantes `BUY`, `SELL`, `SEND`, `RECEIVE` ; `Trade(transfer: WalletTransfer, kind: str, counterparty: str)` ; `classify(transfer, wallet) -> Trade | None` ; `classify_all(transfers, wallet) -> list[Trade]`.
  - `positions` : `TradeRecord(chain_id: int, token: str, kind: str, amount: int, usd: float | None, ts: int, block: int, counterparty: str)` ; `PositionStats` ; `aggregate_positions(records) -> dict[tuple[int, str], PositionStats]`.
  - `filters` : `EARLY_BUYER = "early_buyer"` ; `ChainActivity(txs_7d: int, txs_active: int)` ; `prefilter_reason(checks: list[tuple[ChainActivity, QualificationThresholds]], source) -> str | None` ; `farmer_reason(distinct_received, t) -> str | None` ; `mev_ratio(records, quote_tokens) -> float` ; `distinct_buys(records, quote_tokens) -> int` ; `history_reason(records, quote_tokens, t, source) -> str | None`.
  - `tests.factories.make_thresholds(**overrides) -> QualificationThresholds` (valeurs par défaut de la migration).

- [ ] **Step 1: Fabrique de seuils pour les tests**

`backend/apps/wallets/tests/factories.py` :

```python
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
```

- [ ] **Step 2: Écrire les tests**

`backend/apps/wallets/tests/test_classify.py` :

```python
import pytest

from apps.wallets.services.classify import BUY, RECEIVE, SELL, SEND, classify, classify_all
from integrations.hypersync import WalletTransfer

W = "0x" + "a" * 40
POOL = "0x" + "b" * 40
ROUTER = "0x" + "9" * 40
TOKEN = "0x" + "1" * 40
OTHER = "0x" + "c" * 40


def tr(sender, recipient, tx_from, tx_to):
    return WalletTransfer(
        block=1, timestamp=1, tx_hash="0xt", log_index=0, token=TOKEN, sender=sender,
        recipient=recipient, amount=5, tx_from=tx_from, tx_to=tx_to, tx_value=0,
    )


@pytest.mark.parametrize(
    ("transfer", "kind", "counterparty"),
    [
        (tr(POOL, W, W, ROUTER), BUY, POOL),
        (tr(W, POOL, W, ROUTER), SELL, POOL),
        (tr(W, OTHER, W, TOKEN), SEND, OTHER),
        (tr(OTHER, W, OTHER, TOKEN), RECEIVE, OTHER),
        (tr(POOL, W, OTHER, ROUTER), RECEIVE, POOL),
        (tr(W, OTHER, OTHER, ROUTER), SEND, OTHER),
    ],
)
def test_classify(transfer, kind, counterparty):
    trade = classify(transfer, W.upper().replace("0X", "0x"))
    assert (trade.kind, trade.counterparty) == (kind, counterparty)


def test_self_and_unrelated_transfers_are_ignored():
    assert classify(tr(W, W, W, TOKEN), W) is None
    assert classify(tr(POOL, OTHER, W, ROUTER), W) is None
    assert len(classify_all([tr(W, W, W, TOKEN), tr(POOL, W, W, ROUTER)], W)) == 1
```

`backend/apps/wallets/tests/test_positions.py` :

```python
from apps.wallets.services.positions import TradeRecord, aggregate_positions


def rec(kind, amount, ts, usd=None, token="0xt", chain=1):
    return TradeRecord(chain, token, kind, amount, usd, ts, ts, "0xc")


def test_aggregates_per_chain_and_token():
    stats = aggregate_positions(
        [
            rec("buy", 10, 5, usd=100.0),
            rec("buy", 5, 3),
            rec("sell", 4, 9, usd=60.0),
            rec("send", 2, 10),
            rec("receive", 1, 1),
            rec("buy", 7, 2, token="0xu"),
        ]
    )
    s = stats[(1, "0xt")]
    assert (s.bought, s.sold, s.sent, s.received) == (15, 4, 2, 1)
    assert (s.bought_usd, s.sold_usd, s.buys, s.sells) == (100.0, 60.0, 2, 1)
    assert (s.first_ts, s.last_ts) == (1, 10)
    assert stats[(1, "0xu")].bought == 7
```

`backend/apps/wallets/tests/test_filters.py` :

```python
import pytest

from apps.wallets.services.filters import (
    ChainActivity,
    farmer_reason,
    history_reason,
    mev_ratio,
    prefilter_reason,
)
from apps.wallets.services.positions import TradeRecord
from apps.wallets.tests.factories import make_thresholds

T = make_thresholds()


def rec(kind, token, block):
    return TradeRecord(1, token, kind, 1, None, block, block, "0xc")


@pytest.mark.parametrize(
    ("activities", "source", "reason"),
    [
        ([ChainActivity(10, 10)], "early_buyer", None),
        ([ChainActivity(1401, 10)], "early_buyer", "bot_frequency"),
        ([ChainActivity(1401, 10)], "linked", "bot_frequency"),
        ([ChainActivity(3, 2)], "early_buyer", "inactive"),
        ([ChainActivity(3, 2)], "linked", None),
        ([ChainActivity(3, 2), ChainActivity(9, 9)], "early_buyer", None),
    ],
)
def test_prefilter(activities, source, reason):
    assert prefilter_reason([(a, T) for a in activities], source) == reason


def test_prefilter_uses_each_chain_threshold():
    strict = make_thresholds(max_txs_per_day=1)
    assert prefilter_reason([(ChainActivity(10, 10), strict)], "early_buyer") == "bot_frequency"


def test_farmer():
    assert farmer_reason(301, T) == "farmer"
    assert farmer_reason(300, T) is None


def test_mev_ratio_counts_same_block_round_trips_excluding_quotes():
    records = [
        rec("buy", "0xa", 1), rec("sell", "0xa", 1),
        rec("buy", "0xb", 2), rec("sell", "0xb", 3),
        rec("buy", "0xusdc", 4), rec("sell", "0xusdc", 4),
    ]
    assert mev_ratio(records, {"0xusdc"}) == 50.0


def test_history_reason():
    three = [rec("buy", t, i) for i, t in enumerate(["0xa", "0xb", "0xc"])]
    assert history_reason(three, set(), T, "early_buyer") is None
    assert history_reason(three[:2], set(), T, "early_buyer") == "too_few_trades"
    assert history_reason(three[:2], set(), T, "linked") is None
    bots = three + [rec("sell", t, i) for i, t in enumerate(["0xa", "0xb", "0xc"])]
    assert history_reason(bots, set(), T, "early_buyer") == "bot_mev"
```

- [ ] **Step 3: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/wallets/tests/test_classify.py apps/wallets/tests/test_positions.py apps/wallets/tests/test_filters.py -q"`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 4: Implémenter**

`backend/apps/wallets/services/classify.py` :

```python
"""Classement des transferts d'un wallet. Fonction pure.

Achat : le wallet signe et reçoit via un contrat autre que le token (router, pool).
Vente : le wallet signe et envoie via un contrat autre que le token.
Envoi : appel direct à transfer() du token, ou tokens déplacés par un autre signataire.
Réception : le wallet reçoit sans avoir signé (transfert, airdrop).
"""

from dataclasses import dataclass

from integrations.hypersync import WalletTransfer

BUY = "buy"
SELL = "sell"
SEND = "send"
RECEIVE = "receive"


@dataclass(frozen=True)
class Trade:
    transfer: WalletTransfer
    kind: str
    counterparty: str


def classify(transfer: WalletTransfer, wallet: str) -> Trade | None:
    wallet = wallet.lower()
    if transfer.sender == wallet and transfer.recipient == wallet:
        return None
    via_contract = transfer.tx_from == wallet and transfer.tx_to != transfer.token
    if transfer.recipient == wallet:
        return Trade(transfer, BUY if via_contract else RECEIVE, transfer.sender)
    if transfer.sender == wallet:
        return Trade(transfer, SELL if via_contract else SEND, transfer.recipient)
    return None


def classify_all(transfers: list[WalletTransfer], wallet: str) -> list[Trade]:
    return [trade for trade in (classify(t, wallet) for t in transfers) if trade is not None]
```

`backend/apps/wallets/services/positions.py` :

```python
"""Agrégation des mouvements par (chaîne, token). Fonction pure."""

from dataclasses import dataclass

from apps.wallets.services.classify import BUY, RECEIVE, SELL, SEND


@dataclass(frozen=True)
class TradeRecord:
    chain_id: int
    token: str
    kind: str
    amount: int
    usd: float | None
    ts: int
    block: int
    counterparty: str


@dataclass
class PositionStats:
    bought: int = 0
    sold: int = 0
    sent: int = 0
    received: int = 0
    bought_usd: float = 0.0
    sold_usd: float = 0.0
    buys: int = 0
    sells: int = 0
    first_ts: int = 0
    last_ts: int = 0


def aggregate_positions(records: list[TradeRecord]) -> dict[tuple[int, str], PositionStats]:
    stats: dict[tuple[int, str], PositionStats] = {}
    for record in sorted(records, key=lambda r: r.ts):
        position = stats.setdefault(
            (record.chain_id, record.token), PositionStats(first_ts=record.ts, last_ts=record.ts)
        )
        position.last_ts = record.ts
        if record.kind == BUY:
            position.bought += record.amount
            position.buys += 1
            position.bought_usd += record.usd or 0.0
        elif record.kind == SELL:
            position.sold += record.amount
            position.sells += 1
            position.sold_usd += record.usd or 0.0
        elif record.kind == SEND:
            position.sent += record.amount
        elif record.kind == RECEIVE:
            position.received += record.amount
    return stats
```

`backend/apps/wallets/services/filters.py` :

```python
"""Filtres anti-bruit. Fonctions pures."""

from dataclasses import dataclass

from apps.wallets.services.classify import BUY, SELL
from apps.wallets.services.positions import TradeRecord
from apps.wallets.services.settings import QualificationThresholds

EARLY_BUYER = "early_buyer"


@dataclass(frozen=True)
class ChainActivity:
    txs_7d: int
    txs_active: int


def prefilter_reason(
    checks: list[tuple[ChainActivity, QualificationThresholds]], source: str
) -> str | None:
    if any(activity.txs_7d > t.max_txs_per_day * 7 for activity, t in checks):
        return "bot_frequency"
    if (
        source == EARLY_BUYER
        and checks
        and all(activity.txs_active < t.min_txs_active for activity, t in checks)
    ):
        return "inactive"
    return None


def farmer_reason(distinct_received: int, t: QualificationThresholds) -> str | None:
    return "farmer" if distinct_received > t.max_distinct_tokens else None


def mev_ratio(records: list[TradeRecord], quote_tokens: set[str]) -> float:
    buys = [r for r in records if r.kind == BUY and r.token not in quote_tokens]
    if not buys:
        return 0.0
    sells = {(r.token, r.block) for r in records if r.kind == SELL}
    same_block = sum(1 for r in buys if (r.token, r.block) in sells)
    return same_block * 100 / len(buys)


def distinct_buys(records: list[TradeRecord], quote_tokens: set[str]) -> int:
    return len({r.token for r in records if r.kind == BUY and r.token not in quote_tokens})


def history_reason(
    records: list[TradeRecord], quote_tokens: set[str], t: QualificationThresholds, source: str
) -> str | None:
    if source != EARLY_BUYER:
        return None
    if mev_ratio(records, quote_tokens) > t.max_mev_ratio:
        return "bot_mev"
    if distinct_buys(records, quote_tokens) < t.min_distinct_buys:
        return "too_few_trades"
    return None
```

- [ ] **Step 5: Lancer les tests**

Run: `make test args="apps/wallets -q"`
Expected: PASS.

- [ ] **Step 6: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend/apps/wallets
git commit -m "feat(wallets): classement des mouvements, positions par token et filtres anti-bruit

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Forwarding, liens et tags (fonctions pures)

**Files:**
- Create: `backend/apps/wallets/services/exchanges.py` (partie pure), `entities.py` (partie pure), `tags.py`
- Create: `backend/apps/wallets/tests/test_pure_entities.py`, `test_tags.py`

**Interfaces:**
- Consumes: `WalletTransfer`, `TradeRecord`, constantes de `classify`, `QualificationThresholds`.
- Produces:
  - `exchanges.forwarding(transfers, address, hours, min_forward_pct) -> tuple[float, str | None]` : % des réceptions renvoyées (≥ `min_forward_pct` du montant, en moins de `hours`) vers la destination la plus fréquente, et cette destination.
  - `entities.transfer_after_buy_targets(records, threshold_pct) -> dict[tuple[int, str], dict]` : (chain_id, destinataire) → `{"token", "pct"}`.
  - `tags` : `SNIPER`, `EARLY_BUYER`, `ACCUMULATEUR`, `FLIPPER`, `HOLDER` ; `EarlyBuy(token: str, chain_id: int, is_sniper: bool, bought: int, sold_before_peak: int)` ; `wallet_tags(records, early_buys, quote_tokens, t) -> list[str]` (triée) ; `entity_tags(member_tags, internal_holding) -> list[str]`.

- [ ] **Step 1: Écrire les tests**

`backend/apps/wallets/tests/test_pure_entities.py` :

```python
from apps.wallets.services.entities import transfer_after_buy_targets
from apps.wallets.services.exchanges import forwarding
from apps.wallets.services.positions import TradeRecord
from integrations.hypersync import WalletTransfer

D = "0x" + "d" * 40
HOT = "0x" + "e" * 40
A = "0x" + "1" * 40
X = "0x" + "2" * 40


def tr(token, sender, recipient, amount, ts):
    return WalletTransfer(
        block=ts, timestamp=ts, tx_hash=f"0x{ts}", log_index=0, token=token, sender=sender,
        recipient=recipient, amount=amount, tx_from=sender, tx_to=token, tx_value=0,
    )


def test_deposit_forwards_everything_to_one_destination():
    transfers = [
        tr(A, "0xu1", D, 100, 0), tr(A, D, HOT, 100, 600),
        tr(X, "0xu2", D, 50, 1000), tr(X, D, HOT, 49, 2000),
    ]
    assert forwarding(transfers, D, hours=24, min_forward_pct=90) == (100.0, HOT)


def test_wallet_keeping_tokens_is_not_forwarding():
    transfers = [tr(A, "0xu1", D, 100, 0), tr(A, D, HOT, 10, 600)]
    assert forwarding(transfers, D, 24, 90) == (0.0, None)


def test_forward_outside_window_does_not_count():
    transfers = [tr(A, "0xu1", D, 100, 0), tr(A, D, HOT, 100, 2 * 86_400)]
    assert forwarding(transfers, D, 24, 90) == (0.0, None)


def test_no_reception():
    assert forwarding([], D, 24, 90) == (0.0, None)


def rec(kind, token, amount, counterparty="0xpool", chain=1):
    return TradeRecord(chain, token, kind, amount, None, 1, 1, counterparty)


def test_targets_receiving_most_of_a_token():
    records = [
        rec("buy", "0xa", 100), rec("send", "0xa", 80, "0xvault"), rec("send", "0xa", 5, "0xfriend"),
        rec("receive", "0xusdc", 1000), rec("send", "0xusdc", 700, "0xvault"),
        rec("buy", "0xb", 10), rec("sell", "0xb", 10),
        rec("send", "0xc", 5, "0xz"),
    ]
    assert transfer_after_buy_targets(records, 70) == {(1, "0xvault"): {"token": "0xa", "pct": 80.0}}

```

`backend/apps/wallets/tests/test_tags.py` :

```python
from apps.wallets.services.positions import TradeRecord
from apps.wallets.services.tags import (
    ACCUMULATEUR,
    EARLY_BUYER,
    FLIPPER,
    HOLDER,
    SNIPER,
    EarlyBuy,
    entity_tags,
    wallet_tags,
)
from apps.wallets.tests.factories import make_thresholds

T = make_thresholds()
H = 3600


def rec(kind, token, amount, ts):
    return TradeRecord(1, token, kind, amount, None, ts, ts, "0xp")


def eb(sniper=False, bought=100, sold=0):
    return EarlyBuy(token="0xx", chain_id=1, is_sniper=sniper, bought=bought, sold_before_peak=sold)


def test_sniper_early_buyer_holder():
    tags = wallet_tags([], [eb(sniper=True, sold=40), eb(sold=90)], set(), T)
    assert tags == sorted([SNIPER, EARLY_BUYER, HOLDER])


def test_not_holder_when_mostly_sold():
    assert HOLDER not in wallet_tags([], [eb(sold=60)], set(), T)


def test_flipper_when_most_positions_sold_fast():
    records = [
        rec("buy", "0xa", 100, 0), rec("sell", "0xa", 85, 2 * H),
        rec("buy", "0xb", 100, 0), rec("sell", "0xb", 90, 5 * H),
        rec("buy", "0xc", 100, 0), rec("sell", "0xc", 100, 48 * H),
    ]
    assert FLIPPER in wallet_tags(records, [], set(), T)


def test_not_flipper_when_slow():
    records = [rec("buy", "0xa", 100, 0), rec("sell", "0xa", 100, 48 * H)]
    assert FLIPPER not in wallet_tags(records, [], set(), T)


def test_accumulator_needs_enough_positions():
    one = [rec("buy", "0xa", 50, 0), rec("buy", "0xa", 50, H), rec("sell", "0xa", 10, 2 * H)]
    two = one + [rec("buy", "0xb", 50, 0), rec("buy", "0xb", 50, H)]
    assert ACCUMULATEUR not in wallet_tags(one, [], set(), T)
    assert ACCUMULATEUR in wallet_tags(two, [], set(), T)


def test_quote_tokens_are_ignored():
    records = [rec("buy", "0xusdc", 100, 0), rec("sell", "0xusdc", 100, H)]
    assert wallet_tags(records, [], {"0xusdc"}, T) == []


def test_entity_tags_union_and_internal_holding():
    assert entity_tags([[SNIPER], [ACCUMULATEUR, SNIPER]], internal_holding=True) == sorted(
        [ACCUMULATEUR, HOLDER, SNIPER]
    )
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/wallets/tests/test_pure_entities.py apps/wallets/tests/test_tags.py -q"`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implémenter**

`backend/apps/wallets/services/exchanges.py` :

```python
"""Détection des exchanges : registre d'adresses, hot wallets, adresses de dépôt."""

from collections import Counter

from integrations.hypersync import WalletTransfer


def forwarding(
    transfers: list[WalletTransfer], address: str, hours: int, min_forward_pct: float
) -> tuple[float, str | None]:
    """Part (%) des réceptions renvoyées rapidement vers une même destination. Fonction pure."""
    address = address.lower()
    window = hours * 3600
    receptions = [t for t in transfers if t.recipient == address and t.sender != address]
    sends = [t for t in transfers if t.sender == address and t.recipient != address]
    if not receptions:
        return 0.0, None
    destinations: Counter[str] = Counter()
    for reception in receptions:
        out = [
            s
            for s in sends
            if s.token == reception.token
            and reception.timestamp <= s.timestamp <= reception.timestamp + window
        ]
        if out and sum(s.amount for s in out) * 100 >= reception.amount * min_forward_pct:
            destinations[max(out, key=lambda s: s.amount).recipient] += 1
    if not destinations:
        return 0.0, None
    destination, count = destinations.most_common(1)[0]
    return count * 100 / len(receptions), destination
```

`backend/apps/wallets/services/entities.py` :

```python
"""Entités : wallets d'une même personne, reliés par des transferts ou un financement."""

from collections import defaultdict

from apps.wallets.services.classify import BUY, RECEIVE, SEND
from apps.wallets.services.positions import TradeRecord


def transfer_after_buy_targets(
    records: list[TradeRecord], threshold_pct: float
) -> dict[tuple[int, str], dict]:
    """Destinataires ayant reçu ≥ threshold % d'un token entré dans le wallet. Fonction pure."""
    inflow: dict[tuple[int, str], int] = defaultdict(int)
    sent: dict[tuple[int, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for record in records:
        key = (record.chain_id, record.token)
        if record.kind in (BUY, RECEIVE):
            inflow[key] += record.amount
        elif record.kind == SEND:
            sent[key][record.counterparty] += record.amount
    targets: dict[tuple[int, str], dict] = {}
    for (chain_id, token), per_destination in sent.items():
        total = inflow[(chain_id, token)]
        if total <= 0:
            continue
        for destination, amount in per_destination.items():
            pct = round(amount * 100 / total, 2)
            current = targets.get((chain_id, destination))
            if pct >= threshold_pct and (current is None or pct > current["pct"]):
                targets[(chain_id, destination)] = {"token": token, "pct": pct}
    return targets
```

`backend/apps/wallets/services/tags.py` :

```python
"""Tags de comportement d'un wallet et d'une entité. Fonctions pures."""

from collections import defaultdict
from dataclasses import dataclass

from apps.wallets.services.classify import BUY, RECEIVE, SELL, SEND
from apps.wallets.services.positions import TradeRecord
from apps.wallets.services.settings import QualificationThresholds

SNIPER = "SNIPER"
EARLY_BUYER = "EARLY_BUYER"
ACCUMULATEUR = "ACCUMULATEUR"
FLIPPER = "FLIPPER"
HOLDER = "HOLDER"


@dataclass(frozen=True)
class EarlyBuy:
    token: str
    chain_id: int
    is_sniper: bool
    bought: int
    sold_before_peak: int


def _flipped(records: list[TradeRecord], t: QualificationThresholds) -> bool:
    buys = [r for r in records if r.kind == BUY]
    bought = sum(r.amount for r in buys)
    first_buy = min(r.ts for r in buys)
    sold = 0
    for record in sorted(records, key=lambda r: r.ts):
        if record.kind != SELL:
            continue
        sold += record.amount
        if sold * 100 >= t.flipper_min_sold_pct * bought:
            return record.ts - first_buy <= t.flipper_hours * 3600
    return False


def _accumulating(records: list[TradeRecord], t: QualificationThresholds) -> bool:
    buys = [r for r in records if r.kind == BUY]
    inflow = sum(r.amount for r in records if r.kind in (BUY, RECEIVE))
    out = sum(r.amount for r in records if r.kind in (SELL, SEND))
    return len(buys) >= 2 and out * 100 < t.accumulator_max_out_pct * inflow


def wallet_tags(
    records: list[TradeRecord],
    early_buys: list[EarlyBuy],
    quote_tokens: set[str],
    t: QualificationThresholds,
) -> list[str]:
    tags: set[str] = set()
    if any(e.is_sniper for e in early_buys):
        tags.add(SNIPER)
    if len(early_buys) >= t.early_buyer_min_explosions:
        tags.add(EARLY_BUYER)
    if any(
        e.bought > 0 and (e.bought - e.sold_before_peak) * 100 >= t.holder_min_pct * e.bought
        for e in early_buys
    ):
        tags.add(HOLDER)

    per_position: dict[tuple[int, str], list[TradeRecord]] = defaultdict(list)
    for record in records:
        if record.token not in quote_tokens:
            per_position[(record.chain_id, record.token)].append(record)
    bought_positions = [rs for rs in per_position.values() if any(r.kind == BUY for r in rs)]
    if bought_positions:
        flipped = sum(1 for rs in bought_positions if _flipped(rs, t))
        if flipped * 100 > t.flipper_min_share_pct * len(bought_positions):
            tags.add(FLIPPER)
        if sum(1 for rs in bought_positions if _accumulating(rs, t)) >= t.accumulator_min_positions:
            tags.add(ACCUMULATEUR)
    return sorted(tags)


def entity_tags(member_tags: list[list[str]], internal_holding: bool) -> list[str]:
    tags = {tag for member in member_tags for tag in member}
    if internal_holding:
        tags.add(HOLDER)
    return sorted(tags)
```

- [ ] **Step 4: Lancer les tests**

Run: `make test args="apps/wallets -q"`
Expected: PASS.

- [ ] **Step 5: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend/apps/wallets
git commit -m "feat(wallets): forwarding, liens d'entité et tags

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Prix des mouvements, actifs de cotation, prix natifs et valeur d'un wallet

**Files:**
- Create: `backend/apps/wallets/services/pricing.py`
- Create: `backend/apps/wallets/tests/fakes.py`
- Create: `backend/apps/wallets/tests/test_pricing.py`, `test_pricing_db.py`

**Interfaces:**
- Consumes: `Trade`, `classify_all`, `BUY`, `SELL` (Task 5) ; `KnownAddress`, `DailyPrice`, `TokenPosition` (Task 4) ; `Fungible`, `BudgetExhausted`, `IntegrationError` (Task 2).
- Produces:
  - `STABLE = "stable"`, `WRAPPED = "wrapped"`, `NATIVE_DECIMALS = 18`, `QuoteAsset(kind: str, decimals: int)`.
  - `price_trades(trades, wallet, quotes: dict[str, QuoteAsset], native_price: Callable[[int], float | None]) -> dict[tuple[str, int], float | None]` (clé = (tx_hash, log_index)).
  - `sync_quote_assets(zerion, cfg) -> int`, `quote_assets(chain) -> dict[str, QuoteAsset]`, `quote_tokens(chains) -> set[str]`.
  - `native_price_lookup(chain, zerion, now) -> Callable[[int], float | None]`.
  - `wallet_value(wallet, chains, zerion, rpc_for, now) -> tuple[float, dict]` (lève `BudgetExhausted`).
  - `tests.fakes` : constantes `NOW`, `HEIGHT`, `GENESIS_TS`, `BLOCK_TIME`, `UNIT`, `BUYER`, `VAULT`, `FUNDER`, `POOL`, `ROUTER`, `TOKEN_A`, `TOKEN_B`, `TOKEN_C`, `USDC`, `WETH`, `DEPOSIT`, `HOT` ; `block_of(ts)`, `tr(...)`, `buyer_history()`, `vault_history()` ; `FakeWalletHyperSync`, `FakeZerion`, `FakeRpc`.

- [ ] **Step 1: Écrire les faux clients (scénario partagé)**

`backend/apps/wallets/tests/fakes.py` :

```python
"""Scénario : un wallet d'achat (BUYER) achète 3 tokens, envoie 90 % de A vers un coffre (VAULT).
FUNDER lui a envoyé son premier gaz. Prix : A = 3 $, B = 1 $, C = 2 $, ETH = 2 000 $."""

from collections import Counter
from datetime import UTC, datetime

from integrations.errors import BudgetExhausted
from integrations.hypersync import Funding, WalletTransfer
from integrations.zerion import Fungible

NOW = datetime(2026, 9, 27, 12, tzinfo=UTC)
BLOCK_TIME = 2
HEIGHT = 50_000_000
GENESIS_TS = int(NOW.timestamp()) - HEIGHT * BLOCK_TIME
UNIT = 10**18

BUYER = "0x" + "a" * 40
VAULT = "0x" + "b" * 40
FUNDER = "0x" + "f" * 40
POOL = "0x" + "2" * 40
ROUTER = "0x" + "9" * 40
TOKEN_A = "0x" + "1" * 40
TOKEN_B = "0x" + "3" * 40
TOKEN_C = "0x" + "4" * 40
USDC = "0x" + "c" * 40
WETH = "0x" + "d" * 40
DEPOSIT = "0x" + "5" * 40
HOT = "0x" + "e" * 40


def block_of(ts: int) -> int:
    return (ts - GENESIS_TS) // BLOCK_TIME


def tr(block, tx, token, sender, recipient, amount, tx_from, tx_to, tx_value=0, log_index=0):
    return WalletTransfer(
        block=block, timestamp=GENESIS_TS + block * BLOCK_TIME, tx_hash=tx, log_index=log_index,
        token=token, sender=sender, recipient=recipient, amount=amount, tx_from=tx_from,
        tx_to=tx_to, tx_value=tx_value,
    )


START = HEIGHT - 100_000  # environ 2,3 jours avant NOW


def buyer_history(send_to: str = VAULT) -> list[WalletTransfer]:
    return [
        tr(START - 10, "0xt0", USDC, FUNDER, BUYER, 5_000 * 10**6, FUNDER, USDC),
        tr(START, "0xt1", USDC, BUYER, POOL, 1_000 * 10**6, BUYER, ROUTER, log_index=0),
        tr(START, "0xt1", TOKEN_A, POOL, BUYER, 500 * UNIT, BUYER, ROUTER, log_index=1),
        tr(START + 10, "0xt2", TOKEN_B, POOL, BUYER, 200 * UNIT, BUYER, ROUTER, tx_value=UNIT // 2),
        tr(START + 20, "0xt3", USDC, BUYER, POOL, 300 * 10**6, BUYER, ROUTER, log_index=0),
        tr(START + 20, "0xt3", TOKEN_C, POOL, BUYER, 100 * UNIT, BUYER, ROUTER, log_index=1),
        tr(START + 30, "0xt4", USDC, BUYER, POOL, 300 * 10**6, BUYER, ROUTER, log_index=0),
        tr(START + 30, "0xt4", TOKEN_C, POOL, BUYER, 100 * UNIT, BUYER, ROUTER, log_index=1),
        tr(START + 40, "0xt5", TOKEN_A, BUYER, send_to, 450 * UNIT, BUYER, TOKEN_A),
    ]


def vault_history() -> list[WalletTransfer]:
    return [tr(START + 40, "0xt5", TOKEN_A, BUYER, VAULT, 450 * UNIT, BUYER, TOKEN_A)]


def deposit_history() -> list[WalletTransfer]:
    return [
        tr(START + 40, "0xt5", TOKEN_A, BUYER, DEPOSIT, 450 * UNIT, BUYER, TOKEN_A),
        tr(START + 45, "0xt6", TOKEN_A, DEPOSIT, HOT, 450 * UNIT, HOT, TOKEN_A),
    ]


class FakeWalletHyperSync:
    def __init__(self, transfers=None, tx_counts=None, counterparties=None, fundings=None):
        self.transfers = transfers if transfers is not None else {
            BUYER: buyer_history(), VAULT: vault_history()
        }
        self.tx_counts = tx_counts if tx_counts is not None else {VAULT: 0}
        self.counterparties = counterparties or {}
        self.fundings = fundings if fundings is not None else {BUYER: Funding(FUNDER, START - 20, UNIT)}

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


class FakeZerion:
    PRICES = {TOKEN_A: 3.0, TOKEN_B: 1.0, TOKEN_C: 2.0, USDC: 1.0}

    def __init__(self, exhausted: bool = False):
        self.calls: Counter[str] = Counter()
        self.exhausted = exhausted

    def _call(self, name):
        if self.exhausted:
            raise BudgetExhausted("budget")
        self.calls[name] += 1

    def prices(self, implementations):
        self._call("prices")
        return {
            (chain, address): Fungible(address, "T", self.PRICES[address], {chain: (address, 6 if address == USDC else 18)})
            for chain, address in implementations
            if address in self.PRICES
        }

    def fungible(self, fungible_id):
        self._call("fungible")
        return Fungible(fungible_id, "WETH", 2000.0, {"base": (WETH, 18), "bsc": ("0x" + "7" * 40, 18)})

    def search(self, symbol):
        self._call("search")
        return Fungible("usdc", "USDC", 1.0, {"base": (USDC, 6)}) if symbol == "USDC" else None

    def price_chart(self, fungible_id):
        self._call("chart")
        start = int(NOW.timestamp()) - 400 * 86_400
        return [(start + i * 86_400, 2000.0) for i in range(401)]


class FakeRpc:
    def __init__(self, balances):
        self.balances = balances

    def native_balance(self, address):
        return self.balances.get(address, 0)
```

- [ ] **Step 2: Écrire les tests purs des prix**

`backend/apps/wallets/tests/test_pricing.py` :

```python
from apps.wallets.services.classify import classify_all
from apps.wallets.services.pricing import STABLE, WRAPPED, QuoteAsset, price_trades
from integrations.hypersync import WalletTransfer

W = "0x" + "a" * 40
POOL = "0x" + "b" * 40
ROUTER = "0x" + "9" * 40
OTHER = "0x" + "8" * 40
TOK = "0x" + "1" * 40
TOK2 = "0x" + "3" * 40
USDC = "0x" + "c" * 40
WETH = "0x" + "d" * 40
QUOTES = {USDC: QuoteAsset(STABLE, 6), WETH: QuoteAsset(WRAPPED, 18)}


def tr(tx, idx, token, sender, recipient, amount, tx_to=ROUTER, value=0):
    return WalletTransfer(
        block=1, timestamp=100, tx_hash=tx, log_index=idx, token=token, sender=sender,
        recipient=recipient, amount=amount, tx_from=W, tx_to=tx_to, tx_value=value,
    )


def run(transfers, native=2000.0):
    return price_trades(classify_all(transfers, W), W, QUOTES, lambda ts: native)


def test_buy_paid_in_stablecoin():
    usd = run([tr("0x1", 0, USDC, W, POOL, 1_500 * 10**6), tr("0x1", 1, TOK, POOL, W, 10**18)])
    assert usd[("0x1", 1)] == 1500.0
    assert usd[("0x1", 0)] == 1500.0


def test_buy_paid_in_native():
    assert run([tr("0x2", 0, TOK, POOL, W, 10**18, value=10**18 // 4)])[("0x2", 0)] == 500.0


def test_buy_paid_in_wrapped_native():
    usd = run([tr("0x3", 0, WETH, W, POOL, 10**17), tr("0x3", 1, TOK, POOL, W, 10**18)])
    assert usd[("0x3", 1)] == 200.0


def test_sell_for_stablecoin():
    usd = run([tr("0x4", 0, TOK, W, POOL, 10**18), tr("0x4", 1, USDC, POOL, W, 800 * 10**6)])
    assert usd[("0x4", 0)] == 800.0


def test_split_between_legs_of_same_token():
    usd = run([
        tr("0x5", 0, USDC, W, POOL, 400 * 10**6),
        tr("0x5", 1, TOK, POOL, W, 1 * 10**18),
        tr("0x5", 2, TOK, POOL, W, 3 * 10**18),
    ])
    assert (usd[("0x5", 1)], usd[("0x5", 2)]) == (100.0, 300.0)


def test_unknown_when_two_different_tokens_bought():
    usd = run([
        tr("0x6", 0, USDC, W, POOL, 100 * 10**6),
        tr("0x6", 1, TOK, POOL, W, 10**18),
        tr("0x6", 2, TOK2, POOL, W, 10**18),
    ])
    assert (usd[("0x6", 1)], usd[("0x6", 2)]) == (None, None)


def test_transfer_has_no_price():
    assert run([tr("0x7", 0, TOK, W, OTHER, 10**18, tx_to=TOK)])[("0x7", 0)] is None


def test_native_payment_without_price_is_unknown():
    assert run([tr("0x8", 0, TOK, POOL, W, 10**18, value=10**18)], native=None)[("0x8", 0)] is None
```

- [ ] **Step 3: Écrire les tests en base des prix**

`backend/apps/wallets/tests/test_pricing_db.py` :

```python
import pytest
from django.core.exceptions import ImproperlyConfigured

from apps.discovery.models import PipelineSettings, Token, Wallet
from apps.discovery.tests.factories import make_chain, make_token
from apps.wallets.models import KnownAddress, TokenPosition
from apps.wallets.services.pricing import (
    STABLE,
    WRAPPED,
    QuoteAsset,
    native_price_lookup,
    quote_assets,
    sync_quote_assets,
    wallet_value,
)
from apps.wallets.tests.fakes import BUYER, NOW, TOKEN_A, UNIT, USDC, WETH, FakeRpc, FakeZerion
from integrations.errors import BudgetExhausted

pytestmark = pytest.mark.django_db


@pytest.fixture
def chain():
    return make_chain(native_fungible_id="eth", wrapped_fungible_id="0xweth-id", rpc_url="https://rpc.test/")


def test_sync_quote_assets_registers_wrapped_and_stables(chain):
    assert sync_quote_assets(FakeZerion(), PipelineSettings.load()) == 2
    assert KnownAddress.objects.get(address=USDC).kind == "stablecoin"
    assert Token.objects.get(address=USDC).decimals == 6
    assert KnownAddress.objects.get(address=WETH).kind == "wrapped_native"
    assert quote_assets(chain) == {USDC: QuoteAsset(STABLE, 6), WETH: QuoteAsset(WRAPPED, 18)}


def test_wrapped_asset_only_registered_on_chains_using_it(chain):
    sync_quote_assets(FakeZerion(), PipelineSettings.load())
    assert not KnownAddress.objects.filter(address="0x" + "7" * 40).exists()


def test_native_price_lookup_fetches_chart_once(chain):
    zerion = FakeZerion()
    lookup = native_price_lookup(chain, zerion, NOW)
    assert lookup(int(NOW.timestamp())) == 2000.0
    assert lookup(0) is None
    native_price_lookup(chain, zerion, NOW)
    assert zerion.calls["chart"] == 1


def test_wallet_value_sums_tokens_and_native(chain):
    wallet = Wallet.objects.create(address=BUYER)
    token = make_token(chain, address=TOKEN_A)
    TokenPosition.objects.create(wallet=wallet, token=token, bought_amount=50 * UNIT, first_at=NOW, last_at=NOW)
    total, details = wallet_value(wallet, [chain], FakeZerion(), lambda c: FakeRpc({BUYER: UNIT}), NOW)
    assert total == 2150.0
    assert details["base"] == {"tokens_usd": 150.0, "native_usd": 2000.0}


def test_wallet_value_without_rpc_counts_native_as_zero(chain):
    wallet = Wallet.objects.create(address=BUYER)

    def no_rpc(c):
        raise ImproperlyConfigured("pas de RPC")

    total, details = wallet_value(wallet, [chain], FakeZerion(), no_rpc, NOW)
    assert total == 0.0
    assert details["base"]["native_error"] is True


def test_wallet_value_propagates_budget_exhaustion(chain):
    wallet = Wallet.objects.create(address=BUYER)
    TokenPosition.objects.create(
        wallet=wallet, token=make_token(chain, address=TOKEN_A), bought_amount=UNIT, first_at=NOW, last_at=NOW
    )
    with pytest.raises(BudgetExhausted):
        wallet_value(wallet, [chain], FakeZerion(exhausted=True), lambda c: FakeRpc({}), NOW)
```

- [ ] **Step 4: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/wallets/tests/test_pricing.py apps/wallets/tests/test_pricing_db.py -q"`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 5: Implémenter**

`backend/apps/wallets/services/pricing.py` :

```python
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
            value = amount if quote.kind == STABLE else (amount * price if price is not None else None)
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
            chain=chain, address=address, defaults={"symbol": fungible.symbol[:64], "decimals": decimals}
        )
        KnownAddress.objects.update_or_create(
            chain=chain,
            address=address,
            defaults={"kind": kind, "label": fungible.symbol[:128], "source": KnownAddress.Source.AUTO},
        )
        registered += 1
    return registered


def sync_quote_assets(zerion, cfg) -> int:
    """Natifs wrappés (chaînes qui les utilisent) et stablecoins, adresses récupérées sur Zerion."""
    chains = [chain for chain in Chain.objects.active() if chain.zerion_id]
    registered = 0
    wrapped_ids = sorted({chain.wrapped_fungible_id for chain in chains if chain.wrapped_fungible_id})
    for fungible_id in wrapped_ids:
        users = {c.zerion_id: c for c in chains if c.wrapped_fungible_id == fungible_id}
        registered += _register_quote(zerion.fungible(fungible_id), users, KnownAddress.Kind.WRAPPED_NATIVE)
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
            STABLE if k.kind == KnownAddress.Kind.STABLECOIN else WRAPPED, decimals.get(k.address, 18)
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
    """Prix quotidien de l'actif natif ; la courbe Zerion (1 an) est récupérée au plus une fois par jour."""
    fungible_id = chain.native_fungible_id
    if not fungible_id:
        return lambda ts: None
    latest = DailyPrice.objects.filter(fungible_id=fungible_id).order_by("-day").first()
    if latest is None or latest.day < now.date() - timedelta(days=1):
        by_day = {
            datetime.fromtimestamp(ts, UTC).date(): price for ts, price in zerion.price_chart(fungible_id)
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
            for p in TokenPosition.objects.filter(wallet=wallet, token__chain=chain).select_related("token")
            if p.balance > 0
        ]
        prices = zerion.prices([(chain.zerion_id, p.token.address) for p in positions]) if positions else {}
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
```

- [ ] **Step 6: Lancer les tests**

Run: `make test args="apps/wallets -q"`
Expected: PASS.

- [ ] **Step 7: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend/apps/wallets
git commit -m "feat(wallets): prix des mouvements, actifs de cotation, prix natifs et valeur des wallets

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Clients Zerion (débit + budget) et RPC

**Files:**
- Modify: `backend/apps/discovery/services/clients.py`
- Modify: `backend/apps/discovery/tests/test_clients.py`

**Interfaces:**
- Consumes: `DailyBudget` (Task 2), `RpcClient` (Task 2), réglages (Task 1).
- Produces: `clients.zerion(cfg)` (débit `zerion_requests_per_min`, budget `zerion_daily_budget`) ; `clients.rpc(chain, cfg) -> RpcClient` (lève `ImproperlyConfigured` sans `rpc_url`).

- [ ] **Step 1: Écrire les tests**

Ajouter à `backend/apps/discovery/tests/test_clients.py` :

```python
from integrations.ratelimit import DailyBudget, RateLimiter


def test_zerion_client_has_rate_limit_and_daily_budget(settings):
    settings.ZERION_API_KEY = "k"
    http = clients.zerion(PipelineSettings.load())._http
    assert isinstance(http._limiter, RateLimiter)
    assert isinstance(http._budget, DailyBudget)
    assert http._budget._per_day == 250


def test_rpc_requires_public_url():
    with pytest.raises(ImproperlyConfigured):
        clients.rpc(make_chain(rpc_url=""), PipelineSettings.load())


def test_rpc_client_uses_chain_url():
    rpc = clients.rpc(make_chain(rpc_url="https://rpc.test/"), PipelineSettings.load())
    assert str(rpc._http._client.base_url) == "https://rpc.test/"
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/discovery/tests/test_clients.py -q"`
Expected: FAIL.

- [ ] **Step 3: Implémenter**

Dans `backend/apps/discovery/services/clients.py` :
- imports : `from integrations.ratelimit import DailyBudget, NoopLimiter, RateLimiter` et `from integrations.rpc import RpcClient` ;
- remplacer `zerion()` et ajouter `rpc()` :

```python
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
```

- [ ] **Step 4: Lancer les tests**

Run: `make test args="-q"`
Expected: PASS.

- [ ] **Step 5: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend/apps/discovery
git commit -m "feat(discovery): client Zerion avec débit et budget quotidien, client RPC par chaîne

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: Orchestration — pré-filtre et historique

**Files:**
- Create: `backend/apps/wallets/services/qualification.py`
- Create: `backend/apps/wallets/tests/test_qualification_history.py`

**Interfaces:**
- Consumes: `find_block_at` (découverte), `classify_all`, `TradeRecord`, `aggregate_positions`, filtres (Task 5), `price_trades`, `quote_assets`, `quote_tokens`, `native_price_lookup` (Task 7), `qualification_thresholds` (Task 4), faux clients (Task 7).
- Produces:
  - `Clients(hypersync_for: Callable[[Chain], HyperSyncClient], zerion: ZerionClient, rpc_for: Callable[[Chain], RpcClient])`.
  - `profile_chains(profile) -> list[Chain]`, `block_at(hypersync, ts, height) -> int`, `filter_out(profile, reason, now) -> str`, `records_for(wallet) -> list[TradeRecord]`, `save_trades(wallet, chain, trades, usd)`, `recompute_positions(wallet)`.
  - `prefilter_step(profile, clients, now) -> str`, `history_step(profile, clients, now) -> str`.

- [ ] **Step 1: Écrire les tests**

`backend/apps/wallets/tests/test_qualification_history.py` :

```python
from datetime import timedelta
from decimal import Decimal

import pytest

from apps.discovery.models import PipelineSettings, Wallet
from apps.discovery.tests.factories import make_chain
from apps.wallets.models import KnownAddress, QualificationSettings, TokenPosition, TokenTrade, WalletProfile
from apps.wallets.services.pricing import sync_quote_assets
from apps.wallets.services.qualification import Clients, history_step, prefilter_step
from apps.wallets.tests.fakes import (
    BUYER,
    NOW,
    TOKEN_A,
    TOKEN_B,
    UNIT,
    USDC,
    VAULT,
    FakeRpc,
    FakeWalletHyperSync,
    FakeZerion,
    buyer_history,
)

pytestmark = pytest.mark.django_db


@pytest.fixture
def chain():
    chain = make_chain(native_fungible_id="eth", wrapped_fungible_id="0xweth-id", rpc_url="https://rpc.test/")
    sync_quote_assets(FakeZerion(), PipelineSettings.load())
    return chain


def make_profile(chain, address=BUYER, **overrides):
    wallet, _ = Wallet.objects.get_or_create(address=address)
    return WalletProfile.objects.create(wallet=wallet, chains=[chain.pk], **overrides)


def make_clients(hypersync=None, zerion=None):
    hs = hypersync or FakeWalletHyperSync()
    return Clients(
        hypersync_for=lambda c: hs,
        zerion=zerion or FakeZerion(),
        rpc_for=lambda c: FakeRpc({BUYER: UNIT, VAULT: 5 * UNIT}),
    )


def test_prefilter_passes_active_wallet(chain):
    profile = make_profile(chain)
    assert prefilter_step(profile, make_clients(), NOW) == "prefiltered"
    assert profile.metrics["prefilter"]["base"] == {"txs_7d": 10, "txs_active": 5}


def test_prefilter_filters_bots_and_schedules_recheck(chain):
    profile = make_profile(chain)
    status = prefilter_step(profile, make_clients(FakeWalletHyperSync(tx_counts={BUYER: 5000})), NOW)
    assert (status, profile.filter_reason) == ("filtered", "bot_frequency")
    assert profile.next_analysis_at == NOW + timedelta(days=30)


def test_prefilter_known_exchange(chain):
    KnownAddress.objects.create(chain=None, address=BUYER, kind="exchange")
    profile = make_profile(chain)
    prefilter_step(profile, make_clients(), NOW)
    assert profile.filter_reason == "exchange"


def test_inactive_only_filters_early_buyers(chain):
    quiet = FakeWalletHyperSync(tx_counts={BUYER: 2})
    early = make_profile(chain)
    assert prefilter_step(early, make_clients(quiet), NOW) == "filtered"
    linked = make_profile(chain, address=VAULT, source="linked", depth=1)
    assert prefilter_step(linked, make_clients(FakeWalletHyperSync(tx_counts={VAULT: 0})), NOW) == "prefiltered"


def test_history_saves_priced_trades_and_positions(chain):
    profile = make_profile(chain, status="prefiltered")
    assert history_step(profile, make_clients(), NOW) == "history_fetched"
    assert TokenTrade.objects.filter(wallet=profile.wallet).count() == 9
    buy_a = TokenTrade.objects.get(wallet=profile.wallet, token__address=TOKEN_A, kind="buy")
    buy_b = TokenTrade.objects.get(wallet=profile.wallet, token__address=TOKEN_B, kind="buy")
    assert (buy_a.usd, buy_b.usd) == (Decimal("1000.00"), Decimal("1000.00"))
    assert TokenTrade.objects.get(wallet=profile.wallet, kind="send").usd is None
    a = TokenPosition.objects.get(wallet=profile.wallet, token__address=TOKEN_A)
    assert (a.bought_amount, a.sent_amount, a.balance) == (500 * UNIT, 450 * UNIT, 50 * UNIT)
    usdc = TokenPosition.objects.get(wallet=profile.wallet, token__address=USDC)
    assert (usdc.received_amount, usdc.sold_amount, usdc.sold_usd) == (5000 * 10**6, 1600 * 10**6, Decimal("1600.00"))


def test_history_is_idempotent(chain):
    profile = make_profile(chain, status="prefiltered")
    history_step(profile, make_clients(), NOW)
    profile.status = "prefiltered"
    history_step(profile, make_clients(), NOW)
    assert TokenTrade.objects.filter(wallet=profile.wallet).count() == 9
    assert TokenPosition.objects.filter(wallet=profile.wallet).count() == 4


def test_history_filters_farmers(chain):
    QualificationSettings.objects.filter(chain=None).update(max_distinct_tokens=2)
    profile = make_profile(chain, status="prefiltered")
    assert history_step(profile, make_clients(), NOW) == "filtered"
    assert profile.filter_reason == "farmer"
    assert not TokenTrade.objects.exists()


def test_history_filters_occasional_traders(chain):
    hs = FakeWalletHyperSync(transfers={BUYER: buyer_history()[:3]})
    profile = make_profile(chain, status="prefiltered")
    history_step(profile, make_clients(hs), NOW)
    assert profile.filter_reason == "too_few_trades"
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/wallets/tests/test_qualification_history.py -q"`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implémenter**

`backend/apps/wallets/services/qualification.py` :

```python
"""Qualification d'un wallet par étapes idempotentes : pré-filtre, historique, entités, valeur, tags."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from django.db import transaction

from apps.discovery.models import Chain, Token
from apps.discovery.services.blocks import find_block_at
from apps.wallets.models import KnownAddress, TokenPosition, TokenTrade, WalletProfile
from apps.wallets.services.classify import Trade, classify_all
from apps.wallets.services.filters import (
    ChainActivity,
    farmer_reason,
    history_reason,
    prefilter_reason,
)
from apps.wallets.services.positions import TradeRecord, aggregate_positions
from apps.wallets.services.pricing import (
    native_price_lookup,
    price_trades,
    quote_assets,
    quote_tokens,
)
from apps.wallets.services.settings import qualification_thresholds

Status = WalletProfile.Status
BATCH_SIZE = 1000
DAY = 86_400


@dataclass
class Clients:
    hypersync_for: Callable[[Chain], object]
    zerion: object
    rpc_for: Callable[[Chain], object]


def _dt(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, UTC)


def _decimal(value: float | None) -> Decimal | None:
    return Decimal(str(value)) if value is not None else None


def profile_chains(profile: WalletProfile) -> list[Chain]:
    return list(Chain.objects.active().filter(pk__in=profile.chains))


def block_at(hypersync, ts: int, height: int) -> int:
    return find_block_at(ts, 0, height, hypersync.block_timestamp)


def filter_out(profile: WalletProfile, reason: str, now: datetime) -> str:
    profile.status = Status.FILTERED
    profile.filter_reason = reason
    profile.analyzed_at = now
    profile.next_analysis_at = now + timedelta(days=qualification_thresholds().refilter_after_days)
    profile.save()
    return profile.status


def records_for(wallet) -> list[TradeRecord]:
    return [
        TradeRecord(
            chain_id=trade.token.chain_id,
            token=trade.token.address,
            kind=trade.kind,
            amount=int(trade.amount),
            usd=float(trade.usd) if trade.usd is not None else None,
            ts=int(trade.at.timestamp()),
            block=trade.block,
            counterparty=trade.counterparty,
        )
        for trade in TokenTrade.objects.filter(wallet=wallet).select_related("token")
    ]


def _tokens(chain: Chain, addresses: set[str]) -> dict[str, Token]:
    existing = set(Token.objects.filter(chain=chain, address__in=addresses).values_list("address", flat=True))
    Token.objects.bulk_create(
        [Token(chain=chain, address=a) for a in addresses - existing],
        ignore_conflicts=True,
        batch_size=BATCH_SIZE,
    )
    return {t.address: t for t in Token.objects.filter(chain=chain, address__in=addresses)}


def save_trades(wallet, chain: Chain, trades: list[Trade], usd: dict) -> None:
    tokens = _tokens(chain, {trade.transfer.token for trade in trades})
    TokenTrade.objects.bulk_create(
        [
            TokenTrade(
                wallet=wallet,
                token=tokens[trade.transfer.token],
                kind=trade.kind,
                amount=Decimal(trade.transfer.amount),
                usd=_decimal(usd.get((trade.transfer.tx_hash, trade.transfer.log_index))),
                counterparty=trade.counterparty,
                block=trade.transfer.block,
                at=_dt(trade.transfer.timestamp),
                tx_hash=trade.transfer.tx_hash,
                log_index=trade.transfer.log_index,
            )
            for trade in trades
        ],
        ignore_conflicts=True,
        batch_size=BATCH_SIZE,
    )


def recompute_positions(wallet) -> None:
    stats = aggregate_positions(records_for(wallet))
    tokens = {(t.chain_id, t.address): t for t in Token.objects.filter(trades__wallet=wallet).distinct()}
    with transaction.atomic():
        TokenPosition.objects.filter(wallet=wallet).delete()
        TokenPosition.objects.bulk_create(
            [
                TokenPosition(
                    wallet=wallet,
                    token=tokens[key],
                    bought_amount=Decimal(s.bought),
                    sold_amount=Decimal(s.sold),
                    sent_amount=Decimal(s.sent),
                    received_amount=Decimal(s.received),
                    bought_usd=Decimal(str(round(s.bought_usd, 2))),
                    sold_usd=Decimal(str(round(s.sold_usd, 2))),
                    buys=s.buys,
                    sells=s.sells,
                    first_at=_dt(s.first_ts),
                    last_at=_dt(s.last_ts),
                )
                for key, s in stats.items()
            ],
            batch_size=BATCH_SIZE,
        )


def prefilter_step(profile: WalletProfile, clients: Clients, now: datetime) -> str:
    wallet = profile.wallet
    chains = profile_chains(profile)
    if not chains:
        return filter_out(profile, "no_chain", now)
    if KnownAddress.objects.blocking_for(wallet.address, [c.pk for c in chains]).exists():
        return filter_out(profile, "exchange", now)
    ts_now = int(now.timestamp())
    checks = []
    measures = {}
    for chain in chains:
        t = qualification_thresholds(chain)
        hypersync = clients.hypersync_for(chain)
        height = hypersync.height()
        week = block_at(hypersync, ts_now - 7 * DAY, height)
        active = block_at(hypersync, ts_now - t.inactive_days * DAY, height)
        activity = ChainActivity(
            txs_7d=hypersync.wallet_tx_count(wallet.address, week, height, cap=t.max_txs_per_day * 7 + 1),
            txs_active=hypersync.wallet_tx_count(wallet.address, active, height, cap=t.min_txs_active),
        )
        checks.append((activity, t))
        measures[chain.gt_id] = {"txs_7d": activity.txs_7d, "txs_active": activity.txs_active}
    profile.metrics = {**profile.metrics, "prefilter": measures}
    reason = prefilter_reason(checks, profile.source)
    if reason:
        return filter_out(profile, reason, now)
    profile.status = Status.PREFILTERED
    profile.save(update_fields=["status", "metrics"])
    return profile.status


def history_step(profile: WalletProfile, clients: Clients, now: datetime) -> str:
    wallet = profile.wallet
    chains = profile_chains(profile)
    distinct: dict[str, int] = {}
    for chain in chains:
        t = qualification_thresholds(chain)
        hypersync = clients.hypersync_for(chain)
        height = hypersync.height()
        start = block_at(hypersync, int(now.timestamp()) - t.history_days * DAY, height)
        transfers = hypersync.wallet_transfers(wallet.address, start, height)
        distinct[chain.gt_id] = len({x.token for x in transfers if x.recipient == wallet.address})
        reason = farmer_reason(distinct[chain.gt_id], t)
        if reason:
            profile.metrics = {**profile.metrics, "distinct_tokens": distinct}
            return filter_out(profile, reason, now)
        trades = classify_all(transfers, wallet.address)
        usd = price_trades(
            trades, wallet.address, quote_assets(chain), native_price_lookup(chain, clients.zerion, now)
        )
        save_trades(wallet, chain, trades, usd)
    recompute_positions(wallet)
    profile.metrics = {**profile.metrics, "distinct_tokens": distinct}
    reason = history_reason(
        records_for(wallet), quote_tokens(chains), qualification_thresholds(), profile.source
    )
    if reason:
        return filter_out(profile, reason, now)
    profile.status = Status.HISTORY_FETCHED
    profile.save(update_fields=["status", "metrics"])
    return profile.status
```

- [ ] **Step 4: Lancer les tests**

Run: `make test args="apps/wallets -q"`
Expected: PASS.

- [ ] **Step 5: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend/apps/wallets
git commit -m "feat(wallets): pré-filtre HyperSync et historique par token (mouvements valorisés)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Exchanges, entités, valeur, tags et qualification complète

**Files:**
- Modify: `backend/apps/wallets/services/exchanges.py`, `entities.py`, `qualification.py`
- Modify: `backend/apps/wallets/tests/factories.py`
- Create: `backend/apps/wallets/tests/test_qualification_entities.py`

**Interfaces:**
- Consumes: Tasks 4 à 9.
- Produces:
  - `exchanges.register(address, chain, kind, label="")`, `exchanges.detect_exchange(address, chain, hypersync, t, now, height) -> bool`.
  - `entities.add_link(from_address, to_address, kind, evidence)`, `entities.follow(address, parent, chain, t)`, `entities.funder_is_service(funder, chain, t) -> bool`, `entities.refresh_entity(wallet) -> Entity`.
  - `qualification.PORTFOLIO_REASONS`, `link_step(profile, clients, now)`, `early_buys(wallet) -> list[EarlyBuy]`, `evaluate_entity(entity)`, `complete_step(profile, clients, now) -> str`, `qualify_wallet(profile, clients, now) -> str`.
  - `tests.factories.make_early_buy(wallet, chain, token_address, is_sniper=False, bought=100, sold=0) -> EarlyBuyer`.

- [ ] **Step 1: Fabrique d'early buy**

Ajouter à `backend/apps/wallets/tests/factories.py` :

```python
from apps.discovery.models import Candidate, EarlyBuyer, Explosion, Token
from apps.wallets.tests.fakes import NOW


def make_early_buy(wallet, chain, token_address, is_sniper=False, bought=100, sold=0) -> EarlyBuyer:
    token, _ = Token.objects.get_or_create(chain=chain, address=token_address)
    candidate = Candidate.objects.create(token=token, status="buyers_extracted")
    explosion = Explosion.objects.create(
        candidate=candidate, low_block=1, low_at=NOW, peak_block=2, peak_at=NOW,
        multiplier=10, retention_pct=50,
    )
    return EarlyBuyer.objects.create(
        explosion=explosion, wallet=wallet, first_buy_block=1, first_buy_at=NOW,
        bought_amount=bought, bought_usd=1000, sold_amount=sold, is_sniper=is_sniper,
    )
```

(Regrouper les imports en tête du fichier.)

- [ ] **Step 2: Écrire les tests**

`backend/apps/wallets/tests/test_qualification_entities.py` :

```python
from decimal import Decimal

import pytest

from apps.discovery.models import PipelineSettings, Wallet
from apps.discovery.tests.factories import make_chain
from apps.wallets.models import KnownAddress, QualificationSettings, WalletLink, WalletProfile
from apps.wallets.services.pricing import sync_quote_assets
from apps.wallets.services.qualification import Clients, qualify_wallet
from apps.wallets.tests.factories import make_early_buy
from apps.wallets.tests.fakes import (
    BUYER,
    DEPOSIT,
    FUNDER,
    HOT,
    NOW,
    TOKEN_A,
    UNIT,
    VAULT,
    FakeRpc,
    FakeWalletHyperSync,
    FakeZerion,
    buyer_history,
    deposit_history,
)
from integrations.errors import BudgetExhausted

pytestmark = pytest.mark.django_db


@pytest.fixture
def chain():
    chain = make_chain(native_fungible_id="eth", wrapped_fungible_id="0xweth-id", rpc_url="https://rpc.test/")
    sync_quote_assets(FakeZerion(), PipelineSettings.load())
    return chain


def profile_for(chain, address=BUYER):
    wallet, _ = Wallet.objects.get_or_create(address=address)
    profile, _ = WalletProfile.objects.get_or_create(wallet=wallet, defaults={"chains": [chain.pk]})
    return profile


def clients(hypersync=None, zerion=None):
    hs = hypersync or FakeWalletHyperSync()
    return Clients(
        hypersync_for=lambda c: hs,
        zerion=zerion or FakeZerion(),
        rpc_for=lambda c: FakeRpc({BUYER: UNIT, VAULT: 5 * UNIT}),
    )


def test_buying_wallet_is_judged_with_its_vault(chain):
    buyer = profile_for(chain)
    assert qualify_wallet(buyer, clients(), NOW) == "filtered"
    assert buyer.filter_reason == "portfolio_too_small"
    assert buyer.portfolio_value_usd == Decimal("6150.00")
    assert WalletLink.objects.filter(from_wallet__address=BUYER, to_wallet__address=VAULT, kind="transfer_after_buy").exists()
    assert WalletLink.objects.filter(from_wallet__address=FUNDER, to_wallet__address=BUYER, kind="funding").exists()
    vault = WalletProfile.objects.get(wallet__address=VAULT)
    assert (vault.source, vault.depth, vault.status) == ("linked", 1, "pending")

    assert qualify_wallet(vault, clients(), NOW) == "qualified"
    buyer.refresh_from_db()
    assert buyer.status == "qualified"
    assert buyer.entity_id == vault.entity_id
    assert buyer.entity.portfolio_value_usd == Decimal("17500.00")
    assert buyer.entity.profiles.count() == 3


def test_send_to_forwarding_deposit_creates_no_link(chain):
    hs = FakeWalletHyperSync(
        transfers={BUYER: buyer_history(send_to=DEPOSIT), DEPOSIT: deposit_history()},
        tx_counts={DEPOSIT: 3},
        counterparties={HOT: 5000},
    )
    qualify_wallet(profile_for(chain), clients(hs), NOW)
    assert not WalletLink.objects.filter(to_wallet__address=DEPOSIT).exists()
    assert KnownAddress.objects.get(address=DEPOSIT).kind == "cex_deposit"
    assert KnownAddress.objects.get(address=HOT).kind == "exchange"


def test_never_signing_relay_is_a_deposit(chain):
    hs = FakeWalletHyperSync(
        transfers={BUYER: buyer_history(send_to=DEPOSIT), DEPOSIT: deposit_history()},
        tx_counts={DEPOSIT: 0},
    )
    qualify_wallet(profile_for(chain), clients(hs), NOW)
    assert KnownAddress.objects.get(address=DEPOSIT).kind == "cex_deposit"
    assert not WalletLink.objects.filter(to_wallet__address=DEPOSIT).exists()


def test_hot_wallet_destination_is_not_followed(chain):
    hs = FakeWalletHyperSync(counterparties={VAULT: 5000})
    qualify_wallet(profile_for(chain), clients(hs), NOW)
    assert KnownAddress.objects.get(address=VAULT).kind == "exchange"
    assert not WalletProfile.objects.filter(wallet__address=VAULT).exists()


def test_massive_funder_is_a_service(chain):
    funder = Wallet.objects.create(address=FUNDER)
    for i in range(50):
        other = Wallet.objects.create(address=f"0x{i:040x}")
        WalletLink.objects.create(from_wallet=funder, to_wallet=other, kind="funding")
    qualify_wallet(profile_for(chain), clients(), NOW)
    assert not WalletLink.objects.filter(from_wallet=funder, to_wallet__address=BUYER).exists()
    assert KnownAddress.objects.get(address=FUNDER).kind == "service"


def test_follow_depth_zero_links_without_following(chain):
    QualificationSettings.objects.filter(chain=None).update(follow_depth=0)
    qualify_wallet(profile_for(chain), clients(), NOW)
    assert WalletLink.objects.filter(to_wallet__address=VAULT).exists()
    assert WalletProfile.objects.count() == 1


def test_early_buys_feed_tags(chain):
    buyer = profile_for(chain)
    make_early_buy(buyer.wallet, chain, TOKEN_A, is_sniper=True, bought=100, sold=10)
    qualify_wallet(buyer, clients(), NOW)
    assert buyer.tags == ["HOLDER", "SNIPER"]


def test_budget_exhaustion_keeps_history_fetched(chain):
    buyer = profile_for(chain)
    qualify_wallet(buyer, clients(), NOW)  # remplit le cache des prix natifs
    buyer.status = "history_fetched"
    buyer.save()
    with pytest.raises(BudgetExhausted):
        qualify_wallet(buyer, clients(zerion=FakeZerion(exhausted=True)), NOW)
    buyer.refresh_from_db()
    assert buyer.status == "history_fetched"


def test_qualification_is_idempotent(chain):
    buyer = profile_for(chain)
    qualify_wallet(buyer, clients(), NOW)
    links = WalletLink.objects.count()
    buyer.status = "history_fetched"
    buyer.save()
    qualify_wallet(buyer, clients(), NOW)
    assert WalletLink.objects.count() == links
```

- [ ] **Step 3: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/wallets/tests/test_qualification_entities.py -q"`
Expected: FAIL (`ImportError: cannot import name 'qualify_wallet'`).

- [ ] **Step 4: Détection des exchanges (partie base + HyperSync)**

Ajouter à `backend/apps/wallets/services/exchanges.py` (imports en tête) :

```python
from apps.discovery.services.blocks import find_block_at
from apps.wallets.models import KnownAddress

WEEK = 7 * 86_400


def register(address: str, chain, kind: str, label: str = "") -> None:
    KnownAddress.objects.get_or_create(
        chain=chain,
        address=address.lower(),
        defaults={"kind": kind, "label": label[:128], "source": KnownAddress.Source.AUTO},
    )


def _is_hot(address: str, hypersync, t, from_block: int, height: int) -> bool:
    count = hypersync.distinct_counterparties(
        address, from_block, height, cap=t.hot_wallet_min_counterparties
    )
    return count >= t.hot_wallet_min_counterparties


def detect_exchange(address: str, chain, hypersync, t, now, height: int) -> bool:
    """Exchange, dépôt d'exchange ou service ? Enregistre l'adresse au passage (source auto)."""
    address = address.lower()
    if KnownAddress.objects.blocking_for(address, [chain.pk]).exists():
        return True
    week = find_block_at(int(now.timestamp()) - WEEK, 0, height, hypersync.block_timestamp)
    if _is_hot(address, hypersync, t, week, height):
        register(address, chain, KnownAddress.Kind.EXCHANGE, "hot wallet (auto)")
        return True
    transfers = hypersync.wallet_transfers(address, week, height)
    never_signed = hypersync.wallet_tx_count(address, 0, height, cap=1) == 0
    if never_signed and any(x.sender == address for x in transfers):
        register(address, chain, KnownAddress.Kind.CEX_DEPOSIT, "relais qui ne signe jamais (auto)")
        return True
    share, destination = forwarding(transfers, address, t.deposit_forward_hours, t.deposit_forward_pct)
    if (
        destination
        and share >= t.deposit_forward_pct
        and (
            KnownAddress.objects.blocking_for(destination, [chain.pk]).exists()
            or _is_hot(destination, hypersync, t, week, height)
        )
    ):
        register(destination, chain, KnownAddress.Kind.EXCHANGE, "hot wallet (auto)")
        register(address, chain, KnownAddress.Kind.CEX_DEPOSIT, f"dépôt vers {destination} (auto)")
        return True
    return False
```

- [ ] **Step 5: Liens, suivi et entités (partie base)**

Ajouter à `backend/apps/wallets/services/entities.py` (imports en tête) :

```python
from django.db.models import Q

from apps.discovery.models import Wallet
from apps.wallets.models import BLOCKING_KINDS, Entity, KnownAddress, WalletLink, WalletProfile
from apps.wallets.services.exchanges import register


def add_link(from_address: str, to_address: str, kind: str, evidence: dict) -> None:
    source, _ = Wallet.objects.get_or_create(address=from_address.lower())
    target, _ = Wallet.objects.get_or_create(address=to_address.lower())
    WalletLink.objects.get_or_create(
        from_wallet=source, to_wallet=target, kind=kind, defaults={"evidence": evidence}
    )


def follow(address: str, parent: WalletProfile, chain, t) -> None:
    if parent.depth + 1 > t.follow_depth:
        return
    wallet, _ = Wallet.objects.get_or_create(address=address.lower())
    profile, created = WalletProfile.objects.get_or_create(
        wallet=wallet,
        defaults={"source": WalletProfile.Source.LINKED, "depth": parent.depth + 1, "chains": [chain.pk]},
    )
    if not created and chain.pk not in profile.chains:
        profile.chains = [*profile.chains, chain.pk]
        profile.save(update_fields=["chains"])


def funder_is_service(funder: str, chain, t) -> bool:
    funded = WalletLink.objects.filter(
        from_wallet__address=funder, kind=WalletLink.Kind.FUNDING
    ).count()
    if funded >= t.funder_max_wallets:
        register(funder, chain, KnownAddress.Kind.SERVICE, "financeur massif (auto)")
        return True
    return False


def refresh_entity(wallet) -> Entity:
    """Regroupe les wallets reliés à `wallet` (liens hors adresses bloquantes) dans une entité."""
    blocked = KnownAddress.objects.filter(kind__in=BLOCKING_KINDS).values("address")
    links = WalletLink.objects.exclude(from_wallet__address__in=blocked).exclude(
        to_wallet__address__in=blocked
    )
    members = {wallet.pk}
    frontier = {wallet.pk}
    while frontier:
        pairs = links.filter(
            Q(from_wallet_id__in=frontier) | Q(to_wallet_id__in=frontier)
        ).values_list("from_wallet_id", "to_wallet_id")
        reached = {wallet_id for pair in pairs for wallet_id in pair}
        frontier = reached - members
        members |= reached
    profiles = WalletProfile.objects.filter(wallet_id__in=members)
    entity_ids = sorted({pk for pk in profiles.values_list("entity_id", flat=True) if pk})
    entity = Entity.objects.get(pk=entity_ids[0]) if entity_ids else Entity.objects.create()
    profiles.update(entity=entity)
    Entity.objects.filter(pk__in=entity_ids[1:]).delete()
    return entity
```

- [ ] **Step 6: Étapes finales de la qualification**

Ajouter à `backend/apps/wallets/services/qualification.py` (imports regroupés en tête) :

```python
from apps.discovery.models import EarlyBuyer
from apps.wallets.models import Entity, WalletLink
from apps.wallets.services.entities import (
    add_link,
    follow,
    funder_is_service,
    refresh_entity,
    transfer_after_buy_targets,
)
from apps.wallets.services.exchanges import detect_exchange
from apps.wallets.services.pricing import wallet_value
from apps.wallets.services.tags import EarlyBuy, entity_tags, wallet_tags

PORTFOLIO_REASONS = ("portfolio_too_small", "portfolio_too_large")


def link_step(profile: WalletProfile, clients: Clients, now: datetime) -> None:
    t = qualification_thresholds()
    wallet = profile.wallet
    chains = {chain.pk: chain for chain in profile_chains(profile)}
    contexts: dict[int, tuple] = {}

    def context(chain):
        if chain.pk not in contexts:
            hypersync = clients.hypersync_for(chain)
            contexts[chain.pk] = (hypersync, hypersync.height())
        return contexts[chain.pk]

    targets = transfer_after_buy_targets(records_for(wallet), t.transfer_after_buy_pct)
    for (chain_id, destination), evidence in targets.items():
        chain = chains.get(chain_id)
        if chain is None:
            continue
        hypersync, height = context(chain)
        if detect_exchange(destination, chain, hypersync, t, now, height):
            continue
        add_link(wallet.address, destination, WalletLink.Kind.TRANSFER_AFTER_BUY, {**evidence, "chain": chain.gt_id})
        follow(destination, profile, chain, t)

    for chain in chains.values():
        hypersync, height = context(chain)
        funding = hypersync.first_funding(wallet.address, height)
        if funding is None:
            continue
        if funder_is_service(funding.funder, chain, t) or detect_exchange(
            funding.funder, chain, hypersync, t, now, height
        ):
            continue
        add_link(
            funding.funder,
            wallet.address,
            WalletLink.Kind.FUNDING,
            {"chain": chain.gt_id, "block": funding.block, "value": str(funding.value)},
        )
        follow(funding.funder, profile, chain, t)


def early_buys(wallet) -> list[EarlyBuy]:
    return [
        EarlyBuy(
            token=e.explosion.candidate.token.address,
            chain_id=e.explosion.candidate.token.chain_id,
            is_sniper=e.is_sniper,
            bought=int(e.bought_amount),
            sold_before_peak=int(e.sold_amount),
        )
        for e in EarlyBuyer.objects.filter(wallet=wallet).select_related("explosion__candidate__token")
    ]


def evaluate_entity(entity: Entity) -> None:
    """Valeur et tags de l'entité, puis statut de ses wallets déjà valorisés."""
    t = qualification_thresholds()
    profiles = list(entity.profiles.all())
    value = sum(float(p.portfolio_value_usd) for p in profiles if p.portfolio_value_usd is not None)
    members = {p.wallet_id for p in profiles}
    explosive = list(
        EarlyBuyer.objects.filter(wallet_id__in=members).values_list(
            "explosion__candidate__token__address", flat=True
        )
    )
    internal_holding = WalletLink.objects.filter(
        kind=WalletLink.Kind.TRANSFER_AFTER_BUY,
        from_wallet_id__in=members,
        to_wallet_id__in=members,
        evidence__token__in=explosive,
    ).exists()
    entity.portfolio_value_usd = Decimal(str(round(value, 2)))
    entity.tags = entity_tags([p.tags for p in profiles], internal_holding)
    entity.save()

    if value < t.min_portfolio_usd:
        status, reason = Status.FILTERED, "portfolio_too_small"
    elif value > t.max_portfolio_usd:
        status, reason = Status.FILTERED, "portfolio_too_large"
    else:
        status, reason = Status.QUALIFIED, ""
    for p in profiles:
        judged = p.portfolio_value_usd is not None and (
            p.status in (Status.HISTORY_FETCHED, Status.QUALIFIED)
            or (p.status == Status.FILTERED and p.filter_reason in PORTFOLIO_REASONS)
        )
        if judged and (p.status, p.filter_reason) != (status, reason):
            p.status = status
            p.filter_reason = reason
            p.save(update_fields=["status", "filter_reason"])


def complete_step(profile: WalletProfile, clients: Clients, now: datetime) -> str:
    link_step(profile, clients, now)
    chains = profile_chains(profile)
    total, details = wallet_value(profile.wallet, chains, clients.zerion, clients.rpc_for, now)
    profile.portfolio_value_usd = Decimal(str(total))
    profile.metrics = {**profile.metrics, "value": details}
    profile.tags = wallet_tags(
        records_for(profile.wallet), early_buys(profile.wallet), quote_tokens(chains), qualification_thresholds()
    )
    profile.analyzed_at = now
    profile.attempts = 0
    profile.save()
    evaluate_entity(refresh_entity(profile.wallet))
    profile.refresh_from_db()
    return profile.status


def qualify_wallet(profile: WalletProfile, clients: Clients, now: datetime) -> str:
    if profile.status == Status.PENDING:
        prefilter_step(profile, clients, now)
    if profile.status == Status.PREFILTERED:
        history_step(profile, clients, now)
    if profile.status == Status.HISTORY_FETCHED:
        complete_step(profile, clients, now)
    return profile.status
```

- [ ] **Step 7: Lancer les tests**

Run: `make test args="apps/wallets -q"`
Expected: PASS.

- [ ] **Step 8: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend/apps/wallets
git commit -m "feat(wallets): exchanges, entités, valeur d'entité, tags et qualification complète

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: Tâches Celery, file d'attente et budget

**Files:**
- Modify: `backend/apps/wallets/services/qualification.py` (ajout `enqueue_profiles`)
- Modify: `backend/apps/wallets/tasks.py`
- Create: `backend/apps/wallets/tests/test_tasks.py`

**Interfaces:**
- Consumes: `qualify_wallet`, `filter_out`, `Clients` (Tasks 9-10), `sync_quote_assets` (Task 7), `clients.zerion`, `clients.hypersync`, `clients.rpc`.
- Produces:
  - `qualification.enqueue_profiles(now, cfg) -> int` (nouveaux profils ; remet en `pending` les wallets filtrés dont le délai est passé et qui ont une nouvelle explosion).
  - `tasks.build_clients(cfg) -> Clients`, `tasks.record_failure(profile, exc, max_attempts, now)`, `tasks.qualify_wallets_task() -> dict`, `tasks.qualify_wallet_task(profile_id) -> str`.

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
from apps.wallets.services.qualification import enqueue_profiles
from apps.wallets.tests.factories import make_early_buy
from apps.wallets.tests.fakes import BUYER, NOW, TOKEN_A
from integrations.errors import BudgetExhausted

pytestmark = pytest.mark.django_db


@pytest.fixture
def chain():
    return make_chain()


def test_enqueue_creates_profiles_with_chains(chain):
    extra = make_chain(gt_id="eth", evm_id=1, zerion_id="ethereum")
    cfg = PipelineSettings.load()
    cfg.extra_chains = ["eth"]
    cfg.save()
    make_early_buy(Wallet.objects.create(address=BUYER), chain, TOKEN_A)
    assert enqueue_profiles(NOW, cfg) == 1
    profile = WalletProfile.objects.get()
    assert sorted(profile.chains) == sorted([chain.pk, extra.pk])
    assert enqueue_profiles(NOW, cfg) == 0


def test_enqueue_reopens_filtered_wallet_after_delay_with_new_explosion(chain):
    wallet = Wallet.objects.create(address=BUYER)
    profile = WalletProfile.objects.create(
        wallet=wallet, chains=[chain.pk], status="filtered", filter_reason="inactive",
        analyzed_at=NOW - timedelta(days=40), next_analysis_at=NOW - timedelta(days=10),
    )
    make_early_buy(wallet, chain, TOKEN_A)
    enqueue_profiles(NOW, PipelineSettings.load())
    profile.refresh_from_db()
    assert (profile.status, profile.filter_reason) == ("pending", "")


def test_qualify_wallets_task_schedules_batch(chain):
    for i in range(3):
        WalletProfile.objects.create(wallet=Wallet.objects.create(address=f"0x{i:040x}"), chains=[chain.pk])
    cfg = PipelineSettings.load()
    cfg.qualification_batch_size = 2
    cfg.save()
    with (
        patch.object(tasks, "sync_quote_assets", side_effect=BudgetExhausted("fini")),
        patch.object(tasks.clients, "zerion"),
        patch.object(tasks.qualify_wallet_task, "delay") as delay,
    ):
        result = tasks.qualify_wallets_task.apply().get()
    assert result == {"new_profiles": 0, "scheduled": 2}
    assert delay.call_count == 2


def test_failure_counts_attempts_then_filters(chain):
    profile = WalletProfile.objects.create(wallet=Wallet.objects.create(address=BUYER), chains=[chain.pk])
    cfg = PipelineSettings.load()
    cfg.max_attempts = 2
    cfg.save()
    with patch.object(tasks, "build_clients"), patch.object(tasks, "qualify_wallet", side_effect=RuntimeError("x")):
        tasks.qualify_wallet_task.apply(args=[profile.pk])
        profile.refresh_from_db()
        assert (profile.status, profile.attempts) == ("pending", 1)
        tasks.qualify_wallet_task.apply(args=[profile.pk])
    profile.refresh_from_db()
    assert (profile.status, profile.filter_reason) == ("filtered", "error:RuntimeError")


def test_budget_exhaustion_is_not_a_failure(chain):
    profile = WalletProfile.objects.create(wallet=Wallet.objects.create(address=BUYER), chains=[chain.pk])
    with patch.object(tasks, "build_clients"), patch.object(tasks, "qualify_wallet", side_effect=BudgetExhausted("x")):
        tasks.qualify_wallet_task.apply(args=[profile.pk])
    profile.refresh_from_db()
    assert (profile.status, profile.attempts) == ("pending", 0)
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/wallets/tests/test_tasks.py -q"`
Expected: FAIL.

- [ ] **Step 3: Implémenter `enqueue_profiles`**

Ajouter à `backend/apps/wallets/services/qualification.py` (et `from collections import defaultdict` en tête) :

```python
def enqueue_profiles(now: datetime, cfg) -> int:
    """Crée les profils des nouveaux early buyers ; rouvre les filtrés ayant une nouvelle explosion."""
    extra = set(Chain.objects.active().filter(gt_id__in=cfg.extra_chains).values_list("pk", flat=True))
    by_wallet: dict[int, set[int]] = defaultdict(set)
    rows = EarlyBuyer.objects.filter(wallet__profile__isnull=True).values_list(
        "wallet_id", "explosion__candidate__token__chain_id"
    )
    for wallet_id, chain_id in rows:
        by_wallet[wallet_id].add(chain_id)
    created = 0
    for wallet_id, chain_ids in by_wallet.items():
        _, was_created = WalletProfile.objects.get_or_create(
            wallet_id=wallet_id, defaults={"chains": sorted(chain_ids | extra)}
        )
        created += int(was_created)

    due = WalletProfile.objects.filter(
        status=Status.FILTERED, source=WalletProfile.Source.EARLY_BUYER, next_analysis_at__lte=now
    )
    for profile in due:
        fresh = set(
            EarlyBuyer.objects.filter(
                wallet_id=profile.wallet_id, explosion__candidate__updated_at__gt=profile.analyzed_at
            ).values_list("explosion__candidate__token__chain_id", flat=True)
        )
        if fresh:
            profile.status = Status.PENDING
            profile.filter_reason = ""
            profile.attempts = 0
            profile.chains = sorted(set(profile.chains) | fresh | extra)
            profile.save()
    return created
```

- [ ] **Step 4: Implémenter les tâches**

Remplacer `backend/apps/wallets/tasks.py` par :

```python
"""Tâches Celery de la qualification. Planning : tâche périodique `qualification-daily` (admin)."""

import logging

from celery import shared_task
from django.utils import timezone

from apps.discovery.models import PipelineSettings
from apps.discovery.services import clients
from apps.wallets.models import WalletProfile
from apps.wallets.services.pricing import sync_quote_assets
from apps.wallets.services.qualification import (
    Clients,
    enqueue_profiles,
    filter_out,
    qualify_wallet,
)
from integrations.errors import BudgetExhausted

logger = logging.getLogger(__name__)
IN_PROGRESS = (
    WalletProfile.Status.PENDING,
    WalletProfile.Status.PREFILTERED,
    WalletProfile.Status.HISTORY_FETCHED,
)


def build_clients(cfg: PipelineSettings) -> Clients:
    return Clients(
        hypersync_for=lambda chain: clients.hypersync(chain, cfg),
        zerion=clients.zerion(cfg),
        rpc_for=lambda chain: clients.rpc(chain, cfg),
    )


def record_failure(profile: WalletProfile, exc: Exception, max_attempts: int, now) -> None:
    logger.exception("Échec de qualification du wallet %s", profile.wallet_id, exc_info=exc)
    profile.attempts += 1
    if profile.attempts >= max_attempts:
        filter_out(profile, f"error:{type(exc).__name__}"[:64], now)
    else:
        profile.save(update_fields=["attempts"])


@shared_task
def qualify_wallets_task() -> dict:
    cfg = PipelineSettings.load()
    now = timezone.now()
    try:
        sync_quote_assets(clients.zerion(cfg), cfg)
    except BudgetExhausted:
        logger.warning("Budget Zerion épuisé : actifs de cotation non rafraîchis")
    created = enqueue_profiles(now, cfg)
    ids = list(
        WalletProfile.objects.filter(status__in=IN_PROGRESS)
        .order_by("depth", "id")
        .values_list("id", flat=True)[: cfg.qualification_batch_size]
    )
    for profile_id in ids:
        qualify_wallet_task.delay(profile_id)
    logger.info("%s nouveaux profils, %s wallets programmés", created, len(ids))
    return {"new_profiles": created, "scheduled": len(ids)}


@shared_task
def qualify_wallet_task(profile_id: int) -> str:
    cfg = PipelineSettings.load()
    now = timezone.now()
    profile = WalletProfile.objects.select_related("wallet").get(pk=profile_id)
    if profile.status not in IN_PROGRESS:
        return profile.status
    try:
        return qualify_wallet(profile, build_clients(cfg), now)
    except BudgetExhausted:
        logger.info("Budget Zerion épuisé : wallet %s reporté au prochain passage", profile_id)
        return profile.status
    except Exception as exc:
        record_failure(profile, exc, cfg.max_attempts, now)
        return profile.status
```

- [ ] **Step 5: Lancer les tests**

Run: `make test args="-q"`
Expected: PASS.

- [ ] **Step 6: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend/apps/wallets
git commit -m "feat(wallets): tâches Celery, file d'attente des profils et respect du budget Zerion

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 12: Admin et import CSV d'adresses connues

**Files:**
- Modify: `backend/apps/wallets/admin.py`, `backend/apps/wallets/services/exchanges.py`
- Create: `backend/apps/wallets/forms.py`
- Create: `backend/apps/wallets/templates/admin/wallets/knownaddress/change_list.html`, `import_csv.html`
- Create: `backend/apps/wallets/tests/test_admin.py`

**Interfaces:**
- Produces: `exchanges.import_known_addresses(rows: list[dict]) -> int` (colonnes `address`, `kind`, `label`, `chain` = gt_id ou vide pour toutes les chaînes ; lignes invalides ignorées) ; URL `admin:wallets_knownaddress_import`.

- [ ] **Step 1: Écrire les tests**

`backend/apps/wallets/tests/test_admin.py` :

```python
import pytest
from django.core.files.uploadedfile import SimpleUploadedFile
from django.urls import reverse

from apps.discovery.tests.factories import make_chain
from apps.wallets.models import KnownAddress
from apps.wallets.services.exchanges import import_known_addresses

pytestmark = pytest.mark.django_db


@pytest.fixture
def admin_client(client, django_user_model):
    client.force_login(django_user_model.objects.create_superuser("admin", "a@example.com", "pw"))
    return client


@pytest.mark.parametrize(
    "model",
    ["walletprofile", "entity", "tokenposition", "tokentrade", "walletlink", "knownaddress", "dailyprice", "qualificationsettings"],
)
def test_changelists_load(admin_client, model):
    assert admin_client.get(reverse(f"admin:wallets_{model}_changelist")).status_code == 200


def test_import_known_addresses_skips_invalid_rows():
    make_chain()
    count = import_known_addresses(
        [
            {"address": "0xABC" + "0" * 37, "kind": "exchange", "label": "Binance", "chain": ""},
            {"address": "0x" + "1" * 40, "kind": "bridge", "label": "", "chain": "base"},
            {"address": "pas-une-adresse", "kind": "exchange"},
            {"address": "0x" + "2" * 40, "kind": "inconnu"},
            {"address": "0x" + "3" * 40, "kind": "exchange", "chain": "chaine-inconnue"},
        ]
    )
    assert count == 2
    assert KnownAddress.objects.get(address="0xabc" + "0" * 37).chain is None


def test_import_view(admin_client):
    url = reverse("admin:wallets_knownaddress_import")
    assert admin_client.get(url).status_code == 200
    csv = SimpleUploadedFile("a.csv", b"address,kind,label,chain\n0x" + b"4" * 40 + b",exchange,OKX,\n")
    response = admin_client.post(url, {"file": csv})
    assert response.status_code == 302
    assert KnownAddress.objects.get(address="0x" + "4" * 40).source == "import"
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/wallets/tests/test_admin.py -q"`
Expected: FAIL.

- [ ] **Step 3: Implémenter l'import**

Ajouter à `backend/apps/wallets/services/exchanges.py` (et `from apps.discovery.models import Chain` en tête) :

```python
def import_known_addresses(rows: list[dict]) -> int:
    """Import de listes d'adresses (exchanges, bridges…). Chaîne vide = toutes les chaînes."""
    chains = {chain.gt_id: chain for chain in Chain.objects.all()}
    imported = 0
    for row in rows:
        address = (row.get("address") or "").strip().lower()
        kind = (row.get("kind") or "").strip()
        chain_id = (row.get("chain") or "").strip()
        if not address.startswith("0x") or len(address) != 42 or kind not in KnownAddress.Kind.values:
            continue
        if chain_id and chain_id not in chains:
            continue
        KnownAddress.objects.update_or_create(
            chain=chains.get(chain_id),
            address=address,
            defaults={
                "kind": kind,
                "label": (row.get("label") or "").strip()[:128],
                "source": KnownAddress.Source.IMPORT,
            },
        )
        imported += 1
    return imported
```

- [ ] **Step 4: Formulaire, templates et admin**

`backend/apps/wallets/forms.py` :

```python
"""Formulaires de l'admin de la qualification."""

from django import forms


class KnownAddressImportForm(forms.Form):
    file = forms.FileField(label="Fichier CSV (colonnes : address, kind, label, chain)")
```

`backend/apps/wallets/templates/admin/wallets/knownaddress/change_list.html` :

```html
{% extends "admin/change_list.html" %}
{% block object-tools-items %}
  <li><a href="{% url 'admin:wallets_knownaddress_import' %}">Importer un CSV</a></li>
  {{ block.super }}
{% endblock %}
```

`backend/apps/wallets/templates/admin/wallets/knownaddress/import_csv.html` :

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

`backend/apps/wallets/admin.py` :

```python
"""Admin de la qualification : profils, entités, historique, adresses connues, réglages."""

import csv
import io

from django.contrib import admin, messages
from django.shortcuts import redirect
from django.template.response import TemplateResponse
from django.urls import path, reverse

from apps.wallets.forms import KnownAddressImportForm
from apps.wallets.models import (
    DailyPrice,
    Entity,
    KnownAddress,
    QualificationSettings,
    TokenPosition,
    TokenTrade,
    WalletLink,
    WalletProfile,
)
from apps.wallets.services.exchanges import import_known_addresses


class ReadOnlyAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(WalletProfile)
class WalletProfileAdmin(ReadOnlyAdmin):
    list_display = ["wallet", "status", "filter_reason", "source", "depth", "entity", "portfolio_value_usd", "tags"]
    list_filter = ["status", "filter_reason", "source"]
    search_fields = ["wallet__address"]
    list_select_related = ["wallet", "entity"]


class ProfileInline(admin.TabularInline):
    model = WalletProfile
    fields = ["wallet", "source", "status", "filter_reason", "portfolio_value_usd", "tags"]
    readonly_fields = fields
    extra = 0
    can_delete = False

    def has_add_permission(self, request, obj=None):
        return False


@admin.register(Entity)
class EntityAdmin(ReadOnlyAdmin):
    list_display = ["__str__", "portfolio_value_usd", "tags", "updated_at"]
    inlines = [ProfileInline]


@admin.register(TokenPosition)
class TokenPositionAdmin(ReadOnlyAdmin):
    list_display = ["wallet", "token", "buys", "sells", "bought_usd", "sold_usd", "first_at", "last_at"]
    list_filter = ["token__chain"]
    search_fields = ["wallet__address", "token__address", "token__symbol"]
    list_select_related = ["wallet", "token__chain"]


@admin.register(TokenTrade)
class TokenTradeAdmin(ReadOnlyAdmin):
    list_display = ["at", "wallet", "token", "kind", "amount", "usd", "counterparty"]
    list_filter = ["kind"]
    search_fields = ["wallet__address", "tx_hash", "token__address"]
    ordering = ["-at"]
    list_select_related = ["wallet", "token__chain"]


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
        custom = [
            path("import/", self.admin_site.admin_view(self.import_view), name="wallets_knownaddress_import")
        ]
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


@admin.register(DailyPrice)
class DailyPriceAdmin(ReadOnlyAdmin):
    list_display = ["fungible_id", "day", "usd"]
    list_filter = ["fungible_id"]
    ordering = ["-day"]


@admin.register(QualificationSettings)
class QualificationSettingsAdmin(admin.ModelAdmin):
    list_display = ["__str__", "min_portfolio_usd", "history_days", "follow_depth"]
```

- [ ] **Step 5: Lancer les tests**

Run: `make test args="apps/wallets -q"`
Expected: PASS.

- [ ] **Step 6: Lint et commit**

```bash
docker compose exec web ruff format . && make lint
git add backend/apps/wallets
git commit -m "feat(wallets): admin de la qualification et import CSV d'adresses connues

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 13: Tests live, documentation et essai réel

**Files:**
- Create: `backend/integrations/tests/test_live_wallets.py`
- Modify: `README.md`

- [ ] **Step 1: Écrire les tests live**

`backend/integrations/tests/test_live_wallets.py` :

```python
"""Tests contre les vraies API (quelques appels Zerion). Lancer : make test args="-m live integrations"."""

import os
import time

import pytest

from integrations.http import JsonHttpClient
from integrations.hypersync import HyperSyncClient
from integrations.ratelimit import NoopLimiter
from integrations.rpc import RpcClient
from integrations.zerion import BASE_URL, ZerionClient

pytestmark = pytest.mark.live

USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
ROBINHOOD_WALLET = "0x3296219c6167893ca763c6d515df6a542e5cc395"


@pytest.mark.skipif(not os.environ.get("ZERION_API_KEY"), reason="ZERION_API_KEY absente")
def test_zerion_prices_search_and_chart():
    client = ZerionClient(JsonHttpClient(BASE_URL, limiter=NoopLimiter(), auth=(os.environ["ZERION_API_KEY"], "")))
    price = client.prices([("base", USDC_BASE)])[("base", USDC_BASE)]
    assert 0.9 < price.price < 1.1
    time.sleep(1.5)
    assert client.search("USDC").implementations["base"][0] == USDC_BASE
    time.sleep(1.5)
    assert len(client.price_chart("eth")) > 300


def test_public_rpc_native_balance():
    rpc = RpcClient(JsonHttpClient("https://mainnet.base.org/", limiter=NoopLimiter()))
    assert rpc.native_balance("0x4200000000000000000000000000000000000006") >= 0


@pytest.mark.skipif(not os.environ.get("ENVIO_API_TOKEN"), reason="ENVIO_API_TOKEN absent")
def test_hypersync_wallet_queries_on_robinhood():
    client = HyperSyncClient(4663, os.environ["ENVIO_API_TOKEN"], NoopLimiter())
    height = client.height()
    assert client.wallet_transfers(ROBINHOOD_WALLET, 0, height)
    assert client.wallet_tx_count(ROBINHOOD_WALLET, 0, height, cap=5) == 5
    assert client.first_funding(ROBINHOOD_WALLET, height) is not None
```

- [ ] **Step 2: Lancer les tests live**

Run: `make test args="-m live integrations -v"`
Expected: PASS (ou SKIPPED si une clé manque). En cas d'échec, corriger le client concerné : le format réel prime.

- [ ] **Step 3: Documenter**

Ajouter à la fin de `README.md` :

```markdown
## Qualification des wallets

Chaque jour à 08:00 UTC (tâche `qualification-daily`), après la découverte, chaque early buyer est qualifié en 5 étapes :

1. **Pré-filtre (HyperSync)** : bots (fréquence), wallets inactifs, exchanges connus ;
2. **Historique par token (HyperSync)** : achats, ventes, envois, réceptions sur `history_days`, valorisés par la contrepartie de chaque transaction ; farmers et bots MEV écartés ;
3. **Entités** : transfert après achat, financement initial, financeur commun — les exchanges et dépôts d'exchange ne relient personne ;
4. **Valeur de l'entité** (prix Zerion + solde natif RPC) : entre `min_portfolio_usd` et `max_portfolio_usd` ;
5. **Tags** : SNIPER, EARLY_BUYER, ACCUMULATEUR, FLIPPER, HOLDER.

Zerion ne sert qu'aux prix, sous le budget quotidien `zerion_daily_budget` (Réglages du pipeline). Les seuils sont dans **Réglages de qualification** (globaux + par chaîne) ; les listes d'exchanges s'importent dans **Known addresses → Importer un CSV**.
```

- [ ] **Step 4: Vérification complète**

```bash
make test
make lint
docker compose restart worker beat
```

Expected : tous les tests passent ; lint propre ; le worker liste `apps.wallets.tasks.qualify_wallets_task` et `qualify_wallet_task`.

- [ ] **Step 5: Essai réel limité**

Régler `qualification_batch_size` à 20 dans l'admin (Réglages du pipeline), puis :

```bash
docker compose exec web python manage.py shell -c "
from apps.wallets.tasks import qualify_wallets_task, qualify_wallet_task
from apps.wallets.models import WalletProfile
print(qualify_wallets_task.apply().get())
for p in WalletProfile.objects.filter(status__in=['pending','prefiltered','history_fetched'])[:20]:
    print(p.wallet.address, qualify_wallet_task.apply(args=[p.pk]).get(), p.filter_reason)
"
```

Expected : des wallets `filtered` avec des raisons variées (`bot_frequency`, `inactive`, `too_few_trades`, `portfolio_too_small`…) et, si le lot en contient, des `qualified`. Vérifier dans l'admin quelques entités et le compteur `ratelimit-org-day-remaining` de Zerion (le budget quotidien n'est pas dépassé).

- [ ] **Step 6: Commit**

```bash
git add backend/integrations/tests/test_live_wallets.py README.md
git commit -m "docs(wallets): tests live et documentation de la qualification

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
