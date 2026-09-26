# Découverte multi-chaînes — Plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Chaque jour, trouver les tokens réellement explosifs sur toutes les chaînes EVM supportées (GeckoTerminal) et enregistrer leurs early buyers EOA significatifs (HyperSync), sans aucune chaîne ni aucun paramètre codé en dur.

**Architecture:** Un package `backend/integrations/` contient les clients API sans logique métier (GeckoTerminal, CoinGecko, Zerion, HyperSync) sur un client HTTP commun avec retries et rate limit Redis. L'app Django `backend/apps/discovery/` contient les modèles, les réglages éditables dans l'admin, des fonctions de calcul pures (explosion, recherche de bloc, agrégation des acheteurs), les services qui orchestrent, et les tâches Celery planifiées par `django-celery-beat`.

**Tech Stack:** Python 3.12, Django 5.2, Celery 5.5, PostgreSQL 16, Redis 7, httpx 0.28, hypersync 1.2 (client Python Envio), django-celery-beat 2.9, respx 0.23 (tests), pytest-django, ruff.

**Spec:** `docs/superpowers/specs/2026-09-26-discovery-multichaine-design.md`

## Global Constraints

- Aucune chaîne codée en dur : les chaînes viennent de `sync_chains`.
- Aucun paramètre réglable codé en dur : seuils dans `DetectionSettings` (ligne globale + surcharge par chaîne), paramètres du pipeline dans `PipelineSettings` (singleton), planning dans `django-celery-beat`. Seules les constantes de protocole restent dans le code (URLs d'API, `TRANSFER_TOPIC`, limite de 1 000 bougies GeckoTerminal).
- Secrets uniquement dans les variables d'environnement : `ENVIO_API_TOKEN`, `ZERION_API_KEY`, `COINGECKO_API_KEY` (optionnelle).
- Adresses stockées en minuscules.
- Montants on-chain bruts en `DecimalField(max_digits=78, decimal_places=0)`.
- Acheteur = signataire de la transaction (`tx.from`) qui reçoit les tokens → EOA uniquement. Aucun appel `eth_getCode`.
- `min_buy_usd` puis `max_buyers` (0 = pas de plafond), dans cet ordre.
- Les clients `integrations/` ne lisent jamais la base : ils reçoivent leurs réglages en paramètre.
- Les tests n'appellent jamais le réseau, sauf ceux marqués `@pytest.mark.live` (exclus par défaut).
- Toutes les commandes s'exécutent dans le conteneur `web` via le `Makefile` (`make test args="..."`, `make lint`).
- Convention du socle : chaque app a `apps.py` (`name = "apps.<app>"`), `models.py`, `serializers.py`, `views.py`, `urls.py`, `tasks.py`, `admin.py`, `migrations/`, `tests/`.
- Chaque commit se termine par la ligne `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Écarts assumés avec la spec

1. **`Chain.chain_id` s'appelle `evm_id`.** Sinon `token.chain_id` (clé étrangère Django) et `token.chain.chain_id` (identifiant EVM) se confondent : source de bugs garantie.
2. **`Chain.zerion_id` vaut `""` quand Zerion ne supporte pas la chaîne** (au lieu de `null`) : règle ruff `DJ001` (pas de `null=True` sur un `CharField`).
3. **`services.py` devient un package `services/`** (un module par responsabilité), comme le prévoit la spec, au lieu du fichier unique du template.
4. **Ajout de `PipelineSettings.candidate_cooldown_hours`** (défaut 72) : un token dont un candidat a été clos (rejeté ou extrait) il y a moins de ce délai n'est pas re-candidaté. Sans ça, un token encore en tendance le lendemain serait ré-analysé chaque jour. Ajout des raisons `already_extracted` (le pic détecté n'est pas plus récent que la dernière explosion extraite du token) et `no_pool`.
5. **Pas de raison `prefilter` en base** : un pool qui échoue aux pré-filtres ne devient jamais candidat, il n'y a donc rien à rejeter.
6. **Cache dates → blocs en mémoire, pas dans Redis** : la recherche par interpolation converge en quelques appels, un cache Redis n'apporte rien (YAGNI).
7. **`PipelineSettings` n'est pas créé par la data migration** : `PipelineSettings.load()` le crée avec les valeurs par défaut du modèle au premier accès.

## Carte des fichiers

| Fichier | Responsabilité |
|---|---|
| `backend/pyproject.toml`, `backend/uv.lock` | Nouvelles dépendances, marker `live`, `integrations` dans `testpaths` |
| `backend/config/settings/base.py` | `django_celery_beat`, `apps.discovery`, scheduler base de données, clés API |
| `docker-compose.yml` | Commande `beat` avec le scheduler base de données |
| `.env.example` | `ENVIO_API_TOKEN`, `ZERION_API_KEY`, `COINGECKO_API_KEY` |
| `backend/integrations/errors.py` | Erreurs typées des services externes |
| `backend/integrations/ratelimit.py` | `RateLimiter` (Redis, fenêtre d'une minute) et `NoopLimiter` |
| `backend/integrations/http.py` | `JsonHttpClient` : timeout, retries, backoff, rate limit |
| `backend/integrations/geckoterminal.py` | Réseaux, pools (trending, volume, par token), token, OHLCV |
| `backend/integrations/coingecko.py` | `asset_platforms` → `chain_id` |
| `backend/integrations/zerion.py` | Chaînes supportées par Zerion |
| `backend/integrations/hypersync.py` | Chaînes supportées, hauteur, timestamp de bloc, transferts ERC-20 |
| `backend/apps/discovery/models.py` | Les 9 modèles |
| `backend/apps/discovery/migrations/0001_initial.py` | Générée |
| `backend/apps/discovery/migrations/0002_defaults.py` | Réglages globaux + tâche périodique quotidienne |
| `backend/apps/discovery/services/settings.py` | `Thresholds`, `thresholds_for(chain)` |
| `backend/apps/discovery/services/explosion.py` | Pur : résolution OHLCV, meilleure hausse, verdict |
| `backend/apps/discovery/services/blocks.py` | Pur : premier bloc à un timestamp donné |
| `backend/apps/discovery/services/buyers.py` | Pur : agrégation des acheteurs |
| `backend/apps/discovery/services/clients.py` | Construction des clients depuis `PipelineSettings` + env |
| `backend/apps/discovery/services/chains.py` | `sync_chains` |
| `backend/apps/discovery/services/candidates.py` | Pré-filtres, collecte, ajout manuel |
| `backend/apps/discovery/services/analysis.py` | Analyse d'un candidat |
| `backend/apps/discovery/services/extraction.py` | Extraction des acheteurs d'un candidat |
| `backend/apps/discovery/tasks.py` | Tâches Celery + gestion des échecs |
| `backend/apps/discovery/admin.py`, `forms.py`, `templates/admin/discovery/candidate/*.html` | Admin + ajout manuel |
| `backend/apps/discovery/tests/fakes.py` | Faux clients partagés par les tests |

---

### Task 1: Dépendances, settings et scheduler Beat en base

**Files:**
- Modify: `backend/pyproject.toml`, `backend/uv.lock` (via `uv add`)
- Modify: `backend/config/settings/base.py`
- Modify: `docker-compose.yml` (service `beat`)
- Modify: `.env.example`
- Create: `backend/tests/test_settings_discovery.py`

**Interfaces:**
- Produces: `settings.ENVIO_API_TOKEN: str`, `settings.ZERION_API_KEY: str`, `settings.COINGECKO_API_KEY: str` (chaînes vides si absentes) ; `settings.CELERY_BEAT_SCHEDULER`.

- [ ] **Step 1: Ajouter les dépendances**

```bash
docker compose exec web uv add "httpx>=0.28,<1" "hypersync>=1.2,<2" "django-celery-beat>=2.9,<3"
docker compose exec web uv add --dev "respx>=0.23,<1"
```

Expected: `backend/pyproject.toml` et `backend/uv.lock` modifiés.

- [ ] **Step 2: Configurer pytest dans `backend/pyproject.toml`**

Remplacer le bloc `[tool.pytest.ini_options]` par :

```toml
[tool.pytest.ini_options]
DJANGO_SETTINGS_MODULE = "config.settings.dev"
python_files = ["test_*.py"]
testpaths = ["tests", "apps", "integrations"]
addopts = "-ra -m 'not live'"
markers = ["live: appelle les vraies API externes (lancer avec -m live)"]
```

- [ ] **Step 3: Écrire le test des settings**

`backend/tests/test_settings_discovery.py` :

```python
from django.conf import settings


def test_discovery_apps_installed():
    assert "django_celery_beat" in settings.INSTALLED_APPS


def test_beat_uses_database_scheduler():
    assert settings.CELERY_BEAT_SCHEDULER == "django_celery_beat.schedulers:DatabaseScheduler"


def test_api_keys_default_to_empty_strings():
    assert isinstance(settings.ENVIO_API_TOKEN, str)
    assert isinstance(settings.ZERION_API_KEY, str)
    assert isinstance(settings.COINGECKO_API_KEY, str)
```

(`apps.discovery` est ajoutée à `INSTALLED_APPS` à la Task 6, quand l'app existe ; ce test sera alors complété.)

- [ ] **Step 4: Lancer le test pour vérifier qu'il échoue**

Run: `make test args="tests/test_settings_discovery.py -v"`
Expected: FAIL (`django_celery_beat` absent, `CELERY_BEAT_SCHEDULER` inexistant).

- [ ] **Step 5: Modifier `backend/config/settings/base.py`**

Dans `INSTALLED_APPS`, après `"rest_framework",` ajouter `"django_celery_beat",`.

Après `CELERY_ACCEPT_CONTENT = ["json"]` ajouter :

```python
CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"
```

À la fin du fichier ajouter :

```python
# Clés des services externes (vides si absentes)
ENVIO_API_TOKEN = env("ENVIO_API_TOKEN", default="")
ZERION_API_KEY = env("ZERION_API_KEY", default="")
COINGECKO_API_KEY = env("COINGECKO_API_KEY", default="")
```

- [ ] **Step 6: Modifier le service `beat` de `docker-compose.yml`**

```yaml
  beat:
    <<: *backend
    command: celery -A config beat -l info --scheduler django_celery_beat.schedulers:DatabaseScheduler
```

- [ ] **Step 7: Compléter `.env.example`**

Ajouter à la fin :

```bash

# Services externes
ENVIO_API_TOKEN=
# Clé Zerion brute (celle du dashboard), utilisée en Basic Auth avec un mot de passe vide
ZERION_API_KEY=
# Optionnelle : clé du plan Demo CoinGecko
COINGECKO_API_KEY=
```

Ajouter les trois mêmes lignes (vides) au `.env` local s'il ne les contient pas.

- [ ] **Step 8: Reconstruire, migrer et lancer les tests**

```bash
make up
make migrate
make test args="tests/test_settings_discovery.py -v"
make test
```

Expected: PASS partout (les tests existants du socle, dont `test_heartbeat_is_scheduled_every_minute`, restent verts).

- [ ] **Step 9: Commit**

```bash
git add backend/pyproject.toml backend/uv.lock backend/config/settings/base.py docker-compose.yml .env.example backend/tests/test_settings_discovery.py
git commit -m "feat(discovery): dépendances, clés API et scheduler Beat en base

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Client HTTP commun, erreurs et rate limit

**Files:**
- Create: `backend/integrations/__init__.py`, `backend/integrations/errors.py`, `backend/integrations/ratelimit.py`, `backend/integrations/http.py`
- Create: `backend/integrations/tests/__init__.py`, `backend/integrations/tests/test_ratelimit.py`, `backend/integrations/tests/test_http.py`

**Interfaces:**
- Produces:
  - `integrations.errors` : `IntegrationError`, `NotFound`, `RateLimited`, `UpstreamError`, `TooManyTransfers` (toutes héritent d'`IntegrationError`).
  - `integrations.ratelimit.RateLimiter(client: redis.Redis, name: str, per_minute: int, sleep=time.sleep, clock=time.time)` avec `.acquire() -> None` ; `NoopLimiter().acquire() -> None`.
  - `integrations.http.JsonHttpClient(base_url: str, *, limiter, timeout: float = 15, max_retries: int = 3, headers: dict | None = None, auth: tuple[str, str] | None = None, backoff_seconds: float = 1.0, sleep=time.sleep)` avec `.get(path: str, params: dict | None = None) -> dict | list`.

- [ ] **Step 1: Écrire les tests du rate limiter**

`backend/integrations/__init__.py` :

```python
"""Clients des services externes. Aucune logique métier, aucun accès à la base."""
```

`backend/integrations/tests/__init__.py` : fichier vide.

`backend/integrations/tests/test_ratelimit.py` :

```python
import uuid

import redis
from django.conf import settings

from integrations.ratelimit import NoopLimiter, RateLimiter


class FakeClock:
    def __init__(self, now: float):
        self.now = now
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def make_limiter(per_minute: int, clock: FakeClock) -> RateLimiter:
    client = redis.Redis.from_url(settings.REDIS_URL)
    return RateLimiter(
        client, f"test-{uuid.uuid4()}", per_minute, sleep=clock.sleep, clock=clock.time
    )


def test_allows_requests_under_the_limit():
    clock = FakeClock(now=600.0)
    limiter = make_limiter(2, clock)
    limiter.acquire()
    limiter.acquire()
    assert clock.sleeps == []


def test_waits_for_next_window_when_limit_reached():
    clock = FakeClock(now=610.0)
    limiter = make_limiter(2, clock)
    limiter.acquire()
    limiter.acquire()
    limiter.acquire()
    assert clock.sleeps == [50.0]


def test_noop_limiter_never_waits():
    NoopLimiter().acquire()
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="integrations/tests/test_ratelimit.py -v"`
Expected: FAIL avec `ModuleNotFoundError: No module named 'integrations.ratelimit'`.

- [ ] **Step 3: Implémenter `errors.py` et `ratelimit.py`**

`backend/integrations/errors.py` :

```python
"""Erreurs typées des services externes."""


class IntegrationError(Exception):
    """Erreur d'un service externe."""


class NotFound(IntegrationError):
    """La ressource demandée n'existe pas (HTTP 404)."""


class RateLimited(IntegrationError):
    """Le service refuse encore après toutes les tentatives (HTTP 429)."""


class UpstreamError(IntegrationError):
    """Réponse invalide ou erreur serveur persistante."""


class TooManyTransfers(IntegrationError):
    """Le token dépasse le plafond de transferts autorisé."""
```

`backend/integrations/ratelimit.py` :

```python
"""Limiteur de débit partagé entre workers : fenêtre fixe d'une minute dans Redis."""

import time

import redis


class RateLimiter:
    def __init__(
        self,
        client: redis.Redis,
        name: str,
        per_minute: int,
        sleep=time.sleep,
        clock=time.time,
    ):
        self._client = client
        self._name = name
        self._per_minute = per_minute
        self._sleep = sleep
        self._clock = clock

    def acquire(self) -> None:
        """Bloque jusqu'à ce qu'une requête soit autorisée dans la fenêtre courante."""
        while True:
            now = self._clock()
            window = int(now // 60)
            key = f"ratelimit:{self._name}:{window}"
            count = self._client.incr(key)
            if count == 1:
                self._client.expire(key, 61)
            if count <= self._per_minute:
                return
            self._sleep((window + 1) * 60 - now)


class NoopLimiter:
    def acquire(self) -> None:
        return None
```

- [ ] **Step 4: Lancer les tests du rate limiter**

Run: `make test args="integrations/tests/test_ratelimit.py -v"`
Expected: PASS.

- [ ] **Step 5: Écrire les tests du client HTTP**

`backend/integrations/tests/test_http.py` :

```python
import httpx
import pytest
import respx

from integrations.errors import NotFound, RateLimited, UpstreamError
from integrations.http import JsonHttpClient
from integrations.ratelimit import NoopLimiter

BASE = "https://api.test"


def make_client(max_retries: int = 2) -> tuple[JsonHttpClient, list[float]]:
    sleeps: list[float] = []
    client = JsonHttpClient(
        BASE, limiter=NoopLimiter(), max_retries=max_retries, sleep=sleeps.append
    )
    return client, sleeps


@respx.mock
def test_returns_json():
    respx.get(f"{BASE}/items", params={"page": "1"}).respond(json={"ok": True})
    client, _ = make_client()
    assert client.get("/items", params={"page": 1}) == {"ok": True}


@respx.mock
def test_retries_server_errors_with_backoff():
    respx.get(f"{BASE}/items").mock(
        side_effect=[httpx.Response(500), httpx.Response(503), httpx.Response(200, json=[1])]
    )
    client, sleeps = make_client(max_retries=2)
    assert client.get("/items") == [1]
    assert sleeps == [1.0, 2.0]


@respx.mock
def test_raises_upstream_error_after_retries():
    respx.get(f"{BASE}/items").respond(500)
    client, sleeps = make_client(max_retries=2)
    with pytest.raises(UpstreamError):
        client.get("/items")
    assert len(sleeps) == 2


@respx.mock
def test_raises_rate_limited_after_retries():
    respx.get(f"{BASE}/items").respond(429)
    client, _ = make_client(max_retries=1)
    with pytest.raises(RateLimited):
        client.get("/items")


@respx.mock
def test_404_raises_not_found_without_retry():
    route = respx.get(f"{BASE}/items").respond(404)
    client, sleeps = make_client()
    with pytest.raises(NotFound):
        client.get("/items")
    assert route.call_count == 1
    assert sleeps == []


@respx.mock
def test_other_client_errors_are_not_retried():
    route = respx.get(f"{BASE}/items").respond(400)
    client, _ = make_client()
    with pytest.raises(UpstreamError):
        client.get("/items")
    assert route.call_count == 1


@respx.mock
def test_retries_transport_errors():
    respx.get(f"{BASE}/items").mock(
        side_effect=[httpx.ConnectError("down"), httpx.Response(200, json={"ok": 1})]
    )
    client, _ = make_client()
    assert client.get("/items") == {"ok": 1}
```

- [ ] **Step 6: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="integrations/tests/test_http.py -v"`
Expected: FAIL avec `ModuleNotFoundError: No module named 'integrations.http'`.

- [ ] **Step 7: Implémenter `http.py`**

`backend/integrations/http.py` :

```python
"""Client HTTP JSON commun : timeout, retries avec backoff exponentiel, rate limit."""

import time

import httpx

from integrations.errors import NotFound, RateLimited, UpstreamError

RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


class JsonHttpClient:
    def __init__(
        self,
        base_url: str,
        *,
        limiter,
        timeout: float = 15,
        max_retries: int = 3,
        headers: dict | None = None,
        auth: tuple[str, str] | None = None,
        backoff_seconds: float = 1.0,
        sleep=time.sleep,
    ):
        self._client = httpx.Client(base_url=base_url, timeout=timeout, headers=headers, auth=auth)
        self._limiter = limiter
        self._max_retries = max_retries
        self._backoff = backoff_seconds
        self._sleep = sleep

    def get(self, path: str, params: dict | None = None) -> dict | list:
        attempt = 0
        while True:
            self._limiter.acquire()
            status: int | None = None
            error: Exception | None = None
            try:
                response = self._client.get(path, params=params)
            except httpx.TransportError as exc:
                error = exc
            else:
                status = response.status_code
                if status == 404:
                    raise NotFound(f"GET {path} : introuvable")
                if status < 400:
                    return response.json()
                if status not in RETRYABLE_STATUSES:
                    raise UpstreamError(f"GET {path} : HTTP {status}")
            if attempt >= self._max_retries:
                if status == 429:
                    raise RateLimited(f"GET {path} : HTTP 429")
                raise UpstreamError(f"GET {path} : {status or error}")
            self._sleep(self._backoff * 2**attempt)
            attempt += 1
```

- [ ] **Step 8: Lancer les tests**

Run: `make test args="integrations -v"`
Expected: PASS.

- [ ] **Step 9: Lint et commit**

```bash
make lint
git add backend/integrations
git commit -m "feat(integrations): client HTTP commun, erreurs typées et rate limit Redis

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Client GeckoTerminal

**Files:**
- Create: `backend/integrations/geckoterminal.py`
- Create: `backend/integrations/tests/test_geckoterminal.py`

**Interfaces:**
- Consumes: `JsonHttpClient.get(path, params) -> dict | list` (Task 2).
- Produces:
  - `BASE_URL = "https://api.geckoterminal.com/api/v2"`, `MAX_OHLCV_CANDLES = 1000`.
  - Dataclasses gelées : `GtNetwork(gt_id: str, name: str, coingecko_platform_id: str | None)` ; `GtPool(network: str, address: str, token_address: str, token_symbol: str, token_decimals: int | None, price_change_24h_pct: float, volume_24h_usd: float, liquidity_usd: float, fdv_usd: float, created_at: datetime | None)` ; `GtToken(address: str, symbol: str, decimals: int)` ; `Candle(ts: int, open: float, high: float, low: float, close: float, volume: float)`.
  - `GeckoTerminalClient(http)` : `.networks() -> list[GtNetwork]`, `.trending_pools(page: int) -> list[GtPool]`, `.top_volume_pools(network: str, page: int) -> list[GtPool]`, `.token_pools(network: str, token_address: str) -> list[GtPool]`, `.token(network: str, address: str) -> GtToken`, `.ohlcv(network: str, pool_address: str, timeframe: str, aggregate: int) -> list[Candle]` (triées par `ts` croissant).

- [ ] **Step 1: Écrire les tests**

`backend/integrations/tests/test_geckoterminal.py` :

```python
from datetime import UTC, datetime

import respx

from integrations.geckoterminal import BASE_URL, Candle, GeckoTerminalClient, GtNetwork
from integrations.http import JsonHttpClient
from integrations.ratelimit import NoopLimiter

TOKEN = "0xAbC0000000000000000000000000000000000001"


def client() -> GeckoTerminalClient:
    return GeckoTerminalClient(JsonHttpClient(BASE_URL, limiter=NoopLimiter(), max_retries=0))


def pool_payload(network_rel: bool, network: str = "polygon_pos") -> dict:
    relationships = {
        "base_token": {"data": {"id": f"{network}_{TOKEN}", "type": "token"}},
        "quote_token": {"data": {"id": f"{network}_0xquote", "type": "token"}},
    }
    if network_rel:
        relationships["network"] = {"data": {"id": network, "type": "network"}}
    return {
        "data": [
            {
                "id": f"{network}_0xPOOL",
                "type": "pool",
                "attributes": {
                    "address": "0xPOOL",
                    "pool_created_at": "2026-09-20T10:00:00Z",
                    "fdv_usd": "1500000.5",
                    "reserve_in_usd": "80000",
                    "volume_usd": {"h24": "250000"},
                    "price_change_percentage": {"h24": "120.5"},
                },
                "relationships": relationships,
            }
        ],
        "included": [
            {
                "id": f"{network}_{TOKEN}",
                "type": "token",
                "attributes": {"address": TOKEN, "symbol": "PEPE", "decimals": 9},
            }
        ],
    }


@respx.mock
def test_networks_follows_pagination():
    respx.get(f"{BASE_URL}/networks", params={"page": "1"}).respond(
        json={
            "data": [{"id": "eth", "attributes": {"name": "Ethereum", "coingecko_asset_platform_id": "ethereum"}}],
            "links": {"next": "https://api.geckoterminal.com/api/v2/networks?page=2"},
        }
    )
    respx.get(f"{BASE_URL}/networks", params={"page": "2"}).respond(
        json={
            "data": [{"id": "robinhood", "attributes": {"name": "Robinhood", "coingecko_asset_platform_id": None}}],
            "links": {"next": None},
        }
    )
    assert client().networks() == [
        GtNetwork("eth", "Ethereum", "ethereum"),
        GtNetwork("robinhood", "Robinhood", None),
    ]


@respx.mock
def test_trending_pools_reads_network_from_relationship():
    respx.get(f"{BASE_URL}/networks/trending_pools").respond(json=pool_payload(network_rel=True))
    [pool] = client().trending_pools(page=1)
    assert pool.network == "polygon_pos"
    assert pool.address == "0xpool"
    assert pool.token_address == TOKEN.lower()
    assert pool.token_symbol == "PEPE"
    assert pool.token_decimals == 9
    assert pool.price_change_24h_pct == 120.5
    assert pool.volume_24h_usd == 250000.0
    assert pool.liquidity_usd == 80000.0
    assert pool.fdv_usd == 1500000.5
    assert pool.created_at == datetime(2026, 9, 20, 10, tzinfo=UTC)


@respx.mock
def test_top_volume_pools_uses_given_network():
    route = respx.get(f"{BASE_URL}/networks/polygon_pos/pools").respond(
        json=pool_payload(network_rel=False)
    )
    [pool] = client().top_volume_pools("polygon_pos", page=2)
    assert pool.network == "polygon_pos"
    assert pool.token_address == TOKEN.lower()
    params = route.calls.last.request.url.params
    assert params["sort"] == "h24_volume_usd_desc"
    assert params["page"] == "2"


@respx.mock
def test_token_returns_decimals():
    respx.get(f"{BASE_URL}/networks/base/tokens/{TOKEN.lower()}").respond(
        json={"data": {"attributes": {"address": TOKEN, "symbol": "PEPE", "decimals": 18}}}
    )
    token = client().token("base", TOKEN.lower())
    assert (token.address, token.symbol, token.decimals) == (TOKEN.lower(), "PEPE", 18)


@respx.mock
def test_ohlcv_returns_ascending_candles():
    route = respx.get(f"{BASE_URL}/networks/base/pools/0xpool/ohlcv/hour").respond(
        json={
            "data": {
                "attributes": {
                    "ohlcv_list": [
                        [7200, 2, 3, 1, 2.5, 100],
                        [3600, 1, 2, 0.5, 1.5, 50],
                    ]
                }
            }
        }
    )
    candles = client().ohlcv("base", "0xpool", "hour", 4)
    assert candles == [
        Candle(3600, 1.0, 2.0, 0.5, 1.5, 50.0),
        Candle(7200, 2.0, 3.0, 1.0, 2.5, 100.0),
    ]
    params = route.calls.last.request.url.params
    assert params["aggregate"] == "4"
    assert params["limit"] == "1000"
    assert params["currency"] == "usd"
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="integrations/tests/test_geckoterminal.py -v"`
Expected: FAIL avec `ModuleNotFoundError: No module named 'integrations.geckoterminal'`.

- [ ] **Step 3: Implémenter le client**

`backend/integrations/geckoterminal.py` :

```python
"""Client GeckoTerminal (API publique v2)."""

from dataclasses import dataclass
from datetime import datetime

BASE_URL = "https://api.geckoterminal.com/api/v2"
MAX_OHLCV_CANDLES = 1000


@dataclass(frozen=True)
class GtNetwork:
    gt_id: str
    name: str
    coingecko_platform_id: str | None


@dataclass(frozen=True)
class GtPool:
    network: str
    address: str
    token_address: str
    token_symbol: str
    token_decimals: int | None
    price_change_24h_pct: float
    volume_24h_usd: float
    liquidity_usd: float
    fdv_usd: float
    created_at: datetime | None


@dataclass(frozen=True)
class GtToken:
    address: str
    symbol: str
    decimals: int


@dataclass(frozen=True)
class Candle:
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float


def _float(value) -> float:
    return float(value) if value not in (None, "") else 0.0


def _datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


class GeckoTerminalClient:
    def __init__(self, http):
        self._http = http

    def networks(self) -> list[GtNetwork]:
        networks: list[GtNetwork] = []
        page = 1
        while True:
            payload = self._http.get("/networks", params={"page": page})
            for item in payload.get("data", []):
                attributes = item["attributes"]
                networks.append(
                    GtNetwork(item["id"], attributes["name"], attributes.get("coingecko_asset_platform_id"))
                )
            if not payload.get("links", {}).get("next"):
                return networks
            page += 1

    def trending_pools(self, page: int) -> list[GtPool]:
        params = {"duration": "24h", "page": page, "include": "base_token"}
        return self._pools("/networks/trending_pools", params)

    def top_volume_pools(self, network: str, page: int) -> list[GtPool]:
        params = {"sort": "h24_volume_usd_desc", "page": page, "include": "base_token"}
        return self._pools(f"/networks/{network}/pools", params, network=network)

    def token_pools(self, network: str, token_address: str) -> list[GtPool]:
        params = {"page": 1, "include": "base_token"}
        return self._pools(f"/networks/{network}/tokens/{token_address}/pools", params, network=network)

    def token(self, network: str, address: str) -> GtToken:
        attributes = self._http.get(f"/networks/{network}/tokens/{address}")["data"]["attributes"]
        return GtToken(attributes["address"].lower(), attributes.get("symbol") or "", int(attributes["decimals"]))

    def ohlcv(self, network: str, pool_address: str, timeframe: str, aggregate: int) -> list[Candle]:
        payload = self._http.get(
            f"/networks/{network}/pools/{pool_address}/ohlcv/{timeframe}",
            params={"aggregate": aggregate, "limit": MAX_OHLCV_CANDLES, "currency": "usd"},
        )
        rows = payload["data"]["attributes"]["ohlcv_list"]
        candles = [Candle(int(r[0]), *(float(v) for v in r[1:6])) for r in rows]
        return sorted(candles, key=lambda candle: candle.ts)

    def _pools(self, path: str, params: dict, network: str | None = None) -> list[GtPool]:
        payload = self._http.get(path, params=params)
        tokens = {
            item["id"]: item["attributes"]
            for item in payload.get("included", [])
            if item.get("type") == "token"
        }
        pools = []
        for item in payload.get("data", []):
            relationships = item["relationships"]
            pool_network = network or relationships["network"]["data"]["id"]
            base_id = relationships["base_token"]["data"]["id"]
            token = tokens.get(base_id, {})
            attributes = item["attributes"]
            decimals = token.get("decimals")
            pools.append(
                GtPool(
                    network=pool_network,
                    address=attributes["address"].lower(),
                    token_address=base_id[len(pool_network) + 1 :].lower(),
                    token_symbol=token.get("symbol") or "",
                    token_decimals=int(decimals) if decimals is not None else None,
                    price_change_24h_pct=_float(attributes.get("price_change_percentage", {}).get("h24")),
                    volume_24h_usd=_float(attributes.get("volume_usd", {}).get("h24")),
                    liquidity_usd=_float(attributes.get("reserve_in_usd")),
                    fdv_usd=_float(attributes.get("fdv_usd")),
                    created_at=_datetime(attributes.get("pool_created_at")),
                )
            )
        return pools
```

- [ ] **Step 4: Lancer les tests**

Run: `make test args="integrations/tests/test_geckoterminal.py -v"`
Expected: PASS.

- [ ] **Step 5: Lint et commit**

```bash
make fmt && make lint
git add backend/integrations/geckoterminal.py backend/integrations/tests/test_geckoterminal.py
git commit -m "feat(integrations): client GeckoTerminal (réseaux, pools, token, OHLCV)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Clients CoinGecko, Zerion et annuaire HyperSync

**Files:**
- Create: `backend/integrations/coingecko.py`, `backend/integrations/zerion.py`
- Create: `backend/integrations/hypersync.py` (première partie : annuaire des chaînes)
- Create: `backend/integrations/tests/test_chain_directories.py`

**Interfaces:**
- Consumes: `JsonHttpClient` (Task 2).
- Produces:
  - `integrations.coingecko.BASE_URL = "https://api.coingecko.com/api/v3"` ; `CoinGeckoClient(http).platform_chain_ids() -> dict[str, int]` (id de plateforme CoinGecko → chain id EVM ; plateformes sans `chain_identifier` ignorées).
  - `integrations.zerion.BASE_URL = "https://api.zerion.io/v1"` ; `ZerionClient(http).chain_ids() -> dict[int, str]` (chain id EVM → id Zerion).
  - `integrations.hypersync.CHAINS_URL = "https://chains.hyperquery.xyz"` ; `HyperSyncDirectory(http).supported_chain_ids() -> set[int]` (EVM, hors `TESTNET`) ; `hypersync_url(chain_id: int) -> str`.

- [ ] **Step 1: Écrire les tests**

`backend/integrations/tests/test_chain_directories.py` :

```python
import respx

from integrations.coingecko import BASE_URL as CG_URL
from integrations.coingecko import CoinGeckoClient
from integrations.http import JsonHttpClient
from integrations.hypersync import CHAINS_URL, HyperSyncDirectory, hypersync_url
from integrations.ratelimit import NoopLimiter
from integrations.zerion import BASE_URL as ZERION_URL
from integrations.zerion import ZerionClient


def http(base: str) -> JsonHttpClient:
    return JsonHttpClient(base, limiter=NoopLimiter(), max_retries=0)


@respx.mock
def test_coingecko_platform_chain_ids():
    respx.get(f"{CG_URL}/asset_platforms").respond(
        json=[
            {"id": "ethereum", "chain_identifier": 1},
            {"id": "robinhood", "chain_identifier": 4663},
            {"id": "solana", "chain_identifier": None},
        ]
    )
    assert CoinGeckoClient(http(CG_URL)).platform_chain_ids() == {"ethereum": 1, "robinhood": 4663}


@respx.mock
def test_zerion_chain_ids_parses_hex_external_id():
    respx.get(f"{ZERION_URL}/chains/").respond(
        json={
            "data": [
                {"id": "ethereum", "attributes": {"external_id": "0x1"}},
                {"id": "robinhood", "attributes": {"external_id": "0x1237"}},
                {"id": "solana", "attributes": {"external_id": None}},
            ]
        }
    )
    assert ZerionClient(http(ZERION_URL)).chain_ids() == {1: "ethereum", 4663: "robinhood"}


@respx.mock
def test_hypersync_directory_keeps_evm_mainnets():
    respx.get(f"{CHAINS_URL}/active_chains").respond(
        json=[
            {"name": "eth", "tier": "GOLD", "chain_id": 1, "ecosystem": "evm"},
            {"name": "robinhood", "tier": "STONE", "chain_id": 4663, "ecosystem": "evm"},
            {"name": "arbitrum-sepolia", "tier": "TESTNET", "chain_id": 421614, "ecosystem": "evm"},
            {"name": "fuel-mainnet", "tier": "GOLD", "chain_id": 9889, "ecosystem": "fuel"},
            {"name": "solana-448h", "tier": "TESTNET", "ecosystem": "solana"},
        ]
    )
    assert HyperSyncDirectory(http(CHAINS_URL)).supported_chain_ids() == {1, 4663}


def test_hypersync_url():
    assert hypersync_url(8453) == "https://8453.hypersync.xyz"
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="integrations/tests/test_chain_directories.py -v"`
Expected: FAIL avec `ModuleNotFoundError`.

- [ ] **Step 3: Implémenter les trois clients**

`backend/integrations/coingecko.py` :

```python
"""Client CoinGecko : correspondance plateforme → chain id EVM."""

BASE_URL = "https://api.coingecko.com/api/v3"


class CoinGeckoClient:
    def __init__(self, http):
        self._http = http

    def platform_chain_ids(self) -> dict[str, int]:
        platforms = self._http.get("/asset_platforms")
        return {
            platform["id"]: int(platform["chain_identifier"])
            for platform in platforms
            if platform.get("chain_identifier")
        }
```

`backend/integrations/zerion.py` :

```python
"""Client Zerion : chaînes supportées (l'historique des wallets viendra plus tard)."""

BASE_URL = "https://api.zerion.io/v1"


class ZerionClient:
    def __init__(self, http):
        self._http = http

    def chain_ids(self) -> dict[int, str]:
        payload = self._http.get("/chains/")
        chains = {}
        for item in payload.get("data", []):
            external_id = item["attributes"].get("external_id")
            if external_id:
                chains[int(external_id, 16)] = item["id"]
        return chains
```

`backend/integrations/hypersync.py` :

```python
"""Client HyperSync (Envio) : chaînes supportées, blocs et transferts ERC-20."""

CHAINS_URL = "https://chains.hyperquery.xyz"


def hypersync_url(chain_id: int) -> str:
    return f"https://{chain_id}.hypersync.xyz"


class HyperSyncDirectory:
    def __init__(self, http):
        self._http = http

    def supported_chain_ids(self) -> set[int]:
        chains = self._http.get("/active_chains")
        return {
            chain["chain_id"]
            for chain in chains
            if chain.get("ecosystem") == "evm"
            and chain.get("tier") != "TESTNET"
            and chain.get("chain_id")
        }
```

- [ ] **Step 4: Lancer les tests**

Run: `make test args="integrations/tests/test_chain_directories.py -v"`
Expected: PASS.

- [ ] **Step 5: Lint et commit**

```bash
make fmt && make lint
git add backend/integrations
git commit -m "feat(integrations): annuaires de chaînes CoinGecko, Zerion et HyperSync

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Client HyperSync (hauteur, timestamps, transferts)

**Files:**
- Modify: `backend/integrations/hypersync.py`
- Create: `backend/integrations/tests/test_hypersync.py`

**Interfaces:**
- Consumes: `TooManyTransfers`, `UpstreamError` (Task 2) ; limiter avec `.acquire()`.
- Produces:
  - `TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"`.
  - `Transfer(block: int, timestamp: int, tx_from: str, sender: str, recipient: str, amount: int)` (dataclass gelée, adresses en minuscules).
  - `HyperSyncClient(chain_id: int, api_token: str, limiter, max_retries: int = 3, inner=None)` : `.height() -> int`, `.block_timestamp(number: int) -> int` (mis en cache par instance), `.transfers(token: str, from_block: int, to_block: int, max_transfers: int) -> list[Transfer]` (`to_block` exclusif ; lève `TooManyTransfers` au-delà de `max_transfers`).

- [ ] **Step 1: Écrire les tests (client interne simulé)**

`backend/integrations/tests/test_hypersync.py` :

```python
from types import SimpleNamespace

import pytest

from integrations.errors import TooManyTransfers
from integrations.hypersync import TRANSFER_TOPIC, HyperSyncClient, Transfer
from integrations.ratelimit import NoopLimiter

ALICE = "0x" + "a" * 40
POOL = "0x" + "b" * 40
BOB = "0x" + "c" * 40


def topic(address: str) -> str:
    return "0x" + "0" * 24 + address[2:]


def log(block: int, tx: str, sender: str, recipient: str, amount: int):
    return SimpleNamespace(
        block_number=block,
        transaction_hash=tx,
        data=hex(amount),
        topics=[TRANSFER_TOPIC, topic(sender), topic(recipient)],
    )


def page(next_block: int, logs=(), txs=(), blocks=()):
    return SimpleNamespace(
        next_block=next_block,
        data=SimpleNamespace(logs=list(logs), transactions=list(txs), blocks=list(blocks)),
    )


class FakeInner:
    def __init__(self, pages, height: int = 0):
        self.pages = list(pages)
        self.height = height
        self.from_blocks: list[int] = []

    async def get(self, query):
        self.from_blocks.append(query.from_block)
        return self.pages.pop(0)

    async def get_height(self):
        return self.height


def make_client(inner: FakeInner) -> HyperSyncClient:
    return HyperSyncClient(1, "token", NoopLimiter(), inner=inner)


def test_height():
    assert make_client(FakeInner([], height=123)).height() == 123


def test_block_timestamp_parses_hex_and_caches():
    inner = FakeInner([page(6, blocks=[SimpleNamespace(number=5, timestamp="0x10")])])
    client = make_client(inner)
    assert client.block_timestamp(5) == 16
    assert client.block_timestamp(5) == 16
    assert inner.from_blocks == [5]


def test_transfers_follows_pagination_and_joins_tx_sender():
    inner = FakeInner(
        [
            page(
                150,
                logs=[log(100, "0xt1", POOL, ALICE, 1000)],
                txs=[SimpleNamespace(hash="0xt1", from_=ALICE.upper().replace("0X", "0x"))],
                blocks=[SimpleNamespace(number=100, timestamp="0x64")],
            ),
            page(
                200,
                logs=[
                    log(160, "0xt2", ALICE, BOB, 400),
                    SimpleNamespace(block_number=161, transaction_hash="0xt3", data="0x", topics=[TRANSFER_TOPIC, topic(ALICE), topic(BOB), topic(BOB)]),
                ],
                txs=[SimpleNamespace(hash="0xt2", from_=ALICE), SimpleNamespace(hash="0xt3", from_=ALICE)],
                blocks=[SimpleNamespace(number=160, timestamp=200), SimpleNamespace(number=161, timestamp=202)],
            ),
        ]
    )
    transfers = make_client(inner).transfers("0xtoken", 50, 200, max_transfers=10)
    assert transfers == [
        Transfer(block=100, timestamp=100, tx_from=ALICE, sender=POOL, recipient=ALICE, amount=1000),
        Transfer(block=160, timestamp=200, tx_from=ALICE, sender=ALICE, recipient=BOB, amount=400),
    ]
    assert inner.from_blocks == [50, 150]


def test_transfers_raises_when_over_cap():
    inner = FakeInner(
        [
            page(
                200,
                logs=[log(100, "0xt1", POOL, ALICE, 1), log(101, "0xt1", POOL, ALICE, 1)],
                txs=[SimpleNamespace(hash="0xt1", from_=ALICE)],
                blocks=[SimpleNamespace(number=100, timestamp=1), SimpleNamespace(number=101, timestamp=2)],
            )
        ]
    )
    with pytest.raises(TooManyTransfers):
        make_client(inner).transfers("0xtoken", 0, 200, max_transfers=1)
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="integrations/tests/test_hypersync.py -v"`
Expected: FAIL avec `ImportError: cannot import name 'TRANSFER_TOPIC'`.

- [ ] **Step 3: Compléter `backend/integrations/hypersync.py`**

Remplacer le fichier entier par :

```python
"""Client HyperSync (Envio) : chaînes supportées, blocs et transferts ERC-20."""

import asyncio
from dataclasses import dataclass

import hypersync
from hypersync import (
    BlockField,
    ClientConfig,
    FieldSelection,
    LogField,
    LogSelection,
    Query,
    TransactionField,
)

from integrations.errors import TooManyTransfers, UpstreamError

CHAINS_URL = "https://chains.hyperquery.xyz"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def hypersync_url(chain_id: int) -> str:
    return f"https://{chain_id}.hypersync.xyz"


class HyperSyncDirectory:
    def __init__(self, http):
        self._http = http

    def supported_chain_ids(self) -> set[int]:
        chains = self._http.get("/active_chains")
        return {
            chain["chain_id"]
            for chain in chains
            if chain.get("ecosystem") == "evm"
            and chain.get("tier") != "TESTNET"
            and chain.get("chain_id")
        }


@dataclass(frozen=True)
class Transfer:
    block: int
    timestamp: int
    tx_from: str
    sender: str
    recipient: str
    amount: int


def _int(value) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    return int(value, 16) if value.startswith("0x") else int(value)


def _topic_address(topic: str) -> str:
    return "0x" + topic[-40:].lower()


class HyperSyncClient:
    def __init__(self, chain_id: int, api_token: str, limiter, max_retries: int = 3, inner=None):
        self._inner = inner or hypersync.HypersyncClient(
            ClientConfig(url=hypersync_url(chain_id), bearer_token=api_token, max_num_retries=max_retries)
        )
        self._limiter = limiter
        self._timestamps: dict[int, int] = {}

    def height(self) -> int:
        self._limiter.acquire()
        return asyncio.run(self._inner.get_height())

    def block_timestamp(self, number: int) -> int:
        if number not in self._timestamps:
            query = Query(
                from_block=number,
                to_block=number + 1,
                include_all_blocks=True,
                field_selection=FieldSelection(block=[BlockField.NUMBER, BlockField.TIMESTAMP]),
            )
            response = self._get(query)
            blocks = {block.number: _int(block.timestamp) for block in response.data.blocks}
            if number not in blocks:
                raise UpstreamError(f"HyperSync : bloc {number} absent de la réponse")
            self._timestamps[number] = blocks[number]
        return self._timestamps[number]

    def transfers(self, token: str, from_block: int, to_block: int, max_transfers: int) -> list[Transfer]:
        query = Query(
            from_block=from_block,
            to_block=to_block,
            logs=[LogSelection(address=[token], topics=[[TRANSFER_TOPIC]])],
            field_selection=FieldSelection(
                block=[BlockField.NUMBER, BlockField.TIMESTAMP],
                transaction=[TransactionField.HASH, TransactionField.FROM],
                log=[
                    LogField.BLOCK_NUMBER,
                    LogField.TRANSACTION_HASH,
                    LogField.DATA,
                    LogField.TOPIC0,
                    LogField.TOPIC1,
                    LogField.TOPIC2,
                ],
            ),
        )
        transfers: list[Transfer] = []
        while True:
            response = self._get(query)
            data = response.data
            timestamps = {block.number: _int(block.timestamp) for block in data.blocks}
            senders = {tx.hash: tx.from_.lower() for tx in data.transactions if tx.hash and tx.from_}
            for log in data.logs:
                topics = log.topics or []
                tx_from = senders.get(log.transaction_hash)
                # Les Transfer ERC-721 ont 4 topics et pas de data : on les ignore.
                if len(topics) != 3 or not log.data or log.data == "0x" or tx_from is None:
                    continue
                transfers.append(
                    Transfer(
                        block=log.block_number,
                        timestamp=timestamps.get(log.block_number, 0),
                        tx_from=tx_from,
                        sender=_topic_address(topics[1]),
                        recipient=_topic_address(topics[2]),
                        amount=_int(log.data),
                    )
                )
            if len(transfers) > max_transfers:
                raise TooManyTransfers(f"{token} : plus de {max_transfers} transferts")
            if response.next_block >= to_block:
                return transfers
            query.from_block = response.next_block

    def _get(self, query: Query):
        self._limiter.acquire()
        return asyncio.run(self._inner.get(query))
```

- [ ] **Step 4: Lancer les tests**

Run: `make test args="integrations -v"`
Expected: PASS.

- [ ] **Step 5: Lint et commit**

```bash
make fmt && make lint
git add backend/integrations
git commit -m "feat(integrations): client HyperSync (hauteur, timestamps, transferts ERC-20)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: App `discovery` et modèles

**Files:**
- Create: `backend/apps/discovery/` (via `make startapp name=discovery`)
- Delete: `backend/apps/discovery/services.py` (remplacé par le package `services/`)
- Create: `backend/apps/discovery/services/__init__.py`
- Modify: `backend/apps/discovery/models.py`
- Create: `backend/apps/discovery/migrations/0001_initial.py` (générée)
- Modify: `backend/config/settings/base.py`, `backend/config/urls.py`, `backend/tests/test_settings_discovery.py`
- Create: `backend/apps/discovery/tests/factories.py`, `backend/apps/discovery/tests/test_models.py`

**Interfaces:**
- Produces (dans `apps.discovery.models`) :
  - `THRESHOLD_FIELDS: tuple[str, ...]` (les 14 seuils, dans l'ordre du modèle).
  - `Chain` (+ `Chain.objects.active()`, propriété `is_active`), `DetectionSettings` (+ `clean()`), `PipelineSettings` (+ `PipelineSettings.load()`), `Token`, `Pool`, `Candidate` (+ `Candidate.Status`, `Candidate.objects.open()`), `Explosion`, `Wallet`, `EarlyBuyer`.
  - `CLOSED_STATUSES = ("rejected", "buyers_extracted")`.
  - `apps.discovery.tests.factories` : `make_chain(**overrides) -> Chain`, `make_token(chain=None, address=..., **overrides) -> Token`, `make_candidate(token=None, **overrides) -> Candidate`.

- [ ] **Step 1: Créer l'app**

```bash
make startapp name=discovery
rm backend/apps/discovery/services.py
mkdir -p backend/apps/discovery/services
printf '"""Logique métier de la découverte."""\n' > backend/apps/discovery/services/__init__.py
```

Dans `backend/config/settings/base.py`, ajouter `"apps.discovery",` après `"apps.core",` dans `INSTALLED_APPS`.

Dans `backend/config/urls.py`, ajouter `path("api/discovery/", include("apps.discovery.urls")),` après la ligne `api/core/`.

Si le template n'a pas créé de dossier de tests : `mkdir -p backend/apps/discovery/tests && touch backend/apps/discovery/tests/__init__.py`.

Dans `backend/tests/test_settings_discovery.py`, remplacer le premier test par :

```python
def test_discovery_apps_installed():
    assert "django_celery_beat" in settings.INSTALLED_APPS
    assert "apps.discovery" in settings.INSTALLED_APPS
```

- [ ] **Step 2: Écrire les tests des modèles**

`backend/apps/discovery/tests/factories.py` :

```python
"""Fabriques de données de test."""

from apps.discovery.models import Candidate, Chain, Token


def make_chain(**overrides) -> Chain:
    values = {
        "gt_id": "base",
        "name": "Base",
        "evm_id": 8453,
        "zerion_id": "base",
        "hypersync_supported": True,
        "is_enabled": True,
    }
    values.update(overrides)
    return Chain.objects.create(**values)


def make_token(chain: Chain | None = None, address: str = "0x" + "1" * 40, **overrides) -> Token:
    values = {"symbol": "TKN", "decimals": 18}
    values.update(overrides)
    return Token.objects.create(chain=chain or make_chain(), address=address, **values)


def make_candidate(token: Token | None = None, **overrides) -> Candidate:
    return Candidate.objects.create(token=token or make_token(), **overrides)
```

`backend/apps/discovery/tests/test_models.py` :

```python
import pytest
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction

from apps.discovery.models import Candidate, Chain, DetectionSettings, PipelineSettings
from apps.discovery.tests.factories import make_candidate, make_chain, make_token

pytestmark = pytest.mark.django_db


def test_active_chains_require_every_service():
    make_chain(gt_id="ok")
    make_chain(gt_id="disabled", is_enabled=False)
    make_chain(gt_id="no-hypersync", hypersync_supported=False)
    make_chain(gt_id="no-zerion", zerion_id="")
    make_chain(gt_id="no-evm", evm_id=None)
    assert [chain.gt_id for chain in Chain.objects.active()] == ["ok"]
    assert Chain.objects.get(gt_id="ok").is_active
    assert not Chain.objects.get(gt_id="no-zerion").is_active


def test_only_one_global_detection_settings_row():
    DetectionSettings.objects.filter(chain=None).delete()
    DetectionSettings.objects.create(chain=None)
    with pytest.raises(IntegrityError), transaction.atomic():
        DetectionSettings.objects.create(chain=None)


def test_global_settings_require_every_threshold():
    with pytest.raises(ValidationError):
        DetectionSettings(chain=None, min_multiplier=5).clean()


def test_chain_settings_may_leave_thresholds_empty():
    DetectionSettings(chain=make_chain(), min_multiplier=3).clean()


def test_pipeline_settings_is_a_singleton():
    first = PipelineSettings.load()
    PipelineSettings(trending_pages=2).save()
    assert PipelineSettings.objects.count() == 1
    assert PipelineSettings.load().pk == first.pk
    assert PipelineSettings.load().trending_pages == 2


def test_one_open_candidate_per_token():
    token = make_token()
    make_candidate(token)
    with pytest.raises(IntegrityError), transaction.atomic():
        make_candidate(token)


def test_closed_candidates_do_not_block_a_new_one():
    token = make_token()
    make_candidate(token, status=Candidate.Status.REJECTED)
    make_candidate(token, status=Candidate.Status.BUYERS_EXTRACTED)
    make_candidate(token)
    assert Candidate.objects.open().count() == 1
```

- [ ] **Step 3: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/discovery/tests/test_models.py -v"`
Expected: FAIL avec `ImportError: cannot import name 'Candidate'`.

- [ ] **Step 4: Écrire les modèles**

`backend/apps/discovery/models.py` :

```python
"""Modèles de la découverte : chaînes, réglages, tokens, candidats, explosions, acheteurs."""

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import Q

THRESHOLD_FIELDS = (
    "min_change_24h_pct",
    "min_liquidity_usd",
    "min_volume_usd",
    "peak_volume_window_hours",
    "min_fdv_usd",
    "max_fdv_usd",
    "max_pool_age_hours",
    "min_multiplier",
    "min_retention_pct",
    "confirmation_hours",
    "confirmation_timeout_hours",
    "sniper_blocks",
    "min_buy_usd",
    "max_buyers",
)
CLOSED_STATUSES = ("rejected", "buyers_extracted")
UINT256_DIGITS = 78


class ChainQuerySet(models.QuerySet):
    def active(self):
        return self.filter(
            is_enabled=True, hypersync_supported=True, evm_id__isnull=False
        ).exclude(zerion_id="")


class Chain(models.Model):
    gt_id = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=128)
    evm_id = models.PositiveBigIntegerField(null=True, blank=True)
    zerion_id = models.CharField(max_length=64, blank=True, default="")
    hypersync_supported = models.BooleanField(default=False)
    is_enabled = models.BooleanField(default=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = ChainQuerySet.as_manager()

    class Meta:
        ordering = ["gt_id"]

    def __str__(self):
        return self.gt_id

    @property
    def is_active(self) -> bool:
        return (
            self.is_enabled
            and self.hypersync_supported
            and self.evm_id is not None
            and self.zerion_id != ""
        )


def _usd():
    return models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)


class DetectionSettings(models.Model):
    chain = models.ForeignKey(
        Chain, null=True, blank=True, on_delete=models.CASCADE, related_name="detection_settings"
    )
    min_change_24h_pct = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    min_liquidity_usd = _usd()
    min_volume_usd = _usd()
    peak_volume_window_hours = models.PositiveIntegerField(null=True, blank=True)
    min_fdv_usd = _usd()
    max_fdv_usd = _usd()
    max_pool_age_hours = models.PositiveIntegerField(null=True, blank=True)
    min_multiplier = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    min_retention_pct = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    confirmation_hours = models.PositiveIntegerField(null=True, blank=True)
    confirmation_timeout_hours = models.PositiveIntegerField(null=True, blank=True)
    sniper_blocks = models.PositiveIntegerField(null=True, blank=True)
    min_buy_usd = _usd()
    max_buyers = models.PositiveIntegerField(
        null=True, blank=True, help_text="0 = pas de plafond. Vide = valeur globale."
    )

    class Meta:
        verbose_name = "réglages de détection"
        verbose_name_plural = "réglages de détection"
        constraints = [
            models.UniqueConstraint(
                fields=["chain"], name="discovery_one_settings_per_chain", nulls_distinct=False
            )
        ]

    def __str__(self):
        return f"Réglages {self.chain or 'globaux'}"

    def clean(self):
        if self.chain_id is None:
            missing = [name for name in THRESHOLD_FIELDS if getattr(self, name) is None]
            if missing:
                raise ValidationError(
                    {name: "Obligatoire pour les réglages globaux." for name in missing}
                )


class PipelineSettings(models.Model):
    trending_pages = models.PositiveSmallIntegerField(default=10)
    volume_pages_per_chain = models.PositiveSmallIntegerField(default=3)
    candidate_cooldown_hours = models.PositiveIntegerField(default=72)
    max_transfers_per_token = models.PositiveIntegerField(default=500_000)
    max_attempts = models.PositiveSmallIntegerField(default=3)
    gecko_requests_per_min = models.PositiveIntegerField(default=30)
    hypersync_requests_per_min = models.PositiveIntegerField(default=60)
    http_timeout_seconds = models.PositiveIntegerField(default=15)
    http_max_retries = models.PositiveSmallIntegerField(default=3)

    class Meta:
        verbose_name = "réglages du pipeline"
        verbose_name_plural = "réglages du pipeline"

    def __str__(self):
        return "Réglages du pipeline"

    def save(self, *args, **kwargs):
        self.pk = 1
        super().save(*args, **kwargs)

    @classmethod
    def load(cls) -> "PipelineSettings":
        settings, _ = cls.objects.get_or_create(pk=1)
        return settings


class Token(models.Model):
    chain = models.ForeignKey(Chain, on_delete=models.CASCADE, related_name="tokens")
    address = models.CharField(max_length=66)
    symbol = models.CharField(max_length=64, blank=True)
    decimals = models.PositiveSmallIntegerField(default=18)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["chain", "address"], name="discovery_unique_token")
        ]

    def __str__(self):
        return f"{self.symbol or self.address} ({self.chain})"


class Pool(models.Model):
    token = models.ForeignKey(Token, on_delete=models.CASCADE, related_name="pools")
    address = models.CharField(max_length=66)
    created_block = models.PositiveBigIntegerField(null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["token", "address"], name="discovery_unique_pool")
        ]

    def __str__(self):
        return self.address


class CandidateQuerySet(models.QuerySet):
    def open(self):
        return self.exclude(status__in=CLOSED_STATUSES)


class Candidate(models.Model):
    class Status(models.TextChoices):
        CANDIDATE = "candidate", "Candidat"
        ANALYZED = "analyzed", "Analysé"
        WAITING_CONFIRMATION = "waiting_confirmation", "En attente de confirmation"
        CONFIRMED = "confirmed", "Confirmé"
        BUYERS_EXTRACTED = "buyers_extracted", "Acheteurs extraits"
        REJECTED = "rejected", "Rejeté"

    token = models.ForeignKey(Token, on_delete=models.CASCADE, related_name="candidates")
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.CANDIDATE)
    sources = models.JSONField(default=list)
    metrics = models.JSONField(default=dict)
    rejection_reason = models.CharField(max_length=64, blank=True, default="")
    attempts = models.PositiveSmallIntegerField(default=0)
    next_check_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = CandidateQuerySet.as_manager()

    class Meta:
        indexes = [
            models.Index(fields=["status", "next_check_at"], name="discovery_cand_status_check")
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["token"],
                condition=~Q(status__in=CLOSED_STATUSES),
                name="discovery_one_open_candidate",
            )
        ]

    def __str__(self):
        return f"{self.token} — {self.get_status_display()}"


class Explosion(models.Model):
    candidate = models.OneToOneField(Candidate, on_delete=models.CASCADE, related_name="explosion")
    low_block = models.PositiveBigIntegerField()
    low_at = models.DateTimeField()
    peak_block = models.PositiveBigIntegerField()
    peak_at = models.DateTimeField()
    multiplier = models.DecimalField(max_digits=12, decimal_places=2)
    retention_pct = models.DecimalField(max_digits=7, decimal_places=2)

    def __str__(self):
        return f"{self.candidate.token} ×{self.multiplier}"


class Wallet(models.Model):
    address = models.CharField(max_length=42, unique=True)

    def __str__(self):
        return self.address


class EarlyBuyer(models.Model):
    explosion = models.ForeignKey(Explosion, on_delete=models.CASCADE, related_name="buyers")
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="early_buys")
    first_buy_block = models.PositiveBigIntegerField()
    first_buy_at = models.DateTimeField()
    bought_amount = models.DecimalField(max_digits=UINT256_DIGITS, decimal_places=0)
    bought_usd = models.DecimalField(max_digits=20, decimal_places=2)
    sold_amount = models.DecimalField(max_digits=UINT256_DIGITS, decimal_places=0, default=0)
    is_sniper = models.BooleanField(default=False)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["explosion", "wallet"], name="discovery_one_buyer_per_explosion"
            )
        ]

    def __str__(self):
        return f"{self.wallet} → {self.explosion}"
```

L'index sur `EarlyBuyer.wallet` est créé automatiquement par Django (toute `ForeignKey` est indexée).

- [ ] **Step 5: Générer la migration et lancer les tests**

```bash
docker compose exec web python manage.py makemigrations discovery
make migrate
make test args="apps/discovery tests/test_settings_discovery.py -v"
```

Expected: `0001_initial.py` créée ; PASS.

- [ ] **Step 6: Lint et commit**

```bash
make fmt && make lint
git add backend/apps/discovery backend/config/settings/base.py backend/config/urls.py backend/tests/test_settings_discovery.py
git commit -m "feat(discovery): app et modèles (chaînes, réglages, candidats, explosions, acheteurs)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Valeurs par défaut, planning quotidien et résolution des seuils

**Files:**
- Create: `backend/apps/discovery/migrations/0002_defaults.py`
- Create: `backend/apps/discovery/services/settings.py`
- Create: `backend/apps/discovery/tests/test_settings_service.py`

**Interfaces:**
- Consumes: `DetectionSettings`, `Chain`, `THRESHOLD_FIELDS` (Task 6).
- Produces:
  - `Thresholds` (dataclass gelée) : `min_change_24h_pct: float`, `min_liquidity_usd: float`, `min_volume_usd: float`, `peak_volume_window_hours: int`, `min_fdv_usd: float`, `max_fdv_usd: float`, `max_pool_age_hours: int`, `min_multiplier: float`, `min_retention_pct: float`, `confirmation_hours: int`, `confirmation_timeout_hours: int`, `sniper_blocks: int`, `min_buy_usd: float`, `max_buyers: int`.
  - `thresholds_for(chain: Chain) -> Thresholds`.
  - Tâche périodique `discovery-daily` → `apps.discovery.tasks.run_discovery`, tous les jours à 06:00 UTC.

- [ ] **Step 1: Écrire les tests**

`backend/apps/discovery/tests/test_settings_service.py` :

```python
import pytest
from django_celery_beat.models import PeriodicTask

from apps.discovery.models import DetectionSettings
from apps.discovery.services.settings import Thresholds, thresholds_for
from apps.discovery.tests.factories import make_chain

pytestmark = pytest.mark.django_db


def test_migration_creates_global_defaults():
    thresholds = thresholds_for(make_chain())
    assert thresholds == Thresholds(
        min_change_24h_pct=50.0,
        min_liquidity_usd=10_000.0,
        min_volume_usd=50_000.0,
        peak_volume_window_hours=24,
        min_fdv_usd=100_000.0,
        max_fdv_usd=100_000_000.0,
        max_pool_age_hours=720,
        min_multiplier=5.0,
        min_retention_pct=30.0,
        confirmation_hours=24,
        confirmation_timeout_hours=168,
        sniper_blocks=3,
        min_buy_usd=500.0,
        max_buyers=300,
    )


def test_chain_override_wins_and_empty_fields_inherit():
    chain = make_chain()
    DetectionSettings.objects.create(chain=chain, min_multiplier=3, max_buyers=0)
    thresholds = thresholds_for(chain)
    assert thresholds.min_multiplier == 3.0
    assert thresholds.max_buyers == 0
    assert thresholds.min_buy_usd == 500.0


def test_changes_are_read_at_each_call():
    chain = make_chain()
    DetectionSettings.objects.filter(chain=None).update(min_buy_usd=1000)
    assert thresholds_for(chain).min_buy_usd == 1000.0


def test_daily_periodic_task_exists():
    task = PeriodicTask.objects.get(name="discovery-daily")
    assert task.task == "apps.discovery.tasks.run_discovery"
    assert (task.crontab.minute, task.crontab.hour) == ("0", "6")
    assert task.enabled
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/discovery/tests/test_settings_service.py -v"`
Expected: FAIL avec `ModuleNotFoundError: No module named 'apps.discovery.services.settings'`.

- [ ] **Step 3: Écrire la data migration**

`backend/apps/discovery/migrations/0002_defaults.py` :

```python
"""Réglages globaux par défaut et tâche quotidienne. Ensuite, tout se gère dans l'admin."""

from django.db import migrations

DETECTION_DEFAULTS = {
    "min_change_24h_pct": 50,
    "min_liquidity_usd": 10_000,
    "min_volume_usd": 50_000,
    "peak_volume_window_hours": 24,
    "min_fdv_usd": 100_000,
    "max_fdv_usd": 100_000_000,
    "max_pool_age_hours": 720,
    "min_multiplier": 5,
    "min_retention_pct": 30,
    "confirmation_hours": 24,
    "confirmation_timeout_hours": 168,
    "sniper_blocks": 3,
    "min_buy_usd": 500,
    "max_buyers": 300,
}


def create_defaults(apps, schema_editor):
    DetectionSettings = apps.get_model("discovery", "DetectionSettings")
    CrontabSchedule = apps.get_model("django_celery_beat", "CrontabSchedule")
    PeriodicTask = apps.get_model("django_celery_beat", "PeriodicTask")

    DetectionSettings.objects.get_or_create(chain=None, defaults=DETECTION_DEFAULTS)
    schedule, _ = CrontabSchedule.objects.get_or_create(
        minute="0",
        hour="6",
        day_of_week="*",
        day_of_month="*",
        month_of_year="*",
        timezone="UTC",
    )
    PeriodicTask.objects.get_or_create(
        name="discovery-daily",
        defaults={"task": "apps.discovery.tasks.run_discovery", "crontab": schedule},
    )


def remove_defaults(apps, schema_editor):
    apps.get_model("django_celery_beat", "PeriodicTask").objects.filter(
        name="discovery-daily"
    ).delete()
    apps.get_model("discovery", "DetectionSettings").objects.filter(chain=None).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("discovery", "0001_initial"),
        ("django_celery_beat", "0019_alter_periodictasks_options"),
    ]

    operations = [migrations.RunPython(create_defaults, remove_defaults)]
```

- [ ] **Step 4: Écrire le service de résolution**

`backend/apps/discovery/services/settings.py` :

```python
"""Résolution des seuils : valeur de la chaîne si renseignée, sinon valeur globale."""

from dataclasses import dataclass, fields
from decimal import Decimal

from django.db.models import Q

from apps.discovery.models import Chain, DetectionSettings


@dataclass(frozen=True)
class Thresholds:
    min_change_24h_pct: float
    min_liquidity_usd: float
    min_volume_usd: float
    peak_volume_window_hours: int
    min_fdv_usd: float
    max_fdv_usd: float
    max_pool_age_hours: int
    min_multiplier: float
    min_retention_pct: float
    confirmation_hours: int
    confirmation_timeout_hours: int
    sniper_blocks: int
    min_buy_usd: float
    max_buyers: int


def thresholds_for(chain: Chain) -> Thresholds:
    rows = {
        row.chain_id: row
        for row in DetectionSettings.objects.filter(Q(chain__isnull=True) | Q(chain=chain))
    }
    base = rows[None]
    override = rows.get(chain.pk)
    values = {}
    for field in fields(Thresholds):
        value = getattr(override, field.name) if override else None
        if value is None:
            value = getattr(base, field.name)
        values[field.name] = float(value) if isinstance(value, Decimal) else value
    return Thresholds(**values)
```

- [ ] **Step 5: Migrer et lancer les tests**

```bash
make migrate
make test args="apps/discovery -v"
```

Expected: PASS.

- [ ] **Step 6: Lint et commit**

```bash
make fmt && make lint
git add backend/apps/discovery
git commit -m "feat(discovery): seuils par défaut, planning quotidien et résolution par chaîne

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Détection d'explosion (fonctions pures)

**Files:**
- Create: `backend/apps/discovery/services/explosion.py`
- Create: `backend/apps/discovery/tests/test_explosion.py`

**Interfaces:**
- Consumes: `Candle` (Task 3), `MAX_OHLCV_CANDLES` (Task 3), `Thresholds` (Task 7).
- Produces:
  - `choose_resolution(pool_age_hours: float) -> tuple[str, int]` : `("hour", 1)` si ≤ 1 000 h, `("hour", 4)` si ≤ 4 000 h, `("hour", 12)` si ≤ 12 000 h, sinon `("day", 1)`.
  - `ExplosionSignal(low_ts: int, peak_ts: int, multiplier: float, retention_pct: float | None)`.
  - `Verdict(status: str, reason: str = "", signal: ExplosionSignal | None = None, next_check_ts: int | None = None)` ; `status` ∈ `CONFIRMED = "confirmed"`, `WAITING = "waiting"`, `REJECTED = "rejected"`.
  - `detect_explosion(candles: list[Candle], *, now_ts: int, current_liquidity_usd: float, thresholds: Thresholds) -> Verdict`. Raisons de rejet : `no_explosion`, `low_volume`, `low_liquidity`, `rug`.

- [ ] **Step 1: Écrire les tests**

`backend/apps/discovery/tests/test_explosion.py` :

```python
from dataclasses import replace

import pytest

from apps.discovery.services.explosion import (
    CONFIRMED,
    REJECTED,
    WAITING,
    choose_resolution,
    detect_explosion,
    find_best_run,
)
from apps.discovery.services.settings import Thresholds
from integrations.geckoterminal import Candle

HOUR = 3600
T = Thresholds(
    min_change_24h_pct=50,
    min_liquidity_usd=10_000,
    min_volume_usd=50_000,
    peak_volume_window_hours=24,
    min_fdv_usd=100_000,
    max_fdv_usd=100_000_000,
    max_pool_age_hours=720,
    min_multiplier=5,
    min_retention_pct=30,
    confirmation_hours=24,
    confirmation_timeout_hours=168,
    sniper_blocks=3,
    min_buy_usd=500,
    max_buyers=300,
)


def candles(closes: list[float], volume: float = 10_000) -> list[Candle]:
    return [Candle(i * HOUR, c, c, c, c, volume) for i, c in enumerate(closes)]


# Bas 0.5 à l'heure 2, pic 5.0 à l'heure 4 (×10), puis 30 heures à 3.0 (rétention 60 %).
EXPLOSIVE = [1.0, 0.8, 0.5, 2.0, 5.0] + [3.0] * 30
AFTER_ALL = 100 * HOUR


@pytest.mark.parametrize(
    ("age", "expected"),
    [(10, ("hour", 1)), (1000, ("hour", 1)), (1001, ("hour", 4)), (4001, ("hour", 12)), (12001, ("day", 1))],
)
def test_choose_resolution(age, expected):
    assert choose_resolution(age) == expected


def test_best_run_uses_lowest_close_before_peak():
    low, peak, multiplier = find_best_run(candles([2.0, 1.0, 4.0, 0.5, 1.5]))
    assert (low.close, peak.close, multiplier) == (1.0, 4.0, 4.0)


def test_best_run_none_when_price_only_falls():
    assert find_best_run(candles([5.0, 4.0, 3.0])) is None


def test_confirmed_explosion():
    verdict = detect_explosion(
        candles(EXPLOSIVE), now_ts=AFTER_ALL, current_liquidity_usd=50_000, thresholds=T
    )
    assert verdict.status == CONFIRMED
    assert verdict.signal.low_ts == 2 * HOUR
    assert verdict.signal.peak_ts == 4 * HOUR
    assert verdict.signal.multiplier == 10.0
    assert verdict.signal.retention_pct == 60.0


def test_rejects_small_move():
    verdict = detect_explosion(
        candles([1.0, 2.0, 3.0]), now_ts=AFTER_ALL, current_liquidity_usd=50_000, thresholds=T
    )
    assert (verdict.status, verdict.reason) == (REJECTED, "no_explosion")


def test_multiplier_exactly_at_threshold_passes():
    closes = [1.0, 5.0] + [5.0] * 30
    verdict = detect_explosion(
        candles(closes), now_ts=AFTER_ALL, current_liquidity_usd=50_000, thresholds=T
    )
    assert verdict.status == CONFIRMED


def test_rejects_low_volume_around_peak():
    verdict = detect_explosion(
        candles(EXPLOSIVE, volume=100), now_ts=AFTER_ALL, current_liquidity_usd=50_000, thresholds=T
    )
    assert (verdict.status, verdict.reason) == (REJECTED, "low_volume")


def test_rejects_drained_pool():
    verdict = detect_explosion(
        candles(EXPLOSIVE), now_ts=AFTER_ALL, current_liquidity_usd=500, thresholds=T
    )
    assert (verdict.status, verdict.reason) == (REJECTED, "low_liquidity")


def test_rejects_rug_after_peak():
    closes = [1.0, 0.5, 5.0] + [0.6] * 30
    verdict = detect_explosion(
        candles(closes), now_ts=AFTER_ALL, current_liquidity_usd=50_000, thresholds=T
    )
    assert (verdict.status, verdict.reason) == (REJECTED, "rug")
    assert verdict.signal.retention_pct == 12.0


def test_waits_when_peak_is_too_recent():
    verdict = detect_explosion(
        candles(EXPLOSIVE[:6]), now_ts=6 * HOUR, current_liquidity_usd=50_000, thresholds=T
    )
    assert verdict.status == WAITING
    assert verdict.next_check_ts == 4 * HOUR + 24 * HOUR
    assert verdict.signal.retention_pct is None


def test_threshold_override_changes_verdict():
    verdict = detect_explosion(
        candles([1.0, 3.0] + [3.0] * 30),
        now_ts=AFTER_ALL,
        current_liquidity_usd=50_000,
        thresholds=replace(T, min_multiplier=2),
    )
    assert verdict.status == CONFIRMED
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/discovery/tests/test_explosion.py -v"`
Expected: FAIL avec `ModuleNotFoundError`.

- [ ] **Step 3: Implémenter**

`backend/apps/discovery/services/explosion.py` :

```python
"""Détection d'explosion sur des bougies OHLCV. Fonctions pures, sans base ni réseau."""

from dataclasses import dataclass

from apps.discovery.services.settings import Thresholds
from integrations.geckoterminal import MAX_OHLCV_CANDLES, Candle

CONFIRMED = "confirmed"
WAITING = "waiting"
REJECTED = "rejected"

RESOLUTIONS = (("hour", 1), ("hour", 4), ("hour", 12))


@dataclass(frozen=True)
class ExplosionSignal:
    low_ts: int
    peak_ts: int
    multiplier: float
    retention_pct: float | None


@dataclass(frozen=True)
class Verdict:
    status: str
    reason: str = ""
    signal: ExplosionSignal | None = None
    next_check_ts: int | None = None


def choose_resolution(pool_age_hours: float) -> tuple[str, int]:
    """Plus petite taille de bougie qui couvre toute la vie du pool en ≤ 1 000 bougies."""
    for timeframe, aggregate in RESOLUTIONS:
        if pool_age_hours <= aggregate * MAX_OHLCV_CANDLES:
            return timeframe, aggregate
    return "day", 1


def find_best_run(candles: list[Candle]) -> tuple[Candle, Candle, float] | None:
    """Meilleur rapport clôture du pic / plus bas précédent, en un seul passage."""
    best = None
    low = None
    for candle in candles:
        if candle.close <= 0:
            continue
        if low is None or candle.close < low.close:
            low = candle
            continue
        ratio = candle.close / low.close
        if best is None or ratio > best[2]:
            best = (low, candle, ratio)
    return best


def detect_explosion(
    candles: list[Candle], *, now_ts: int, current_liquidity_usd: float, thresholds: Thresholds
) -> Verdict:
    run = find_best_run(candles)
    if run is None or run[2] < thresholds.min_multiplier:
        return Verdict(REJECTED, "no_explosion")
    low, peak, multiplier = run

    half_window = thresholds.peak_volume_window_hours * 3600 / 2
    volume = sum(c.volume for c in candles if abs(c.ts - peak.ts) <= half_window)
    if volume < thresholds.min_volume_usd:
        return Verdict(REJECTED, "low_volume")
    if current_liquidity_usd < thresholds.min_liquidity_usd:
        return Verdict(REJECTED, "low_liquidity")

    confirm_ts = peak.ts + thresholds.confirmation_hours * 3600
    if now_ts < confirm_ts:
        signal = ExplosionSignal(low.ts, peak.ts, round(multiplier, 2), None)
        return Verdict(WAITING, signal=signal, next_check_ts=confirm_ts)

    later = [c for c in candles if c.ts >= confirm_ts]
    reference = later[0] if later else candles[-1]
    retention = round(reference.close / peak.close * 100, 2)
    signal = ExplosionSignal(low.ts, peak.ts, round(multiplier, 2), retention)
    if retention < thresholds.min_retention_pct:
        return Verdict(REJECTED, "rug", signal=signal)
    return Verdict(CONFIRMED, signal=signal)
```

- [ ] **Step 4: Lancer les tests**

Run: `make test args="apps/discovery/tests/test_explosion.py -v"`
Expected: PASS.

- [ ] **Step 5: Lint et commit**

```bash
make fmt && make lint
git add backend/apps/discovery/services/explosion.py backend/apps/discovery/tests/test_explosion.py
git commit -m "feat(discovery): détection d'explosion (multiplicateur, volume, liquidité, rétention)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: Recherche de bloc et agrégation des acheteurs (fonctions pures)

**Files:**
- Create: `backend/apps/discovery/services/blocks.py`, `backend/apps/discovery/services/buyers.py`
- Create: `backend/apps/discovery/tests/test_blocks.py`, `backend/apps/discovery/tests/test_buyers.py`

**Interfaces:**
- Consumes: `Transfer` (Task 5), `Candle` (Task 3).
- Produces:
  - `find_block_at(target_ts: int, low: int, high: int, timestamp_of: Callable[[int], int]) -> int` : premier bloc de `[low, high]` dont le timestamp est ≥ `target_ts` (renvoie `low` si la cible est avant, `high` si elle est après).
  - `BuyerStats(wallet: str, first_buy_block: int, first_buy_ts: int, bought_amount: int = 0, bought_usd: float = 0.0, sold_amount: int = 0, is_sniper: bool = False)` (dataclass modifiable).
  - `aggregate_buyers(transfers: list[Transfer], *, low_block: int, peak_block: int, pool_created_block: int, candles: list[Candle], decimals: int, sniper_blocks: int, min_buy_usd: float, max_buyers: int) -> list[BuyerStats]` triés par `bought_usd` décroissant.

- [ ] **Step 1: Écrire les tests de `find_block_at`**

`backend/apps/discovery/tests/test_blocks.py` :

```python
import pytest

from apps.discovery.services.blocks import find_block_at


def regular(genesis: int = 1000, block_time: int = 2):
    calls = []

    def timestamp_of(block: int) -> int:
        calls.append(block)
        return genesis + block * block_time

    return timestamp_of, calls


@pytest.mark.parametrize(("target", "expected"), [(1000, 0), (1001, 1), (1002, 1), (21000, 10000)])
def test_finds_first_block_at_or_after_target(target, expected):
    timestamp_of, _ = regular()
    assert find_block_at(target, 0, 20_000_000, timestamp_of) == expected


def test_clamps_to_range():
    timestamp_of, _ = regular()
    assert find_block_at(0, 100, 200, timestamp_of) == 100
    assert find_block_at(10**12, 100, 200, timestamp_of) == 200


def test_converges_fast_on_regular_chains():
    timestamp_of, calls = regular()
    find_block_at(1000 + 2 * 12_345_678, 0, 20_000_000, timestamp_of)
    assert len(calls) <= 6


def test_stays_logarithmic_on_irregular_chains():
    # Blocs très lents au début puis très rapides : l'interpolation seule dégénère.
    def timestamp_of(block: int) -> int:
        return block * 1000 if block < 1000 else 1_000_000 + (block - 1000)

    calls = []

    def counting(block: int) -> int:
        calls.append(block)
        return timestamp_of(block)

    assert find_block_at(500_000, 0, 10_000_000, counting) == 500
    assert len(calls) <= 60
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/discovery/tests/test_blocks.py -v"`
Expected: FAIL avec `ModuleNotFoundError`.

- [ ] **Step 3: Implémenter `blocks.py`**

`backend/apps/discovery/services/blocks.py` :

```python
"""Conversion timestamp → numéro de bloc. Fonction pure : la source des timestamps est injectée."""

from collections.abc import Callable


def find_block_at(target_ts: int, low: int, high: int, timestamp_of: Callable[[int], int]) -> int:
    """Premier bloc de [low, high] dont le timestamp est >= target_ts.

    Alterne interpolation (rapide quand le temps de bloc est régulier) et dichotomie
    (garantit un nombre d'appels logarithmique quand il ne l'est pas).
    """
    low_ts, high_ts = timestamp_of(low), timestamp_of(high)
    if target_ts <= low_ts:
        return low
    if target_ts > high_ts:
        return high
    # Invariant : timestamp(low) < target_ts <= timestamp(high)
    step = 0
    while high - low > 1:
        if step % 2 == 0 and high_ts > low_ts:
            guess = low + (target_ts - low_ts) * (high - low) // (high_ts - low_ts)
        else:
            guess = (low + high) // 2
        middle = min(max(guess, low + 1), high - 1)
        middle_ts = timestamp_of(middle)
        if middle_ts < target_ts:
            low, low_ts = middle, middle_ts
        else:
            high, high_ts = middle, middle_ts
        step += 1
    return high
```

- [ ] **Step 4: Lancer les tests**

Run: `make test args="apps/discovery/tests/test_blocks.py -v"`
Expected: PASS.

- [ ] **Step 5: Écrire les tests de `aggregate_buyers`**

`backend/apps/discovery/tests/test_buyers.py` :

```python
from apps.discovery.services.buyers import ZERO_ADDRESS, aggregate_buyers
from integrations.geckoterminal import Candle
from integrations.hypersync import Transfer

UNIT = 10**18
POOL = "0x" + "b" * 40
ROUTER = "0x" + "9" * 40
ALICE = "0x" + "a" * 40
SNIPER = "0x" + "5" * 40
SMALL = "0x" + "c" * 40
AIRDROPPED = "0x" + "d" * 40
FRIEND = "0x" + "e" * 40

# Prix 1 $ jusqu'à t=1000, puis 2 $.
CANDLES = [Candle(0, 1, 1, 1, 1.0, 0), Candle(1000, 2, 2, 2, 2.0, 0)]


def buy(wallet: str, block: int, tokens: int, ts: int = 10, sender: str = POOL) -> Transfer:
    return Transfer(block=block, timestamp=ts, tx_from=wallet, sender=sender, recipient=wallet, amount=tokens * UNIT)


def run(transfers, **overrides):
    params = dict(
        low_block=1000,
        peak_block=2000,
        pool_created_block=100,
        candles=CANDLES,
        decimals=18,
        sniper_blocks=3,
        min_buy_usd=500,
        max_buyers=300,
    )
    params.update(overrides)
    return aggregate_buyers(transfers, **params)


def test_buy_is_signer_receiving_tokens():
    [alice] = run([buy(ALICE, 200, 600)])
    assert alice.wallet == ALICE
    assert alice.first_buy_block == 200
    assert alice.bought_amount == 600 * UNIT
    assert alice.bought_usd == 600.0
    assert not alice.is_sniper


def test_buys_through_router_count():
    assert run([buy(ALICE, 200, 600, sender=ROUTER)])[0].wallet == ALICE


def test_usd_uses_candle_price_at_buy_time():
    [alice] = run([buy(ALICE, 200, 300, ts=10), buy(ALICE, 900, 300, ts=1500)])
    assert alice.bought_usd == 300 * 1.0 + 300 * 2.0


def test_buys_after_low_block_are_not_early():
    assert run([buy(ALICE, 1001, 10_000)]) == []


def test_airdrops_and_mints_are_ignored():
    airdrop = Transfer(block=200, timestamp=10, tx_from=FRIEND, sender=FRIEND, recipient=AIRDROPPED, amount=10_000 * UNIT)
    mint = Transfer(block=200, timestamp=10, tx_from=ALICE, sender=ZERO_ADDRESS, recipient=ALICE, amount=10_000 * UNIT)
    assert run([airdrop, mint]) == []


def test_tokens_sent_before_peak_count_as_sold():
    sell = Transfer(block=1500, timestamp=1500, tx_from=ALICE, sender=ALICE, recipient=POOL, amount=200 * UNIT)
    after_peak = Transfer(block=2500, timestamp=2500, tx_from=ALICE, sender=ALICE, recipient=POOL, amount=100 * UNIT)
    [alice] = run([buy(ALICE, 200, 600), sell, after_peak])
    assert alice.sold_amount == 200 * UNIT


def test_sniper_flag():
    [sniper] = run([buy(SNIPER, 103, 1000)])
    assert sniper.is_sniper


def test_small_buyers_are_dropped():
    assert run([buy(SMALL, 200, 10)]) == []


def test_keeps_biggest_buyers_up_to_cap():
    buyers = run([buy(ALICE, 200, 600), buy(SNIPER, 300, 2000), buy(FRIEND, 400, 900)], max_buyers=2)
    assert [b.wallet for b in buyers] == [SNIPER, FRIEND]


def test_zero_cap_keeps_everyone_above_threshold():
    buyers = run([buy(ALICE, 200, 600), buy(SNIPER, 300, 2000), buy(FRIEND, 400, 900)], max_buyers=0)
    assert len(buyers) == 3
```

- [ ] **Step 6: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/discovery/tests/test_buyers.py -v"`
Expected: FAIL avec `ModuleNotFoundError`.

- [ ] **Step 7: Implémenter `buyers.py`**

`backend/apps/discovery/services/buyers.py` :

```python
"""Agrégation des early buyers depuis les transferts ERC-20. Fonction pure.

Un achat = le signataire de la transaction reçoit les tokens. Seul un EOA signe une
transaction : les contrats (bots, smart accounts, routers) sont exclus par construction.
"""

from bisect import bisect_right
from dataclasses import dataclass

from integrations.geckoterminal import Candle
from integrations.hypersync import Transfer

ZERO_ADDRESS = "0x" + "0" * 40


@dataclass
class BuyerStats:
    wallet: str
    first_buy_block: int
    first_buy_ts: int
    bought_amount: int = 0
    bought_usd: float = 0.0
    sold_amount: int = 0
    is_sniper: bool = False


def aggregate_buyers(
    transfers: list[Transfer],
    *,
    low_block: int,
    peak_block: int,
    pool_created_block: int,
    candles: list[Candle],
    decimals: int,
    sniper_blocks: int,
    min_buy_usd: float,
    max_buyers: int,
) -> list[BuyerStats]:
    candle_times = [candle.ts for candle in candles]

    def price_at(ts: int) -> float:
        index = bisect_right(candle_times, ts) - 1
        return candles[max(index, 0)].close

    scale = 10**decimals
    stats: dict[str, BuyerStats] = {}
    for transfer in sorted(transfers, key=lambda t: t.block):
        if transfer.block > peak_block:
            continue
        signer = transfer.tx_from
        is_buy = transfer.recipient == signer and transfer.sender not in (signer, ZERO_ADDRESS)
        if is_buy and transfer.block <= low_block:
            buyer = stats.get(signer)
            if buyer is None:
                buyer = stats[signer] = BuyerStats(signer, transfer.block, transfer.timestamp)
            buyer.bought_amount += transfer.amount
            buyer.bought_usd += transfer.amount / scale * price_at(transfer.timestamp)
        elif transfer.sender == signer and transfer.recipient != signer and signer in stats:
            stats[signer].sold_amount += transfer.amount

    kept = [buyer for buyer in stats.values() if buyer.bought_usd >= min_buy_usd]
    for buyer in kept:
        buyer.bought_usd = round(buyer.bought_usd, 2)
        buyer.is_sniper = buyer.first_buy_block - pool_created_block <= sniper_blocks
    kept.sort(key=lambda buyer: buyer.bought_usd, reverse=True)
    return kept[:max_buyers] if max_buyers > 0 else kept
```

- [ ] **Step 8: Lancer les tests**

Run: `make test args="apps/discovery/tests/test_blocks.py apps/discovery/tests/test_buyers.py -v"`
Expected: PASS.

- [ ] **Step 9: Lint et commit**

```bash
make fmt && make lint
git add backend/apps/discovery/services/blocks.py backend/apps/discovery/services/buyers.py backend/apps/discovery/tests/test_blocks.py backend/apps/discovery/tests/test_buyers.py
git commit -m "feat(discovery): recherche de bloc et agrégation des early buyers (EOA, USD, snipers)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Faux clients, construction des clients et synchronisation des chaînes

**Files:**
- Create: `backend/apps/discovery/tests/fakes.py`
- Create: `backend/apps/discovery/services/clients.py`
- Create: `backend/apps/discovery/services/chains.py`
- Create: `backend/apps/discovery/tests/test_chains.py`, `backend/apps/discovery/tests/test_clients.py`

**Interfaces:**
- Consumes: clients des Tasks 3 à 5 ; `PipelineSettings` (Task 6).
- Produces:
  - `apps.discovery.services.clients` : `geckoterminal(cfg) -> GeckoTerminalClient`, `coingecko(cfg) -> CoinGeckoClient`, `zerion(cfg) -> ZerionClient` (lève `ImproperlyConfigured` sans `ZERION_API_KEY`), `hypersync_directory(cfg) -> HyperSyncDirectory`, `hypersync(chain, cfg) -> HyperSyncClient` (lève `ImproperlyConfigured` sans `ENVIO_API_TOKEN`). `cfg` est un `PipelineSettings`.
  - `apps.discovery.services.chains.sync_chains(gt, coingecko, directory, zerion) -> int` (nombre de chaînes synchronisées ; ne modifie jamais `is_enabled`).
  - `apps.discovery.tests.fakes` : `FakeGeckoTerminal`, `FakeCoinGecko`, `FakeDirectory`, `FakeZerion`, `FakeHyperSync`, `explosive_candles(start_ts)`, constantes `NOW`, `POOL_CREATED`, `TOKEN`, `POOL`, `ALICE`, `SNIPER`, `SMALL`, `STRANGER`, `UNIT`, `GENESIS_TS`, `BLOCK_TIME`, `block_of(ts)`.

- [ ] **Step 1: Écrire les faux clients partagés**

`backend/apps/discovery/tests/fakes.py` :

```python
"""Faux clients déterministes : un token qui explose sur Base, avec trois acheteurs."""

from datetime import UTC, datetime, timedelta

from integrations.geckoterminal import Candle, GtNetwork, GtPool, GtToken
from integrations.hypersync import Transfer

HOUR = 3600
UNIT = 10**18
NOW = datetime(2026, 9, 20, 12, tzinfo=UTC)
POOL_CREATED = NOW - timedelta(hours=100)
TOKEN = "0x" + "1" * 40
POOL = "0x" + "2" * 40
ALICE = "0x" + "a" * 40
SNIPER = "0x" + "5" * 40
SMALL = "0x" + "c" * 40
STRANGER = "0x" + "d" * 40

# Chaîne simulée : un bloc toutes les 2 secondes, le pool est créé au bloc 500.
BLOCK_TIME = 2
GENESIS_TS = int(POOL_CREATED.timestamp()) - 500 * BLOCK_TIME


def block_of(ts: int) -> int:
    return (ts - GENESIS_TS) // BLOCK_TIME


def explosive_candles(start_ts: int) -> list[Candle]:
    """Bas 0.5 à l'heure 10, pic 5.0 à l'heure 20 (×10), puis 3.0 (rétention 60 %)."""
    closes = [1.0] * 10 + [0.5] + [0.5 + 0.45 * i for i in range(1, 10)] + [5.0] + [3.0] * 30
    return [Candle(start_ts + i * HOUR, c, c, c, c, 100_000) for i, c in enumerate(closes)]


def make_pool(**overrides) -> GtPool:
    values = dict(
        network="base",
        address=POOL,
        token_address=TOKEN,
        token_symbol="BOOM",
        token_decimals=18,
        price_change_24h_pct=120.0,
        volume_24h_usd=200_000.0,
        liquidity_usd=50_000.0,
        fdv_usd=1_000_000.0,
        created_at=POOL_CREATED,
    )
    values.update(overrides)
    return GtPool(**values)


class FakeGeckoTerminal:
    def __init__(self, trending=None, volume=None, token_pools=None, candles=None):
        self.trending = [make_pool()] if trending is None else trending
        self.volume = volume or {}
        self.pools_by_token = token_pools
        self.candles = candles if candles is not None else explosive_candles(int(POOL_CREATED.timestamp()))

    def networks(self):
        return [GtNetwork("base", "Base", "base"), GtNetwork("solana", "Solana", "solana")]

    def trending_pools(self, page):
        return self.trending if page == 1 else []

    def top_volume_pools(self, network, page):
        return self.volume.get(network, []) if page == 1 else []

    def token_pools(self, network, token_address):
        if self.pools_by_token is not None:
            return self.pools_by_token
        return [make_pool()]

    def token(self, network, address):
        return GtToken(address, "BOOM", 18)

    def ohlcv(self, network, pool_address, timeframe, aggregate):
        return self.candles


class FakeCoinGecko:
    def platform_chain_ids(self):
        return {"base": 8453}


class FakeDirectory:
    def supported_chain_ids(self):
        return {8453}


class FakeZerion:
    def chain_ids(self):
        return {8453: "base"}


class FakeHyperSync:
    def __init__(self, transfers=None, error=None):
        self._transfers = transfers
        self._error = error
        self.transfer_calls = []

    def height(self):
        return block_of(int(NOW.timestamp()))

    def block_timestamp(self, number):
        return GENESIS_TS + number * BLOCK_TIME

    def transfers(self, token, from_block, to_block, max_transfers):
        self.transfer_calls.append((token, from_block, to_block, max_transfers))
        if self._error:
            raise self._error
        if self._transfers is not None:
            return self._transfers
        start = int(POOL_CREATED.timestamp())
        low_ts = start + 10 * HOUR
        return [
            # Sniper : 1 bloc après la création du pool, 2 000 tokens à 1 $.
            self._buy(SNIPER, 501, 2000),
            # Alice : 1 000 tokens à 1 $, puis sort 400 tokens avant le pic.
            self._buy(ALICE, 600, 1000),
            Transfer(block_of(low_ts + 3 * HOUR), low_ts + 3 * HOUR, ALICE, ALICE, POOL, 400 * UNIT),
            # Petit acheteur : 10 tokens → 10 $, ignoré.
            self._buy(SMALL, 700, 10),
            # Airdrop : le destinataire n'a pas signé, ignoré.
            Transfer(800, self.block_timestamp(800), SMALL, POOL, STRANGER, 5000 * UNIT),
        ]

    def _buy(self, wallet, block, tokens):
        return Transfer(block, self.block_timestamp(block), wallet, POOL, wallet, tokens * UNIT)
```

- [ ] **Step 2: Écrire les tests**

`backend/apps/discovery/tests/test_chains.py` :

```python
import pytest

from apps.discovery.models import Chain
from apps.discovery.services.chains import sync_chains
from apps.discovery.tests.fakes import FakeCoinGecko, FakeDirectory, FakeGeckoTerminal, FakeZerion

pytestmark = pytest.mark.django_db


def run_sync() -> int:
    return sync_chains(FakeGeckoTerminal(), FakeCoinGecko(), FakeDirectory(), FakeZerion())


def test_sync_creates_chains_with_support_flags():
    assert run_sync() == 2
    base = Chain.objects.get(gt_id="base")
    assert (base.evm_id, base.zerion_id, base.hypersync_supported) == (8453, "base", True)
    assert base.is_active
    solana = Chain.objects.get(gt_id="solana")
    assert (solana.evm_id, solana.zerion_id, solana.hypersync_supported) == (None, "", False)
    assert not solana.is_active


def test_sync_never_touches_is_enabled():
    run_sync()
    Chain.objects.filter(gt_id="base").update(is_enabled=False)
    run_sync()
    assert not Chain.objects.get(gt_id="base").is_enabled


def test_sync_is_idempotent():
    run_sync()
    run_sync()
    assert Chain.objects.count() == 2
```

`backend/apps/discovery/tests/test_clients.py` :

```python
import pytest
from django.core.exceptions import ImproperlyConfigured

from apps.discovery.models import PipelineSettings
from apps.discovery.services import clients
from apps.discovery.tests.factories import make_chain

pytestmark = pytest.mark.django_db


def test_zerion_requires_api_key(settings):
    settings.ZERION_API_KEY = ""
    with pytest.raises(ImproperlyConfigured):
        clients.zerion(PipelineSettings.load())


def test_hypersync_requires_api_token(settings):
    settings.ENVIO_API_TOKEN = ""
    with pytest.raises(ImproperlyConfigured):
        clients.hypersync(make_chain(), PipelineSettings.load())


def test_geckoterminal_client_is_built_from_settings():
    assert clients.geckoterminal(PipelineSettings.load()) is not None
```

- [ ] **Step 3: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/discovery/tests/test_chains.py apps/discovery/tests/test_clients.py -v"`
Expected: FAIL avec `ModuleNotFoundError`.

- [ ] **Step 4: Implémenter `clients.py`**

`backend/apps/discovery/services/clients.py` :

```python
"""Construction des clients externes à partir des réglages du pipeline et des clés d'environnement."""

import redis
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from apps.discovery.models import Chain, PipelineSettings
from integrations import coingecko as cg
from integrations import geckoterminal as gt
from integrations import zerion as zr
from integrations.http import JsonHttpClient
from integrations.hypersync import CHAINS_URL, HyperSyncClient, HyperSyncDirectory
from integrations.ratelimit import NoopLimiter, RateLimiter


def _redis() -> redis.Redis:
    return redis.Redis.from_url(settings.REDIS_URL)


def _http(base_url: str, cfg: PipelineSettings, limiter=None, **kwargs) -> JsonHttpClient:
    return JsonHttpClient(
        base_url,
        limiter=limiter or NoopLimiter(),
        timeout=cfg.http_timeout_seconds,
        max_retries=cfg.http_max_retries,
        **kwargs,
    )


def geckoterminal(cfg: PipelineSettings) -> gt.GeckoTerminalClient:
    limiter = RateLimiter(_redis(), "geckoterminal", cfg.gecko_requests_per_min)
    return gt.GeckoTerminalClient(_http(gt.BASE_URL, cfg, limiter))


def coingecko(cfg: PipelineSettings) -> cg.CoinGeckoClient:
    headers = {"x-cg-demo-api-key": settings.COINGECKO_API_KEY} if settings.COINGECKO_API_KEY else None
    return cg.CoinGeckoClient(_http(cg.BASE_URL, cfg, headers=headers))


def zerion(cfg: PipelineSettings) -> zr.ZerionClient:
    if not settings.ZERION_API_KEY:
        raise ImproperlyConfigured("ZERION_API_KEY manquante")
    return zr.ZerionClient(_http(zr.BASE_URL, cfg, auth=(settings.ZERION_API_KEY, "")))


def hypersync_directory(cfg: PipelineSettings) -> HyperSyncDirectory:
    return HyperSyncDirectory(_http(CHAINS_URL, cfg))


def hypersync(chain: Chain, cfg: PipelineSettings) -> HyperSyncClient:
    if not settings.ENVIO_API_TOKEN:
        raise ImproperlyConfigured("ENVIO_API_TOKEN manquant")
    limiter = RateLimiter(_redis(), "hypersync", cfg.hypersync_requests_per_min)
    return HyperSyncClient(
        chain.evm_id, settings.ENVIO_API_TOKEN, limiter, max_retries=cfg.http_max_retries
    )
```

- [ ] **Step 5: Implémenter `chains.py`**

`backend/apps/discovery/services/chains.py` :

```python
"""Synchronisation des chaînes : GeckoTerminal → CoinGecko (chain id) → HyperSync, Zerion."""

from apps.discovery.models import Chain


def sync_chains(gt, coingecko, directory, zerion) -> int:
    platform_ids = coingecko.platform_chain_ids()
    hypersync_ids = directory.supported_chain_ids()
    zerion_ids = zerion.chain_ids()
    count = 0
    for network in gt.networks():
        evm_id = platform_ids.get(network.coingecko_platform_id) if network.coingecko_platform_id else None
        Chain.objects.update_or_create(
            gt_id=network.gt_id,
            defaults={
                "name": network.name,
                "evm_id": evm_id,
                "zerion_id": zerion_ids.get(evm_id, "") if evm_id else "",
                "hypersync_supported": evm_id in hypersync_ids if evm_id else False,
            },
        )
        count += 1
    return count
```

- [ ] **Step 6: Lancer les tests**

Run: `make test args="apps/discovery -v"`
Expected: PASS.

- [ ] **Step 7: Lint et commit**

```bash
make fmt && make lint
git add backend/apps/discovery
git commit -m "feat(discovery): synchronisation dynamique des chaînes et construction des clients

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: Collecte des candidats et ajout manuel

**Files:**
- Create: `backend/apps/discovery/services/candidates.py`
- Create: `backend/apps/discovery/tests/test_candidates.py`

**Interfaces:**
- Consumes: `thresholds_for`, `Thresholds` (Task 7) ; `GtPool` (Task 3) ; modèles (Task 6) ; fakes (Task 10).
- Produces:
  - Constantes `SOURCE_TRENDING = "trending"`, `SOURCE_VOLUME = "volume"`, `SOURCE_MANUAL = "manual"`.
  - `passes_prefilter(pool: GtPool, thresholds: Thresholds, now: datetime) -> bool`.
  - `upsert_token_and_pools(chain: Chain, pools: list[GtPool]) -> Token` (le premier pool fournit symbole et décimales).
  - `collect_candidates(gt, cfg: PipelineSettings, now: datetime) -> int` (nombre de candidats créés).
  - `add_manual_candidate(chain: Chain, token_address: str, gt) -> Candidate` (lève `ValueError` si aucun pool ou candidat déjà en cours).

- [ ] **Step 1: Écrire les tests**

`backend/apps/discovery/tests/test_candidates.py` :

```python
from datetime import timedelta

import pytest

from apps.discovery.models import Candidate, PipelineSettings, Pool
from apps.discovery.services.candidates import (
    SOURCE_MANUAL,
    SOURCE_TRENDING,
    SOURCE_VOLUME,
    add_manual_candidate,
    collect_candidates,
    passes_prefilter,
)
from apps.discovery.services.settings import thresholds_for
from apps.discovery.tests.factories import make_chain
from apps.discovery.tests.fakes import NOW, POOL, TOKEN, FakeGeckoTerminal, make_pool

pytestmark = pytest.mark.django_db


@pytest.fixture
def chain():
    return make_chain()


@pytest.mark.parametrize(
    "overrides",
    [
        {"price_change_24h_pct": 10},
        {"liquidity_usd": 100},
        {"volume_24h_usd": 100},
        {"fdv_usd": 50_000},
        {"fdv_usd": 500_000_000},
        {"created_at": NOW - timedelta(days=60)},
        {"created_at": None},
    ],
)
def test_prefilter_rejects(chain, overrides):
    assert not passes_prefilter(make_pool(**overrides), thresholds_for(chain), NOW)


def test_prefilter_accepts(chain):
    assert passes_prefilter(make_pool(), thresholds_for(chain), NOW)


def test_collect_creates_candidate_with_sources_and_metrics(chain):
    gt = FakeGeckoTerminal(volume={"base": [make_pool(liquidity_usd=90_000.0, address="0x" + "3" * 40)]})
    assert collect_candidates(gt, PipelineSettings.load(), NOW) == 1
    candidate = Candidate.objects.get()
    assert candidate.token.address == TOKEN
    assert candidate.token.symbol == "BOOM"
    assert candidate.sources == [SOURCE_TRENDING, SOURCE_VOLUME]
    assert candidate.metrics["liquidity_usd"] == 90_000.0
    assert set(Pool.objects.values_list("address", flat=True)) == {POOL, "0x" + "3" * 40}


def test_collect_ignores_inactive_chains(chain):
    gt = FakeGeckoTerminal(trending=[make_pool(network="solana")])
    assert collect_candidates(gt, PipelineSettings.load(), NOW) == 0


def test_collect_is_idempotent(chain):
    collect_candidates(FakeGeckoTerminal(), PipelineSettings.load(), NOW)
    assert collect_candidates(FakeGeckoTerminal(), PipelineSettings.load(), NOW) == 0
    assert Candidate.objects.count() == 1


def test_recently_closed_token_is_not_recandidated(chain):
    collect_candidates(FakeGeckoTerminal(), PipelineSettings.load(), NOW)
    # queryset.update() ne touche pas updated_at : on le fixe explicitement.
    Candidate.objects.update(status=Candidate.Status.REJECTED, updated_at=NOW)
    assert collect_candidates(FakeGeckoTerminal(), PipelineSettings.load(), NOW) == 0


def test_token_closed_before_cooldown_can_come_back(chain):
    collect_candidates(FakeGeckoTerminal(), PipelineSettings.load(), NOW)
    # queryset.update() ne touche pas updated_at : on le fixe explicitement.
    Candidate.objects.update(status=Candidate.Status.REJECTED, updated_at=NOW)
    later = NOW + timedelta(hours=73)
    assert collect_candidates(FakeGeckoTerminal(), PipelineSettings.load(), later) == 1


def test_add_manual_candidate_skips_prefilters(chain):
    gt = FakeGeckoTerminal(token_pools=[make_pool(price_change_24h_pct=-50)])
    candidate = add_manual_candidate(chain, TOKEN.upper().replace("0X", "0x"), gt)
    assert candidate.sources == [SOURCE_MANUAL]
    assert candidate.token.address == TOKEN


def test_add_manual_candidate_without_pool_fails(chain):
    with pytest.raises(ValueError):
        add_manual_candidate(chain, TOKEN, FakeGeckoTerminal(token_pools=[]))


def test_add_manual_candidate_twice_fails(chain):
    add_manual_candidate(chain, TOKEN, FakeGeckoTerminal())
    with pytest.raises(ValueError):
        add_manual_candidate(chain, TOKEN, FakeGeckoTerminal())
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/discovery/tests/test_candidates.py -v"`
Expected: FAIL avec `ModuleNotFoundError`.

- [ ] **Step 3: Implémenter**

`backend/apps/discovery/services/candidates.py` :

```python
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
```

- [ ] **Step 4: Lancer les tests**

Run: `make test args="apps/discovery/tests/test_candidates.py -v"`
Expected: PASS.

- [ ] **Step 5: Lint et commit**

```bash
make fmt && make lint
git add backend/apps/discovery/services/candidates.py backend/apps/discovery/tests/test_candidates.py
git commit -m "feat(discovery): collecte multi-chaînes des candidats et ajout manuel

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 12: Analyse d'un candidat

**Files:**
- Create: `backend/apps/discovery/services/analysis.py`
- Create: `backend/apps/discovery/tests/test_analysis.py`

**Interfaces:**
- Consumes: `choose_resolution`, `detect_explosion`, `CONFIRMED`, `WAITING`, `REJECTED` (Task 8) ; `find_block_at` (Task 9) ; `thresholds_for` (Task 7) ; `upsert_token_and_pools` (Task 11) ; fakes (Task 10).
- Produces:
  - `PriceHistory(pools: list[GtPool], main: GtPool, candles: list[Candle])` et `fetch_price_history(gt, chain: Chain, token: Token, now: datetime) -> PriceHistory` (met aussi à jour symbole, décimales et pools du token ; lève `ValueError("no_pool")` sans pool).
  - `reject(candidate: Candidate, reason: str) -> str` (passe en `REJECTED`, renvoie `"rejected"`).
  - `analyze_candidate(candidate: Candidate, *, gt, hypersync_for: Callable[[Chain], HyperSyncClient], now: datetime) -> str` : renvoie le nouveau statut (`"confirmed"`, `"waiting_confirmation"` ou `"rejected"`). Raisons ajoutées : `chain_inactive`, `confirmation_timeout`, `already_extracted`, `no_pool`.

- [ ] **Step 1: Écrire les tests**

`backend/apps/discovery/tests/test_analysis.py` :

```python
from datetime import timedelta

import pytest

from apps.discovery.models import Candidate, Explosion, Pool
from apps.discovery.services.analysis import analyze_candidate
from apps.discovery.tests.factories import make_candidate, make_chain, make_token
from apps.discovery.tests.fakes import (
    GENESIS_TS,
    HOUR,
    NOW,
    POOL,
    POOL_CREATED,
    TOKEN,
    FakeGeckoTerminal,
    FakeHyperSync,
    block_of,
    explosive_candles,
)

pytestmark = pytest.mark.django_db


@pytest.fixture
def candidate():
    return make_candidate(make_token(make_chain(), address=TOKEN, decimals=9))


def analyze(candidate, gt=None, now=NOW):
    return analyze_candidate(
        candidate, gt=gt or FakeGeckoTerminal(), hypersync_for=lambda chain: FakeHyperSync(), now=now
    )


def test_confirms_explosion_and_stores_blocks(candidate):
    assert analyze(candidate) == Candidate.Status.CONFIRMED
    candidate.refresh_from_db()
    assert candidate.status == Candidate.Status.CONFIRMED
    explosion = candidate.explosion
    start = int(POOL_CREATED.timestamp())
    assert explosion.low_block == block_of(start + 10 * HOUR)
    assert explosion.peak_block == block_of(start + 20 * HOUR)
    assert float(explosion.multiplier) == 10.0
    assert float(explosion.retention_pct) == 60.0
    assert candidate.token.decimals == 18
    assert Pool.objects.get(address=POOL).created_block == 500


def test_rejects_when_no_explosion(candidate):
    flat = FakeGeckoTerminal(candles=explosive_candles(0)[:10])
    assert analyze(candidate, gt=flat) == Candidate.Status.REJECTED
    candidate.refresh_from_db()
    assert candidate.rejection_reason == "no_explosion"


def test_waits_when_peak_is_recent(candidate):
    recent = NOW - timedelta(hours=100) + timedelta(hours=30)
    assert analyze(candidate, now=recent) == Candidate.Status.WAITING_CONFIRMATION
    candidate.refresh_from_db()
    peak = int(POOL_CREATED.timestamp()) + 20 * HOUR
    assert candidate.next_check_at.timestamp() == peak + 24 * HOUR
    assert not Explosion.objects.exists()


def test_waiting_too_long_is_rejected(candidate):
    Candidate.objects.filter(pk=candidate.pk).update(
        status=Candidate.Status.WAITING_CONFIRMATION, created_at=NOW - timedelta(hours=200)
    )
    candidate.refresh_from_db()
    assert analyze(candidate) == Candidate.Status.REJECTED
    candidate.refresh_from_db()
    assert candidate.rejection_reason == "confirmation_timeout"


def test_inactive_chain_is_rejected(candidate):
    candidate.token.chain.is_enabled = False
    candidate.token.chain.save()
    assert analyze(candidate) == Candidate.Status.REJECTED
    candidate.refresh_from_db()
    assert candidate.rejection_reason == "chain_inactive"


def test_token_without_pool_is_rejected(candidate):
    assert analyze(candidate, gt=FakeGeckoTerminal(token_pools=[])) == Candidate.Status.REJECTED
    candidate.refresh_from_db()
    assert candidate.rejection_reason == "no_pool"


def test_same_peak_as_previous_explosion_is_rejected(candidate):
    analyze(candidate)
    Candidate.objects.filter(pk=candidate.pk).update(status=Candidate.Status.BUYERS_EXTRACTED)
    again = make_candidate(candidate.token)
    assert analyze(again) == Candidate.Status.REJECTED
    again.refresh_from_db()
    assert again.rejection_reason == "already_extracted"


def test_genesis_timestamp_is_consistent_with_fake():
    assert GENESIS_TS + 500 * 2 == int(POOL_CREATED.timestamp())
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/discovery/tests/test_analysis.py -v"`
Expected: FAIL avec `ModuleNotFoundError`.

- [ ] **Step 3: Implémenter**

`backend/apps/discovery/services/analysis.py` :

```python
"""Analyse d'un candidat : historique de prix, verdict d'explosion, conversion en blocs."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from apps.discovery.models import Candidate, Chain, Explosion, Token
from apps.discovery.services.blocks import find_block_at
from apps.discovery.services.candidates import upsert_token_and_pools
from apps.discovery.services.explosion import REJECTED, WAITING, choose_resolution, detect_explosion
from apps.discovery.services.settings import thresholds_for
from integrations.geckoterminal import Candle, GtPool


@dataclass(frozen=True)
class PriceHistory:
    pools: list[GtPool]
    main: GtPool
    candles: list[Candle]


def _datetime(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=UTC)


def fetch_price_history(gt, chain: Chain, token: Token, now: datetime) -> PriceHistory:
    info = gt.token(chain.gt_id, token.address)
    token.symbol = info.symbol[:64]
    token.decimals = info.decimals
    token.save(update_fields=["symbol", "decimals"])
    pools = gt.token_pools(chain.gt_id, token.address)
    if not pools:
        raise ValueError("no_pool")
    main = max(pools, key=lambda pool: pool.liquidity_usd)
    upsert_token_and_pools(chain, [main] + [p for p in pools if p is not main])
    created_at = main.created_at or now - timedelta(days=3650)
    timeframe, aggregate = choose_resolution((now - created_at).total_seconds() / 3600)
    candles = gt.ohlcv(chain.gt_id, main.address, timeframe, aggregate)
    return PriceHistory(pools, main, candles)


def reject(candidate: Candidate, reason: str) -> str:
    candidate.status = Candidate.Status.REJECTED
    candidate.rejection_reason = reason
    candidate.save(update_fields=["status", "rejection_reason", "updated_at"])
    return candidate.status


def analyze_candidate(
    candidate: Candidate, *, gt, hypersync_for: Callable[[Chain], object], now: datetime
) -> str:
    token = candidate.token
    chain = token.chain
    if not chain.is_active:
        return reject(candidate, "chain_inactive")
    thresholds = thresholds_for(chain)
    timeout = candidate.created_at + timedelta(hours=thresholds.confirmation_timeout_hours)
    if candidate.status == Candidate.Status.WAITING_CONFIRMATION and timeout < now:
        return reject(candidate, "confirmation_timeout")

    try:
        history = fetch_price_history(gt, chain, token, now)
    except ValueError:
        return reject(candidate, "no_pool")

    verdict = detect_explosion(
        history.candles,
        now_ts=int(now.timestamp()),
        current_liquidity_usd=sum(pool.liquidity_usd for pool in history.pools),
        thresholds=thresholds,
    )
    if verdict.status == REJECTED:
        return reject(candidate, verdict.reason)

    signal = verdict.signal
    previous = (
        Explosion.objects.filter(candidate__token=token)
        .exclude(candidate=candidate)
        .order_by("-peak_at")
        .first()
    )
    if previous and previous.peak_at >= _datetime(signal.peak_ts):
        return reject(candidate, "already_extracted")

    if verdict.status == WAITING:
        candidate.status = Candidate.Status.WAITING_CONFIRMATION
        candidate.next_check_at = _datetime(verdict.next_check_ts)
        candidate.save(update_fields=["status", "next_check_at", "updated_at"])
        return candidate.status

    hypersync = hypersync_for(chain)
    height = hypersync.height()
    low_block = find_block_at(signal.low_ts, 0, height, hypersync.block_timestamp)
    peak_block = find_block_at(signal.peak_ts, low_block, height, hypersync.block_timestamp)
    created_by_address = {pool.address: pool.created_at for pool in history.pools}
    for pool in token.pools.filter(created_block__isnull=True):
        created_at = created_by_address.get(pool.address)
        if created_at is not None:
            pool.created_block = find_block_at(
                int(created_at.timestamp()), 0, low_block, hypersync.block_timestamp
            )
            pool.save(update_fields=["created_block"])

    Explosion.objects.update_or_create(
        candidate=candidate,
        defaults={
            "low_block": low_block,
            "low_at": _datetime(signal.low_ts),
            "peak_block": peak_block,
            "peak_at": _datetime(signal.peak_ts),
            "multiplier": signal.multiplier,
            "retention_pct": signal.retention_pct,
        },
    )
    candidate.status = Candidate.Status.CONFIRMED
    candidate.next_check_at = None
    candidate.save(update_fields=["status", "next_check_at", "updated_at"])
    return candidate.status
```

- [ ] **Step 4: Lancer les tests**

Run: `make test args="apps/discovery/tests/test_analysis.py -v"`
Expected: PASS.

- [ ] **Step 5: Lint et commit**

```bash
make fmt && make lint
git add backend/apps/discovery/services/analysis.py backend/apps/discovery/tests/test_analysis.py
git commit -m "feat(discovery): analyse d'un candidat (verdict, attente, conversion en blocs)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 13: Extraction des early buyers

**Files:**
- Create: `backend/apps/discovery/services/extraction.py`
- Create: `backend/apps/discovery/tests/test_extraction.py`

**Interfaces:**
- Consumes: `fetch_price_history`, `reject`, `analyze_candidate` (Task 12) ; `aggregate_buyers` (Task 9) ; `thresholds_for` (Task 7) ; `TooManyTransfers` (Task 2) ; fakes (Task 10).
- Produces: `extract_buyers(candidate: Candidate, *, gt, hypersync, cfg: PipelineSettings, now: datetime) -> int` (nombre d'acheteurs enregistrés ; passe le candidat en `BUYERS_EXTRACTED`, ou `REJECTED` avec `too_many_transfers` / `no_pool`). Les autres exceptions remontent à l'appelant.

- [ ] **Step 1: Écrire les tests**

`backend/apps/discovery/tests/test_extraction.py` :

```python
from decimal import Decimal

import pytest

from apps.discovery.models import Candidate, EarlyBuyer, PipelineSettings, Wallet
from apps.discovery.services.analysis import analyze_candidate
from apps.discovery.services.extraction import extract_buyers
from apps.discovery.tests.factories import make_candidate, make_chain, make_token
from apps.discovery.tests.fakes import (
    ALICE,
    NOW,
    SNIPER,
    TOKEN,
    UNIT,
    FakeGeckoTerminal,
    FakeHyperSync,
)
from integrations.errors import TooManyTransfers

pytestmark = pytest.mark.django_db


@pytest.fixture
def confirmed():
    candidate = make_candidate(make_token(make_chain(), address=TOKEN))
    analyze_candidate(
        candidate, gt=FakeGeckoTerminal(), hypersync_for=lambda chain: FakeHyperSync(), now=NOW
    )
    candidate.refresh_from_db()
    assert candidate.status == Candidate.Status.CONFIRMED
    return candidate


def extract(candidate, hypersync=None):
    return extract_buyers(
        candidate,
        gt=FakeGeckoTerminal(),
        hypersync=hypersync or FakeHyperSync(),
        cfg=PipelineSettings.load(),
        now=NOW,
    )


def test_stores_significant_eoa_buyers(confirmed):
    assert extract(confirmed) == 2
    confirmed.refresh_from_db()
    assert confirmed.status == Candidate.Status.BUYERS_EXTRACTED
    buyers = {b.wallet.address: b for b in EarlyBuyer.objects.select_related("wallet")}
    assert set(buyers) == {ALICE, SNIPER}
    alice = buyers[ALICE]
    assert alice.bought_amount == Decimal(1000 * UNIT)
    assert alice.bought_usd == Decimal("1000.00")
    assert alice.sold_amount == Decimal(400 * UNIT)
    assert not alice.is_sniper
    assert buyers[SNIPER].is_sniper
    assert buyers[SNIPER].bought_usd == Decimal("2000.00")


def test_queries_transfers_from_pool_creation_to_peak(confirmed):
    hypersync = FakeHyperSync()
    extract(confirmed, hypersync)
    token, from_block, to_block, cap = hypersync.transfer_calls[0]
    assert (token, from_block) == (TOKEN, 500)
    assert to_block == confirmed.explosion.peak_block + 1
    assert cap == PipelineSettings.load().max_transfers_per_token


def test_too_many_transfers_rejects(confirmed):
    assert extract(confirmed, FakeHyperSync(error=TooManyTransfers("trop"))) == 0
    confirmed.refresh_from_db()
    assert (confirmed.status, confirmed.rejection_reason) == (
        Candidate.Status.REJECTED,
        "too_many_transfers",
    )


def test_wallets_are_shared_between_explosions(confirmed):
    Wallet.objects.create(address=ALICE)
    extract(confirmed)
    assert Wallet.objects.filter(address=ALICE).count() == 1
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/discovery/tests/test_extraction.py -v"`
Expected: FAIL avec `ModuleNotFoundError`.

- [ ] **Step 3: Implémenter**

`backend/apps/discovery/services/extraction.py` :

```python
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


def extract_buyers(candidate: Candidate, *, gt, hypersync, cfg: PipelineSettings, now: datetime) -> int:
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
            Wallet.objects.filter(address__in=[b.wallet for b in buyers]).values_list("address", "id")
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
```

- [ ] **Step 4: Lancer les tests**

Run: `make test args="apps/discovery/tests/test_extraction.py -v"`
Expected: PASS.

- [ ] **Step 5: Lint et commit**

```bash
make fmt && make lint
git add backend/apps/discovery/services/extraction.py backend/apps/discovery/tests/test_extraction.py
git commit -m "feat(discovery): extraction des early buyers via HyperSync

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 14: Tâches Celery, gestion des échecs et pipeline quotidien

**Files:**
- Modify: `backend/apps/discovery/tasks.py`
- Create: `backend/apps/discovery/tests/test_tasks.py`

**Interfaces:**
- Consumes: `sync_chains` (Task 10), `collect_candidates` (Task 11), `analyze_candidate` (Task 12), `extract_buyers` (Task 13), `clients` (Task 10), `PipelineSettings.load()` (Task 6).
- Produces (tâches Celery de `apps.discovery.tasks`) : `sync_chains_task() -> int`, `collect_candidates_task() -> int`, `analyze_candidates_task() -> dict[str, int]`, `extract_early_buyers_task() -> int` (programme une sous-tâche par candidat confirmé), `extract_candidate_buyers(candidate_id: int) -> int`, `run_discovery() -> None` (chaîne les quatre). Fonction `record_failure(candidate: Candidate, exc: Exception, max_attempts: int) -> None`.

- [ ] **Step 1: Écrire les tests**

`backend/apps/discovery/tests/test_tasks.py` :

```python
from unittest.mock import patch

import pytest

from apps.discovery import tasks
from apps.discovery.models import Candidate, Chain, EarlyBuyer, PipelineSettings
from apps.discovery.tests.factories import make_candidate, make_token
from apps.discovery.tests.fakes import (
    NOW,
    FakeCoinGecko,
    FakeDirectory,
    FakeGeckoTerminal,
    FakeHyperSync,
    FakeZerion,
)

pytestmark = pytest.mark.django_db


@pytest.fixture
def fake_clients():
    with (
        patch.object(tasks.clients, "geckoterminal", return_value=FakeGeckoTerminal()),
        patch.object(tasks.clients, "coingecko", return_value=FakeCoinGecko()),
        patch.object(tasks.clients, "hypersync_directory", return_value=FakeDirectory()),
        patch.object(tasks.clients, "zerion", return_value=FakeZerion()),
        patch.object(tasks.clients, "hypersync", return_value=FakeHyperSync()),
        patch.object(tasks.timezone, "now", return_value=NOW),
    ):
        yield


def run_pipeline():
    tasks.sync_chains_task.apply()
    tasks.collect_candidates_task.apply()
    tasks.analyze_candidates_task.apply()
    for candidate_id in Candidate.objects.filter(status="confirmed").values_list("id", flat=True):
        tasks.extract_candidate_buyers.apply(args=[candidate_id])


def test_full_pipeline_extracts_buyers(fake_clients):
    run_pipeline()
    assert Chain.objects.active().count() == 1
    candidate = Candidate.objects.get()
    assert candidate.status == Candidate.Status.BUYERS_EXTRACTED
    assert EarlyBuyer.objects.count() == 2


def test_pipeline_is_idempotent(fake_clients):
    run_pipeline()
    run_pipeline()
    assert Candidate.objects.count() == 1
    assert EarlyBuyer.objects.count() == 2


def test_extract_task_schedules_one_subtask_per_confirmed_candidate(fake_clients):
    tasks.sync_chains_task.apply()
    tasks.collect_candidates_task.apply()
    tasks.analyze_candidates_task.apply()
    with patch.object(tasks.extract_candidate_buyers, "delay") as delay:
        assert tasks.extract_early_buyers_task.apply().get() == 1
    delay.assert_called_once_with(Candidate.objects.get().pk)


def test_failure_increments_attempts_then_rejects(fake_clients):
    tasks.sync_chains_task.apply()
    candidate = make_candidate(make_token(Chain.objects.get(gt_id="base")))
    cfg = PipelineSettings.load()
    cfg.max_attempts = 2
    cfg.save()
    with patch.object(tasks, "analyze_candidate", side_effect=RuntimeError("boom")):
        tasks.analyze_candidates_task.apply()
        candidate.refresh_from_db()
        assert (candidate.status, candidate.attempts) == (Candidate.Status.CANDIDATE, 1)
        tasks.analyze_candidates_task.apply()
    candidate.refresh_from_db()
    assert candidate.status == Candidate.Status.REJECTED
    assert candidate.rejection_reason == "error:RuntimeError"


def test_run_discovery_chains_the_four_tasks():
    with patch.object(tasks, "chain") as chain:
        tasks.run_discovery.apply()
    names = [sig.task for sig in chain.call_args.args]
    assert names == [
        "apps.discovery.tasks.sync_chains_task",
        "apps.discovery.tasks.collect_candidates_task",
        "apps.discovery.tasks.analyze_candidates_task",
        "apps.discovery.tasks.extract_early_buyers_task",
    ]
    chain.return_value.apply_async.assert_called_once()
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/discovery/tests/test_tasks.py -v"`
Expected: FAIL avec `AttributeError: module 'apps.discovery.tasks' has no attribute 'clients'`.

- [ ] **Step 3: Implémenter**

`backend/apps/discovery/tasks.py` :

```python
"""Tâches Celery de la découverte. Planning : tâche périodique `discovery-daily` (admin)."""

import logging
from collections import Counter

from celery import chain, shared_task
from django.db.models import Q
from django.utils import timezone

from apps.discovery.models import Candidate, PipelineSettings
from apps.discovery.services import clients
from apps.discovery.services.analysis import analyze_candidate
from apps.discovery.services.candidates import collect_candidates
from apps.discovery.services.chains import sync_chains
from apps.discovery.services.extraction import extract_buyers

logger = logging.getLogger(__name__)


def record_failure(candidate: Candidate, exc: Exception, max_attempts: int) -> None:
    logger.exception("Échec sur le candidat %s", candidate.pk, exc_info=exc)
    candidate.attempts += 1
    fields = ["attempts", "updated_at"]
    if candidate.attempts >= max_attempts:
        candidate.status = Candidate.Status.REJECTED
        candidate.rejection_reason = f"error:{type(exc).__name__}"[:64]
        fields += ["status", "rejection_reason"]
    candidate.save(update_fields=fields)


@shared_task
def sync_chains_task() -> int:
    cfg = PipelineSettings.load()
    count = sync_chains(
        clients.geckoterminal(cfg),
        clients.coingecko(cfg),
        clients.hypersync_directory(cfg),
        clients.zerion(cfg),
    )
    logger.info("%s chaînes synchronisées", count)
    return count


@shared_task
def collect_candidates_task() -> int:
    cfg = PipelineSettings.load()
    created = collect_candidates(clients.geckoterminal(cfg), cfg, timezone.now())
    logger.info("%s nouveaux candidats", created)
    return created


@shared_task
def analyze_candidates_task() -> dict[str, int]:
    cfg = PipelineSettings.load()
    gt = clients.geckoterminal(cfg)
    now = timezone.now()
    due = Candidate.objects.filter(
        Q(status=Candidate.Status.CANDIDATE)
        | Q(status=Candidate.Status.WAITING_CONFIRMATION, next_check_at__lte=now)
    ).select_related("token__chain")
    counts: Counter[str] = Counter()
    for candidate in due:
        try:
            status = analyze_candidate(
                candidate,
                gt=gt,
                hypersync_for=lambda chain_: clients.hypersync(chain_, cfg),
                now=now,
            )
        except Exception as exc:
            record_failure(candidate, exc, cfg.max_attempts)
            status = "error"
        counts[status] += 1
    logger.info("Analyse : %s", dict(counts))
    return dict(counts)


@shared_task
def extract_early_buyers_task() -> int:
    ids = list(
        Candidate.objects.filter(status=Candidate.Status.CONFIRMED).values_list("id", flat=True)
    )
    for candidate_id in ids:
        extract_candidate_buyers.delay(candidate_id)
    return len(ids)


@shared_task
def extract_candidate_buyers(candidate_id: int) -> int:
    cfg = PipelineSettings.load()
    candidate = Candidate.objects.select_related("token__chain", "explosion").get(pk=candidate_id)
    if candidate.status != Candidate.Status.CONFIRMED:
        return 0
    try:
        return extract_buyers(
            candidate,
            gt=clients.geckoterminal(cfg),
            hypersync=clients.hypersync(candidate.token.chain, cfg),
            cfg=cfg,
            now=timezone.now(),
        )
    except Exception as exc:
        record_failure(candidate, exc, cfg.max_attempts)
        return 0


@shared_task
def run_discovery() -> None:
    chain(
        sync_chains_task.si(),
        collect_candidates_task.si(),
        analyze_candidates_task.si(),
        extract_early_buyers_task.si(),
    ).apply_async()
```

- [ ] **Step 4: Lancer les tests**

Run: `make test args="apps/discovery -v"`
Expected: PASS.

- [ ] **Step 5: Lint et commit**

```bash
make fmt && make lint
git add backend/apps/discovery/tasks.py backend/apps/discovery/tests/test_tasks.py
git commit -m "feat(discovery): tâches Celery, gestion des échecs et pipeline quotidien

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 15: Admin et ajout manuel d'un token

**Files:**
- Modify: `backend/apps/discovery/admin.py`
- Create: `backend/apps/discovery/forms.py`
- Create: `backend/apps/discovery/templates/admin/discovery/candidate/change_list.html`
- Create: `backend/apps/discovery/templates/admin/discovery/candidate/add_manual.html`
- Create: `backend/apps/discovery/tests/test_admin.py`

**Interfaces:**
- Consumes: modèles (Task 6), `add_manual_candidate` (Task 11), `clients.geckoterminal` (Task 10), `PipelineSettings.load()`.
- Produces: URL d'admin nommée `admin:discovery_candidate_add_manual` (`/admin/discovery/candidate/add-manual/`) ; `ManualCandidateForm(chain: ModelChoiceField sur Chain.objects.active(), address: CharField)`.

- [ ] **Step 1: Écrire les tests**

`backend/apps/discovery/tests/test_admin.py` :

```python
from unittest.mock import patch

import pytest
from django.urls import reverse

from apps.discovery.models import Candidate, EarlyBuyer, PipelineSettings
from apps.discovery.tests.factories import make_chain
from apps.discovery.tests.fakes import TOKEN, FakeGeckoTerminal

pytestmark = pytest.mark.django_db


@pytest.fixture
def admin_client(client, django_user_model):
    user = django_user_model.objects.create_superuser("admin", "a@example.com", "pw")
    client.force_login(user)
    return client


@pytest.mark.parametrize(
    "model",
    ["chain", "detectionsettings", "pipelinesettings", "candidate", "explosion", "earlybuyer", "wallet"],
)
def test_changelists_load(admin_client, model):
    assert admin_client.get(reverse(f"admin:discovery_{model}_changelist")).status_code == 200


def test_pipeline_settings_cannot_be_added_twice(admin_client):
    PipelineSettings.load()
    assert admin_client.get(reverse("admin:discovery_pipelinesettings_add")).status_code == 403


def test_early_buyers_are_read_only(admin_client):
    assert admin_client.get(reverse("admin:discovery_earlybuyer_add")).status_code == 403
    assert not EarlyBuyer.objects.exists()


def test_manual_add_creates_candidate(admin_client):
    chain = make_chain()
    url = reverse("admin:discovery_candidate_add_manual")
    assert admin_client.get(url).status_code == 200
    with patch("apps.discovery.admin.clients.geckoterminal", return_value=FakeGeckoTerminal()):
        response = admin_client.post(url, {"chain": chain.pk, "address": TOKEN})
    assert response.status_code == 302
    assert Candidate.objects.get().sources == ["manual"]


def test_manual_add_shows_error_when_no_pool(admin_client):
    chain = make_chain()
    url = reverse("admin:discovery_candidate_add_manual")
    with patch(
        "apps.discovery.admin.clients.geckoterminal",
        return_value=FakeGeckoTerminal(token_pools=[]),
    ):
        response = admin_client.post(url, {"chain": chain.pk, "address": TOKEN})
    assert response.status_code == 200
    assert "Aucun pool" in response.content.decode()
    assert not Candidate.objects.exists()
```

- [ ] **Step 2: Lancer les tests pour vérifier qu'ils échouent**

Run: `make test args="apps/discovery/tests/test_admin.py -v"`
Expected: FAIL (`NoReverseMatch` : modèles non enregistrés).

- [ ] **Step 3: Écrire le formulaire**

`backend/apps/discovery/forms.py` :

```python
"""Formulaires de l'admin de la découverte."""

from django import forms

from apps.discovery.models import Chain


class ManualCandidateForm(forms.Form):
    chain = forms.ModelChoiceField(queryset=Chain.objects.active(), label="Chaîne")
    address = forms.CharField(max_length=66, label="Adresse du token")
```

- [ ] **Step 4: Écrire les templates**

`backend/apps/discovery/templates/admin/discovery/candidate/change_list.html` :

```html
{% extends "admin/change_list.html" %}
{% block object-tools-items %}
  <li><a href="{% url 'admin:discovery_candidate_add_manual' %}">Ajouter un token</a></li>
  {{ block.super }}
{% endblock %}
```

`backend/apps/discovery/templates/admin/discovery/candidate/add_manual.html` :

```html
{% extends "admin/base_site.html" %}
{% block content %}
  <h1>Ajouter un token à analyser</h1>
  {% if error %}<p class="errornote">{{ error }}</p>{% endif %}
  <form method="post">
    {% csrf_token %}
    {{ form.as_p }}
    <input type="submit" value="Ajouter" class="default">
  </form>
{% endblock %}
```

- [ ] **Step 5: Écrire l'admin**

`backend/apps/discovery/admin.py` :

```python
"""Admin de la découverte : réglages éditables, résultats en lecture seule, ajout manuel."""

from django.contrib import admin, messages
from django.shortcuts import redirect
from django.template.response import TemplateResponse
from django.urls import path, reverse

from apps.discovery.forms import ManualCandidateForm
from apps.discovery.models import (
    Candidate,
    Chain,
    DetectionSettings,
    EarlyBuyer,
    Explosion,
    PipelineSettings,
    Wallet,
)
from apps.discovery.services import clients
from apps.discovery.services.candidates import add_manual_candidate


class ReadOnlyAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(Chain)
class ChainAdmin(admin.ModelAdmin):
    list_display = ["gt_id", "name", "evm_id", "zerion_id", "hypersync_supported", "is_enabled"]
    list_editable = ["is_enabled"]
    list_filter = ["is_enabled", "hypersync_supported"]
    search_fields = ["gt_id", "name"]
    readonly_fields = ["gt_id", "name", "evm_id", "zerion_id", "hypersync_supported", "updated_at"]

    def has_add_permission(self, request):
        return False


@admin.register(DetectionSettings)
class DetectionSettingsAdmin(admin.ModelAdmin):
    list_display = ["__str__", "min_multiplier", "min_buy_usd", "max_buyers"]


@admin.register(PipelineSettings)
class PipelineSettingsAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        return not PipelineSettings.objects.exists()

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(Candidate)
class CandidateAdmin(admin.ModelAdmin):
    list_display = ["token", "status", "rejection_reason", "attempts", "next_check_at", "created_at"]
    list_filter = ["status", "token__chain", "rejection_reason"]
    search_fields = ["token__address", "token__symbol"]
    list_select_related = ["token__chain"]
    readonly_fields = ["token", "sources", "metrics", "created_at", "updated_at"]

    def has_add_permission(self, request):
        return False

    def get_urls(self):
        custom = [
            path(
                "add-manual/",
                self.admin_site.admin_view(self.add_manual_view),
                name="discovery_candidate_add_manual",
            )
        ]
        return custom + super().get_urls()

    def add_manual_view(self, request):
        form = ManualCandidateForm(request.POST or None)
        error = None
        if request.method == "POST" and form.is_valid():
            gt = clients.geckoterminal(PipelineSettings.load())
            try:
                add_manual_candidate(form.cleaned_data["chain"], form.cleaned_data["address"], gt)
            except ValueError as exc:
                error = str(exc)
            else:
                messages.success(request, "Token ajouté : il sera analysé au prochain passage.")
                return redirect(reverse("admin:discovery_candidate_changelist"))
        context = {**self.admin_site.each_context(request), "form": form, "error": error}
        return TemplateResponse(request, "admin/discovery/candidate/add_manual.html", context)


@admin.register(Explosion)
class ExplosionAdmin(ReadOnlyAdmin):
    list_display = ["candidate", "multiplier", "retention_pct", "low_at", "peak_at"]
    list_select_related = ["candidate__token__chain"]


@admin.register(EarlyBuyer)
class EarlyBuyerAdmin(ReadOnlyAdmin):
    list_display = ["wallet", "explosion", "bought_usd", "is_sniper", "first_buy_at"]
    list_filter = ["is_sniper"]
    ordering = ["-bought_usd"]
    search_fields = ["wallet__address"]
    list_select_related = ["wallet", "explosion__candidate__token__chain"]


@admin.register(Wallet)
class WalletAdmin(ReadOnlyAdmin):
    search_fields = ["address"]
```

- [ ] **Step 6: Lancer les tests**

Run: `make test args="apps/discovery -v"`
Expected: PASS.

- [ ] **Step 7: Lint et commit**

```bash
make fmt && make lint
git add backend/apps/discovery
git commit -m "feat(discovery): admin (réglages, résultats en lecture seule, ajout manuel)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 16: Tests live, documentation et vérification de bout en bout

**Files:**
- Create: `backend/integrations/tests/test_live.py`
- Modify: `README.md` (section « Découverte »)

**Interfaces:**
- Consumes: tous les clients (Tasks 2 à 5).

- [ ] **Step 1: Écrire les tests live**

`backend/integrations/tests/test_live.py` :

```python
"""Tests contre les vraies API. Lancer avec : make test args="-m live integrations"."""

import os

import pytest

from integrations import coingecko, geckoterminal, zerion
from integrations.http import JsonHttpClient
from integrations.hypersync import CHAINS_URL, HyperSyncClient, HyperSyncDirectory
from integrations.ratelimit import NoopLimiter

pytestmark = pytest.mark.live


def http(base: str, **kwargs) -> JsonHttpClient:
    return JsonHttpClient(base, limiter=NoopLimiter(), **kwargs)


def test_geckoterminal_trending_and_ohlcv():
    client = geckoterminal.GeckoTerminalClient(http(geckoterminal.BASE_URL))
    pools = client.trending_pools(page=1)
    assert pools
    candles = client.ohlcv(pools[0].network, pools[0].address, "hour", 1)
    assert candles and candles == sorted(candles, key=lambda c: c.ts)


def test_coingecko_maps_base():
    assert coingecko.CoinGeckoClient(http(coingecko.BASE_URL)).platform_chain_ids()["base"] == 8453


def test_hypersync_directory_supports_base():
    assert 8453 in HyperSyncDirectory(http(CHAINS_URL)).supported_chain_ids()


@pytest.mark.skipif(not os.environ.get("ZERION_API_KEY"), reason="ZERION_API_KEY absente")
def test_zerion_supports_base():
    client = zerion.ZerionClient(http(zerion.BASE_URL, auth=(os.environ["ZERION_API_KEY"], "")))
    assert client.chain_ids()[8453] == "base"


@pytest.mark.skipif(not os.environ.get("ENVIO_API_TOKEN"), reason="ENVIO_API_TOKEN absent")
def test_hypersync_reads_blocks_and_transfers():
    client = HyperSyncClient(8453, os.environ["ENVIO_API_TOKEN"], NoopLimiter())
    height = client.height()
    assert client.block_timestamp(height - 100) > 1_700_000_000
    usdc = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    transfers = client.transfers(usdc, height - 20, height - 10, max_transfers=100_000)
    assert transfers
    assert all(t.tx_from.startswith("0x") and len(t.tx_from) == 42 for t in transfers)
```

- [ ] **Step 2: Lancer les tests live**

Run: `make test args="-m live integrations -v"`
Expected: PASS pour GeckoTerminal, CoinGecko et l'annuaire HyperSync ; les tests Zerion et HyperSync passent si les clés sont dans `.env`, sinon ils sont `SKIPPED`. Si un test échoue, corriger le client concerné (les formats réels priment) avant de continuer.

- [ ] **Step 3: Documenter dans `README.md`**

Ajouter à la fin du `README.md` :

```markdown
## Découverte multi-chaînes

Chaque jour à 06:00 UTC (tâche périodique `discovery-daily`, modifiable dans l'admin), le backend :

1. synchronise les chaînes (GeckoTerminal, CoinGecko, HyperSync, Zerion) ;
2. collecte les candidats (trending global + top volume par chaîne active) ;
3. détecte les explosions (×N, volume, liquidité, rétention après le pic) ;
4. extrait les early buyers EOA significatifs via HyperSync.

Clés à renseigner dans `.env` : `ENVIO_API_TOKEN`, `ZERION_API_KEY` (et `COINGECKO_API_KEY`, optionnelle).

Tous les réglages sont dans l'admin : **Réglages de détection** (globaux + par chaîne), **Réglages du pipeline**, **Chaînes** (interrupteur), **Tâches périodiques**. Un token repéré ailleurs s'ajoute via **Candidats → Ajouter un token**.

Lancer le pipeline à la main :

    make shell
    >>> from apps.discovery.tasks import run_discovery
    >>> run_discovery.delay()

Tests contre les vraies API : `make test args="-m live integrations"`.
```

- [ ] **Step 4: Vérification complète**

```bash
make test
make lint
make up
docker compose logs beat --tail 20
```

Expected : tous les tests passent (les tests live sont exclus par défaut) ; lint propre ; les logs de `beat` montrent `DatabaseScheduler`.

- [ ] **Step 5: Essai réel (si les clés sont présentes)**

```bash
docker compose exec web python manage.py shell -c "from apps.discovery.tasks import sync_chains_task, collect_candidates_task, analyze_candidates_task; print(sync_chains_task()); print(collect_candidates_task()); print(analyze_candidates_task())"
```

Expected : un nombre de chaînes > 0, des candidats créés sur plusieurs chaînes, un dictionnaire de statuts. Vérifier dans l'admin que `eth`, `bsc`, `base` et `robinhood` sont actives. Puis lancer `extract_early_buyers_task.delay()` et contrôler 2-3 acheteurs d'un token confirmé sur l'explorateur de blocs (achat avant le point bas, adresse EOA, montant cohérent).

- [ ] **Step 6: Commit**

```bash
git add backend/integrations/tests/test_live.py README.md
git commit -m "docs(discovery): tests live et documentation de la découverte

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
