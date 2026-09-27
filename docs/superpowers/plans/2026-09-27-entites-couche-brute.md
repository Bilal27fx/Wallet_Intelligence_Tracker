# Entités et couche brute — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Regrouper les wallets d'une même personne en entités (découverte HyperSync + qualification Zerion), classer les early buyers par entité sans bots, et stocker une couche brute complète (transferts on-chain retenus, métadonnées de tokens, portefeuille par token).

**Architecture:** La passe 1 de l'extraction devient un `FlowScanner` (fonction pure) qui agrège achats, ventes et envois par couple d'adresses ; `classify_recipients` / `resolve_flows` / `group_entities` / `select_entities` (purs) en déduisent les positions avec héritage, les liens coffre, les entités et le top sans bots. Une passe filtrée (`EntityPass`) enregistre les transferts bruts des entités retenues et suit les coffres pendant la montée. Un service unique `entity_graph` crée les liens et fusionne les entités pour la découverte et la qualification.

**Tech Stack:** Django 5.2, Postgres 16, Redis 7, Celery 5.5, hypersync 1.2.1, Zerion API, pytest, ruff.

**Spec:** `docs/superpowers/specs/2026-09-27-entites-couche-brute-design.md`

## Global Constraints

- Aucun paramètre en dur : nouveaux réglages dans l'admin — `DetectionSettings.hub_min_senders` (10), `vault_follow_depth` (2), `bot_window_days` (7), `vault_min_pct` (20) ; `PipelineSettings.zerion_operation_types` (types actuels), `token_info_refresh_days` (30). Réutilisés (valeurs globales de `QualificationSettings`) : `deposit_forward_pct`, `deposit_forward_hours`, `max_txs_per_day`.
- Un bot (> `max_txs_per_day` × `bot_window_days` transactions signées sur les `bot_window_days` jours précédant le creux) est écarté : ni lien ni entité, ses envois sont des sorties.
- `max_buyers` compte des **entités** ; `min_buy_usd` s'applique à la position de l'entité au creux.
- Vente = envoi vers un pool du token ; sortie = envoi vers un hub, une adresse de dépôt ou une adresse `KnownAddress` bloquante, ou petit envoi ; transfert d'entité = gros envoi (≥ `vault_min_pct` % de ce qui est entré chez l'expéditeur ; `transfer_after_buy_pct`, 70 %, reste le seuil des liens côté qualification) vers toute autre adresse.
- Prix de revient hérité au **coût moyen** de l'expéditeur au moment du transfert ; date du premier achat = la plus ancienne.
- Filtre anti-spam Zerion conservé ; pas de bougies stockées.
- Portefeuille : `/wallets/{a}/positions/?filter[positions]=no_filter` remplace `/portfolio` (un seul appel, total et répartition par chaîne recalculés) — vérifié en live : 577 positions renvoyées en une réponse.
- `deposit` / `withdraw` ne sont ajoutés au réglage `zerion_operation_types` qu'après mesure (Task 10).
- Commandes : `make test`, `make lint`, `make makemigrations`, `make migrate`. Vérifier `make lint` **séparément** (ne pas le chaîner avec `| tail` avant un commit). Commits avec `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## File map

- `backend/integrations/hypersync.py` — `Transfer.tx_hash/log_index`, `transfer_pages(..., participants=)`.
- `backend/integrations/zerion.py` — `ZerionClient(operation_types=)`, `portfolio()` via `/positions/`, `PortfolioItem`, `TokenMeta`, `token_metadata()`.
- `backend/apps/discovery/models.py` — `Entity`, `Wallet.entity`, `EarlyBuyer` (+champs), `EntityEarlyBuy`, `ExcludedBuyer`, `TokenTransfer`, réglages.
- `backend/apps/wallets/models.py` — `WalletLink.source/rejected/TRANSFER_TO_VAULT`, `TokenTrade.is_internal`, `TokenInfo`, `PortfolioSnapshot`, `PortfolioPosition`.
- Create `backend/apps/discovery/services/flows.py` — scanner, classement, positions, entités, sélection, passe entité (purs).
- Create `backend/apps/discovery/services/entity_buys.py` — agrégation `EntityEarlyBuy` d'une explosion.
- Create `backend/apps/wallets/services/entity_graph.py` — `ensure_entity`, `link`, `merge`, `detach`.
- Create `backend/apps/wallets/services/raw.py` — `save_portfolio`, `refresh_token_info`.
- Modify `backend/apps/discovery/services/extraction.py` — orchestration.
- Delete `backend/apps/discovery/services/buyers.py` et `tests/test_buyers.py` (remplacés par `flows`).
- Modify `backend/apps/wallets/services/qualification.py`, `entities.py`, `tasks.py`, admins.

---

### Task 1: Modèles, réglages, migrations

**Files:**
- Modify: `backend/apps/discovery/models.py`, `backend/apps/discovery/services/settings.py`, `backend/apps/wallets/models.py`
- Create: migrations générées `discovery/0012_entities.py`, `wallets/0007_entities_raw.py`
- Test: `backend/apps/discovery/tests/test_settings_service.py`, `test_explosion.py` (T), `backend/integrations/tests/test_live.py` (LIVE_THRESHOLDS), `backend/apps/wallets/tests/test_models.py` (créer s'il n'existe pas)

**Interfaces — Produces:**
- `Entity(created_at, updated_at, merged_into)`, `Wallet.entity` (FK nullable, `related_name="wallets"`).
- `EarlyBuyer` + `entity` (FK nullable), `inherited_amount`, `inherited_usd`, `inherited_from` (FK Wallet nullable, `related_name="+"`).
- `EntityEarlyBuy(entity, explosion, held_amount, held_usd, first_buy_at, sold_during_rise_pct, rank)` unique (entity, explosion).
- `ExcludedBuyer(explosion, wallet, reason, txs_per_day, held_usd)` unique (explosion, wallet).
- `TokenTransfer(explosion, token, tx_hash, log_index, block, at, tx_from, sender, recipient, amount, kind)` unique (token, tx_hash, log_index) ; `TokenTransfer.Kind` = `buy, sell, exit, internal, vault, receive`.
- `Thresholds.hub_min_senders: int`, `vault_follow_depth: int`, `bot_window_days: int`, `vault_min_pct: float`.
- `PipelineSettings.zerion_operation_types: str`, `token_info_refresh_days: int`.
- `WalletLink.source` (`hypersync` / `zerion`, défaut `zerion`), `WalletLink.rejected` (bool), `WalletLink.Kind.TRANSFER_TO_VAULT`; `STRONG_LINKS` inclut `TRANSFER_TO_VAULT`.
- `TokenTrade.is_internal` (bool, défaut False).
- `TokenInfo(chain, address, fungible_id, symbol, name, decimals, total_supply, circulating_supply, verified, raw, fetched_at)` unique (chain, address).
- `PortfolioSnapshot(wallet, fetched_at, total_usd, raw)`, `PortfolioPosition(snapshot, chain, token_address, fungible_id, symbol, position_type, quantity, price_usd, value_usd)`.

- [ ] **Step 1: Tests des défauts** — `test_settings_service.py` : ajouter à l'attendu `hub_min_senders=10, vault_follow_depth=2, bot_window_days=7, vault_min_pct=20.0` et à `test_pipeline_defaults_for_explosion_v2` :

```python
    assert cfg.zerion_operation_types == "trade,send,receive,execute,mint,burn,claim"
    assert cfg.token_info_refresh_days == 30
```

Ajouter `hub_min_senders=10, vault_follow_depth=2, bot_window_days=7, vault_min_pct=20` au `T` de `test_explosion.py` et à `LIVE_THRESHOLDS` de `test_live.py`.

Créer `backend/apps/wallets/tests/test_models_entities.py` :

```python
import pytest

from apps.discovery.models import Entity, Wallet
from apps.wallets.models import STRONG_LINKS, WalletLink

pytestmark = pytest.mark.django_db


def test_wallet_belongs_to_entity():
    entity = Entity.objects.create()
    wallet = Wallet.objects.create(address="0x" + "1" * 40, entity=entity)
    assert list(entity.wallets.all()) == [wallet]


def test_vault_link_is_strong_and_defaults():
    assert WalletLink.Kind.TRANSFER_TO_VAULT in STRONG_LINKS
    a = Wallet.objects.create(address="0x" + "1" * 40)
    b = Wallet.objects.create(address="0x" + "2" * 40)
    link = WalletLink.objects.create(from_wallet=a, to_wallet=b, kind="transfer_to_vault")
    assert (link.source, link.rejected) == ("zerion", False)
```

- [ ] **Step 2: Vérifier l'échec** — `make test args="apps/discovery/tests/test_settings_service.py apps/wallets/tests/test_models_entities.py"` → FAIL.

- [ ] **Step 3: Modèles discovery** — `THRESHOLD_FIELDS` + `"hub_min_senders", "vault_follow_depth", "bot_window_days", "vault_min_pct"`. Dans `DetectionSettings` :

```python
    hub_min_senders = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Un destinataire alimenté par au moins autant d'expéditeurs est un hub "
        "(router, exchange) : lui envoyer = sortie.",
    )
    vault_follow_depth = models.PositiveSmallIntegerField(
        null=True, blank=True, help_text="Niveaux de coffres suivis pendant la montée."
    )
    bot_window_days = models.PositiveSmallIntegerField(
        null=True, blank=True, help_text="Jours avant le creux sur lesquels on mesure l'activité bot."
    )
    vault_min_pct = models.DecimalField(
        max_digits=6,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Envoi minimum (% de ce que l'expéditeur a reçu) pour qu'un envoi crée un coffre.",
    )
```

`PipelineSettings` :

```python
    zerion_operation_types = models.CharField(
        max_length=300,
        default=OPERATION_TYPES,
        help_text="Types de transactions Zerion récupérés (séparés par des virgules).",
    )
    token_info_refresh_days = models.PositiveIntegerField(default=30)
```

(`from integrations.zerion import OPERATION_TYPES`). Avant `Wallet` :

```python
class Entity(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    merged_into = models.ForeignKey(
        "self", null=True, blank=True, on_delete=models.SET_NULL, related_name="absorbed"
    )

    class Meta:
        verbose_name_plural = "entities"

    def __str__(self):
        return f"Entité {self.pk}"
```

`Wallet` : `entity = models.ForeignKey(Entity, null=True, blank=True, on_delete=models.SET_NULL, related_name="wallets")`.

`EarlyBuyer` : ajouter

```python
    entity = models.ForeignKey(
        Entity, null=True, blank=True, on_delete=models.SET_NULL, related_name="early_buys"
    )
    inherited_amount = models.DecimalField(max_digits=UINT256_DIGITS, decimal_places=0, default=0)
    inherited_usd = models.DecimalField(
        max_digits=20, decimal_places=2, default=0, help_text="Prix de revient hérité."
    )
    inherited_from = models.ForeignKey(
        Wallet, null=True, blank=True, on_delete=models.SET_NULL, related_name="+"
    )
```

Après `EarlyBuyer` :

```python
class EntityEarlyBuy(models.Model):
    entity = models.ForeignKey(Entity, on_delete=models.CASCADE, related_name="explosion_buys")
    explosion = models.ForeignKey(Explosion, on_delete=models.CASCADE, related_name="entity_buys")
    held_amount = models.DecimalField(max_digits=UINT256_DIGITS, decimal_places=0)
    held_usd = models.DecimalField(max_digits=20, decimal_places=2)
    first_buy_at = models.DateTimeField()
    sold_during_rise_pct = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    rank = models.PositiveIntegerField(default=0)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["entity", "explosion"], name="discovery_one_entity_buy_per_explosion"
            )
        ]
        ordering = ["explosion", "rank"]


class ExcludedBuyer(models.Model):
    explosion = models.ForeignKey(Explosion, on_delete=models.CASCADE, related_name="excluded")
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="exclusions")
    reason = models.CharField(max_length=32)
    txs_per_day = models.DecimalField(max_digits=12, decimal_places=1, null=True, blank=True)
    held_usd = models.DecimalField(max_digits=20, decimal_places=2, default=0)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["explosion", "wallet"], name="discovery_one_exclusion_per_explosion"
            )
        ]


class TokenTransfer(models.Model):
    class Kind(models.TextChoices):
        BUY = "buy", "Achat"
        SELL = "sell", "Vente"
        EXIT = "exit", "Sortie"
        INTERNAL = "internal", "Interne à l'entité"
        VAULT = "vault", "Vers un nouveau coffre"
        RECEIVE = "receive", "Réception"

    explosion = models.ForeignKey(Explosion, on_delete=models.CASCADE, related_name="transfers")
    token = models.ForeignKey(Token, on_delete=models.CASCADE, related_name="transfers")
    tx_hash = models.CharField(max_length=66)
    log_index = models.PositiveIntegerField()
    block = models.PositiveBigIntegerField()
    at = models.DateTimeField()
    tx_from = models.CharField(max_length=42)
    sender = models.CharField(max_length=42)
    recipient = models.CharField(max_length=42)
    amount = models.DecimalField(max_digits=UINT256_DIGITS, decimal_places=0)
    kind = models.CharField(max_length=16, choices=Kind.choices)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["token", "tx_hash", "log_index"], name="discovery_unique_token_transfer"
            )
        ]
        indexes = [models.Index(fields=["explosion", "sender"], name="discovery_transfer_sender")]
```

`settings.py` / `Thresholds` : ajouter à la fin `hub_min_senders: int`, `vault_follow_depth: int`, `bot_window_days: int`, `vault_min_pct: float`.

- [ ] **Step 4: Modèles wallets** — `WalletLink` :

```python
    class Kind(models.TextChoices):
        TRANSFER_AFTER_BUY = "transfer_after_buy", "Transfert après achat"
        BIG_RECEIVE = "big_receive", "Gros transfert reçu"
        TRANSFER_TO_VAULT = "transfer_to_vault", "Transfert vers un coffre (on-chain)"
        FUNDING = "funding", "Financement initial (information)"

    class LinkSource(models.TextChoices):
        HYPERSYNC = "hypersync", "HyperSync"
        ZERION = "zerion", "Zerion"
    ...
    source = models.CharField(max_length=16, choices=LinkSource.choices, default=LinkSource.ZERION)
    rejected = models.BooleanField(
        default=False, help_text="Rattachement refusé dans l'admin : jamais recréé."
    )
```

`STRONG_LINKS = (WalletLink.Kind.TRANSFER_AFTER_BUY, WalletLink.Kind.BIG_RECEIVE, WalletLink.Kind.TRANSFER_TO_VAULT)`. `TokenTrade` : `is_internal = models.BooleanField(default=False)`. Nouveaux modèles :

```python
class TokenInfo(models.Model):
    chain = models.CharField(max_length=64)
    address = models.CharField(max_length=66)
    fungible_id = models.CharField(max_length=100, blank=True, default="")
    symbol = models.CharField(max_length=64, blank=True, default="")
    name = models.CharField(max_length=200, blank=True, default="")
    decimals = models.PositiveSmallIntegerField(null=True, blank=True)
    total_supply = models.DecimalField(max_digits=60, decimal_places=18, null=True, blank=True)
    circulating_supply = models.DecimalField(
        max_digits=60, decimal_places=18, null=True, blank=True
    )
    verified = models.BooleanField(default=False)
    raw = models.JSONField(default=dict, blank=True)
    fetched_at = models.DateTimeField()

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["chain", "address"], name="wallets_unique_token_info")
        ]

    def __str__(self):
        return f"{self.symbol or self.address} ({self.chain})"


class PortfolioSnapshot(models.Model):
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name="portfolios")
    fetched_at = models.DateTimeField()
    total_usd = _usd()
    raw = models.JSONField(default=dict, blank=True)

    class Meta:
        indexes = [models.Index(fields=["wallet", "fetched_at"], name="wallets_portfolio_date")]


class PortfolioPosition(models.Model):
    snapshot = models.ForeignKey(
        PortfolioSnapshot, on_delete=models.CASCADE, related_name="positions"
    )
    chain = models.CharField(max_length=64)
    token_address = models.CharField(max_length=66)
    fungible_id = models.CharField(max_length=100, blank=True, default="")
    symbol = models.CharField(max_length=64, blank=True, default="")
    position_type = models.CharField(max_length=32, blank=True, default="")
    quantity = models.DecimalField(max_digits=60, decimal_places=18)
    price_usd = models.DecimalField(max_digits=40, decimal_places=18, null=True, blank=True)
    value_usd = _usd(null=True, blank=True)
```

- [ ] **Step 5: Migrations** — `make makemigrations` ; renommer en `discovery/0012_entities.py` et `wallets/0007_entities_raw.py`. Dans la migration discovery, ajouter :

```python
def set_defaults(apps, schema_editor):
    apps.get_model("discovery", "DetectionSettings").objects.filter(chain=None).update(
        hub_min_senders=10, vault_follow_depth=2, bot_window_days=7, vault_min_pct=20
    )
```

avec `migrations.RunPython(set_defaults, migrations.RunPython.noop)` en dernier. `make migrate`.

- [ ] **Step 6: Vérifier** — `make test` → PASS ; `make lint` → PASS.

- [ ] **Step 7: Commit** — `feat: modèles entités et couche brute`

---

### Task 2: Service `entity_graph`

**Files:**
- Create: `backend/apps/discovery/services/entity_buys.py`, `backend/apps/wallets/services/entity_graph.py`
- Test: `backend/apps/wallets/tests/test_entity_graph.py`

**Interfaces:**
- Produces:
  - `entity_buys.refresh_entity_buys(explosion) -> int` : recalcule les `EntityEarlyBuy` d'une explosion à partir de ses `EarlyBuyer` (somme par entité, rang par `held_usd` décroissant) ; retourne le nombre d'entités.
  - `entity_graph.ensure_entity(wallet) -> Entity`
  - `entity_graph.link(from_address, to_address, kind, source, evidence) -> WalletLink | None` (None si un lien rejeté existe entre les deux wallets, dans un sens ou l'autre ; un lien fort rattache/fusionne).
  - `entity_graph.merge(keep, gone) -> Entity`, `entity_graph.detach(wallet) -> None`.

- [ ] **Step 1: Tests**

```python
import pytest

from apps.discovery.models import Entity, Wallet
from apps.wallets.models import WalletLink
from apps.wallets.services import entity_graph
from apps.wallets.tests.factories import make_early_buy
from apps.discovery.tests.factories import make_chain

pytestmark = pytest.mark.django_db

A, B, C, D = ("0x" + c * 40 for c in "abcd")


def entity_of(address):
    return Wallet.objects.get(address=address).entity_id


def test_strong_link_creates_entity():
    entity_graph.link(A, B, WalletLink.Kind.TRANSFER_TO_VAULT, "hypersync", {"pct": 80})
    assert entity_of(A) is not None and entity_of(A) == entity_of(B)
    assert WalletLink.objects.get().source == "hypersync"


def test_weak_link_does_not_group():
    entity_graph.link(A, B, WalletLink.Kind.FUNDING, "zerion", {})
    assert entity_of(A) is None and entity_of(B) is None


def test_chain_joins_same_entity():
    entity_graph.link(A, B, WalletLink.Kind.TRANSFER_TO_VAULT, "hypersync", {})
    entity_graph.link(B, C, WalletLink.Kind.TRANSFER_AFTER_BUY, "zerion", {})
    assert entity_of(A) == entity_of(B) == entity_of(C)


def test_link_between_two_entities_merges():
    entity_graph.link(A, B, WalletLink.Kind.TRANSFER_TO_VAULT, "hypersync", {})
    entity_graph.link(C, D, WalletLink.Kind.TRANSFER_TO_VAULT, "hypersync", {})
    gone = entity_of(C)
    entity_graph.link(B, C, WalletLink.Kind.BIG_RECEIVE, "zerion", {})
    assert len({entity_of(x) for x in (A, B, C, D)}) == 1
    assert Entity.objects.get(pk=gone).merged_into_id == entity_of(A)


def test_detached_wallet_is_never_relinked():
    entity_graph.link(A, B, WalletLink.Kind.TRANSFER_TO_VAULT, "hypersync", {})
    entity_graph.detach(Wallet.objects.get(address=B))
    assert entity_of(B) is None
    assert entity_graph.link(B, A, WalletLink.Kind.BIG_RECEIVE, "zerion", {}) is None
    assert entity_of(B) is None


def test_refresh_entity_buys_sums_members_and_ranks():
    chain = make_chain()
    a = Wallet.objects.create(address=A)
    b = Wallet.objects.create(address=B)
    c = Wallet.objects.create(address=C)
    buy_a = make_early_buy(a, chain, "0x" + "e" * 40)
    explosion = buy_a.explosion
    entity_graph.link(A, B, WalletLink.Kind.TRANSFER_TO_VAULT, "hypersync", {})
    a.refresh_from_db()
    buy_a.entity = a.entity
    buy_a.held_usd = 300
    buy_a.save()
    b.refresh_from_db()
    buy_a.pk = None
    buy_a.wallet, buy_a.entity, buy_a.held_usd = b, b.entity, 500
    buy_a.save()
    single = entity_graph.ensure_entity(c)
    buy_a.pk = None
    buy_a.wallet, buy_a.entity, buy_a.held_usd = c, single, 700
    buy_a.save()
    from apps.discovery.services.entity_buys import refresh_entity_buys

    assert refresh_entity_buys(explosion) == 2
    top = explosion.entity_buys.order_by("rank")
    assert [(float(e.held_usd), e.rank) for e in top] == [(800.0, 1), (700.0, 2)]
```

- [ ] **Step 2: Vérifier l'échec** — `make test args="apps/wallets/tests/test_entity_graph.py"` → FAIL (module absent).

- [ ] **Step 3: Implémenter `entity_buys.py`**

```python
"""Position des entités par explosion, agrégée depuis les early buyers de leurs wallets."""

from collections import defaultdict
from decimal import Decimal

from apps.discovery.models import EarlyBuyer, EntityEarlyBuy, Explosion


def refresh_entity_buys(explosion: Explosion) -> int:
    rows = EarlyBuyer.objects.filter(explosion=explosion, entity__isnull=False)
    totals: dict[int, dict] = defaultdict(
        lambda: {"held_amount": Decimal(0), "held_usd": Decimal(0), "sold": Decimal(0), "first": None}
    )
    for row in rows:
        total = totals[row.entity_id]
        total["held_amount"] += row.held_amount
        total["held_usd"] += row.held_usd
        total["sold"] += row.sold_amount
        if total["first"] is None or row.first_buy_at < total["first"]:
            total["first"] = row.first_buy_at
    EntityEarlyBuy.objects.filter(explosion=explosion).delete()
    ranked = sorted(totals.items(), key=lambda item: item[1]["held_usd"], reverse=True)
    EntityEarlyBuy.objects.bulk_create(
        [
            EntityEarlyBuy(
                entity_id=entity_id,
                explosion=explosion,
                held_amount=total["held_amount"],
                held_usd=total["held_usd"],
                first_buy_at=total["first"],
                sold_during_rise_pct=round(total["sold"] * 100 / total["held_amount"], 2)
                if total["held_amount"]
                else Decimal(0),
                rank=rank,
            )
            for rank, (entity_id, total) in enumerate(ranked, start=1)
        ]
    )
    return len(ranked)
```

- [ ] **Step 4: Implémenter `entity_graph.py`**

```python
"""Entités : wallets d'une même personne, regroupés par liens forts (découverte et qualification)."""

from django.db import transaction
from django.db.models import Q

from apps.discovery.models import EarlyBuyer, Entity, Explosion, Wallet
from apps.discovery.services.entity_buys import refresh_entity_buys
from apps.wallets.models import STRONG_LINKS, WalletLink


def ensure_entity(wallet: Wallet) -> Entity:
    if wallet.entity_id is None:
        wallet.entity = Entity.objects.create()
        wallet.save(update_fields=["entity"])
    return wallet.entity


def _refresh_explosions(entity_ids: list[int]) -> None:
    explosions = Explosion.objects.filter(
        buyers__wallet__entity_id__in=entity_ids
    ).distinct()
    for explosion in explosions:
        EarlyBuyer.objects.filter(explosion=explosion, wallet__entity_id__in=entity_ids).update(
            entity=None
        )
        for row in EarlyBuyer.objects.filter(explosion=explosion, wallet__entity_id__in=entity_ids):
            row.entity_id = row.wallet.entity_id
            row.save(update_fields=["entity"])
        refresh_entity_buys(explosion)


def merge(keep: Entity, gone: Entity) -> Entity:
    if keep.pk == gone.pk:
        return keep
    Wallet.objects.filter(entity=gone).update(entity=keep)
    gone.merged_into = keep
    gone.save(update_fields=["merged_into"])
    _refresh_explosions([keep.pk])
    return keep


def _join(a: Wallet, b: Wallet) -> None:
    a.refresh_from_db()
    b.refresh_from_db()
    if a.entity_id and b.entity_id:
        keep, gone = sorted([a.entity, b.entity], key=lambda entity: entity.pk)
        merge(keep, gone)
    elif a.entity_id or b.entity_id:
        entity_id = a.entity_id or b.entity_id
        Wallet.objects.filter(pk__in=[a.pk, b.pk]).update(entity_id=entity_id)
        _refresh_explosions([entity_id])
    else:
        entity = Entity.objects.create()
        Wallet.objects.filter(pk__in=[a.pk, b.pk]).update(entity=entity)
        _refresh_explosions([entity.pk])


def link(from_address: str, to_address: str, kind: str, source: str, evidence: dict):
    a, _ = Wallet.objects.get_or_create(address=from_address.lower())
    b, _ = Wallet.objects.get_or_create(address=to_address.lower())
    pair = Q(from_wallet=a, to_wallet=b) | Q(from_wallet=b, to_wallet=a)
    if WalletLink.objects.filter(pair, rejected=True).exists():
        return None
    with transaction.atomic():
        created, _ = WalletLink.objects.get_or_create(
            from_wallet=a, to_wallet=b, kind=kind, defaults={"evidence": evidence, "source": source}
        )
        if kind in STRONG_LINKS:
            _join(a, b)
    return created


def detach(wallet: Wallet) -> None:
    entity_id = wallet.entity_id
    WalletLink.objects.filter(
        Q(from_wallet=wallet) | Q(to_wallet=wallet), kind__in=STRONG_LINKS
    ).update(rejected=True)
    wallet.entity = None
    wallet.save(update_fields=["entity"])
    EarlyBuyer.objects.filter(wallet=wallet).update(entity=None)
    if entity_id:
        _refresh_explosions([entity_id])
```

- [ ] **Step 5: Vérifier** — `make test && make lint` → PASS.

- [ ] **Step 6: Commit** — `feat(wallets): service d'entités (liens, fusion, détachement)`

---

### Task 3: HyperSync — hash, index de log et filtre participants

**Files:**
- Modify: `backend/integrations/hypersync.py`, `backend/integrations/tests/test_hypersync.py`, `backend/apps/discovery/tests/fakes.py`

**Interfaces — Produces:**
- `Transfer(block, timestamp, tx_from, sender, recipient, amount, tx_hash="", log_index=0)`.
- `transfer_pages(token, from_block, to_block, senders=None, participants=None)` : `participants` = transferts dont l'expéditeur **ou** le destinataire est dans la liste (deux `LogSelection`).
- `FakeHyperSync.transfer_pages(token, from_block, to_block, senders=None, participants=None)` (enregistre `(token, from_block, to_block, senders, participants)`), `FakeHyperSync.wallet_tx_count(address, from_block, to_block, cap)` (depuis `tx_counts: dict`), transferts par défaut avec `tx_hash` uniques.

- [ ] **Step 1: Tests** — dans `test_hypersync.py`, `log()` ajoute `log_index=block` ; l'attendu de `test_transfer_pages_follow_pagination_and_join_tx_sender` porte `tx_hash="0xt1", log_index=100` / `tx_hash="0xt2", log_index=160`. Ajouter :

```python
def test_transfer_pages_filter_on_participants():
    inner = FakeInner([page(200)])
    collect(make_client(inner), "0xtoken", 0, 200, participants=[ALICE])
    assert [s.topics for s in inner.queries[0].logs] == [
        [[TRANSFER_TOPIC], [topic(ALICE)]],
        [[TRANSFER_TOPIC], [], [topic(ALICE)]],
    ]
```

- [ ] **Step 2: Vérifier l'échec** — `make test args="integrations/tests/test_hypersync.py"` → FAIL.

- [ ] **Step 3: Implémenter** — `Transfer` : ajouter `tx_hash: str = ""` et `log_index: int = 0` en fin. Dans `transfer_pages` :

```python
        if participants:
            addresses = [address_topic(a) for a in participants]
            logs = [
                LogSelection(address=[token], topics=[[TRANSFER_TOPIC], addresses]),
                LogSelection(address=[token], topics=[[TRANSFER_TOPIC], [], addresses]),
            ]
        else:
            topics = [[TRANSFER_TOPIC]]
            if senders:
                topics.append([address_topic(sender) for sender in senders])
            logs = [LogSelection(address=[token], topics=topics)]
```

`LogField.LOG_INDEX` ajouté à la sélection ; `_token_transfers` renseigne `tx_hash=log.transaction_hash, log_index=log.log_index or 0`.

`fakes.py` : `FakeHyperSync.__init__(self, transfers=None, error=None, page_size=2, tx_counts=None)` ; `_buy` et le transfert d'Alice reçoivent `tx_hash=f"0x{block:x}"` (et `log_index=0`) ; `transfer_pages` :

```python
    def transfer_pages(self, token, from_block, to_block, senders=None, participants=None):
        self.transfer_calls.append((token, from_block, to_block, senders, participants))
        if self._error:
            raise self._error
        wanted = set(participants or [])
        selected = [
            t
            for t in self._all_transfers()
            if from_block <= t.block < to_block
            and (senders is None or t.sender in senders)
            and (not wanted or t.sender in wanted or t.recipient in wanted)
        ]
        for start in range(0, len(selected), self.page_size):
            yield selected[start : start + self.page_size]

    def wallet_tx_count(self, address, from_block, to_block, cap):
        return min(self.tx_counts.get(address, 0), cap)
```

- [ ] **Step 4: Vérifier** — `make test args="integrations"` → PASS (l'extraction est migrée en Task 5).

- [ ] **Step 5: Commit** — `feat(hypersync): hash, index de log et filtre expéditeur ou destinataire`

---

### Task 4: Flux purs — scanner, destinataires, héritage, entités, bots, passe entité

**Files:**
- Create: `backend/apps/discovery/services/flows.py`
- Test: `backend/apps/discovery/tests/test_flows.py`

**Interfaces — Produces** (tous purs) :
- `FlowScanner(*, candles, decimals, pools, hub_min_senders)` : `.add(transfers)`, `.price_at(ts)`, attributs `bought, cost, first_buy, sold, moves, senders, received_at, pools`.
- `hubs(scanner) -> set[str]`
- `classify_recipients(scanner, *, known, deposit_forward_pct, deposit_forward_hours) -> dict[str, str]` (valeurs `HUB`, `DEPOSIT`, `KNOWN`, `VAULT`).
- `resolve_flows(scanner, kinds, *, big_pct, bots=frozenset()) -> Flows` avec `Flows(holders: dict[str, Holder], links: list[VaultLink])`, `Holder.held`, `Holder.held_cost`, `Holder.inherited`, `Holder.inherited_cost`, `Holder.inherited_from`, `Holder.first_block`, `Holder.first_ts`, `Holder.bought`, `Holder.cost`.
- `group_entities(flows, *, trough_price, scale) -> list[EntityCandidate]` (`wallets`, `held_amount`, `held_usd`, `first_ts`), triés par `held_usd` décroissant.
- `select_entities(scanner, kinds, *, bot_check, big_pct, trough_price, scale, min_usd, max_entities) -> Selection(flows, selected, bots)` ; `bot_check(address) -> tuple[bool, float]` ; `bots: dict[str, tuple[float, float]]` (txs/jour, position $).
- `EntityPass(*, group_of, held, trough_block, pools, exits, big_pct)` : `.add(transfers)`, `.take_new_vaults() -> dict[str, NewVault]`, `.rows: list[tuple[Transfer, str]]`, `.sold_rise: dict[str, int]`.

- [ ] **Step 1: Tests** (`test_flows.py`) :

```python
from apps.discovery.services.flows import (
    DEPOSIT,
    HUB,
    VAULT,
    EntityPass,
    FlowScanner,
    classify_recipients,
    group_entities,
    resolve_flows,
    select_entities,
)
from integrations.geckoterminal import Candle
from integrations.hypersync import Transfer

UNIT = 10**18
POOL = "0x" + "b" * 40
A, B, C, D, H = ("0x" + c * 40 for c in "acdef")
CANDLES = [Candle(0, 1, 1, 1, 1.0, 0)]
_n = iter(range(10**6))


def tr(block, signer, sender, recipient, tokens):
    i = next(_n)
    return Transfer(block, block * 10, signer, sender, recipient, tokens * UNIT, f"0x{i:x}", 0)


def buy(block, wallet, tokens):
    return tr(block, wallet, POOL, wallet, tokens)


def send(block, sender, recipient, tokens):
    return tr(block, sender, sender, recipient, tokens)


def scan(transfers, hub_min_senders=3):
    scanner = FlowScanner(candles=CANDLES, decimals=18, pools={POOL}, hub_min_senders=hub_min_senders)
    for t in transfers:
        scanner.add([t])
    return scanner


def resolve(scanner, big_pct=20, bots=frozenset(), known=frozenset()):
    kinds = classify_recipients(
        scanner, known=set(known), deposit_forward_pct=90, deposit_forward_hours=1
    )
    return kinds, resolve_flows(scanner, kinds, big_pct=big_pct, bots=bots)


def test_sell_to_pool_reduces_position():
    _, flows = resolve(scan([buy(1, A, 1000), send(2, A, POOL, 300)]))
    assert flows.holders[A].held == 700 * UNIT


def test_big_send_to_vault_moves_position_and_cost():
    kinds, flows = resolve(scan([buy(1, A, 1000), send(2, A, B, 800)]))
    assert kinds[B] == VAULT
    assert flows.holders[A].held == 200 * UNIT
    vault = flows.holders[B]
    assert (vault.held, vault.inherited_cost, vault.inherited_from) == (800 * UNIT, 800.0, A)
    assert vault.first_block == 1
    [link] = flows.links
    assert (link.sender, link.recipient, link.pct) == (A, B, 80.0)


def test_send_to_hub_is_an_exit():
    feeders = [send(1, x, H, 1) for x in ("0x" + "1" * 40, "0x" + "2" * 40, "0x" + "3" * 40)]
    kinds, flows = resolve(scan(feeders + [buy(2, A, 1000), send(3, A, H, 900)]))
    assert kinds[H] == HUB
    assert flows.holders[A].held == 100 * UNIT and not flows.links


def test_deposit_address_forwarding_to_hub_is_an_exit():
    feeders = [send(1, x, H, 1) for x in ("0x" + "1" * 40, "0x" + "2" * 40, "0x" + "3" * 40)]
    transfers = feeders + [buy(2, A, 1000), send(3, A, D, 900), tr(4, H, D, H, 900)]
    kinds, flows = resolve(scan(transfers))
    assert kinds[D] == DEPOSIT
    assert not flows.links and D not in {h for h, x in flows.holders.items() if x.held}


def test_small_send_is_an_exit():
    _, flows = resolve(scan([buy(1, A, 1000), send(2, A, B, 100)]))
    assert flows.holders[A].held == 900 * UNIT and not flows.links


def test_known_exchange_is_an_exit():
    _, flows = resolve(scan([buy(1, A, 1000), send(2, A, B, 800)]), known={B})
    assert not flows.links


def test_chain_of_vaults_keeps_first_buy_and_cost():
    _, flows = resolve(scan([buy(1, A, 1000), send(5, A, B, 1000), send(9, B, C, 1000)]))
    assert flows.holders[C].held == 1000 * UNIT
    assert (flows.holders[C].first_block, flows.holders[C].inherited_cost) == (1, 1000.0)


def test_vault_that_buys_itself_blends_costs():
    _, flows = resolve(scan([buy(1, A, 1000), send(2, A, B, 1000), buy(3, B, 500)]))
    vault = flows.holders[B]
    assert (vault.held, vault.bought, vault.inherited) == (1500 * UNIT, 500 * UNIT, 1000 * UNIT)


def test_bot_sender_creates_no_link_and_is_dropped():
    _, flows = resolve(scan([buy(1, A, 1000), send(2, A, B, 800)]), bots=frozenset({A}))
    assert not flows.links and A not in flows.holders and B not in flows.holders


def test_group_entities_sums_members():
    _, flows = resolve(scan([buy(1, A, 1000), send(2, A, B, 800), buy(3, C, 500), send(4, C, B, 500)]))
    [entity] = group_entities(flows, trough_price=2.0, scale=UNIT)
    assert entity.wallets == sorted([A, B, C])
    assert entity.held_usd == 3000.0


def test_select_entities_replaces_bot_by_next():
    scanner = scan([buy(1, A, 5000), buy(2, B, 1000), buy(3, C, 800)])
    kinds = classify_recipients(scanner, known=set(), deposit_forward_pct=90, deposit_forward_hours=1)
    selection = select_entities(
        scanner,
        kinds,
        bot_check=lambda address: (address == A, 300.0),
        big_pct=20,
        trough_price=1.0,
        scale=UNIT,
        min_usd=500,
        max_entities=2,
    )
    assert [c.wallets for c in selection.selected] == [[B], [C]]
    assert selection.bots == {A: (300.0, 5000.0)}


def test_entity_pass_counts_rise_sells_and_finds_new_vault():
    tracker = EntityPass(
        group_of={A: 0}, held={A: 1000 * UNIT}, trough_block=10, pools={POOL}, exits={H}, big_pct=20
    )
    tracker.add([buy(5, A, 1000), send(11, A, POOL, 100), send(12, A, H, 50), send(13, A, B, 500)])
    assert tracker.sold_rise[A] == 150 * UNIT
    assert [kind for _, kind in tracker.rows] == ["buy", "sell", "exit", "vault"]
    new = tracker.take_new_vaults()
    assert list(new) == [B] and new[B].sender == A
    tracker.add([send(14, B, POOL, 200), send(15, A, B, 10)])
    assert tracker.sold_rise[B] == 200 * UNIT
    assert tracker.rows[-1][1] == "internal"
```

- [ ] **Step 2: Vérifier l'échec** — `make test args="apps/discovery/tests/test_flows.py"` → FAIL (module absent).

- [ ] **Step 3: Implémenter `flows.py`**

```python
"""Flux d'un token explosif : achats, ventes, sorties et transferts d'entité. Fonctions pures.

Vente = envoi vers un pool du token. Sortie = envoi vers un hub (router, exchange), une adresse
de dépôt, une adresse connue, ou petit envoi. Transfert d'entité = gros envoi vers toute autre
adresse : la position, son prix de revient (coût moyen) et la date du premier achat la suivent.
"""

from bisect import bisect_right
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field

from integrations.geckoterminal import Candle
from integrations.hypersync import Transfer

ZERO_ADDRESS = "0x" + "0" * 40
HUB, DEPOSIT, KNOWN, VAULT = "hub", "deposit", "known", "vault"


@dataclass
class PairFlow:
    amount: int
    first_block: int
    first_ts: int
    tx_hash: str


class FlowScanner:
    """Passe 1 : agrège page par page ; mémoire proportionnelle aux adresses, pas aux transferts."""

    def __init__(self, *, candles: list[Candle], decimals: int, pools: set[str], hub_min_senders: int):
        self._candles = candles
        self._times = [candle.ts for candle in candles]
        self._scale = 10**decimals
        self.pools = set(pools)
        self.hub_min_senders = hub_min_senders
        self.bought: dict[str, int] = defaultdict(int)
        self.cost: dict[str, float] = defaultdict(float)
        self.first_buy: dict[str, tuple[int, int]] = {}
        self.sold: dict[str, int] = defaultdict(int)
        self.moves: dict[tuple[str, str], PairFlow] = {}
        self.senders: dict[str, set[str]] = defaultdict(set)
        self.received_at: dict[str, int] = {}

    def price_at(self, ts: int) -> float:
        if not self._candles:
            return 0.0
        index = bisect_right(self._times, ts) - 1
        return self._candles[max(index, 0)].close

    def _tracked(self, address: str) -> bool:
        return address in self.bought or address in self.received_at

    def add(self, transfers: list[Transfer]) -> None:
        for t in sorted(transfers, key=lambda t: (t.block, t.log_index)):
            signer = t.tx_from
            if t.recipient == signer and t.sender not in (signer, ZERO_ADDRESS):
                self.bought[signer] += t.amount
                self.cost[signer] += t.amount / self._scale * self.price_at(t.timestamp)
                self.first_buy.setdefault(signer, (t.block, t.timestamp))
                continue
            if t.recipient in self.pools:
                if self._tracked(t.sender):
                    self.sold[t.sender] += t.amount
                continue
            senders = self.senders[t.recipient]
            if len(senders) < self.hub_min_senders:
                senders.add(t.sender)
            if self._tracked(t.sender) and t.recipient != t.sender:
                flow = self.moves.get((t.sender, t.recipient))
                if flow is None:
                    flow = self.moves[(t.sender, t.recipient)] = PairFlow(
                        0, t.block, t.timestamp, t.tx_hash
                    )
                flow.amount += t.amount
                self.received_at.setdefault(t.recipient, t.timestamp)


def hubs(scanner: FlowScanner) -> set[str]:
    return {r for r, senders in scanner.senders.items() if len(senders) >= scanner.hub_min_senders}


def classify_recipients(
    scanner: FlowScanner, *, known: set[str], deposit_forward_pct: float, deposit_forward_hours: int
) -> dict[str, str]:
    hub_set = hubs(scanner)
    received: dict[str, int] = defaultdict(int)
    outgoing: dict[str, list[tuple[str, PairFlow]]] = defaultdict(list)
    for (sender, recipient), flow in scanner.moves.items():
        received[recipient] += flow.amount
        outgoing[sender].append((recipient, flow))
    kinds = {}
    for recipient, total in received.items():
        if recipient in hub_set:
            kinds[recipient] = HUB
        elif recipient in known:
            kinds[recipient] = KNOWN
        else:
            start = scanner.received_at.get(recipient, 0)
            forwarded = sum(
                flow.amount
                for target, flow in outgoing[recipient]
                if target in hub_set and flow.first_ts - start <= deposit_forward_hours * 3600
            )
            is_deposit = total and forwarded * 100 >= deposit_forward_pct * total
            kinds[recipient] = DEPOSIT if is_deposit else VAULT
    return kinds


@dataclass
class Holder:
    address: str
    bought: int = 0
    cost: float = 0.0
    first_block: int = 0
    first_ts: int = 0
    out: int = 0
    inherited: int = 0
    inherited_cost: float = 0.0
    inherited_from: str = ""
    sent_to_vaults: int = 0

    @property
    def inflow(self) -> int:
        return self.bought + self.inherited

    @property
    def avg_cost(self) -> float:
        return (self.cost + self.inherited_cost) / self.inflow if self.inflow else 0.0

    @property
    def held(self) -> int:
        return max(self.inflow - self.out - self.sent_to_vaults, 0)

    @property
    def held_cost(self) -> float:
        return self.held * self.avg_cost


@dataclass(frozen=True)
class VaultLink:
    sender: str
    recipient: str
    amount: int
    pct: float
    first_block: int
    tx_hash: str


@dataclass
class Flows:
    holders: dict[str, Holder]
    links: list[VaultLink] = field(default_factory=list)


def resolve_flows(
    scanner: FlowScanner, kinds: dict[str, str], *, big_pct: float, bots=frozenset()
) -> Flows:
    holders: dict[str, Holder] = {}
    for address, amount in scanner.bought.items():
        block, ts = scanner.first_buy[address]
        holders[address] = Holder(address, amount, scanner.cost[address], block, ts)
    for address, amount in scanner.sold.items():
        holders.setdefault(address, Holder(address)).out += amount
    links = []
    for (sender, recipient), flow in sorted(scanner.moves.items(), key=lambda kv: kv[1].first_block):
        source = holders.get(sender)
        if source is None or source.inflow == 0:
            continue
        pct = flow.amount * 100 / source.inflow
        is_vault = (
            kinds.get(recipient) == VAULT
            and pct >= big_pct
            and sender not in bots
            and recipient not in bots
        )
        available = source.inflow - source.out - source.sent_to_vaults
        if not is_vault or available <= 0:
            source.out += flow.amount
            continue
        amount = min(flow.amount, available)
        vault = holders.setdefault(recipient, Holder(recipient))
        vault.inherited += amount
        vault.inherited_cost += amount * source.avg_cost
        vault.inherited_from = vault.inherited_from or sender
        if source.first_block and (not vault.first_block or source.first_block < vault.first_block):
            vault.first_block, vault.first_ts = source.first_block, source.first_ts
        source.sent_to_vaults += amount
        links.append(VaultLink(sender, recipient, amount, round(pct, 2), flow.first_block, flow.tx_hash))
    for bot in bots:
        holders.pop(bot, None)
    return Flows(holders, links)


@dataclass(frozen=True)
class EntityCandidate:
    wallets: list[str]
    held_amount: int
    held_usd: float
    first_ts: int


def group_entities(flows: Flows, *, trough_price: float, scale: int) -> list[EntityCandidate]:
    parent: dict[str, str] = {}

    def find(address: str) -> str:
        parent.setdefault(address, address)
        while parent[address] != address:
            parent[address] = parent[parent[address]]
            address = parent[address]
        return address

    for link in flows.links:
        parent[find(link.sender)] = find(link.recipient)
    groups: dict[str, list[str]] = defaultdict(list)
    for address, holder in flows.holders.items():
        if holder.held > 0 or address in parent:
            groups[find(address)].append(address)
    candidates = []
    for members in groups.values():
        held = sum(flows.holders[a].held for a in members if a in flows.holders)
        firsts = [flows.holders[a].first_ts for a in members if flows.holders.get(a) and flows.holders[a].first_ts]
        candidates.append(
            EntityCandidate(sorted(members), held, round(held / scale * trough_price, 2), min(firsts, default=0))
        )
    return sorted(candidates, key=lambda c: c.held_usd, reverse=True)


@dataclass
class Selection:
    flows: Flows
    selected: list[EntityCandidate]
    bots: dict[str, tuple[float, float]]


def select_entities(
    scanner: FlowScanner,
    kinds: dict[str, str],
    *,
    bot_check: Callable[[str], tuple[bool, float]],
    big_pct: float,
    trough_price: float,
    scale: int,
    min_usd: float,
    max_entities: int,
) -> Selection:
    """Top des entités au creux ; un bot trouvé est retiré et le calcul recommence sans lui."""
    bots: dict[str, tuple[float, float]] = {}
    checked: dict[str, tuple[bool, float]] = {}
    while True:
        flows = resolve_flows(scanner, kinds, big_pct=big_pct, bots=frozenset(bots))
        ranked = [
            c for c in group_entities(flows, trough_price=trough_price, scale=scale) if c.held_usd >= min_usd
        ]
        selected: list[EntityCandidate] = []
        found = False
        for candidate in ranked:
            for wallet in candidate.wallets:
                if wallet not in checked:
                    checked[wallet] = bot_check(wallet)
                if checked[wallet][0]:
                    held = flows.holders.get(wallet)
                    usd = round(held.held / scale * trough_price, 2) if held else 0.0
                    bots[wallet] = (checked[wallet][1], usd)
                    found = True
            if found:
                break
            selected.append(candidate)
            if max_entities and len(selected) >= max_entities:
                break
        if not found:
            return Selection(flows, selected, bots)


@dataclass(frozen=True)
class NewVault:
    sender: str
    amount: int
    block: int
    tx_hash: str


class EntityPass:
    """Passe filtrée sur les wallets retenus : transferts bruts, ventes de la montée, coffres."""

    def __init__(self, *, group_of: dict[str, int], held: dict[str, int], trough_block: int,
                 pools: set[str], exits: set[str], big_pct: float):
        self.group_of = dict(group_of)
        self.held = defaultdict(int, held)
        self.trough_block = trough_block
        self.pools = set(pools)
        self.exits = set(exits)
        self.big_pct = big_pct
        self.rows: list[tuple[Transfer, str]] = []
        self.sold_rise: dict[str, int] = defaultdict(int)
        self._new: dict[str, NewVault] = {}
        self._seen: set[tuple[str, int]] = set()

    def add(self, transfers: list[Transfer]) -> None:
        for t in sorted(transfers, key=lambda t: (t.block, t.log_index)):
            key = (t.tx_hash, t.log_index)
            if key in self._seen:
                continue
            sender_in, recipient_in = t.sender in self.group_of, t.recipient in self.group_of
            if not (sender_in or recipient_in):
                continue
            self._seen.add(key)
            self.rows.append((t, self._kind(t, sender_in, recipient_in)))

    def _kind(self, t: Transfer, sender_in: bool, recipient_in: bool) -> str:
        signer = t.tx_from
        if sender_in and recipient_in and self.group_of[t.sender] == self.group_of[t.recipient]:
            return "internal"
        if recipient_in and not sender_in:
            is_buy = t.recipient == signer and t.sender not in (signer, ZERO_ADDRESS)
            return "buy" if is_buy else "receive"
        rise = t.block > self.trough_block
        if t.recipient in self.pools:
            kind = "sell"
        elif (
            rise
            and t.recipient not in self.exits
            and self.held[t.sender]
            and t.amount * 100 >= self.big_pct * self.held[t.sender]
        ):
            self._new.setdefault(t.recipient, NewVault(t.sender, t.amount, t.block, t.tx_hash))
            return "vault"
        else:
            kind = "exit"
        if rise:
            self.sold_rise[t.sender] += t.amount
        return kind

    def take_new_vaults(self) -> dict[str, NewVault]:
        new, self._new = self._new, {}
        for vault, info in new.items():
            self.group_of[vault] = self.group_of[info.sender]
            self.held[vault] += info.amount
        return new
```

Formater avec `ruff format` (les lignes longues ci-dessus seront repliées).

- [ ] **Step 4: Vérifier** — `make test args="apps/discovery/tests/test_flows.py"` → PASS ; corriger l'implémentation (pas les attendus) si un cas échoue, sauf erreur d'arithmétique avérée dans le test.

- [ ] **Step 5: Commit** — `feat(discovery): flux purs (destinataires, héritage, entités, bots, passe entité)`

---

### Task 5: Extraction par entités

**Files:**
- Modify: `backend/apps/discovery/services/extraction.py`, `backend/apps/discovery/tests/test_extraction.py`, `backend/apps/discovery/tests/test_tasks.py`
- Delete: `backend/apps/discovery/services/buyers.py`, `backend/apps/discovery/tests/test_buyers.py`

**Interfaces:**
- Consumes: Tasks 1-4 (`FlowScanner`, `classify_recipients`, `select_entities`, `EntityPass`, `hubs`, `entity_graph.link/ensure_entity`, `refresh_entity_buys`, `transfer_pages(participants=)`, `wallet_tx_count`).
- Produces: `extract_buyers(candidate, *, gt, hypersync, cfg, now) -> int` (nombre d'**entités** retenues) ; `bot_checker(hypersync, chain, explosion, thresholds, q) -> Callable[[str], tuple[bool, float]]`.

- [ ] **Step 1: Tests** — `test_extraction.py` : `test_stores_significant_eoa_buyers` attend désormais 2 **entités** (sniper, Alice) et, en plus des assertions actuelles, `EntityEarlyBuy.objects.count() == 2` et `buyers[ALICE].entity_id is not None`. `test_two_passes_…` devient :

```python
def test_scan_then_entity_pass_on_retained_wallets(confirmed):
    hypersync = FakeHyperSync()
    extract(confirmed, hypersync)
    explosion = confirmed.explosion
    first, *rest = hypersync.transfer_calls
    assert first == (TOKEN, 500, explosion.trough_block + 1, None, None)
    assert rest[0][4] == sorted([ALICE, SNIPER])
    assert rest[0][2] == explosion.peak_block + 1
    assert TokenTransfer.objects.filter(explosion=explosion, kind="sell").count() == 1
```

Remplacer `test_sell_pass_is_batched` par `test_entity_pass_is_batched` (`cfg.sell_pass_batch_size = 1` → deux appels participants `[[SNIPER], [ALICE]]`). Ajouter :

```python
VAULT = "0x" + "7" * 40


def vault_transfers():
    base = FakeHyperSync()._all_transfers()
    move = Transfer(610, FakeHyperSync().block_timestamp(610), ALICE, ALICE, VAULT, 600 * UNIT, "0xmove", 0)
    return base + [move]


def test_vault_inherits_and_joins_alice_entity(confirmed):
    extract(confirmed, FakeHyperSync(transfers=vault_transfers()))
    alice = EarlyBuyer.objects.get(wallet__address=ALICE)
    vault = EarlyBuyer.objects.get(wallet__address=VAULT)
    assert alice.entity_id == vault.entity_id
    assert vault.inherited_amount == Decimal(600 * UNIT)
    assert vault.inherited_from.address == ALICE
    assert WalletLink.objects.get(to_wallet__address=VAULT).source == "hypersync"
    assert EntityEarlyBuy.objects.get(entity_id=alice.entity_id).held_usd == Decimal("500.00")


def test_bot_is_excluded_and_recorded(confirmed):
    extract(confirmed, FakeHyperSync(tx_counts={SNIPER: 10_000}))
    assert not EarlyBuyer.objects.filter(wallet__address=SNIPER).exists()
    excluded = ExcludedBuyer.objects.get()
    assert (excluded.wallet.address, excluded.reason) == (SNIPER, "bot")
```

(imports : `EntityEarlyBuy, ExcludedBuyer, TokenTransfer` de `apps.discovery.models`, `WalletLink` de `apps.wallets.models`, `Transfer` de `integrations.hypersync`.) Le test « garde-fou partiel » et « fenêtre d'achat » restent (le tuple d'appel a maintenant 5 éléments : `hypersync.transfer_calls[0][1]`). `test_tasks.py` : `EarlyBuyer.objects.count() == 2` inchangé.

- [ ] **Step 2: Vérifier l'échec** — `make test args="apps/discovery/tests/test_extraction.py"` → FAIL.

- [ ] **Step 3: Implémenter `extraction.py`**

```python
"""Extraction des early buyers d'une explosion, par entité (passe complète puis passe filtrée)."""

from datetime import UTC, datetime
from decimal import Decimal

import redis
from django.conf import settings
from django.db import transaction

from apps.discovery.models import (
    Candidate,
    EarlyBuyer,
    ExcludedBuyer,
    Explosion,
    PipelineSettings,
    TokenTransfer,
    Wallet,
)
from apps.discovery.services.analysis import fetch_price_history, reject
from apps.discovery.services.blocks import find_block_at, find_block_near
from apps.discovery.services.entity_buys import refresh_entity_buys
from apps.discovery.services.flows import (
    DEPOSIT,
    KNOWN,
    EntityPass,
    FlowScanner,
    classify_recipients,
    hubs,
    select_entities,
)
from apps.discovery.services.settings import Thresholds, thresholds_for
from apps.wallets.models import BLOCKING_KINDS, KnownAddress, WalletLink
from apps.wallets.services import entity_graph
from apps.wallets.services.settings import qualification_thresholds

BATCH_SIZE = 1000
DAY = 86_400


def _at(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=UTC)


def buy_window_start(explosion: Explosion, first_block: int, thresholds: Thresholds, hypersync) -> int:
    (inchangé)


def bot_checker(hypersync, chain, explosion: Explosion, thresholds: Thresholds, q):
    """Transactions signées sur les `bot_window_days` jours avant le creux, en cache Redis."""
    days = thresholds.bot_window_days
    limit = q.max_txs_per_day * days
    start_ts = int(explosion.trough_at.timestamp()) - days * DAY
    start = find_block_near(start_ts, explosion.trough_block, hypersync.block_timestamp)
    cache = redis.Redis.from_url(settings.REDIS_URL)

    def check(address: str) -> tuple[bool, float]:
        key = f"botcheck:{chain.pk}:{explosion.trough_block}:{days}:{address}"
        cached = cache.get(key)
        if cached is None:
            count = hypersync.wallet_tx_count(address, start, explosion.trough_block + 1, limit + 1)
            cache.set(key, count, ex=30 * DAY)
        else:
            count = int(cached)
        return count > limit, round(count / days, 1)

    return check


def extract_buyers(candidate: Candidate, *, gt, hypersync, cfg: PipelineSettings, now: datetime) -> int:
    token = candidate.token
    chain = token.chain
    explosion = candidate.explosion
    thresholds = thresholds_for(chain)
    q = qualification_thresholds()

    pools = list(token.pools.exclude(created_block__isnull=True))
    if not pools:
        reject(candidate, "no_pool")
        return 0
    first_block = min(pool.created_block for pool in pools)
    history = fetch_price_history(gt, chain, token, now)

    # Passe 1 : flux complets de la fenêtre d'achat jusqu'au creux.
    scanner = FlowScanner(
        candles=history.candles,
        decimals=token.decimals,
        pools={pool.address.lower() for pool in token.pools.all()},
        hub_min_senders=thresholds.hub_min_senders,
    )
    status = Explosion.Extraction.COMPLETE
    seen = 0
    start = buy_window_start(explosion, first_block, thresholds, hypersync)
    for page in hypersync.transfer_pages(token.address, start, explosion.trough_block + 1):
        scanner.add(page)
        seen += len(page)
        if seen >= cfg.max_transfers_per_token:
            status = Explosion.Extraction.PARTIAL
            break

    known = set(
        KnownAddress.objects.filter(kind__in=BLOCKING_KINDS).values_list("address", flat=True)
    )
    kinds = classify_recipients(
        scanner,
        known=known,
        deposit_forward_pct=q.deposit_forward_pct,
        deposit_forward_hours=q.deposit_forward_hours,
    )
    trough_price = scanner.price_at(int(explosion.trough_at.timestamp()))
    scale = 10**token.decimals
    selection = select_entities(
        scanner,
        kinds,
        bot_check=bot_checker(hypersync, chain, explosion, thresholds, q),
        big_pct=thresholds.vault_min_pct,
        trough_price=trough_price,
        scale=scale,
        min_usd=thresholds.min_buy_usd,
        max_entities=thresholds.max_buyers,
    )

    # Passe entité : transferts bruts, ventes pendant la montée, coffres suivis.
    group_of = {w: i for i, c in enumerate(selection.selected) for w in c.wallets}
    held = {w: selection.flows.holders[w].held for w in group_of if w in selection.flows.holders}
    exits = hubs(scanner) | {r for r, k in kinds.items() if k in (DEPOSIT, KNOWN)}
    tracker = EntityPass(
        group_of=group_of,
        held=held,
        trough_block=explosion.trough_block,
        pools=scanner.pools,
        exits=exits,
        big_pct=thresholds.vault_min_pct,
    )
    batch = max(cfg.sell_pass_batch_size, 1)
    wave, wave_start, rise_links = sorted(group_of), start, []
    for level in range(thresholds.vault_follow_depth + 1):
        for offset in range(0, len(wave), batch):
            for page in hypersync.transfer_pages(
                token.address, wave_start, explosion.peak_block + 1,
                participants=wave[offset : offset + batch],
            ):
                tracker.add(page)
        new = tracker.take_new_vaults()
        rise_links += [(v.sender, vault, v) for vault, v in new.items()]
        if not new or level == thresholds.vault_follow_depth:
            break
        wave, wave_start = sorted(new), min(v.block for v in new.values())

    with transaction.atomic():
        _save(candidate, explosion, token, selection, tracker, rise_links, trough_price, scale,
              scanner, first_block, thresholds, status)
    return len(selection.selected)


def _save(candidate, explosion, token, selection, tracker, rise_links, trough_price, scale,
          scanner, first_block, thresholds, status) -> None:
    flows = selection.flows
    for link in flows.links:
        if link.sender in tracker.group_of:
            entity_graph.link(
                link.sender, link.recipient, WalletLink.Kind.TRANSFER_TO_VAULT,
                WalletLink.LinkSource.HYPERSYNC,
                {"chain": token.chain.gt_id, "token": token.address, "pct": link.pct,
                 "amount": str(link.amount), "block": link.first_block, "tx": link.tx_hash},
            )
    for sender, vault, info in rise_links:
        entity_graph.link(
            sender, vault, WalletLink.Kind.TRANSFER_TO_VAULT, WalletLink.LinkSource.HYPERSYNC,
            {"chain": token.chain.gt_id, "token": token.address, "amount": str(info.amount),
             "block": info.block, "tx": info.tx_hash, "during_rise": True},
        )
    members = sorted(tracker.group_of)
    Wallet.objects.bulk_create([Wallet(address=a) for a in members], ignore_conflicts=True)
    wallets = {w.address: w for w in Wallet.objects.filter(address__in=members)}
    for address in members:
        entity_graph.ensure_entity(wallets[address])

    rows = []
    for address in members:
        holder = flows.holders.get(address)
        own = scanner.first_buy.get(address)
        first_block_, first_ts = (holder.first_block, holder.first_ts) if holder and holder.first_block else (explosion.trough_block, int(explosion.trough_at.timestamp()))
        held_amount = holder.held if holder else tracker.held[address]
        rows.append(EarlyBuyer(
            explosion=explosion,
            wallet=wallets[address],
            entity_id=wallets[address].entity_id,
            first_buy_block=first_block_,
            first_buy_at=_at(first_ts),
            bought_amount=Decimal(holder.bought if holder else 0),
            bought_usd=Decimal(str(round(holder.cost, 2))) if holder else Decimal(0),
            held_amount=Decimal(held_amount),
            held_usd=Decimal(str(round(held_amount / scale * trough_price, 2))),
            inherited_amount=Decimal(holder.inherited if holder else tracker.held[address]),
            inherited_usd=Decimal(str(round(holder.inherited_cost, 2))) if holder else Decimal(0),
            inherited_from=wallets.get(holder.inherited_from) if holder and holder.inherited_from else None,
            sold_amount=Decimal(tracker.sold_rise.get(address, 0)),
            is_sniper=bool(own) and own[0] - first_block <= thresholds.sniper_blocks,
        ))
    EarlyBuyer.objects.bulk_create(rows, ignore_conflicts=True, batch_size=BATCH_SIZE)
    # Les ensure_entity/link ont pu fusionner des entités : on relit l'entité courante.
    for row in EarlyBuyer.objects.filter(explosion=explosion).select_related("wallet"):
        if row.entity_id != row.wallet.entity_id:
            row.entity_id = row.wallet.entity_id
            row.save(update_fields=["entity"])
    refresh_entity_buys(explosion)

    bots = selection.bots
    Wallet.objects.bulk_create([Wallet(address=a) for a in bots], ignore_conflicts=True)
    bot_wallets = {w.address: w for w in Wallet.objects.filter(address__in=list(bots))}
    ExcludedBuyer.objects.bulk_create(
        [
            ExcludedBuyer(explosion=explosion, wallet=bot_wallets[a], reason="bot",
                          txs_per_day=Decimal(str(per_day)), held_usd=Decimal(str(usd)))
            for a, (per_day, usd) in bots.items()
        ],
        ignore_conflicts=True,
    )
    TokenTransfer.objects.bulk_create(
        [
            TokenTransfer(explosion=explosion, token=token, tx_hash=t.tx_hash, log_index=t.log_index,
                          block=t.block, at=_at(t.timestamp), tx_from=t.tx_from, sender=t.sender,
                          recipient=t.recipient, amount=Decimal(t.amount), kind=kind)
            for t, kind in tracker.rows
        ],
        ignore_conflicts=True,
        batch_size=BATCH_SIZE,
    )
    explosion.extraction_status = status
    explosion.save(update_fields=["extraction_status"])
    candidate.status = Candidate.Status.BUYERS_EXTRACTED
    candidate.save(update_fields=["status", "updated_at"])
```

Supprimer `buyers.py` et `test_buyers.py` (`git rm`). Formater avec `ruff format`.

- [ ] **Step 4: Vérifier** — `make test && make lint` → PASS (les tests `wallets` qui construisent des `EarlyBuyer` via la fabrique restent valides : les nouveaux champs ont des défauts).

- [ ] **Step 5: Commit** — `feat(discovery): early buyers classés par entité, sans bots, avec transferts bruts`

---

### Task 6: Qualification — liens Zerion en entités, mouvements internes, valeur et priorité par entité

**Files:**
- Modify: `backend/apps/wallets/services/qualification.py`, `backend/apps/wallets/services/entities.py`
- Test: `backend/apps/wallets/tests/test_qualification_entities.py` (nouveau), tests existants à adapter

**Interfaces:**
- Consumes: `entity_graph.link`, `EntityEarlyBuy`, `TokenTrade.is_internal`.
- Produces: `mark_internal(wallet) -> int` ; `records_for(wallet)` ignore `is_internal` ; `linked_profiles(wallet)` = profils des autres wallets de l'entité ; `compute_priority(wallet, cfg)` par entité ; `add_link` supprimé de `entities.py`.

- [ ] **Step 1: Tests**

```python
import pytest

from apps.discovery.models import PipelineSettings, Wallet
from apps.discovery.tests.factories import make_chain
from apps.wallets.models import TokenTrade, WalletLink, WalletProfile
from apps.wallets.services import entity_graph
from apps.wallets.services.qualification import (
    compute_priority,
    linked_profiles,
    mark_internal,
    records_for,
)
from apps.wallets.tests.factories import make_early_buy
from apps.wallets.tests.fakes import BUYER, TOKEN_A, TOKEN_B, VAULT

pytestmark = pytest.mark.django_db


def test_trades_between_entity_wallets_are_internal(make_trade):
    entity_graph.link(BUYER, VAULT, WalletLink.Kind.TRANSFER_AFTER_BUY, "zerion", {})
    buyer = Wallet.objects.get(address=BUYER)
    make_trade(buyer, kind="send", counterparty=VAULT)
    make_trade(buyer, kind="buy", counterparty="0x" + "9" * 40)
    assert mark_internal(buyer) == 1
    assert [r.kind for r in records_for(buyer)] == ["buy"]


def test_linked_profiles_are_entity_members():
    entity_graph.link(BUYER, VAULT, WalletLink.Kind.TRANSFER_AFTER_BUY, "zerion", {})
    buyer = Wallet.objects.get(address=BUYER)
    vault = Wallet.objects.get(address=VAULT)
    WalletProfile.objects.create(wallet=buyer)
    other = WalletProfile.objects.create(wallet=vault, source="linked")
    assert list(linked_profiles(buyer)) == [other]


def test_priority_counts_entity_explosions():
    chain = make_chain()
    one = Wallet.objects.create(address="0x" + "1" * 40)
    two = Wallet.objects.create(address="0x" + "2" * 40)
    make_early_buy(one, chain, TOKEN_A)
    make_early_buy(two, chain, TOKEN_B)
    entity_graph.link(one.address, two.address, WalletLink.Kind.TRANSFER_TO_VAULT, "hypersync", {})
    one.refresh_from_db()
    cfg = PipelineSettings.load()
    solo = Wallet.objects.create(address="0x" + "3" * 40)
    make_early_buy(solo, chain, "0x" + "c" * 40)
    assert compute_priority(one, cfg) > compute_priority(solo, cfg)
```

`make_trade` : fixture dans `apps/wallets/tests/conftest.py` qui crée une `WalletTransaction` minimale puis un `TokenTrade` (`chain="base"`, `token_address=TOKEN_A`, `quantity=1`, `amount=10**18`, `mined_at=NOW`, `direction` = `out` pour send/sell, `in` sinon, `transfer_index` incrémental).

`make_early_buy` (fabrique) : après création, `refresh_entity_buys` n'est pas nécessaire — `compute_priority` lit `EarlyBuyer` quand `EntityEarlyBuy` est vide (voir implémentation).

- [ ] **Step 2: Vérifier l'échec** — `make test args="apps/wallets/tests/test_qualification_entities.py"` → FAIL.

- [ ] **Step 3: Implémenter**

`entities.py` : supprimer `add_link`. `qualification.py` :

```python
def mark_internal(wallet) -> int:
    """Envois / réceptions entre wallets d'une même entité : ni achat ni vente."""
    if wallet.entity_id is None:
        return 0
    members = list(
        Wallet.objects.filter(entity_id=wallet.entity_id).values_list("address", flat=True)
    )
    return TokenTrade.objects.filter(
        wallet__entity_id=wallet.entity_id,
        kind__in=[TokenTrade.Kind.SEND, TokenTrade.Kind.RECEIVE],
        counterparty__in=members,
        is_internal=False,
    ).update(is_internal=True)
```

`records_for` : `TokenTrade.objects.filter(wallet=wallet, is_internal=False)`. `link_step` : chaque `add_link(source, target, kind, evidence)` devient `entity_graph.link(source, target, kind, WalletLink.LinkSource.ZERION, evidence)` (y compris `FUNDING`). `decide_step`, après `link_step(...)` :

```python
    wallet.refresh_from_db()
    if mark_internal(wallet):
        recompute_positions(wallet)
    records = records_for(wallet)
```

`linked_profiles` :

```python
def linked_profiles(wallet):
    if wallet.entity_id is None:
        return WalletProfile.objects.none()
    return WalletProfile.objects.filter(wallet__entity_id=wallet.entity_id).exclude(wallet=wallet)
```

`compute_priority` :

```python
def compute_priority(wallet, cfg) -> float:
    """Explosions captées par l'entité (un rug pèse `rug_priority_weight`), puis position au creux."""
    rug = Q(explosion__retention_status=Explosion.Retention.RUG)
    if wallet.entity_id:
        rows = EntityEarlyBuy.objects.filter(entity_id=wallet.entity_id)
        if not rows.exists():
            rows = EarlyBuyer.objects.filter(wallet__entity_id=wallet.entity_id)
    else:
        rows = EarlyBuyer.objects.filter(wallet=wallet)
    stats = rows.aggregate(
        kept=Count("explosion", filter=~rug, distinct=True),
        rugs=Count("explosion", filter=rug, distinct=True),
        usd=Sum("held_usd"),
    )
    explosions = stats["kept"] + float(cfg.rug_priority_weight) * stats["rugs"]
    return explosions * 1_000_000 + min(float(stats["usd"] or 0), 999_999.0)
```

(imports : `EntityEarlyBuy`, `Wallet`, `entity_graph`.) Adapter les tests existants qui créaient des `WalletLink` à la main pour tester la valeur liée : passer par `entity_graph.link`.

- [ ] **Step 4: Vérifier** — `make test && make lint` → PASS.

- [ ] **Step 5: Commit** — `feat(wallets): entités côté qualification (liens, mouvements internes, valeur, priorité)`

---

### Task 7: Zerion — portefeuille par token et types d'opérations réglables

**Files:**
- Modify: `backend/integrations/zerion.py`, `backend/apps/discovery/services/clients.py`, `backend/apps/wallets/tests/fakes.py`
- Create: `backend/apps/wallets/services/raw.py`
- Modify: `backend/apps/wallets/services/qualification.py` (`decide_step`, `value_linked_step`)
- Test: `backend/integrations/tests/test_zerion_history.py`, `backend/apps/wallets/tests/test_raw.py`

**Interfaces — Produces:**
- `PortfolioItem(chain, token_address, fungible_id, symbol, position_type, quantity: Decimal, price_usd, value_usd)` ; `Portfolio(total_usd, by_chain, positions=[], raw={})`.
- `ZerionClient(http, operation_types=OPERATION_TYPES)` ; `portfolio(address)` lit `/wallets/{a}/positions/?filter[positions]=no_filter&filter[trash]=only_non_trash` (suit `links.next`).
- `raw.save_portfolio(wallet, portfolio, now) -> PortfolioSnapshot`.

- [ ] **Step 1: Tests** — remplacer `test_portfolio` dans `integrations/tests/test_zerion_history.py` :

```python
@respx.mock
def test_portfolio_from_positions():
    route = respx.get(f"{BASE_URL}/wallets/{W}/positions/").respond(
        json={
            "data": [
                {
                    "attributes": {
                        "position_type": "wallet",
                        "quantity": {"numeric": "4855000.5"},
                        "price": 0.2124,
                        "value": 1031383.02,
                        "fungible_info": {
                            "symbol": "AI",
                            "implementations": [{"chain_id": "robinhood", "address": TOKEN.upper().replace("0X", "0x")}],
                        },
                    },
                    "relationships": {
                        "chain": {"data": {"id": "robinhood"}},
                        "fungible": {"data": {"id": "ai-id"}},
                    },
                },
                {
                    "attributes": {
                        "position_type": "wallet",
                        "quantity": {"numeric": "0.5"},
                        "price": 2600.0,
                        "value": 1300.0,
                        "fungible_info": {"symbol": "ETH", "implementations": [{"chain_id": "base", "address": None}]},
                    },
                    "relationships": {"chain": {"data": {"id": "base"}}, "fungible": {"data": {"id": "eth"}}},
                },
            ],
            "links": {},
        }
    )
    portfolio = client().portfolio(W)
    assert portfolio.total_usd == 1032683.02
    assert portfolio.by_chain == {"robinhood": 1031383.02, "base": 1300.0}
    ai, eth = portfolio.positions
    assert (ai.chain, ai.token_address, ai.fungible_id, ai.quantity) == ("robinhood", TOKEN, "ai-id", Decimal("4855000.5"))
    assert eth.token_address == NATIVE
    assert route.calls.last.request.url.params["filter[positions]"] == "no_filter"


@respx.mock
def test_operation_types_are_configurable():
    route = respx.get(f"{BASE_URL}/wallets/{W}/transactions/").respond(json={"data": [], "links": {}})
    custom = ZerionClient(JsonHttpClient(BASE_URL, limiter=NoopLimiter(), max_retries=0), operation_types="trade,deposit")
    custom.transactions(W, datetime(2026, 3, 1, tzinfo=UTC))
    assert route.calls.last.request.url.params["filter[operation_types]"] == "trade,deposit"
```

`apps/wallets/tests/test_raw.py` :

```python
from decimal import Decimal

import pytest

from apps.discovery.models import Wallet
from apps.wallets.services.raw import save_portfolio
from apps.wallets.tests.fakes import NOW
from integrations.zerion import Portfolio, PortfolioItem

pytestmark = pytest.mark.django_db


def test_save_portfolio_keeps_each_token():
    wallet = Wallet.objects.create(address="0x" + "1" * 40)
    portfolio = Portfolio(
        1300.0,
        {"base": 1300.0},
        [PortfolioItem("base", "native", "eth", "ETH", "wallet", Decimal("0.5"), 2600.0, 1300.0)],
        {"data": []},
    )
    snapshot = save_portfolio(wallet, portfolio, NOW)
    [position] = snapshot.positions.all()
    assert (position.symbol, position.quantity, float(position.value_usd)) == ("ETH", Decimal("0.5"), 1300.0)
    assert float(snapshot.total_usd) == 1300.0
```

- [ ] **Step 2: Vérifier l'échec** — `make test args="integrations/tests/test_zerion_history.py apps/wallets/tests/test_raw.py"` → FAIL.

- [ ] **Step 3: Implémenter** — `zerion.py` :

```python
@dataclass(frozen=True)
class PortfolioItem:
    chain: str
    token_address: str
    fungible_id: str
    symbol: str
    position_type: str
    quantity: Decimal
    price_usd: float | None
    value_usd: float | None


@dataclass(frozen=True)
class Portfolio:
    total_usd: float
    by_chain: dict[str, float]
    positions: list[PortfolioItem] = field(default_factory=list)
    raw: dict = field(default_factory=dict)


def _position(item: dict) -> PortfolioItem:
    attributes = item["attributes"]
    relationships = item.get("relationships") or {}
    chain = ((relationships.get("chain") or {}).get("data") or {}).get("id", "")
    info = attributes.get("fungible_info") or {}
    implementation = next(
        (i for i in info.get("implementations") or [] if i.get("chain_id") == chain), {}
    )
    return PortfolioItem(
        chain=chain,
        token_address=(implementation.get("address") or NATIVE).lower(),
        fungible_id=((relationships.get("fungible") or {}).get("data") or {}).get("id", ""),
        symbol=info.get("symbol") or "",
        position_type=attributes.get("position_type") or "",
        quantity=Decimal(str((attributes.get("quantity") or {}).get("numeric") or "0")),
        price_usd=_optional_float(attributes.get("price")),
        value_usd=_optional_float(attributes.get("value")),
    )
```

`ZerionClient.__init__(self, http, operation_types: str = OPERATION_TYPES)` → `self._operation_types` utilisé dans `transactions`. `portfolio` :

```python
    def portfolio(self, address: str) -> Portfolio:
        params = {
            "currency": "usd",
            "filter[positions]": "no_filter",
            "filter[trash]": "only_non_trash",
            "page[size]": 100,
        }
        items, pages = [], []
        cursor = None
        while True:
            payload = self._http.get(
                f"/wallets/{address}/positions/",
                params={**params, **({"page[after]": cursor} if cursor else {})},
            )
            pages.append(payload)
            items += [_position(item) for item in payload.get("data", [])]
            cursor = _cursor((payload.get("links") or {}).get("next"))
            if cursor is None:
                break
        by_chain: dict[str, float] = {}
        for item in items:
            by_chain[item.chain] = round(by_chain.get(item.chain, 0.0) + (item.value_usd or 0.0), 2)
        total = round(sum(item.value_usd or 0.0 for item in items), 2)
        return Portfolio(total, by_chain, items, {"pages": pages})
```

(`from dataclasses import dataclass, field`.) `clients.zerion(cfg)` passe `operation_types=cfg.zerion_operation_types`. `FakeZerion.portfolio` inchangé (les défauts couvrent `positions`/`raw`).

`raw.py` :

```python
"""Couche brute Zerion : portefeuille par token, métadonnées de tokens."""

from decimal import Decimal

from apps.wallets.models import PortfolioPosition, PortfolioSnapshot


def _decimal(value, places=2):
    return Decimal(str(round(value, places))) if value is not None else None


def save_portfolio(wallet, portfolio, now) -> PortfolioSnapshot:
    snapshot = PortfolioSnapshot.objects.create(
        wallet=wallet, fetched_at=now, total_usd=_decimal(portfolio.total_usd), raw=portfolio.raw
    )
    PortfolioPosition.objects.bulk_create(
        [
            PortfolioPosition(
                snapshot=snapshot,
                chain=p.chain,
                token_address=p.token_address,
                fungible_id=p.fungible_id[:100],
                symbol=p.symbol[:64],
                position_type=p.position_type[:32],
                quantity=p.quantity,
                price_usd=Decimal(str(p.price_usd)) if p.price_usd is not None else None,
                value_usd=_decimal(p.value_usd),
            )
            for p in portfolio.positions
        ]
    )
    return snapshot
```

`decide_step` et `value_linked_step` : après `portfolio = clients.zerion.portfolio(...)`, appeler `save_portfolio(profile.wallet, portfolio, now)`.

- [ ] **Step 4: Vérifier** — `make test && make lint` → PASS.

- [ ] **Step 5: Commit** — `feat(zerion): portefeuille par token et types d'opérations réglables`

---

### Task 8: Métadonnées de tokens (`TokenInfo`)

**Files:**
- Modify: `backend/integrations/zerion.py`, `backend/apps/wallets/services/raw.py`, `backend/apps/wallets/tasks.py`, `backend/apps/wallets/tests/fakes.py`
- Test: `backend/integrations/tests/test_zerion_prices.py`, `backend/apps/wallets/tests/test_raw.py`

**Interfaces — Produces:**
- `TokenMeta(fungible_id, symbol, name, verified, total_supply, circulating_supply, implementations: dict[str, tuple[str, int]], raw)`.
- `ZerionClient.token_metadata(implementations: list[tuple[str, str]]) -> list[TokenMeta]` (lots de `MAX_IMPLEMENTATIONS_PER_CALL`).
- `raw.refresh_token_info(zerion, now, cfg) -> int` (tokens enregistrés ; s'arrête proprement sur `BudgetExhausted`).
- `FakeZerion.token_metadata(implementations)` (depuis `metas: dict[(chain, address), TokenMeta]`).

- [ ] **Step 1: Tests** — `test_zerion_prices.py` :

```python
@respx.mock
def test_token_metadata_parses_supply():
    respx.get(f"{BASE_URL}/fungibles/").respond(
        json={
            "data": [
                {
                    "id": "ai-id",
                    "attributes": {
                        "name": "AI",
                        "symbol": "AI",
                        "flags": {"verified": False},
                        "implementations": [{"chain_id": "robinhood", "address": "0xAbC", "decimals": 18}],
                        "market_data": {"total_supply": 991257335.43, "circulating_supply": 991258467.25},
                    },
                }
            ]
        }
    )
    [meta] = client().token_metadata([("robinhood", "0xabc")])
    assert (meta.fungible_id, meta.total_supply, meta.verified) == ("ai-id", 991257335.43, False)
    assert meta.implementations == {"robinhood": ("0xabc", 18)}
```

`test_raw.py` :

```python
def test_refresh_token_info_fetches_unknown_tokens_once(make_trade):
    wallet = Wallet.objects.create(address="0x" + "1" * 40)
    make_trade(wallet, kind="buy", counterparty="0x" + "9" * 40)
    zerion = FakeZerion(metas={("base", TOKEN_A): meta_for(TOKEN_A)})
    cfg = PipelineSettings.load()
    assert refresh_token_info(zerion, NOW, cfg) == 1
    info = TokenInfo.objects.get()
    assert (info.fungible_id, float(info.total_supply)) == ("fid", 1_000_000.0)
    assert refresh_token_info(zerion, NOW, cfg) == 0
    assert zerion.calls["token_metadata"] == 1
```

(`meta_for(address)` : fabrique dans `fakes.py` renvoyant `TokenMeta("fid", "TKA", "Token A", False, 1_000_000.0, 900_000.0, {"base": (address, 18)}, {})`.)

- [ ] **Step 2: Vérifier l'échec** — FAIL.

- [ ] **Step 3: Implémenter** — `zerion.py` :

```python
@dataclass(frozen=True)
class TokenMeta:
    fungible_id: str
    symbol: str
    name: str
    verified: bool
    total_supply: float | None
    circulating_supply: float | None
    implementations: dict[str, tuple[str, int]]
    raw: dict


def _meta(item: dict) -> TokenMeta:
    attributes = item["attributes"]
    market = attributes.get("market_data") or {}
    return TokenMeta(
        fungible_id=item["id"],
        symbol=attributes.get("symbol") or "",
        name=attributes.get("name") or "",
        verified=bool((attributes.get("flags") or {}).get("verified")),
        total_supply=_optional_float(market.get("total_supply")),
        circulating_supply=_optional_float(market.get("circulating_supply")),
        implementations={
            i["chain_id"]: (i["address"].lower(), int(i.get("decimals") or 0))
            for i in attributes.get("implementations") or []
            if i.get("address")
        },
        raw=item,
    )
```

et `token_metadata` sur le modèle de `prices()` (même endpoint, mêmes lots) retournant `[_meta(item) for item in data]`.

`raw.py` :

```python
def refresh_token_info(zerion, now, cfg) -> int:
    """Métadonnées Zerion des tokens vus dans l'historique : absents ou plus vieux que le délai."""
    fresh = now - timedelta(days=cfg.token_info_refresh_days)
    known = set(
        TokenInfo.objects.filter(fetched_at__gte=fresh).values_list("chain", "address")
    )
    wanted = sorted(
        {
            pair
            for pair in TokenTrade.objects.exclude(token_address=NATIVE)
            .values_list("chain", "token_address")
            .distinct()
            if pair not in known
        }
    )
    saved = 0
    for start in range(0, len(wanted), MAX_IMPLEMENTATIONS_PER_CALL):
        batch = wanted[start : start + MAX_IMPLEMENTATIONS_PER_CALL]
        try:
            metas = zerion.token_metadata(batch)
        except BudgetExhausted:
            break
        found = {}
        for meta in metas:
            for chain, (address, decimals) in meta.implementations.items():
                found[(chain, address)] = (meta, decimals)
        for chain, address in batch:
            meta, decimals = found.get((chain, address), (None, None))
            TokenInfo.objects.update_or_create(
                chain=chain,
                address=address,
                defaults={
                    "fungible_id": meta.fungible_id if meta else "",
                    "symbol": (meta.symbol if meta else "")[:64],
                    "name": (meta.name if meta else "")[:200],
                    "decimals": decimals,
                    "total_supply": _supply(meta.total_supply) if meta else None,
                    "circulating_supply": _supply(meta.circulating_supply) if meta else None,
                    "verified": meta.verified if meta else False,
                    "raw": meta.raw if meta else {},
                    "fetched_at": now,
                },
            )
            saved += 1
    return saved


def _supply(value):
    return Decimal(str(value)) if value is not None else None
```

(Un token introuvable est enregistré vide avec `fetched_at` : il n'est pas redemandé avant le délai.) `tasks.py` : nouvelle tâche

```python
@shared_task
def refresh_token_info_task() -> int:
    cfg = PipelineSettings.load()
    saved = refresh_token_info(clients.zerion(cfg), timezone.now(), cfg)
    logger.info("%s tokens décrits", saved)
    return saved
```

et `qualify_wallets_task` la déclenche à la fin : `refresh_token_info_task.delay()`.

- [ ] **Step 4: Vérifier** — `make test && make lint` → PASS.

- [ ] **Step 5: Commit** — `feat(wallets): métadonnées Zerion des tokens (supply)`

---

### Task 9: Admin

**Files:**
- Modify: `backend/apps/discovery/admin.py`, `backend/apps/wallets/admin.py`
- Test: `backend/apps/discovery/tests/test_admin.py`

**Interfaces:** Consumes `entity_graph.detach`.

- [ ] **Step 1: Tests**

```python
def test_entity_pages_load(admin_client):
    entity = Entity.objects.create()
    Wallet.objects.create(address="0x" + "1" * 40, entity=entity)
    assert admin_client.get(reverse("admin:discovery_entity_changelist")).status_code == 200
    assert admin_client.get(reverse("admin:discovery_entity_change", args=[entity.pk])).status_code == 200


def test_detach_action(admin_client):
    entity = Entity.objects.create()
    wallet = Wallet.objects.create(address="0x" + "1" * 40, entity=entity)
    admin_client.post(
        reverse("admin:discovery_wallet_changelist"),
        {"action": "detach_from_entity", "_selected_action": [wallet.pk]},
    )
    wallet.refresh_from_db()
    assert wallet.entity_id is None
```

- [ ] **Step 2: Vérifier l'échec** — FAIL.

- [ ] **Step 3: Implémenter** — `discovery/admin.py` :
  - `EntityAdmin` : `list_display = ["__str__", "wallet_count", "created_at", "merged_into"]`, inlines lecture seule `WalletInline` (address) et `EntityEarlyBuyInline` (explosion, held_usd, sold_during_rise_pct, rank) ; `wallet_count` annoté (`Count("wallets")`).
  - `WalletAdmin` : ajouter `entity` à `list_display` et l'action :

```python
    @admin.action(description="Détacher de son entité (le lien ne sera pas recréé)")
    def detach_from_entity(self, request, queryset):
        for wallet in queryset:
            entity_graph.detach(wallet)
```

  - `EarlyBuyerAdmin` : `list_display` + `entity`, `inherited_usd` ; lecture seule.
  - `EntityEarlyBuyAdmin`, `ExcludedBuyerAdmin`, `TokenTransferAdmin` (`list_filter = ["kind"]`, `search_fields = ["sender", "recipient", "tx_hash"]`) : `ReadOnlyAdmin`.
  - `wallets/admin.py` : `WalletLinkAdmin` + `source`, `rejected` ; `TokenInfoAdmin`, `PortfolioSnapshotAdmin` (inline positions) en lecture seule ; `TokenTradeAdmin` + `is_internal` en filtre.

- [ ] **Step 4: Vérifier** — `make test && make lint` → PASS.

- [ ] **Step 5: Commit** — `feat(admin): entités, transferts bruts, exclusions, métadonnées, portefeuilles`

---

### Task 10: Mesures, test live, documentation, relance réelle

**Files:**
- Modify: `backend/integrations/tests/test_live.py`, `README.md`

- [ ] **Step 1: Mesure deposit / withdraw** (script jetable dans le scratchpad, pas de code produit) : pour 20 wallets qualifiés, compter les pages Zerion avec les types actuels puis avec `deposit,withdraw` en plus (dépense ≈ 40 à 80 requêtes). Si l'écart ≤ 20 %, mettre à jour `zerion_operation_types` en base (admin ou shell) ; sinon, reporter les chiffres à l'utilisateur avant de changer.

- [ ] **Step 2: Test live** — dans `test_live.py`, avec le marqueur `live`, vérifier que `ZerionClient.portfolio` renvoie des positions pour `0x44df085447dbebcf69c6675c3b8a795c7fdeb3f4` (skip sans clé) et que `token_metadata([("robinhood", AI_TOKEN)])` renvoie une supply > 0.

- [ ] **Step 3: README** — section « Découverte » : entités (vente / sortie / coffre, héritage du prix de revient), filtre bot, top par entité, `TokenTransfer` ; section « Qualification » : entités, mouvements internes, portefeuille par token, `TokenInfo` ; nouveaux réglages.

- [ ] **Step 4: Vérifier** — `make test` puis `make lint` (séparément) → PASS. Commit `docs: entités et couche brute`.

- [ ] **Step 5: Relance réelle sur AI** — redémarrer worker/beat ; supprimer les `EarlyBuyer` de l'explosion AI (candidat 42), remettre le candidat en `confirmed`, lancer `extract_candidate_buyers` ; relever : nombre d'entités retenues, entités à plusieurs wallets, coffres entrés, bots exclus (dont `0x000461…c111` si candidat), rang de l'entité de `0x44df…`, `TokenTransfer` enregistrés, durée.
