# Explosion v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Détecter le dernier creux avant la montée finale (score multiplicateur × maturité), confirmer sans attente, et extraire les early buyers au fil de l'eau sans plafond bloquant.

**Architecture:** La détection reste une fonction pure sur les bougies (`explosion.py`) : pics locaux → dernier creux → score. La rétention devient une note mesurée plus tard (`pending` → `held` / `rug`). L'extraction consomme HyperSync page par page (`transfer_pages`) dans un `BuyerAggregator` (passe 1 achats jusqu'au creux, passe 2 ventes filtrées sur les acheteurs retenus).

**Tech Stack:** Django 5.2, Postgres 16, Celery 5.5, hypersync 1.2.1, pytest, ruff.

**Spec:** `docs/superpowers/specs/2026-09-27-explosion-v2-design.md`

## Global Constraints

- Aucun paramètre en dur : tout seuil est un champ admin (`DetectionSettings` global + surcharge par chaîne, ou `PipelineSettings`).
- Défauts : `explosion_window_hours` = 168, `maturity_hours` = 336, `breakout_multiplier` = 2, `buyer_window_hours` = 0 (0 = depuis le lancement), `sell_pass_batch_size` = 500, `rug_priority_weight` = 0,2.
- `maturity_hours` = 0 désactive la pondération (facteur 1).
- Règle d'achat inchangée : le signataire (`tx.from`) reçoit les tokens, l'expéditeur n'est ni lui ni l'adresse zéro. Règle de vente inchangée : le signataire envoie à un autre.
- `WAITING_CONFIRMATION` reste dans les choix (historique) ; les candidats encore dans ce statut sont ré-analysés normalement.
- Écart assumé avec la spec : `rug_priority_weight` va dans `PipelineSettings` (la priorité est par wallet, pas par chaîne), pas dans `DetectionSettings`.
- Commandes : `make test`, `make lint`, `make makemigrations`, `make migrate` (Docker Compose). Commits avec `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## File map

- `backend/apps/discovery/models.py` — champs Explosion / DetectionSettings / PipelineSettings.
- `backend/apps/discovery/migrations/0008_explosion_trough.py` (écrit à la main : renommages), `0009_explosion_v2.py` (généré + RunPython).
- `backend/apps/discovery/services/settings.py` — `Thresholds`.
- `backend/apps/discovery/services/explosion.py` — `find_trough`, `find_waves`, `detect_explosion`, `measure_retention`.
- `backend/apps/discovery/services/analysis.py` — `analyze_candidate`, `apply_retention`, `update_retention`.
- `backend/apps/discovery/tasks.py` — pas de rétention dans `analyze_candidates_task`.
- `backend/integrations/hypersync.py` — `transfer_pages` remplace `transfers` ; `TooManyTransfers` supprimé.
- `backend/apps/discovery/services/buyers.py` — `BuyerAggregator` remplace `aggregate_buyers`.
- `backend/apps/discovery/services/extraction.py` — deux passes, garde-fou `partial`.
- `backend/apps/wallets/services/qualification.py` — `compute_priority(wallet, cfg)`.
- Tests : `apps/discovery/tests/{fakes,test_explosion,test_analysis,test_buyers,test_extraction,test_tasks,test_settings_service}.py`, `integrations/tests/{test_hypersync,test_live}.py`, `apps/wallets/tests/{factories,test_tasks}.py`.

---

### Task 1: Modèles, migrations, seuils

**Files:**
- Modify: `backend/apps/discovery/models.py`, `backend/apps/discovery/services/settings.py`, `backend/apps/discovery/admin.py`, `backend/apps/discovery/services/analysis.py`, `backend/apps/discovery/services/extraction.py`, `backend/apps/wallets/tests/factories.py`
- Create: `backend/apps/discovery/migrations/0008_explosion_trough.py`, `0009_explosion_v2.py`
- Test: `backend/apps/discovery/tests/test_settings_service.py`, `test_analysis.py`, `test_explosion.py`

**Interfaces:**
- Produces: `Explosion.trough_block`, `Explosion.trough_at`, `Explosion.score`, `Explosion.peak_price` (float, nullable), `Explosion.retention_pct` (nullable), `Explosion.Retention` (`PENDING="pending"`, `HELD="held"`, `RUG="rug"`), `Explosion.Extraction` (`COMPLETE="complete"`, `PARTIAL="partial"`); `Thresholds.maturity_hours: int`, `Thresholds.breakout_multiplier: float`, `Thresholds.buyer_window_hours: int` (et plus de `confirmation_timeout_hours`) ; `PipelineSettings.sell_pass_batch_size`, `PipelineSettings.rug_priority_weight`.

- [ ] **Step 1: Test des défauts**

Dans `test_settings_service.py`, remplacer l'attendu de `test_migration_creates_global_defaults` :

```python
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
        sniper_blocks=3,
        min_buy_usd=500.0,
        max_buyers=300,
        explosion_window_hours=168,
        maturity_hours=336,
        breakout_multiplier=2.0,
        buyer_window_hours=0,
    )
```

Ajouter :

```python
def test_pipeline_defaults_for_explosion_v2():
    cfg = PipelineSettings.load()
    assert cfg.sell_pass_batch_size == 500
    assert float(cfg.rug_priority_weight) == 0.2
```

(import `PipelineSettings` depuis `apps.discovery.models`).

- [ ] **Step 2: Vérifier l'échec** — `make test args="apps/discovery/tests/test_settings_service.py"` → FAIL (champs inconnus).

- [ ] **Step 3: Modèles**

`THRESHOLD_FIELDS` : retirer `"confirmation_timeout_hours"`, ajouter `"maturity_hours"`, `"breakout_multiplier"`, `"buyer_window_hours"`.

`DetectionSettings` : supprimer `confirmation_timeout_hours` ; remplacer `explosion_window_hours` et ajouter :

```python
    explosion_window_hours = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Le pic doit se trouver dans ces dernières heures (ignoré pour un ajout manuel).",
    )
    maturity_hours = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Âge du token au creux à partir duquel une vague compte pleinement. 0 = pas de pondération.",
    )
    breakout_multiplier = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Une vague précédente retombée coupe le creux si elle a dépassé ce multiple.",
    )
    buyer_window_hours = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Heures d'achat avant le creux. 0 = depuis le lancement.",
    )
```

`PipelineSettings` : ajouter

```python
    sell_pass_batch_size = models.PositiveIntegerField(
        default=500, help_text="Adresses d'acheteurs par requête de la passe des ventes."
    )
    rug_priority_weight = models.DecimalField(
        max_digits=4,
        decimal_places=2,
        default=0.2,
        help_text="Poids d'une explosion « rug » dans la priorité de qualification.",
    )
```

`Explosion` :

```python
class Explosion(models.Model):
    class Retention(models.TextChoices):
        PENDING = "pending", "À mesurer"
        HELD = "held", "Tenue"
        RUG = "rug", "Rug"

    class Extraction(models.TextChoices):
        COMPLETE = "complete", "Complète"
        PARTIAL = "partial", "Partielle"

    candidate = models.OneToOneField(Candidate, on_delete=models.CASCADE, related_name="explosion")
    trough_block = models.PositiveBigIntegerField()
    trough_at = models.DateTimeField()
    peak_block = models.PositiveBigIntegerField()
    peak_at = models.DateTimeField()
    peak_price = models.FloatField(null=True, blank=True)
    multiplier = models.DecimalField(max_digits=12, decimal_places=2)
    score = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    retention_pct = models.DecimalField(max_digits=7, decimal_places=2, null=True, blank=True)
    retention_status = models.CharField(
        max_length=16, choices=Retention.choices, default=Retention.PENDING
    )
    extraction_status = models.CharField(
        max_length=16, choices=Extraction.choices, blank=True, default=""
    )
```

`settings.py` / `Thresholds` : retirer `confirmation_timeout_hours`, ajouter à la fin `maturity_hours: int`, `breakout_multiplier: float`, `buyer_window_hours: int`.

- [ ] **Step 4: Migrations**

`0008_explosion_trough.py` (à la main, pour que Django renomme au lieu de supprimer/recréer) :

```python
from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [("discovery", "0007_pipeline_v2")]

    operations = [
        migrations.RenameField("explosion", "low_block", "trough_block"),
        migrations.RenameField("explosion", "low_at", "trough_at"),
    ]
```

Puis `make makemigrations` → renommer le fichier généré en `0009_explosion_v2.py` et y ajouter :

```python
def set_defaults(apps, schema_editor):
    apps.get_model("discovery", "DetectionSettings").objects.filter(chain=None).update(
        explosion_window_hours=168, maturity_hours=336, breakout_multiplier=2, buyer_window_hours=0
    )
    Explosion = apps.get_model("discovery", "Explosion")
    Explosion.objects.filter(retention_pct__isnull=False).update(retention_status="held")
    Explosion.objects.filter(candidate__status="buyers_extracted").update(
        extraction_status="complete"
    )
```

avec `migrations.RunPython(set_defaults, migrations.RunPython.noop)` en dernière opération.

- [ ] **Step 5: Renommages dans le code existant**

- `analysis.py` : supprimer le bloc `timeout = … confirmation_timeout …` (3 lignes) ; clés `"low_block"` → `"trough_block"`, `"low_at"` → `"trough_at"`.
- `extraction.py` : `low_block=explosion.low_block` → `low_block=explosion.trough_block`.
- `admin.py` : `list_display = ["candidate", "multiplier", "score", "retention_status", "retention_pct", "extraction_status", "trough_at", "peak_at"]`, `list_filter = ["retention_status", "extraction_status"]`.
- `wallets/tests/factories.py` `make_early_buy` : `trough_block=1, trough_at=NOW`, et un paramètre `retention_status="held"` passé à `Explosion.objects.create`.
- `test_analysis.py` : `explosion.low_block` → `explosion.trough_block` ; supprimer `test_waiting_too_long_is_rejected`.
- `test_explosion.py` : retirer `confirmation_timeout_hours=168` de `T`, ajouter `maturity_hours=0, breakout_multiplier=2, buyer_window_hours=0`.

- [ ] **Step 6: Vérifier** — `make migrate && make test && make lint` → PASS.

- [ ] **Step 7: Commit** — `feat(discovery): modèle explosion v2 (creux, score, rétention, extraction)`

---

### Task 2: Détection v2 et rétention non bloquante

**Files:**
- Modify: `backend/apps/discovery/services/explosion.py`, `backend/apps/discovery/services/analysis.py`, `backend/apps/discovery/tasks.py`, `backend/apps/discovery/tests/fakes.py`
- Test: `backend/apps/discovery/tests/test_explosion.py`, `test_analysis.py`, `test_tasks.py`

**Interfaces:**
- Consumes: Task 1 (`Explosion.Retention`, `Thresholds.maturity_hours`, `breakout_multiplier`).
- Produces:
  - `find_trough(closes: list[float], peak_index: int, breakout_multiplier: float) -> int`
  - `Wave(trough: Candle, peak: Candle, multiplier: float, score: float)`
  - `find_waves(candles, *, pool_created_ts: int, since_ts: int | None, thresholds) -> list[Wave]`
  - `detect_explosion(candles, *, now_ts, pool_created_ts, current_liquidity_usd, thresholds, window_hours: int | None) -> Verdict` avec `Verdict(status, reason="", wave=None)` ; `CONFIRMED` / `REJECTED`.
  - `measure_retention(candles, *, peak_ts, peak_price, now_ts, thresholds) -> tuple[float | None, str]` (`PENDING` / `HELD` / `RUG`).
  - `analysis.apply_retention(explosion, candles, thresholds, now) -> str`, `analysis.update_retention(explosion, *, gt, now) -> str`.

- [ ] **Step 1: Tests purs** — réécrire `test_explosion.py` (imports : `CONFIRMED, HELD, PENDING, REJECTED, RUG, choose_resolution, detect_explosion, find_trough, measure_retention`) :

```python
HOUR = 3600
DAY = 24 * HOUR
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
    sniper_blocks=3,
    min_buy_usd=500,
    max_buyers=300,
    explosion_window_hours=1000,
    maturity_hours=0,
    breakout_multiplier=2,
    buyer_window_hours=0,
)


def candles(closes, volume=10_000, step=HOUR):
    return [Candle(i * step, c, c, c, c, volume) for i, c in enumerate(closes)]


def detect(closes, now_ts=100 * HOUR, liquidity=50_000, window_hours=None, step=HOUR,
           volume=10_000, **overrides):
    return detect_explosion(
        candles(closes, volume, step),
        now_ts=now_ts,
        pool_created_ts=0,
        current_liquidity_usd=liquidity,
        thresholds=replace(T, **overrides),
        window_hours=window_hours,
    )


EXPLOSIVE = [1.0, 0.8, 0.5, 2.0, 5.0] + [3.0] * 30
```

Tests (garder `test_choose_resolution` tel quel) :

```python
def test_trough_skips_back_over_small_rebound():
    # 0.2 → rebond à 0.35 (< ×2) → 0.3 → montée : le creux reste 0.2.
    assert find_trough([1.0, 0.2, 0.35, 0.3, 1.0, 2.0], 5, 2) == 1


def test_trough_stops_at_previous_wave():
    # 0.2 → vague à 0.6 (×3) retombée à 0.3 → montée : le creux est 0.3 (cas AI).
    assert find_trough([1.0, 1.0, 0.2, 0.6, 0.4, 0.3, 1.0, 2.0, 3.0], 8, 2) == 5


def test_confirmed_explosion():
    verdict = detect(EXPLOSIVE)
    assert verdict.status == CONFIRMED
    wave = verdict.wave
    assert (wave.trough.ts, wave.peak.ts, wave.multiplier, wave.score) == (
        2 * HOUR, 4 * HOUR, 10.0, 10.0,
    )


def test_last_trough_before_final_rise_wins():
    closes = [1.0, 1.0, 0.2, 0.6, 0.4, 0.3, 1.0, 2.0, 3.0] + [2.5] * 30
    wave = detect(closes).wave
    assert (wave.trough.ts, wave.multiplier) == (5 * HOUR, 10.0)


def test_mature_wave_beats_bigger_launch_wave():
    # ×50 au jour 2, puis ×10 au jour 33 ; maturité 14 jours.
    closes = [1.0, 1.0, 0.1, 5.0] + [2.0] * 29 + [1.0, 10.0] + [8.0] * 3
    verdict = detect(closes, now_ts=40 * DAY, step=DAY, volume=100_000, maturity_hours=336)
    assert (verdict.wave.trough.ts, verdict.wave.multiplier, verdict.wave.score) == (
        33 * DAY, 10.0, 10.0,
    )
    unweighted = detect(closes, now_ts=40 * DAY, step=DAY, volume=100_000)
    assert unweighted.wave.multiplier == 50.0


def test_rejects_small_move():
    assert (detect([1.0, 2.0, 3.0]).status, detect([1.0, 2.0, 3.0]).reason) == (
        REJECTED, "no_explosion",
    )


def test_multiplier_exactly_at_threshold_passes():
    assert detect([1.0, 5.0] + [5.0] * 30).status == CONFIRMED


def test_rejects_low_volume_around_peak():
    assert detect(EXPLOSIVE, volume=100).reason == "low_volume"


def test_rejects_drained_pool():
    assert detect(EXPLOSIVE, liquidity=500).reason == "low_liquidity"


def test_threshold_override_changes_verdict():
    assert detect([1.0, 3.0] + [3.0] * 30, min_multiplier=2).status == CONFIRMED


def test_peak_outside_window_is_rejected():
    closes = [1.0, 0.5, 5.0] + [3.0] * 200
    verdict = detect(closes, now_ts=203 * HOUR, window_hours=72)
    assert (verdict.status, verdict.reason) == (REJECTED, "no_explosion")


def test_manual_candidate_has_no_window():
    closes = [1.0, 0.5, 5.0] + [3.0] * 200
    assert detect(closes, now_ts=203 * HOUR, window_hours=None).wave.multiplier == 10.0


def test_recent_explosion_wins_over_bigger_old_one():
    closes = [1.0, 0.25, 5.0] + [1.0] * 100 + [0.5, 3.0] + [2.0] * 30
    verdict = detect(closes, now_ts=len(closes) * HOUR, window_hours=72)
    assert (verdict.wave.multiplier, verdict.wave.trough.ts) == (6.0, 103 * HOUR)


def test_retention_pending_before_confirmation_delay():
    assert measure_retention(
        candles(EXPLOSIVE), peak_ts=4 * HOUR, peak_price=5.0, now_ts=10 * HOUR, thresholds=T
    ) == (None, PENDING)


def test_retention_held_and_rug():
    held = measure_retention(
        candles(EXPLOSIVE), peak_ts=4 * HOUR, peak_price=5.0, now_ts=100 * HOUR, thresholds=T
    )
    rug = measure_retention(
        candles([1.0, 0.5, 5.0] + [0.6] * 30),
        peak_ts=2 * HOUR, peak_price=5.0, now_ts=100 * HOUR, thresholds=T,
    )
    assert held == (60.0, HELD)
    assert rug == (12.0, RUG)
```

- [ ] **Step 2: Vérifier l'échec** — `make test args="apps/discovery/tests/test_explosion.py"` → FAIL (import).

- [ ] **Step 3: Implémenter `explosion.py`**

```python
"""Détection d'explosion sur des bougies OHLCV. Fonctions pures, sans base ni réseau."""

from dataclasses import dataclass

from apps.discovery.services.settings import Thresholds
from integrations.geckoterminal import MAX_OHLCV_CANDLES, Candle

CONFIRMED = "confirmed"
REJECTED = "rejected"

PENDING = "pending"
HELD = "held"
RUG = "rug"

RESOLUTIONS = (("hour", 1), ("hour", 4), ("hour", 12))


@dataclass(frozen=True)
class Wave:
    trough: Candle
    peak: Candle
    multiplier: float
    score: float


@dataclass(frozen=True)
class Verdict:
    status: str
    reason: str = ""
    wave: Wave | None = None


def choose_resolution(pool_age_hours: float) -> tuple[str, int]:
    (inchangé)


def is_peak(closes: list[float], index: int) -> bool:
    """Début d'un sommet local : monte depuis la bougie précédente, ne remonte pas ensuite."""
    last = index == len(closes) - 1
    return index > 0 and closes[index] > closes[index - 1] and (
        last or closes[index] >= closes[index + 1]
    )


def find_trough(closes: list[float], peak_index: int, breakout_multiplier: float) -> int:
    """Dernier creux avant la montée finale vers le pic.

    En remontant depuis le pic, le creux recule vers chaque clôture plus basse, sauf si le prix
    a dépassé `breakout_multiplier` × cette clôture entre-temps : c'est alors une vague
    précédente, retombée, qui n'appartient pas à la montée finale.
    """
    trough = peak_index
    highest = 0.0
    for index in range(peak_index - 1, -1, -1):
        close = closes[index]
        if close <= 0:
            continue
        if close < closes[trough]:
            if highest > breakout_multiplier * close:
                break
            trough, highest = index, 0.0
        else:
            highest = max(highest, close)
    return trough


def maturity(age_seconds: int, maturity_hours: int) -> float:
    if maturity_hours <= 0:
        return 1.0
    return min(1.0, max(age_seconds, 0) / (maturity_hours * 3600))


def find_waves(
    candles: list[Candle], *, pool_created_ts: int, since_ts: int | None, thresholds: Thresholds
) -> list[Wave]:
    closes = [candle.close for candle in candles]
    waves = []
    for index, peak in enumerate(candles):
        if (since_ts is not None and peak.ts < since_ts) or not is_peak(closes, index):
            continue
        trough = candles[find_trough(closes, index, thresholds.breakout_multiplier)]
        if trough is peak or trough.close <= 0:
            continue
        multiplier = peak.close / trough.close
        weight = maturity(trough.ts - pool_created_ts, thresholds.maturity_hours)
        waves.append(Wave(trough, peak, round(multiplier, 2), round(multiplier * weight, 2)))
    return waves


def detect_explosion(
    candles: list[Candle],
    *,
    now_ts: int,
    pool_created_ts: int,
    current_liquidity_usd: float,
    thresholds: Thresholds,
    window_hours: int | None,
) -> Verdict:
    since_ts = None if window_hours is None else now_ts - window_hours * 3600
    waves = [
        wave
        for wave in find_waves(
            candles, pool_created_ts=pool_created_ts, since_ts=since_ts, thresholds=thresholds
        )
        if wave.multiplier >= thresholds.min_multiplier
    ]
    if not waves:
        return Verdict(REJECTED, "no_explosion")

    half_window = thresholds.peak_volume_window_hours * 3600 / 2
    waves = [
        wave
        for wave in waves
        if sum(c.volume for c in candles if abs(c.ts - wave.peak.ts) <= half_window)
        >= thresholds.min_volume_usd
    ]
    if not waves:
        return Verdict(REJECTED, "low_volume")
    if current_liquidity_usd < thresholds.min_liquidity_usd:
        return Verdict(REJECTED, "low_liquidity")
    return Verdict(CONFIRMED, wave=max(waves, key=lambda wave: (wave.score, wave.peak.ts)))


def measure_retention(
    candles: list[Candle], *, peak_ts: int, peak_price: float, now_ts: int, thresholds: Thresholds
) -> tuple[float | None, str]:
    """Clôture à pic + `confirmation_hours` rapportée au pic ; `pending` avant ce délai."""
    confirm_ts = peak_ts + thresholds.confirmation_hours * 3600
    if now_ts < confirm_ts or not candles or peak_price <= 0:
        return None, PENDING
    later = [candle for candle in candles if candle.ts >= confirm_ts]
    reference = later[0] if later else candles[-1]
    retention = round(reference.close / peak_price * 100, 2)
    return retention, HELD if retention >= thresholds.min_retention_pct else RUG
```

- [ ] **Step 4: Vérifier** — `make test args="apps/discovery/tests/test_explosion.py"` → PASS.

- [ ] **Step 5: Tests d'analyse** — dans `fakes.py`, `explosive_candles(start_ts, after_peak=3.0)` (remplace le `[3.0] * 30` par `[after_peak] * 30`). Dans `test_analysis.py` : remplacer `test_waits_when_peak_is_recent` par :

```python
def test_recent_peak_is_confirmed_with_pending_retention(candidate):
    recent = POOL_CREATED + timedelta(hours=30)
    assert analyze(candidate, now=recent) == Candidate.Status.CONFIRMED
    explosion = Explosion.objects.get()
    assert explosion.retention_status == Explosion.Retention.PENDING
    assert explosion.retention_pct is None
    assert update_retention(explosion, gt=FakeGeckoTerminal(), now=NOW) == "held"
    explosion.refresh_from_db()
    assert float(explosion.retention_pct) == 60.0


def test_rug_is_confirmed_and_flagged(candidate):
    rug = FakeGeckoTerminal(candles=explosive_candles(int(POOL_CREATED.timestamp()), 0.6))
    assert analyze(candidate, gt=rug) == Candidate.Status.CONFIRMED
    assert Explosion.objects.get().retention_status == Explosion.Retention.RUG


def test_peak_outside_window_is_rejected_unless_manual(candidate):
    later = NOW + timedelta(days=30)
    assert analyze(candidate, now=later) == Candidate.Status.REJECTED
    manual = make_candidate(candidate.token, sources=["manual"])
    assert analyze(manual, now=later) == Candidate.Status.CONFIRMED
```

et dans `test_confirms_explosion_and_stores_blocks` ajouter `assert float(explosion.score) == 0.3` (×10 × 10 h / 336 h), `assert explosion.peak_price == 5.0`, `assert explosion.retention_status == Explosion.Retention.HELD`. Import `update_retention` depuis `apps.discovery.services.analysis`.

Dans `test_tasks.py`, ajouter :

```python
def test_analyze_task_measures_pending_retention(fake_clients):
    tasks.sync_chains_task.apply()
    tasks.collect_candidates_task.apply()
    tasks.analyze_candidates_task.apply()
    Explosion.objects.update(retention_status="pending", retention_pct=None)
    counts = tasks.analyze_candidates_task.apply().get()
    assert counts["retention_held"] == 1
    assert Explosion.objects.get().retention_status == "held"
```

- [ ] **Step 6: Vérifier l'échec** — `make test args="apps/discovery/tests"` → FAIL.

- [ ] **Step 7: Implémenter l'analyse**

`analysis.py` : imports `from apps.discovery.services.candidates import SOURCE_MANUAL, upsert_token_and_pools` et `from apps.discovery.services.explosion import REJECTED, choose_resolution, detect_explosion, measure_retention`. Ajouter :

```python
def apply_retention(explosion: Explosion, candles, thresholds, now: datetime) -> str:
    retention, status = measure_retention(
        candles,
        peak_ts=int(explosion.peak_at.timestamp()),
        peak_price=explosion.peak_price or 0.0,
        now_ts=int(now.timestamp()),
        thresholds=thresholds,
    )
    explosion.retention_pct = retention
    explosion.retention_status = status
    explosion.save(update_fields=["retention_pct", "retention_status"])
    return status


def update_retention(explosion: Explosion, *, gt, now: datetime) -> str:
    token = explosion.candidate.token
    thresholds = thresholds_for(token.chain)
    if now < explosion.peak_at + timedelta(hours=thresholds.confirmation_hours):
        return explosion.retention_status
    history = fetch_price_history(gt, token.chain, token, now)
    return apply_retention(explosion, history.candles, thresholds, now)
```

`analyze_candidate` après `fetch_price_history` :

```python
    created_at = history.main.created_at
    if created_at is not None:
        pool_created_ts = int(created_at.timestamp())
    else:
        pool_created_ts = history.candles[0].ts if history.candles else 0
    verdict = detect_explosion(
        history.candles,
        now_ts=int(now.timestamp()),
        pool_created_ts=pool_created_ts,
        current_liquidity_usd=sum(pool.liquidity_usd for pool in history.pools),
        thresholds=thresholds,
        window_hours=None
        if SOURCE_MANUAL in candidate.sources
        else thresholds.explosion_window_hours,
    )
    if verdict.status == REJECTED:
        return reject(candidate, verdict.reason)

    wave = verdict.wave
    (contrôle already_extracted avec wave.peak.ts)
    (supprimer le bloc WAITING)
    trough_block = find_block_at(wave.trough.ts, 0, height, hypersync.block_timestamp)
    peak_block = find_block_at(wave.peak.ts, trough_block, height, hypersync.block_timestamp)
    (created_block des pools : borne haute trough_block)
    explosion, _ = Explosion.objects.update_or_create(
        candidate=candidate,
        defaults={
            "trough_block": trough_block,
            "trough_at": _datetime(wave.trough.ts),
            "peak_block": peak_block,
            "peak_at": _datetime(wave.peak.ts),
            "peak_price": wave.peak.close,
            "multiplier": wave.multiplier,
            "score": wave.score,
            "retention_pct": None,
            "retention_status": Explosion.Retention.PENDING,
        },
    )
    apply_retention(explosion, history.candles, thresholds, now)
```

`tasks.py` / `analyze_candidates_task` : `due = Candidate.objects.filter(status__in=[Candidate.Status.CANDIDATE, Candidate.Status.WAITING_CONFIRMATION]).select_related("token__chain")` puis, après la boucle :

```python
    pending = Explosion.objects.filter(
        retention_status=Explosion.Retention.PENDING
    ).select_related("candidate__token__chain")
    for explosion in pending:
        try:
            counts[f"retention_{update_retention(explosion, gt=gt, now=now)}"] += 1
        except Exception:
            logger.exception("Échec de la mesure de rétention de l'explosion %s", explosion.pk)
```

- [ ] **Step 8: Vérifier** — `make test && make lint` → PASS.

- [ ] **Step 9: Commit** — `feat(discovery): détection du dernier creux, score de maturité, rétention non bloquante`

---

### Task 3: HyperSync `transfer_pages`

**Files:**
- Modify: `backend/integrations/hypersync.py`, `backend/integrations/tests/test_hypersync.py`, `backend/integrations/tests/test_live.py`

**Interfaces:**
- Produces: `HyperSyncClient.transfer_pages(token: str, from_block: int, to_block: int, senders: list[str] | None = None) -> Iterator[list[Transfer]]` — une liste par page HyperSync, `[from_block, to_block)`, filtre topic1 si `senders`.

- [ ] **Step 1: Tests** — dans `test_hypersync.py`, `FakeInner.get` enregistre aussi `self.queries.append(query)` (`self.queries = []` dans `__init__`). Remplacer les trois tests `transfers` :

```python
def collect(client, *args, **kwargs):
    return [list(page) for page in client.transfer_pages(*args, **kwargs)]


def test_transfer_pages_follow_pagination_and_join_tx_sender():
    (mêmes pages que l'ancien test)
    pages = collect(make_client(inner), "0xtoken", 50, 200)
    assert pages == [
        [Transfer(block=100, timestamp=100, tx_from=ALICE, sender=POOL, recipient=ALICE, amount=1000)],
        [Transfer(block=160, timestamp=200, tx_from=ALICE, sender=ALICE, recipient=BOB, amount=400)],
    ]
    assert inner.from_blocks == [50, 150]
    assert inner.queries[0].logs[0].topics == [[TRANSFER_TOPIC]]


def test_transfer_pages_filter_on_senders():
    inner = FakeInner([page(200)])
    collect(make_client(inner), "0xtoken", 0, 200, senders=[ALICE, BOB])
    assert inner.queries[0].logs[0].topics == [[TRANSFER_TOPIC], [topic(ALICE), topic(BOB)]]


def test_transfer_pages_empty_range_makes_no_call():
    inner = FakeInner([])
    assert collect(make_client(inner), "0xtoken", 200, 200) == []
    assert inner.from_blocks == []


def test_transfer_pages_accept_real_topics_padding():
    (même page que l'ancien test)
    [[transfer]] = collect(make_client(inner), "0xtoken", 0, 200)
    assert transfer.amount == 7
```

Retirer l'import `TooManyTransfers` et `pytest` s'il n'est plus utilisé. `test_live.py` :

```python
    pages = client.transfer_pages(usdc, height - 20, height - 10)
    transfers = [transfer for page in pages for transfer in page]
```

- [ ] **Step 2: Vérifier l'échec** — `make test args="integrations/tests/test_hypersync.py"` → FAIL.

- [ ] **Step 3: Implémenter** — remplacer `transfers()` par :

```python
    def transfer_pages(
        self, token: str, from_block: int, to_block: int, senders: list[str] | None = None
    ) -> Iterator[list[Transfer]]:
        """Transferts ERC-20 du token page par page ; `senders` filtre l'expéditeur (topic1)."""
        if from_block >= to_block:
            return
        topics = [[TRANSFER_TOPIC]]
        if senders:
            topics.append([address_topic(sender) for sender in senders])
        query = Query(
            from_block=from_block,
            to_block=to_block,
            logs=[LogSelection(address=[token], topics=topics)],
            field_selection=(la même FieldSelection qu'avant),
        )
        for data in self._pages(query, to_block):
            yield self._token_transfers(data)

    @staticmethod
    def _token_transfers(data) -> list[Transfer]:
        (corps de l'ancienne boucle sur data.logs, retourne la liste de la page)
```

`from collections.abc import Iterator` ; import `from integrations.errors import UpstreamError` seul ; supprimer `TooManyTransfers` de `errors.py` une fois l'extraction migrée (Task 4).

- [ ] **Step 4: Vérifier** — `make test args="integrations"` → PASS (l'extraction, qui appelle encore `transfers`, est couverte par ses propres tests avec le faux client : à migrer en Task 4 ; lancer uniquement `integrations` ici).

- [ ] **Step 5: Commit** — `feat(hypersync): transferts page par page avec filtre expéditeur`

---

### Task 4: Extraction au fil de l'eau

**Files:**
- Modify: `backend/apps/discovery/services/buyers.py`, `backend/apps/discovery/services/extraction.py`, `backend/apps/discovery/tests/fakes.py`, `backend/integrations/errors.py`
- Test: `backend/apps/discovery/tests/test_buyers.py`, `test_extraction.py`

**Interfaces:**
- Consumes: `transfer_pages` (Task 3), `Explosion.trough_block/trough_at/extraction_status`, `Thresholds.buyer_window_hours`, `PipelineSettings.sell_pass_batch_size` (Task 1).
- Produces: `BuyerAggregator(candles, decimals)` avec `.add(transfers)`, `.add_sells(transfers)`, `.select(*, pool_created_block, sniper_blocks, min_buy_usd, max_buyers) -> list[BuyerStats]` ; `buy_window_start(explosion, first_block, thresholds, hypersync) -> int`.

- [ ] **Step 1: Tests de l'agrégateur** — dans `test_buyers.py`, remplacer `run` :

```python
def run(transfers, sells=(), **overrides):
    params = dict(pool_created_block=100, sniper_blocks=3, min_buy_usd=500, max_buyers=300)
    params.update(overrides)
    aggregator = BuyerAggregator(candles=CANDLES, decimals=18)
    for transfer in transfers:
        aggregator.add([transfer])  # une page par transfert
    kept = aggregator.select(**params)
    aggregator.add_sells(list(sells))
    return kept
```

Remplacer `test_buys_after_low_block_are_not_early` et `test_tokens_sent_before_peak_count_as_sold` par :

```python
def test_buys_accumulate_across_pages():
    [alice] = run([buy(ALICE, 200, 300), buy(ALICE, 300, 300)])
    assert (alice.first_buy_block, alice.bought_amount) == (200, 600 * UNIT)


def sell(wallet, block, tokens):
    return Transfer(block=block, timestamp=block, tx_from=wallet, sender=wallet, recipient=POOL,
                    amount=tokens * UNIT)


def test_sells_before_trough_count():
    [alice] = run([buy(ALICE, 200, 600), sell(ALICE, 250, 100)])
    assert alice.sold_amount == 100 * UNIT


def test_second_pass_sells_update_kept_buyers_only():
    [alice] = run([buy(ALICE, 200, 600)], sells=[sell(ALICE, 1500, 200), sell(FRIEND, 1500, 50)])
    assert alice.sold_amount == 200 * UNIT
```

Import `BuyerAggregator, ZERO_ADDRESS` depuis `apps.discovery.services.buyers`.

- [ ] **Step 2: Vérifier l'échec** — `make test args="apps/discovery/tests/test_buyers.py"` → FAIL.

- [ ] **Step 3: Implémenter `buyers.py`** (docstring de module conservée, `BuyerStats` inchangé) :

```python
class BuyerAggregator:
    """Totaux par acheteur alimentés page par page : la mémoire suit le nombre d'acheteurs."""

    def __init__(self, *, candles: list[Candle], decimals: int):
        self._candles = candles
        self._times = [candle.ts for candle in candles]
        self._scale = 10**decimals
        self.stats: dict[str, BuyerStats] = {}

    def _price_at(self, ts: int) -> float:
        index = bisect_right(self._times, ts) - 1
        return self._candles[max(index, 0)].close

    def add(self, transfers: list[Transfer]) -> None:
        """Passe 1 (fenêtre d'achat → creux) : achats et ventes."""
        for transfer in sorted(transfers, key=lambda t: t.block):
            signer = transfer.tx_from
            if transfer.recipient == signer and transfer.sender not in (signer, ZERO_ADDRESS):
                buyer = self.stats.get(signer)
                if buyer is None:
                    buyer = self.stats[signer] = BuyerStats(
                        signer, transfer.block, transfer.timestamp
                    )
                buyer.bought_amount += transfer.amount
                buyer.bought_usd += transfer.amount / self._scale * self._price_at(
                    transfer.timestamp
                )
            else:
                self._count_sell(transfer)

    def add_sells(self, transfers: list[Transfer]) -> None:
        """Passe 2 (creux → pic) : ventes des acheteurs connus."""
        for transfer in transfers:
            self._count_sell(transfer)

    def _count_sell(self, transfer: Transfer) -> None:
        signer = transfer.tx_from
        if transfer.sender == signer and transfer.recipient != signer and signer in self.stats:
            self.stats[signer].sold_amount += transfer.amount

    def select(
        self, *, pool_created_block: int, sniper_blocks: int, min_buy_usd: float, max_buyers: int
    ) -> list[BuyerStats]:
        kept = [buyer for buyer in self.stats.values() if buyer.bought_usd >= min_buy_usd]
        for buyer in kept:
            buyer.bought_usd = round(buyer.bought_usd, 2)
            buyer.is_sniper = buyer.first_buy_block - pool_created_block <= sniper_blocks
        kept.sort(key=lambda buyer: buyer.bought_usd, reverse=True)
        return kept[:max_buyers] if max_buyers > 0 else kept
```

Supprimer `aggregate_buyers`.

- [ ] **Step 4: Vérifier** — `make test args="apps/discovery/tests/test_buyers.py"` → PASS.

- [ ] **Step 5: Faux client et tests d'extraction** — dans `fakes.py`, `FakeHyperSync.__init__(self, transfers=None, error=None, page_size=2)` et remplacer `transfers()` par :

```python
    def transfer_pages(self, token, from_block, to_block, senders=None):
        self.transfer_calls.append((token, from_block, to_block, senders))
        if self._error:
            raise self._error
        selected = [
            t
            for t in self._all_transfers()
            if from_block <= t.block < to_block and (senders is None or t.sender in senders)
        ]
        for start in range(0, len(selected), self.page_size):
            yield selected[start : start + self.page_size]
```

(`_all_transfers()` = l'ancien corps qui construit la liste par défaut, ou `self._transfers`.) Comme c'est un générateur, `error` est levée à la première itération.

Dans `test_extraction.py` : supprimer l'import `TooManyTransfers` et remplacer `test_queries_transfers_from_pool_creation_to_peak` / `test_too_many_transfers_rejects` par :

```python
def test_two_passes_buys_to_trough_then_sells_of_kept_buyers(confirmed):
    hypersync = FakeHyperSync()
    extract(confirmed, hypersync)
    explosion = confirmed.explosion
    assert hypersync.transfer_calls == [
        (TOKEN, 500, explosion.trough_block + 1, None),
        (TOKEN, explosion.trough_block + 1, explosion.peak_block + 1, sorted([ALICE, SNIPER])),
    ]
    explosion.refresh_from_db()
    assert explosion.extraction_status == Explosion.Extraction.COMPLETE


def test_sell_pass_is_batched(confirmed):
    cfg = PipelineSettings.load()
    cfg.sell_pass_batch_size = 1
    cfg.save()
    hypersync = FakeHyperSync()
    extract(confirmed, hypersync)
    assert [call[3] for call in hypersync.transfer_calls[1:]] == [[ALICE], [SNIPER]]


def test_transfer_guard_keeps_partial_buyers(confirmed):
    cfg = PipelineSettings.load()
    cfg.max_transfers_per_token = 2
    cfg.save()
    assert extract(confirmed) == 2
    confirmed.explosion.refresh_from_db()
    assert confirmed.explosion.extraction_status == Explosion.Extraction.PARTIAL


def test_buyer_window_limits_buy_pass(confirmed):
    DetectionSettings.objects.filter(chain=None).update(buyer_window_hours=9)
    hypersync = FakeHyperSync()
    assert extract(confirmed, hypersync) == 0
    start = int(POOL_CREATED.timestamp()) + HOUR
    assert hypersync.transfer_calls[0][1] == block_of(start)
```

(imports : `DetectionSettings, Explosion` ; `HOUR, POOL_CREATED, block_of`.) Le faux client pagine par 2 : la 1re page contient les achats du sniper (bloc 501) et d'Alice (bloc 600), d'où 2 acheteurs avec le garde-fou à 2.

- [ ] **Step 6: Vérifier l'échec** — `make test args="apps/discovery/tests/test_extraction.py"` → FAIL.

- [ ] **Step 7: Implémenter `extraction.py`**

```python
"""Extraction des early buyers d'une explosion, page par page (mémoire ∝ nombre d'acheteurs)."""

from datetime import UTC, datetime
from decimal import Decimal

from django.db import transaction

from apps.discovery.models import Candidate, EarlyBuyer, Explosion, PipelineSettings, Wallet
from apps.discovery.services.analysis import fetch_price_history, reject
from apps.discovery.services.blocks import find_block_at
from apps.discovery.services.buyers import BuyerAggregator
from apps.discovery.services.settings import thresholds_for

BATCH_SIZE = 1000


def buy_window_start(explosion: Explosion, first_block: int, thresholds, hypersync) -> int:
    """Début de la fenêtre d'achat : `buyer_window_hours` avant le creux, jamais avant le pool."""
    if thresholds.buyer_window_hours <= 0:
        return first_block
    start_ts = int(explosion.trough_at.timestamp()) - thresholds.buyer_window_hours * 3600
    return find_block_at(start_ts, first_block, explosion.trough_block, hypersync.block_timestamp)


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

    history = fetch_price_history(gt, chain, token, now)
    aggregator = BuyerAggregator(candles=history.candles, decimals=token.decimals)
    status = Explosion.Extraction.COMPLETE
    seen = 0
    start = buy_window_start(explosion, first_block, thresholds, hypersync)
    for page in hypersync.transfer_pages(token.address, start, explosion.trough_block + 1):
        aggregator.add(page)
        seen += len(page)
        if seen >= cfg.max_transfers_per_token:
            status = Explosion.Extraction.PARTIAL
            break

    buyers = aggregator.select(
        pool_created_block=first_block,
        sniper_blocks=thresholds.sniper_blocks,
        min_buy_usd=thresholds.min_buy_usd,
        max_buyers=thresholds.max_buyers,
    )
    wallets = sorted(buyer.wallet for buyer in buyers)
    batch = max(cfg.sell_pass_batch_size, 1)
    for offset in range(0, len(wallets), batch):
        for page in hypersync.transfer_pages(
            token.address,
            explosion.trough_block + 1,
            explosion.peak_block + 1,
            senders=wallets[offset : offset + batch],
        ):
            aggregator.add_sells(page)

    with transaction.atomic():
        (création Wallet / EarlyBuyer inchangée)
        explosion.extraction_status = status
        explosion.save(update_fields=["extraction_status"])
        candidate.status = Candidate.Status.BUYERS_EXTRACTED
        candidate.save(update_fields=["status", "updated_at"])
    return len(buyers)
```

Supprimer `TooManyTransfers` de `integrations/errors.py`.

- [ ] **Step 8: Vérifier** — `make test && make lint` → PASS.

- [ ] **Step 9: Commit** — `feat(discovery): extraction en deux passes au fil de l'eau, garde-fou partiel`

---

### Task 5: Priorité pondérée par la rétention

**Files:**
- Modify: `backend/apps/wallets/services/qualification.py`
- Test: `backend/apps/wallets/tests/test_tasks.py`

**Interfaces:**
- Consumes: `PipelineSettings.rug_priority_weight`, `Explosion.retention_status`, `make_early_buy(..., retention_status=...)` (Task 1).
- Produces: `compute_priority(wallet, cfg) -> float`.

- [ ] **Step 1: Test**

```python
def test_priority_weights_rug_explosions_down(chain):
    held = Wallet.objects.create(address="0x" + "1" * 40)
    rugged = Wallet.objects.create(address="0x" + "2" * 40)
    make_early_buy(held, chain, TOKEN_A)
    make_early_buy(rugged, chain, TOKEN_B, retention_status="rug")
    cfg = PipelineSettings.load()
    assert compute_priority(held, cfg) > compute_priority(rugged, cfg) > 0
```

et `test_priority_prefers_more_explosions` passe `PipelineSettings.load()` en second argument.

- [ ] **Step 2: Vérifier l'échec** — `make test args="apps/wallets/tests/test_tasks.py"` → FAIL.

- [ ] **Step 3: Implémenter**

```python
def compute_priority(wallet, cfg) -> float:
    """Explosions captées (poids fort, un rug compte `rug_priority_weight`), puis montant acheté."""
    rug = Q(explosion__retention_status=Explosion.Retention.RUG)
    stats = EarlyBuyer.objects.filter(wallet=wallet).aggregate(
        kept=Count("id", filter=~rug), rugs=Count("id", filter=rug), usd=Sum("bought_usd")
    )
    explosions = stats["kept"] + float(cfg.rug_priority_weight) * stats["rugs"]
    return explosions * 1_000_000 + min(float(stats["usd"] or 0), 999_999.0)
```

Les deux appels dans `enqueue_profiles` deviennent `compute_priority(profile.wallet, cfg)`. Importer `Explosion` et `Q` s'ils manquent.

- [ ] **Step 4: Vérifier** — `make test && make lint` → PASS.

- [ ] **Step 5: Commit** — `feat(wallets): priorité réduite pour les acheteurs d'explosions rug`

---

### Task 6: Test live AI, documentation, relance

**Files:**
- Modify: `backend/integrations/tests/test_live.py`, `README.md`

- [ ] **Step 1: Test live** (marqueur `live`, lancé avec `make test args="-m live integrations"`) :

```python
AI_TOKEN = "0x2e8c31162b855a2ffa90f6f8634643ad6f111e18"


def test_ai_token_trough_is_mid_august():
    client = geckoterminal.GeckoTerminalClient(http(geckoterminal.BASE_URL))
    pool = max(client.token_pools(ROBINHOOD_GT_ID, AI_TOKEN), key=lambda p: p.liquidity_usd)
    candles = client.ohlcv(ROBINHOOD_GT_ID, pool.address, "hour", 4)
    verdict = detect_explosion(
        candles,
        now_ts=candles[-1].ts,
        pool_created_ts=int(pool.created_at.timestamp()),
        current_liquidity_usd=pool.liquidity_usd,
        thresholds=LIVE_THRESHOLDS,
        window_hours=None,
    )
    trough = datetime.fromtimestamp(verdict.wave.trough.ts, tz=UTC)
    assert datetime(2026, 8, 15, tzinfo=UTC) <= trough <= datetime(2026, 8, 20, tzinfo=UTC)
```

`ROBINHOOD_GT_ID` = `gt_id` de la chaîne Robinhood en base (à relever avant d'écrire le test), `LIVE_THRESHOLDS` = les défauts globaux de la Task 1.

- [ ] **Step 2: Lancer** — `make test args="-m live integrations -k ai_token"` → PASS (sinon, analyser le creux obtenu avant de toucher au code).

- [ ] **Step 3: README** — section « Découverte multi-chaînes » : décrire le dernier creux, le score de maturité, l'absence d'attente (rétention en note), l'extraction en deux passes et le garde-fou `partial`, et les nouveaux réglages.

- [ ] **Step 4: Vérifier** — `make test && make lint` → PASS.

- [ ] **Step 5: Commit** — `docs(discovery): explosion v2 et test live du token AI`

- [ ] **Step 6: Relance réelle** — redémarrer worker/beat (`docker compose restart worker beat`), ajouter le token AI en candidat manuel (admin), lancer l'analyse puis l'extraction ; relever creux, score, rétention, nombre d'acheteurs et statut d'extraction.
