# WIT V1 — Migration Django + Celery + Redis + Docker

**Date:** 2026-05-31
**Author:** Claude + Bilal
**Status:** Design validé, prêt pour implémentation

---

## Contexte

WIT V1 est actuellement un ensemble de scripts Python CLI qui :
- Collectent des données via APIs externes (Zerion, Dune, DexScreener)
- Stockent dans SQLite
- Exécutent des pipelines interconnectés (discovery → tracking → FIFO → scoring → consensus)
- Utilisent un scheduler Python basique (`schedule` library)
- Frontend Next.js avec données mockées

**Problème actuel :**
- Pas d'API REST pour le frontend
- SQLite limite la concurrence en production
- Scheduler basique, pas de gestion d'erreurs robuste
- Code non containerisé, difficile à déployer

**Objectif :** Migrer vers une architecture Django professionnelle avec API REST, PostgreSQL, Celery, Redis, et Docker.

---

## 1. Architecture Globale

### Stack Technique
- **Backend:** Django 5.0 + Django REST Framework
- **Database:** PostgreSQL 16
- **Task Queue:** Celery 5.x + Redis (broker + result backend)
- **Web Server:** Gunicorn (production) / Django dev server (dev)
- **Containerization:** Docker + Docker Compose

### Services Docker (5 conteneurs)

```
┌─────────────────┐
│   nginx:80      │ ← Reverse proxy (optionnel pour production)
└────────┬────────┘
         │
┌────────▼────────┐
│   backend       │ ← Django + Gunicorn (API REST)
│   (Gunicorn)    │
└────────┬────────┘
         │
    ┌────┴─────┬──────────┬──────────┐
    │          │          │          │
┌───▼───┐ ┌───▼───┐ ┌────▼────┐ ┌───▼────┐
│ postgres│ │ redis │ │ worker  │ │  beat  │
│  :5432  │ │ :6379 │ │ (Celery)│ │(Celery)│
└─────────┘ └───────┘ └─────────┘ └────────┘
```

**Rôle de chaque service :**
1. **backend:** Expose l'API REST Django pour le frontend Next.js
2. **worker:** Exécute les tâches lourdes (pipelines, FIFO, scoring)
3. **beat:** Scheduler Celery (remplace `schedule.py` actuel)
4. **redis:** Broker de messages Celery + cache Django (optionnel)
5. **postgres:** Base de données principale

### Flux de données

```
Frontend (Next.js)
    ↓ HTTP REST
Django API (lecture: wallets, consensus, analytics)
    ↓ ORM
PostgreSQL
    ↑ écriture
Celery Workers (pipelines: discovery, scoring, tracking)
    ↑ déclenchement
Celery Beat (scheduler automatique)
```

---

## 2. Structure du Projet Django

### Organisation des fichiers

```
WIT_V1_perso/
├── backend/                         # Nouveau répertoire Django
│   ├── manage.py
│   ├── config/                     # Settings projet
│   │   ├── settings/
│   │   │   ├── base.py            # Config commune
│   │   │   ├── development.py     # Dev local
│   │   │   └── production.py      # Production Docker
│   │   ├── urls.py
│   │   ├── celery.py              # Config Celery
│   │   └── wsgi.py
│   │
│   ├── api/                        # API Router central
│   │   ├── urls.py                # Route vers les APIs de chaque app
│   │   └── permissions.py         # Permissions globales
│   │
│   ├── wallets/                    # 📱 App principale : Tout ce qui concerne les wallets
│   │   ├── models/
│   │   │   ├── base.py            # Wallet, Token, Transaction
│   │   │   ├── discovery.py       # WalletBrute, DiscoverySession
│   │   │   ├── analytics.py       # TokenAnalytics, WalletScore, TierPerformance
│   │   │   ├── consensus.py       # ConsensusSignal
│   │   │   └── relationships.py   # 🆕 FUTUR : WalletRelationship, WalletCluster
│   │   │
│   │   ├── services/              # Business logic organisée
│   │   │   ├── discovery/
│   │   │   │   ├── dune_client.py
│   │   │   │   └── explosion_detector.py
│   │   │   ├── tracking/
│   │   │   │   ├── zerion_client.py
│   │   │   │   └── balance_tracker.py
│   │   │   ├── analytics/
│   │   │   │   ├── fifo_calculator.py
│   │   │   │   ├── wallet_scorer.py
│   │   │   │   └── tier_analyzer.py
│   │   │   ├── consensus/
│   │   │   │   └── consensus_detector.py
│   │   │   └── relationships/     # 🆕 FUTUR : Wallet graph analysis
│   │   │       ├── graph_analyzer.py
│   │   │       └── child_wallet_detector.py
│   │   │
│   │   ├── serializers/           # Serializers DRF
│   │   │   ├── wallet.py
│   │   │   ├── analytics.py
│   │   │   ├── consensus.py
│   │   │   └── relationships.py   # 🆕 FUTUR
│   │   │
│   │   ├── views/                 # API Views
│   │   │   ├── wallets.py
│   │   │   ├── analytics.py
│   │   │   ├── consensus.py
│   │   │   └── relationships.py   # 🆕 FUTUR
│   │   │
│   │   ├── tasks.py               # Toutes les Celery tasks
│   │   ├── urls.py                # Routes API
│   │   ├── management/commands/   # CLI commands
│   │   │   ├── run_discovery.py
│   │   │   ├── run_tracking.py
│   │   │   ├── run_scoring.py
│   │   │   └── run_consensus.py
│   │   └── admin.py
│   │
│   ├── notifications/              # 📱 App séparée : Outputs (Telegram, webhooks...)
│   │   ├── models.py              # NotificationLog
│   │   ├── services/
│   │   │   └── telegram_bot.py
│   │   ├── tasks.py
│   │   └── admin.py
│   │
│   ├── common/                    # Utils partagés (pas une app installée)
│   │   ├── api_clients/           # Clients API externes partagés
│   │   │   ├── base.py
│   │   │   ├── zerion.py
│   │   │   ├── dune.py
│   │   │   └── dexscreener.py
│   │   ├── utils.py
│   │   ├── constants.py
│   │   └── exceptions.py
│   │
│   └── requirements/
│       ├── base.txt
│       ├── development.txt
│       └── production.txt
│
├── frontend/                        # Next.js (renommé de Front/)
│   └── ...
│
├── docker/
│   ├── backend/
│   │   └── Dockerfile
│   ├── worker/
│   │   └── Dockerfile
│   ├── nginx/
│   │   └── nginx.conf
│   └── postgres/
│
├── docker-compose.yml
├── docker-compose.prod.yml
├── .env.example
│
└── smart_wallet_analysis/           # ANCIEN CODE (à conserver temporairement)
```

### Apps Django (séparation par domaine métier)

**INSTALLED_APPS :**
```python
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    ...
    'rest_framework',

    # WIT Apps
    'wallets',          # Tout ce qui concerne les wallets (discovery, tracking, scoring, consensus, relationships...)
    'notifications',    # Outputs (Telegram, webhooks...)
]
```

**Principe d'organisation :**
- **`wallets/`** = app principale (tout ce qui analyse les wallets : discovery, tracking, scoring, consensus, relationships futures...)
- **`notifications/`** = app séparée (outputs indépendants : Telegram, email, webhooks...)

**Pourquoi cette structure ?**
- ✅ Simple : 2 apps Django seulement (pas de sur-découpage)
- ✅ Logique métier groupée : tout ce qui analyse les wallets est dans `wallets/`
- ✅ Scalable : nouvelle analyse de wallets = ajouter dans `wallets/services/`, nouveau domaine métier = nouvelle app Django
- ✅ Standard Django : utilise le système d'apps natif

**Règle de séparation :**
- Nouvelle **analyse de wallets** (ex: relationships, patterns, ML predictions) → ajouter dans `wallets/`
- Nouveau **domaine métier indépendant** (ex: trading automatique, backtesting avec interface, gestion users) → créer nouvelle app Django

---

## 3. Modèles Django & Migration PostgreSQL

### Stratégie de migration des données

**Tables SQLite actuelles (10 tables) :**
- `wallets`, `tokens`, `transaction_history`, `wallet_position_changes`
- `token_analytics`, `wallet_tier_performance`, `wallet_qualified`, `smart_wallets`
- `wallet_brute`, `consensus_signal` (si existe)

**Étape 1 : Générer modèles Django depuis SQLite**
```bash
python manage.py inspectdb > wallets/models/generated.py
```

**Étape 2 : Nettoyer et organiser les modèles**
- Refactoriser les modèles auto-générés
- Ajouter `ForeignKey`, `indexes`, `Meta` options propres
- Séparer dans `base.py`, `discovery.py`, `analytics.py`, `consensus.py`

**Étape 3 : Créer les migrations Django**
```bash
python manage.py makemigrations
python manage.py migrate
```

**Étape 4 : Migrer les données SQLite → PostgreSQL**

**Option choisie : Script de migration manuel Django ORM**
```python
# wallets/management/commands/migrate_from_sqlite.py
import sqlite3
from django.core.management.base import BaseCommand
from wallets.models import Wallet, Token, Transaction

class Command(BaseCommand):
    def handle(self, *args, **options):
        conn = sqlite3.connect('data/db/wit_database.db')
        cursor = conn.cursor()

        # Migrer wallets
        cursor.execute("SELECT * FROM wallets")
        for row in cursor.fetchall():
            Wallet.objects.update_or_create(
                address=row[0],
                defaults={'period': row[1], 'portfolio_value': row[2]}
            )

        # Migrer tokens, transactions, analytics...
        # ...

        self.stdout.write(self.style.SUCCESS('Migration terminée'))
```

**Avantages :**
- ✅ Contrôle total sur la transformation des données
- ✅ Peut gérer des transformations complexes si nécessaire
- ✅ Django ORM gère automatiquement les relations ForeignKey

---

## 4. API REST Django (DRF)

### Endpoints principaux pour le frontend Next.js

```python
# Wallets
GET    /api/v1/wallets/                          # Liste smart wallets
GET    /api/v1/wallets/{address}/                # Détails wallet
GET    /api/v1/wallets/{address}/positions/      # Positions actuelles
GET    /api/v1/wallets/{address}/transactions/   # Historique transactions
GET    /api/v1/wallets/{address}/analytics/      # Métriques FIFO/ROI

# Analytics
GET    /api/v1/analytics/performance/            # Performance globale
GET    /api/v1/analytics/tiers/                  # Analyse par tier (3k, 6k, 9k, 12k)

# Consensus
GET    /api/v1/consensus/signals/                # Signaux de consensus actifs
GET    /api/v1/consensus/signals/{id}/           # Détail signal + wallets impliqués
GET    /api/v1/consensus/history/                # Historique signaux

# Stats
GET    /api/v1/stats/overview/                   # Dashboard stats
```

### Architecture DRF

```python
# wallets/views/wallets.py
from rest_framework import viewsets
from rest_framework.decorators import action
from wallets.models import Wallet
from wallets.serializers.wallet import WalletSerializer

class WalletViewSet(viewsets.ReadOnlyModelViewSet):
    """API read-only pour les wallets."""
    queryset = Wallet.objects.filter(is_smart_wallet=True)
    serializer_class = WalletSerializer

    @action(detail=True, methods=['get'])
    def analytics(self, request, pk=None):
        """Retourne les analytics d'un wallet."""
        wallet = self.get_object()
        analytics = wallet.token_analytics.all()
        # ... retourner FIFO, ROI, win rate, etc.
        return Response(data)
```

**Configuration :**
- **Pagination :** 50 items par défaut, configurable
- **Filtres :** DRF filters (par score, ROI, période, etc.)
- **Permissions :** Read-only pour le moment (pas d'authentification requise)
- **Documentation :** Auto-générée via DRF Spectacular (Swagger/OpenAPI)

---

## 5. Celery + Redis (Task Queue)

### Configuration Celery

```python
# config/celery.py
from celery import Celery
from celery.schedules import crontab

app = Celery('wit')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

# Celery Beat Schedule (remplace run_pipelines.py scheduler)
app.conf.beat_schedule = {
    'discovery-pipeline-daily': {
        'task': 'wallets.tasks.run_discovery_pipeline',
        'schedule': crontab(hour=2, minute=0),  # 02:00 daily
    },
    'scoring-pipeline-2days': {
        'task': 'wallets.tasks.run_scoring_pipeline',
        'schedule': crontab(hour=4, minute=0, day_of_week='*/2'),  # Every 2 days at 04:00
    },
    'tracking-live-2hours': {
        'task': 'wallets.tasks.run_tracking_live',
        'schedule': crontab(minute=0, hour='*/2'),  # Every 2 hours
    },
    'consensus-detection-2hours': {
        'task': 'wallets.tasks.run_consensus_detection',
        'schedule': crontab(minute=30, hour='*/2'),  # Every 2h at :30
    },
}
```

### Tâches Celery principales

```python
# wallets/tasks.py
from celery import shared_task
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

@shared_task(bind=True, max_retries=3)
def run_discovery_pipeline(self):
    """Token discovery pipeline (Dune + explosion detection)."""
    try:
        from wallets.services.discovery.pipeline import DiscoveryPipeline
        pipeline = DiscoveryPipeline()
        result = pipeline.run()
        logger.info(f"Discovery completed: {result}")
        return result
    except Exception as exc:
        logger.error(f"Discovery failed: {exc}")
        self.retry(exc=exc, countdown=300)  # Retry après 5min

@shared_task
def run_tracking_live():
    """Live tracking des smart wallets."""
    from wallets.services.tracking.live_tracker import LiveTracker
    tracker = LiveTracker()
    result = tracker.run()
    logger.info(f"Tracking completed: {result}")
    return result

@shared_task
def run_scoring_pipeline():
    """FIFO + Scoring + Tier analysis."""
    from wallets.services.analytics.scoring_pipeline import ScoringPipeline
    pipeline = ScoringPipeline()
    result = pipeline.run()
    logger.info(f"Scoring completed: {result}")
    return result

@shared_task
def run_consensus_detection():
    """Détection consensus + Telegram alert."""
    from wallets.services.consensus.consensus_detector import ConsensusDetector
    detector = ConsensusDetector()
    signals = detector.detect()

    if signals:
        logger.info(f"Consensus detected: {len(signals)} signals")
        send_telegram_alerts.delay([s.id for s in signals])

    return len(signals)

@shared_task
def send_telegram_alerts(signal_ids):
    """Envoie alertes Telegram."""
    from wallets.models import ConsensusSignal
    from notifications.services.telegram_bot import send_consensus_alert

    for signal_id in signal_ids:
        signal = ConsensusSignal.objects.get(id=signal_id)
        send_consensus_alert(signal)
```

### Redis Configuration

```python
# config/settings/base.py
CELERY_BROKER_URL = os.getenv('REDIS_URL', 'redis://redis:6379/0')
CELERY_RESULT_BACKEND = os.getenv('REDIS_URL', 'redis://redis:6379/0')
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = 'Europe/Paris'
CELERY_TASK_TRACK_STARTED = True
CELERY_TASK_TIME_LIMIT = 30 * 60  # 30 minutes max par tâche
```

---

## 6. Docker & docker-compose

### docker-compose.yml (development)

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: wit_database
      POSTGRES_USER: wit_user
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U wit_user"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 3s
      retries: 3

  backend:
    build:
      context: .
      dockerfile: docker/backend/Dockerfile
    command: gunicorn config.wsgi:application --bind 0.0.0.0:8000 --workers 4
    volumes:
      - ./backend:/app
      - static_volume:/app/staticfiles
      - media_volume:/app/media
    ports:
      - "8000:8000"
    env_file:
      - .env
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    environment:
      - DATABASE_URL=postgresql://wit_user:${POSTGRES_PASSWORD}@postgres:5432/wit_database
      - REDIS_URL=redis://redis:6379/0

  worker:
    build:
      context: .
      dockerfile: docker/backend/Dockerfile
    command: celery -A config worker --loglevel=info --concurrency=4
    volumes:
      - ./backend:/app
    env_file:
      - .env
    depends_on:
      - postgres
      - redis
      - backend
    environment:
      - DATABASE_URL=postgresql://wit_user:${POSTGRES_PASSWORD}@postgres:5432/wit_database
      - REDIS_URL=redis://redis:6379/0

  beat:
    build:
      context: .
      dockerfile: docker/backend/Dockerfile
    command: celery -A config beat --loglevel=info --scheduler django_celery_beat.schedulers:DatabaseScheduler
    volumes:
      - ./backend:/app
    env_file:
      - .env
    depends_on:
      - postgres
      - redis
      - backend
    environment:
      - DATABASE_URL=postgresql://wit_user:${POSTGRES_PASSWORD}@postgres:5432/wit_database
      - REDIS_URL=redis://redis:6379/0

volumes:
  postgres_data:
  static_volume:
  media_volume:
```

### Dockerfile Backend

```dockerfile
# docker/backend/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY backend/requirements/production.txt .
RUN pip install --no-cache-dir -r production.txt

# Copy application
COPY backend/ .

# Create non-root user
RUN useradd -m -u 1000 wit && chown -R wit:wit /app
USER wit

# Collect static files
RUN python manage.py collectstatic --noinput || true

EXPOSE 8000

CMD ["gunicorn", "config.wsgi:application", "--bind", "0.0.0.0:8000"]
```

### Commandes Docker

```bash
# Development
docker-compose up -d                           # Démarrer tous les services
docker-compose logs -f worker                  # Voir logs du worker
docker-compose exec backend python manage.py migrate
docker-compose exec backend python manage.py createsuperuser
docker-compose exec backend python manage.py migrate_from_sqlite

# Production
docker-compose -f docker-compose.prod.yml up -d --build
docker-compose -f docker-compose.prod.yml logs -f
```

---

## 7. Migration de la Logique Métier

### Stratégie de migration du code existant

**Principe : Services Pattern**

Tout le code métier actuel dans `smart_wallet_analysis/` sera migré vers `backend/wallets/services/` en utilisant Django ORM au lieu de raw SQL.

### Mapping des fichiers

| Ancien fichier | Nouveau fichier Django |
|----------------|------------------------|
| `token_discovery_manual/dune_api_loop_manual.py` | `wallets/services/discovery/dune_client.py` |
| `token_discovery_manual/runner.py` | `wallets/services/discovery/explosion_detector.py` |
| `wallet_tracker/wallet_balances_extractor.py` | `wallets/services/tracking/zerion_client.py` |
| `tracking_live/live_wallet_balances_extractor_zerion.py` | `wallets/services/tracking/balance_tracker.py` |
| `score_engine/fifo_clean_simple.py` | `wallets/services/analytics/fifo_calculator.py` |
| `score_engine/wallet_scoring_system.py` | `wallets/services/analytics/wallet_scorer.py` |
| `score_engine/simple_wallet_analyzer.py` | `wallets/services/analytics/tier_analyzer.py` |
| `consensus_live/consensus_live_detector.py` | `wallets/services/consensus/consensus_detector.py` |
| `Telegram/telegram_bot.py` | `notifications/services/telegram_bot.py` |

### Exemple de migration

**AVANT (SQLite raw queries) :**
```python
# smart_wallet_analysis/score_engine/fifo_clean_simple.py
import sqlite3
from smart_wallet_analysis.config import TRACKING_LIVE as _TL

def calculate_fifo_for_wallet(wallet_address):
    conn = sqlite3.connect('data/db/wit_database.db')
    cursor = conn.cursor()

    MIN_VALUE = _TL["MIN_TOKEN_VALUE"]

    cursor.execute("""
        SELECT * FROM transaction_history
        WHERE wallet_address = ?
        ORDER BY date
    """, (wallet_address,))

    # ... logique FIFO avec raw SQL
    # ... calculs ROI, win rate

    cursor.execute("""
        INSERT OR REPLACE INTO token_analytics
        (wallet_address, token_symbol, total_invested, roi_percentage)
        VALUES (?, ?, ?, ?)
    """, (wallet_address, token, invested, roi))

    conn.commit()
```

**APRÈS (Django ORM) :**
```python
# wallets/services/analytics/fifo_calculator.py
from django.conf import settings
from wallets.models import Wallet, Transaction, TokenAnalytics

class FIFOCalculator:
    """Service pour calculer FIFO accounting."""

    def __init__(self):
        self.min_value = settings.TRACKING_LIVE_MIN_TOKEN_VALUE

    def calculate_for_wallet(self, wallet):
        """Calculate FIFO pour un wallet Django ORM."""
        transactions = wallet.transactions.order_by('date')

        # Même logique métier, mais avec Django ORM
        fifo_data = {}
        for tx in transactions:
            # ... calculs FIFO identiques
            pass

        # Sauvegarde avec Django ORM (atomic)
        for token_symbol, data in fifo_data.items():
            TokenAnalytics.objects.update_or_create(
                wallet=wallet,
                token_symbol=token_symbol,
                defaults={
                    'total_invested': data['invested'],
                    'roi_percentage': data['roi'],
                    'is_winning': data['roi'] > 0,
                    'status': self._get_status(data['roi']),
                }
            )

        return fifo_data

    def _get_status(self, roi):
        if roi > 50:
            return 'GAGNANT'
        elif roi < -20:
            return 'PERDANT'
        return 'NEUTRE'
```

### Refactoring checklist par service

Pour chaque service migré :

1. ✅ **Remplacer SQLite raw queries → Django ORM**
2. ✅ **Remplacer `config.py` constants → Django settings**
3. ✅ **Remplacer logging custom → Django logging**
4. ✅ **API clients externes** : garder la logique, juste adapter les imports
5. ✅ **Tests unitaires** : créer des tests Django pour valider que la logique est identique

**Configuration Django settings :**
```python
# config/settings/base.py

# Tracking Live config (migré de config.py)
TRACKING_LIVE_MIN_TOKEN_VALUE = 500
TRACKING_LIVE_HOURS_LOOKBACK = 24

# Score Engine config
SCORE_ENGINE_MIN_SCORE = 20
SCORE_ENGINE_ROI_WEIGHT = 0.6
SCORE_ENGINE_WINRATE_WEIGHT = 0.3
SCORE_ENGINE_ACTIVITY_WEIGHT = 0.1

# Consensus config
CONSENSUS_MIN_WHALES = 2
CONSENSUS_MCAP_MIN = 100_000
CONSENSUS_MCAP_MAX = 100_000_000
```

---

## 8. Tests & Validation

### Stratégie de validation de la migration

Pour garantir que la logique Django produit les **mêmes résultats** que le code actuel :

### Tests de comparaison (Migration Validation)

```python
# wallets/tests/test_migration_validation.py
import sqlite3
from django.test import TestCase
from wallets.models import Wallet, Transaction, TokenAnalytics
from wallets.services.analytics.fifo_calculator import FIFOCalculator

class FIFOMigrationTest(TestCase):
    """Valide que le FIFO Django = FIFO SQLite."""

    fixtures = ['test_wallets.json']

    def test_fifo_results_match_old_system(self):
        """Compare résultats FIFO ancien vs nouveau système."""

        # 1. Charger données test depuis l'ancienne DB SQLite
        old_results = self._get_old_fifo_results('0xTestWallet')

        # 2. Calculer avec le nouveau système Django
        wallet = Wallet.objects.get(address='0xTestWallet')
        calculator = FIFOCalculator()
        calculator.calculate_for_wallet(wallet)
        new_results = TokenAnalytics.objects.filter(wallet=wallet).first()

        # 3. Comparer (tolérance de 0.01% pour les arrondis)
        self.assertAlmostEqual(
            old_results['total_invested'],
            new_results.total_invested,
            places=2
        )
        self.assertAlmostEqual(
            old_results['roi_percentage'],
            new_results.roi_percentage,
            places=2
        )

    def _get_old_fifo_results(self, wallet_address):
        """Lit les résultats de l'ancienne DB SQLite."""
        conn = sqlite3.connect('data/db/wit_database.db')
        cursor = conn.cursor()
        cursor.execute(
            "SELECT total_invested, roi_percentage FROM token_analytics WHERE wallet_address = ?",
            (wallet_address,)
        )
        row = cursor.fetchone()
        return {'total_invested': row[0], 'roi_percentage': row[1]}
```

### Tests unitaires Django

```python
# wallets/tests/test_services.py
from django.test import TestCase
from wallets.models import Wallet, TokenAnalytics
from wallets.services.analytics.wallet_scorer import WalletScorer

class WalletScorerTest(TestCase):
    def setUp(self):
        self.wallet = Wallet.objects.create(address='0xTest')
        TokenAnalytics.objects.create(
            wallet=self.wallet,
            token_symbol='ETH',
            total_invested=1000,
            roi_percentage=50,
            is_winning=True
        )

    def test_scoring_calculation(self):
        """Test calcul du score wallet."""
        scorer = WalletScorer()
        score = scorer.calculate_score(self.wallet)

        # Score = 60% ROI + 30% winrate + 10% activity
        # Score attendu = (0.6 * 50) + (0.3 * 100) + (0.1 * activity)
        self.assertGreater(score, 0)
        self.assertLessEqual(score, 100)
```

```python
# wallets/tests/test_api.py
from rest_framework.test import APITestCase
from wallets.models import Wallet

class WalletAPITest(APITestCase):
    def setUp(self):
        Wallet.objects.create(address='0xTest', is_smart_wallet=True)

    def test_wallet_list_endpoint(self):
        """Test GET /api/v1/wallets/"""
        response = self.client.get('/api/v1/wallets/')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data['results']), 1)

    def test_wallet_detail_endpoint(self):
        """Test GET /api/v1/wallets/{address}/"""
        response = self.client.get('/api/v1/wallets/0xTest/')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['address'], '0xTest')
```

### Tests Celery

```python
# wallets/tests/test_tasks.py
from django.test import TestCase
from wallets.tasks import run_discovery_pipeline

class CeleryTasksTest(TestCase):
    def test_discovery_pipeline_task(self):
        """Test que la tâche discovery s'exécute sans erreur."""
        # Mode synchrone pour les tests
        result = run_discovery_pipeline.apply()
        self.assertTrue(result.successful())
```

### Exécution des tests

```bash
# Tous les tests
docker-compose exec backend python manage.py test

# Tests spécifiques
docker-compose exec backend python manage.py test wallets.tests.test_migration_validation

# Coverage
docker-compose exec backend coverage run --source='.' manage.py test
docker-compose exec backend coverage report
```

---

## 9. Stratégie de Déploiement & Migration Progressive

### Plan de migration (branche séparée)

**Branche Git :** `feature/django-migration`

```
Étape 1 : Setup infrastructure
├── Créer projet Django
├── Setup Docker + PostgreSQL + Redis
├── Créer modèles Django (inspectdb + refactoring)
└── Migrer données SQLite → PostgreSQL (script custom)

Étape 2 : API REST
├── Implémenter endpoints API (wallets, analytics, consensus)
├── Tests API
├── Connecter frontend Next.js à l'API Django
└── Valider que le frontend affiche les vraies données

Étape 3 : Migration logique métier (service par service)
├── Discovery pipeline (Dune client + explosion detector)
├── Tracking pipeline (Zerion client + balance tracker)
├── FIFO + Scoring (calculator + scorer + tier analyzer)
├── Consensus detection
└── Tests de validation pour chaque service (ancien vs nouveau)

Étape 4 : Celery + Scheduler
├── Convertir en Celery tasks
├── Setup Celery Beat
├── Tests end-to-end (pipelines complets)
└── Vérifier scheduler automatique

Étape 5 : Production
├── docker-compose.prod.yml
├── Variables d'environnement production
├── Tests finaux sur toute la stack
└── Documentation déploiement
```

### Gestion des deux systèmes en parallèle

**Pendant la migration (durée estimée : 1-2 semaines) :**
- ✅ Ancien système continue de tourner sur branche `main`
- ✅ Nouveau système se développe sur `feature/django-migration`
- ✅ PostgreSQL peut coexister avec SQLite (ports différents, bases différentes)
- ✅ Frontend peut basculer entre mock data et API Django (via variable d'env)

**Validation avant merge :**
```bash
# Sur feature/django-migration
docker-compose up -d
docker-compose exec backend python manage.py test
docker-compose exec backend python manage.py migrate_from_sqlite

# Lancer manuellement chaque pipeline et comparer résultats
docker-compose exec backend python manage.py run_discovery
docker-compose exec backend python manage.py run_scoring
docker-compose exec backend python manage.py run_consensus

# Vérifier que les résultats dans PostgreSQL = résultats dans SQLite
```

### Bascule finale vers production

```bash
# 1. Arrêter ancien scheduler (si tourne sur VPS)
ssh root@46.224.0.146
pkill -f run_pipelines.py

# 2. Sur la branche Django
git checkout feature/django-migration
docker-compose -f docker-compose.prod.yml up -d --build

# 3. Migration finale des données
docker-compose exec backend python manage.py migrate_from_sqlite

# 4. Vérifier que tout tourne
docker-compose logs -f
docker-compose exec backend python manage.py test

# 5. Merge vers main
git checkout main
git merge feature/django-migration
git push origin main
```

### Rollback strategy (si problème)

```bash
# Retour rapide à l'ancien système
git checkout main
python -m smart_wallet_analysis.discovery_pipeline_runner
python run_pipelines.py scheduler

# OU redémarrer ancien service systemd sur VPS
ssh root@46.224.0.146
systemctl start wit_scheduler.service
```

### Monitoring post-déploiement

```bash
# Logs Celery worker
docker-compose logs -f worker

# Logs Celery beat (scheduler)
docker-compose logs -f beat

# Logs backend API
docker-compose logs -f backend

# État des tâches Celery
docker-compose exec backend python manage.py shell
>>> from wallets.tasks import run_discovery_pipeline
>>> result = run_discovery_pipeline.delay()
>>> result.status
```

---

## 10. Checklist de Migration

### Phase 1 : Infrastructure (Jour 1-2)

- [ ] Créer branche `feature/django-migration`
- [ ] Créer projet Django avec structure finale
- [ ] Setup Docker (postgres, redis, backend, worker, beat)
- [ ] Générer modèles Django (`inspectdb`)
- [ ] Refactoriser modèles (ForeignKey, indexes, Meta)
- [ ] Créer migrations Django
- [ ] Script `migrate_from_sqlite` fonctionnel
- [ ] Données migrées avec succès dans PostgreSQL

### Phase 2 : API REST (Jour 3-4)

- [ ] Serializers DRF pour tous les modèles
- [ ] ViewSets wallets (list, detail, positions, transactions, analytics)
- [ ] ViewSets analytics (performance, tiers)
- [ ] ViewSets consensus (signals, history)
- [ ] Tests API (200 OK, pagination, filtres)
- [ ] Frontend Next.js connecté à l'API Django
- [ ] Frontend affiche vraies données (pas mock)

### Phase 3 : Logique Métier (Jour 5-6)

- [ ] Service Discovery (Dune client + explosion detector)
- [ ] Service Tracking (Zerion client + balance tracker)
- [ ] Service FIFO Calculator
- [ ] Service Wallet Scorer
- [ ] Service Tier Analyzer
- [ ] Service Consensus Detector
- [ ] Service Telegram Notifier
- [ ] Tests de validation (ancien vs nouveau pour FIFO, scoring, etc.)

### Phase 4 : Celery (Jour 7)

- [ ] Config Celery (`config/celery.py`)
- [ ] Tasks (`run_discovery_pipeline`, `run_tracking_live`, etc.)
- [ ] Celery Beat schedule
- [ ] Tests Celery tasks
- [ ] Management commands Django (`run_discovery`, `run_scoring`, etc.)

### Phase 5 : Production (Jour 8)

- [ ] `docker-compose.prod.yml`
- [ ] Variables d'environnement production (`.env.example`)
- [ ] Tests end-to-end complets
- [ ] Documentation déploiement
- [ ] Migration finale sur VPS
- [ ] Monitoring configuré
- [ ] Merge vers `main`

---

## 11. Risques & Mitigations

| Risque | Impact | Probabilité | Mitigation |
|--------|--------|-------------|------------|
| Régression logique métier (FIFO, scoring différent) | Élevé | Moyen | Tests de validation (ancien vs nouveau), comparaison résultats |
| Perte de données lors migration SQLite → PostgreSQL | Élevé | Faible | Backup SQLite avant migration, script idempotent (update_or_create) |
| Circular imports entre apps Django | Moyen | Moyen | Structure claire (wallets → notifications, pas inverse) |
| Performance PostgreSQL < SQLite pour petits datasets | Faible | Faible | PostgreSQL optimisé avec indexes, connexion pooling |
| Celery tasks qui crashent en production | Moyen | Moyen | Retry automatique, logging détaillé, monitoring Flower |
| Docker containers qui manquent de mémoire | Moyen | Faible | Limits Docker (memory, CPU), monitoring ressources |

---

## 12. Points de Décision Validés

1. **Database :** PostgreSQL (pas SQLite en prod)
2. **Scripts :** Django Management Commands (pas scripts séparés)
3. **Architecture Docker :** Multi-conteneurs (backend, worker, beat séparés)
4. **Structure Apps :** 2 apps Django (wallets pour toutes les analyses, notifications pour les outputs)
5. **Migration données :** Script Django ORM custom (pas export/import SQL brut)
6. **API :** Django REST Framework read-only (pas d'auth pour l'instant)
7. **Tests :** Tests de validation migration (ancien vs nouveau)
8. **Déploiement :** Branche séparée, migration progressive, rollback possible

---

## Conclusion

Cette migration transforme WIT V1 d'un ensemble de scripts CLI vers une **architecture backend professionnelle** :

✅ **API REST** pour le frontend Next.js
✅ **PostgreSQL** pour robustesse en production
✅ **Celery + Redis** pour pipelines asynchrones fiables
✅ **Docker** pour déploiement reproductible
✅ **Django Admin** pour debug et monitoring
✅ **Scalable** : prêt pour futures features (wallet graph, etc.)

**Next step :** Créer le plan d'implémentation détaillé avec la skill `writing-plans`.
