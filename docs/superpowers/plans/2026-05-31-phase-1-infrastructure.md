# Phase 1: Infrastructure Setup - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Setup Django project with Docker, PostgreSQL, migrate data from SQLite, and have a functional Django Admin.

**Architecture:** Django 5.0 project with 2 apps (wallets, notifications), PostgreSQL 16 database, Docker multi-container setup (postgres, redis, backend, worker, beat), data migration from SQLite.

**Tech Stack:** Django 5.0, PostgreSQL 16, Docker, docker-compose, psycopg2

---

## File Structure

**New files to create:**
```
backend/
├── manage.py
├── config/
│   ├── __init__.py
│   ├── settings/
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── development.py
│   │   └── production.py
│   ├── urls.py
│   ├── wsgi.py
│   └── celery.py
├── wallets/
│   ├── __init__.py
│   ├── models/
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── discovery.py
│   │   ├── analytics.py
│   │   └── consensus.py
│   ├── admin.py
│   ├── apps.py
│   └── migrations/
├── notifications/
│   ├── __init__.py
│   ├── models.py
│   ├── admin.py
│   └── apps.py
├── common/
│   ├── __init__.py
│   └── constants.py
└── requirements/
    ├── base.txt
    ├── development.txt
    └── production.txt

docker/
├── backend/
│   └── Dockerfile
└── postgres/
    └── init.sql

docker-compose.yml
.env.example
```

---

## Task 1: Create Git Branch

**Files:**
- Modify: `.git/` (new branch)

- [ ] **Step 1: Create feature branch**

```bash
git checkout -b feature/django-migration
```

Expected: `Switched to a new branch 'feature/django-migration'`

- [ ] **Step 2: Verify branch**

```bash
git branch
```

Expected: `* feature/django-migration` (avec astérisque)

- [ ] **Step 3: Initial commit**

```bash
git commit --allow-empty -m "chore: start Django migration on feature branch"
```

---

## Task 2: Create Django Project Structure

**Files:**
- Create: `backend/manage.py`
- Create: `backend/config/__init__.py`
- Create: `backend/config/settings/base.py`
- Create: `backend/config/urls.py`
- Create: `backend/config/wsgi.py`

- [ ] **Step 1: Create backend directory**

```bash
mkdir -p backend/config/settings
touch backend/manage.py
touch backend/config/__init__.py
touch backend/config/settings/__init__.py
touch backend/config/settings/base.py
touch backend/config/settings/development.py
touch backend/config/settings/production.py
touch backend/config/urls.py
touch backend/config/wsgi.py
```

- [ ] **Step 2: Write manage.py**

Create `backend/manage.py`:

```python
#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys


def main():
    """Run administrative tasks."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.development')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
```

- [ ] **Step 3: Make manage.py executable**

```bash
chmod +x backend/manage.py
```

- [ ] **Step 4: Write base settings**

Create `backend/config/settings/base.py`:

```python
"""Django base settings for WIT V1."""
import os
from pathlib import Path

# Build paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Security
SECRET_KEY = os.getenv('DJANGO_SECRET_KEY', 'django-insecure-dev-key-change-in-production')
DEBUG = False
ALLOWED_HOSTS = []

# Applications
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',

    # Third-party
    'rest_framework',

    # WIT apps
    'wallets',
    'notifications',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'config.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'config.wsgi.application'

# Database (default, overridden in dev/prod)
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.getenv('POSTGRES_DB', 'wit_database'),
        'USER': os.getenv('POSTGRES_USER', 'wit_user'),
        'PASSWORD': os.getenv('POSTGRES_PASSWORD', 'changeme'),
        'HOST': os.getenv('POSTGRES_HOST', 'postgres'),
        'PORT': os.getenv('POSTGRES_PORT', '5432'),
    }
}

# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]

# Internationalization
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'Europe/Paris'
USE_I18N = True
USE_TZ = True

# Static files
STATIC_URL = 'static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'

# Default primary key field type
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# REST Framework
REST_FRAMEWORK = {
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.PageNumberPagination',
    'PAGE_SIZE': 50,
}

# WIT Configuration (migré de config.py)
TRACKING_LIVE_MIN_TOKEN_VALUE = 500
TRACKING_LIVE_HOURS_LOOKBACK = 24

SCORE_ENGINE_MIN_SCORE = 20
SCORE_ENGINE_ROI_WEIGHT = 0.6
SCORE_ENGINE_WINRATE_WEIGHT = 0.3
SCORE_ENGINE_ACTIVITY_WEIGHT = 0.1

CONSENSUS_MIN_WHALES = 2
CONSENSUS_MCAP_MIN = 100_000
CONSENSUS_MCAP_MAX = 100_000_000
```

- [ ] **Step 5: Write development settings**

Create `backend/config/settings/development.py`:

```python
"""Development settings."""
from .base import *

DEBUG = True
ALLOWED_HOSTS = ['*']

# Development-specific settings
INTERNAL_IPS = ['127.0.0.1']
```

- [ ] **Step 6: Write production settings**

Create `backend/config/settings/production.py`:

```python
"""Production settings."""
from .base import *

DEBUG = False
ALLOWED_HOSTS = os.getenv('ALLOWED_HOSTS', 'localhost').split(',')

# Security settings
SECURE_SSL_REDIRECT = True
SESSION_COOKIE_SECURE = True
CSRF_COOKIE_SECURE = True
SECURE_BROWSER_XSS_FILTER = True
SECURE_CONTENT_TYPE_NOSNIFF = True
X_FRAME_OPTIONS = 'DENY'
```

- [ ] **Step 7: Write urls.py**

Create `backend/config/urls.py`:

```python
"""URL configuration for WIT V1."""
from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', include('api.urls')),
]
```

- [ ] **Step 8: Write wsgi.py**

Create `backend/config/wsgi.py`:

```python
"""WSGI config for WIT V1."""
import os
from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.production')
application = get_wsgi_application()
```

- [ ] **Step 9: Commit**

```bash
git add backend/manage.py backend/config/
git commit -m "feat(infrastructure): create Django project structure"
```

---

## Task 3: Create Requirements Files

**Files:**
- Create: `backend/requirements/base.txt`
- Create: `backend/requirements/development.txt`
- Create: `backend/requirements/production.txt`

- [ ] **Step 1: Create requirements directory**

```bash
mkdir -p backend/requirements
```

- [ ] **Step 2: Write base requirements**

Create `backend/requirements/base.txt`:

```
Django==5.0.1
djangorestframework==3.14.0
psycopg2-binary==2.9.9
celery==5.3.4
redis==5.0.1
python-dotenv==1.0.0
requests==2.31.0
pandas==2.1.4
```

- [ ] **Step 3: Write development requirements**

Create `backend/requirements/development.txt`:

```
-r base.txt

# Development tools
django-debug-toolbar==4.2.0
ipython==8.19.0
pytest==7.4.3
pytest-django==4.7.0
pytest-cov==4.1.0
```

- [ ] **Step 4: Write production requirements**

Create `backend/requirements/production.txt`:

```
-r base.txt

# Production server
gunicorn==21.2.0
```

- [ ] **Step 5: Commit**

```bash
git add backend/requirements/
git commit -m "feat(infrastructure): add requirements files"
```

---

## Task 4: Create Django Apps

**Files:**
- Create: `backend/wallets/__init__.py`
- Create: `backend/wallets/apps.py`
- Create: `backend/wallets/models/__init__.py`
- Create: `backend/wallets/admin.py`
- Create: `backend/notifications/__init__.py`
- Create: `backend/notifications/apps.py`
- Create: `backend/notifications/models.py`
- Create: `backend/notifications/admin.py`

- [ ] **Step 1: Create wallets app structure**

```bash
mkdir -p backend/wallets/models
mkdir -p backend/wallets/migrations
touch backend/wallets/__init__.py
touch backend/wallets/apps.py
touch backend/wallets/models/__init__.py
touch backend/wallets/admin.py
```

- [ ] **Step 2: Write wallets/apps.py**

Create `backend/wallets/apps.py`:

```python
"""Wallets app configuration."""
from django.apps import AppConfig


class WalletsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'wallets'
    verbose_name = 'Wallets & Analytics'
```

- [ ] **Step 3: Create notifications app**

```bash
mkdir -p backend/notifications/migrations
touch backend/notifications/__init__.py
touch backend/notifications/apps.py
touch backend/notifications/models.py
touch backend/notifications/admin.py
```

- [ ] **Step 4: Write notifications/apps.py**

Create `backend/notifications/apps.py`:

```python
"""Notifications app configuration."""
from django.apps import AppConfig


class NotificationsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'notifications'
    verbose_name = 'Notifications & Alerts'
```

- [ ] **Step 5: Write notifications/models.py (placeholder)**

Create `backend/notifications/models.py`:

```python
"""Notification models."""
from django.db import models


class NotificationLog(models.Model):
    """Log des notifications envoyées."""
    created_at = models.DateTimeField(auto_now_add=True)
    notification_type = models.CharField(max_length=50)
    message = models.TextField()
    sent_successfully = models.BooleanField(default=False)
    error_message = models.TextField(blank=True)

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Notification Log'
        verbose_name_plural = 'Notification Logs'

    def __str__(self):
        return f"{self.notification_type} - {self.created_at}"
```

- [ ] **Step 6: Write notifications/admin.py**

Create `backend/notifications/admin.py`:

```python
"""Notifications admin."""
from django.contrib import admin
from .models import NotificationLog


@admin.register(NotificationLog)
class NotificationLogAdmin(admin.ModelAdmin):
    list_display = ['created_at', 'notification_type', 'sent_successfully']
    list_filter = ['notification_type', 'sent_successfully', 'created_at']
    search_fields = ['message']
    readonly_fields = ['created_at']
```

- [ ] **Step 7: Commit**

```bash
git add backend/wallets/ backend/notifications/
git commit -m "feat(infrastructure): create Django apps (wallets, notifications)"
```

---

## Task 5: Docker Setup

**Files:**
- Create: `docker-compose.yml`
- Create: `docker/backend/Dockerfile`
- Create: `.env.example`
- Create: `.dockerignore`

- [ ] **Step 1: Create docker directory**

```bash
mkdir -p docker/backend
mkdir -p docker/postgres
```

- [ ] **Step 2: Write Dockerfile for backend**

Create `docker/backend/Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY backend/requirements/production.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy application
COPY backend/ /app/

# Create non-root user
RUN useradd -m -u 1000 wit && chown -R wit:wit /app

# Collect static files (will fail first time, that's ok)
RUN python manage.py collectstatic --noinput || true

USER wit

EXPOSE 8000

CMD ["gunicorn", "config.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "4"]
```

- [ ] **Step 3: Write .dockerignore**

Create `.dockerignore`:

```
__pycache__
*.pyc
*.pyo
*.pyd
.Python
*.so
*.egg
*.egg-info
dist
build
.git
.env
*.sqlite3
.pytest_cache
.coverage
htmlcov
```

- [ ] **Step 4: Write docker-compose.yml**

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: ${POSTGRES_DB:-wit_database}
      POSTGRES_USER: ${POSTGRES_USER:-wit_user}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-changeme}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-wit_user}"]
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
    command: python manage.py runserver 0.0.0.0:8000
    volumes:
      - ./backend:/app
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
      - DJANGO_SETTINGS_MODULE=config.settings.development
      - POSTGRES_HOST=postgres
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
      - DJANGO_SETTINGS_MODULE=config.settings.development
      - POSTGRES_HOST=postgres
      - REDIS_URL=redis://redis:6379/0

  beat:
    build:
      context: .
      dockerfile: docker/backend/Dockerfile
    command: celery -A config beat --loglevel=info
    volumes:
      - ./backend:/app
    env_file:
      - .env
    depends_on:
      - postgres
      - redis
      - backend
    environment:
      - DJANGO_SETTINGS_MODULE=config.settings.development
      - POSTGRES_HOST=postgres
      - REDIS_URL=redis://redis:6379/0

volumes:
  postgres_data:
```

- [ ] **Step 5: Write .env.example**

Create `.env.example`:

```bash
# Database
POSTGRES_DB=wit_database
POSTGRES_USER=wit_user
POSTGRES_PASSWORD=changeme_in_production

# Django
DJANGO_SECRET_KEY=changeme_in_production
DJANGO_SETTINGS_MODULE=config.settings.development

# APIs (copier depuis .env actuel)
ZERION_API_KEY=your_zerion_key
ZERION_API_KEY_2=your_zerion_backup_key
DUNE_API_KEY=your_dune_key
ETHERSCAN_API_KEY=your_etherscan_key
ALCHEMY_API_KEY=your_alchemy_key
CG_API_KEY=your_coingecko_key
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHANNEL_ID=your_channel_id
```

- [ ] **Step 6: Copy .env.example to .env**

```bash
cp .env.example .env
# Puis éditer .env avec les vraies valeurs depuis l'ancien .env
```

- [ ] **Step 7: Commit**

```bash
git add docker-compose.yml docker/ .env.example .dockerignore
git commit -m "feat(infrastructure): add Docker configuration"
```

---

## Task 6: Generate Django Models from SQLite

**Files:**
- Create: `backend/wallets/models/base.py`
- Create: `backend/wallets/models/discovery.py`
- Create: `backend/wallets/models/analytics.py`
- Create: `backend/wallets/models/consensus.py`
- Modify: `backend/wallets/models/__init__.py`

- [ ] **Step 1: Start Docker containers**

```bash
docker-compose up -d postgres redis
```

Expected: postgres and redis containers running

- [ ] **Step 2: Wait for postgres to be ready**

```bash
docker-compose logs postgres
```

Expected: "database system is ready to accept connections"

- [ ] **Step 3: Run inspectdb to generate models**

```bash
# Temporairement pointer vers l'ancienne DB SQLite
docker-compose run --rm backend python manage.py inspectdb --database default > /tmp/inspected_models.py
```

Note: This will fail because we haven't configured SQLite. Instead, we'll write models manually based on schema.

- [ ] **Step 4: Write base models manually**

Create `backend/wallets/models/base.py`:

```python
"""Base wallet models."""
from django.db import models


class Wallet(models.Model):
    """Wallet principal."""
    address = models.CharField(max_length=42, primary_key=True)
    period = models.CharField(max_length=20)  # 14d, 30d, 200d, 360d, manual
    total_portfolio_value = models.FloatField(null=True, blank=True)
    is_smart_wallet = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'wallets'
        ordering = ['-total_portfolio_value']

    def __str__(self):
        return f"{self.address[:10]}... ({self.period})"


class Token(models.Model):
    """Position token d'un wallet."""
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name='tokens')
    fungible_id = models.CharField(max_length=255)
    symbol = models.CharField(max_length=50)
    contract_address = models.CharField(max_length=42)
    chain = models.CharField(max_length=50)
    amount = models.FloatField()
    usd_value = models.FloatField()
    in_portfolio = models.BooleanField(default=True)

    class Meta:
        db_table = 'tokens'
        unique_together = [['wallet', 'fungible_id']]
        ordering = ['-usd_value']

    def __str__(self):
        return f"{self.symbol} ({self.wallet.address[:10]}...)"


class Transaction(models.Model):
    """Historique des transactions."""
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name='transactions')
    fungible_id = models.CharField(max_length=255)
    symbol = models.CharField(max_length=50)
    hash = models.CharField(max_length=66)
    date = models.DateTimeField()
    operation_type = models.CharField(max_length=50)
    action_type = models.CharField(max_length=20)  # buy, sell
    swap_description = models.TextField(blank=True)
    contract_address = models.CharField(max_length=42)
    quantity = models.FloatField()
    price_per_token = models.FloatField()
    total_value_usd = models.FloatField()
    direction = models.CharField(max_length=10)  # in, out
    recipient_address = models.CharField(max_length=42, blank=True)
    sender_address = models.CharField(max_length=42, blank=True)

    class Meta:
        db_table = 'transaction_history'
        unique_together = [['wallet', 'hash']]
        ordering = ['-date']
        indexes = [
            models.Index(fields=['wallet', 'date']),
            models.Index(fields=['fungible_id']),
        ]

    def __str__(self):
        return f"{self.action_type} {self.symbol} - {self.hash[:10]}..."


class WalletPositionChange(models.Model):
    """Changements de positions détectés (live tracking)."""
    session_id = models.CharField(max_length=100)
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name='position_changes')
    symbol = models.CharField(max_length=50)
    fungible_id = models.CharField(max_length=255)
    contract_address = models.CharField(max_length=42)
    change_type = models.CharField(max_length=20)  # NEW, ACCUMULATION, REDUCTION, EXIT
    old_amount = models.FloatField()
    new_amount = models.FloatField()
    usd_change = models.FloatField()
    detected_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'wallet_position_changes'
        ordering = ['-detected_at']

    def __str__(self):
        return f"{self.change_type} {self.symbol} - {self.wallet.address[:10]}..."
```

- [ ] **Step 5: Write discovery models**

Create `backend/wallets/models/discovery.py`:

```python
"""Discovery models."""
from django.db import models


class WalletBrute(models.Model):
    """Wallets bruts issus du token discovery."""
    wallet_address = models.CharField(max_length=42)
    token_address = models.CharField(max_length=42)
    contract_address = models.CharField(max_length=42)
    chain = models.CharField(max_length=50)
    temporality = models.CharField(max_length=20)  # 14d, 30d, 200d, 360d

    class Meta:
        db_table = 'wallet_brute'
        unique_together = [['wallet_address', 'token_address', 'temporality']]

    def __str__(self):
        return f"{self.wallet_address[:10]}... - {self.temporality}"
```

- [ ] **Step 6: Write analytics models**

Create `backend/wallets/models/analytics.py`:

```python
"""Analytics models."""
from django.db import models
from .base import Wallet


class TokenAnalytics(models.Model):
    """Résultats FIFO par wallet × token."""
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name='token_analytics')
    token_symbol = models.CharField(max_length=50)
    total_invested = models.FloatField()
    total_realized = models.FloatField()
    roi_percentage = models.FloatField()
    is_winning = models.BooleanField()
    status = models.CharField(max_length=20)  # GAGNANT, PERDANT, NEUTRE
    holding_days = models.IntegerField(default=0)
    in_portfolio = models.BooleanField(default=True)

    class Meta:
        db_table = 'token_analytics'
        unique_together = [['wallet', 'token_symbol']]
        indexes = [
            models.Index(fields=['wallet', 'roi_percentage']),
        ]

    def __str__(self):
        return f"{self.token_symbol} - {self.wallet.address[:10]}... ({self.roi_percentage:.1f}%)"


class WalletTierPerformance(models.Model):
    """Performance par palier d'investissement."""
    wallet = models.ForeignKey(Wallet, on_delete=models.CASCADE, related_name='tier_performance')
    tier_usd = models.IntegerField()  # 3000, 6000, 9000, 12000
    roi_percentage = models.FloatField()
    winrate = models.FloatField()
    nb_trades = models.IntegerField()
    nb_gagnants = models.IntegerField()
    is_optimal_tier = models.BooleanField(default=False)

    class Meta:
        db_table = 'wallet_tier_performance'
        unique_together = [['wallet', 'tier_usd']]

    def __str__(self):
        return f"{self.wallet.address[:10]}... - ${self.tier_usd}"


class WalletQualified(models.Model):
    """Wallets qualifiés (ayant passé le filtre)."""
    wallet = models.OneToOneField(Wallet, on_delete=models.CASCADE, primary_key=True, related_name='qualification')
    final_score = models.FloatField()
    classification = models.CharField(max_length=20)  # ELITE, EXCELLENT, BON, MOYEN, FAIBLE
    weighted_roi = models.FloatField()
    taux_reussite = models.FloatField()
    nb_trades = models.IntegerField()

    class Meta:
        db_table = 'wallet_qualified'

    def __str__(self):
        return f"{self.wallet.address[:10]}... - {self.classification}"


class SmartWallet(models.Model):
    """Smart wallets sélectionnés."""
    wallet = models.OneToOneField(Wallet, on_delete=models.CASCADE, primary_key=True, related_name='smart_wallet_data')
    optimal_threshold_tier = models.IntegerField()
    quality_score = models.FloatField()
    threshold_status = models.CharField(max_length=20)  # EXCELLENT, GOOD, AVERAGE, POOR
    optimal_roi = models.FloatField()
    optimal_winrate = models.FloatField()
    global_roi = models.FloatField()
    global_winrate = models.FloatField()

    class Meta:
        db_table = 'smart_wallets'

    def __str__(self):
        return f"{self.wallet.address[:10]}... - {self.threshold_status}"
```

- [ ] **Step 7: Write consensus models**

Create `backend/wallets/models/consensus.py`:

```python
"""Consensus models."""
from django.db import models
from .base import Wallet


class ConsensusSignal(models.Model):
    """Signal de consensus détecté."""
    token_symbol = models.CharField(max_length=50)
    contract_address = models.CharField(max_length=42)
    chain = models.CharField(max_length=50)
    nb_wallets = models.IntegerField()
    total_usd_invested = models.FloatField()
    market_cap = models.FloatField(null=True, blank=True)
    detected_at = models.DateTimeField(auto_now_add=True)
    wallets = models.ManyToManyField(Wallet, related_name='consensus_signals')

    class Meta:
        db_table = 'consensus_signals'
        ordering = ['-detected_at']

    def __str__(self):
        return f"{self.token_symbol} - {self.nb_wallets} wallets"
```

- [ ] **Step 8: Update models __init__.py**

Create `backend/wallets/models/__init__.py`:

```python
"""Wallets models."""
from .base import Wallet, Token, Transaction, WalletPositionChange
from .discovery import WalletBrute
from .analytics import TokenAnalytics, WalletTierPerformance, WalletQualified, SmartWallet
from .consensus import ConsensusSignal

__all__ = [
    'Wallet',
    'Token',
    'Transaction',
    'WalletPositionChange',
    'WalletBrute',
    'TokenAnalytics',
    'WalletTierPerformance',
    'WalletQualified',
    'SmartWallet',
    'ConsensusSignal',
]
```

- [ ] **Step 9: Commit**

```bash
git add backend/wallets/models/
git commit -m "feat(models): create Django models for wallets"
```

---

## Task 7: Create Django Admin

**Files:**
- Modify: `backend/wallets/admin.py`

- [ ] **Step 1: Write wallets admin**

Create `backend/wallets/admin.py`:

```python
"""Wallets admin."""
from django.contrib import admin
from .models import (
    Wallet, Token, Transaction, WalletPositionChange,
    WalletBrute, TokenAnalytics, WalletTierPerformance,
    WalletQualified, SmartWallet, ConsensusSignal
)


@admin.register(Wallet)
class WalletAdmin(admin.ModelAdmin):
    list_display = ['address', 'period', 'total_portfolio_value', 'is_smart_wallet', 'updated_at']
    list_filter = ['period', 'is_smart_wallet']
    search_fields = ['address']
    readonly_fields = ['created_at', 'updated_at']


@admin.register(Token)
class TokenAdmin(admin.ModelAdmin):
    list_display = ['symbol', 'wallet', 'amount', 'usd_value', 'chain', 'in_portfolio']
    list_filter = ['chain', 'in_portfolio']
    search_fields = ['symbol', 'contract_address', 'wallet__address']


@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    list_display = ['symbol', 'action_type', 'quantity', 'total_value_usd', 'date', 'wallet']
    list_filter = ['action_type', 'operation_type', 'date']
    search_fields = ['symbol', 'hash', 'wallet__address']
    date_hierarchy = 'date'


@admin.register(WalletPositionChange)
class WalletPositionChangeAdmin(admin.ModelAdmin):
    list_display = ['symbol', 'change_type', 'usd_change', 'wallet', 'detected_at']
    list_filter = ['change_type', 'detected_at']
    search_fields = ['symbol', 'wallet__address']


@admin.register(WalletBrute)
class WalletBruteAdmin(admin.ModelAdmin):
    list_display = ['wallet_address', 'temporality', 'chain']
    list_filter = ['temporality', 'chain']
    search_fields = ['wallet_address']


@admin.register(TokenAnalytics)
class TokenAnalyticsAdmin(admin.ModelAdmin):
    list_display = ['token_symbol', 'wallet', 'roi_percentage', 'status', 'is_winning']
    list_filter = ['status', 'is_winning', 'in_portfolio']
    search_fields = ['token_symbol', 'wallet__address']


@admin.register(WalletTierPerformance)
class WalletTierPerformanceAdmin(admin.ModelAdmin):
    list_display = ['wallet', 'tier_usd', 'roi_percentage', 'winrate', 'is_optimal_tier']
    list_filter = ['tier_usd', 'is_optimal_tier']


@admin.register(WalletQualified)
class WalletQualifiedAdmin(admin.ModelAdmin):
    list_display = ['wallet', 'classification', 'final_score', 'weighted_roi', 'taux_reussite']
    list_filter = ['classification']
    search_fields = ['wallet__address']


@admin.register(SmartWallet)
class SmartWalletAdmin(admin.ModelAdmin):
    list_display = ['wallet', 'threshold_status', 'optimal_threshold_tier', 'quality_score', 'optimal_roi']
    list_filter = ['threshold_status']
    search_fields = ['wallet__address']


@admin.register(ConsensusSignal)
class ConsensusSignalAdmin(admin.ModelAdmin):
    list_display = ['token_symbol', 'nb_wallets', 'total_usd_invested', 'market_cap', 'detected_at']
    list_filter = ['detected_at', 'chain']
    search_fields = ['token_symbol', 'contract_address']
    date_hierarchy = 'detected_at'
    filter_horizontal = ['wallets']
```

- [ ] **Step 2: Commit**

```bash
git add backend/wallets/admin.py
git commit -m "feat(admin): create Django admin for wallets"
```

---

## Task 8: Create Migrations and Apply

**Files:**
- Create: `backend/wallets/migrations/0001_initial.py`
- Create: `backend/notifications/migrations/0001_initial.py`

- [ ] **Step 1: Build Docker images**

```bash
docker-compose build
```

Expected: All images built successfully

- [ ] **Step 2: Create migrations**

```bash
docker-compose run --rm backend python manage.py makemigrations
```

Expected: "Migrations for 'wallets':" and "Migrations for 'notifications':"

- [ ] **Step 3: Apply migrations**

```bash
docker-compose run --rm backend python manage.py migrate
```

Expected: "Applying wallets.0001_initial... OK"

- [ ] **Step 4: Create superuser**

```bash
docker-compose run --rm backend python manage.py createsuperuser
```

Enter: username=admin, email=admin@wit.com, password=admin (dev only)

- [ ] **Step 5: Start all services**

```bash
docker-compose up -d
```

- [ ] **Step 6: Verify services are running**

```bash
docker-compose ps
```

Expected: All services "Up"

- [ ] **Step 7: Access Django admin**

Open browser: http://localhost:8000/admin
Login: admin / admin

Expected: Admin dashboard visible, can see Wallets, Tokens, etc. (empty for now)

- [ ] **Step 8: Commit migrations**

```bash
git add backend/wallets/migrations/ backend/notifications/migrations/
git commit -m "feat(migrations): create initial database schema"
```

---

## Task 9: Data Migration Script

**Files:**
- Create: `backend/wallets/management/commands/migrate_from_sqlite.py`

- [ ] **Step 1: Create management commands directory**

```bash
mkdir -p backend/wallets/management/commands
touch backend/wallets/management/__init__.py
touch backend/wallets/management/commands/__init__.py
```

- [ ] **Step 2: Write migration command**

Create `backend/wallets/management/commands/migrate_from_sqlite.py`:

```python
"""Migrate data from SQLite to PostgreSQL."""
import sqlite3
from django.core.management.base import BaseCommand
from wallets.models import (
    Wallet, Token, Transaction, WalletPositionChange,
    WalletBrute, TokenAnalytics, WalletTierPerformance,
    WalletQualified, SmartWallet
)


class Command(BaseCommand):
    help = 'Migrate data from SQLite to PostgreSQL'

    def add_arguments(self, parser):
        parser.add_argument(
            '--sqlite-path',
            type=str,
            default='data/db/wit_database.db',
            help='Path to SQLite database'
        )

    def handle(self, *args, **options):
        sqlite_path = options['sqlite_path']
        self.stdout.write(f"Connecting to SQLite: {sqlite_path}")

        conn = sqlite3.connect(sqlite_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Migrate wallets
        self.stdout.write("Migrating wallets...")
        cursor.execute("SELECT * FROM wallets")
        wallets_count = 0
        for row in cursor.fetchall():
            Wallet.objects.update_or_create(
                address=row['wallet_address'],
                defaults={
                    'period': row['period'],
                    'total_portfolio_value': row['total_portfolio_value'],
                }
            )
            wallets_count += 1
        self.stdout.write(self.style.SUCCESS(f"✓ Migrated {wallets_count} wallets"))

        # Migrate tokens
        self.stdout.write("Migrating tokens...")
        cursor.execute("SELECT * FROM tokens")
        tokens_count = 0
        for row in cursor.fetchall():
            wallet = Wallet.objects.get(address=row['wallet_address'])
            Token.objects.update_or_create(
                wallet=wallet,
                fungible_id=row['fungible_id'],
                defaults={
                    'symbol': row['symbol'],
                    'contract_address': row['contract_address'],
                    'chain': row['chain'],
                    'amount': row['amount'],
                    'usd_value': row['usd_value'],
                    'in_portfolio': bool(row['in_portfolio']),
                }
            )
            tokens_count += 1
        self.stdout.write(self.style.SUCCESS(f"✓ Migrated {tokens_count} tokens"))

        # Migrate transactions
        self.stdout.write("Migrating transactions...")
        cursor.execute("SELECT * FROM transaction_history")
        tx_count = 0
        for row in cursor.fetchall():
            wallet = Wallet.objects.get(address=row['wallet_address'])
            Transaction.objects.update_or_create(
                wallet=wallet,
                hash=row['hash'],
                defaults={
                    'fungible_id': row['fungible_id'],
                    'symbol': row['symbol'],
                    'date': row['date'],
                    'operation_type': row['operation_type'],
                    'action_type': row['action_type'],
                    'swap_description': row['swap_description'] or '',
                    'contract_address': row['contract_address'],
                    'quantity': row['quantity'],
                    'price_per_token': row['price_per_token'],
                    'total_value_usd': row['total_value_usd'],
                    'direction': row['direction'],
                    'recipient_address': row['recipient_address'] or '',
                    'sender_address': row['sender_address'] or '',
                }
            )
            tx_count += 1
        self.stdout.write(self.style.SUCCESS(f"✓ Migrated {tx_count} transactions"))

        # Migrate token analytics
        self.stdout.write("Migrating token analytics...")
        cursor.execute("SELECT * FROM token_analytics")
        analytics_count = 0
        for row in cursor.fetchall():
            wallet = Wallet.objects.get(address=row['wallet_address'])
            TokenAnalytics.objects.update_or_create(
                wallet=wallet,
                token_symbol=row['token_symbol'],
                defaults={
                    'total_invested': row['total_invested'],
                    'total_realized': row['total_realized'],
                    'roi_percentage': row['roi_percentage'],
                    'is_winning': bool(row['is_winning']),
                    'status': row['status'],
                    'holding_days': row['holding_days'],
                    'in_portfolio': bool(row['in_portfolio']),
                }
            )
            analytics_count += 1
        self.stdout.write(self.style.SUCCESS(f"✓ Migrated {analytics_count} token analytics"))

        # Migrate wallet tier performance
        self.stdout.write("Migrating wallet tier performance...")
        cursor.execute("SELECT * FROM wallet_tier_performance")
        tier_count = 0
        for row in cursor.fetchall():
            wallet = Wallet.objects.get(address=row['wallet_address'])
            WalletTierPerformance.objects.update_or_create(
                wallet=wallet,
                tier_usd=row['tier_usd'],
                defaults={
                    'roi_percentage': row['roi_percentage'],
                    'winrate': row['winrate'],
                    'nb_trades': row['nb_trades'],
                    'nb_gagnants': row['nb_gagnants'],
                    'is_optimal_tier': bool(row['is_optimal_tier']),
                }
            )
            tier_count += 1
        self.stdout.write(self.style.SUCCESS(f"✓ Migrated {tier_count} tier performances"))

        # Migrate wallet qualified
        self.stdout.write("Migrating wallet qualified...")
        cursor.execute("SELECT * FROM wallet_qualified")
        qualified_count = 0
        for row in cursor.fetchall():
            wallet = Wallet.objects.get(address=row['wallet_address'])
            WalletQualified.objects.update_or_create(
                wallet=wallet,
                defaults={
                    'final_score': row['final_score'],
                    'classification': row['classification'],
                    'weighted_roi': row['weighted_roi'],
                    'taux_reussite': row['taux_reussite'],
                    'nb_trades': row['nb_trades'],
                }
            )
            qualified_count += 1
        self.stdout.write(self.style.SUCCESS(f"✓ Migrated {qualified_count} qualified wallets"))

        # Migrate smart wallets
        self.stdout.write("Migrating smart wallets...")
        cursor.execute("SELECT * FROM smart_wallets")
        smart_count = 0
        for row in cursor.fetchall():
            wallet = Wallet.objects.get(address=row['wallet_address'])
            wallet.is_smart_wallet = True
            wallet.save()

            SmartWallet.objects.update_or_create(
                wallet=wallet,
                defaults={
                    'optimal_threshold_tier': row['optimal_threshold_tier'],
                    'quality_score': row['quality_score'],
                    'threshold_status': row['threshold_status'],
                    'optimal_roi': row['optimal_roi'],
                    'optimal_winrate': row['optimal_winrate'],
                    'global_roi': row['global_roi'],
                    'global_winrate': row['global_winrate'],
                }
            )
            smart_count += 1
        self.stdout.write(self.style.SUCCESS(f"✓ Migrated {smart_count} smart wallets"))

        # Migrate wallet brute
        self.stdout.write("Migrating wallet brute...")
        cursor.execute("SELECT * FROM wallet_brute")
        brute_count = 0
        for row in cursor.fetchall():
            WalletBrute.objects.update_or_create(
                wallet_address=row['wallet_address'],
                token_address=row['token_address'],
                temporality=row['temporality'],
                defaults={
                    'contract_address': row['contract_address'],
                    'chain': row['chain'],
                }
            )
            brute_count += 1
        self.stdout.write(self.style.SUCCESS(f"✓ Migrated {brute_count} wallet brute"))

        conn.close()

        self.stdout.write(self.style.SUCCESS("\n🎉 Migration completed successfully!"))
        self.stdout.write(f"Total: {wallets_count} wallets, {tokens_count} tokens, {tx_count} transactions")
```

- [ ] **Step 3: Commit**

```bash
git add backend/wallets/management/
git commit -m "feat(migration): add SQLite to PostgreSQL migration command"
```

---

## Task 10: Run Data Migration

**Files:**
- None (execution only)

- [ ] **Step 1: Copy SQLite database to accessible location**

```bash
# Ensure data/db/wit_database.db is accessible
ls -la data/db/wit_database.db
```

Expected: File exists

- [ ] **Step 2: Run migration command**

```bash
docker-compose run --rm -v $(pwd)/data:/data backend python manage.py migrate_from_sqlite --sqlite-path /data/db/wit_database.db
```

Expected:
```
Migrating wallets...
✓ Migrated X wallets
Migrating tokens...
✓ Migrated Y tokens
...
🎉 Migration completed successfully!
```

- [ ] **Step 3: Verify data in admin**

Open browser: http://localhost:8000/admin

Expected: Wallets, Tokens, Transactions visible with data

- [ ] **Step 4: Verify smart wallets**

In admin, filter Wallets by "is_smart_wallet = Yes"

Expected: Smart wallets visible

- [ ] **Step 5: Test Django ORM queries**

```bash
docker-compose exec backend python manage.py shell
```

```python
from wallets.models import Wallet, SmartWallet
print(f"Total wallets: {Wallet.objects.count()}")
print(f"Smart wallets: {Wallet.objects.filter(is_smart_wallet=True).count()}")
```

Expected: Numbers match SQLite data

- [ ] **Step 6: Commit (empty commit for checkpoint)**

```bash
git commit --allow-empty -m "checkpoint: data migration completed and verified"
```

---

## Validation Checkpoint

Before moving to Phase 2, verify:

- [ ] **Docker containers running**
```bash
docker-compose ps
```
Expected: All 5 services "Up"

- [ ] **PostgreSQL accessible**
```bash
docker-compose exec postgres psql -U wit_user -d wit_database -c "\dt"
```
Expected: List of tables visible

- [ ] **Django admin accessible**
Open: http://localhost:8000/admin
Expected: Can login and see data

- [ ] **All data migrated**
Check counts in admin match SQLite

- [ ] **Tests pass**
```bash
docker-compose exec backend python manage.py test
```
Expected: All tests pass (even if 0 tests for now)

---

## Summary

**Phase 1 completed! You now have:**

✅ Django project with proper structure
✅ 2 Django apps (wallets, notifications)
✅ Docker multi-container setup (5 services)
✅ PostgreSQL database with migrated data
✅ Django Admin functional
✅ All SQLite data migrated to PostgreSQL

**Next phase:** Create API REST endpoints (Phase 2)

**To continue:**
```bash
# Open next plan
cat docs/superpowers/plans/2026-05-31-phase-2-api-rest.md
```
