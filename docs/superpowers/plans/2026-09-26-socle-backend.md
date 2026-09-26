# Socle technique backend — Plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Un socle Docker Compose (Django + DRF, PostgreSQL, Redis, worker Celery, Celery Beat) qui démarre en une commande, sans logique métier, avec un health check et des tâches de test.

**Architecture:** Monorepo avec l'ancien code dans `legacy/`, le backend Django dans `backend/` et un emplacement réservé `frontend/`. Une seule image Docker multi-stage (`dev` / `prod`) sert `web`, `worker` et `beat`, orchestrés par un `docker-compose.yml` à la racine. Chaque app Django de `backend/apps/` suit une structure standard (models, serializers, views, urls, services, tasks, admin, tests).

**Tech Stack:** Python 3.12, Django 5.2 LTS, Django REST Framework 3.16, Celery 5.5, PostgreSQL 16, Redis 7, uv, django-environ, pytest-django, ruff, gunicorn, WhiteNoise.

**Spec:** `docs/superpowers/specs/2026-09-26-socle-backend-design.md`

## Global Constraints

- Python `>=3.12,<3.13`, image de base `python:3.12-slim`.
- Django `>=5.2,<5.3` (LTS).
- Images `postgres:16-alpine` et `redis:7-alpine`.
- Dépendances gérées par uv uniquement (`backend/pyproject.toml` + `backend/uv.lock`). Pas de `requirements.txt`.
- Toute la config vient des variables d'environnement (`django-environ`). Aucun secret dans le code.
- Timezone `UTC`.
- Toutes les commandes (tests, lint, manage.py) s'exécutent dans le conteneur `web`, via le `Makefile` à la racine.
- Chaque app vit dans `backend/apps/<app>/` avec `apps.py` (`name = "apps.<app>"`), `models.py`, `serializers.py`, `views.py`, `urls.py`, `services.py`, `tasks.py`, `admin.py`, `migrations/`, `tests/`. Ses URLs sont montées sous `/api/<app>/`.
- La logique vit dans `services.py`, jamais dans les vues.
- Le health check ne renvoie jamais le détail d'une exception, seulement `"ok"` ou `"error"`. Le détail va dans les logs.
- Chaque commit se termine par la ligne `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

**Écart assumé avec la spec :** le `conftest.py` partagé est placé à `backend/conftest.py` (et non dans `backend/tests/`) pour que ses fixtures soient visibles aussi par les tests des apps (`backend/apps/*/tests/`). `backend/tests/` contient les tests au niveau du projet.

## Carte des fichiers

| Fichier | Responsabilité |
|---|---|
| `legacy/**` | Ancien code V1, déplacé tel quel |
| `.gitignore` | Règles d'exclusion racine (réécrit) |
| `frontend/README.md` | Emplacement réservé |
| `docker-compose.yml` | Services `db`, `redis`, `web`, `worker`, `beat` |
| `Makefile` | Commandes de dev |
| `.env.example` | Variables d'environnement documentées |
| `README.md` | Documentation racine (réécrit) |
| `backend/pyproject.toml` / `backend/uv.lock` | Dépendances et config pytest / ruff |
| `backend/Dockerfile` | Image multi-stage `base` / `dev` / `prod` |
| `backend/docker/entrypoint.sh` | Migrations optionnelles puis `exec "$@"` |
| `backend/manage.py` | Point d'entrée Django |
| `backend/config/settings/{base,dev,prod}.py` | Settings |
| `backend/config/celery.py` | App Celery |
| `backend/config/__init__.py` | Charge l'app Celery avec Django |
| `backend/config/urls.py` | Routage racine |
| `backend/config/wsgi.py` / `asgi.py` | Serveurs |
| `backend/config/app_template/**` | Template pour `make startapp` |
| `backend/conftest.py` | Fixtures pytest partagées |
| `backend/tests/test_smoke.py` | Tests projet (admin, settings) |
| `backend/apps/core/services.py` | Checks de santé |
| `backend/apps/core/tasks.py` | Tâches `ping` et `heartbeat` |
| `backend/apps/core/views.py` / `urls.py` | Endpoint `/api/core/health/` |

---

### Task 1: Déplacer la V1 dans `legacy/`

**Files:**
- Move: `assets/`, `config/`, `data/`, `db/`, `scripts/`, `smart_wallet_analysis/`, `sql_queries/`, `README.md`, `requirements.txt`, `run_pipelines.py`, `test.py` → `legacy/`
- Modify: `.gitignore` (réécriture complète)
- Create: `frontend/README.md`

**Interfaces:**
- Consumes: rien
- Produces: une racine libre pour `backend/`, `frontend/`, `docker-compose.yml`, `Makefile`

- [ ] **Step 1: Déplacer l'ancien code avec git (historique conservé)**

```bash
mkdir legacy
git mv assets config data db scripts smart_wallet_analysis sql_queries README.md requirements.txt run_pipelines.py test.py legacy/
```

`git mv` d'un dossier déplace aussi les fichiers ignorés qu'il contient (`data/cache`, `data/db`) sur le disque.

- [ ] **Step 2: Réécrire `.gitignore`**

Les anciennes règles du type `data/cache/` sont ancrées à la racine : après le déplacement, elles ne matcheraient plus. Contenu complet de `.gitignore` :

```gitignore
# Secrets
.env
**/.env

# Python
__pycache__/
*.py[cod]
.venv/
venv/
.pytest_cache/
.ruff_cache/

# Django
backend/staticfiles/
backend/media/

# IDE / OS
.vscode/
.idea/
*.swp
.DS_Store
Thumbs.db
.claude/
CLAUDE.md
CODEX.md

# Logs & temp
*.log
*.tmp
*.temp

# Legacy V1 : données locales
*.db
*.db-shm
*.db-wal
*.sqbpro
*.pkl
legacy/data/db/
legacy/data/cache/
legacy/data/backtesting/
legacy/data/raw/csv/
legacy/data/processed/
**/chromedriver*
```

- [ ] **Step 3: Créer `frontend/README.md`**

```markdown
# Frontend

Emplacement réservé. La techno (Next.js, Vite + React…) sera choisie dans un cycle ultérieur, avec son propre service dans `docker-compose.yml`.
```

- [ ] **Step 4: Vérifier que rien d'ignoré n'apparaît**

Run: `git status --short | grep -v '^R ' `
Expected: seulement `M  .gitignore` (ou ` M`) et `?? frontend/`. Aucun fichier de `legacy/data/cache` ni `legacy/data/db`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore: déplace la V1 dans legacy/ et prépare la nouvelle structure

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Projet Django et stack Docker

**Files:**
- Create: `backend/pyproject.toml`, `backend/uv.lock` (généré), `backend/manage.py`
- Create: `backend/config/__init__.py`, `backend/config/celery.py`, `backend/config/urls.py`, `backend/config/wsgi.py`, `backend/config/asgi.py`
- Create: `backend/config/settings/__init__.py`, `backend/config/settings/base.py`, `backend/config/settings/dev.py`, `backend/config/settings/prod.py`
- Create: `backend/apps/__init__.py`
- Create: `backend/Dockerfile`, `backend/docker/entrypoint.sh`, `backend/.dockerignore`
- Create: `docker-compose.yml`, `.env.example`, `Makefile`
- Create: `backend/conftest.py`, `backend/tests/__init__.py`, `backend/tests/test_smoke.py`

**Interfaces:**
- Consumes: la racine libérée par la Task 1
- Produces:
  - `config.celery.app` : instance `celery.Celery` nommée `"config"`
  - `settings.REDIS_URL: str`
  - `settings.CELERY_BEAT_SCHEDULE: dict` (vide ici, rempli en Task 3)
  - Fixture pytest `api_client` → `rest_framework.test.APIClient`
  - Cibles make : `up`, `down`, `logs`, `shell`, `migrate`, `makemigrations`, `test`, `lint`, `fmt`

- [ ] **Step 1: Écrire `backend/pyproject.toml`**

```toml
[project]
name = "wit-backend"
version = "0.1.0"
description = "Wallet Intelligence Tracker — backend"
requires-python = ">=3.12,<3.13"
dependencies = [
    "django>=5.2,<5.3",
    "djangorestframework>=3.16,<4",
    "django-environ>=0.12,<1",
    "psycopg[binary]>=3.2,<4",
    "celery[redis]>=5.5,<6",
    "redis",
    "gunicorn>=23,<24",
    "whitenoise>=6.9,<7",
]

[dependency-groups]
dev = [
    "pytest>=8.3,<9",
    "pytest-django>=4.11,<5",
    "ruff>=0.12,<1",
]

[tool.uv]
package = false

[tool.pytest.ini_options]
DJANGO_SETTINGS_MODULE = "config.settings.dev"
python_files = ["test_*.py"]
testpaths = ["tests", "apps"]
addopts = "-ra"

[tool.ruff]
line-length = 100
target-version = "py312"
extend-exclude = ["**/migrations/**"]

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "DJ"]

[tool.ruff.lint.per-file-ignores]
"config/settings/*.py" = ["F403", "F405"]
```

- [ ] **Step 2: Générer `backend/uv.lock` dans un conteneur (uv n'est pas installé en local)**

Run:
```bash
docker run --rm -v "$PWD/backend:/app" -w /app ghcr.io/astral-sh/uv:python3.12-bookworm-slim uv lock
```
Expected: `Resolved N packages`, fichier `backend/uv.lock` créé.

- [ ] **Step 3: Écrire les settings**

`backend/config/settings/__init__.py` : fichier vide.

`backend/config/settings/base.py` :

```python
"""Settings communs à tous les environnements."""

from pathlib import Path

import environ

BASE_DIR = Path(__file__).resolve().parent.parent.parent

env = environ.Env(DEBUG=(bool, False))

SECRET_KEY = env("SECRET_KEY")
DEBUG = env("DEBUG")
ALLOWED_HOSTS = env.list("ALLOWED_HOSTS", default=[])

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"
ASGI_APPLICATION = "config.asgi.application"

DATABASES = {"default": env.db("DATABASE_URL")}
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

LANGUAGE_CODE = "fr-fr"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "staticfiles"

# Redis / Celery
REDIS_URL = env("REDIS_URL")

CELERY_BROKER_URL = REDIS_URL
CELERY_RESULT_BACKEND = REDIS_URL
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TIMEZONE = TIME_ZONE
CELERY_BEAT_SCHEDULE: dict = {}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {"format": "%(asctime)s %(levelname)s %(name)s %(message)s"},
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "formatter": "default",
        },
    },
    "root": {"handlers": ["console"], "level": env("LOG_LEVEL", default="INFO")},
}
```

`backend/config/settings/dev.py` :

```python
"""Settings de développement local."""

from .base import *

DEBUG = True
```

`backend/config/settings/prod.py` :

```python
"""Settings de production (Dokploy)."""

from .base import *
from .base import MIDDLEWARE, env

DEBUG = False

MIDDLEWARE = [MIDDLEWARE[0], "whitenoise.middleware.WhiteNoiseMiddleware", *MIDDLEWARE[1:]]

STORAGES = {
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
    "staticfiles": {"BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage"},
}

SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
SESSION_COOKIE_SECURE = True
CSRF_COOKIE_SECURE = True
SECURE_CONTENT_TYPE_NOSNIFF = True
CSRF_TRUSTED_ORIGINS = env.list("CSRF_TRUSTED_ORIGINS", default=[])
```

- [ ] **Step 4: Écrire Celery, URLs, WSGI/ASGI et `manage.py`**

`backend/config/celery.py` :

```python
"""App Celery du projet : découvre automatiquement les tasks.py de chaque app."""

import os

from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")

app = Celery("config")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()
```

`backend/config/__init__.py` :

```python
from .celery import app as celery_app

__all__ = ("celery_app",)
```

`backend/config/urls.py` :

```python
from django.contrib import admin
from django.urls import path

urlpatterns = [
    path("admin/", admin.site.urls),
]
```

`backend/config/wsgi.py` :

```python
import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")

application = get_wsgi_application()
```

`backend/config/asgi.py` :

```python
import os

from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")

application = get_asgi_application()
```

`backend/manage.py` :

```python
#!/usr/bin/env python
"""Utilitaire en ligne de commande de Django."""

import os
import sys


def main():
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
```

`backend/apps/__init__.py` : fichier vide.

- [ ] **Step 5: Écrire l'image Docker**

`backend/Dockerfile` :

```dockerfile
FROM python:3.12-slim AS base

COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /uvx /bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]


FROM base AS dev

RUN uv sync --frozen
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]


FROM base AS prod

COPY . .
RUN SECRET_KEY=build \
    DATABASE_URL=sqlite:////tmp/build.db \
    REDIS_URL=redis://localhost:6379/0 \
    DJANGO_SETTINGS_MODULE=config.settings.prod \
    python manage.py collectstatic --noinput
RUN useradd --create-home app && chown -R app:app /app
USER app
CMD ["gunicorn", "config.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "3"]
```

Le venv vit dans `/opt/venv` : le montage de `./backend` sur `/app` en dev ne le masque pas.

`backend/docker/entrypoint.sh` :

```sh
#!/bin/sh
set -e

if [ "$RUN_MIGRATIONS" = "1" ]; then
    python manage.py migrate --noinput
fi

exec "$@"
```

`backend/.dockerignore` :

```
.venv
__pycache__
*.pyc
.pytest_cache
.ruff_cache
staticfiles
.env
```

- [ ] **Step 6: Écrire `docker-compose.yml`, `.env.example` et le `Makefile` (racine)**

`docker-compose.yml` :

```yaml
x-backend: &backend
  build:
    context: ./backend
    target: dev
  env_file: .env
  volumes:
    - ./backend:/app
  depends_on:
    db:
      condition: service_healthy
    redis:
      condition: service_healthy

services:
  db:
    image: postgres:16-alpine
    env_file: .env
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}"]
      interval: 5s
      timeout: 5s
      retries: 10

  redis:
    image: redis:7-alpine
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 10

  web:
    <<: *backend
    command: python manage.py runserver 0.0.0.0:8000
    environment:
      RUN_MIGRATIONS: "1"
    ports:
      - "8000:8000"

  worker:
    <<: *backend
    command: celery -A config worker -l info

  beat:
    <<: *backend
    command: celery -A config beat -l info --schedule /tmp/celerybeat-schedule

volumes:
  postgres_data:
```

`--schedule /tmp/...` évite que Beat écrive son fichier d'état dans `./backend` (monté depuis l'hôte).

`.env.example` :

```dotenv
# Django
DJANGO_SETTINGS_MODULE=config.settings.dev
SECRET_KEY=change-me
DEBUG=True
ALLOWED_HOSTS=localhost,127.0.0.1
LOG_LEVEL=INFO

# PostgreSQL
POSTGRES_DB=wit
POSTGRES_USER=wit
POSTGRES_PASSWORD=wit
DATABASE_URL=postgres://wit:wit@db:5432/wit

# Redis
REDIS_URL=redis://redis:6379/0
```

`Makefile` (les lignes de recette commencent par une **tabulation**) :

```make
COMPOSE := docker compose
EXEC := $(COMPOSE) exec web

.PHONY: up down logs shell migrate makemigrations test lint fmt

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f $(s)

shell:
	$(EXEC) python manage.py shell

migrate:
	$(EXEC) python manage.py migrate

makemigrations:
	$(EXEC) python manage.py makemigrations

test:
	$(EXEC) pytest $(args)

lint:
	$(EXEC) ruff check .
	$(EXEC) ruff format --check .

fmt:
	$(EXEC) ruff check --fix .
	$(EXEC) ruff format .
```

- [ ] **Step 7: Écrire les tests de fumée**

`backend/conftest.py` :

```python
import pytest
from rest_framework.test import APIClient


@pytest.fixture
def api_client():
    return APIClient()
```

`backend/tests/__init__.py` : fichier vide.

`backend/tests/test_smoke.py` :

```python
import pytest
from django.conf import settings


def test_timezone_is_utc():
    assert settings.TIME_ZONE == "UTC"
    assert settings.USE_TZ is True


def test_database_is_postgresql():
    assert settings.DATABASES["default"]["ENGINE"] == "django.db.backends.postgresql"


@pytest.mark.django_db
def test_admin_redirects_anonymous_to_login(client):
    response = client.get("/admin/")
    assert response.status_code == 302
    assert "/admin/login/" in response["Location"]
```

- [ ] **Step 8: Démarrer la stack**

Run:
```bash
cp .env.example .env
make up
docker compose ps
```
Expected: `db` et `redis` en `healthy`, `web`, `worker` et `beat` en `running`. `make logs s=web` montre `Applying auth.0001_initial... OK` puis `Starting development server at http://0.0.0.0:8000/`.

- [ ] **Step 9: Lancer les tests et le lint**

Run: `make test`
Expected: `3 passed`

Run: `make lint`
Expected: `All checks passed!` et `N files already formatted`. Si le format échoue, lancer `make fmt` puis relancer `make lint`.

- [ ] **Step 10: Vérifier que la cible prod build**

Run: `docker build --target prod -t wit-backend:prod ./backend`
Expected: build réussi, l'étape `collectstatic` affiche `N static files copied`.

- [ ] **Step 11: Commit**

```bash
git add backend docker-compose.yml .env.example Makefile
git commit -m "feat: socle Django + DRF, PostgreSQL, Redis et Celery sous Docker Compose

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Vérifier que `.env` n'est **pas** dans le commit : `git show --stat HEAD | grep -c '\.env$'` doit afficher `0`.

---

### Task 3: App `core` et tâches Celery `ping` / `heartbeat`

**Files:**
- Create: `backend/apps/core/__init__.py`, `apps.py`, `models.py`, `serializers.py`, `views.py`, `urls.py`, `services.py`, `tasks.py`, `admin.py`, `migrations/__init__.py`, `tests/__init__.py`, `tests/test_tasks.py`
- Modify: `backend/config/settings/base.py` (INSTALLED_APPS, CELERY_BEAT_SCHEDULE)

**Interfaces:**
- Consumes: `config.celery.app` (Task 2), fixture pytest `caplog`
- Produces:
  - `apps.core.tasks.ping() -> str` (renvoie `"pong"`), nom Celery `apps.core.tasks.ping`
  - `apps.core.tasks.heartbeat() -> None` (log `"heartbeat"` au niveau INFO sur le logger `apps.core.tasks`), nom Celery `apps.core.tasks.heartbeat`
  - Entrée Beat `"core-heartbeat"` toutes les 60 s

- [ ] **Step 1: Créer le squelette de l'app**

`backend/apps/core/__init__.py`, `backend/apps/core/migrations/__init__.py`, `backend/apps/core/tests/__init__.py` : fichiers vides.

`backend/apps/core/apps.py` :

```python
from django.apps import AppConfig


class CoreConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.core"
```

`backend/apps/core/models.py` :

```python
"""Modèles de l'app core (aucun pour l'instant)."""
```

`backend/apps/core/serializers.py` :

```python
"""Serializers de l'app core (aucun pour l'instant)."""
```

`backend/apps/core/admin.py` :

```python
"""Admin de l'app core (aucun modèle enregistré)."""
```

`backend/apps/core/views.py` :

```python
"""Vues de l'app core."""
```

`backend/apps/core/urls.py` :

```python
app_name = "core"

urlpatterns: list = []
```

`backend/apps/core/services.py` :

```python
"""Logique métier de l'app core."""
```

Dans `backend/config/settings/base.py`, ajouter l'app à la fin de `INSTALLED_APPS` :

```python
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "apps.core",
]
```

- [ ] **Step 2: Écrire les tests qui échouent**

`backend/apps/core/tests/test_tasks.py` :

```python
import logging

from django.conf import settings

from apps.core.tasks import heartbeat, ping


def test_ping_returns_pong():
    result = ping.apply()
    assert result.get() == "pong"


def test_heartbeat_logs(caplog):
    with caplog.at_level(logging.INFO, logger="apps.core.tasks"):
        heartbeat.apply()
    assert "heartbeat" in caplog.text


def test_heartbeat_is_scheduled_every_minute():
    entry = settings.CELERY_BEAT_SCHEDULE["core-heartbeat"]
    assert entry["task"] == "apps.core.tasks.heartbeat"
    assert entry["schedule"] == 60.0
```

`.apply()` exécute la tâche localement, sans passer par Redis ni le worker.

- [ ] **Step 3: Vérifier que les tests échouent**

Run: `make test args=apps/core/tests/test_tasks.py`
Expected: FAIL à la collecte, `ModuleNotFoundError: No module named 'apps.core.tasks'`

- [ ] **Step 4: Implémenter les tâches et le planning Beat**

`backend/apps/core/tasks.py` :

```python
"""Tâches Celery de l'app core."""

import logging

from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task
def ping() -> str:
    """Vérifie la chaîne web → redis → worker."""
    return "pong"


@shared_task
def heartbeat() -> None:
    """Prouve que Beat planifie bien les tâches."""
    logger.info("heartbeat")
```

Dans `backend/config/settings/base.py`, remplacer `CELERY_BEAT_SCHEDULE: dict = {}` par :

```python
CELERY_BEAT_SCHEDULE = {
    "core-heartbeat": {
        "task": "apps.core.tasks.heartbeat",
        "schedule": 60.0,
    },
}
```

- [ ] **Step 5: Vérifier que les tests passent**

Run: `make test args=apps/core/tests/test_tasks.py`
Expected: `3 passed`

- [ ] **Step 6: Vérifier la chaîne réelle web → redis → worker**

Run:
```bash
docker compose restart worker beat
docker compose exec web python manage.py shell -c "from apps.core.tasks import ping; print(ping.delay().get(timeout=10))"
```
Expected: `pong`

Run (après au moins une minute) : `docker compose logs worker | grep heartbeat`
Expected: au moins une ligne `INFO apps.core.tasks heartbeat`.

- [ ] **Step 7: Lint et commit**

Run: `make lint`
Expected: aucune erreur.

```bash
git add backend/apps/core backend/config/settings/base.py
git commit -m "feat(core): app core avec tâches Celery ping et heartbeat

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Services de health check

**Files:**
- Modify: `backend/apps/core/services.py`
- Create: `backend/apps/core/tests/test_services.py`

**Interfaces:**
- Consumes: `settings.REDIS_URL` et `config.celery.app` (Task 2)
- Produces (dans `apps.core.services`) :
  - Constantes `OK = "ok"`, `ERROR = "error"`
  - `check_database() -> str`
  - `check_redis() -> str`
  - `check_celery() -> str`
  - `run_health_checks() -> dict` au format `{"status": "ok"|"error", "checks": {"database": str, "redis": str, "celery": str}}`. Appelle les trois checks **par leur nom dans le module** à chaque appel, pour qu'un `patch("apps.core.services.check_xxx")` fonctionne.

- [ ] **Step 1: Écrire les tests qui échouent**

`backend/apps/core/tests/test_services.py` :

```python
from unittest.mock import patch

import pytest

from apps.core import services


@pytest.mark.django_db
def test_check_database_ok():
    assert services.check_database() == services.OK


def test_check_database_error():
    with patch("apps.core.services.connection.cursor", side_effect=Exception("down")):
        assert services.check_database() == services.ERROR


def test_check_redis_ok():
    assert services.check_redis() == services.OK


def test_check_redis_error(settings):
    settings.REDIS_URL = "redis://localhost:1/0"
    assert services.check_redis() == services.ERROR


def test_check_celery_ok_when_a_worker_replies():
    with patch.object(services.celery_app.control, "ping", return_value=[{"w1": {"ok": "pong"}}]):
        assert services.check_celery() == services.OK


def test_check_celery_error_when_no_worker():
    with patch.object(services.celery_app.control, "ping", return_value=[]):
        assert services.check_celery() == services.ERROR


def test_check_celery_error_when_broker_fails():
    with patch.object(services.celery_app.control, "ping", side_effect=Exception("down")):
        assert services.check_celery() == services.ERROR


def test_run_health_checks_all_ok():
    with (
        patch("apps.core.services.check_database", return_value=services.OK),
        patch("apps.core.services.check_redis", return_value=services.OK),
        patch("apps.core.services.check_celery", return_value=services.OK),
    ):
        assert services.run_health_checks() == {
            "status": "ok",
            "checks": {"database": "ok", "redis": "ok", "celery": "ok"},
        }


def test_run_health_checks_one_failure():
    with (
        patch("apps.core.services.check_database", return_value=services.OK),
        patch("apps.core.services.check_redis", return_value=services.ERROR),
        patch("apps.core.services.check_celery", return_value=services.OK),
    ):
        assert services.run_health_checks() == {
            "status": "error",
            "checks": {"database": "ok", "redis": "error", "celery": "ok"},
        }
```

`test_check_redis_ok` utilise le vrai Redis du compose : c'est volontaire, les tests tournent dans la stack.

- [ ] **Step 2: Vérifier que les tests échouent**

Run: `make test args=apps/core/tests/test_services.py`
Expected: FAIL, `AttributeError: module 'apps.core.services' has no attribute 'check_database'`

- [ ] **Step 3: Implémenter les services**

`backend/apps/core/services.py` (contenu complet) :

```python
"""Checks de santé de l'infrastructure : base de données, Redis, workers Celery."""

import logging

import redis
from django.conf import settings
from django.db import connection

from config.celery import app as celery_app

logger = logging.getLogger(__name__)

OK = "ok"
ERROR = "error"


def check_database() -> str:
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
    except Exception:
        logger.exception("Health check database en échec")
        return ERROR
    return OK


def check_redis() -> str:
    try:
        client = redis.Redis.from_url(
            settings.REDIS_URL, socket_connect_timeout=1, socket_timeout=1
        )
        client.ping()
    except Exception:
        logger.exception("Health check redis en échec")
        return ERROR
    return OK


def check_celery() -> str:
    try:
        replies = celery_app.control.ping(timeout=1.0)
    except Exception:
        logger.exception("Health check celery en échec")
        return ERROR
    if not replies:
        logger.error("Health check celery : aucun worker n'a répondu")
        return ERROR
    return OK


def run_health_checks() -> dict:
    checks = {
        "database": check_database(),
        "redis": check_redis(),
        "celery": check_celery(),
    }
    status = OK if all(value == OK for value in checks.values()) else ERROR
    return {"status": status, "checks": checks}
```

- [ ] **Step 4: Vérifier que les tests passent**

Run: `make test args=apps/core/tests/test_services.py`
Expected: `9 passed`

- [ ] **Step 5: Lint et commit**

Run: `make lint`
Expected: aucune erreur.

```bash
git add backend/apps/core/services.py backend/apps/core/tests/test_services.py
git commit -m "feat(core): services de health check (database, redis, celery)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Endpoint `GET /api/core/health/`

**Files:**
- Modify: `backend/apps/core/views.py`, `backend/apps/core/urls.py`, `backend/config/urls.py`
- Create: `backend/apps/core/tests/test_views.py`

**Interfaces:**
- Consumes: `apps.core.services.run_health_checks`, `OK`, `ERROR`, `check_database`, `check_redis`, `check_celery` (Task 4) ; fixture `api_client` (Task 2)
- Produces:
  - `apps.core.views.HealthView` (DRF `APIView`, sans authentification)
  - Route nommée `core:health` → `/api/core/health/`

- [ ] **Step 1: Écrire les tests qui échouent**

`backend/apps/core/tests/test_views.py` :

```python
from unittest.mock import patch

from django.urls import reverse

from apps.core import services


def _patch_checks(database, redis, celery):
    return (
        patch("apps.core.services.check_database", return_value=database),
        patch("apps.core.services.check_redis", return_value=redis),
        patch("apps.core.services.check_celery", return_value=celery),
    )


def test_health_url():
    assert reverse("core:health") == "/api/core/health/"


def test_health_returns_200_when_all_ok(api_client):
    db, rd, cl = _patch_checks(services.OK, services.OK, services.OK)
    with db, rd, cl:
        response = api_client.get("/api/core/health/")
    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "checks": {"database": "ok", "redis": "ok", "celery": "ok"},
    }


def test_health_returns_503_when_database_down(api_client):
    db, rd, cl = _patch_checks(services.ERROR, services.OK, services.OK)
    with db, rd, cl:
        response = api_client.get("/api/core/health/")
    assert response.status_code == 503
    assert response.json()["checks"]["database"] == "error"


def test_health_returns_503_when_redis_down(api_client):
    db, rd, cl = _patch_checks(services.OK, services.ERROR, services.OK)
    with db, rd, cl:
        response = api_client.get("/api/core/health/")
    assert response.status_code == 503
    assert response.json()["checks"]["redis"] == "error"


def test_health_returns_503_when_no_worker(api_client):
    db, rd, cl = _patch_checks(services.OK, services.OK, services.ERROR)
    with db, rd, cl:
        response = api_client.get("/api/core/health/")
    assert response.status_code == 503
    assert response.json() == {
        "status": "error",
        "checks": {"database": "ok", "redis": "ok", "celery": "error"},
    }
```

- [ ] **Step 2: Vérifier que les tests échouent**

Run: `make test args=apps/core/tests/test_views.py`
Expected: FAIL, `NoReverseMatch: 'core' is not a registered namespace`

- [ ] **Step 3: Implémenter la vue et le routage**

`backend/apps/core/views.py` (contenu complet) :

```python
"""Vues de l'app core."""

from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core import services


class HealthView(APIView):
    """Etat de l'infrastructure. 200 si tout est OK, 503 sinon."""

    authentication_classes: list = []
    permission_classes = [AllowAny]

    def get(self, request):
        result = services.run_health_checks()
        code = (
            status.HTTP_200_OK
            if result["status"] == services.OK
            else status.HTTP_503_SERVICE_UNAVAILABLE
        )
        return Response(result, status=code)
```

`backend/apps/core/urls.py` (contenu complet) :

```python
from django.urls import path

from apps.core.views import HealthView

app_name = "core"

urlpatterns = [
    path("health/", HealthView.as_view(), name="health"),
]
```

`backend/config/urls.py` (contenu complet) :

```python
from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/core/", include("apps.core.urls")),
]
```

- [ ] **Step 4: Vérifier que les tests passent**

Run: `make test args=apps/core/tests/test_views.py`
Expected: `5 passed`

- [ ] **Step 5: Vérifier l'endpoint réel**

Run: `curl -s -w '\n%{http_code}\n' localhost:8000/api/core/health/`
Expected:
```
{"status":"ok","checks":{"database":"ok","redis":"ok","celery":"ok"}}
200
```

Run: `docker compose stop worker && curl -s -w '\n%{http_code}\n' localhost:8000/api/core/health/ ; docker compose start worker`
Expected: `"celery":"error"` et `503`.

- [ ] **Step 6: Lint et commit**

Run: `make lint`
Expected: aucune erreur.

```bash
git add backend/apps/core backend/config/urls.py
git commit -m "feat(core): endpoint GET /api/core/health/

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Template d'app et `make startapp`

**Files:**
- Create: `backend/config/app_template/__init__.py-tpl`, `apps.py-tpl`, `models.py-tpl`, `serializers.py-tpl`, `views.py-tpl`, `urls.py-tpl`, `services.py-tpl`, `tasks.py-tpl`, `admin.py-tpl`, `migrations/__init__.py-tpl`, `tests/__init__.py-tpl`
- Create: `backend/tests/test_app_template.py`
- Modify: `Makefile`

**Interfaces:**
- Consumes: la commande Django `startapp --template` ; `settings.BASE_DIR` (Task 2)
- Produces: cible `make startapp name=<app>` qui crée `backend/apps/<app>/` selon la convention

Django renomme les fichiers `*.py-tpl` en `*.py` et remplace `{{ app_name }}` et `{{ camel_case_app_name }}`.

- [ ] **Step 1: Écrire le test qui échoue**

`backend/tests/test_app_template.py` :

```python
from django.conf import settings
from django.core.management import call_command

TEMPLATE = settings.BASE_DIR / "config" / "app_template"

EXPECTED_FILES = [
    "__init__.py",
    "apps.py",
    "models.py",
    "serializers.py",
    "views.py",
    "urls.py",
    "services.py",
    "tasks.py",
    "admin.py",
    "migrations/__init__.py",
    "tests/__init__.py",
]


def test_startapp_template_creates_standard_structure(tmp_path):
    target = tmp_path / "wallets"
    target.mkdir()

    call_command("startapp", "wallets", str(target), template=str(TEMPLATE))

    for relative in EXPECTED_FILES:
        assert (target / relative).is_file(), f"{relative} manquant"

    apps_py = (target / "apps.py").read_text()
    assert "class WalletsConfig(AppConfig):" in apps_py
    assert 'name = "apps.wallets"' in apps_py

    urls_py = (target / "urls.py").read_text()
    assert 'app_name = "wallets"' in urls_py
```

- [ ] **Step 2: Vérifier que le test échoue**

Run: `make test args=tests/test_app_template.py`
Expected: FAIL, `CommandError: ... app_template ... does not exist`

- [ ] **Step 3: Écrire le template**

`backend/config/app_template/__init__.py-tpl`, `backend/config/app_template/migrations/__init__.py-tpl`, `backend/config/app_template/tests/__init__.py-tpl` : fichiers vides.

`backend/config/app_template/apps.py-tpl` :

```python
from django.apps import AppConfig


class {{ camel_case_app_name }}Config(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.{{ app_name }}"
```

`backend/config/app_template/models.py-tpl` :

```python
"""Modèles de l'app {{ app_name }}."""
```

`backend/config/app_template/serializers.py-tpl` :

```python
"""Serializers de l'app {{ app_name }}."""
```

`backend/config/app_template/views.py-tpl` :

```python
"""Vues de l'app {{ app_name }}."""
```

`backend/config/app_template/urls.py-tpl` :

```python
app_name = "{{ app_name }}"

urlpatterns: list = []
```

`backend/config/app_template/services.py-tpl` :

```python
"""Logique métier de l'app {{ app_name }}."""
```

`backend/config/app_template/tasks.py-tpl` :

```python
"""Tâches Celery de l'app {{ app_name }}."""
```

`backend/config/app_template/admin.py-tpl` :

```python
"""Admin de l'app {{ app_name }}."""
```

- [ ] **Step 4: Vérifier que le test passe**

Run: `make test args=tests/test_app_template.py`
Expected: `1 passed`

- [ ] **Step 5: Ajouter la cible `startapp` au `Makefile`**

Ajouter `startapp` à la ligne `.PHONY` :

```make
.PHONY: up down logs shell migrate makemigrations test lint fmt startapp
```

Ajouter à la fin du `Makefile` (tabulations en début de recette) :

```make
startapp:
	@test -n "$(name)" || (echo "Usage: make startapp name=<app>" && exit 1)
	$(EXEC) sh -c "mkdir -p apps/$(name) && python manage.py startapp --template config/app_template $(name) apps/$(name)"
	@echo "Ensuite : ajouter \"apps.$(name)\" à INSTALLED_APPS et path(\"api/$(name)/\", include(\"apps.$(name).urls\")) à config/urls.py"
```

- [ ] **Step 6: Vérifier la cible pour de vrai, puis nettoyer**

Run:
```bash
make startapp name=demo
ls backend/apps/demo backend/apps/demo/migrations backend/apps/demo/tests
rm -rf backend/apps/demo
make startapp
```
Expected: `demo` contient les 9 fichiers + `migrations/__init__.py` + `tests/__init__.py`. Le dernier appel sans `name` affiche `Usage: make startapp name=<app>` et échoue.

- [ ] **Step 7: Lint et commit**

Run: `make lint`
Expected: aucune erreur (les `.py-tpl` ne sont pas lintés).

```bash
git add backend/config/app_template backend/tests/test_app_template.py Makefile
git commit -m "feat: template d'app standard et commande make startapp

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: README et vérification finale

**Files:**
- Create: `README.md` (racine)

**Interfaces:**
- Consumes: tout ce qui précède
- Produces: documentation de démarrage

- [ ] **Step 1: Écrire `README.md`**

````markdown
# Wallet Intelligence Tracker

Identifie, analyse et suit en temps réel les wallets crypto les plus performants.

> Refonte en cours. L'ancienne version (V1, scripts Python + SQLite) est conservée dans [`legacy/`](legacy/).

## Stack

- **Backend** : Python 3.12, Django 5.2, Django REST Framework
- **Base de données** : PostgreSQL 16
- **Tâches asynchrones** : Celery 5 (worker + Beat), Redis 7
- **Outillage** : Docker Compose, uv, pytest, ruff

## Structure

```
backend/     API Django, tâches Celery
frontend/    réservé (cycle ultérieur)
legacy/      ancienne version V1
docs/        specs et plans
```

## Démarrage

Prérequis : Docker et Docker Compose.

```bash
cp .env.example .env
make up
curl localhost:8000/api/core/health/
```

Réponse attendue :

```json
{"status": "ok", "checks": {"database": "ok", "redis": "ok", "celery": "ok"}}
```

Admin Django : créer un compte avec `docker compose exec web python manage.py createsuperuser`, puis ouvrir http://localhost:8000/admin/.

## Commandes

| Commande | Action |
|---|---|
| `make up` / `make down` | Démarre / arrête la stack |
| `make logs [s=worker]` | Suit les logs (d'un service si `s` est donné) |
| `make shell` | Shell Django |
| `make migrate` / `make makemigrations` | Migrations |
| `make test [args=...]` | Tests pytest |
| `make lint` / `make fmt` | Lint / formatage (ruff) |
| `make startapp name=<app>` | Crée une app dans `backend/apps/` |

## Services

| Service | Rôle |
|---|---|
| `db` | PostgreSQL |
| `redis` | Broker et backend de résultats Celery |
| `web` | Django (`runserver` en dev), port 8000 |
| `worker` | Worker Celery |
| `beat` | Planificateur Celery |

## Convention des apps

Chaque app de `backend/apps/<app>/` contient `models.py`, `serializers.py`, `views.py`, `urls.py` (monté sous `/api/<app>/`), `services.py` (logique métier), `tasks.py` (tâches Celery), `admin.py`, `migrations/` et `tests/`. Utiliser `make startapp name=<app>` pour en créer une.

## Configuration

Toute la configuration passe par les variables d'environnement, documentées dans [`.env.example`](.env.example). Le fichier `.env` n'est jamais versionné.
````

- [ ] **Step 2: Vérification complète depuis zéro**

Run:
```bash
make down
docker compose down -v
make up
docker compose ps
```
Expected: les 5 services démarrent, `db` et `redis` en `healthy`.

Vérifier chaque critère de réussite de la spec :

| # | Commande | Attendu |
|---|---|---|
| 1 | `docker compose ps` | 5 services up |
| 2 | `curl -s -w '\n%{http_code}\n' localhost:8000/api/core/health/` | les 3 checks à `ok`, `200` |
| 3 | `docker compose exec web python manage.py shell -c "from apps.core.tasks import ping; print(ping.delay().get(timeout=10))"` | `pong` |
| 4 | Attendre 70 s puis `docker compose logs worker \| grep heartbeat` | au moins une ligne |
| 5 | `make test && make lint` | tous les tests passent (21), lint OK |
| 6 | `curl -s -o /dev/null -w '%{http_code}\n' localhost:8000/admin/` | `302` |

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: README du nouveau socle

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
