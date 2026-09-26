# Socle technique — Docker, Django, PostgreSQL, Redis, Celery

**Date :** 2026-09-26
**Statut :** validé en brainstorming, en attente de relecture

## Contexte

Wallet Intelligence Tracker (WIT) V1 est un ensemble de scripts Python (~11 000 lignes) orchestrés par `schedule`, stockant leurs données dans SQLite. On repart de zéro : nouvelle architecture, nouveaux services, nouveaux algorithmes. Les données SQLite sont abandonnées.

La refonte est découpée en sous-projets indépendants, chacun avec sa propre spec, son plan et son implémentation :

1. **Socle technique** ← ce document
2. Modèle de données métier
3. Services (clients API externes)
4. Pipelines en tâches Celery
5. Frontend, backtesting, déploiement Dokploy

## Objectif

Poser un socle backend solide qui démarre en une commande, sans aucune logique métier, sur lequel les sous-projets suivants viendront s'ajouter.

## Décisions

| Sujet | Décision |
|---|---|
| Ancien code | Déplacé dans `legacy/`, intact |
| Cible | Dev local uniquement ; structure prête pour un déploiement Dokploy ultérieur |
| Rôle de Django | Backend API REST (DRF) + admin ; frontend séparé plus tard |
| Stack | Python 3.12, Django 5.2 LTS, DRF, Celery 5, PostgreSQL 16, Redis 7 |
| Dépendances | uv (`pyproject.toml` + `uv.lock`) |
| Config | Variables d'environnement via `django-environ` |
| Qualité | pytest-django, ruff |

## Structure du repo

```
Wallet_Intelligence_Tracker/
├── legacy/                      # ancien code V1, intact
├── backend/
│   ├── config/
│   │   ├── settings/
│   │   │   ├── __init__.py
│   │   │   ├── base.py
│   │   │   ├── dev.py
│   │   │   └── prod.py
│   │   ├── __init__.py          # charge l'app Celery
│   │   ├── celery.py
│   │   ├── urls.py
│   │   ├── wsgi.py
│   │   └── asgi.py
│   ├── apps/
│   │   ├── __init__.py
│   │   └── core/
│   ├── tests/                   # conftest partagé
│   ├── docker/entrypoint.sh
│   ├── Dockerfile
│   ├── pyproject.toml
│   ├── uv.lock
│   └── manage.py
├── frontend/
│   └── README.md                # emplacement réservé
├── docs/
├── docker-compose.yml
├── Makefile
├── .env.example
├── .gitignore
└── README.md
```

Tout ce qui concerne Python vit dans `backend/`. La racine ne contient que l'orchestration.

### Convention des apps

Chaque app de `backend/apps/` est autonome et suit la même structure :

```
apps/<app>/
├── __init__.py
├── apps.py              # name = "apps.<app>"
├── models.py
├── serializers.py
├── views.py
├── urls.py              # inclus dans config/urls.py sous /api/<app>/
├── services.py          # logique métier, hors des vues
├── tasks.py             # tâches Celery de l'app
├── admin.py
├── migrations/__init__.py
└── tests/__init__.py
```

`core` suit cette structure et sert de référence. Les fichiers sans contenu utile restent présents (vides ou avec un docstring) pour garder la convention.

## Services Docker Compose (dev)

| Service | Image / build | Commande | Détails |
|---|---|---|---|
| `db` | `postgres:16-alpine` | — | Volume `postgres_data`, healthcheck `pg_isready` |
| `redis` | `redis:7-alpine` | — | Healthcheck `redis-cli ping` |
| `web` | `./backend`, cible `dev` | `python manage.py runserver 0.0.0.0:8000` | Port 8000, migrations au démarrage via l'entrypoint |
| `worker` | `./backend`, cible `dev` | `celery -A config worker -l info` | — |
| `beat` | `./backend`, cible `dev` | `celery -A config beat -l info` | — |

- `web`, `worker` et `beat` partagent la même image et lisent le même `.env`.
- En dev, `./backend` est monté en volume sur `/app` : pas de rebuild à chaque modification.
- Les trois dépendent de `db` et `redis` avec `condition: service_healthy`.

## Configuration

Variables d'environnement (documentées dans `.env.example`) :

| Variable | Exemple dev |
|---|---|
| `DJANGO_SETTINGS_MODULE` | `config.settings.dev` |
| `SECRET_KEY` | `change-me` |
| `DEBUG` | `True` |
| `ALLOWED_HOSTS` | `localhost,127.0.0.1` |
| `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | `wit` / `wit` / `wit` |
| `DATABASE_URL` | `postgres://wit:wit@db:5432/wit` |
| `REDIS_URL` | `redis://redis:6379/0` |

- **`base.py`** : apps installées (Django, DRF, `apps.core`), base de données depuis `DATABASE_URL`, Celery (broker et backend de résultats sur `REDIS_URL`, sérialisation JSON), timezone UTC, logging sur stdout, planning de Beat.
- **`dev.py`** : `DEBUG=True`.
- **`prod.py`** : `DEBUG=False`, `SECURE_*` et cookies sécurisés, WhiteNoise pour les fichiers statiques.

## Celery

- `config/celery.py` crée l'app `config`, lit la configuration Django (namespace `CELERY_`) et appelle `autodiscover_tasks()` pour trouver les `tasks.py` de chaque app.
- `config/__init__.py` importe l'app Celery pour qu'elle soit chargée avec Django.
- Le planning Beat est déclaré dans `CELERY_BEAT_SCHEDULE`, versionné dans le code :
  - `core.tasks.heartbeat` toutes les 60 secondes.

## App `core`

### Health check : `GET /api/core/health/`

- Vue DRF sans authentification.
- La logique vit dans `services.py` : une fonction par check, chacune renvoie `"ok"` ou `"error"` :
  - `database` : exécute `SELECT 1`.
  - `redis` : `PING` sur `REDIS_URL`.
  - `celery` : `control.ping()` avec un timeout d'une seconde ; `ok` si au moins un worker répond.
- Réponse `200` si tous les checks sont `ok`, `503` sinon :

```json
{"status": "ok", "checks": {"database": "ok", "redis": "ok", "celery": "ok"}}
```

En cas d'échec, `status` vaut `"error"`. Aucun détail d'exception n'est renvoyé dans la réponse ; il est écrit dans les logs.

### Tâches

- `core.tasks.ping` renvoie `"pong"`. Elle sert à vérifier la chaîne `web → redis → worker`.
- `core.tasks.heartbeat` écrit une ligne de log, pour vérifier que Beat tourne.

## Dockerfile (`backend/Dockerfile`, multi-stage)

- **`base`** : `python:3.12-slim`, uv copié depuis l'image officielle, `uv sync --frozen --no-dev` dans une couche mise en cache. L'environnement virtuel est dans le `PATH`.
- **`dev`** : `uv sync --frozen`, qui ajoute les dépendances de dev. Le code est monté par le compose.
- **`prod`** : copie le code, lance `collectstatic`, utilisateur non-root, commande par défaut gunicorn sur `config.wsgi`.

`docker/entrypoint.sh` lance `python manage.py migrate --noinput` si la variable `RUN_MIGRATIONS=1` est définie (uniquement pour `web`), puis exécute la commande reçue (`exec "$@"`).

## Makefile (racine)

| Commande | Action |
|---|---|
| `make up` | `docker compose up -d --build` |
| `make down` | `docker compose down` |
| `make logs [s=<service>]` | Suit les logs, d'un service en particulier si `s` est donné |
| `make shell` | `python manage.py shell` dans `web` |
| `make migrate` / `make makemigrations` | Applique ou crée les migrations |
| `make test` | `pytest` dans `web` |
| `make lint` / `make fmt` | `ruff check` / `ruff format` dans `web` |
| `make startapp name=<app>` | Crée `apps/<app>/` selon la convention des apps |

## Tests

- pytest-django, configuration dans `pyproject.toml` (`DJANGO_SETTINGS_MODULE=config.settings.dev`).
- Les tests tournent dans le conteneur `web`, contre le vrai PostgreSQL.
- Cas couverts :
  - Health check : tous les checks OK → `200`.
  - Base, Redis ou worker indisponibles (fonctions de check simulées) → `503` avec le check concerné à `"error"`.
  - Tâche `ping` exécutée en mode eager → `"pong"`.
  - Tâche `heartbeat` → écrit une ligne de log.

## Critères de réussite

1. `cp .env.example .env && make up` démarre les 5 services sans erreur.
2. `curl localhost:8000/api/core/health/` renvoie `200` avec les 3 checks à `ok`.
3. `ping.delay().get(timeout=10)` depuis `make shell` renvoie `"pong"`.
4. Les logs de `beat` et du worker montrent le heartbeat toutes les minutes.
5. `make test` et `make lint` passent.
6. L'admin Django est accessible sur `/admin/` après `createsuperuser`.

## Hors périmètre

- Tables et modèles métier
- Services et clients d'API externes (Zerion, Dune, Etherscan, DexScreener, Telegram…)
- Frontend
- Déploiement Dokploy (compose prod, domaine, HTTPS)
- CI GitHub Actions
