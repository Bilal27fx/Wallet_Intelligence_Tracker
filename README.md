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

Le projet Compose s'appelle `wit` : conteneurs `wit-web-1`, volume `wit_postgres_data`, etc.

## Convention des apps

Chaque app de `backend/apps/<app>/` contient `models.py`, `serializers.py`, `views.py`, `urls.py` (monté sous `/api/<app>/`), `services.py` (logique métier), `tasks.py` (tâches Celery), `admin.py`, `migrations/` et `tests/`. Utiliser `make startapp name=<app>` pour en créer une.

## Configuration

Toute la configuration passe par les variables d'environnement, documentées dans [`.env.example`](.env.example). Le fichier `.env` n'est jamais versionné.
