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

## Qualification des wallets

Chaque jour à 08:00 UTC (tâche `qualification-daily`), après la découverte, chaque early buyer est qualifié en 5 étapes :

1. **Pré-filtre (HyperSync)** : bots (fréquence), wallets inactifs, exchanges connus ;
2. **Historique par token (HyperSync)** : achats, ventes, envois, réceptions sur `history_days`, valorisés par la contrepartie de chaque transaction ; farmers et bots MEV écartés ;
3. **Entités** : transfert après achat, financement initial, financeur commun — les exchanges et dépôts d'exchange ne relient personne ;
4. **Valeur de l'entité** (prix Zerion + solde natif RPC) : entre `min_portfolio_usd` et `max_portfolio_usd` ;
5. **Tags** : SNIPER, EARLY_BUYER, ACCUMULATEUR, FLIPPER, HOLDER.

Zerion ne sert qu'aux prix, sous le budget quotidien `zerion_daily_budget` (Réglages du pipeline). Les seuils sont dans **Réglages de qualification** (globaux + par chaîne) ; les listes d'exchanges s'importent dans **Known addresses → Importer un CSV**.
