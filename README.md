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
3. détecte les explosions et les confirme sans attente ;
4. extrait les early buyers EOA significatifs via HyperSync.

**Détection (explosion v2).** Chaque pic local dont la date tombe dans `explosion_window_hours` (aucune fenêtre pour un ajout manuel) reçoit son *dernier creux* : en remontant depuis le pic, le creux recule vers chaque prix plus bas, sauf si une vague précédente a dépassé `breakout_multiplier` × ce prix puis est retombée. Une vague est valable si pic / creux ≥ `min_multiplier`, avec le volume et la liquidité suffisants. Parmi les vagues valables, on retient le meilleur score = multiplicateur × min(1, âge du token au creux ÷ `maturity_hours`) : une vague mature l'emporte sur un pic de lancement. Le token est rejeté si le meilleur score reste sous `min_score` (pump de lancement, raison `low_score`). Une vague au-delà de `max_multiplier` est ignorée, car c'est une anomalie : pool vidé, prix quasi nul. Les dates sont converties en blocs à partir du rythme récent de la chaîne (`find_block_near`).

**Rétention.** Ce n'est plus une attente : la rétention est mesurée dès que le pic a `confirmation_hours` (`pending` → `held` / `rug`). Les acheteurs d'une explosion `rug` pèsent `rug_priority_weight` dans la priorité de qualification.

**Extraction par entité.**
- **Passe 1** (page par page, de `max(création du pool, creux − buyer_window_hours)` au creux) : achats, ventes et envois par couple d'adresses. Chaque destinataire est classé :
  - pool du token → **vente** ;
  - hub (alimenté par au moins `hub_min_senders` expéditeurs : router, exchange, burn), adresse de dépôt (fait suivre ≥ `deposit_forward_pct` % vers un hub en moins de `deposit_forward_hours`), adresse connue, ou petit envoi → **sortie** ;
  - sinon, un envoi d'au moins `vault_min_pct` % de ce que l'expéditeur a reçu → **coffre** : la quantité, le prix de revient (coût moyen) et la date du premier achat passent au coffre, et les deux wallets forment une **entité**.
- **Sélection** : les entités sont classées par position au creux (somme de leurs wallets) ; top `max_buyers` **entités** au-dessus de `min_buy_usd`. Un wallet à plus de `max_txs_per_day` transactions/jour sur les `bot_window_days` jours avant le creux est un **bot** : écarté (trace dans *Acheteurs exclus*), ni lien ni entité.
- **Passe entité** (filtrée sur les wallets retenus, jusqu'au pic) : transferts bruts enregistrés (*Transferts du token*), ventes pendant la montée, coffres découverts pendant la montée suivis sur `vault_follow_depth` niveaux.
- Au-delà de `max_transfers_per_token`, la passe 1 s'arrête et l'explosion est marquée `partial`.

Clés à renseigner dans `.env` : `ENVIO_API_TOKEN`, `ZERION_API_KEY` (et `COINGECKO_API_KEY`, optionnelle).

Tous les réglages sont dans l'admin : **Réglages de détection** (globaux + par chaîne), **Réglages du pipeline**, **Chaînes** (interrupteur), **Tâches périodiques**. Un token repéré ailleurs s'ajoute via **Candidats → Ajouter un token**.

Lancer le pipeline à la main :

    make shell
    >>> from apps.discovery.tasks import run_discovery
    >>> run_discovery.delay()

Tests contre les vraies API : `make test args="-m live integrations"`.

## Qualification des wallets (v2)

Chaque jour à 08:00 UTC (tâche `qualification-daily`) :

1. **Pré-filtre HyperSync (gratuit)** sur les chaînes `prefilter_chains` (Base, Robinhood, BSC, Ethereum, Arc) + la chaîne où le wallet a été repéré : bot, inactif, farmer, MEV, exchange — mesures additionnées.
2. **Historique Zerion 6 mois**, toutes chaînes EVM, pour les survivants : chaque mouvement avec son type, sa quantité, son prix et sa valeur au moment de la transaction. Reprise au curseur le lendemain si le budget est atteint.
3. **Décision sur historique complet** : farmer / MEV revérifiés, liens forts (transfert après achat ≥ `transfer_after_buy_pct`, gros transfert reçu en %) qui créent ou fusionnent les **entités** (mêmes entités que la découverte), mouvements entre wallets d'une entité marqués **internes** (ni achat ni vente), valeur de l'entité (somme des portefeuilles de ses wallets), tags. Le portefeuille est lu **par token** (`/positions/`, un appel) et conservé à chaque passage.
4. **Mise à jour incrémentale** des wallets qualifiés tous les `history_refresh_days` jours.

**Couche brute** : types de transactions Zerion réglables (`zerion_operation_types`, dont `deposit` / `withdraw`), métadonnées de chaque token rencontré (supply, vérifié ; `/fungibles`, rafraîchies tous les `token_info_refresh_days` jours). Une entité se détache dans l'admin (*Wallets → Détacher de son entité*) : le lien n'est plus jamais recréé.

Ordre de traitement : wallets liés (1 appel), historiques en cours, nouveaux wallets par priorité. Budget Zerion : `zerion_daily_budget` (1 800/jour, plan Developer).
