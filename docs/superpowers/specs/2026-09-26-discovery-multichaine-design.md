# Découverte multi-chaînes — tokens explosifs et early buyers

**Date :** 2026-09-26
**Statut :** validé en brainstorming, en attente de relecture

## Contexte

La V1 (`legacy/smart_wallet_analysis/token_discovery_manual/`) détectait les tokens explosifs sur GeckoTerminal pour deux chaînes codées en dur (Base, BSC), puis envoyait chaque token à une requête Dune pour lister les early buyers. Limites :

- chaînes figées, une seule page de pools `trending` + `new_pools` ;
- explosion scorée par `écart en heures × % de hausse`, sans contrôle de rug ;
- fenêtres Dune calculées depuis « maintenant », pas depuis l'explosion ;
- tout `Transfer` entrant compté comme achat (airdrops, transferts, contrats) ;
- dépendance à Dune (crédits, attente des exécutions).

Ce sous-projet reconstruit la découverte dans le nouveau backend, en s'inspirant de la V1 sans la recopier. Dune est remplacé par HyperSync (Envio). L'historique des wallets (Zerion) et le scoring restent hors périmètre.

## Objectif

Chaque jour, sans aucune chaîne codée en dur :

1. trouver les tokens qui ont réellement explosé, sur toutes les chaînes EVM supportées ;
2. pour chacun, enregistrer les wallets EOA qui ont acheté significativement avant l'explosion.

## Décisions

| Sujet | Décision |
|---|---|
| Source des candidats | GeckoTerminal (API publique gratuite) : trending global + top volume 24 h par chaîne, triés localement par hausse (nos « top gainers ») ; ajout manuel via l'admin |
| Early buyers | HyperSync (Envio), remplace Dune |
| Chaînes | Synchronisées chaque jour ; active si GeckoTerminal + HyperSync + Zerion la supportent et qu'elle n'est pas désactivée dans l'admin |
| Non-EVM (Solana…) | Hors périmètre (HyperSync ne supporte Solana qu'en testnet) |
| Explosion | Multiplicateur ×N (point bas → pic) + garde-fous volume, liquidité, rétention après le pic |
| Acheteur | Le signataire de la transaction (`tx.from`) reçoit les tokens → EOA uniquement, par construction |
| Taille | `bought_usd ≥ min_buy_usd`, puis au plus `max_buyers` plus gros par explosion ; les miettes ne sont pas stockées |
| Paramètres | **Aucun paramètre codé en dur.** Seuils métier dans `DetectionSettings` (globaux + surcharge par chaîne), paramètres du pipeline dans `PipelineSettings` (singleton), planning dans `django-celery-beat`. Tout est modifiable dans l'admin ; le code ne contient que des valeurs par défaut de migration |
| Évolution | Source de candidats interchangeable (Megafilter CoinGecko ajoutable plus tard) |

## Structure

```
backend/
├── integrations/                # package Python simple, pas une app Django
│   ├── __init__.py
│   ├── http.py                  # session httpx, timeouts, retries, rate limit Redis
│   ├── geckoterminal.py
│   ├── coingecko.py
│   ├── hypersync.py
│   ├── zerion.py                # uniquement la liste des chaînes pour ce sous-projet
│   └── tests/
└── apps/discovery/              # app Django, convention du socle
    ├── models.py
    ├── admin.py
    ├── services/
    │   ├── chains.py            # synchronisation des chaînes
    │   ├── candidates.py        # collecte + pré-filtres
    │   ├── explosion.py         # détection (fonctions pures)
    │   └── buyers.py            # agrégation des acheteurs (fonctions pures) + extraction
    ├── tasks.py
    └── tests/
```

`integrations/` ne contient aucune logique métier : chaque client appelle son API et renvoie des structures typées (dataclasses). `apps/discovery/services/` contient la logique ; les fonctions de calcul (explosion, agrégation) sont pures et testables sans base ni réseau.

## Modèles

Principe : le strict nécessaire, rien de recalculable. Clés primaires `BigAutoField`. Adresses stockées en minuscules.

| Modèle | Champs |
|---|---|
| `Chain` | `gt_id` (unique), `name`, `evm_id` (chain id EVM, null si inconnu), `zerion_id` (vide si non supporté), `hypersync_supported`, `is_enabled` (défaut `True`), `updated_at` |
| `DetectionSettings` | `chain` (FK unique, null = ligne globale) + seuils nullables (null = valeur globale) : `min_change_24h_pct`, `min_liquidity_usd`, `min_volume_usd`, `peak_volume_window_hours`, `min_fdv_usd`, `max_fdv_usd`, `max_pool_age_hours`, `min_multiplier`, `min_retention_pct`, `confirmation_hours`, `confirmation_timeout_hours`, `sniper_blocks`, `min_buy_usd`, `max_buyers` (0 = pas de plafond) |
| `PipelineSettings` | Singleton (une seule ligne) : `trending_pages`, `volume_pages_per_chain`, `candidate_cooldown_hours`, `max_transfers_per_token`, `max_attempts`, `gecko_requests_per_min`, `hypersync_requests_per_min`, `http_timeout_seconds`, `http_max_retries` |
| `Token` | `chain`, `address`, `symbol`, `decimals` — unique (`chain`, `address`) |
| `Pool` | `token`, `address`, `created_block` — unique (`token`, `address`) |
| `Candidate` | `token`, `status`, `sources` (liste), `metrics` (JSON : hausse 24 h, volume, liquidité, FDV à la détection), `rejection_reason`, `attempts`, `next_check_at`, `created_at`, `updated_at` |
| `Explosion` | `candidate` (OneToOne), `low_block`, `low_at`, `peak_block`, `peak_at`, `multiplier`, `retention_pct` |
| `Wallet` | `address` (unique) |
| `EarlyBuyer` | `explosion`, `wallet`, `first_buy_block`, `first_buy_at`, `bought_amount`, `bought_usd`, `sold_amount`, `is_sniper` — unique (`explosion`, `wallet`) |

- Montants on-chain bruts : `DecimalField(max_digits=78, decimal_places=0)` (uint256), convertis avec `Token.decimals` à l'affichage.
- `sold_amount` : tokens envoyés par le wallet entre son premier achat et le pic.
- `Pool.address` accepte les identifiants de pool Uniswap v4 (32 octets).

### Valeurs initiales

Créées par une data migration, puis gérées uniquement dans l'admin. Ce sont des points de départ, à calibrer sur des tokens réels.

**`DetectionSettings` (ligne globale)**

| Seuil | Valeur |
|---|---|
| `min_change_24h_pct` | 50 |
| `min_liquidity_usd` | 10 000 |
| `min_volume_usd` | 50 000 |
| `peak_volume_window_hours` | 24 |
| `min_fdv_usd` / `max_fdv_usd` | 100 000 / 100 000 000 |
| `max_pool_age_hours` | 720 |
| `min_multiplier` | 5 |
| `min_retention_pct` | 30 |
| `confirmation_hours` | 24 |
| `confirmation_timeout_hours` | 168 |
| `sniper_blocks` | 3 |
| `min_buy_usd` | 500 |
| `max_buyers` | 300 |

`min_buy_usd` et `max_buyers` s'appliquent dans cet ordre : on garde les acheteurs ≥ `min_buy_usd`, puis les `max_buyers` plus gros parmi eux.

**`PipelineSettings`**

| Paramètre | Valeur |
|---|---|
| `trending_pages` | 10 |
| `volume_pages_per_chain` | 3 |
| `candidate_cooldown_hours` | 72 |
| `max_transfers_per_token` | 500 000 |
| `max_attempts` | 3 |
| `gecko_requests_per_min` | 30 |
| `hypersync_requests_per_min` | 60 |
| `http_timeout_seconds` | 15 |
| `http_max_retries` | 3 |

Restent dans le code uniquement les constantes de protocole (URLs des API, signature de l'event `Transfer`, limite de 1 000 bougies imposée par GeckoTerminal). Les secrets restent dans les variables d'environnement.

Les réglages sont lus au début de chaque tâche : une modification dans l'admin s'applique au passage suivant, sans redémarrage.

### Index

- `Candidate` : (`status`, `next_check_at`) ; contrainte unique partielle : un seul candidat par token dont le statut n'est ni `REJECTED` ni `BUYERS_EXTRACTED`.
- `EarlyBuyer` : unique (`explosion`, `wallet`) + index sur `wallet` (requête centrale du futur scoring : « sur combien d'explosions ce wallet était-il en avance ? »).

## Cycle de vie d'un candidat

```
CANDIDATE ──► ANALYZED ──► WAITING_CONFIRMATION ──► CONFIRMED ──► BUYERS_EXTRACTED
     │            │                  │                   │
     └────────────┴──────────────────┴───────────────────┴──► REJECTED (rejection_reason)
```

| Statut | Signification |
|---|---|
| `CANDIDATE` | Vu dans une source, pré-filtres passés |
| `ANALYZED` | OHLCV récupéré, explosion ≥ `min_multiplier` trouvée (état transitoire dans la même tâche) |
| `WAITING_CONFIRMATION` | Pic trop récent pour mesurer la rétention ; `next_check_at = peak_at + confirmation_hours` |
| `CONFIRMED` | Garde-fous passés |
| `BUYERS_EXTRACTED` | Early buyers enregistrés |
| `REJECTED` | Avec raison : `no_pool`, `no_explosion`, `low_volume`, `low_liquidity`, `rug`, `confirmation_timeout`, `already_extracted`, `too_many_transfers`, `chain_inactive`, `error:<type>` |

## Pipeline quotidien (Celery)

Le planning est géré par `django-celery-beat` (planning en base, éditable dans l'admin : heure, fréquence, activation). Une data migration crée la tâche périodique par défaut : chaque jour à 06:00 UTC, la chaîne `sync_chains → collect_candidates → analyze_candidates → extract_early_buyers`. Chaque tâche est aussi lançable seule et ne traite que les candidats dans l'état attendu : relancer ne crée pas de doublon.

### 1. `sync_chains`

1. GeckoTerminal `GET /networks` (toutes les pages) → `gt_id`, `name`, `coingecko_asset_platform_id`.
2. CoinGecko `GET /asset_platforms` → `chain_identifier` = `chain_id`.
3. HyperSync `GET https://chains.hyperquery.xyz/active_chains` → `hypersync_supported` si le `chain_id` y figure avec `ecosystem = evm` et un tier non `TESTNET`.
4. Zerion `GET /v1/chains` → `zerion_id` par correspondance de `chain_id`.
5. Upsert de `Chain`. `is_enabled` n'est jamais modifié par la synchro.

URL HyperSync dérivée : `https://{evm_id}.hypersync.xyz`.

### 2. `collect_candidates`

1. `GET /networks/trending_pools?duration=24h`, `trending_pages` pages (toutes chaînes).
2. Pour chaque chaîne active : `GET /networks/{gt_id}/pools?sort=h24_volume_usd_desc`, `volume_pages_per_chain` pages.
3. Pools de chaînes inactives ignorés. Dédoublonnage par token (pool le plus liquide retenu pour les métriques).
4. Pré-filtres (`DetectionSettings` de la chaîne) : hausse 24 h, liquidité, volume 24 h, FDV min/max, âge du pool.
5. Tri local par hausse 24 h. Upsert `Token`, `Pool`, création du `Candidate` (`sources`, `metrics`) si aucun candidat en cours pour ce token et si aucun candidat de ce token n'a été clos depuis moins de `candidate_cooldown_hours`. Un pool qui échoue aux pré-filtres ne devient pas candidat (rien n'est stocké).

Ajout manuel : action admin « Ajouter un token » (chaîne + adresse) → récupère token et pools sur GeckoTerminal, crée un `Candidate` avec `sources = ["manual"]`, sans pré-filtres.

### 3. `analyze_candidates`

Traite les `CANDIDATE` et les `WAITING_CONFIRMATION` dont `next_check_at` est passé.

1. Récupère tous les pools du token (`GET /networks/{gt_id}/tokens/{address}/pools`) et `decimals`. Upsert `Pool`.
2. OHLCV du pool le plus liquide. Résolution choisie pour couvrir l'âge du pool en ≤ 1 000 bougies (1 h, 4 h ou 1 j).
3. `detect_explosion(candles, settings)` (pure) : meilleur ratio `pic / plus bas précédent` sur les clôtures, en un seul passage. Sous `min_multiplier` → `REJECTED/no_explosion`.
4. Garde-fous :
   - volume cumulé sur `peak_volume_window_hours` autour du pic ≥ `min_volume_usd`, sinon `low_volume` ;
   - liquidité actuelle ≥ `min_liquidity_usd`, sinon `low_liquidity` ;
   - si `now < peak_at + confirmation_hours` → `WAITING_CONFIRMATION` ;
   - sinon rétention = clôture à `peak_at + confirmation_hours` / pic ; < `min_retention_pct` → `rug`.
5. Si le pic détecté n'est pas plus récent que celui de la dernière explosion déjà extraite du token → `REJECTED/already_extracted`. Un candidat en attente depuis plus de `confirmation_timeout_hours` → `REJECTED/confirmation_timeout`. Un nouveau pic plus haut est pris en compte naturellement à la réévaluation.
6. Conversion dates → blocs (`low_at`, `peak_at`, création des pools) par recherche binaire sur les timestamps de blocs HyperSync, avec cache Redis. Création/mise à jour de `Explosion` → `CONFIRMED`.

### 4. `extract_early_buyers`

Une sous-tâche Celery par candidat `CONFIRMED`.

1. HyperSync : logs `Transfer` (`topic0 = 0xddf252ad…`) du token, de `min(Pool.created_block)` à `peak_block`, joints à leur transaction pour obtenir `tx.from`. Lecture page par page (`next_block`) ; au-delà de `max_transfers_per_token` → `REJECTED/too_many_transfers`.
2. `aggregate_buyers(transfers, explosion, pools, candles, settings)` (pure) :
   - **achat** : `to == tx.from` et bloc `≤ low_block` ;
   - **sortie** : `from == tx.from`, bloc entre le premier achat et `peak_block` → `sold_amount` ;
   - tokens reçus sans être signataire (airdrop, transfert) : ignorés ;
   - `bought_usd` = Σ quantité × clôture de la bougie contenant le bloc de l'achat ;
   - `is_sniper` si `first_buy_block − created_block du pool ≤ sniper_blocks` ;
   - garde `bought_usd ≥ min_buy_usd`, trie par `bought_usd` décroissant, coupe à `max_buyers`.
3. `bulk_create` de `Wallet` et `EarlyBuyer` par lots, `ignore_conflicts=True` → `BUYERS_EXTRACTED`.

Les acheteurs sont des EOA par construction : seul un EOA signe une transaction. Bots contrats, smart accounts ERC-4337, routers et contrats ne sont jamais le couple (signataire, destinataire). Les EOA délégués EIP-7702 sont conservés (wallet d'une personne). Limite assumée : les smart accounts ne sont pas couverts.

## Clients `integrations/`

- `httpx` avec timeout `http_timeout_seconds` ; jusqu'à `http_max_retries` retries avec backoff exponentiel sur 429 et 5xx ; erreurs typées (`RateLimited`, `UpstreamError`, `NotFound`).
- Rate limit partagé entre workers via Redis (token bucket par API) : débits `gecko_requests_per_min` et `hypersync_requests_per_min`. Les clients reçoivent ces réglages en paramètre et ne lisent jamais la base eux-mêmes.
- HyperSync via le client Python officiel `hypersync`.

Variables d'environnement ajoutées à `.env.example` : `ENVIO_API_TOKEN`, `ZERION_API_KEY`, `COINGECKO_API_KEY` (optionnelle, plan Demo).

## Gestion des erreurs

- Échec sur un candidat : statut inchangé, `attempts += 1`, repris au prochain passage ; à `max_attempts` échecs → `REJECTED/error:<type>`. Le détail de l'exception va dans les logs.
- Chaîne devenue inactive : ses candidats en cours → `REJECTED/chain_inactive`.
- Une sous-tâche d'extraction en échec n'affecte pas les autres.

## Admin

- `Chain` : liste filtrable, `is_enabled` éditable, le reste en lecture seule.
- `DetectionSettings` : édition complète.
- `PipelineSettings` : édition, ajout et suppression désactivés (singleton).
- Tâches périodiques (`django-celery-beat`) : heure, fréquence, activation.
- `Candidate` : filtres par statut, chaîne, raison de rejet ; action « Ajouter un token ».
- `Explosion`, `EarlyBuyer` : lecture seule ; `EarlyBuyer` filtrable par sniper, trié par `bought_usd`.

## Scalabilité

- Pas de stockage des transferts bruts : agrégation en mémoire, une ligne par (explosion, wallet), plafonnée à `max_buyers`.
- Index dimensionnés pour les requêtes réelles (voir Modèles).
- Écritures en masse, idempotentes.
- Parallélisme par token : ajouter des workers suffit.
- Partitionnement de `EarlyBuyer` par date possible plus tard sans changer le modèle ; pas maintenant.

## Tests

- **Fonctions pures** (`detect_explosion`, `aggregate_buyers`, choix de résolution OHLCV), tests en tableau : pas d'explosion, multiplicateur limite, rug, attente de confirmation, nouveau pic, achat, sortie, airdrop ignoré, sniper, seuil USD, coupe à `max_buyers`.
- **Réglages** : résolution des seuils (valeur de la chaîne si renseignée, sinon globale) ; `max_buyers = 0` = pas de plafond ; une modification en base est prise en compte à la tâche suivante.
- **Clients** : réponses HTTP enregistrées (`respx`), sans réseau.
- **Tâches** : contre le vrai PostgreSQL, clients simulés ; idempotence (deux exécutions → mêmes lignes) ; transitions de statut ; `attempts`.
- **Live** : un test par client contre les vraies API, marqué `@pytest.mark.live`, exclu par défaut.

## Critères de réussite

1. `sync_chains` crée les chaînes et active au moins `eth`, `bsc`, `base` et `robinhood` (clés présentes).
2. `collect_candidates` crée des candidats sur plusieurs chaînes sans aucune chaîne codée en dur.
3. Sur 2-3 tokens explosifs connus, les early buyers extraits sont cohérents avec une vérification manuelle sur l'explorateur de blocs (achats avant le point bas, EOA, montants).
4. Relancer toute la chaîne de tâches ne crée aucun doublon.
5. `make test` et `make lint` passent.

## Hors périmètre

- Historique des wallets (Zerion), FIFO, scoring, tracking live, consensus, Telegram
- Solana et chaînes non-EVM
- Megafilter CoinGecko (prévu comme source interchangeable)
- API REST publique de la découverte (l'admin suffit pour ce sous-projet)
- Smart accounts ERC-4337
