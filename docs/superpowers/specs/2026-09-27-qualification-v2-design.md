# Qualification des wallets — v2 (historique Zerion, wallets liés)

**Date :** 2026-09-27
**Statut :** validé en discussion, en attente de relecture
**Remplace en partie :** `2026-09-27-qualification-wallets-design.md` (v1)

## Pourquoi une v2

L'essai réel de la v1 sur 20 early buyers de XL a montré :

1. **Prix manquants.** Reconstruire le prix de chaque mouvement depuis la blockchain (contrepartie dans la transaction) laisse des trous inévitables : transferts simples, échanges token contre token, ventes payées en natif via un router, solveurs. Or le scoring a besoin d'un prix pour chaque transaction.
2. **Vue mono-chaîne.** Analyser seulement la chaîne où le wallet a été repéré sous-estime sa valeur (ex. 968 $ sur Robinhood alors qu'il détient ~5 100 $ sur BSC) et ignore une partie de son activité (Arc, BSC, Base…).
3. **Bruit des réceptions.** Des centaines d'airdrops de spam ; mais une grosse réception venant d'un wallet est un signal utile.
4. **Entités trop complexes.** Regrouper des wallets en chaîne, avec statut et rôles, rend les décisions difficiles à expliquer.

Mesure faite sur 4 vrais wallets : l'historique Zerion (`/wallets/{address}/transactions`) donne **92 à 96 % des mouvements avec prix**, le type d'opération et **toutes les chaînes EVM**. Clé Zerion passée au plan **Developer** (gratuit, 2 000 appels/jour, 10/s).

## Décisions

| Sujet | v1 | v2 |
|---|---|---|
| Source de l'historique | HyperSync, prix reconstruits | **Zerion `/transactions`**, prix Zerion |
| Chaînes | Chaîne où le wallet a été repéré | **Toutes les chaînes EVM** (Zerion) |
| Période | 365 jours | **90 jours** (`history_days`, réglable jusqu'à 180) |
| Opérations | Tous les transferts ERC-20 | `trade`, `send`, `receive`, et `execute` / `mint` / `burn` **seulement s'ils contiennent des transferts** ; pas d'`approve` |
| Données par mouvement | Montant, prix reconstruit | **Données Zerion complètes** + réponse brute JSON par transaction |
| Seuil bot | 200 tx/jour | **50 tx/jour** (moyenne 7 jours) |
| Unité jugée | Entité (groupe de wallets) | **Le wallet**, avec ses **wallets liés directs** |
| Liens | Transfert après achat, financement, financeur commun (tous fusionnent) | **Forts** (décision) : transfert après achat, gros transfert reçu. **Informatifs** : financement initial, financeur commun |
| Gros transfert reçu | — | **≥ X % des entrées du wallet** sur la période (en %, jamais en $) |
| Valeur | Soldes reconstruits + solde natif RPC | **Zerion `/portfolio`** (toutes chaînes, 1 appel) du wallet **+ de ses wallets liés directs** |
| Ordre de traitement | Ancienneté | **Meilleurs signaux d'abord** (nombre d'explosions, montant de l'early buy) |
| Quota Zerion | 250/jour | **1 800/jour**, débit 300/min, **20 pages max par wallet** |

Zerion reste interdit pour l'identification des early buyers (découverte = HyperSync) et pour le pré-filtre (HyperSync, gratuit).

## Déroulé par wallet

```
PENDING ──► PREFILTERED ──► HISTORY_FETCHED ──► QUALIFIED
    └───────────┴────────────────┴──► FILTERED (filter_reason)
```

### 1. Pré-filtre — HyperSync (inchangé, seuil resserré)

Sur la chaîne où le wallet a été repéré : fréquence (`max_txs_per_day` = 50, moyenne 7 jours) → `bot_frequency` ; inactivité → `inactive` ; adresse connue (exchange, dépôt…) → `exchange`. Le filtre `farmer` passe à l'étape 2 (il se mesure mieux sur l'historique Zerion multi-chaînes).

### 2. Historique — Zerion

- `GET /wallets/{address}/transactions/` avec `filter[min_mined_at]` = maintenant − `history_days`, `filter[operation_types]` = `trade,send,receive,execute,mint,burn`, `filter[trash]=only_non_trash`, `currency=usd`, `page[size]=100`.
- Pagination jusqu'à la fin de la période ou `max_history_pages` (20). Atteindre le plafond est enregistré dans `metrics` (`history_truncated = true`) : c'est un indice de bot.
- Les transactions sans transfert sont ignorées. Les transactions `failed` sont ignorées.
- Filtres sur l'historique :
  - `farmer` : plus de `max_distinct_tokens` tokens différents reçus sur la période ;
  - `bot_mev` : part des achats revendus dans le **même bloc** au-delà de `max_mev_ratio` ;
  - `too_few_trades` : moins de `min_distinct_buys` tokens achetés.
- Écriture idempotente (mise à jour à la relance), puis recalcul des positions.

### 3. Wallets liés — liens forts uniquement

Calculés depuis les mouvements Zerion du wallet :

| Lien | Règle | Seuil réglable |
|---|---|---|
| **Transfert après achat** | Le wallet envoie à B ≥ X % d'un token qu'il a acheté ou reçu | `transfer_after_buy_pct` (70 %) |
| **Gros transfert reçu** | Le wallet reçoit de B, en valeur, ≥ X % de toutes ses entrées de la période | `big_receive_pct` (30 %) |

Avant de créer un lien, B passe les **trois couches anti-exchange** de la v1 (registre, hot wallet, dépôt d'exchange) via HyperSync, sur la chaîne du mouvement. Exchange ou dépôt → pas de lien (un envoi vers un exchange est une vente).

Le wallet lié B reçoit un profil `source = linked`, `depth = 1` ; **il n'est pas suivi plus loin** (liens directs seulement). Il ne passe que les contrôles anti-exchange et la valorisation (1 appel `/portfolio`) : pas de pré-filtre d'activité, pas d'historique complet.

**Informatif (sans effet sur la décision)** : financement initial (premier dépôt de gaz, HyperSync) et financeur commun, enregistrés comme liens `funding` pour l'affichage.

Petits transferts reçus (poussière, airdrops) : ni lien, ni effet.

### 4. Valeur — Zerion `/portfolio`

- `GET /wallets/{address}/portfolio?currency=usd` : valeur totale toutes chaînes et répartition par chaîne (stockée dans `metrics`).
- Valeur jugée = valeur du wallet + valeur de ses wallets liés **forts** directs.
- `min_portfolio_usd` ≤ valeur ≤ `max_portfolio_usd` → `QUALIFIED`, sinon `portfolio_too_small` / `portfolio_too_large`.
- Quand la valeur d'un wallet lié arrive plus tard, les wallets qui lui sont liés sont réévalués.

### 5. Tags

SNIPER, EARLY_BUYER, ACCUMULATEUR, FLIPPER, HOLDER, calculés sur les mouvements Zerion du wallet (logique v1 inchangée, seuils réglables ; calibrage de FLIPPER à revoir sur données réelles).

## Données

### `WalletTransaction` (nouveau) — une transaction Zerion

| Champ | Contenu |
|---|---|
| `wallet` | Wallet analysé |
| `zerion_id` | Identifiant Zerion de la transaction (unique avec `wallet`) |
| `chain` | Chaîne (id Zerion, et FK `Chain` si connue) |
| `tx_hash`, `block`, `mined_at` | Transaction |
| `operation_type` | `trade`, `send`, `receive`, `execute`, `mint`, `burn` |
| `status` | `confirmed` |
| `fee_usd` | Frais de gaz en $ |
| `raw` | Réponse Zerion brute (JSON) |

### `TokenTrade` (refondu) — un transfert d'une transaction

| Champ | Contenu |
|---|---|
| `transaction` | FK `WalletTransaction` |
| `wallet`, `token` | Wallet ; token (`discovery.Token`, créé si besoin avec symbole et décimales Zerion) |
| `kind` | `buy` / `sell` (transfert d'un `trade`), `send` / `receive` (autres) |
| `direction` | `in` / `out` |
| `quantity` | Quantité décimale Zerion |
| `amount` | Montant brut (entier, `DecimalField(78, 0)`) |
| `price_usd` | Prix unitaire au moment de la transaction (nullable) |
| `value_usd` | Valeur au moment de la transaction (nullable) |
| `counterparty` | Expéditeur ou destinataire |
| `transfer_index` | Rang du transfert dans la transaction (unicité avec `transaction`) |

`TokenPosition` reste, recalculée depuis `TokenTrade` (montants et valeurs Zerion).

### `WalletProfile`

Ajouts : `priority` (score de priorité), `linked_value_usd` (valeur des wallets liés). Suppression de `entity`.

### Supprimé (YAGNI)

- `Entity` et le regroupement en chaîne (`refresh_entity`, `evaluate_entity`) ;
- la reconstruction des prix côté HyperSync (`price_trades`, actifs de cotation, `DailyPrice`, `sync_quote_assets`) ;
- le client RPC et le solde natif (inclus dans `/portfolio`).

`WalletLink.kind` : `transfer_after_buy`, `big_receive` (forts), `funding` (informatif).

### Réglages

`QualificationSettings` : `max_txs_per_day` = 50, `history_days` = 90, `big_receive_pct` = 30 ; suppression de `follow_depth`, `funder_max_wallets` (financement informatif). `PipelineSettings` : `zerion_daily_budget` = 1 800, `zerion_requests_per_min` = 300, `max_history_pages` = 20.

## Priorité de traitement

`priority` = nombre d'explosions où le wallet est early buyer (poids fort) puis montant total des early buys en $. La tâche quotidienne traite les profils en attente par priorité décroissante, dans la limite du budget Zerion. Les wallets liés (1 appel chacun) passent avant les nouveaux wallets, pour que les valeurs liées soient complètes.

## Coût Zerion estimé

| Poste | Appels |
|---|---|
| Historique d'un wallet (90 j, sans `approve`) | ~5 à 20 pages |
| Valeur d'un wallet ou d'un wallet lié | 1 |
| Par jour (budget 1 800) | ~100 à 150 wallets complets |

## Gestion des erreurs

- Budget Zerion épuisé → le wallet reste dans son statut, repris au prochain passage (pas une erreur).
- Échec → `attempts += 1`, à `max_attempts` → `FILTERED/error:<type>`.

## Tests

- Clients Zerion (`respx`) : pagination, filtres, parsing des transferts (quantité, prix, valeur, direction), `/portfolio`.
- Purs : conversion transaction Zerion → lignes, classement buy/sell/send/receive, positions, filtres (farmer, MEV, trop peu de trades), liens forts (transfert après achat, gros transfert reçu en %), tags, priorité.
- Tâches : idempotence, plafond de pages, budget, réévaluation d'un wallet quand la valeur de son wallet lié arrive.
- Live : historique et portfolio Zerion sur un vrai wallet.

## Critères de réussite

1. Chaque mouvement stocké porte le type d'opération Zerion et, quand Zerion le fournit, prix et valeur au moment de la transaction.
2. Un wallet actif sur plusieurs chaînes est vu sur toutes (historique et valeur).
3. Le cas « wallet d'achat → coffre » est qualifié grâce à la valeur du coffre lié.
4. Les airdrops de spam ne créent ni lien ni bruit dans les décisions.
5. Le budget Zerion quotidien n'est jamais dépassé ; les meilleurs wallets passent en premier.
6. `make test` et `make lint` passent.

## Hors périmètre

- Scoring (rendement, FIFO)
- Groupes de wallets au-delà des liens directs (entités complètes) — possible plus tard, les liens sont stockés
- Chaînes non EVM
