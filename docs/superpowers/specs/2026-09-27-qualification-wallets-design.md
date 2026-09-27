# Qualification des wallets — filtrage, historique, entités, tags

**Date :** 2026-09-27
**Statut :** validé en brainstorming, en attente de relecture

## Contexte

La découverte (`apps/discovery`) produit chaque jour des early buyers : des wallets EOA qui ont acheté un token avant son explosion. Beaucoup sont du bruit (bots, farmers, wallets jetables, dépôts d'exchange), et certains traders achètent avec un wallet puis envoient tout sur un autre : juger chaque wallet isolément les fait passer pour des petits wallets sans intérêt.

Ce sous-projet qualifie ces wallets : il écarte le bruit à bas coût, récupère l'historique des bons wallets organisé par token, regroupe les wallets d'une même personne en entités et étiquette leur comportement. Le scoring (rendement, FIFO) est un sous-projet suivant.

## Objectif

Pour chaque early buyer :

1. l'écarter tôt et gratuitement s'il est du bruit ;
2. sinon, stocker son historique par token (résumé + mouvements) ;
3. le rattacher à son entité (wallets liés) ;
4. juger la valeur au niveau de l'entité ;
5. lui attribuer des tags de comportement.

## Décisions

| Sujet | Décision |
|---|---|
| Découpage | Un sous-projet, 5 étapes : pré-filtre → historique → entités → valeur → tags |
| Données on-chain | HyperSync (transferts, transactions, financement, contreparties) |
| Zerion | **Uniquement pour les prix** : prix actuels par lots, courbes de prix des actifs de cotation, liste des stablecoins |
| Solde natif | `eth_getBalance` sur le RPC public de la chaîne (URL synchronisée depuis Zerion) |
| Chaînes analysées | Chaînes où le wallet a été repéré + liste supplémentaire réglable (vide par défaut) |
| Historique | `history_days` = 365, réglable |
| Filtres | Pré-filtre HyperSync (fréquence, diversité, inactivité), filtres d'historique (MEV, trop peu de trades), valeur d'**entité** (10 000 $ min.) |
| Entités | Transfert après achat, financement initial, financeur commun (avec garde-fou anti-exchange) ; migration de portefeuille plus tard |
| Exchanges | Trois couches : registre d'adresses connues, hot wallet par comportement, adresse de dépôt par son lien avec un hot wallet |
| Tags | SNIPER, EARLY_BUYER, ACCUMULATEUR, FLIPPER, HOLDER (plusieurs possibles, par wallet et par entité) |
| Paramètres | Aucun codé en dur : `QualificationSettings` (global + par chaîne), budgets dans `PipelineSettings` |

## Structure

```
backend/
├── integrations/
│   ├── hypersync.py            # + requêtes par wallet (transferts, transactions, financement, contreparties)
│   ├── zerion.py               # + prix actuels par lots, courbes de prix, stablecoins
│   └── rpc.py                  # eth_getBalance sur RPC public
└── apps/wallets/
    ├── models.py
    ├── admin.py
    ├── services/
    │   ├── settings.py         # résolution des seuils (global / chaîne)
    │   ├── classify.py         # pur : classement des mouvements
    │   ├── positions.py        # pur : agrégation par token
    │   ├── filters.py          # pur : pré-filtre et filtres d'historique
    │   ├── exchanges.py        # pur + registre : hot wallet, dépôt d'exchange
    │   ├── entities.py         # liens + regroupement (union-find)
    │   ├── pricing.py          # prix des mouvements et valeur des portefeuilles
    │   ├── tags.py             # pur : tags
    │   └── qualification.py    # orchestration d'un wallet
    ├── tasks.py
    └── tests/
```

## Modèles (app `wallets`)

| Modèle | Champs |
|---|---|
| `WalletProfile` | `wallet` (OneToOne `discovery.Wallet`), `status`, `filter_reason`, `entity` (FK nullable), `portfolio_value_usd`, `metrics` (JSON : mesures du pré-filtre), `tags` (ArrayField), `attempts`, `analyzed_at`, `next_analysis_at` |
| `TokenPosition` | `wallet`, `token` (`discovery.Token`), `bought_amount`, `sold_amount`, `sent_amount`, `received_amount`, `bought_usd`, `sold_usd`, `buys`, `sells`, `first_at`, `last_at` — unique (wallet, token) |
| `TokenTrade` | `wallet`, `token`, `kind` (buy, sell, send, receive), `amount`, `usd` (nullable = prix inconnu), `counterparty`, `block`, `at`, `tx_hash`, `log_index` — unique (`tx_hash`, `log_index`, `wallet`) ; index (wallet, token, at) |
| `Entity` | `portfolio_value_usd`, `tags` (ArrayField), `updated_at` |
| `WalletLink` | `from_wallet`, `to_wallet`, `kind` (transfer_after_buy, funding, common_funder), `evidence` (JSON) — unique (from, to, kind) |
| `KnownAddress` | `chain` (nullable = toutes), `address`, `kind` (exchange, cex_deposit, bridge, router, mev, stablecoin), `label`, `source` (import, manual, auto) — unique (chain, address) |
| `QualificationSettings` | `chain` (nullable = global) + seuils nullables (voir ci-dessous) |

Ajouts aux modèles existants :

- `discovery.Chain.rpc_url` (public, rempli par `sync_chains` depuis Zerion) et `discovery.Chain.native_fungible_id` (id Zerion de l'actif natif).
- `discovery.PipelineSettings` : `zerion_daily_budget` (250), `zerion_requests_per_sec` (1), `zerion_price_batch_size` (25), `stablecoin_symbols` (JSON, `["USDC", "USDT", "DAI"]`), `extra_chains` (JSON, `[]`), `qualification_batch_size` (100 wallets par passage).

Montants bruts en `DecimalField(78, 0)`. Tags en `ArrayField` avec index GIN.

### Statuts d'un wallet

```
PENDING ──► PREFILTERED ──► HISTORY_FETCHED ──► QUALIFIED
    └───────────┴────────────────┴──► FILTERED (filter_reason)
```

`FILTERED` n'est jamais supprimé ; le wallet n'est pas retraité avant `refilter_after_days`. Raisons : `bot_frequency`, `farmer`, `inactive`, `bot_mev`, `too_few_trades`, `portfolio_too_small`, `portfolio_too_large`, `exchange`, `error:<type>`.

### Seuils (`QualificationSettings`, ligne globale par défaut)

| Seuil | Défaut | Rôle |
|---|---|---|
| `max_txs_per_day` | 200 | Moyenne sur 7 jours ; au-delà → `bot_frequency` |
| `max_distinct_tokens` | 300 | Tokens différents reçus sur l'historique ; au-delà → `farmer` |
| `inactive_days` / `min_txs_active` | 90 / 5 | Moins de 5 tx signées sur 90 jours → `inactive` |
| `history_days` | 365 | Profondeur de l'historique |
| `max_mev_ratio` | 30 | % de trades achetés et revendus dans le même bloc ; au-delà → `bot_mev` |
| `min_distinct_buys` | 3 | Tokens différents achetés ; en dessous → `too_few_trades` |
| `min_portfolio_usd` / `max_portfolio_usd` | 10 000 / 50 000 000 | Valeur de l'**entité** |
| `link_min_share_pct` | 30 | Part minimale envoyée à un destinataire pour le suivre |
| `transfer_after_buy_pct` | 70 | Part d'un token acheté envoyée à B pour créer un lien |
| `follow_depth` | 1 | Niveaux de wallets suivis (max 2) |
| `funder_max_wallets` | 50 | Au-delà, un financeur est un service, il ne relie personne |
| `hot_wallet_min_counterparties` | 1 000 | Contreparties distinctes sur 7 jours pour un hot wallet |
| `deposit_forward_pct` / `deposit_forward_hours` | 90 / 24 | Signature d'une adresse de dépôt |
| `flipper_hours` | 24 | Revente de l'essentiel en moins de N heures |
| `holder_min_pct` | 50 | Part gardée jusqu'au pic d'une explosion |
| `accumulator_max_out_pct` | 20 | Plusieurs achats, moins de N % ressortis |
| `early_buyer_min_explosions` | 2 | Early buyer sur au moins N explosions |
| `refilter_after_days` | 30 | Délai avant de reconsidérer un wallet filtré |

`sniper_blocks` reste celui de la découverte.

## Déroulé

Tâche quotidienne `qualify_wallets` (planifiée dans l'admin, après la découverte), qui prend jusqu'à `qualification_batch_size` wallets `PENDING` (les nouveaux `EarlyBuyer` créent leur `WalletProfile`) et lance une sous-tâche par wallet. Chaque étape est idempotente et reprend là où elle s'est arrêtée.

### 1. Pré-filtre (HyperSync, gratuit)

Sur chaque chaîne analysée :

- nombre de transactions signées sur 7 jours (arrêt dès le seuil atteint) → `bot_frequency` ;
- nombre de transactions signées sur `inactive_days` → `inactive` ;
- nombre de tokens différents reçus sur `history_days` (arrêt dès le seuil) → `farmer` ;
- l'adresse est dans `KnownAddress` (exchange, dépôt…) → `exchange`.

### 2. Historique (HyperSync)

Tous les `Transfer` ERC-20 où le wallet est émetteur ou destinataire sur `history_days`, joints à leur transaction (`from`, `to`, `value`), avec `log_index`.

Classement (fonction pure) :

| Type | Règle |
|---|---|
| `buy` | Le wallet signe et reçoit le token, via un contrat autre que le token |
| `sell` | Le wallet signe et envoie le token vers un pool / router (appel autre que `transfer()` du token) |
| `send` | Le wallet signe un appel direct à `transfer()` du token vers une autre adresse |
| `receive` | Le wallet reçoit sans être signataire (transfert, airdrop) |

Les tokens sans prix Zerion au moment de la valorisation (spam) sont gardés en base mais ignorés pour les filtres et la valeur. Filtres d'historique : `bot_mev`, `too_few_trades`. Écriture de `TokenTrade` (en masse, idempotente) puis recalcul de `TokenPosition`.

### 3. Entités

- **Transfert après achat** : depuis `TokenTrade` (et les transferts déjà récupérés à l'extraction) : le wallet envoie ≥ `transfer_after_buy_pct` d'un token acheté à B sans le vendre.
- **Financement initial** : première transaction reçue avec `value > 0` (HyperSync).
- **Financeur commun** : même financeur initial, s'il a financé ≤ `funder_max_wallets` wallets.

Avant de créer un lien vers B, B passe les **trois couches anti-exchange** :

1. `KnownAddress` (registre : import de listes publiques, ajout manuel, auto-alimentation) ;
2. **hot wallet** : ≥ `hot_wallet_min_counterparties` contreparties distinctes sur 7 jours ;
3. **dépôt d'exchange** : renvoie ≥ `deposit_forward_pct` de ses réceptions en moins de `deposit_forward_hours` vers une seule adresse qui est un hot wallet, ou n'a jamais signé de transaction.

Exchange ou dépôt → pas de lien, l'envoi est traité comme une vente, l'adresse est enregistrée dans `KnownAddress` (`source = auto`). Sinon B est lié, reçoit un `WalletProfile` (`PENDING`) et est suivi jusqu'à `follow_depth` niveaux. Les entités sont les composantes connexes des liens (union-find), recalculées à chaque nouveau lien.

### 4. Valeur de l'entité (Zerion prix + RPC)

- Soldes par token (depuis `TokenPosition`) × prix actuels Zerion, demandés par lots de `zerion_price_batch_size`.
- \+ solde natif via `eth_getBalance`.
- Somme sur tous les wallets de l'entité → `min_portfolio_usd` / `max_portfolio_usd`.
- Budget Zerion quotidien épuisé → le wallet reste `HISTORY_FETCHED` et passe au prochain passage (pas une erreur).

**Prix des mouvements** (`TokenTrade.usd`) : par la contrepartie dans la même transaction (actif natif via `tx.value`, wrapped natif, stablecoins) × prix historique de l'actif de cotation. Courbes de prix des actifs natifs : un appel Zerion par actif et par jour, mis en cache. Stablecoins : adresses par chaîne récupérées depuis Zerion à partir de `stablecoin_symbols`, prix = 1 $. Sans contrepartie reconnue : `usd = null`. Limite assumée : une vente payée en natif via un router (transfert interne) n'a pas de contrepartie visible dans les logs → `usd = null` pour l'instant.

### 5. Tags (fonction pure)

| Tag | Règle |
|---|---|
| `SNIPER` | Au moins un `EarlyBuyer.is_sniper` |
| `EARLY_BUYER` | Early buyer sur ≥ `early_buyer_min_explosions` explosions |
| `ACCUMULATEUR` | Tokens achetés en ≥ 2 achats avec < `accumulator_max_out_pct` % ressortis |
| `FLIPPER` | Majorité des positions revendues à ≥ 80 % en moins de `flipper_hours` |
| `HOLDER` | ≥ `holder_min_pct` % d'un token explosif gardé jusqu'au pic |

Calculés par wallet puis par entité (union des positions de ses wallets : « achète sur A, garde sur B » = HOLDER). Wallet → `QUALIFIED`.

## Clients

- **HyperSync** : `wallet_tx_count(address, from_block, to_block, cap)`, `wallet_transfers(address, from_block, to_block)`, `first_funding(address)`, `distinct_counterparties(address, from_block, to_block, cap)`. Les comptages s'arrêtent dès que `cap` est atteint.
- **Zerion** (prix uniquement) : `prices(implementations) -> dict`, `price_chart(fungible_id, period) -> list`, `stablecoins(symbols) -> dict[chain, list[address]]`, et `chain_ids()` enrichi de `rpc_url` et `native_fungible_id`. Limiteur de débit + compteur de budget quotidien dans Redis.
- **RPC** : `native_balance(rpc_url, address) -> int`.

## Gestion des erreurs

- Une sous-tâche par wallet ; échec → `attempts += 1`, à `max_attempts` → `FILTERED/error:<type>`.
- Budget Zerion épuisé → attente du lendemain, pas un échec.
- RPC public indisponible → solde natif compté à 0 et signalé dans `metrics`.

## Admin

- `WalletProfile` : filtres par statut, raison, tag, chaîne ; lien vers l'entité ; positions en ligne.
- `Entity` : wallets, liens (avec leur preuve), valeur, tags.
- `TokenPosition` / `TokenTrade` : lecture seule, filtrables par wallet et token.
- `KnownAddress` : édition complète, import de liste (CSV).
- `QualificationSettings` : édition complète.

## Scalabilité

- `TokenTrade` n'est rempli que pour les wallets qui passent le pré-filtre ; mouvements utiles uniquement ; index (wallet, token, at).
- Comptages HyperSync plafonnés (`cap`) : un hot wallet ne coûte pas plus cher qu'un wallet normal.
- Zerion : lots de prix + cache des courbes + budget quotidien.
- Tags en `ArrayField` + GIN.

## Tests

- **Purs** (en tableau) : classement des mouvements, agrégation des positions, taux MEV, pré-filtre et filtres, détection hot wallet et dépôt, union-find des entités, tags.
- **Clients** : `respx` et faux client HyperSync.
- **Tâches** : PostgreSQL réel, idempotence (deux passages → mêmes lignes), respect du budget Zerion, suivi des liens limité à `follow_depth`.
- **Live** (`-m live`) : prix Zerion par lots, courbe de prix, requêtes HyperSync sur un vrai wallet, `eth_getBalance`.

## Critères de réussite

1. Sur les 300 early buyers de XL, les bots et farmers sont écartés avant tout appel Zerion.
2. Un wallet d'achat qui envoie tout vers un wallet coffre est regroupé avec lui, et l'entité passe le filtre de valeur.
3. Les envois vers un exchange ne créent aucun lien et sont comptés comme des ventes.
4. Le budget Zerion quotidien n'est jamais dépassé.
5. Relancer la qualification ne crée aucun doublon.
6. `make test` et `make lint` passent.

## Hors périmètre

- Scoring (rendement, FIFO, fenêtres 30 / 90 / 365 j)
- Tracking live et alertes
- Migration de portefeuille (règle V1)
- Prix des ventes en natif via transfert interne (traces)
- Chaînes non EVM
