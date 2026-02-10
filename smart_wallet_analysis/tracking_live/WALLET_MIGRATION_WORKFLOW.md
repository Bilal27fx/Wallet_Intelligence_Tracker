# Wallet Migration — Workflow de développement

## Objectif

Détecter quand un smart wallet transfère >70% de son portefeuille vers un nouveau wallet (EOA),
récupérer l'historique du wallet fils, injecter les prix d'achat originaux de la mère via
`inherited_price_per_token`, puis scorer le fils avec ces prix corrigés.

---

## Principe fondamental : Option B — Prix injectés non-destructifs

**Ne jamais modifier `price_per_token`** dans `transaction_history`.
Utiliser exclusivement `inherited_price_per_token` pour stocker le prix hérité.

Le moteur FIFO (`fifo_clean_simple.py`) applique déjà :
```python
effective_price = float(inherited_price) if inherited_price else float(price)
```

Donc si `inherited_price_per_token` est rempli → FIFO l'utilise automatiquement.
Si re-fetch Zerion → `price_per_token` est écrasé mais `inherited_price_per_token` est intact.

---

## Schéma des tables impliquées

### `wallets`
```
wallet_address       TEXT  PK
period               TEXT  -- '14d' | 'manual' | 'migration'  ← tag wallet fils
total_portfolio_value REAL
is_active            BOOLEAN
is_scored            INTEGER  -- 0 = pas encore scoré
transactions_extracted INTEGER -- 0 = historique pas encore récupéré
last_sync            TIMESTAMP
```

### `smart_wallets`
```
wallet_address       TEXT
threshold_status     TEXT  -- 'EXCELLENT' | 'MANUAL' | 'MIGRATION'  ← tag wallet fils
quality_score        REAL
optimal_roi          REAL
... (autres métriques scoring)
```

### `wallet_migrations`
```
old_wallet           TEXT  -- wallet mère (smart wallet suivi)
new_wallet           TEXT  -- wallet fils (destination détectée)
migration_date       TIMESTAMP
tokens_transferred   TEXT  -- JSON [{symbol, contract_address, fungible_id, quantity, value_usd}]
total_value_transferred REAL
transfer_percentage  REAL
is_validated         BOOLEAN
```

### `transaction_history`
```
wallet_address              TEXT
symbol                      TEXT
direction                   TEXT  -- 'in' | 'out'
action_type                 TEXT  -- 'buy' | 'sell'
price_per_token             REAL  -- prix Zerion brut, NE PAS MODIFIER
inherited_price_per_token   REAL  -- NULL par défaut, rempli lors de migration ← clé
is_inherited_from_wallet    TEXT  -- adresse du wallet mère
```

---

## Workflow étape par étape

### Étape 1 — Détection de migration
**Fichier :** `wallet_migration_detector.py`

**Source :** uniquement les wallets présents dans `smart_wallets` (INNER JOIN avec `wallets`)

**Logique :**
1. Pour chaque smart wallet, appeler Zerion `/v1/wallets/{address}/transactions/?filter[operation_types]=send`
2. Fenêtre temporelle : **7 jours maximum** (168h)
3. Agréger la valeur envoyée par adresse destination via `transfer["recipient"]`
4. Si une destination reçoit **>70% de `total_portfolio_value`** → migration candidate

**Règles :**
- Ne jamais utiliser `recipient_address` depuis `transaction_history` (ne contient que les wallets déjà trackés)
- Toujours lire `transfer["recipient"]` directement depuis la réponse Zerion
- `total_portfolio_value` vient de `wallets.total_portfolio_value` (valeur au moment de la détection)

---

### Étape 2 — Vérification EOA
**Fichier :** `wallet_migration_detector.py` via `smart_contrat_remover.ContractChecker`

**Logique :**
```python
is_contract = _contract_checker.is_contract_single(destination)
# True  → smart contract → IGNORER
# False → EOA            → CONTINUER
# None  → API fail       → IGNORER par sécurité
```

**Règle :** En cas de doute (None), toujours rejeter. Vaut mieux rater une migration que d'enregistrer un contrat.

---

### Étape 3 — Insertion du wallet fils dans `wallets`
**Fichier :** `wallet_migration_detector.py`

**Règles :**
- `period = 'migration'` → tag explicite qui le distingue des wallets trackés normalement
- `is_active = 1`
- `is_scored = 0` → le scoring ne l'a pas encore traité
- `transactions_extracted = 0` → l'historique n'a pas encore été récupéré
- `total_portfolio_value = 0` → sera mis à jour lors du fetch Zerion
- Utiliser `INSERT OR IGNORE` → si le wallet fils est déjà dans `wallets` pour une autre raison, ne pas écraser

```sql
INSERT OR IGNORE INTO wallets (
    wallet_address, period, is_active, is_scored, transactions_extracted,
    total_portfolio_value, created_at, updated_at
) VALUES (?, 'migration', 1, 0, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
```

---

### Étape 4 — Récupération de l'historique du wallet fils
**Fichier :** `wallet_migration_detector.py` via `live_wallet_transaction_tracker_extractor_zerion.get_token_transaction_history_zerion_full`

**Logique :**
1. Récupérer les tokens transférés depuis `wallet_migrations.tokens_transferred` (JSON)
2. Pour chaque token (fungible_id disponible), appeler `get_token_transaction_history_zerion_full(fils, fungible_id)`
3. Stocker dans `transaction_history` avec `analyze_and_store_complete_transactions()`

**Règles :**
- Utiliser les `fungible_id` présents dans `tokens_transferred` du `wallet_migrations`
- Ne pas re-fetcher les tokens sans `fungible_id` (skip)
- Après fetch réussi → mettre `wallets.transactions_extracted = 1`
- Respecter le rate limiting (1.5s entre tokens)

---

### Étape 5 — Injection des prix hérités
**Fichier :** `wallet_migration_detector.py` → `_inherit_prices()`

**Logique :**
Pour chaque token transféré, récupérer le prix d'achat moyen pondéré du wallet mère :

```sql
SELECT SUM(ABS(quantity) * price_per_token) / SUM(ABS(quantity))
FROM transaction_history
WHERE wallet_address = old_wallet
AND symbol = ?
AND action_type = 'buy'
AND price_per_token > 0
AND quantity != 0
```

Puis l'injecter sur les transactions `receive` du wallet fils :

```sql
UPDATE transaction_history
SET inherited_price_per_token = ?,
    is_inherited_from_wallet = ?
WHERE wallet_address = new_wallet
AND symbol = ?
AND direction = 'in'
AND inherited_price_per_token IS NULL
```

**Règles :**
- Ne jamais modifier `price_per_token` — colonne Zerion en lecture seule
- Si aucun achat trouvé pour un token chez la mère → skip ce token (ne pas mettre 0)
- `is_inherited_from_wallet` = adresse du wallet mère → traçabilité
- Condition `inherited_price_per_token IS NULL` → idempotent, jamais d'écrasement

---

### Étape 6 — Enregistrement dans `wallet_migrations`
**Fichier :** `wallet_migration_detector.py` → `_save_migration()`

**Règle :** Utiliser `INSERT OR IGNORE` avec la contrainte `UNIQUE(old_wallet, new_wallet, migration_date)` → pas de doublon si le detector tourne plusieurs fois.

---

### Étape 7 — Scoring du wallet fils
**Fichier :** moteur de scoring existant (`fifo_clean_simple.py` + `wallet_scoring_system.py`)

**Le FIFO lit déjà automatiquement `inherited_price_per_token` :**
```python
effective_price = float(inherited_price) if inherited_price else float(price)
```
**Aucune modification du FIFO nécessaire.**

**Après scoring :**
- Insérer le fils dans `smart_wallets` avec `threshold_status = 'MIGRATION'`
- Mettre `wallets.is_scored = 1`

**Règle :** Le fils doit être scoré exactement comme n'importe quel autre wallet. Pas de traitement spécial dans le moteur de scoring.

---

## Ordre d'exécution dans `wallet_migration_detector.py`

```
detect_migrations()
│
├── 1. Query smart_wallets INNER JOIN wallets
├── 2. fetch_recent_transactions(wallet_mère, 168h)
├── 3. analyze_transfers_for_migration() → destination candidate
├── 4. EOA check → is_contract_single(destination)
│       False → continuer | True/None → ignorer
│
├── 5. INSERT fils dans wallets (period='migration', is_scored=0)
├── 6. fetch historique Zerion du fils (par token via fungible_id)
├── 7. _inherit_prices() → inherited_price_per_token sur receives du fils
└── 8. _save_migration() → wallet_migrations
```

Le scoring (`is_scored=0`) est traité lors du prochain cycle du pipeline de scoring,
pas dans ce module. Séparation claire des responsabilités.

---

## Règles générales pour Claude

1. **`price_per_token` est en lecture seule** — jamais de UPDATE sur cette colonne
2. **`inherited_price_per_token` uniquement sur `direction='in'`** — les ventes du fils ont leur propre prix Zerion
3. **Toujours `INSERT OR IGNORE`** pour `wallets` et `wallet_migrations` — le detector tourne en boucle
4. **Source des wallets à analyser = `smart_wallets` INNER JOIN `wallets`** — jamais `wallets` seul
5. **`transfer["recipient"]` depuis l'API Zerion** — jamais `recipient_address` depuis la BDD locale
6. **Fenêtre 7 jours fixe pour les migrations** — indépendante du `hours_lookback` du tracking live
7. **EOA check obligatoire avant toute insertion** — pas de smart contract dans `wallets`
8. **`period='migration'` dans `wallets`** — permet de filter/identifier les wallets fils facilement
9. **`threshold_status='MIGRATION'` dans `smart_wallets`** — après scoring, distingue les wallets fils des wallets découverts normalement
10. **Ne pas scorer dans `wallet_migration_detector.py`** — le scoring est délégué au pipeline existant via `is_scored=0`
