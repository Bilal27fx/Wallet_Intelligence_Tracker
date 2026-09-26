# 🔴 Tracking Live

Suivi en temps réel des changements de positions des smart wallets.

---

## 🎯 Objectif

Détecter instantanément les achats/ventes des smart wallets et mettre à jour leur historique.

---

## 🚀 Runner Principal

**Fichier:** `run.py`

**Usage:**
```bash
# Tracking complet (balances + transactions)
python smart_wallet_analysis/tracking_live/run.py

# Balances uniquement (sans mise à jour historique)
python smart_wallet_analysis/tracking_live/run.py --balance-only

# Transactions uniquement
python smart_wallet_analysis/tracking_live/run.py --transactions-only

# Configuration personnalisée
python smart_wallet_analysis/tracking_live/run.py --min-usd 1000 --hours-lookback 48
```

---

## 📊 Pipeline (2 Phases)

### Phase 1: Détection des Changements
**Fichier:** `live_wallet_balances_extractor_zerion.py`

**Fonction:** `run_live_wallet_changes_tracker()`

**Ce qu'il fait:**
1. Récupère les balances actuelles de tous les smart wallets
2. Compare avec les balances précédentes (table `tokens`)
3. Détecte les changements (nouveaux tokens, quantités modifiées, disparitions)
4. Stocke les changements dans `wallet_position_changes`
5. Met à jour les positions actuelles dans `tokens` et `wallets`

**Détection:**
- ✅ **Nouveau token** → `in_portfolio = true`
- 📉 **Quantité modifiée** → Met à jour `amount`, `usd_value`
- ❌ **Token disparu** → `in_portfolio = false`

**Output:**
- Table `wallet_position_changes` (log des changements)
- Mise à jour tables `tokens` et `wallets`

---

### Phase 2: Remplacement Historique
**Fichier:** `live_wallet_transaction_tracker_extractor_zerion.py`

**Fonction:** `run_optimized_transaction_tracking(min_usd=500, hours_lookback=24)`

**Ce qu'il fait:**
1. Récupère les tokens modifiés récemment (Phase 1)
2. Filtre par valeur minimale (`min_usd`)
3. **Supprime** l'ancien historique pour ces tokens
4. **Recréé** l'historique complet depuis le début
5. Sauvegarde dans `transaction_history`

**Pourquoi remplacer l'historique complet ?**
- Évite les doublons
- Garantit la cohérence des données
- Capture les transactions manquées précédemment

**Optimisation:**
- Traite uniquement les tokens modifiés récemment
- Filtre par seuil USD (ignore petites positions)
- Rotation automatique des clés API (rate limiting)

**Output:**
- Table `transaction_history` mise à jour

---

## 🗄️ Tables Modifiées

### wallet_position_changes (créée)
Log de tous les changements détectés

**Colonnes:**
- `wallet_address`
- `token_symbol`
- `change_type` (NEW, MODIFIED, REMOVED)
- `old_amount`, `new_amount`
- `old_usd_value`, `new_usd_value`
- `detected_at` (timestamp)

### tokens (mise à jour)
Positions actuelles

**Modifications:**
- Champ `in_portfolio` (true/false)
- `amount` et `usd_value` mis à jour
- `last_updated` actualisé

### wallets (mise à jour)
Profils wallets

**Modifications:**
- `total_portfolio_value` recalculé
- `last_sync` actualisé

### transaction_history (remplacée partiellement)
Historique des transactions

**Modifications:**
- **Suppression** historique ancien pour tokens modifiés
- **Insertion** historique complet recréé

---

## ⚙️ Options de Configuration

### --balance-only
Lance uniquement la Phase 1 (détection changements)

**Usage:**
```bash
python run.py --balance-only
```

**Quand l'utiliser:**
- Check rapide des changements
- Pas besoin de l'historique complet
- Économiser des appels API

---

### --transactions-only
Lance uniquement la Phase 2 (mise à jour historique)

**Usage:**
```bash
python run.py --transactions-only
```

**Quand l'utiliser:**
- Phase 1 déjà exécutée
- Mise à jour uniquement de l'historique
- Batch processing

---

### --min-usd [montant]
Seuil minimum USD pour tracker les transactions

**Défaut:** 500
**Usage:**
```bash
python run.py --min-usd 1000
```

**Impact:**
- Plus élevé = moins d'API calls, plus rapide
- Plus bas = plus de détails, plus lent

---

### --hours-lookback [heures]
Analyse les changements des dernières X heures

**Défaut:** 24
**Usage:**
```bash
python run.py --hours-lookback 48
```

**Impact:**
- Plus élevé = plus de changements détectés
- Plus bas = focus changements récents uniquement

---

## 🔄 Workflow Typique

### Suivi Quotidien (Matin)
```bash
# Tracking complet
python run.py --min-usd 500
```

### Suivi Rapide (Intraday)
```bash
# Balances uniquement (rapide)
python run.py --balance-only
```

### Analyse Changements Récents
```bash
# Focus 6 dernières heures, positions importantes
python run.py --hours-lookback 6 --min-usd 1000
```

---

## 📊 Exemple de Sortie

```
================================================================================
🚀 TRACKING LIVE COMPLET - WIT V1
================================================================================
⏰ Démarrage: 2024-12-22 14:30:00
🔧 Configuration:
   • Transaction tracking: ✅ Activé
   • Seuil minimum: $500
   • Analyse des dernières: 24h

============================================================
🔍 PHASE 1: DÉTECTION CHANGEMENTS & MISE À JOUR POSITIONS
============================================================

🔄 Traitement smart_wallets...
   • 0xabc... [1/50] ✅ 3 changements détectés
   • 0xdef... [2/50] ✅ 1 changement détecté
   ...

✅ Phase 1 terminée avec succès! (45.2s)
   🔄 Changements détectés et positions mises à jour
   📊 Tables mises à jour: wallet_position_changes, tokens, wallets

============================================================
📈 PHASE 2: REMPLACEMENT HISTORIQUE TOKENS MODIFIÉS
============================================================

📋 15 tokens modifiés à traiter
🔄 Processing token 1/15: PEPE (0xabc...)
   ✅ Historique complet recréé (45 transactions)
...

✅ Phase 2 terminée avec succès! (123.5s)
   📚 Historiques complets remplacés pour tokens modifiés
   📊 Table mise à jour: transaction_history

============================================================
🎉 TRACKING LIVE COMPLET TERMINÉ
============================================================
✅ Phase 1: Détection changements + Mise à jour positions
✅ Phase 2: Remplacement historique complet
⏱️ Durée totale: 168.7s
🏁 Fin: 2024-12-22 14:33:00

📊 Base de données mise à jour:
   • wallet_position_changes   (changements détectés)
   • tokens                    (positions actuelles avec in_portfolio)
   • wallets                   (valeurs de portefeuille)
   • transaction_history       (historiques complets)
================================================================================
```

---

## 🔑 Gestion des API Keys

### Rotation Automatique
Le système utilise 2 clés API Zerion avec rotation automatique:

```python
API_KEYS = [API_KEY_1, API_KEY_2]
```

**Avantages:**
- Double le rate limit
- Continue si une clé rate limitée
- Rotation transparente

**Configuration (.env):**
```bash
ZERION_API_KEY=key_1
ZERION_API_KEY_2=key_2
```

---

## ⚡ Performance

### Phase 1 (Balances)
- ~1 seconde par wallet
- 50 wallets = ~50 secondes
- Optimisé avec batching

### Phase 2 (Historique)
- Varie selon nombre de transactions
- Moyenne: 2-5 secondes par token
- 15 tokens = ~1-2 minutes

### Total
- 50 wallets + 15 tokens = ~2-3 minutes
- Dépend du nombre de changements détectés

---

## 💡 Conseils d'Utilisation

### ✅ Bonnes Pratiques

**Fréquence recommandée:**
- Mode complet: 2-3x par jour
- Mode balance-only: toutes les heures
- Ajuster selon volatilité du marché

**Seuils recommandés:**
- Trading actif: `--min-usd 500`
- Positions importantes: `--min-usd 1000`
- Tous les mouvements: `--min-usd 100`

**Monitoring:**
- Consulter `wallet_position_changes` pour voir activité
- Comparer `in_portfolio` pour voir entrées/sorties
- Analyser patterns temporels

---

### ❌ À Éviter

- ❌ Lancer trop fréquemment (rate limiting API)
- ❌ `--min-usd` trop bas (appels API inutiles)
- ❌ Ignorer les erreurs API (clés invalides)
- ❌ Oublier de vérifier les changements détectés

---

## 🔧 Dépannage

### Erreur: "Rate limit exceeded"
**Solution:** Attendre 1 minute, les clés API rotent automatiquement

### Aucun changement détecté
**Vérification:**
```sql
SELECT * FROM wallet_position_changes
WHERE detected_at > datetime('now', '-24 hours')
ORDER BY detected_at DESC;
```

### Historique non mis à jour
**Vérification:**
```sql
SELECT COUNT(*) FROM transaction_history
WHERE wallet_address = '0xabc...' AND token_symbol = 'PEPE';
```

---

## 📊 Requêtes SQL Utiles

### Changements récents (24h)
```sql
SELECT wallet_address, token_symbol, change_type,
       new_usd_value - old_usd_value as usd_change
FROM wallet_position_changes
WHERE detected_at > datetime('now', '-24 hours')
ORDER BY ABS(new_usd_value - old_usd_value) DESC;
```

### Tokens actifs par wallet
```sql
SELECT wallet_address, COUNT(*) as active_tokens
FROM tokens
WHERE in_portfolio = 1
GROUP BY wallet_address
ORDER BY active_tokens DESC;
```

### Top mouvements du jour
```sql
SELECT wallet_address, token_symbol,
       (new_usd_value - old_usd_value) as change_usd
FROM wallet_position_changes
WHERE DATE(detected_at) = DATE('now')
ORDER BY ABS(change_usd) DESC
LIMIT 10;
```

---

## 📝 Notes

- **Phase 2 est optionnelle** mais recommandée pour cohérence
- **Historique complet remplacé** pour éviter doublons
- **Rotation API automatique** pour contourner rate limits
- **Tables optimisées** avec index pour performance
- **Compatible** avec Score Engine pour re-scoring post-update
