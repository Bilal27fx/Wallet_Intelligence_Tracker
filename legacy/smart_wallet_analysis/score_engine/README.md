# ⭐ Score Engine

Module d'analyse FIFO et de scoring des wallets avec classification par qualité.

---

## 🎯 Objectif

Analyser et scorer les wallets pour identifier les meilleurs investisseurs crypto.

---

## 🚀 Runner Principal

**Fichier:** `score_engine_runner.py`

**Usage:**
```bash
# Analyse complète
python smart_wallet_analysis/score_engine/score_engine_runner.py

# Avec filtre qualité (wallets exceptionnels uniquement)
python smart_wallet_analysis/score_engine/score_engine_runner.py --quality 0.9

# Sans statistiques détaillées
python smart_wallet_analysis/score_engine/score_engine_runner.py --no-stats
```

---

## 📊 Pipeline (4 Étapes)

### 1. FIFO Analysis
**Fichier:** `fifo_clean_simple.py`

**Fonction:** `run_fifo_analysis()`

**Ce qu'il fait:**
- Algorithme FIFO (First In First Out) par token/wallet
- Calcule: ROI, investissement total, PnL réalisé/non réalisé
- Gère stablecoins ($1.00 fixe) et prix actuels (DexScreener/CoinGecko)

**Output:** Table `token_analytics` (métriques par token)

**Métriques calculées:**
- `total_invested` - Montant total investi
- `roi_percentage` - ROI en %
- `realized_pnl` - Profit/Perte réalisé
- `unrealized_pnl` - Profit/Perte non réalisé

---

### 2. Wallet Scoring
**Fichier:** `wallet_scoring_system.py`

**Fonction:** `score_all_wallets(min_score=20)`

**Ce qu'il fait:**
- Calcule un score composite (0-100+) par wallet
- Filtre les wallets avec score < 20
- Sauvegarde dans `wallet_qualified`

**Formule de score:**
```python
Score = (ROI_pondéré × 0.6) + (Taux_réussite × 0.3) + (Log_trades × 0.1)

ROI_pondéré = Σ(ROI × Investissement) / Σ(Investissement)
Taux_réussite = Trades avec ROI ≥ 80% / Total trades
```

**Critères de qualification:**
- Score ≥ 20
- ROI pondéré ≥ 50%
- Minimum 3 trades

**Output:** Table `wallet_qualified`

---

### 3. Simple Wallet Analyzer
**Fichier:** `simple_wallet_analyzer.py`

**Fonction:** `analyze_qualified_wallets()`

**Ce qu'il fait:**
- Analyse par paliers d'investissement (3K → 12K, pas de 1K)
- Pour chaque palier: ROI, taux réussite, nb trades
- Classifie trades: gagnants (≥80%), perdants (<0%), neutres (0-80%)

**Paliers analysés:**
```
3K, 4K, 5K, 6K, 7K, 8K, 9K, 10K, 11K, 12K
```

**Exemple de métriques par palier:**
```
Palier 5K:
- ROI: 245.6%
- Taux réussite: 65.2%
- Trades: 23 (15 gagnants, 3 perdants, 5 neutres)
```

**Output:** Table `wallet_profiles`

---

### 4. Optimal Threshold Analyzer
**Fichier:** `optimal_threshold_analyzer.py`

**Fonction:** `analyze_all_qualified_wallets(quality_filter=0.0)`

**Ce qu'il fait:**
- Trouve le seuil optimal d'investissement par wallet
- Calcule un score de qualité (0.0-1.0)
- Filtre les wallets de faible qualité

**Algorithme:**
1. Filtre paliers fiables (ROI>0, WinRate≥20%, Trades≥5)
2. Calcule score J_t = 0.6·ROI_norm + 0.4·WinRate + 0.1·log(Trades)
3. Trouve seuil optimal (plateau stable au 60e percentile)
4. Calcule qualité basée sur performances au-dessus du seuil

**Critères de qualité:**
- `q_w < 0.1` → Wallet neutre (exclu)
- `0.1 ≤ q_w < 0.3` → Qualité faible
- `0.3 ≤ q_w < 0.7` → Qualité acceptable
- `0.7 ≤ q_w < 0.9` → Haute qualité
- `q_w ≥ 0.9` → Qualité exceptionnelle

**Output:** Table `smart_wallets`

**Arguments:**
- `--quality 0.9` → Filtre wallets exceptionnels uniquement
- `--show-stats` → Affiche statistiques détaillées

---

## 🗄️ Tables Créées

### token_analytics
Métriques FIFO par token/wallet

**Colonnes principales:**
- `wallet_address`
- `token_symbol`
- `total_invested`
- `roi_percentage`
- `realized_pnl`
- `unrealized_pnl`

### wallet_qualified
Wallets qualifiés avec scores

**Colonnes principales:**
- `wallet_address`
- `score` (0-100+)
- `roi_pondere`
- `taux_reussite`
- `nb_trades`

### wallet_profiles
Analyse détaillée par paliers

**Colonnes principales:**
- `wallet_address`
- `tier_3k_roi`, `tier_3k_taux_reussite`, `tier_3k_nb_trades`, ...
- `tier_4k_roi`, `tier_4k_taux_reussite`, `tier_4k_nb_trades`, ...
- ... (jusqu'à 12K)

### smart_wallets
Wallets exceptionnels avec seuils optimaux

**Colonnes principales:**
- `wallet_address`
- `optimal_threshold` (seuil optimal en K€)
- `quality` (score 0.0-1.0)
- `metrics` (JSON avec détails)

---

## 📈 Métriques Clés

### ROI (Return On Investment)
```
ROI = ((Valeur actuelle - Investissement) / Investissement) × 100
```

### ROI Pondéré
```
ROI_pondéré = Σ(ROI × Investissement) / Σ(Investissement)
```
Donne plus de poids aux trades avec investissement important

### Taux de Réussite
```
Taux = Nombre de trades ROI ≥ 80% / Total trades
```

### Score Composite
```
Score = 0.6 × ROI_pondéré + 0.3 × Taux_réussite + 0.1 × log(1 + nb_trades)
```

---

## 🔍 Exemple de Workflow

```bash
# 1. Analyser tous les wallets
python score_engine_runner.py

# Résultat:
# ✅ FIFO: 1,234 wallets analysés
# ✅ Scoring: 456 wallets qualifiés (score ≥ 20)
# ✅ Paliers: 456 profils créés
# ✅ Seuils optimaux: 123 smart wallets identifiés

# 2. Filtrer wallets exceptionnels uniquement
python score_engine_runner.py --quality 0.9

# Résultat:
# 🎯 15 wallets exceptionnels (qualité ≥ 0.9)
# 🏆 TOP 5:
#    1. 0xabc... | Seuil: 7K | Qualité: 0.95
#    2. 0xdef... | Seuil: 5K | Qualité: 0.93
#    ...
```

---

## ⚙️ Configuration

**Constantes importantes:**

```python
# Scoring
MIN_SCORE = 20              # Score minimum pour qualification
MIN_ROI_PONDERE = 50.0      # ROI pondéré minimum (%)
MIN_TRADES = 3              # Nombre minimum de trades

# Paliers
TIERS = [3000, 4000, ..., 12000]  # Paliers en USD

# Seuils optimaux
MIN_TRADES_THRESHOLD = 5    # Trades minimum par palier
MIN_WINRATE_THRESHOLD = 20  # WinRate minimum (%)
QUALITY_THRESHOLD = 0.1     # Qualité minimum
```

---

## 💡 Conseils d'Utilisation

✅ **Bonnes pratiques:**
- Lancer après Wallet Tracker (données fraîches)
- Utiliser `--quality 0.9` pour filtrer l'élite
- Consulter `wallet_profiles` pour analyse détaillée

❌ **À éviter:**
- Lancer sans données fraîches dans `transaction_history`
- Modifier les constantes sans comprendre l'impact
- Ignorer les avertissements de qualité

---

## 📊 Interprétation des Résultats

### Score ≥ 80
🏆 **Elite** - Excellents investisseurs, suivre leurs mouvements

### Score 50-80
⭐ **Bons** - Solides performances, à surveiller

### Score 20-50
✅ **Qualifiés** - Correctes performances, potentiel

### Score < 20
❌ **Non qualifiés** - Exclus de l'analyse

### Qualité ≥ 0.9
💎 **Exceptionnels** - Consistance remarquable

### Qualité 0.7-0.9
🌟 **Haute qualité** - Très fiables

### Qualité 0.3-0.7
📈 **Acceptable** - À surveiller

### Qualité < 0.3
⚠️ **Faible** - Inconsistant

---

## 📝 Notes

- FIFO = First In First Out (comptabilité classique)
- Stablecoins fixés à $1.00 pour éviter biais
- Prix actuels via DexScreener (fallback CoinGecko)
- Analyse par paliers permet d'identifier le sweet spot d'investissement
