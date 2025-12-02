-- Ajouter les colonnes pour l'analyse de seuil optimal à smart_wallets

-- Seuil optimal et qualité
ALTER TABLE smart_wallets ADD COLUMN optimal_threshold_tier INTEGER DEFAULT NULL;
ALTER TABLE smart_wallets ADD COLUMN quality_score REAL DEFAULT NULL;
ALTER TABLE smart_wallets ADD COLUMN threshold_status TEXT DEFAULT NULL;

-- Métriques du seuil optimal
ALTER TABLE smart_wallets ADD COLUMN reliable_tiers_count INTEGER DEFAULT NULL;
ALTER TABLE smart_wallets ADD COLUMN j_score_max REAL DEFAULT NULL;
ALTER TABLE smart_wallets ADD COLUMN j_score_avg REAL DEFAULT NULL;

-- Performances au-dessus du seuil
ALTER TABLE smart_wallets ADD COLUMN trades_above_threshold INTEGER DEFAULT NULL;
ALTER TABLE smart_wallets ADD COLUMN winrate_above_threshold REAL DEFAULT NULL;
ALTER TABLE smart_wallets ADD COLUMN roi_above_threshold REAL DEFAULT NULL;

-- Performances globales (tous paliers)
ALTER TABLE smart_wallets ADD COLUMN winrate_global REAL DEFAULT NULL;
ALTER TABLE smart_wallets ADD COLUMN roi_global REAL DEFAULT NULL;

-- Métadonnées de l'analyse
ALTER TABLE smart_wallets ADD COLUMN threshold_analysis_date TIMESTAMP DEFAULT NULL;

-- Index pour optimiser les requêtes sur le seuil optimal
CREATE INDEX IF NOT EXISTS idx_smart_wallets_optimal_threshold ON smart_wallets(optimal_threshold_tier);
CREATE INDEX IF NOT EXISTS idx_smart_wallets_quality_score ON smart_wallets(quality_score DESC);
CREATE INDEX IF NOT EXISTS idx_smart_wallets_threshold_status ON smart_wallets(threshold_status);