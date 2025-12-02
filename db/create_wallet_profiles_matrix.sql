-- Nouvelle structure table wallet_profiles avec matrice de performance
-- Remplace complètement l'ancienne structure

DROP TABLE IF EXISTS wallet_profiles;

CREATE TABLE wallet_profiles (
    id INTEGER PRIMARY KEY,
    wallet_address TEXT NOT NULL UNIQUE,
    
    -- MÉTRIQUES GLOBALES
    total_positions INTEGER NOT NULL DEFAULT 0,
    total_invested REAL NOT NULL DEFAULT 0,
    total_profit_loss REAL NOT NULL DEFAULT 0,
    roi_global REAL NOT NULL DEFAULT 0,           -- ROI global %
    success_rate_global REAL NOT NULL DEFAULT 0, -- Taux réussite global %
    winning_positions INTEGER NOT NULL DEFAULT 0,
    
    -- MATRICE ROI PAR TRANCHE INVESTISSEMENT x TEMPS (9 cases)
    roi_small_3m REAL DEFAULT NULL,    -- <5K, 3 mois
    roi_small_6m REAL DEFAULT NULL,    -- <5K, 6 mois  
    roi_small_12m REAL DEFAULT NULL,   -- <5K, 12 mois
    roi_medium_3m REAL DEFAULT NULL,   -- 5K-10K, 3 mois
    roi_medium_6m REAL DEFAULT NULL,   -- 5K-10K, 6 mois
    roi_medium_12m REAL DEFAULT NULL,  -- 5K-10K, 12 mois
    roi_large_3m REAL DEFAULT NULL,    -- >10K, 3 mois
    roi_large_6m REAL DEFAULT NULL,    -- >10K, 6 mois
    roi_large_12m REAL DEFAULT NULL,   -- >10K, 12 mois
    
    -- MATRICE TAUX RÉUSSITE PAR TRANCHE INVESTISSEMENT x TEMPS (9 cases)
    success_small_3m REAL DEFAULT NULL,    -- <5K, 3 mois
    success_small_6m REAL DEFAULT NULL,    -- <5K, 6 mois
    success_small_12m REAL DEFAULT NULL,   -- <5K, 12 mois
    success_medium_3m REAL DEFAULT NULL,   -- 5K-10K, 3 mois
    success_medium_6m REAL DEFAULT NULL,   -- 5K-10K, 6 mois
    success_medium_12m REAL DEFAULT NULL,  -- 5K-10K, 12 mois
    success_large_3m REAL DEFAULT NULL,    -- >10K, 3 mois
    success_large_6m REAL DEFAULT NULL,    -- >10K, 6 mois
    success_large_12m REAL DEFAULT NULL,   -- >10K, 12 mois
    
    -- MATRICE NOMBRE POSITIONS PAR TRANCHE INVESTISSEMENT x TEMPS (9 cases)
    positions_small_3m INTEGER DEFAULT 0,    -- <5K, 3 mois
    positions_small_6m INTEGER DEFAULT 0,    -- <5K, 6 mois
    positions_small_12m INTEGER DEFAULT 0,   -- <5K, 12 mois
    positions_medium_3m INTEGER DEFAULT 0,   -- 5K-10K, 3 mois
    positions_medium_6m INTEGER DEFAULT 0,   -- 5K-10K, 6 mois
    positions_medium_12m INTEGER DEFAULT 0,  -- 5K-10K, 12 mois
    positions_large_3m INTEGER DEFAULT 0,    -- >10K, 3 mois
    positions_large_6m INTEGER DEFAULT 0,    -- >10K, 6 mois
    positions_large_12m INTEGER DEFAULT 0,   -- >10K, 12 mois
    
    -- MATRICE MONTANTS INVESTIS PAR TRANCHE INVESTISSEMENT x TEMPS (9 cases)
    invested_small_3m REAL DEFAULT 0,    -- <5K, 3 mois
    invested_small_6m REAL DEFAULT 0,    -- <5K, 6 mois
    invested_small_12m REAL DEFAULT 0,   -- <5K, 12 mois
    invested_medium_3m REAL DEFAULT 0,   -- 5K-10K, 3 mois
    invested_medium_6m REAL DEFAULT 0,   -- 5K-10K, 6 mois
    invested_medium_12m REAL DEFAULT 0,  -- 5K-10K, 12 mois
    invested_large_3m REAL DEFAULT 0,    -- >10K, 3 mois
    invested_large_6m REAL DEFAULT 0,    -- >10K, 6 mois
    invested_large_12m REAL DEFAULT 0,   -- >10K, 12 mois
    
    -- SCORES CALCULÉS (pour ranking)
    score_consistency REAL DEFAULT 0,     -- Consistance performance temporelle
    score_diversification REAL DEFAULT 0, -- Diversification tranches investissement
    score_final REAL DEFAULT 0,           -- Score final agrégé
    
    -- MÉTADONNÉES
    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour optimiser les requêtes
CREATE INDEX idx_wallet_profiles_score_final ON wallet_profiles(score_final DESC);
CREATE INDEX idx_wallet_profiles_roi_global ON wallet_profiles(roi_global DESC);
CREATE INDEX idx_wallet_profiles_success_global ON wallet_profiles(success_rate_global DESC);
CREATE INDEX idx_wallet_profiles_total_invested ON wallet_profiles(total_invested DESC);
CREATE INDEX idx_wallet_profiles_analysis_date ON wallet_profiles(analysis_date DESC);