-- Simplification de la table wallet_profiles
-- ROI + taux réussite + compteurs par tranche/période

DROP TABLE IF EXISTS wallet_profiles;

CREATE TABLE wallet_profiles (
    id INTEGER PRIMARY KEY,
    wallet_address TEXT NOT NULL UNIQUE,
    
    -- TOTAL POSITIONS GLOBALES
    total_positions INTEGER DEFAULT 0,
    
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
    
    -- MATRICE NOMBRE DE TRADES PAR TRANCHE INVESTISSEMENT x TEMPS (9 cases)
    trades_small_3m INTEGER DEFAULT 0,    -- <5K, 3 mois
    trades_small_6m INTEGER DEFAULT 0,    -- <5K, 6 mois
    trades_small_12m INTEGER DEFAULT 0,   -- <5K, 12 mois
    trades_medium_3m INTEGER DEFAULT 0,   -- 5K-10K, 3 mois
    trades_medium_6m INTEGER DEFAULT 0,   -- 5K-10K, 6 mois
    trades_medium_12m INTEGER DEFAULT 0,  -- 5K-10K, 12 mois
    trades_large_3m INTEGER DEFAULT 0,    -- >10K, 3 mois
    trades_large_6m INTEGER DEFAULT 0,    -- >10K, 6 mois
    trades_large_12m INTEGER DEFAULT 0,   -- >10K, 12 mois
    
    -- MATRICE NOMBRE DE RÉUSSITES PAR TRANCHE INVESTISSEMENT x TEMPS (9 cases)
    wins_small_3m INTEGER DEFAULT 0,    -- <5K, 3 mois
    wins_small_6m INTEGER DEFAULT 0,    -- <5K, 6 mois
    wins_small_12m INTEGER DEFAULT 0,   -- <5K, 12 mois
    wins_medium_3m INTEGER DEFAULT 0,   -- 5K-10K, 3 mois
    wins_medium_6m INTEGER DEFAULT 0,   -- 5K-10K, 6 mois
    wins_medium_12m INTEGER DEFAULT 0,  -- 5K-10K, 12 mois
    wins_large_3m INTEGER DEFAULT 0,    -- >10K, 3 mois
    wins_large_6m INTEGER DEFAULT 0,    -- >10K, 6 mois
    wins_large_12m INTEGER DEFAULT 0,   -- >10K, 12 mois
    
    -- MÉTADONNÉES
    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour optimiser les requêtes
CREATE INDEX idx_wallet_profiles_address ON wallet_profiles(wallet_address);
CREATE INDEX idx_wallet_profiles_analysis_date ON wallet_profiles(analysis_date DESC);