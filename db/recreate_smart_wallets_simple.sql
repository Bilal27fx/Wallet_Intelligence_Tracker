-- Supprimer et recréer la table smart_wallets avec uniquement les colonnes essentielles

DROP TABLE IF EXISTS smart_wallets;

CREATE TABLE smart_wallets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_address TEXT UNIQUE NOT NULL,
    
    -- Seuil optimal et qualité
    optimal_threshold_tier INTEGER NOT NULL,    -- Seuil optimal τ_w (3K-12K)
    quality_score REAL NOT NULL,               -- Qualité q_w (0-1)
    threshold_status TEXT NOT NULL,            -- NEUTRAL, EXCELLENT, GOOD, AVERAGE, POOR, NO_RELIABLE_TIERS
    
    -- Métriques de la tranche optimale
    optimal_roi REAL NOT NULL,                 -- ROI moyen au seuil optimal
    optimal_winrate REAL NOT NULL,            -- Taux de réussite au seuil optimal
    optimal_trades INTEGER NOT NULL,          -- Nombre de trades au seuil optimal
    optimal_gagnants INTEGER NOT NULL,        -- Trades gagnants au seuil optimal
    optimal_perdants INTEGER NOT NULL,        -- Trades perdants au seuil optimal
    optimal_neutres INTEGER NOT NULL,         -- Trades neutres au seuil optimal
    
    -- Métriques globales (tous paliers)
    global_roi REAL NOT NULL,                 -- ROI moyen global
    global_winrate REAL NOT NULL,            -- Taux de réussite global
    global_trades INTEGER NOT NULL,          -- Nombre total de trades
    
    -- Scores techniques
    j_score_max REAL NOT NULL,               -- Score J_t maximum
    j_score_avg REAL NOT NULL,               -- Score J_t moyen
    reliable_tiers_count INTEGER NOT NULL,   -- Nombre de paliers fiables
    
    -- Métadonnées
    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour optimiser les requêtes
CREATE INDEX idx_smart_wallets_wallet ON smart_wallets(wallet_address);
CREATE INDEX idx_smart_wallets_quality ON smart_wallets(quality_score DESC);
CREATE INDEX idx_smart_wallets_threshold ON smart_wallets(optimal_threshold_tier);
CREATE INDEX idx_smart_wallets_status ON smart_wallets(threshold_status);
CREATE INDEX idx_smart_wallets_roi ON smart_wallets(optimal_roi DESC);