-- Table smart_wallets avec la même structure que wallet_profiles
-- Pour stocker uniquement les wallets avec ROI > 100%
CREATE TABLE IF NOT EXISTS smart_wallets (
    id INTEGER PRIMARY KEY,
    wallet_address TEXT NOT NULL UNIQUE,
    
    -- Performance globale
    total_score REAL DEFAULT 0,
    roi_global REAL DEFAULT 0,
    taux_reussite REAL DEFAULT 0,
    jours_derniere_activite INTEGER DEFAULT 0,
    
    -- Capital et gains
    capital_investi REAL DEFAULT 0,
    gains_realises REAL DEFAULT 0,
    valeur_actuelle REAL DEFAULT 0,
    gains_totaux REAL DEFAULT 0,
    profit_net REAL DEFAULT 0,
    
    -- Répartition tokens
    total_tokens INTEGER DEFAULT 0,
    tokens_gagnants INTEGER DEFAULT 0,
    tokens_neutres INTEGER DEFAULT 0,
    tokens_perdants INTEGER DEFAULT 0,
    tokens_airdrops INTEGER DEFAULT 0,
    
    -- Airdrops vs Trading
    gains_airdrops REAL DEFAULT 0,
    gains_trading REAL DEFAULT 0,
    ratio_skill_chance REAL DEFAULT 0,
    
    -- Performance par tranche (<10k)
    petits_count INTEGER DEFAULT 0,
    petits_gagnants INTEGER DEFAULT 0,
    petits_roi REAL DEFAULT 0,
    petits_reussite REAL DEFAULT 0,
    petits_investi REAL DEFAULT 0,
    petits_retour REAL DEFAULT 0,
    
    -- Performance par tranche (10k-50k)
    gros_count INTEGER DEFAULT 0,
    gros_gagnants INTEGER DEFAULT 0,
    gros_roi REAL DEFAULT 0,
    gros_reussite REAL DEFAULT 0,
    gros_investi REAL DEFAULT 0,
    gros_retour REAL DEFAULT 0,
    
    -- Performance par tranche (>50k)
    whales_count INTEGER DEFAULT 0,
    whales_gagnants INTEGER DEFAULT 0,
    whales_roi REAL DEFAULT 0,
    whales_reussite REAL DEFAULT 0,
    whales_investi REAL DEFAULT 0,
    whales_retour REAL DEFAULT 0,
    
    -- Nouvelle logique de scoring : meilleure tranche
    best_tranche TEXT DEFAULT NULL,                -- petits/gros/whales
    best_tranche_name TEXT DEFAULT NULL,           -- nom complet de la tranche
    best_tranche_roi REAL DEFAULT 0,              -- ROI de la meilleure tranche
    best_tranche_success REAL DEFAULT 0,          -- Taux réussite de la meilleure tranche  
    best_tranche_winners INTEGER DEFAULT 0,       -- Nombre de tokens gagnants dans la meilleure tranche
    consistency_factor REAL DEFAULT 1.0,          -- Facteur de consistance appliqué
    
    -- Date
    analyse_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour optimiser les requêtes
CREATE INDEX IF NOT EXISTS idx_smart_wallets_roi ON smart_wallets(roi_global DESC);
CREATE INDEX IF NOT EXISTS idx_smart_wallets_wallet ON smart_wallets(wallet_address);
CREATE INDEX IF NOT EXISTS idx_smart_wallets_score ON smart_wallets(total_score DESC);