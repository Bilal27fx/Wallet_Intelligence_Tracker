-- =====================================================
-- TABLE WALLET_PROFILES - VERSION SIMPLE
-- =====================================================
-- Structure simple pour stocker les données de l'algorithme simple
-- Pour chaque palier: ROI, taux_reussite, nb_trades, gagnants, perdants, neutres

CREATE TABLE wallet_profiles (
    id INTEGER PRIMARY KEY,
    wallet_address TEXT NOT NULL UNIQUE,
    analyse_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- PALIER 3K
    tier_3k_roi REAL DEFAULT 0,
    tier_3k_taux_reussite REAL DEFAULT 0,
    tier_3k_nb_trades INTEGER DEFAULT 0,
    tier_3k_gagnants INTEGER DEFAULT 0,
    tier_3k_perdants INTEGER DEFAULT 0,
    tier_3k_neutres INTEGER DEFAULT 0,
    
    -- PALIER 4K
    tier_4k_roi REAL DEFAULT 0,
    tier_4k_taux_reussite REAL DEFAULT 0,
    tier_4k_nb_trades INTEGER DEFAULT 0,
    tier_4k_gagnants INTEGER DEFAULT 0,
    tier_4k_perdants INTEGER DEFAULT 0,
    tier_4k_neutres INTEGER DEFAULT 0,
    
    -- PALIER 5K
    tier_5k_roi REAL DEFAULT 0,
    tier_5k_taux_reussite REAL DEFAULT 0,
    tier_5k_nb_trades INTEGER DEFAULT 0,
    tier_5k_gagnants INTEGER DEFAULT 0,
    tier_5k_perdants INTEGER DEFAULT 0,
    tier_5k_neutres INTEGER DEFAULT 0,
    
    -- PALIER 6K
    tier_6k_roi REAL DEFAULT 0,
    tier_6k_taux_reussite REAL DEFAULT 0,
    tier_6k_nb_trades INTEGER DEFAULT 0,
    tier_6k_gagnants INTEGER DEFAULT 0,
    tier_6k_perdants INTEGER DEFAULT 0,
    tier_6k_neutres INTEGER DEFAULT 0,
    
    -- PALIER 7K
    tier_7k_roi REAL DEFAULT 0,
    tier_7k_taux_reussite REAL DEFAULT 0,
    tier_7k_nb_trades INTEGER DEFAULT 0,
    tier_7k_gagnants INTEGER DEFAULT 0,
    tier_7k_perdants INTEGER DEFAULT 0,
    tier_7k_neutres INTEGER DEFAULT 0,
    
    -- PALIER 8K
    tier_8k_roi REAL DEFAULT 0,
    tier_8k_taux_reussite REAL DEFAULT 0,
    tier_8k_nb_trades INTEGER DEFAULT 0,
    tier_8k_gagnants INTEGER DEFAULT 0,
    tier_8k_perdants INTEGER DEFAULT 0,
    tier_8k_neutres INTEGER DEFAULT 0,
    
    -- PALIER 9K
    tier_9k_roi REAL DEFAULT 0,
    tier_9k_taux_reussite REAL DEFAULT 0,
    tier_9k_nb_trades INTEGER DEFAULT 0,
    tier_9k_gagnants INTEGER DEFAULT 0,
    tier_9k_perdants INTEGER DEFAULT 0,
    tier_9k_neutres INTEGER DEFAULT 0,
    
    -- PALIER 10K
    tier_10k_roi REAL DEFAULT 0,
    tier_10k_taux_reussite REAL DEFAULT 0,
    tier_10k_nb_trades INTEGER DEFAULT 0,
    tier_10k_gagnants INTEGER DEFAULT 0,
    tier_10k_perdants INTEGER DEFAULT 0,
    tier_10k_neutres INTEGER DEFAULT 0,
    
    -- PALIER 11K
    tier_11k_roi REAL DEFAULT 0,
    tier_11k_taux_reussite REAL DEFAULT 0,
    tier_11k_nb_trades INTEGER DEFAULT 0,
    tier_11k_gagnants INTEGER DEFAULT 0,
    tier_11k_perdants INTEGER DEFAULT 0,
    tier_11k_neutres INTEGER DEFAULT 0,
    
    -- PALIER 12K
    tier_12k_roi REAL DEFAULT 0,
    tier_12k_taux_reussite REAL DEFAULT 0,
    tier_12k_nb_trades INTEGER DEFAULT 0,
    tier_12k_gagnants INTEGER DEFAULT 0,
    tier_12k_perdants INTEGER DEFAULT 0,
    tier_12k_neutres INTEGER DEFAULT 0
);

-- INDEX POUR PERFORMANCE
CREATE INDEX idx_wallet_profiles_wallet ON wallet_profiles(wallet_address);
CREATE INDEX idx_wallet_profiles_12k_roi ON wallet_profiles(tier_12k_roi DESC);
CREATE INDEX idx_wallet_profiles_12k_taux ON wallet_profiles(tier_12k_taux_reussite DESC);