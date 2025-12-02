-- =====================================================
-- TABLE WALLET_PROFILES - VERSION MULTI-PALIERS
-- =====================================================
-- Structure optimisée pour l'analyse multi-paliers (3K à 12K)
-- Chaque palier a ses propres colonnes de métriques

CREATE TABLE wallet_profiles (
    id INTEGER PRIMARY KEY,
    wallet_address TEXT NOT NULL UNIQUE,
    
    -- METADATA
    total_tokens_all INTEGER DEFAULT 0,        -- Nombre total de tokens dans le wallet
    analyse_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- PALIER 3K (3000€)
    tier_3000_nb_tokens INTEGER DEFAULT 0,
    tier_3000_total_invested REAL DEFAULT 0,
    tier_3000_roi_pondere REAL DEFAULT 0,
    tier_3000_nb_gagnants INTEGER DEFAULT 0,
    tier_3000_nb_perdants INTEGER DEFAULT 0,
    tier_3000_nb_breakeven INTEGER DEFAULT 0,
    tier_3000_taux_reussite REAL DEFAULT 0,
    
    -- PALIER 4K (4000€)
    tier_4000_nb_tokens INTEGER DEFAULT 0,
    tier_4000_total_invested REAL DEFAULT 0,
    tier_4000_roi_pondere REAL DEFAULT 0,
    tier_4000_nb_gagnants INTEGER DEFAULT 0,
    tier_4000_nb_perdants INTEGER DEFAULT 0,
    tier_4000_nb_breakeven INTEGER DEFAULT 0,
    tier_4000_taux_reussite REAL DEFAULT 0,
    
    -- PALIER 5K (5000€)
    tier_5000_nb_tokens INTEGER DEFAULT 0,
    tier_5000_total_invested REAL DEFAULT 0,
    tier_5000_roi_pondere REAL DEFAULT 0,
    tier_5000_nb_gagnants INTEGER DEFAULT 0,
    tier_5000_nb_perdants INTEGER DEFAULT 0,
    tier_5000_nb_breakeven INTEGER DEFAULT 0,
    tier_5000_taux_reussite REAL DEFAULT 0,
    
    -- PALIER 6K (6000€)
    tier_6000_nb_tokens INTEGER DEFAULT 0,
    tier_6000_total_invested REAL DEFAULT 0,
    tier_6000_roi_pondere REAL DEFAULT 0,
    tier_6000_nb_gagnants INTEGER DEFAULT 0,
    tier_6000_nb_perdants INTEGER DEFAULT 0,
    tier_6000_nb_breakeven INTEGER DEFAULT 0,
    tier_6000_taux_reussite REAL DEFAULT 0,
    
    -- PALIER 7K (7000€)
    tier_7000_nb_tokens INTEGER DEFAULT 0,
    tier_7000_total_invested REAL DEFAULT 0,
    tier_7000_roi_pondere REAL DEFAULT 0,
    tier_7000_nb_gagnants INTEGER DEFAULT 0,
    tier_7000_nb_perdants INTEGER DEFAULT 0,
    tier_7000_nb_breakeven INTEGER DEFAULT 0,
    tier_7000_taux_reussite REAL DEFAULT 0,
    
    -- PALIER 8K (8000€)
    tier_8000_nb_tokens INTEGER DEFAULT 0,
    tier_8000_total_invested REAL DEFAULT 0,
    tier_8000_roi_pondere REAL DEFAULT 0,
    tier_8000_nb_gagnants INTEGER DEFAULT 0,
    tier_8000_nb_perdants INTEGER DEFAULT 0,
    tier_8000_nb_breakeven INTEGER DEFAULT 0,
    tier_8000_taux_reussite REAL DEFAULT 0,
    
    -- PALIER 9K (9000€)
    tier_9000_nb_tokens INTEGER DEFAULT 0,
    tier_9000_total_invested REAL DEFAULT 0,
    tier_9000_roi_pondere REAL DEFAULT 0,
    tier_9000_nb_gagnants INTEGER DEFAULT 0,
    tier_9000_nb_perdants INTEGER DEFAULT 0,
    tier_9000_nb_breakeven INTEGER DEFAULT 0,
    tier_9000_taux_reussite REAL DEFAULT 0,
    
    -- PALIER 10K (10000€)
    tier_10000_nb_tokens INTEGER DEFAULT 0,
    tier_10000_total_invested REAL DEFAULT 0,
    tier_10000_roi_pondere REAL DEFAULT 0,
    tier_10000_nb_gagnants INTEGER DEFAULT 0,
    tier_10000_nb_perdants INTEGER DEFAULT 0,
    tier_10000_nb_breakeven INTEGER DEFAULT 0,
    tier_10000_taux_reussite REAL DEFAULT 0,
    
    -- PALIER 11K (11000€)
    tier_11000_nb_tokens INTEGER DEFAULT 0,
    tier_11000_total_invested REAL DEFAULT 0,
    tier_11000_roi_pondere REAL DEFAULT 0,
    tier_11000_nb_gagnants INTEGER DEFAULT 0,
    tier_11000_nb_perdants INTEGER DEFAULT 0,
    tier_11000_nb_breakeven INTEGER DEFAULT 0,
    tier_11000_taux_reussite REAL DEFAULT 0,
    
    -- PALIER 12K (12000€)
    tier_12000_nb_tokens INTEGER DEFAULT 0,
    tier_12000_total_invested REAL DEFAULT 0,
    tier_12000_roi_pondere REAL DEFAULT 0,
    tier_12000_nb_gagnants INTEGER DEFAULT 0,
    tier_12000_nb_perdants INTEGER DEFAULT 0,
    tier_12000_nb_breakeven INTEGER DEFAULT 0,
    tier_12000_taux_reussite REAL DEFAULT 0
);

-- =====================================================
-- INDEX POUR PERFORMANCE
-- =====================================================

-- Index principal sur wallet_address
CREATE INDEX idx_wallet_profiles_wallet ON wallet_profiles(wallet_address);

-- Index sur les paliers les plus utilisés
CREATE INDEX idx_wallet_profiles_tier_3k_roi ON wallet_profiles(tier_3000_roi_pondere DESC);
CREATE INDEX idx_wallet_profiles_tier_6k_roi ON wallet_profiles(tier_6000_roi_pondere DESC);
CREATE INDEX idx_wallet_profiles_tier_12k_roi ON wallet_profiles(tier_12000_roi_pondere DESC);

-- Index sur les taux de réussite
CREATE INDEX idx_wallet_profiles_tier_3k_taux ON wallet_profiles(tier_3000_taux_reussite DESC);
CREATE INDEX idx_wallet_profiles_tier_12k_taux ON wallet_profiles(tier_12000_taux_reussite DESC);

-- Index composé pour analyses rapides
CREATE INDEX idx_wallet_profiles_tier_3k_composite ON wallet_profiles(tier_3000_nb_tokens, tier_3000_roi_pondere DESC);
CREATE INDEX idx_wallet_profiles_tier_12k_composite ON wallet_profiles(tier_12000_nb_tokens, tier_12000_roi_pondere DESC);

-- =====================================================
-- COMMENTAIRES DE STRUCTURE
-- =====================================================

/*
STRUCTURE WALLET_PROFILES MULTI-PALIERS

Cette table analyse chaque wallet sur 10 paliers d'investissement :
- 3K, 4K, 5K, 6K, 7K, 8K, 9K, 10K, 11K, 12K

Pour chaque palier, on calcule :
- nb_tokens : Nombre de tokens avec investissement ≥ seuil
- total_invested : Capital total investi sur ce palier  
- roi_pondere : ROI pondéré par capital investi
- nb_gagnants : Tokens avec ROI ≥ 80%
- nb_perdants : Tokens avec ROI < 0%
- nb_breakeven : Tokens avec ROI entre 0% et 80%
- taux_reussite : % de tokens gagnants (≥80%)

OBJECTIF :
Identifier les wallets qui restent performants même sur des investissements importants.
Les vrais bons traders maintiennent de bonnes performances sur tous les paliers.

UTILISATION :
- Comparer la stabilité cross-paliers
- Identifier les spécialistes de gros tickets vs petits tickets
- Détecter les wallets avec vraie expertise vs chance
*/