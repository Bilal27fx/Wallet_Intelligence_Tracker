-- Table wallet_profiles version 2 avec métriques détaillées
-- Inclut tous les scores et métriques demandés
CREATE TABLE IF NOT EXISTS wallet_profiles (
    id INTEGER PRIMARY KEY,
    wallet_address TEXT NOT NULL UNIQUE,
    
    -- SCORES FINAUX
    score_total REAL DEFAULT 0,                    -- Score global final /100
    score_global REAL DEFAULT 0,                   -- Score basé ROI global × taux réussite global (/50)
    score_meilleure_tranche REAL DEFAULT 0,        -- Score meilleure tranche (/30)
    score_activite REAL DEFAULT 0,                 -- Score activité (/20)
    
    -- PERFORMANCE GLOBALE (toutes tranches confondues)
    roi_global REAL DEFAULT 0,                     -- ROI global du wallet %
    taux_reussite_global REAL DEFAULT 0,           -- Taux de réussite global %
    nombre_trades_total INTEGER DEFAULT 0,         -- Nombre total de trades
    nombre_trades_gagnants_total INTEGER DEFAULT 0, -- Nombre de trades gagnants total
    
    -- MEILLEURE TRANCHE
    best_tranche_key TEXT DEFAULT NULL,            -- petits/gros/whales
    best_tranche_name TEXT DEFAULT NULL,           -- Nom complet de la tranche
    best_tranche_roi REAL DEFAULT 0,               -- ROI de la meilleure tranche %
    best_tranche_taux_reussite REAL DEFAULT 0,     -- Taux réussite de la meilleure tranche %
    best_tranche_nombre_trades INTEGER DEFAULT 0,  -- Nombre de trades dans la meilleure tranche
    best_tranche_nombre_gagnants INTEGER DEFAULT 0, -- Nombre de trades gagnants dans la meilleure tranche
    consistency_factor REAL DEFAULT 1.0,           -- Facteur de consistance appliqué
    
    -- CAPITAL ET GAINS
    total_investi REAL DEFAULT 0,                  -- Somme totale investie
    total_gains_realises REAL DEFAULT 0,           -- Gains réalisés (vendus)
    total_valeur_actuelle REAL DEFAULT 0,          -- Valeur actuelle du portefeuille
    total_valeur_portefeuille REAL DEFAULT 0,      -- Gains réalisés + valeur actuelle
    profit_net REAL DEFAULT 0,                     -- Profit net total
    
    -- RÉPARTITION TOKENS
    total_tokens INTEGER DEFAULT 0,                -- Total tokens analysés
    tokens_gagnants INTEGER DEFAULT 0,             -- Tokens gagnants
    tokens_neutres INTEGER DEFAULT 0,              -- Tokens neutres
    tokens_perdants INTEGER DEFAULT 0,             -- Tokens perdants
    tokens_airdrops INTEGER DEFAULT 0,             -- Tokens d'airdrops
    tokens_investissements INTEGER DEFAULT 0,      -- Tokens achetés
    
    -- SOURCES DE GAINS
    gains_airdrops REAL DEFAULT 0,                 -- Gains des airdrops
    gains_investissements REAL DEFAULT 0,          -- Gains des investissements
    
    -- PERFORMANCE PAR TRANCHE PETITS (<10k)
    petits_nombre_trades INTEGER DEFAULT 0,
    petits_nombre_gagnants INTEGER DEFAULT 0,
    petits_roi REAL DEFAULT 0,
    petits_taux_reussite REAL DEFAULT 0,
    petits_investi REAL DEFAULT 0,
    petits_valeur_finale REAL DEFAULT 0,
    
    -- PERFORMANCE PAR TRANCHE GROS (10k-50k)
    gros_nombre_trades INTEGER DEFAULT 0,
    gros_nombre_gagnants INTEGER DEFAULT 0,
    gros_roi REAL DEFAULT 0,
    gros_taux_reussite REAL DEFAULT 0,
    gros_investi REAL DEFAULT 0,
    gros_valeur_finale REAL DEFAULT 0,
    
    -- PERFORMANCE PAR TRANCHE WHALES (>50k)
    whales_nombre_trades INTEGER DEFAULT 0,
    whales_nombre_gagnants INTEGER DEFAULT 0,
    whales_roi REAL DEFAULT 0,
    whales_taux_reussite REAL DEFAULT 0,
    whales_investi REAL DEFAULT 0,
    whales_valeur_finale REAL DEFAULT 0,
    
    -- ACTIVITÉ
    jours_derniere_activite INTEGER DEFAULT 0,
    derniere_activite DATE DEFAULT NULL,
    
    -- METADATA
    analyse_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour optimiser les requêtes
CREATE INDEX IF NOT EXISTS idx_wallet_profiles_score ON wallet_profiles(score_total DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_profiles_roi ON wallet_profiles(roi_global DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_profiles_wallet ON wallet_profiles(wallet_address);
CREATE INDEX IF NOT EXISTS idx_wallet_profiles_tranche ON wallet_profiles(best_tranche_key);