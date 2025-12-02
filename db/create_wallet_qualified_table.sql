-- Table pour stocker les wallets qualifiés avec leur score composite
CREATE TABLE IF NOT EXISTS wallet_qualified (
    id INTEGER PRIMARY KEY,
    wallet_address TEXT NOT NULL UNIQUE,
    analyse_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Score composite et classification
    final_score REAL NOT NULL,
    classification TEXT NOT NULL, -- ELITE, EXCELLENT, BON, MOYEN, FAIBLE
    
    -- Métriques principales
    weighted_roi REAL NOT NULL,       -- ROI pondéré par investissement
    nb_trades INTEGER NOT NULL,       -- Nombre total de trades
    taux_reussite REAL NOT NULL,      -- Pourcentage de trades gagnants (>=80% ROI)
    total_invested REAL NOT NULL,     -- Montant total investi
    
    -- Répartition des trades
    gagnants INTEGER NOT NULL,        -- Trades avec ROI >= 80%
    perdants INTEGER NOT NULL,        -- Trades avec ROI < 0%
    neutres INTEGER NOT NULL,         -- Trades avec ROI 0-80%
    
    -- Scores détaillés
    roi_score REAL NOT NULL,          -- Score ROI (0-100)
    activity_score REAL NOT NULL,     -- Score activité (0-100)
    success_score REAL NOT NULL,      -- Score réussite (0-100)
    quality_bonus REAL NOT NULL       -- Bonus qualité (0-50)
);

-- Index pour optimiser les requêtes
CREATE INDEX IF NOT EXISTS idx_wallet_qualified_score ON wallet_qualified(final_score DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_qualified_classification ON wallet_qualified(classification);
CREATE INDEX IF NOT EXISTS idx_wallet_qualified_roi ON wallet_qualified(weighted_roi DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_qualified_date ON wallet_qualified(analyse_date DESC);