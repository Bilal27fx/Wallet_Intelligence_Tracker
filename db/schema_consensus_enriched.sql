-- ============================================================================
-- SCHÉMA ENRICHI POUR DASHBOARD DE CONSENSUS
-- ============================================================================

-- Table principale des consensus (déjà existante, on ajoute juste des colonnes)
-- consensus_live existe déjà, on va créer les tables associées

-- ============================================================================
-- TABLE 1: Détails des wallets participants par consensus
-- ============================================================================
CREATE TABLE IF NOT EXISTS consensus_wallets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    consensus_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    contract_address TEXT NOT NULL,
    detection_date DATETIME NOT NULL,

    -- Wallet info
    wallet_address TEXT NOT NULL,
    wallet_status TEXT NOT NULL,  -- EXCEPTIONAL, GOOD, AVERAGE
    optimal_threshold_tier INTEGER NOT NULL,  -- 3, 6, 9, 12 (en K$)
    quality_score REAL NOT NULL,

    -- Investment dans ce consensus
    investment_usd REAL NOT NULL,
    transaction_count INTEGER NOT NULL,
    first_buy_date DATETIME NOT NULL,
    last_buy_date DATETIME NOT NULL,

    -- Performance du wallet au seuil optimal
    optimal_roi REAL NOT NULL,
    optimal_winrate REAL NOT NULL,
    optimal_trades INTEGER NOT NULL,
    optimal_gagnants INTEGER NOT NULL,
    optimal_perdants INTEGER NOT NULL,

    -- Performance globale du wallet
    global_roi REAL NOT NULL,
    global_winrate REAL NOT NULL,
    global_trades INTEGER NOT NULL,

    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(consensus_id, wallet_address),
    FOREIGN KEY (consensus_id) REFERENCES consensus_live(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_consensus_wallets_consensus ON consensus_wallets(consensus_id);
CREATE INDEX IF NOT EXISTS idx_consensus_wallets_wallet ON consensus_wallets(wallet_address);
CREATE INDEX IF NOT EXISTS idx_consensus_wallets_detection ON consensus_wallets(detection_date);

-- ============================================================================
-- TABLE 2: Performance par tranche d'investissement pour chaque wallet
-- ============================================================================
CREATE TABLE IF NOT EXISTS wallet_tier_performance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_address TEXT NOT NULL,
    tier_usd INTEGER NOT NULL,  -- 3000, 6000, 9000, 12000

    -- Métriques de performance
    roi_percentage REAL NOT NULL,
    winrate REAL NOT NULL,
    nb_trades INTEGER NOT NULL,
    nb_gagnants INTEGER NOT NULL,
    nb_perdants INTEGER NOT NULL,
    nb_neutres INTEGER NOT NULL,

    total_invested REAL NOT NULL,
    total_profit REAL NOT NULL,

    -- Métriques avancées
    sharpe_ratio REAL,
    max_drawdown REAL,
    avg_gain REAL,
    avg_loss REAL,

    -- Statut
    is_optimal_tier BOOLEAN DEFAULT FALSE,

    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(wallet_address, tier_usd)
);

CREATE INDEX IF NOT EXISTS idx_tier_performance_wallet ON wallet_tier_performance(wallet_address);
CREATE INDEX IF NOT EXISTS idx_tier_performance_tier ON wallet_tier_performance(tier_usd);

-- ============================================================================
-- TABLE 3: Historique des trades par wallet et par tranche
-- ============================================================================
CREATE TABLE IF NOT EXISTS wallet_trades_by_tier (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_address TEXT NOT NULL,
    tier_usd INTEGER NOT NULL,  -- Tranche d'investissement

    -- Trade info
    token_symbol TEXT NOT NULL,
    token_contract TEXT NOT NULL,

    -- Investissement
    total_invested REAL NOT NULL,
    total_quantity REAL NOT NULL,
    avg_buy_price REAL NOT NULL,

    -- Résultat
    roi_percentage REAL NOT NULL,
    profit_usd REAL NOT NULL,

    -- Status
    is_winner BOOLEAN,  -- TRUE si ROI >= 50%, FALSE si < -20%, NULL sinon

    -- Dates
    first_buy_date DATETIME NOT NULL,
    last_transaction_date DATETIME NOT NULL,

    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_trades_tier_wallet ON wallet_trades_by_tier(wallet_address);
CREATE INDEX IF NOT EXISTS idx_trades_tier_tier ON wallet_trades_by_tier(tier_usd);
CREATE INDEX IF NOT EXISTS idx_trades_tier_token ON wallet_trades_by_tier(token_contract);

-- ============================================================================
-- TABLE 4: Métadonnées enrichies des tokens (pour chaque consensus)
-- ============================================================================
CREATE TABLE IF NOT EXISTS token_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contract_address TEXT NOT NULL UNIQUE,
    symbol TEXT NOT NULL,
    name TEXT,

    -- Token info
    decimals INTEGER,
    total_supply TEXT,  -- Peut être très grand
    token_type TEXT,  -- ERC20, BEP20, etc.

    -- Holders
    holder_count INTEGER,
    top_10_holders_percentage REAL,

    -- Contract info
    is_verified BOOLEAN,
    is_proxy BOOLEAN,
    creation_date DATETIME,
    creator_address TEXT,

    -- Social
    website TEXT,
    twitter TEXT,
    telegram TEXT,
    discord TEXT,

    -- Security flags
    is_honeypot BOOLEAN,
    has_mint_function BOOLEAN,
    has_pause_function BOOLEAN,
    can_take_back_ownership BOOLEAN,

    -- Chain
    chain_id TEXT NOT NULL,

    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_token_metadata_contract ON token_metadata(contract_address);
CREATE INDEX IF NOT EXISTS idx_token_metadata_chain ON token_metadata(chain_id);

-- ============================================================================
-- TABLE 5: Historique de prix pour chaque consensus détecté
-- ============================================================================
CREATE TABLE IF NOT EXISTS consensus_price_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    consensus_id INTEGER NOT NULL,
    contract_address TEXT NOT NULL,

    -- Prix
    timestamp DATETIME NOT NULL,
    price_usd REAL NOT NULL,

    -- Volume et liquidité
    volume_24h REAL,
    liquidity_usd REAL,
    market_cap REAL,

    -- Performance depuis détection
    performance_pct REAL,
    time_since_detection_hours INTEGER,

    FOREIGN KEY (consensus_id) REFERENCES consensus_live(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_price_history_consensus ON consensus_price_history(consensus_id);
CREATE INDEX IF NOT EXISTS idx_price_history_timestamp ON consensus_price_history(timestamp);

-- ============================================================================
-- VUE: Résumé complet des consensus avec métriques agrégées
-- ============================================================================
CREATE VIEW IF NOT EXISTS v_consensus_full AS
SELECT
    cl.id,
    cl.symbol,
    cl.contract_address,
    cl.detection_date,
    cl.whale_count,
    cl.total_investment,
    cl.first_buy,
    cl.last_buy,

    -- Métriques token
    cl.price_usd,
    cl.market_cap_circulating,
    cl.liquidity_usd,
    cl.volume_24h,
    cl.price_change_24h,

    -- Métriques consensus
    COUNT(DISTINCT cw.wallet_address) as wallet_count_detailed,
    SUM(CASE WHEN cw.wallet_status = 'EXCEPTIONAL' THEN 1 ELSE 0 END) as exceptional_wallets,
    SUM(CASE WHEN cw.wallet_status = 'GOOD' THEN 1 ELSE 0 END) as good_wallets,
    AVG(cw.optimal_roi) as avg_wallet_roi,
    AVG(cw.optimal_winrate) as avg_wallet_winrate,

    -- Token metadata
    tm.name as token_name,
    tm.holder_count,
    tm.token_type,
    tm.website,
    tm.twitter,

    -- Performance
    (SELECT price_usd FROM consensus_price_history
     WHERE consensus_id = cl.id
     ORDER BY timestamp DESC LIMIT 1) as current_price,

    cl.is_active,
    cl.period_start,
    cl.period_end

FROM consensus_live cl
LEFT JOIN consensus_wallets cw ON cl.id = cw.consensus_id
LEFT JOIN token_metadata tm ON cl.contract_address = tm.contract_address
GROUP BY cl.id;

-- ============================================================================
-- VUE: Top performers par tranche
-- ============================================================================
CREATE VIEW IF NOT EXISTS v_top_wallets_by_tier AS
SELECT
    tier_usd,
    wallet_address,
    roi_percentage,
    winrate,
    nb_trades,
    nb_gagnants,
    total_invested,
    is_optimal_tier,
    RANK() OVER (PARTITION BY tier_usd ORDER BY roi_percentage DESC) as rank_in_tier
FROM wallet_tier_performance
WHERE nb_trades >= 10  -- Minimum pour être considéré
ORDER BY tier_usd, roi_percentage DESC;

-- ============================================================================
-- VUE: Statistiques globales
-- ============================================================================
CREATE VIEW IF NOT EXISTS v_consensus_stats AS
SELECT
    COUNT(*) as total_consensus,
    COUNT(CASE WHEN is_active = 1 THEN 1 END) as active_consensus,
    AVG(whale_count) as avg_whales_per_consensus,
    AVG(total_investment) as avg_investment,
    SUM(total_investment) as total_volume_tracked,

    -- Par période
    COUNT(CASE WHEN detection_date >= datetime('now', '-1 day') THEN 1 END) as consensus_24h,
    COUNT(CASE WHEN detection_date >= datetime('now', '-7 days') THEN 1 END) as consensus_7d,
    COUNT(CASE WHEN detection_date >= datetime('now', '-30 days') THEN 1 END) as consensus_30d

FROM consensus_live;
