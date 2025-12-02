-- Table pour stocker les analytics détaillées par token
CREATE TABLE IF NOT EXISTS token_analytics (
    id INTEGER PRIMARY KEY,
    wallet_address TEXT NOT NULL,
    token_symbol TEXT NOT NULL,
    contract_address TEXT,
    
    -- Performance FIFO
    total_invested REAL NOT NULL DEFAULT 0,
    total_realized REAL NOT NULL DEFAULT 0,
    current_value REAL NOT NULL DEFAULT 0,
    total_gains REAL NOT NULL DEFAULT 0,  -- total_realized + current_value
    profit_loss REAL NOT NULL DEFAULT 0,  -- total_gains - total_invested
    roi_percentage REAL,
    is_airdrop BOOLEAN NOT NULL DEFAULT FALSE,
    is_winning BOOLEAN,
    status TEXT, -- GAGNANT, PERDANT, NEUTRE, AIRDROP_GAGNANT
    
    -- Comportement
    holding_days INTEGER DEFAULT 0,
    trading_style TEXT, -- QUICK_FLIP, SHORT_SWING, MEDIUM_HOLD, LONG_HOLD
    entry_pattern TEXT, -- SINGLE_BUY, DOUBLE_DOWN, DCA_IN
    exit_pattern TEXT,  -- HOLDING, SINGLE_EXIT, PARTIAL_EXIT, DCA_OUT
    airdrop_ratio REAL DEFAULT 0,
    
    -- Transactions détaillées
    num_achats INTEGER DEFAULT 0,
    num_receptions INTEGER DEFAULT 0,
    num_ventes INTEGER DEFAULT 0,
    num_envois INTEGER DEFAULT 0,
    total_transactions INTEGER DEFAULT 0,
    total_entries INTEGER DEFAULT 0, -- achats + receptions
    total_exits INTEGER DEFAULT 0,   -- ventes + envois
    
    -- Prix et quantités
    weighted_avg_buy_price REAL DEFAULT 0,
    weighted_avg_sell_price REAL DEFAULT 0,
    current_price REAL,
    price_source TEXT, -- DexScreener, CoinGecko, null
    remaining_quantity REAL DEFAULT 0,
    remaining_cost REAL DEFAULT 0, -- coût FIFO restant
    
    -- Position actuelle
    in_portfolio BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Dates importantes
    first_transaction_date TIMESTAMP,
    last_transaction_date TIMESTAMP,
    last_activity_date TIMESTAMP,
    
    -- Métadonnées
    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(wallet_address, token_symbol)
);