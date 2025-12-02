-- Table pour stocker les interactions des smart wallets (> 500$ USD)
-- Modèle: wallet_mere (smart wallet) <-> wallet_fils (interacting wallet)

CREATE TABLE IF NOT EXISTS graph_wallet (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Wallet mère (notre smart wallet)
    wallet_mere TEXT NOT NULL,              -- Smart wallet qu'on analyse
    
    -- Wallet fils (qui interagit avec le wallet mère)
    wallet_fils TEXT NOT NULL,              -- Wallet qui send/receive
    wallet_fils_type TEXT DEFAULT 'EOA',    -- 'EOA' ou 'Smart Contract'
    
    -- Direction de la transaction
    direction TEXT NOT NULL,                -- 'SEND' ou 'RECEIVE' (du point de vue wallet_mere)
    
    -- Informations sur la transaction
    transaction_hash TEXT NOT NULL,         -- Hash unique de la transaction
    transaction_date TIMESTAMP NOT NULL,   -- Date/heure de la transaction
    
    -- Données financières
    amount_usd REAL NOT NULL,              -- Montant en USD (> 500$)
    token_quantity REAL NOT NULL,         -- Nombre de tokens échangés
    token_symbol TEXT NOT NULL,           -- Symbole du token (ETH, USDC, etc.)
    token_contract TEXT,                  -- Adresse du contrat token (NULL pour ETH natif)
    price_per_token REAL,                 -- Prix du token en USD au moment de la transaction
    
    -- Métadonnées
    chain TEXT DEFAULT 'ethereum',        -- Blockchain (ethereum, base, bsc, etc.)
    block_number INTEGER,                 -- Numéro du bloc
    
    -- Timestamps de suivi
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Index pour les performances
    UNIQUE(transaction_hash, wallet_mere, wallet_fils),  -- Éviter les doublons
    
    -- Contraintes
    CHECK(amount_usd >= 500.0),           -- Minimum 500$ USD
    CHECK(wallet_mere != wallet_fils),    -- Pas de self-transfers
    CHECK(direction IN ('SEND', 'RECEIVE')),
    CHECK(wallet_fils_type IN ('EOA', 'Smart Contract'))
);

-- Index pour optimiser les requêtes
CREATE INDEX IF NOT EXISTS idx_graph_wallet_mere ON graph_wallet(wallet_mere);
CREATE INDEX IF NOT EXISTS idx_graph_wallet_fils ON graph_wallet(wallet_fils);
CREATE INDEX IF NOT EXISTS idx_graph_wallet_direction ON graph_wallet(direction);
CREATE INDEX IF NOT EXISTS idx_graph_wallet_date ON graph_wallet(transaction_date);
CREATE INDEX IF NOT EXISTS idx_graph_wallet_token ON graph_wallet(token_symbol);