-- =====================================================
-- TABLE: wallet_migrations
-- =====================================================
-- Cette table track les migrations de wallets (quand un wallet transfere
-- la majorite de ses fonds vers un nouveau wallet)
-- Permet d'heriter les prix d'achat reels pour calculer la performance correctement
-- =====================================================

CREATE TABLE IF NOT EXISTS wallet_migrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    old_wallet TEXT NOT NULL,           -- Adresse du wallet source (ancien)
    new_wallet TEXT NOT NULL,           -- Adresse du wallet destination (nouveau)
    migration_date TIMESTAMP NOT NULL,  -- Date de detection de la migration
    migration_hash TEXT,                -- Hash de la transaction principale de migration
    tokens_transferred TEXT,            -- JSON: liste des tokens transferes avec leurs prix herités
    total_value_transferred REAL,       -- Valeur totale transferee en USD au moment du transfert
    transfer_percentage REAL,           -- % du portefeuille transfere (pour valider que c'est bien une migration)
    is_validated BOOLEAN DEFAULT 0,     -- Si la migration a ete validee/confirmee
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    FOREIGN KEY (old_wallet) REFERENCES wallets(wallet_address),
    UNIQUE(old_wallet, new_wallet, migration_date)
);

-- Index pour requetes rapides
CREATE INDEX IF NOT EXISTS idx_migrations_old_wallet ON wallet_migrations(old_wallet);
CREATE INDEX IF NOT EXISTS idx_migrations_new_wallet ON wallet_migrations(new_wallet);
CREATE INDEX IF NOT EXISTS idx_migrations_date ON wallet_migrations(migration_date);

-- =====================================================
-- MODIFICATION: transaction_history
-- =====================================================
-- Ajouter une colonne pour stocker le prix d'achat herite
-- lors de migrations de wallets
-- =====================================================

-- Verifier si la colonne existe deja avant de l'ajouter
-- SQLite ne supporte pas IF NOT EXISTS pour ALTER TABLE, donc on utilise un PRAGMA check

ALTER TABLE transaction_history ADD COLUMN inherited_price_per_token REAL DEFAULT NULL;
ALTER TABLE transaction_history ADD COLUMN is_inherited_from_wallet TEXT DEFAULT NULL;

-- Index pour les requetes sur les prix herités
CREATE INDEX IF NOT EXISTS idx_transaction_history_inherited ON transaction_history(wallet_address, symbol)
WHERE inherited_price_per_token IS NOT NULL;
