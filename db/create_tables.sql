-- =====================================================
-- WIT Database Schema - Tables principales
-- =====================================================

-- =====================================================
-- WALLETS : Identité + portfolio value de base
-- =====================================================
CREATE TABLE IF NOT EXISTS wallets (
    wallet_address VARCHAR(42) PRIMARY KEY,
    period VARCHAR(10),                   -- 14d, 30d, 200d, 360d, manual
    total_portfolio_value DECIMAL(20,2),
    is_active BOOLEAN DEFAULT TRUE,
    last_sync TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_period (period),
    INDEX idx_portfolio_value (total_portfolio_value),
    INDEX idx_last_sync (last_sync)
);

-- =====================================================
-- TOKENS : Token + historique complet par wallet
-- =====================================================
CREATE TABLE IF NOT EXISTS tokens (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    wallet_address VARCHAR(42) NOT NULL,
    fungible_id VARCHAR(100) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    contract_address VARCHAR(42),
    chain VARCHAR(20),
    
    -- Balance actuelle
    current_amount DECIMAL(30,18),
    current_usd_value DECIMAL(20,2),
    current_price_per_token DECIMAL(20,8),
    
    -- Historique (JSON pour flexibilité)
    transaction_history LONGTEXT,        -- JSON array des transactions
    last_transaction_date TIMESTAMP,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (wallet_address) REFERENCES wallets(wallet_address) ON DELETE CASCADE,
    UNIQUE KEY unique_wallet_token (wallet_address, fungible_id),
    INDEX idx_wallet (wallet_address),
    INDEX idx_fungible_id (fungible_id),
    INDEX idx_symbol (symbol),
    INDEX idx_contract (contract_address),
    INDEX idx_chain (chain),
    INDEX idx_usd_value (current_usd_value),
    INDEX idx_last_tx_date (last_transaction_date)
);

-- =====================================================
-- SCORING : Tous les scores et analytics
-- =====================================================
CREATE TABLE IF NOT EXISTS scoring (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    wallet_address VARCHAR(42) NOT NULL,
    scoring_type VARCHAR(30) NOT NULL,    -- simple_score, performance_score, etc.
    period VARCHAR(10) NOT NULL,
    
    -- Scores de base
    rank INTEGER,
    final_score DECIMAL(10,4),
    roi_percentage DECIMAL(10,4),
    roi_score DECIMAL(10,2),
    
    -- Profil tokens
    winning_tokens INTEGER,
    neutral_tokens INTEGER,
    losing_tokens INTEGER,
    total_tokens INTEGER,
    winning_tokens_percentage DECIMAL(8,2),
    winning_tokens_score DECIMAL(10,2),
    
    -- Profil activité
    days_since_activity INTEGER,
    last_activity DATE,
    activity_score DECIMAL(10,2),
    
    -- Profil trading (dans scoring comme tu dis)
    profile_type VARCHAR(20),             -- whale, big_whale, trader, holder, degen
    trading_style VARCHAR(20),            -- hodler, swing_trader, day_trader
    risk_profile VARCHAR(20),             -- conservative, moderate, aggressive
    
    -- Valeurs financières
    total_invested DECIMAL(20,2),
    total_gains DECIMAL(20,2),
    total_current_value DECIMAL(20,2),
    
    scoring_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (wallet_address) REFERENCES wallets(wallet_address) ON DELETE CASCADE,
    INDEX idx_wallet_type (wallet_address, scoring_type),
    INDEX idx_rank (rank),
    INDEX idx_final_score (final_score),
    INDEX idx_period (period),
    INDEX idx_scoring_date (scoring_date),
    INDEX idx_profile_type (profile_type)
);

-- =====================================================
-- CACHE : Cache unifié pour toutes les APIs
-- =====================================================
CREATE TABLE IF NOT EXISTS cache (
    cache_key VARCHAR(255) PRIMARY KEY,
    cache_type VARCHAR(30) NOT NULL,      -- zerion_balances, zerion_transactions, dexscreener
    wallet_address VARCHAR(42),           -- NULL si cache global
    fungible_id VARCHAR(100),             -- NULL si pas token spécifique
    data LONGTEXT,                        -- JSON data
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_expiry (expires_at),
    INDEX idx_type_wallet (cache_type, wallet_address),
    INDEX idx_composite (cache_type, wallet_address, fungible_id)
);

-- =====================================================
-- Vue utilitaire pour les top wallets
-- =====================================================
CREATE OR REPLACE VIEW top_wallets AS
SELECT 
    w.wallet_address,
    w.period,
    w.total_portfolio_value,
    s.rank,
    s.final_score,
    s.roi_percentage,
    s.profile_type,
    COUNT(t.id) as token_count,
    w.last_sync
FROM wallets w
LEFT JOIN scoring s ON w.wallet_address = s.wallet_address 
    AND s.scoring_type = 'simple_score'
LEFT JOIN tokens t ON w.wallet_address = t.wallet_address
WHERE w.is_active = TRUE
GROUP BY w.wallet_address, w.period, w.total_portfolio_value, s.rank, s.final_score, s.roi_percentage, s.profile_type, w.last_sync
ORDER BY s.rank ASC, w.total_portfolio_value DESC;

-- =====================================================
-- Procédure de nettoyage du cache
-- =====================================================
DELIMITER //
CREATE PROCEDURE CleanExpiredCache()
BEGIN
    DELETE FROM cache WHERE expires_at < NOW();
    SELECT ROW_COUNT() AS deleted_entries;
END //
DELIMITER ;

-- =====================================================
-- Fonctions utilitaires
-- =====================================================

-- Fonction pour compter les tokens par wallet
DELIMITER //
CREATE FUNCTION GetTokenCount(wallet_addr VARCHAR(42))
RETURNS INT
READS SQL DATA
DETERMINISTIC
BEGIN
    DECLARE token_count INT DEFAULT 0;
    SELECT COUNT(*) INTO token_count 
    FROM tokens 
    WHERE wallet_address = wallet_addr;
    RETURN token_count;
END //
DELIMITER ;

-- Fonction pour obtenir la valeur totale du portfolio
DELIMITER //
CREATE FUNCTION GetPortfolioValue(wallet_addr VARCHAR(42))
RETURNS DECIMAL(20,2)
READS SQL DATA
DETERMINISTIC
BEGIN
    DECLARE portfolio_value DECIMAL(20,2) DEFAULT 0.00;
    SELECT COALESCE(SUM(current_usd_value), 0.00) INTO portfolio_value
    FROM tokens 
    WHERE wallet_address = wallet_addr;
    RETURN portfolio_value;
END //
DELIMITER ;

-- =====================================================
-- Index composites pour performances
-- =====================================================

-- Index pour les requêtes de consensus
CREATE INDEX idx_tokens_wallet_symbol_date ON tokens(wallet_address, symbol, last_transaction_date);

-- Index pour les requêtes de scoring
CREATE INDEX idx_scoring_rank_score ON scoring(scoring_type, rank, final_score);

-- Index pour les requêtes temporelles
CREATE INDEX idx_wallets_sync_active ON wallets(last_sync, is_active);

-- =====================================================
-- Triggers pour maintenir la cohérence
-- =====================================================

-- Trigger pour mettre à jour updated_at sur wallets
DELIMITER //
CREATE TRIGGER wallets_update_timestamp 
BEFORE UPDATE ON wallets
FOR EACH ROW
BEGIN
    SET NEW.updated_at = CURRENT_TIMESTAMP;
END //
DELIMITER ;

-- Trigger pour mettre à jour updated_at sur tokens
DELIMITER //
CREATE TRIGGER tokens_update_timestamp 
BEFORE UPDATE ON tokens
FOR EACH ROW
BEGIN
    SET NEW.updated_at = CURRENT_TIMESTAMP;
END //
DELIMITER ;

-- Trigger pour synchroniser total_portfolio_value dans wallets
DELIMITER //
CREATE TRIGGER sync_portfolio_value_insert
AFTER INSERT ON tokens
FOR EACH ROW
BEGIN
    UPDATE wallets 
    SET total_portfolio_value = GetPortfolioValue(NEW.wallet_address),
        updated_at = CURRENT_TIMESTAMP
    WHERE wallet_address = NEW.wallet_address;
END //
DELIMITER ;

DELIMITER //
CREATE TRIGGER sync_portfolio_value_update
AFTER UPDATE ON tokens
FOR EACH ROW
BEGIN
    UPDATE wallets 
    SET total_portfolio_value = GetPortfolioValue(NEW.wallet_address),
        updated_at = CURRENT_TIMESTAMP
    WHERE wallet_address = NEW.wallet_address;
END //
DELIMITER ;

-- =====================================================
-- Requêtes d'exemple pour tests
-- =====================================================

/*
-- Exemples d'usage après création des tables :

-- 1. Insérer un wallet
INSERT INTO wallets (wallet_address, period, is_active) 
VALUES ('0x1234567890abcdef1234567890abcdef12345678', '30d', TRUE);

-- 2. Insérer des tokens pour ce wallet
INSERT INTO tokens (wallet_address, fungible_id, symbol, current_usd_value) 
VALUES 
('0x1234567890abcdef1234567890abcdef12345678', 'ethereum-pepe', 'PEPE', 5000.00),
('0x1234567890abcdef1234567890abcdef12345678', 'ethereum-chainlink', 'LINK', 3000.00);

-- 3. Ajouter scoring
INSERT INTO scoring (wallet_address, scoring_type, period, rank, final_score)
VALUES ('0x1234567890abcdef1234567890abcdef12345678', 'simple_score', '30d', 1, 95.5);

-- 4. Requête top wallets
SELECT * FROM top_wallets LIMIT 10;

-- 5. Nettoyer le cache
CALL CleanExpiredCache();

-- 6. Stats générales
SELECT 
    COUNT(*) as total_wallets,
    AVG(total_portfolio_value) as avg_portfolio,
    MAX(total_portfolio_value) as max_portfolio
FROM wallets WHERE is_active = TRUE;
*/