#!/usr/bin/env python3
"""
Script simple pour créer les tables SQLite
"""

import sqlite3
from pathlib import Path

# Chemin vers le fichier SQLite
SQLITE_PATH = Path(__file__).parent.parent / "data" / "db" / "wit_database.db"

def create_tables():
    """Crée les tables directement en SQLite"""
    
    # Créer le dossier si nécessaire
    SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Conserver l'ancienne base et ajouter les nouvelles tables
    if SQLITE_PATH.exists():
        print(f"📂 Base existante détectée, ajout des nouvelles tables seulement")
    else:
        print(f"📂 Création d'une nouvelle base")
    
    conn = sqlite3.connect(str(SQLITE_PATH))
    cursor = conn.cursor()
    
    # Table wallets
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS wallets (
        wallet_address TEXT PRIMARY KEY,
        period TEXT,
        total_portfolio_value REAL,
        token_count INTEGER DEFAULT 0,
        is_active BOOLEAN DEFAULT TRUE,
        last_sync TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    print("✅ Table wallets créée")
    
    # Table tokens
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet_address TEXT NOT NULL,
        fungible_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        contract_address TEXT,
        chain TEXT,
        current_amount REAL,
        current_usd_value REAL,
        current_price_per_token REAL,
        transaction_history TEXT,
        last_transaction_date TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (wallet_address) REFERENCES wallets(wallet_address),
        UNIQUE(wallet_address, fungible_id)
    );
    """)
    
    # Table transaction_history pour un stockage structuré
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transaction_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet_address TEXT NOT NULL,
        fungible_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        date TIMESTAMP NOT NULL,
        hash TEXT NOT NULL,
        operation_type TEXT NOT NULL,
        action_type TEXT NOT NULL,
        swap_description TEXT,
        contract_address TEXT,
        quantity REAL,
        price_per_token REAL,
        total_value_usd REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (wallet_address) REFERENCES wallets(wallet_address),
        UNIQUE(hash, wallet_address, fungible_id)
    );
    """)
    print("✅ Table tokens créée")
    print("✅ Table transaction_history créée")
    
    
    # Index pour les performances
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallets_portfolio ON wallets(total_portfolio_value);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tokens_wallet ON tokens(wallet_address);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tokens_symbol ON tokens(symbol);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_history_wallet ON transaction_history(wallet_address);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_history_date ON transaction_history(date);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_history_hash ON transaction_history(hash);")
    
    # === NOUVELLES TABLES POUR TRACKING LIVE DES CHANGEMENTS ===
    
    
    # Table pour historique des changements de positions
    cursor.execute("""
    CREATE TABLE wallet_position_changes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        wallet_address TEXT NOT NULL,
        symbol TEXT NOT NULL,
        contract_address TEXT,
        change_type TEXT NOT NULL, -- 'NEW', 'ACCUMULATION', 'REDUCTION', 'EXIT'
        
        -- Quantités
        old_amount REAL DEFAULT 0,
        new_amount REAL DEFAULT 0,
        amount_change REAL NOT NULL,
        change_percentage REAL,
        
        -- Valeurs USD
        old_usd_value REAL DEFAULT 0,
        new_usd_value REAL DEFAULT 0,
        usd_change REAL NOT NULL,
        
        -- Métadonnées
        detected_at DATETIME NOT NULL,
        price_per_token REAL,
        
        FOREIGN KEY (wallet_address) REFERENCES wallets(wallet_address),
        UNIQUE(session_id, wallet_address, symbol, change_type)
    );
    """)
    print("✅ Table wallet_position_changes créée")
    
    # Table wallet_brute pour remplacer les CSV top_wallets
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS wallet_brute (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet_address TEXT NOT NULL,
        token_address TEXT NOT NULL,
        token_symbol TEXT,
        contract_address TEXT NOT NULL,
        chain TEXT NOT NULL,
        temporality TEXT NOT NULL,
        detection_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        UNIQUE(wallet_address, token_address, temporality)
    );
    """)
    print("✅ Table wallet_brute créée")
    
    # Table smart_wallets (référence pour les jointures)
    cursor.execute("""
    CREATE TABLE smart_wallets (
        wallet_address TEXT PRIMARY KEY,
        rank INTEGER,
        final_score REAL,
        total_current_value REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    print("✅ Table smart_wallets créée")
    
    # Index pour performance des nouvelles tables
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallet_brute_token ON wallet_brute(token_address);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallet_brute_wallet ON wallet_brute(wallet_address);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallet_brute_chain ON wallet_brute(chain);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallet_brute_temporality ON wallet_brute(temporality);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallet_brute_detection_date ON wallet_brute(detection_date);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_changes_wallet_symbol ON wallet_position_changes(wallet_address, symbol);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_changes_detected_at ON wallet_position_changes(detected_at);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_changes_session ON wallet_position_changes(session_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_changes_type ON wallet_position_changes(change_type);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_smart_wallets_rank ON smart_wallets(rank);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_smart_wallets_score ON smart_wallets(final_score);")
    
    # Table pour stocker les prix historiques des consensus
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS consensus_prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token_symbol TEXT NOT NULL,
        contract_address TEXT NOT NULL,
        chain TEXT NOT NULL DEFAULT 'ethereum',
        consensus_date TIMESTAMP NOT NULL,
        days_since_consensus INTEGER NOT NULL,
        
        -- Prix et volume du jour
        price_date DATE NOT NULL,
        nb_trades INTEGER,
        avg_price_usd REAL,
        vwap_price_usd REAL,
        volume_usd REAL,
        volume_token REAL,
        
        -- Métadonnées
        execution_id TEXT,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        UNIQUE(contract_address, price_date)
    );
    """)
    print("✅ Table consensus_prices créée")
    
    # Index pour consensus_prices
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_consensus_prices_contract ON consensus_prices(contract_address);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_consensus_prices_symbol ON consensus_prices(token_symbol);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_consensus_prices_date ON consensus_prices(price_date);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_consensus_prices_consensus_date ON consensus_prices(consensus_date);")
    
    print("✅ Index créés")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Base SQLite créée: {SQLITE_PATH}")
    
    # Test rapide
    conn = sqlite3.connect(str(SQLITE_PATH))
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    conn.close()
    
    print(f"📋 Tables créées: {len(tables)}")
    for table in tables:
        print(f"   • {table[0]}")

if __name__ == "__main__":
    create_tables()