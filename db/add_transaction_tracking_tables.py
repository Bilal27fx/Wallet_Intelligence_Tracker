#!/usr/bin/env python3
"""
Script pour ajouter les tables de tracking des transactions
SEULEMENT pour les wallets avec changements détectés
"""

import sqlite3
from pathlib import Path

from smart_wallet_analysis.logger import get_logger

# Chemin vers le fichier SQLite
SQLITE_PATH = Path(__file__).parent.parent / "data" / "db" / "wit_database.db"
logger = get_logger("db.add_transaction_tracking_tables")

def add_transaction_tracking_tables():
    """Ajoute les tables de tracking des transactions sans toucher à l'existant"""
    
    if not SQLITE_PATH.exists():
        logger.info(f"❌ Base de données non trouvée: {SQLITE_PATH}")
        return False
    
    conn = sqlite3.connect(str(SQLITE_PATH))
    cursor = conn.cursor()
    
    try:
        # Table pour snapshot des dernières transactions connues par wallet
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS wallet_transaction_snapshots (
            wallet_address TEXT NOT NULL,
            last_transaction_hash TEXT,
            last_transaction_date DATETIME,
            transaction_count INTEGER DEFAULT 0,
            last_sync DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            
            PRIMARY KEY (wallet_address)
        );
        """)
        logger.info("✅ Table wallet_transaction_snapshots ajoutée")
        
        # Table pour nouvelles transactions détectées depuis le dernier scan
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS wallet_new_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            wallet_address TEXT NOT NULL,
            transaction_hash TEXT NOT NULL,
            block_number INTEGER,
            transaction_timestamp DATETIME,
            from_address TEXT,
            to_address TEXT,
            value_eth REAL,
            gas_used INTEGER,
            gas_price REAL,
            transaction_fee_eth REAL,
            
            -- Détails des tokens transférés (JSON)
            token_transfers TEXT, -- JSON: [{"token":"USDC","amount":1000,"direction":"in"}]
            
            -- Métadonnées de détection
            detected_at DATETIME NOT NULL,
            correlation_with_changes TEXT, -- JSON des changements corrélés
            
            UNIQUE(transaction_hash, wallet_address)
        );
        """)
        logger.info("✅ Table wallet_new_transactions ajoutée")
        
        # Index pour performance optimale
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_snapshots_wallet ON wallet_transaction_snapshots(wallet_address);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_snapshots_sync ON wallet_transaction_snapshots(last_sync);")
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_new_tx_wallet ON wallet_new_transactions(wallet_address);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_new_tx_session ON wallet_new_transactions(session_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_new_tx_hash ON wallet_new_transactions(transaction_hash);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_new_tx_timestamp ON wallet_new_transactions(transaction_timestamp);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_new_tx_detected ON wallet_new_transactions(detected_at);")
        
        logger.info("✅ Index de performance ajoutés")
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ Tables de tracking des transactions ajoutées avec succès")
        
        # Vérifier les nouvelles tables
        conn = sqlite3.connect(str(SQLITE_PATH))
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            AND (name LIKE '%transaction%' OR name LIKE '%position%')
            ORDER BY name
        """)
        tracking_tables = cursor.fetchall()
        conn.close()
        
        logger.info(f"📋 Tables de tracking disponibles:")
        for table in tracking_tables:
            logger.info(f"   • {table[0]}")
        
        return True
        
    except Exception as e:
        logger.info(f"❌ Erreur lors de l'ajout des tables: {e}")
        conn.rollback()
        conn.close()
        return False

def show_table_structure():
    """Affiche la structure des nouvelles tables"""
    conn = sqlite3.connect(str(SQLITE_PATH))
    cursor = conn.cursor()
    
    tables = ['wallet_transaction_snapshots', 'wallet_new_transactions']
    
    for table in tables:
        try:
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            
            logger.info(f"\n📊 Structure de {table}:")
            logger.info(f"{'Colonne':<30} {'Type':<15} {'Contraintes'}")
            logger.info("-" * 60)
            for col in columns:
                name, type_name, not_null, default, pk = col
                constraints = []
                if pk: constraints.append("PRIMARY KEY")
                if not_null: constraints.append("NOT NULL")
                if default: constraints.append(f"DEFAULT {default}")
                
                logger.info(f"{name:<30} {type_name:<15} {', '.join(constraints)}")
                
        except Exception as e:
            logger.info(f"❌ Erreur lecture structure {table}: {e}")
    
    conn.close()

if __name__ == "__main__":
    logger.info("🚀 AJOUT DES TABLES DE TRACKING DES TRANSACTIONS")
    logger.info(f"📂 Base de données: {SQLITE_PATH}")
    logger.info("🎯 OPTIMISATION: Transactions récupérées SEULEMENT pour wallets avec changements")
    
    success = add_transaction_tracking_tables()
    
    if success:
        logger.info("\n✅ SUCCÈS! Les tables de tracking des transactions sont prêtes")
        logger.info("   • Récupération conditionnelle des transactions")
        logger.info("   • Corrélation automatique changements ↔ transactions")
        logger.info("   • Optimisation des calls API blockchain")
        
        # Afficher la structure
        show_table_structure()
        
    else:
        logger.info("\n❌ ÉCHEC! Vérifier les erreurs ci-dessus")
