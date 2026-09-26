#!/usr/bin/env python3
"""
Migration script to fix the UNIQUE constraint in token_analytics table.
Changes from UNIQUE(wallet_address, token_symbol) to UNIQUE(wallet_address, contract_address)
to allow multiple tokens with the same symbol but different contract addresses.
"""

import sqlite3
from pathlib import Path

from smart_wallet_analysis.logger import get_logger

# Configuration
DB_PATH = Path(__file__).parent.parent / "data" / "db" / "wit_database.db"
logger = get_logger("db.fix_token_analytics_unique_constraint")

def fix_token_analytics_unique_constraint():
    """Fix the unique constraint in token_analytics table"""
    
    logger.info("🔧 === MIGRATION: FIX TOKEN_ANALYTICS UNIQUE CONSTRAINT ===")
    logger.info("📋 Problème: Contrainte UNIQUE(wallet_address, token_symbol) empêche")
    logger.info("📋 de sauvegarder plusieurs tokens avec le même symbole mais différents contrats")
    logger.info("📋 Solution: Changer vers UNIQUE(wallet_address, contract_address)")
    logger.info("=" * 80)
    
    try:
        # Connexion à la base
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        
        # 1. Vérifier l'état actuel de la table
        logger.info("🔍 Vérification de l'état actuel...")
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='token_analytics'")
        current_schema = cursor.fetchone()
        
        if current_schema:
            logger.info(f"📋 Schéma actuel trouvé")
            if "UNIQUE(wallet_address, token_symbol)" in current_schema[0]:
                logger.info("❌ Contrainte problématique détectée: UNIQUE(wallet_address, token_symbol)")
            elif "UNIQUE(wallet_address, contract_address)" in current_schema[0]:
                logger.info("✅ Contrainte correcte déjà en place: UNIQUE(wallet_address, contract_address)")
                logger.info("🎯 Aucune migration nécessaire!")
                conn.close()
                return True
            else:
                logger.info("⚠️ Contrainte UNIQUE non trouvée dans le schéma")
        else:
            logger.info("❌ Table token_analytics non trouvée!")
            conn.close()
            return False
        
        # 2. Compter les données existantes
        cursor.execute("SELECT COUNT(*) FROM token_analytics")
        total_records = cursor.fetchone()[0]
        logger.info(f"📊 {total_records} enregistrements existants dans token_analytics")
        
        # 3. Identifier les conflits potentiels
        cursor.execute("""
            SELECT wallet_address, token_symbol, COUNT(*) as count
            FROM token_analytics 
            GROUP BY wallet_address, token_symbol 
            HAVING COUNT(*) > 1
        """)
        conflicts = cursor.fetchall()
        
        if conflicts:
            logger.info(f"⚠️ {len(conflicts)} conflits détectés (même wallet + même symbol):")
            for wallet, symbol, count in conflicts:
                logger.info(f"   - {wallet[:12]}... + {symbol}: {count} entrées")
                
                # Afficher les détails des conflits
                cursor.execute("""
                    SELECT contract_address, total_invested, current_value 
                    FROM token_analytics 
                    WHERE wallet_address = ? AND token_symbol = ?
                """, (wallet, symbol))
                details = cursor.fetchall()
                for i, (contract, invested, current) in enumerate(details, 1):
                    logger.info(f"     {i}. Contract: {contract[:12]}... | Investi: ${invested:,.0f} | Actuel: ${current:,.0f}")
        else:
            logger.info("✅ Aucun conflit détecté")
        
        # 4. Créer la nouvelle table avec la contrainte corrigée
        logger.info("\n🔨 Création de la nouvelle table...")
        cursor.execute("""
            CREATE TABLE token_analytics_new (
                id INTEGER PRIMARY KEY,
                wallet_address TEXT NOT NULL,
                token_symbol TEXT NOT NULL,
                contract_address TEXT,
                
                -- Performance FIFO
                total_invested REAL NOT NULL DEFAULT 0,
                total_realized REAL NOT NULL DEFAULT 0,
                current_value REAL NOT NULL DEFAULT 0,
                total_gains REAL NOT NULL DEFAULT 0,
                profit_loss REAL NOT NULL DEFAULT 0,
                roi_percentage REAL,
                is_airdrop BOOLEAN NOT NULL DEFAULT FALSE,
                is_winning BOOLEAN,
                status TEXT,
                
                -- Comportement
                holding_days INTEGER DEFAULT 0,
                trading_style TEXT,
                entry_pattern TEXT,
                exit_pattern TEXT,
                airdrop_ratio REAL DEFAULT 0,
                
                -- Transactions détaillées
                num_achats INTEGER DEFAULT 0,
                num_receptions INTEGER DEFAULT 0,
                num_ventes INTEGER DEFAULT 0,
                num_envois INTEGER DEFAULT 0,
                total_transactions INTEGER DEFAULT 0,
                total_entries INTEGER DEFAULT 0,
                total_exits INTEGER DEFAULT 0,
                
                -- Prix et quantités
                weighted_avg_buy_price REAL DEFAULT 0,
                weighted_avg_sell_price REAL DEFAULT 0,
                current_price REAL,
                price_source TEXT,
                remaining_quantity REAL DEFAULT 0,
                remaining_cost REAL DEFAULT 0,
                
                -- Position actuelle
                in_portfolio BOOLEAN NOT NULL DEFAULT TRUE,
                
                -- Dates importantes
                first_transaction_date TIMESTAMP,
                last_transaction_date TIMESTAMP,
                last_activity_date TIMESTAMP,
                
                -- Métadonnées
                analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                scorable_airdrop INTEGER DEFAULT 1,
                is_goodairdrop BOOLEAN NOT NULL DEFAULT FALSE,
                avg_buy_price REAL DEFAULT 0,
                avg_sell_price REAL DEFAULT 0,
                is_analysed BOOLEAN NOT NULL DEFAULT FALSE,
                
                -- CONTRAINTE CORRIGÉE: utiliser contract_address au lieu de token_symbol
                UNIQUE(wallet_address, contract_address)
            )
        """)
        logger.info("✅ Nouvelle table créée avec contrainte UNIQUE(wallet_address, contract_address)")
        
        # 5. Copier les données en gérant les doublons
        logger.info("📋 Migration des données...")
        
        # Copier toutes les données, la nouvelle contrainte gérera les doublons automatiquement
        cursor.execute("""
            INSERT OR IGNORE INTO token_analytics_new
            SELECT * FROM token_analytics
        """)
        
        migrated_count = cursor.rowcount
        logger.info(f"✅ {migrated_count} enregistrements migrés")
        
        # 6. Vérifier que toutes les données importantes ont été migrées
        cursor.execute("SELECT COUNT(*) FROM token_analytics_new")
        new_total = cursor.fetchone()[0]
        
        logger.info(f"📊 Avant: {total_records} enregistrements")
        logger.info(f"📊 Après: {new_total} enregistrements")
        
        if new_total < total_records:
            logger.info(f"⚠️ {total_records - new_total} enregistrements perdus (doublons supprimés)")
            
            # Identifier quels enregistrements ont été perdus
            cursor.execute("""
                SELECT wallet_address, token_symbol, contract_address, total_invested
                FROM token_analytics 
                WHERE (wallet_address, contract_address) NOT IN (
                    SELECT wallet_address, contract_address FROM token_analytics_new
                )
            """)
            lost_records = cursor.fetchall()
            
            if lost_records:
                logger.info("📋 Enregistrements perdus (doublons):")
                for wallet, symbol, contract, invested in lost_records:
                    logger.info(f"   - {wallet[:12]}... | {symbol} | {contract[:12]}... | ${invested:,.0f}")
        
        # 7. Remplacer l'ancienne table par la nouvelle
        logger.info("\n🔄 Remplacement de l'ancienne table...")
        cursor.execute("DROP TABLE token_analytics")
        cursor.execute("ALTER TABLE token_analytics_new RENAME TO token_analytics")
        
        # 8. Créer les index pour les performances
        logger.info("🏗️ Création des index...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_token_analytics_wallet ON token_analytics(wallet_address)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_token_analytics_symbol ON token_analytics(token_symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_token_analytics_contract ON token_analytics(contract_address)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_token_analytics_roi ON token_analytics(roi_percentage)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_token_analytics_portfolio ON token_analytics(in_portfolio)")
        
        # 9. Valider la migration
        logger.info("\n🔍 Validation de la migration...")
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='token_analytics'")
        new_schema = cursor.fetchone()[0]
        
        if "UNIQUE(wallet_address, contract_address)" in new_schema:
            logger.info("✅ Contrainte UNIQUE correctement mise à jour!")
        else:
            logger.info("❌ Erreur: contrainte UNIQUE non trouvée dans le nouveau schéma")
            conn.rollback()
            conn.close()
            return False
        
        # Commit final
        conn.commit()
        conn.close()
        
        logger.info("\n🎉 === MIGRATION TERMINÉE AVEC SUCCÈS ===")
        logger.info("✅ La table token_analytics utilise maintenant UNIQUE(wallet_address, contract_address)")
        logger.info("✅ Les tokens avec le même symbole mais différents contrats peuvent maintenant être sauvegardés")
        logger.info("✅ Exemple: 2 tokens RUSSELL avec différents contract_address pour le même wallet")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.info(f"❌ Erreur durant la migration: {e}")
        import traceback
        traceback.print_exc()
        
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        
        return False

if __name__ == "__main__":
    success = fix_token_analytics_unique_constraint()
    if success:
        logger.info("\n🎯 Migration réussie! Vous pouvez maintenant relancer l'analyse du wallet.")
        logger.info("🔄 Les 2 tokens RUSSELL seront maintenant correctement sauvegardés.")
    else:
        logger.info("\n❌ Migration échouée. Vérifiez les erreurs ci-dessus.")
