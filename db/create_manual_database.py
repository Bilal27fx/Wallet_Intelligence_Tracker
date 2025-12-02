#!/usr/bin/env python3
"""
Script pour créer une base de données SÉPARÉE pour les analyses manuelles
Cette BDD aura la même structure que wit_database.db mais sera vide et dédiée aux analyses manuelles
"""

import sqlite3
from pathlib import Path

# Chemin vers la nouvelle BDD manuelle
MANUAL_DB_PATH = Path(__file__).parent.parent / "data" / "db" / "wit_database_manual.db"

def create_manual_database():
    """Crée la base de données pour les analyses manuelles (copie de structure de wit_database.db)"""

    # Créer le dossier si nécessaire
    MANUAL_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Si la BDD existe déjà, demander confirmation
    if MANUAL_DB_PATH.exists():
        print(f"⚠️  La base manuelle existe déjà : {MANUAL_DB_PATH}")
        response = input("Voulez-vous la recréer (toutes les données seront perdues) ? [y/N]: ")
        if response.lower() != 'y':
            print("❌ Opération annulée")
            return False
        MANUAL_DB_PATH.unlink()

    print(f"📂 Création de la base manuelle : {MANUAL_DB_PATH}")

    conn = sqlite3.connect(str(MANUAL_DB_PATH))
    cursor = conn.cursor()

    # ============================================
    # TABLES PRINCIPALES
    # ============================================

    # Table wallets
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS wallets (
        wallet_address TEXT PRIMARY KEY,
        period TEXT,
        total_portfolio_value REAL,
        token_count INTEGER DEFAULT 0,
        is_active BOOLEAN DEFAULT TRUE,
        transactions_extracted BOOLEAN DEFAULT 0,
        is_scored BOOLEAN DEFAULT 0,
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
        in_portfolio BOOLEAN DEFAULT 1,
        last_transaction_date TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (wallet_address) REFERENCES wallets(wallet_address),
        UNIQUE(wallet_address, fungible_id)
    );
    """)
    print("✅ Table tokens créée")

    # Table transaction_history
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
    print("✅ Table transaction_history créée")

    # Table token_analytics
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS token_analytics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet_address TEXT NOT NULL,
        token_symbol TEXT NOT NULL,
        contract_address TEXT,

        -- Métriques financières
        total_invested REAL DEFAULT 0,
        total_realized REAL DEFAULT 0,
        current_value REAL DEFAULT 0,
        total_gains REAL DEFAULT 0,
        profit_loss REAL DEFAULT 0,
        roi_percentage REAL DEFAULT 0,

        -- Classification
        is_airdrop BOOLEAN DEFAULT 0,
        is_winning BOOLEAN,
        status TEXT,

        -- Comportement de trading
        holding_days INTEGER DEFAULT 0,
        trading_style TEXT,
        entry_pattern TEXT,
        exit_pattern TEXT,
        airdrop_ratio REAL DEFAULT 0,

        -- Statistiques de transactions
        num_achats INTEGER DEFAULT 0,
        num_receptions INTEGER DEFAULT 0,
        num_ventes INTEGER DEFAULT 0,
        num_envois INTEGER DEFAULT 0,
        total_transactions INTEGER DEFAULT 0,
        total_entries INTEGER DEFAULT 0,
        total_exits INTEGER DEFAULT 0,

        -- Prix
        weighted_avg_buy_price REAL,
        weighted_avg_sell_price REAL,
        current_price REAL,
        price_source TEXT,

        -- Position actuelle
        remaining_quantity REAL DEFAULT 0,
        in_portfolio BOOLEAN DEFAULT 1,

        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        FOREIGN KEY (wallet_address) REFERENCES wallets(wallet_address),
        UNIQUE(wallet_address, token_symbol)
    );
    """)
    print("✅ Table token_analytics créée")

    # Table wallet_profiles
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS wallet_profiles (
        wallet_address TEXT PRIMARY KEY,

        -- Scores et métriques principales
        total_score REAL DEFAULT 0,
        roi_global REAL DEFAULT 0,
        taux_reussite REAL DEFAULT 0,
        jours_derniere_activite INTEGER DEFAULT 0,

        -- Métriques financières globales
        capital_investi REAL DEFAULT 0,
        gains_realises REAL DEFAULT 0,
        valeur_actuelle REAL DEFAULT 0,
        gains_totaux REAL DEFAULT 0,
        profit_net REAL DEFAULT 0,

        -- Répartition des tokens
        total_tokens INTEGER DEFAULT 0,
        tokens_gagnants INTEGER DEFAULT 0,
        tokens_neutres INTEGER DEFAULT 0,
        tokens_perdants INTEGER DEFAULT 0,
        tokens_airdrops INTEGER DEFAULT 0,

        -- Skill vs Chance
        gains_airdrops REAL DEFAULT 0,
        gains_trading REAL DEFAULT 0,
        ratio_skill_chance REAL DEFAULT 50,

        -- Tier Petits (<10K)
        petits_count INTEGER DEFAULT 0,
        petits_gagnants INTEGER DEFAULT 0,
        petits_roi REAL DEFAULT 0,
        petits_reussite REAL DEFAULT 0,
        petits_investi REAL DEFAULT 0,
        petits_retour REAL DEFAULT 0,

        -- Tier Gros (10K-50K)
        gros_count INTEGER DEFAULT 0,
        gros_gagnants INTEGER DEFAULT 0,
        gros_roi REAL DEFAULT 0,
        gros_reussite REAL DEFAULT 0,
        gros_investi REAL DEFAULT 0,
        gros_retour REAL DEFAULT 0,

        -- Tier Whales (>50K)
        whales_count INTEGER DEFAULT 0,
        whales_gagnants INTEGER DEFAULT 0,
        whales_roi REAL DEFAULT 0,
        whales_reussite REAL DEFAULT 0,
        whales_investi REAL DEFAULT 0,
        whales_retour REAL DEFAULT 0,

        -- Meilleure tranche
        best_tranche TEXT,
        best_tranche_name TEXT,
        best_tranche_roi REAL DEFAULT 0,
        best_tranche_success REAL DEFAULT 0,
        best_tranche_winners INTEGER DEFAULT 0,
        consistency_factor REAL DEFAULT 1.0,

        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        FOREIGN KEY (wallet_address) REFERENCES wallets(wallet_address)
    );
    """)
    print("✅ Table wallet_profiles créée")

    # Table smart_wallets (wallets avec score >= 40)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS smart_wallets (
        wallet_address TEXT PRIMARY KEY,
        rank INTEGER,

        -- Scores et métriques principales
        total_score REAL DEFAULT 0,
        score_final REAL DEFAULT 0,
        roi_global REAL DEFAULT 0,
        taux_reussite REAL DEFAULT 0,
        jours_derniere_activite INTEGER DEFAULT 0,

        -- Métriques financières globales
        capital_investi REAL DEFAULT 0,
        gains_realises REAL DEFAULT 0,
        valeur_actuelle REAL DEFAULT 0,
        gains_totaux REAL DEFAULT 0,
        profit_net REAL DEFAULT 0,
        total_current_value REAL DEFAULT 0,

        -- Répartition des tokens
        total_tokens INTEGER DEFAULT 0,
        tokens_gagnants INTEGER DEFAULT 0,
        tokens_neutres INTEGER DEFAULT 0,
        tokens_perdants INTEGER DEFAULT 0,
        tokens_airdrops INTEGER DEFAULT 0,

        -- Skill vs Chance
        gains_airdrops REAL DEFAULT 0,
        gains_trading REAL DEFAULT 0,
        ratio_skill_chance REAL DEFAULT 50,

        -- Tier Petits (<10K)
        petits_count INTEGER DEFAULT 0,
        petits_gagnants INTEGER DEFAULT 0,
        petits_roi REAL DEFAULT 0,
        petits_reussite REAL DEFAULT 0,
        petits_investi REAL DEFAULT 0,
        petits_retour REAL DEFAULT 0,

        -- Tier Gros (10K-50K)
        gros_count INTEGER DEFAULT 0,
        gros_gagnants INTEGER DEFAULT 0,
        gros_roi REAL DEFAULT 0,
        gros_reussite REAL DEFAULT 0,
        gros_investi REAL DEFAULT 0,
        gros_retour REAL DEFAULT 0,

        -- Tier Whales (>50K)
        whales_count INTEGER DEFAULT 0,
        whales_gagnants INTEGER DEFAULT 0,
        whales_roi REAL DEFAULT 0,
        whales_reussite REAL DEFAULT 0,
        whales_investi REAL DEFAULT 0,
        whales_retour REAL DEFAULT 0,

        -- Meilleure tranche
        best_tranche TEXT,
        best_tranche_name TEXT,
        best_tranche_roi REAL DEFAULT 0,
        best_tranche_success REAL DEFAULT 0,
        best_tranche_winners INTEGER DEFAULT 0,
        consistency_factor REAL DEFAULT 1.0,

        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    print("✅ Table smart_wallets créée")

    # ============================================
    # INDEX POUR PERFORMANCES
    # ============================================

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallets_portfolio ON wallets(total_portfolio_value);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallets_active ON wallets(is_active);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallets_extracted ON wallets(transactions_extracted);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallets_scored ON wallets(is_scored);")

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tokens_wallet ON tokens(wallet_address);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tokens_symbol ON tokens(symbol);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tokens_portfolio ON tokens(in_portfolio);")

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_history_wallet ON transaction_history(wallet_address);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_history_symbol ON transaction_history(symbol);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_history_date ON transaction_history(date);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_history_hash ON transaction_history(hash);")

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_token_analytics_wallet ON token_analytics(wallet_address);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_token_analytics_symbol ON token_analytics(token_symbol);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_token_analytics_status ON token_analytics(status);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_token_analytics_portfolio ON token_analytics(in_portfolio);")

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallet_profiles_score ON wallet_profiles(total_score);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallet_profiles_roi ON wallet_profiles(roi_global);")

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_smart_wallets_rank ON smart_wallets(rank);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_smart_wallets_score ON smart_wallets(score_final);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_smart_wallets_total_score ON smart_wallets(total_score);")

    print("✅ Index créés")

    conn.commit()
    conn.close()

    print(f"\n✅ Base de données manuelle créée avec succès : {MANUAL_DB_PATH}")

    # Test rapide
    conn = sqlite3.connect(str(MANUAL_DB_PATH))
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    conn.close()

    print(f"\n📋 Tables créées : {len(tables)}")
    for table in tables:
        print(f"   • {table[0]}")

    print(f"\n💡 Cette base de données sera utilisée exclusivement pour les analyses manuelles")
    print(f"💡 Les analyses automatiques continuent d'utiliser wit_database.db")

    return True

if __name__ == "__main__":
    print("🎯 CRÉATION BASE DE DONNÉES POUR ANALYSES MANUELLES")
    print("=" * 60)
    create_manual_database()
