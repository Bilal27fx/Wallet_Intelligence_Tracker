"""
Utilitaires pour la base de données MANUELLE (wit_database_manual.db)
Version spécialisée de database_utils.py pour les analyses manuelles
"""

import os
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

# Configuration BDD MANUELLE
SQLITE_PATH = Path(__file__).parent.parent / "data" / "db" / "wit_database_manual.db"

class DatabaseManager:
    """Gestionnaire de base de données pour les analyses manuelles (SQLite uniquement)"""

    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self):
        """Établit la connexion à la base manuelle"""
        try:
            self.connection = sqlite3.connect(str(SQLITE_PATH))
            self.connection.row_factory = sqlite3.Row  # Pour dict-like access
            self.cursor = self.connection.cursor()
            return True
        except Exception as e:
            print(f"❌ Erreur connexion BDD manuelle: {e}")
            return False

    def disconnect(self):
        """Ferme la connexion"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """Exécute une requête SELECT et retourne les résultats"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

            return [dict(row) for row in self.cursor.fetchall()]
        except Exception as e:
            print(f"❌ Erreur requête: {e}")
            return []

    def execute_update(self, query: str, params: tuple = None) -> int:
        """Exécute une requête INSERT/UPDATE/DELETE et retourne le nombre de lignes affectées"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

            self.connection.commit()
            return self.cursor.rowcount
        except Exception as e:
            print(f"❌ Erreur mise à jour: {e}")
            return 0

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()

# =====================================================
# FONCTIONS UTILITAIRES POUR WALLETS
# =====================================================

def get_wallet(wallet_address: str) -> Optional[Dict]:
    """Récupère les infos d'un wallet de la BDD manuelle"""
    with DatabaseManager() as db:
        query = "SELECT * FROM wallets WHERE wallet_address = ?"
        results = db.execute_query(query, (wallet_address,))
        return results[0] if results else None

def insert_wallet(wallet_address: str, period: str, total_value: float = 0.0, token_count: int = 0) -> bool:
    """Insère un nouveau wallet dans la BDD manuelle"""
    with DatabaseManager() as db:
        query = """
        INSERT OR IGNORE INTO wallets (wallet_address, period, total_portfolio_value, token_count, is_active)
        VALUES (?, ?, ?, ?, TRUE)
        """
        affected = db.execute_update(query, (wallet_address, period, total_value, token_count))
        return affected > 0

def update_wallet_value(wallet_address: str, total_value: float) -> bool:
    """Met à jour la valeur totale d'un wallet"""
    with DatabaseManager() as db:
        query = """
        UPDATE wallets
        SET total_portfolio_value = ?, last_sync = CURRENT_TIMESTAMP
        WHERE wallet_address = ?
        """
        affected = db.execute_update(query, (total_value, wallet_address))
        return affected > 0

def mark_wallet_transactions_extracted(wallet_address: str) -> bool:
    """Marque un wallet comme ayant ses transactions extraites"""
    try:
        conn = sqlite3.connect(str(SQLITE_PATH))
        cursor = conn.cursor()

        cursor.execute("""
        UPDATE wallets
        SET transactions_extracted = 1
        WHERE wallet_address = ?
        """, (wallet_address,))

        conn.commit()
        conn.close()

        print(f"✅ Wallet {wallet_address[:10]}... marqué comme traité")
        return True
    except Exception as e:
        print(f"❌ Erreur marquage wallet {wallet_address}: {e}")
        return False

def mark_wallet_scored(wallet_address: str) -> bool:
    """Marque un wallet comme ayant été scoré"""
    try:
        conn = sqlite3.connect(str(SQLITE_PATH))
        cursor = conn.cursor()

        cursor.execute("""
        UPDATE wallets
        SET is_scored = 1
        WHERE wallet_address = ?
        """, (wallet_address,))

        conn.commit()
        conn.close()

        print(f"✅ Wallet {wallet_address[:10]}... marqué comme scoré")
        return True
    except Exception as e:
        print(f"❌ Erreur marquage scoring wallet {wallet_address}: {e}")
        return False

# =====================================================
# FONCTIONS UTILITAIRES POUR TOKENS
# =====================================================

def insert_token(wallet_address: str, fungible_id: str, symbol: str,
                contract_address: str, chain: str, amount: float,
                usd_value: float, price: float, tx_history: List[Dict] = None,
                in_portfolio: bool = True) -> bool:
    """Insère ou met à jour un token dans la BDD manuelle"""
    with DatabaseManager() as db:
        tx_history_json = json.dumps(tx_history or [])

        query = """
        INSERT OR REPLACE INTO tokens
        (wallet_address, fungible_id, symbol, contract_address, chain,
         current_amount, current_usd_value, current_price_per_token, transaction_history, in_portfolio)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        affected = db.execute_update(query, (
            wallet_address, fungible_id, symbol, contract_address, chain,
            amount, usd_value, price, tx_history_json, int(in_portfolio)
        ))
        return affected > 0

def get_wallet_tokens(wallet_address: str) -> List[Dict]:
    """Récupère tous les tokens d'un wallet"""
    with DatabaseManager() as db:
        query = """
        SELECT * FROM tokens
        WHERE wallet_address = ?
        ORDER BY current_usd_value DESC
        """
        return db.execute_query(query, (wallet_address,))

def insert_transaction(wallet_address: str, fungible_id: str, symbol: str, date: str,
                      hash_tx: str, operation_type: str, action_type: str,
                      swap_description: str, contract_address: str, quantity: float,
                      price_per_token: float, total_value_usd: float) -> bool:
    """Insère une transaction dans la table transaction_history de la BDD manuelle"""
    with DatabaseManager() as db:
        query = """
        INSERT OR IGNORE INTO transaction_history
        (wallet_address, fungible_id, symbol, date, hash, operation_type, action_type,
         swap_description, contract_address, quantity, price_per_token, total_value_usd)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        affected = db.execute_update(query, (
            wallet_address, fungible_id, symbol, date, hash_tx, operation_type,
            action_type, swap_description, contract_address, quantity,
            price_per_token, total_value_usd
        ))
        return affected > 0

# =====================================================
# FONCTIONS STATISTIQUES
# =====================================================

def get_database_stats() -> Dict:
    """Récupère les stats générales de la BDD manuelle"""
    with DatabaseManager() as db:
        stats = {}

        # Stats wallets
        result = db.execute_query("SELECT COUNT(*) as count FROM wallets WHERE is_active = TRUE")
        stats['active_wallets'] = result[0]['count'] if result else 0

        # Stats tokens
        result = db.execute_query("SELECT COUNT(*) as count FROM tokens")
        stats['total_tokens'] = result[0]['count'] if result else 0

        # Stats token_analytics
        result = db.execute_query("SELECT COUNT(*) as count FROM token_analytics")
        stats['token_analytics_entries'] = result[0]['count'] if result else 0

        # Valeur totale des portfolios
        result = db.execute_query("SELECT SUM(total_portfolio_value) as total FROM wallets WHERE is_active = TRUE")
        stats['total_portfolio_value'] = result[0]['total'] if result and result[0]['total'] else 0

        return stats

if __name__ == "__main__":
    # Test des fonctions
    print("🔍 Test des fonctions de base de données MANUELLE")

    # Stats
    stats = get_database_stats()
    print(f"📊 Stats BDD Manuelle: {stats}")

    print("✅ Tests terminés")
