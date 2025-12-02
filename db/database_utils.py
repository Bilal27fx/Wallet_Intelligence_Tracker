"""
Utilitaires pour la base de données WIT
Connexions, requêtes communes, et helpers
"""

import os
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union
from dotenv import load_dotenv

# Import MySQL connector only if available
try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

# Charger les variables d'environnement
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

# Configuration BDD
DB_TYPE = os.getenv("DB_TYPE", "sqlite")
DB_HOST = os.getenv("DB_HOST", "localhost")  
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_NAME = os.getenv("DB_NAME", "wit_database")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
SQLITE_PATH = Path(__file__).parent.parent / "data" / "db" / "wit_database.db"

class DatabaseManager:
    """Gestionnaire de base de données unifié SQLite/MySQL"""
    
    def __init__(self):
        self.db_type = DB_TYPE.lower()
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """Établit la connexion à la base"""
        try:
            if self.db_type == "sqlite":
                self.connection = sqlite3.connect(str(SQLITE_PATH))
                self.connection.row_factory = sqlite3.Row  # Pour dict-like access
            else:  # MySQL
                if not MYSQL_AVAILABLE:
                    raise ImportError("MySQL connector non disponible")
                self.connection = mysql.connector.connect(
                    host=DB_HOST,
                    port=DB_PORT,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    database=DB_NAME,
                    autocommit=True
                )
            
            self.cursor = self.connection.cursor()
            return True
            
        except Exception as e:
            print(f"❌ Erreur connexion BDD: {e}")
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
            
            # Récupérer les résultats
            if self.db_type == "sqlite":
                return [dict(row) for row in self.cursor.fetchall()]
            else:  # MySQL
                columns = [desc[0] for desc in self.cursor.description]
                rows = self.cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]
                
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
            
            if self.db_type == "sqlite":
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
    """Récupère les infos d'un wallet"""
    with DatabaseManager() as db:
        query = "SELECT * FROM wallets WHERE wallet_address = ?"
        results = db.execute_query(query, (wallet_address,))
        return results[0] if results else None

def insert_wallet(wallet_address: str, period: str, total_value: float = 0.0, token_count: int = 0) -> bool:
    """Insère un nouveau wallet"""
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

def get_active_wallets(limit: int = None) -> List[Dict]:
    """Récupère les wallets actifs"""
    with DatabaseManager() as db:
        query = "SELECT * FROM wallets WHERE is_active = TRUE ORDER BY total_portfolio_value DESC"
        if limit:
            query += f" LIMIT {limit}"
        return db.execute_query(query)

def get_unprocessed_wallets(limit: int = None) -> List[Dict]:
    """Récupère UNIQUEMENT les wallets dont l'historique n'a PAS encore été extrait"""
    conn = sqlite3.connect('data/db/wit_database.db')
    cursor = conn.cursor()
    
    query = """
    SELECT wallet_address, total_portfolio_value, period, created_at
    FROM wallets 
    WHERE (transactions_extracted = 0 OR transactions_extracted IS NULL)
    ORDER BY total_portfolio_value DESC
    """
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    conn.close()
    
    print(f"📊 {len(results)} wallets sans historique extrait trouvés")
    return results

def mark_wallet_transactions_extracted(wallet_address: str) -> bool:
    """Marque un wallet comme ayant ses transactions extraites"""
    try:
        conn = sqlite3.connect('data/db/wit_database.db')
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

def get_unscored_wallets(limit: int = None) -> List[str]:
    """Récupère UNIQUEMENT les wallets qui n'ont PAS encore été scorés"""
    conn = sqlite3.connect('data/db/wit_database.db')
    cursor = conn.cursor()
    
    query = """
    SELECT DISTINCT w.wallet_address, w.total_portfolio_value 
    FROM wallets w
    WHERE w.wallet_address IN (
        SELECT DISTINCT th.wallet_address FROM transaction_history th
    )
    AND (w.is_scored = 0 OR w.is_scored IS NULL)
    ORDER BY w.total_portfolio_value DESC
    """
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query)
    wallet_addresses = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    
    print(f"📊 {len(wallet_addresses)} wallets non scorés trouvés")
    return wallet_addresses

def get_unscored_wallets_with_transactions_extracted(limit: int = None) -> List[str]:
    """Récupère UNIQUEMENT les wallets qui n'ont PAS encore été scorés ET dont les transactions ont été extraites"""
    conn = sqlite3.connect('data/db/wit_database.db')
    cursor = conn.cursor()
    
    query = """
    SELECT DISTINCT w.wallet_address, w.total_portfolio_value 
    FROM wallets w
    WHERE w.wallet_address IN (
        SELECT DISTINCT th.wallet_address FROM transaction_history th
    )
    AND (w.is_scored = 0 OR w.is_scored IS NULL)
    AND w.transactions_extracted = 1
    ORDER BY w.total_portfolio_value DESC
    """
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query)
    wallet_addresses = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    
    print(f"📊 {len(wallet_addresses)} wallets non scorés avec transactions extraites trouvés")
    return wallet_addresses

def mark_wallet_scored(wallet_address: str) -> bool:
    """Marque un wallet comme ayant été scoré"""
    try:
        conn = sqlite3.connect('data/db/wit_database.db')
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
    """Insère ou met à jour un token"""
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

def get_token_transaction_history(wallet_address: str, fungible_id: str) -> List[Dict]:
    """Récupère l'historique des transactions d'un token"""
    with DatabaseManager() as db:
        query = """
        SELECT transaction_history FROM tokens 
        WHERE wallet_address = ? AND fungible_id = ?
        """
        results = db.execute_query(query, (wallet_address, fungible_id))
        
        if results and results[0]['transaction_history']:
            try:
                return json.loads(results[0]['transaction_history'])
            except json.JSONDecodeError:
                return []
        return []

def insert_transaction(wallet_address: str, fungible_id: str, symbol: str, date: str,
                      hash_tx: str, operation_type: str, action_type: str, 
                      swap_description: str, contract_address: str, quantity: float,
                      price_per_token: float, total_value_usd: float) -> bool:
    """Insère une transaction dans la table transaction_history"""
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

def update_token_history(wallet_address: str, fungible_id: str, new_transactions: List[Dict]) -> bool:
    """Met à jour l'historique des transactions d'un token"""
    with DatabaseManager() as db:
        # Récupérer l'historique existant
        existing = get_token_transaction_history(wallet_address, fungible_id)
        
        # Fusionner avec les nouvelles transactions (éviter doublons par hash)
        existing_hashes = {tx.get('hash', '') for tx in existing}
        unique_new = [tx for tx in new_transactions if tx.get('hash', '') not in existing_hashes]
        
        # Combiner et trier par date
        all_transactions = existing + unique_new
        all_transactions.sort(key=lambda x: x.get('date', ''), reverse=True)
        
        # Mettre à jour en BDD
        query = """
        UPDATE tokens 
        SET transaction_history = ?, last_transaction_date = CURRENT_TIMESTAMP
        WHERE wallet_address = ? AND fungible_id = ?
        """
        
        affected = db.execute_update(query, (
            json.dumps(all_transactions), wallet_address, fungible_id
        ))
        return affected > 0

# =====================================================
# FONCTIONS UTILITAIRES POUR SCORING
# =====================================================

def insert_scoring(wallet_address: str, scoring_type: str, period: str, 
                  rank: int, final_score: float, **kwargs) -> bool:
    """Insère un score pour un wallet"""
    with DatabaseManager() as db:
        # Construire la requête dynamiquement selon les kwargs
        base_fields = ['wallet_address', 'scoring_type', 'period', 'rank', 'final_score']
        values = [wallet_address, scoring_type, period, rank, final_score]
        
        extra_fields = []
        for key, value in kwargs.items():
            if key in ['roi_percentage', 'roi_score', 'winning_tokens', 'total_tokens', 
                      'profile_type', 'trading_style', 'total_invested']:
                extra_fields.append(key)
                values.append(value)
        
        all_fields = base_fields + extra_fields
        placeholders = ', '.join(['?' for _ in all_fields])
        fields_str = ', '.join(all_fields)
        
        query = f"""
        INSERT OR REPLACE INTO scoring ({fields_str})
        VALUES ({placeholders})
        """
        
        affected = db.execute_update(query, tuple(values))
        return affected > 0

def update_smart_wallets_ranks():
    """Met à jour les ranks dans la table smart_wallets basés sur score_final"""
    try:
        if DB_TYPE == "sqlite":
            conn = sqlite3.connect(str(SQLITE_PATH))
        else:
            conn = mysql.connector.connect(
                host=DB_HOST, port=DB_PORT, database=DB_NAME,
                user=DB_USER, password=DB_PASSWORD
            )
        
        cursor = conn.cursor()
        
        # Compter les smart wallets existants
        cursor.execute("SELECT COUNT(*) FROM smart_wallets WHERE score_final > 0")
        total_wallets = cursor.fetchone()[0]
        print(f"📊 {total_wallets} smart wallets à classer")
        
        # Mettre à jour les ranks basés sur score_final
        update_query = """
        UPDATE smart_wallets 
        SET rank = (
            SELECT COUNT(*) + 1 
            FROM smart_wallets sw2 
            WHERE sw2.score_final > smart_wallets.score_final
        )
        WHERE score_final > 0
        """
        
        cursor.execute(update_query)
        conn.commit()
        
        print(f"✅ Ranks mis à jour avec succès!")
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Erreur mise à jour ranks: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def get_top_wallets_by_score(scoring_type: str = 'simple_score', limit: int = 50) -> List[Dict]:
    """Récupère les top wallets par score"""
    with DatabaseManager() as db:
        query = """
        SELECT w.wallet_address, w.total_portfolio_value, s.rank, s.final_score,
               s.roi_percentage, s.profile_type, s.total_tokens
        FROM wallets w
        JOIN scoring s ON w.wallet_address = s.wallet_address
        WHERE s.scoring_type = ? AND w.is_active = TRUE
        ORDER BY s.rank ASC
        LIMIT ?
        """
        return db.execute_query(query, (scoring_type, limit))

# =====================================================
# FONCTIONS UTILITAIRES POUR CACHE
# =====================================================

def get_cache(cache_key: str) -> Optional[Dict]:
    """Récupère une entrée du cache"""
    with DatabaseManager() as db:
        query = """
        SELECT data FROM cache 
        WHERE cache_key = ? AND expires_at > CURRENT_TIMESTAMP
        """
        results = db.execute_query(query, (cache_key,))
        
        if results:
            try:
                return json.loads(results[0]['data'])
            except json.JSONDecodeError:
                return None
        return None

def set_cache(cache_key: str, data: Dict, cache_type: str, 
              expiry_hours: int = 24, wallet_address: str = None, 
              fungible_id: str = None) -> bool:
    """Définit une entrée de cache"""
    with DatabaseManager() as db:
        expires_at = datetime.now() + timedelta(hours=expiry_hours)
        
        query = """
        INSERT OR REPLACE INTO cache 
        (cache_key, cache_type, wallet_address, fungible_id, data, expires_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        
        affected = db.execute_update(query, (
            cache_key, cache_type, wallet_address, fungible_id, 
            json.dumps(data), expires_at
        ))
        return affected > 0

def clean_expired_cache() -> int:
    """Nettoie le cache expiré"""
    with DatabaseManager() as db:
        query = "DELETE FROM cache WHERE expires_at < CURRENT_TIMESTAMP"
        return db.execute_update(query)

# =====================================================
# FONCTIONS STATISTIQUES
# =====================================================

def get_database_stats() -> Dict:
    """Récupère les stats générales de la BDD"""
    with DatabaseManager() as db:
        stats = {}
        
        # Stats wallets
        result = db.execute_query("SELECT COUNT(*) as count FROM wallets WHERE is_active = TRUE")
        stats['active_wallets'] = result[0]['count'] if result else 0
        
        # Stats tokens
        result = db.execute_query("SELECT COUNT(*) as count FROM tokens")
        stats['total_tokens'] = result[0]['count'] if result else 0
        
        # Stats scoring
        result = db.execute_query("SELECT COUNT(*) as count FROM scoring")
        stats['scoring_entries'] = result[0]['count'] if result else 0
        
        # Stats cache
        result = db.execute_query("SELECT COUNT(*) as count FROM cache WHERE expires_at > CURRENT_TIMESTAMP")
        stats['active_cache_entries'] = result[0]['count'] if result else 0
        
        # Valeur totale des portfolios
        result = db.execute_query("SELECT SUM(total_portfolio_value) as total FROM wallets WHERE is_active = TRUE")
        stats['total_portfolio_value'] = result[0]['total'] if result and result[0]['total'] else 0
        
        return stats

# =====================================================
# EXEMPLE D'USAGE
# =====================================================

if __name__ == "__main__":
    # Test des fonctions
    print("🔍 Test des fonctions de base de données")
    
    # Stats
    stats = get_database_stats()
    print(f"📊 Stats BDD: {stats}")
    
    # Test cache
    test_data = {"test": "value", "timestamp": datetime.now().isoformat()}
    set_cache("test_key", test_data, "test_cache")
    
    cached = get_cache("test_key")
    print(f"🗂️ Cache test: {cached}")
    
    print("✅ Tests terminés")

def create_token_analytics_table():
    """Crée la table token_analytics si elle n'existe pas"""
    try:
        conn = sqlite3.connect('data/db/wit_database.db')
        cursor = conn.cursor()
        
        # Lire le script SQL
        sql_file = Path(__file__).parent / "create_token_analytics_table.sql"
        with open(sql_file, 'r') as f:
            sql_script = f.read()
        
        cursor.execute(sql_script)
        conn.commit()
        conn.close()
        print("✅ Table token_analytics créée/vérifiée")
        return True
    except Exception as e:
        print(f"❌ Erreur création table token_analytics: {e}")
        return False

def create_wallet_profiles_table():
    """Crée la table wallet_profiles si elle n'existe pas"""
    try:
        conn = sqlite3.connect('data/db/wit_database.db')
        cursor = conn.cursor()
        
        # Lire le script SQL
        sql_file = Path(__file__).parent / "create_wallet_profiles_table.sql"
        with open(sql_file, 'r') as f:
            sql_script = f.read()
        
        cursor.execute(sql_script)
        conn.commit()
        conn.close()
        print("✅ Table wallet_profiles créée/vérifiée")
        return True
    except Exception as e:
        print(f"❌ Erreur création table wallet_profiles: {e}")
        return False

def save_token_analytics_to_db(wallet_address: str, token_results: List[Dict]) -> bool:
    """Sauvegarde les analytics de tokens en base"""
    try:
        conn = sqlite3.connect('data/db/wit_database.db')
        cursor = conn.cursor()
        
        for token in token_results:
            behavior = token.get('behavior', {})
            stats = token.get('stats', {})
            
            # Cap ROI infinity à une valeur très élevée pour la DB
            roi_value = token['roi'] if token['roi'] != float('inf') else 999999
            
            # Calcul des valeurs dérivées
            total_gains = token['gains_totaux'] + token['valeur_actuelle']
            profit_loss = total_gains - token['invested']
            
            # Déterminer le statut
            if token.get('is_airdrop', False):
                if token['gains_totaux'] > 0 or token['valeur_actuelle'] > 0:
                    status = 'AIRDROP_GAGNANT'
                else:
                    status = 'AIRDROP_MORT'
            elif token.get('is_winning') == True:
                status = 'GAGNANT'
            elif token.get('is_winning') == False:
                status = 'PERDANT'
            else:
                status = 'NEUTRE'
            
            # Position actuelle
            in_portfolio = token['remaining_quantity'] > 0.001
            
            # Source prix
            price_source = None
            if token.get('current_price') is not None:
                price_source = 'FOUND'  # On pourrait stocker la vraie source si disponible
            
            cursor.execute("""
                INSERT OR REPLACE INTO token_analytics (
                    wallet_address, token_symbol, contract_address,
                    total_invested, total_realized, current_value, total_gains, profit_loss, roi_percentage,
                    is_airdrop, is_winning, status,
                    holding_days, trading_style, entry_pattern, exit_pattern, airdrop_ratio,
                    num_achats, num_receptions, num_ventes, num_envois,
                    total_transactions, total_entries, total_exits,
                    weighted_avg_buy_price, weighted_avg_sell_price, current_price, price_source,
                    remaining_quantity, in_portfolio
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                wallet_address,
                token['token_symbol'],
                token.get('contract_address', ''),
                token['invested'],
                token['gains_totaux'],
                token['valeur_actuelle'],
                total_gains,
                profit_loss,
                roi_value,
                token.get('is_airdrop', False),
                token.get('is_winning'),
                status,
                behavior.get('holding_days', 0),
                behavior.get('trading_style', ''),
                behavior.get('entry_pattern', ''),
                behavior.get('exit_pattern', ''),
                behavior.get('airdrop_ratio', 0),
                stats.get('achats', behavior.get('num_achats', 0)),
                stats.get('receptions', behavior.get('num_receptions', 0)),
                stats.get('ventes', behavior.get('num_ventes', 0)),
                stats.get('envois', behavior.get('num_envois', 0)),
                stats.get('achats', 0) + stats.get('receptions', 0) + stats.get('ventes', 0) + stats.get('envois', 0),
                stats.get('achats', 0) + stats.get('receptions', 0),
                stats.get('ventes', 0) + stats.get('envois', 0),
                behavior.get('weighted_avg_buy_price', 0),
                behavior.get('weighted_avg_sell_price', 0),
                token.get('current_price'),
                price_source,
                token['remaining_quantity'],
                in_portfolio
            ))
        
        conn.commit()
        conn.close()
        print(f"💾 {len(token_results)} tokens sauvés dans token_analytics")
        return True
        
    except Exception as e:
        print(f"❌ Erreur sauvegarde token analytics: {e}")
        return False

def save_wallet_profile_to_db(wallet_analysis: Dict) -> bool:
    """Sauvegarde le profil wallet complet en base"""
    try:
        conn = sqlite3.connect('data/db/wit_database.db')
        cursor = conn.cursor()
        
        wallet_address = wallet_analysis['wallet_address']
        
        # Déterminer le statut d'activité
        days_inactive = wallet_analysis['days_since_last_activity']
        if days_inactive <= 7:
            statut_activite = 'TRES_ACTIF'
        elif days_inactive <= 30:
            statut_activite = 'ACTIF'
        elif days_inactive <= 90:
            statut_activite = 'MODERE'
        else:
            statut_activite = 'INACTIF'
        
        # Extraction des stats par tranche
        investment_levels = wallet_analysis.get('investment_levels', {})
        
        # Petits (<10k)
        petits_stats = investment_levels.get('petits', {}).get('stats', {})
        petits_count = petits_stats.get('count', 0)
        petits_gagnants = petits_stats.get('winners', 0)
        petits_roi = petits_stats.get('roi', 0)
        petits_reussite = petits_stats.get('success_rate', 0)
        petits_investi = petits_stats.get('total_invested', 0)
        petits_retour = petits_stats.get('total_return', 0)
        
        # Gros (10k-50k)
        gros_stats = investment_levels.get('gros', {}).get('stats', {})
        gros_count = gros_stats.get('count', 0)
        gros_gagnants = gros_stats.get('winners', 0)
        gros_roi = gros_stats.get('roi', 0)
        gros_reussite = gros_stats.get('success_rate', 0)
        gros_investi = gros_stats.get('total_invested', 0)
        gros_retour = gros_stats.get('total_return', 0)
        
        # Whales (>50k)
        whales_stats = investment_levels.get('whales', {}).get('stats', {})
        whales_count = whales_stats.get('count', 0)
        whales_gagnants = whales_stats.get('winners', 0)
        whales_roi = whales_stats.get('roi', 0)
        whales_reussite = whales_stats.get('success_rate', 0)
        whales_investi = whales_stats.get('total_invested', 0)
        whales_retour = whales_stats.get('total_return', 0)
        
        # Déterminer force/faiblesse
        force_principale = f"Forte performance en {wallet_analysis.get('best_tranche', 'petits').upper()}"
        if wallet_analysis.get('roi_percentage', 0) < 0:
            point_attention = "ROI négatif - revoir la stratégie"
        elif wallet_analysis.get('winning_rate', 0) < 40:
            point_attention = "Faible taux de réussite"
        elif wallet_analysis.get('days_since_last_activity', 999) > 90:
            point_attention = "Inactivité prolongée"
        else:
            point_attention = "Performance stable"
        
        # Calcul du skill vs chance ratio
        total_gains = wallet_analysis.get('total_portfolio_value', 0)
        airdrop_gains = wallet_analysis.get('airdrop_gains', 0)
        investment_gains = wallet_analysis.get('investment_gains', 0)
        
        if total_gains > 0:
            ratio_skill_chance = (investment_gains / total_gains * 100)
        else:
            ratio_skill_chance = 50  # Neutre si pas de gains
        
        # INSERT structure avec nouvelles colonnes de meilleure tranche
        cursor.execute("""
            INSERT OR REPLACE INTO wallet_profiles (
                wallet_address, total_score, roi_global, taux_reussite, jours_derniere_activite,
                capital_investi, gains_realises, valeur_actuelle, gains_totaux, profit_net,
                total_tokens, tokens_gagnants, tokens_neutres, tokens_perdants, tokens_airdrops,
                gains_airdrops, gains_trading, ratio_skill_chance,
                petits_count, petits_gagnants, petits_roi, petits_reussite, petits_investi, petits_retour,
                gros_count, gros_gagnants, gros_roi, gros_reussite, gros_investi, gros_retour,
                whales_count, whales_gagnants, whales_roi, whales_reussite, whales_investi, whales_retour,
                best_tranche, best_tranche_name, best_tranche_roi, best_tranche_success, best_tranche_winners, consistency_factor
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            wallet_address,
            wallet_analysis.get('total_score', 0),
            wallet_analysis.get('roi_percentage', 0),
            wallet_analysis.get('winning_rate', 0),
            days_inactive,
            wallet_analysis.get('total_invested', 0),
            wallet_analysis.get('total_gains_totaux', 0),
            wallet_analysis.get('total_valeur_actuelle', 0),
            wallet_analysis.get('total_portfolio_value', 0),
            wallet_analysis.get('total_benefice', 0),
            wallet_analysis.get('total_tokens', 0),
            wallet_analysis.get('winning_tokens', 0),
            wallet_analysis.get('neutral_tokens', 0),
            wallet_analysis.get('losing_tokens', 0),
            wallet_analysis.get('airdrop_tokens', 0),
            airdrop_gains,
            investment_gains,
            ratio_skill_chance,
            petits_count, petits_gagnants, petits_roi, petits_reussite, petits_investi, petits_retour,
            gros_count, gros_gagnants, gros_roi, gros_reussite, gros_investi, gros_retour,
            whales_count, whales_gagnants, whales_roi, whales_reussite, whales_investi, whales_retour,
            wallet_analysis.get('best_tranche', None),
            wallet_analysis.get('best_tranche_name', None),
            wallet_analysis.get('best_tranche_roi', 0),
            wallet_analysis.get('best_tranche_success', 0),
            wallet_analysis.get('best_tranche_winners', 0),
            wallet_analysis.get('consistency_factor', 1.0)
        ))
        
        conn.commit()
        conn.close()
        print(f"💾 Profil wallet {wallet_address[:12]}... sauvé dans wallet_profiles")
        
        # Si score total >= 40, sauver aussi dans smart_wallets  
        if wallet_analysis.get('total_score', 0) >= 40:
            save_to_smart_wallets(wallet_analysis)
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur sauvegarde wallet profile: {e}")
        return False

def create_consensus_live_table():
    """Crée la table consensus_live pour stocker les consensus détectés"""
    try:
        conn = sqlite3.connect('data/db/wit_database.db')
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS consensus_live (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                contract_address TEXT,
                whale_count INTEGER NOT NULL,
                total_investment REAL NOT NULL,
                first_buy DATETIME,
                last_buy DATETIME,
                detection_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                
                -- Données DexScreener
                price_usd REAL,
                market_cap_circulating REAL,
                market_cap_fdv REAL,
                market_cap_ratio REAL,
                liquidity_usd REAL,
                volume_24h REAL,
                volume_1h REAL,
                volume_6h REAL,
                price_change_5m REAL,
                price_change_1h REAL,
                price_change_6h REAL,
                price_change_24h REAL,
                price_change_7d REAL,
                price_change_30d REAL,
                transactions_24h_buys INTEGER,
                transactions_24h_sells INTEGER,
                transactions_1h_buys INTEGER,
                transactions_1h_sells INTEGER,
                quality_score INTEGER,
                quality_rating TEXT,
                dex_name TEXT,
                chain_id TEXT,
                pair_address TEXT,
                
                -- Métadonnées de filtrage
                passes_mc_filter BOOLEAN DEFAULT 1,
                passes_evolution_filter BOOLEAN DEFAULT 1,
                is_active BOOLEAN DEFAULT 1,
                
                -- Index unique sur symbol + contract_address pour éviter doublons
                UNIQUE(symbol, contract_address)
            )
        """)
        
        # Table pour les détails des wallets par consensus
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS consensus_whales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                consensus_id INTEGER NOT NULL,
                wallet_address TEXT NOT NULL,
                whale_rank INTEGER,
                whale_score REAL,
                whale_roi REAL,
                whale_success_rate REAL,
                investment_usd REAL,
                transaction_count INTEGER,
                
                -- Quantités détaillées
                total_bought_qty REAL,
                total_bought_usd REAL,
                total_sold_qty REAL,
                total_sold_usd REAL,
                remaining_qty REAL,
                current_portfolio_qty REAL,
                current_portfolio_value REAL,
                
                -- Historique transactions (JSON)
                transaction_dates TEXT, -- JSON array des dates de transactions
                
                FOREIGN KEY (consensus_id) REFERENCES consensus_live (id),
                UNIQUE(consensus_id, wallet_address)
            )
        """)
        
        # Index pour améliorer les performances
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_consensus_symbol ON consensus_live(symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_consensus_detection_date ON consensus_live(detection_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_consensus_active ON consensus_live(is_active)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_whales_consensus ON consensus_whales(consensus_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_whales_wallet ON consensus_whales(wallet_address)")
        
        conn.commit()
        conn.close()
        print("✅ Tables consensus_live et consensus_whales créées/vérifiées")
        return True
        
    except Exception as e:
        print(f"❌ Erreur création tables consensus: {e}")
        return False

def save_consensus_to_db(consensus_data: dict) -> bool:
    """Sauvegarde un consensus dans la base de données"""
    try:
        conn = sqlite3.connect('data/db/wit_database.db')
        cursor = conn.cursor()
        
        for token, data in consensus_data.items():
            # Données DexScreener
            dex_data = data.get('dexscreener_data', {})
            
            # Insérer le consensus principal
            cursor.execute("""
                INSERT OR REPLACE INTO consensus_live (
                    symbol, contract_address, whale_count, total_investment,
                    first_buy, last_buy, detection_date,
                    price_usd, market_cap_circulating, market_cap_fdv, market_cap_ratio,
                    liquidity_usd, volume_24h, volume_1h, volume_6h,
                    price_change_5m, price_change_1h, price_change_6h,
                    price_change_24h, price_change_7d, price_change_30d,
                    transactions_24h_buys, transactions_24h_sells,
                    transactions_1h_buys, transactions_1h_sells,
                    quality_score, quality_rating, dex_name, chain_id, pair_address,
                    passes_mc_filter, passes_evolution_filter
                ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                token,
                data.get('contract_address'),
                data['whale_count'],
                data['total_investment'],
                data['first_buy'].isoformat() if data['first_buy'] else None,
                data['last_buy'].isoformat() if data['last_buy'] else None,
                
                # DexScreener data
                dex_data.get('price_usd'),
                dex_data.get('market_cap_circulating'),
                dex_data.get('market_cap_fdv'),
                dex_data.get('market_cap_ratio'),
                dex_data.get('liquidity_usd'),
                dex_data.get('volume_24h'),
                dex_data.get('volume_1h'),
                dex_data.get('volume_6h'),
                dex_data.get('price_change_5m'),
                dex_data.get('price_change_1h'),
                dex_data.get('price_change_6h'),
                dex_data.get('price_change_24h'),
                dex_data.get('price_change_7d'),
                dex_data.get('price_change_30d'),
                dex_data.get('transactions_24h', {}).get('buys'),
                dex_data.get('transactions_24h', {}).get('sells'),
                dex_data.get('transactions_1h', {}).get('buys'),
                dex_data.get('transactions_1h', {}).get('sells'),
                dex_data.get('quality_score'),
                dex_data.get('quality_rating'),
                dex_data.get('dex'),
                dex_data.get('chain_id'),
                dex_data.get('pair_address'),
                dex_data.get('passes_mc_filter', True),
                dex_data.get('passes_evolution_filter', True)
            ))
            
            # Récupérer l'ID du consensus
            consensus_id = cursor.lastrowid
            
            # Insérer les détails des wallets
            for wallet_addr, whale_info in data['whales'].items():
                import json
                tx_dates_json = json.dumps(whale_info.get('transaction_dates', []))
                
                cursor.execute("""
                    INSERT OR REPLACE INTO consensus_whales (
                        consensus_id, wallet_address, whale_rank, whale_score,
                        whale_roi, whale_success_rate, investment_usd, transaction_count,
                        total_bought_qty, total_bought_usd, total_sold_qty, total_sold_usd,
                        remaining_qty, current_portfolio_qty, current_portfolio_value,
                        transaction_dates
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    consensus_id,
                    wallet_addr,
                    whale_info.get('whale_rank'),
                    whale_info.get('whale_score'),
                    whale_info.get('whale_roi'),
                    whale_info.get('whale_success_rate'),
                    whale_info.get('investment_usd'),
                    whale_info.get('transaction_count'),
                    whale_info.get('total_bought_qty'),
                    whale_info.get('total_bought_usd'),
                    whale_info.get('total_sold_qty'),
                    whale_info.get('total_sold_usd'),
                    whale_info.get('remaining_qty'),
                    whale_info.get('current_portfolio_qty'),
                    whale_info.get('current_portfolio_value'),
                    tx_dates_json
                ))
        
        conn.commit()
        conn.close()
        print(f"✅ {len(consensus_data)} consensus sauvegardés en base")
        return True
        
    except Exception as e:
        print(f"❌ Erreur sauvegarde consensus: {e}")
        return False

def get_consensus_from_db(hours_back: int = 24) -> dict:
    """Récupère les consensus depuis la base de données"""
    try:
        conn = sqlite3.connect('data/db/wit_database.db')
        cursor = conn.cursor()
        
        # Récupérer les consensus récents
        cursor.execute("""
            SELECT * FROM consensus_live 
            WHERE detection_date >= datetime('now', '-{} hours')
            AND is_active = 1
            ORDER BY detection_date DESC, total_investment DESC
        """.format(hours_back))
        
        consensus_rows = cursor.fetchall()
        consensus_columns = [desc[0] for desc in cursor.description]
        
        consensus_data = {}
        
        for row in consensus_rows:
            consensus = dict(zip(consensus_columns, row))
            symbol = consensus['symbol']
            consensus_id = consensus['id']
            
            # Récupérer les wallets pour ce consensus
            cursor.execute("""
                SELECT * FROM consensus_whales 
                WHERE consensus_id = ?
                ORDER BY whale_rank ASC
            """, (consensus_id,))
            
            whale_rows = cursor.fetchall()
            whale_columns = [desc[0] for desc in cursor.description]
            
            whales = {}
            for whale_row in whale_rows:
                whale = dict(zip(whale_columns, whale_row))
                wallet_addr = whale['wallet_address']
                
                # Parser les dates de transactions
                import json
                try:
                    transaction_dates = json.loads(whale['transaction_dates'] or '[]')
                except:
                    transaction_dates = []
                
                whales[wallet_addr] = {
                    'whale_rank': whale['whale_rank'],
                    'whale_score': whale['whale_score'],
                    'whale_roi': whale['whale_roi'],
                    'whale_success_rate': whale['whale_success_rate'],
                    'investment_usd': whale['investment_usd'],
                    'transaction_count': whale['transaction_count'],
                    'total_bought_qty': whale['total_bought_qty'],
                    'total_bought_usd': whale['total_bought_usd'],
                    'total_sold_qty': whale['total_sold_qty'],
                    'total_sold_usd': whale['total_sold_usd'],
                    'remaining_qty': whale['remaining_qty'],
                    'current_portfolio_qty': whale['current_portfolio_qty'],
                    'current_portfolio_value': whale['current_portfolio_value'],
                    'transaction_dates': transaction_dates
                }
            
            # Construire les données DexScreener
            dexscreener_data = {
                'price_usd': consensus['price_usd'],
                'market_cap_circulating': consensus['market_cap_circulating'],
                'market_cap_fdv': consensus['market_cap_fdv'],
                'market_cap_ratio': consensus['market_cap_ratio'],
                'market_cap': consensus['market_cap_circulating'] or consensus['market_cap_fdv'],
                'liquidity_usd': consensus['liquidity_usd'],
                'volume_24h': consensus['volume_24h'],
                'volume_1h': consensus['volume_1h'],
                'volume_6h': consensus['volume_6h'],
                'price_change_5m': consensus['price_change_5m'],
                'price_change_1h': consensus['price_change_1h'],
                'price_change_6h': consensus['price_change_6h'],
                'price_change_24h': consensus['price_change_24h'],
                'price_change_7d': consensus['price_change_7d'],
                'price_change_30d': consensus['price_change_30d'],
                'transactions_24h': {
                    'buys': consensus['transactions_24h_buys'],
                    'sells': consensus['transactions_24h_sells']
                },
                'transactions_1h': {
                    'buys': consensus['transactions_1h_buys'],
                    'sells': consensus['transactions_1h_sells']
                },
                'quality_score': consensus['quality_score'],
                'quality_rating': consensus['quality_rating'],
                'dex': consensus['dex_name'],
                'chain_id': consensus['chain_id'],
                'pair_address': consensus['pair_address'],
                'passes_mc_filter': consensus['passes_mc_filter'],
                'passes_evolution_filter': consensus['passes_evolution_filter']
            }
            
            # Construire l'objet consensus final
            consensus_data[symbol] = {
                'whale_count': consensus['whale_count'],
                'total_investment': consensus['total_investment'],
                'first_buy': consensus['first_buy'],
                'last_buy': consensus['last_buy'],
                'contract_address': consensus['contract_address'],
                'whales': whales,
                'dexscreener_data': dexscreener_data,
                'detection_date': consensus['detection_date']
            }
        
        conn.close()
        print(f"✅ {len(consensus_data)} consensus récupérés depuis la base")
        return consensus_data
        
    except Exception as e:
        print(f"❌ Erreur récupération consensus: {e}")
        return {}

def clean_old_consensus(days_old: int = 7):
    """Nettoie les anciens consensus"""
    try:
        conn = sqlite3.connect('data/db/wit_database.db')
        cursor = conn.cursor()
        
        # Supprimer les anciens consensus
        cursor.execute("""
            DELETE FROM consensus_live 
            WHERE detection_date < datetime('now', '-{} days')
        """.format(days_old))
        
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        
        print(f"🧹 {deleted_count} anciens consensus supprimés (>{days_old} jours)")
        return deleted_count
        
    except Exception as e:
        print(f"❌ Erreur nettoyage consensus: {e}")
        return 0

def save_to_smart_wallets(wallet_analysis: Dict) -> bool:
    """Sauvegarde un wallet avec score >= 40 dans la table smart_wallets"""
    try:
        conn = sqlite3.connect('data/db/wit_database.db')
        cursor = conn.cursor()
        
        wallet_address = wallet_analysis['wallet_address']
        
        # Déterminer le statut d'activité
        days_inactive = wallet_analysis['days_since_last_activity']
        
        # Extraction des stats par tranche
        investment_levels = wallet_analysis.get('investment_levels', {})
        
        # Petits (<10k)
        petits_stats = investment_levels.get('petits', {}).get('stats', {})
        petits_count = petits_stats.get('count', 0)
        petits_gagnants = petits_stats.get('winners', 0)
        petits_roi = petits_stats.get('roi', 0)
        petits_reussite = petits_stats.get('success_rate', 0)
        petits_investi = petits_stats.get('total_invested', 0)
        petits_retour = petits_stats.get('total_return', 0)
        
        # Gros (10k-50k)
        gros_stats = investment_levels.get('gros', {}).get('stats', {})
        gros_count = gros_stats.get('count', 0)
        gros_gagnants = gros_stats.get('winners', 0)
        gros_roi = gros_stats.get('roi', 0)
        gros_reussite = gros_stats.get('success_rate', 0)
        gros_investi = gros_stats.get('total_invested', 0)
        gros_retour = gros_stats.get('total_return', 0)
        
        # Whales (>50k)
        whales_stats = investment_levels.get('whales', {}).get('stats', {})
        whales_count = whales_stats.get('count', 0)
        whales_gagnants = whales_stats.get('winners', 0)
        whales_roi = whales_stats.get('roi', 0)
        whales_reussite = whales_stats.get('success_rate', 0)
        whales_investi = whales_stats.get('total_invested', 0)
        whales_retour = whales_stats.get('total_return', 0)
        
        # Calcul du skill vs chance ratio
        total_gains = wallet_analysis.get('total_portfolio_value', 0)
        airdrop_gains = wallet_analysis.get('airdrop_gains', 0)
        investment_gains = wallet_analysis.get('investment_gains', 0)
        
        if total_gains > 0:
            ratio_skill_chance = (investment_gains / total_gains * 100)
        else:
            ratio_skill_chance = 50  # Neutre si pas de gains
        
        # INSERT dans smart_wallets (même structure que wallet_profiles)
        cursor.execute("""
            INSERT OR REPLACE INTO smart_wallets (
                wallet_address, total_score, roi_global, taux_reussite, jours_derniere_activite,
                capital_investi, gains_realises, valeur_actuelle, gains_totaux, profit_net,
                total_tokens, tokens_gagnants, tokens_neutres, tokens_perdants, tokens_airdrops,
                gains_airdrops, gains_trading, ratio_skill_chance,
                petits_count, petits_gagnants, petits_roi, petits_reussite, petits_investi, petits_retour,
                gros_count, gros_gagnants, gros_roi, gros_reussite, gros_investi, gros_retour,
                whales_count, whales_gagnants, whales_roi, whales_reussite, whales_investi, whales_retour,
                best_tranche, best_tranche_name, best_tranche_roi, best_tranche_success, best_tranche_winners, consistency_factor
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            wallet_address,
            wallet_analysis.get('total_score', 0),
            wallet_analysis.get('roi_percentage', 0),
            wallet_analysis.get('winning_rate', 0),
            days_inactive,
            wallet_analysis.get('total_invested', 0),
            wallet_analysis.get('total_gains_totaux', 0),
            wallet_analysis.get('total_valeur_actuelle', 0),
            wallet_analysis.get('total_portfolio_value', 0),
            wallet_analysis.get('total_benefice', 0),
            wallet_analysis.get('total_tokens', 0),
            wallet_analysis.get('winning_tokens', 0),
            wallet_analysis.get('neutral_tokens', 0),
            wallet_analysis.get('losing_tokens', 0),
            wallet_analysis.get('airdrop_tokens', 0),
            airdrop_gains,
            investment_gains,
            ratio_skill_chance,
            petits_count, petits_gagnants, petits_roi, petits_reussite, petits_investi, petits_retour,
            gros_count, gros_gagnants, gros_roi, gros_reussite, gros_investi, gros_retour,
            whales_count, whales_gagnants, whales_roi, whales_reussite, whales_investi, whales_retour,
            wallet_analysis.get('best_tranche', None),
            wallet_analysis.get('best_tranche_name', None),
            wallet_analysis.get('best_tranche_roi', 0),
            wallet_analysis.get('best_tranche_success', 0),
            wallet_analysis.get('best_tranche_winners', 0),
            wallet_analysis.get('consistency_factor', 1.0)
        ))
        
        conn.commit()
        conn.close()
        print(f"🎯 Smart wallet {wallet_address[:12]}... sauvé (Score: {wallet_analysis.get('total_score', 0)}/100)")
        return True
        
    except Exception as e:
        print(f"❌ Erreur sauvegarde smart wallet: {e}")
        return False