"""
Data Access Object (DAO) pour la table tokens_discovered
Facilite l'insertion et la récupération des tokens découverts
"""

import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional


# Path vers la base de données
DB_PATH = Path(__file__).parent.parent.parent / "data" / "db" / "wit_database.db"


class TokensDiscoveredDAO:
    """DAO pour gérer les tokens découverts"""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path

    def insert_token(self, token_data: Dict) -> Optional[int]:
        """
        Insère un token découvert dans la table

        Args:
            token_data (dict): Données du token avec les clés:
                - token_id (str): ID CoinGecko
                - symbol (str): Symbole du token
                - name (str): Nom complet
                - discovery_period (str): Période (14d, 30d, 200d, 1y)
                - discovered_at (str/datetime): Date de découverte
                + optionnel: contract_address, platform, current_price_usd, etc.

        Returns:
            int: ID du token inséré, ou None si erreur
        """

        required_fields = ['token_id', 'symbol', 'name', 'discovery_period', 'discovered_at']
        for field in required_fields:
            if field not in token_data:
                raise ValueError(f"Champ requis manquant: {field}")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Convertir discovered_at en timestamp si nécessaire
            if isinstance(token_data.get('discovered_at'), datetime):
                token_data['discovered_at'] = token_data['discovered_at'].isoformat()

            # Préparer les colonnes et valeurs
            columns = list(token_data.keys())
            placeholders = ', '.join(['?' for _ in columns])
            columns_str = ', '.join(columns)

            query = f"""
                INSERT INTO tokens_discovered ({columns_str})
                VALUES ({placeholders})
            """

            cursor.execute(query, list(token_data.values()))
            conn.commit()

            token_id = cursor.lastrowid
            return token_id

        except sqlite3.IntegrityError as e:
            # Token déjà existant (UNIQUE constraint)
            print(f"⚠️  Token déjà enregistré: {token_data.get('symbol')} ({e})")
            return None

        except Exception as e:
            print(f"❌ Erreur insertion token {token_data.get('symbol')}: {e}")
            conn.rollback()
            return None

        finally:
            conn.close()

    def insert_tokens_batch(self, tokens_list: List[Dict]) -> int:
        """
        Insère plusieurs tokens en batch

        Args:
            tokens_list (list): Liste de dictionnaires de tokens

        Returns:
            int: Nombre de tokens insérés avec succès
        """

        inserted_count = 0
        for token_data in tokens_list:
            token_id = self.insert_token(token_data)
            if token_id:
                inserted_count += 1

        return inserted_count

    def update_token_contract(self, token_id: int, contract_address: str,
                              platform: str, cmc_id: Optional[int] = None) -> bool:
        """
        Met à jour les informations de contrat d'un token

        Args:
            token_id (int): ID du token
            contract_address (str): Adresse du contrat
            platform (str): Blockchain (ethereum, bsc, etc.)
            cmc_id (int, optional): ID CoinMarketCap

        Returns:
            bool: True si succès
        """

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                UPDATE tokens_discovered
                SET contract_address = ?,
                    platform = ?,
                    cmc_id = ?,
                    has_contract = 1,
                    is_evm_compatible = 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (contract_address, platform, cmc_id, token_id))

            conn.commit()
            return cursor.rowcount > 0

        except Exception as e:
            print(f"❌ Erreur update contract pour token {token_id}: {e}")
            conn.rollback()
            return False

        finally:
            conn.close()

    def mark_wallets_extracted(self, token_id: int, wallets_count: int) -> bool:
        """
        Marque qu'un token a eu ses wallets extraits

        Args:
            token_id (int): ID du token
            wallets_count (int): Nombre de wallets découverts

        Returns:
            bool: True si succès
        """

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                UPDATE tokens_discovered
                SET wallets_extracted = 1,
                    wallets_count = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (wallets_count, token_id))

            conn.commit()
            return cursor.rowcount > 0

        except Exception as e:
            print(f"❌ Erreur mark wallets extracted pour token {token_id}: {e}")
            conn.rollback()
            return False

        finally:
            conn.close()

    def get_tokens_by_period(self, period: str, active_only: bool = True) -> List[Dict]:
        """
        Récupère tous les tokens d'une période

        Args:
            period (str): Période (14d, 30d, 200d, 1y)
            active_only (bool): Seulement les tokens actifs

        Returns:
            list: Liste de dictionnaires représentant les tokens
        """

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            query = "SELECT * FROM tokens_discovered WHERE discovery_period = ?"
            params = [period]

            if active_only:
                query += " AND is_active = 1"

            query += " ORDER BY discovered_at DESC"

            cursor.execute(query, params)
            rows = cursor.fetchall()

            return [dict(row) for row in rows]

        finally:
            conn.close()

    def get_tokens_without_contracts(self) -> List[Dict]:
        """
        Récupère les tokens qui n'ont pas encore de contrat

        Returns:
            list: Liste de tokens sans contrat
        """

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT * FROM tokens_discovered
                WHERE has_contract = 0
                AND is_active = 1
                ORDER BY discovered_at DESC
            """)

            rows = cursor.fetchall()
            return [dict(row) for row in rows]

        finally:
            conn.close()

    def get_tokens_without_wallets(self) -> List[Dict]:
        """
        Récupère les tokens qui n'ont pas encore leurs wallets extraits

        Returns:
            list: Liste de tokens sans wallets
        """

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT * FROM tokens_discovered
                WHERE wallets_extracted = 0
                AND has_contract = 1
                AND is_active = 1
                ORDER BY discovered_at DESC
            """)

            rows = cursor.fetchall()
            return [dict(row) for row in rows]

        finally:
            conn.close()

    def get_statistics(self) -> Dict:
        """
        Récupère des statistiques sur les tokens découverts

        Returns:
            dict: Statistiques
        """

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT
                    COUNT(*) as total_tokens,
                    COUNT(DISTINCT symbol) as unique_symbols,
                    COUNT(CASE WHEN has_contract = 1 THEN 1 END) as with_contracts,
                    COUNT(CASE WHEN wallets_extracted = 1 THEN 1 END) as wallets_extracted,
                    SUM(wallets_count) as total_wallets,
                    MIN(discovered_at) as first_discovery,
                    MAX(discovered_at) as last_discovery
                FROM tokens_discovered
                WHERE is_active = 1
            """)

            row = cursor.fetchone()

            return {
                'total_tokens': row[0],
                'unique_symbols': row[1],
                'with_contracts': row[2],
                'wallets_extracted': row[3],
                'total_wallets': row[4] or 0,
                'first_discovery': row[5],
                'last_discovery': row[6]
            }

        finally:
            conn.close()

    def get_token_by_coingecko_id_and_period(self, token_id: str, period: str) -> Optional[Dict]:
        """
        Récupère un token par son ID CoinGecko et sa période

        Args:
            token_id (str): ID CoinGecko
            period (str): Période de découverte

        Returns:
            dict: Token trouvé ou None
        """

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT * FROM tokens_discovered
                WHERE token_id = ? AND discovery_period = ?
                LIMIT 1
            """, (token_id, period))

            row = cursor.fetchone()
            return dict(row) if row else None

        finally:
            conn.close()

    def update_token_full(self, db_id: int, token_data: Dict) -> bool:
        """
        Met à jour tous les champs d'un token

        Args:
            db_id (int): ID dans la BDD
            token_data (dict): Données à mettre à jour

        Returns:
            bool: True si succès
        """

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Construire dynamiquement la requête UPDATE
            set_clauses = []
            values = []

            for key, value in token_data.items():
                if key != 'id':  # Ne pas updater l'ID
                    set_clauses.append(f"{key} = ?")
                    values.append(value)

            set_clauses.append("updated_at = CURRENT_TIMESTAMP")
            values.append(db_id)

            query = f"""
                UPDATE tokens_discovered
                SET {', '.join(set_clauses)}
                WHERE id = ?
            """

            cursor.execute(query, values)
            conn.commit()

            return cursor.rowcount > 0

        except Exception as e:
            print(f"❌ Erreur update token {db_id}: {e}")
            conn.rollback()
            return False

        finally:
            conn.close()

    def insert_token_full(self, token_data: Dict) -> Optional[int]:
        """
        Insère un token avec tous ses champs enrichis

        Args:
            token_data (dict): Données complètes du token

        Returns:
            int: ID du token inséré ou None
        """

        # Ajouter discovered_at si absent
        if 'discovered_at' not in token_data:
            token_data['discovered_at'] = datetime.now().isoformat()

        # Ajouter source si absent
        if 'source' not in token_data:
            token_data['source'] = 'coingecko'

        return self.insert_token(token_data)


# Fonctions utilitaires pour faciliter l'utilisation

def save_token_from_coingecko(token_json: Dict, period: str, rank: int) -> Optional[int]:
    """
    Sauvegarde un token depuis les données CoinGecko

    Args:
        token_json (dict): Données brutes CoinGecko
        period (str): Période de découverte
        rank (int): Rang du token

    Returns:
        int: ID du token inséré
    """

    dao = TokensDiscoveredDAO()

    # Extraire la colonne de changement de prix appropriée
    price_change_col = f"price_change_percentage_{period}_in_currency"

    token_data = {
        'token_id': token_json.get('id'),
        'symbol': token_json.get('symbol', '').upper(),
        'name': token_json.get('name'),
        'current_price_usd': token_json.get('current_price'),
        'market_cap_usd': token_json.get('market_cap'),
        'total_volume_usd': token_json.get('total_volume'),
        'discovery_period': period,
        'discovery_rank': rank,
        'discovered_at': datetime.now().isoformat(),
        'source': 'coingecko'
    }

    # Ajouter les changements de prix disponibles
    if price_change_col in token_json:
        token_data[f'price_change_{period}'] = token_json[price_change_col]

    # Ajouter d'autres changements de prix si disponibles
    for p in ['1h', '24h', '7d', '14d', '30d', '200d', '1y']:
        col = f"price_change_percentage_{p}_in_currency"
        if col in token_json:
            token_data[f'price_change_{p}'] = token_json[col]

    return dao.insert_token(token_data)


if __name__ == "__main__":
    # Test du DAO
    dao = TokensDiscoveredDAO()

    print("📊 Statistiques tokens_discovered:")
    stats = dao.get_statistics()
    for key, value in stats.items():
        print(f"   • {key}: {value}")
