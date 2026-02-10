"""
Data Access Object (DAO) pour la table wallet_brute
Facilite l'insertion et la récupération des wallets découverts
"""

import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional


# Path vers la base de données
DB_PATH = Path(__file__).parent.parent.parent / "data" / "db" / "wit_database.db"


class WalletBruteDAO:
    """DAO pour gérer les wallets bruts découverts"""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path

    def insert_wallet(self, wallet_data: Dict) -> Optional[int]:
        """
        Insère un wallet découvert dans la table

        Args:
            wallet_data (dict): Données du wallet avec les clés:
                - wallet_address (str): Adresse du wallet
                - token_address (str): Adresse du token
                - token_symbol (str): Symbole du token
                - contract_address (str): Adresse du contrat
                - chain (str): Blockchain (ethereum, bsc, etc.)
                - temporality (str): Temporalité (14d, 30d, 200d, 360d)

        Returns:
            int: ID du wallet inséré, ou None si erreur
        """

        required_fields = ['wallet_address', 'token_address', 'contract_address', 'chain', 'temporality']
        for field in required_fields:
            if field not in wallet_data:
                raise ValueError(f"Champ requis manquant: {field}")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT INTO wallet_brute
                    (wallet_address, token_address, token_symbol, contract_address, chain, temporality, detection_date)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                wallet_data['wallet_address'],
                wallet_data['token_address'],
                wallet_data.get('token_symbol'),
                wallet_data['contract_address'],
                wallet_data['chain'],
                wallet_data['temporality']
            ))

            conn.commit()
            wallet_id = cursor.lastrowid
            return wallet_id

        except sqlite3.IntegrityError as e:
            # Wallet déjà existant (UNIQUE constraint)
            return None

        except Exception as e:
            print(f"❌ Erreur insertion wallet {wallet_data.get('wallet_address')}: {e}")
            conn.rollback()
            return None

        finally:
            conn.close()

    def insert_wallets_batch(self, wallets_list: List[Dict]) -> int:
        """
        Insère plusieurs wallets en batch

        Args:
            wallets_list (list): Liste de dictionnaires de wallets

        Returns:
            int: Nombre de wallets insérés avec succès
        """

        inserted_count = 0
        skipped_count = 0

        for wallet_data in wallets_list:
            wallet_id = self.insert_wallet(wallet_data)
            if wallet_id:
                inserted_count += 1
            else:
                skipped_count += 1

        if skipped_count > 0:
            print(f"⏩ {skipped_count} wallets déjà présents (skipped)")

        return inserted_count

    def get_wallets_by_token(self, token_address: str, temporality: Optional[str] = None) -> List[Dict]:
        """
        Récupère tous les wallets pour un token donné

        Args:
            token_address (str): Adresse du token
            temporality (str, optional): Filtrer par temporalité

        Returns:
            list: Liste de dictionnaires représentant les wallets
        """

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            query = "SELECT * FROM wallet_brute WHERE token_address = ?"
            params = [token_address]

            if temporality:
                query += " AND temporality = ?"
                params.append(temporality)

            query += " ORDER BY detection_date DESC"

            cursor.execute(query, params)
            rows = cursor.fetchall()

            return [dict(row) for row in rows]

        finally:
            conn.close()

    def get_wallets_by_temporality(self, temporality: str) -> List[Dict]:
        """
        Récupère tous les wallets d'une temporalité donnée

        Args:
            temporality (str): Temporalité (14d, 30d, 200d, 360d)

        Returns:
            list: Liste de dictionnaires représentant les wallets
        """

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT * FROM wallet_brute
                WHERE temporality = ?
                ORDER BY detection_date DESC
            """, (temporality,))

            rows = cursor.fetchall()
            return [dict(row) for row in rows]

        finally:
            conn.close()

    def get_new_wallets(self) -> List[Dict]:
        """
        Récupère les wallets qui ne sont pas encore dans la table wallets

        Returns:
            list: Liste de wallets non présents dans la table wallets
        """

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT DISTINCT wb.wallet_address, wb.token_symbol, wb.contract_address,
                       wb.chain, wb.temporality
                FROM wallet_brute wb
                LEFT JOIN wallets w ON LOWER(wb.wallet_address) = LOWER(w.wallet_address)
                WHERE w.wallet_address IS NULL
                ORDER BY wb.detection_date DESC
            """)

            rows = cursor.fetchall()
            return [dict(row) for row in rows]

        finally:
            conn.close()

    def check_wallet_exists(self, wallet_address: str, token_address: str, temporality: str) -> bool:
        """
        Vérifie si un wallet existe déjà pour un token et une temporalité

        Args:
            wallet_address (str): Adresse du wallet
            token_address (str): Adresse du token
            temporality (str): Temporalité

        Returns:
            bool: True si le wallet existe
        """

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT COUNT(*) FROM wallet_brute
                WHERE wallet_address = ? AND token_address = ? AND temporality = ?
            """, (wallet_address, token_address, temporality))

            count = cursor.fetchone()[0]
            return count > 0

        finally:
            conn.close()

    def get_statistics(self) -> Dict:
        """
        Récupère des statistiques sur les wallets découverts

        Returns:
            dict: Statistiques
        """

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT
                    COUNT(*) as total_entries,
                    COUNT(DISTINCT wallet_address) as unique_wallets,
                    COUNT(DISTINCT token_address) as unique_tokens,
                    MIN(detection_date) as first_detection,
                    MAX(detection_date) as last_detection
                FROM wallet_brute
            """)

            row = cursor.fetchone()

            # Stats par temporalité
            cursor.execute("""
                SELECT temporality, COUNT(*) as count
                FROM wallet_brute
                GROUP BY temporality
                ORDER BY temporality
            """)

            temporality_stats = {row[0]: row[1] for row in cursor.fetchall()}

            return {
                'total_entries': row[0],
                'unique_wallets': row[1],
                'unique_tokens': row[2],
                'first_detection': row[3],
                'last_detection': row[4],
                'by_temporality': temporality_stats
            }

        finally:
            conn.close()

    def clear_table(self) -> int:
        """
        Vide complètement la table wallet_brute après traitement

        Returns:
            int: Nombre d'entrées supprimées
        """

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Compter les entrées avant suppression
            cursor.execute("SELECT COUNT(*) FROM wallet_brute")
            count = cursor.fetchone()[0]

            # Vider la table
            cursor.execute("DELETE FROM wallet_brute")
            conn.commit()

            return count

        except Exception as e:
            print(f"❌ Erreur lors du vidage de wallet_brute: {e}")
            conn.rollback()
            return 0

        finally:
            conn.close()


if __name__ == "__main__":
    # Test du DAO
    dao = WalletBruteDAO()

    print("📊 Statistiques wallet_brute:")
    stats = dao.get_statistics()
    print(f"   • Total entries: {stats['total_entries']}")
    print(f"   • Unique wallets: {stats['unique_wallets']}")
    print(f"   • Unique tokens: {stats['unique_tokens']}")
    print(f"   • First detection: {stats['first_detection']}")
    print(f"   • Last detection: {stats['last_detection']}")
    print(f"   • By temporality: {stats['by_temporality']}")
