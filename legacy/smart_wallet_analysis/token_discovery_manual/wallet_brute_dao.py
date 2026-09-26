"""DAO pour la table wallet_brute."""

import sqlite3
from typing import Dict, List, Optional

from smart_wallet_analysis.config import DB_PATH
from smart_wallet_analysis.logger import get_logger

logger = get_logger("token_discovery.manual.wallet_brute_dao")


class WalletBruteDAO:
    """Accès lecture/écriture à wallet_brute."""

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path

    def _connect(self):
        """Retourne une connexion SQLite."""
        return sqlite3.connect(str(self.db_path))

    def ensure_table(self) -> bool:
        """Crée la table wallet_brute et ses index si nécessaire."""
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as conn:
                conn.execute(
                    """
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
                    )
                    """
                )
                conn.execute("CREATE INDEX IF NOT EXISTS idx_wallet_brute_token ON wallet_brute(token_address)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_wallet_brute_wallet ON wallet_brute(wallet_address)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_wallet_brute_chain ON wallet_brute(chain)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_wallet_brute_temporality ON wallet_brute(temporality)")
            return True
        except Exception as e:
            logger.error("Erreur création table wallet_brute: %s", e)
            return False

    def insert_wallet(self, wallet_data: Dict) -> Optional[int]:
        """Insère un wallet et retourne son ID, ou None si ignoré/erreur."""
        required = ("wallet_address", "token_address", "contract_address", "chain", "temporality")
        if any(field not in wallet_data for field in required):
            missing = [f for f in required if f not in wallet_data]
            raise ValueError(f"Champs requis manquants: {missing}")

        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO wallet_brute
                    (wallet_address, token_address, token_symbol, contract_address, chain, temporality, detection_date)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        wallet_data["wallet_address"],
                        wallet_data["token_address"],
                        wallet_data.get("token_symbol"),
                        wallet_data["contract_address"],
                        wallet_data["chain"],
                        wallet_data["temporality"],
                    ),
                )
                if cursor.rowcount == 0:
                    return None
                return cursor.lastrowid
        except Exception as e:
            logger.error("Erreur insertion wallet %s: %s", wallet_data.get("wallet_address"), e)
            return None

    def insert_wallets_batch(self, wallets_list: List[Dict]) -> int:
        """Insère un lot de wallets et retourne le nombre d'insertions."""
        if not wallets_list:
            return 0

        valid_rows = []
        for wallet_data in wallets_list:
            required = ("wallet_address", "token_address", "contract_address", "chain", "temporality")
            if any(field not in wallet_data for field in required):
                logger.warning("Wallet ignoré (champs manquants): %s", wallet_data)
                continue
            valid_rows.append(
                (
                    wallet_data["wallet_address"],
                    wallet_data["token_address"],
                    wallet_data.get("token_symbol"),
                    wallet_data["contract_address"],
                    wallet_data["chain"],
                    wallet_data["temporality"],
                )
            )

        if not valid_rows:
            return 0

        try:
            with self._connect() as conn:
                before = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO wallet_brute
                    (wallet_address, token_address, token_symbol, contract_address, chain, temporality, detection_date)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    valid_rows,
                )
                inserted = conn.total_changes - before
                skipped = len(valid_rows) - inserted
                if skipped > 0:
                    logger.info("%s wallets déjà présents (skipped)", skipped)
                return inserted
        except Exception as e:
            logger.error("Erreur insertion batch wallet_brute: %s", e)
            return 0

    def get_wallets_by_token(self, token_address: str, temporality: Optional[str] = None) -> List[Dict]:
        """Récupère les wallets pour un token donné."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            query = "SELECT * FROM wallet_brute WHERE token_address = ?"
            params = [token_address]
            if temporality:
                query += " AND temporality = ?"
                params.append(temporality)
            query += " ORDER BY detection_date DESC"
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]

    def get_wallets_by_temporality(self, temporality: str) -> List[Dict]:
        """Récupère les wallets pour une temporalité."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT * FROM wallet_brute
                WHERE temporality = ?
                ORDER BY detection_date DESC
                """,
                (temporality,),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_new_wallets(self) -> List[Dict]:
        """Récupère les wallets de wallet_brute absents de wallets."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT DISTINCT wb.wallet_address, wb.token_symbol, wb.contract_address,
                       wb.chain, wb.temporality
                FROM wallet_brute wb
                LEFT JOIN wallets w ON LOWER(wb.wallet_address) = LOWER(w.wallet_address)
                WHERE w.wallet_address IS NULL
                ORDER BY wb.detection_date DESC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def check_wallet_exists(self, wallet_address: str, token_address: str, temporality: str) -> bool:
        """Vérifie l'existence d'un wallet pour un token/temporality."""
        with self._connect() as conn:
            count = conn.execute(
                """
                SELECT COUNT(*) FROM wallet_brute
                WHERE wallet_address = ? AND token_address = ? AND temporality = ?
                """,
                (wallet_address, token_address, temporality),
            ).fetchone()[0]
            return count > 0

    def token_already_processed(self, token_address: str, chain: str, temporality: str) -> bool:
        """Vérifie si un token est déjà traité pour chain/temporality."""
        with self._connect() as conn:
            count = conn.execute(
                """
                SELECT COUNT(*) FROM wallet_brute
                WHERE token_address = ? AND chain = ? AND temporality = ?
                """,
                (token_address, chain, temporality),
            ).fetchone()[0]
            return count > 0

    def get_statistics(self) -> Dict:
        """Retourne les statistiques agrégées de wallet_brute."""
        with self._connect() as conn:
            global_stats = conn.execute(
                """
                SELECT
                    COUNT(*) as total_entries,
                    COUNT(DISTINCT wallet_address) as unique_wallets,
                    COUNT(DISTINCT token_address) as unique_tokens,
                    MIN(detection_date) as first_detection,
                    MAX(detection_date) as last_detection
                FROM wallet_brute
                """
            ).fetchone()
            temporal_rows = conn.execute(
                """
                SELECT temporality, COUNT(*) as count
                FROM wallet_brute
                GROUP BY temporality
                ORDER BY temporality
                """
            ).fetchall()
            return {
                "total_entries": global_stats[0],
                "unique_wallets": global_stats[1],
                "unique_tokens": global_stats[2],
                "first_detection": global_stats[3],
                "last_detection": global_stats[4],
                "by_temporality": {row[0]: row[1] for row in temporal_rows},
            }

    def clear_table(self) -> int:
        """Vide wallet_brute et retourne le nombre de lignes supprimées."""
        try:
            with self._connect() as conn:
                count = conn.execute("SELECT COUNT(*) FROM wallet_brute").fetchone()[0]
                conn.execute("DELETE FROM wallet_brute")
                return count
        except Exception as e:
            logger.error("Erreur lors du vidage de wallet_brute: %s", e)
            return 0
