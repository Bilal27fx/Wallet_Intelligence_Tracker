"""
Générateur simple de données de graphe pour les wallets
Produit des données standardisées réutilisables pour les dashboards
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from db.database_utils import DatabaseManager
import json
import logging
from typing import Dict, List, Any

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleGraphGenerator:
    """Générateur simple de données de graphe pour les wallets"""
    
    def __init__(self):
        self.db = DatabaseManager()
    
    def get_wallet_data(self, wallet_address: str) -> Dict[str, Any]:
        """Récupère les données d'un wallet depuis la BDD"""
        try:
            self.db.connect()
            
            # Récupérer les données du wallet
            wallet_query = """
            SELECT wallet_address, total_value, score_final, roi_1_day, roi_1_week, roi_1_month
            FROM smart_wallets 
            WHERE wallet_address = ?
            """
            
            cursor = self.db.connection.cursor()
            cursor.execute(wallet_query, (wallet_address,))
            wallet_row = cursor.fetchone()
            
            if not wallet_row:
                return {
                    "address": wallet_address,
                    "type": "smart_wallet",
                    "current_value": 0,
                    "roi_1d": 0,
                    "roi_1w": 0,
                    "roi_1m": 0,
                    "score": 0
                }
            
            return {
                "address": wallet_row[0],
                "type": "smart_wallet",
                "current_value": wallet_row[2] or 0,
                "roi_1d": wallet_row[4] or 0,
                "roi_1w": wallet_row[5] or 0,
                "roi_1m": wallet_row[6] or 0,
                "score": wallet_row[3] or 0
            }
            
        except Exception as e:
            logger.error(f"Erreur récupération wallet data: {e}")
            return {
                "address": wallet_address,
                "type": "unknown",
                "current_value": 0,
                "roi_1d": 0,
                "roi_1w": 0,
                "roi_1m": 0,
                "score": 0
            }
        finally:
            if self.db.connection:
                self.db.connection.close()
    
    def generate_graph_data(self, wallet_address: str, min_amount_usd: float = 500) -> Dict[str, Any]:
        """
        Génère les données de graphe standardisées pour un wallet
        Format simple et réutilisable pour les dashboards
        """
        try:
            self.db.connect()
            
            # Récupérer les relations depuis graph_wallet
            query = """
            SELECT 
                wallet_mere, wallet_fils, wallet_fils_type, direction,
                COUNT(*) as transaction_count,
                SUM(amount_usd) as total_amount,
                AVG(amount_usd) as avg_amount,
                GROUP_CONCAT(DISTINCT token_symbol) as tokens,
                MIN(transaction_date) as first_date,
                MAX(transaction_date) as last_date
            FROM graph_wallet 
            WHERE (wallet_mere = ? OR wallet_fils = ?)
            AND amount_usd >= ?
            GROUP BY wallet_mere, wallet_fils, direction
            ORDER BY total_amount DESC
            """
            
            cursor = self.db.connection.cursor()
            cursor.execute(query, (wallet_address, wallet_address, min_amount_usd))
            relations = cursor.fetchall()
            
            if not relations:
                logger.warning(f"Aucune relation trouvée pour {wallet_address}")
                return {"nodes": [], "edges": []}
            
            # Collecter tous les wallets uniques
            wallet_addresses = set()
            wallet_addresses.add(wallet_address.lower())
            
            for relation in relations:
                wallet_addresses.add(relation[0].lower())  # wallet_mere
                wallet_addresses.add(relation[1].lower())  # wallet_fils
            
            # Créer les nœuds
            nodes = []
            for addr in wallet_addresses:
                wallet_data = self.get_wallet_data(addr)
                
                # Déterminer le type de nœud
                if addr == wallet_address.lower():
                    node_type = "smart_wallet_main"
                elif wallet_data["current_value"] > 0:
                    node_type = "smart_wallet"
                else:
                    node_type = "eoa_wallet"
                
                nodes.append({
                    "id": addr,
                    "type": node_type,
                    "address": addr,
                    "current_value": wallet_data["current_value"],
                    "roi_1d": wallet_data["roi_1d"],
                    "roi_1w": wallet_data["roi_1w"], 
                    "roi_1m": wallet_data["roi_1m"],
                    "score": wallet_data["score"]
                })
            
            # Créer les arêtes
            edges = []
            for relation in relations:
                wallet_mere = relation[0].lower()
                wallet_fils = relation[1].lower()
                direction = relation[3]
                
                edges.append({
                    "source": wallet_mere,
                    "target": wallet_fils,
                    "direction": direction,
                    "color": "#00ff80" if direction == "RECEIVE" else "#ff0080",
                    "transaction_count": relation[4],
                    "total_amount": relation[5],
                    "avg_amount": relation[6],
                    "tokens": relation[7] or "",
                    "first_date": relation[8] or "",
                    "last_date": relation[9] or ""
                })
            
            graph_data = {
                "nodes": nodes,
                "edges": edges,
                "center_wallet": wallet_address.lower(),
                "stats": {
                    "total_nodes": len(nodes),
                    "total_edges": len(edges),
                    "total_volume": sum(edge["total_amount"] for edge in edges)
                }
            }
            
            logger.info(f"✅ Graphe généré: {len(nodes)} nœuds, {len(edges)} arêtes")
            return graph_data
            
        except Exception as e:
            logger.error(f"Erreur génération graphe: {e}")
            return {"nodes": [], "edges": []}
        finally:
            if self.db.connection:
                self.db.connection.close()
    
    def save_graph_json(self, wallet_address: str, output_file: str = None) -> str:
        """Sauvegarde le graphe au format JSON"""
        if not output_file:
            safe_addr = wallet_address.replace("0x", "").lower()[:12]
            output_file = f"graph_data_{safe_addr}.json"
        
        output_path = Path(__file__).parent.parent.parent / "data" / "graphs" / output_file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        graph_data = self.generate_graph_data(wallet_address)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(graph_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📄 Graphe sauvé: {output_path}")
        return str(output_path)

def main():
    """Point d'entrée principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Générateur simple de graphe wallet')
    parser.add_argument('--wallet', '-w', required=True, help='Adresse du wallet')
    parser.add_argument('--min-amount', '-a', type=float, default=500.0, help='Montant minimum USD')
    parser.add_argument('--output', '-o', help='Fichier de sortie JSON')
    
    args = parser.parse_args()
    
    generator = SimpleGraphGenerator()
    output_file = generator.save_graph_json(args.wallet, args.output)
    
    print(f"✅ Graphe généré: {output_file}")

if __name__ == "__main__":
    main()