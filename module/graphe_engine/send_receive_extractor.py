# -*- coding: utf-8 -*-
"""
Script de collecte des transactions > 500$ USD pour le graph engine
Extrait les SEND/RECEIVE des smart wallets vers la table graph_wallet
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from db.database_utils import DatabaseManager
from datetime import datetime
import logging
import requests
import time
import os
from dotenv import load_dotenv
from urllib.parse import urlparse, parse_qs

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Charger les clés API
load_dotenv()
ETHERSCAN_API = os.getenv("ETHERSCAN_API_KEY")
API_KEY = os.getenv("ZERION_API_KEY")

class WalletTypeChecker:
    """Vérificateur de type de wallet (EOA vs Smart Contract)"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'WIT-Graph-Engine/1.0'})
        self.cache = {}  # Cache pour éviter les appels répétés
    
    def is_contract(self, address: str) -> str:
        """
        Vérifie si une adresse est un contrat ou EOA
        Retourne 'Smart Contract' ou 'EOA'
        """
        if not ETHERSCAN_API:
            logger.warning("⚠️ Clé ETHERSCAN_API_KEY manquante, type par défaut: EOA")
            return 'EOA'
        
        # Vérifier le cache
        if address in self.cache:
            return self.cache[address]
        
        try:
            url = "https://api.etherscan.io/v2/api"
            params = {
                "chainid": "1",
                "module": "proxy", 
                "action": "eth_getCode",
                "address": address,
                "apikey": ETHERSCAN_API
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Gestion rate limit
            if data.get("status") == "0" and "rate limit" in data.get("message", "").lower():
                logger.warning("Rate limit Etherscan, pause 2s...")
                time.sleep(2)
                wallet_type = 'EOA'
            else:
                code = data.get("result", "")
                wallet_type = "Smart Contract" if (code and code != "0x") else "EOA"
            
            # Mettre en cache
            self.cache[address] = wallet_type
            
            # Rate limiting
            time.sleep(0.2)  # 200ms entre appels
            
            return wallet_type
            
        except Exception as e:
            logger.warning(f"Erreur vérification type {address}: {e}")
            return 'EOA'  # Valeur par défaut

class GraphWalletCollector:
    """Collecteur de transactions pour le graph engine"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.wallet_checker = WalletTypeChecker()
    
    def collect_high_value_transactions(self, min_amount_usd=500.0, wallet_address=None, batch_size=5):
        """
        Collecte les transactions > min_amount_usd des smart wallets
        :param wallet_address: Si spécifié, traite uniquement ce wallet
        :param batch_size: Nombre de wallets à traiter par batch
        """
        try:
            self.db.connect()
            
            # Créer la table si elle n'existe pas
            self._create_table_if_not_exists()
            
            # Récupérer les smart wallets à traiter
            if wallet_address:
                smart_wallets = [wallet_address]
                logger.info(f"Mode wallet unique: {wallet_address}")
            else:
                smart_wallets = self._get_smart_wallets()
                logger.info(f"Mode batch: {len(smart_wallets)} smart wallets trouvés")
            
            # Traitement par batch
            total_transactions = 0
            total_wallets = len(smart_wallets)
            
            for batch_start in range(0, total_wallets, batch_size):
                batch_end = min(batch_start + batch_size, total_wallets)
                current_batch = smart_wallets[batch_start:batch_end]
                
                batch_num = (batch_start // batch_size) + 1
                total_batches = (total_wallets + batch_size - 1) // batch_size
                
                logger.info(f"\n🔄 BATCH {batch_num}/{total_batches} - Traitement de {len(current_batch)} wallets")
                logger.info("=" * 80)
                
                batch_transactions = 0
                
                for i, wallet_mere in enumerate(current_batch):
                    wallet_num = batch_start + i + 1
                    logger.info(f"\n📍 [{wallet_num}/{total_wallets}] Wallet mère: {wallet_mere}")
                    
                    # Vérifier si ce wallet a déjà été traité
                    if self._wallet_already_processed(wallet_mere):
                        logger.info(f"  ⏭️ Wallet déjà traité, passage au suivant")
                        continue
                    
                    # Récupérer les transactions > min_amount_usd pour ce wallet
                    transactions = self._get_high_value_transactions(wallet_mere, min_amount_usd)
                    
                    if transactions:
                        logger.info(f"  📊 {len(transactions)} transactions > ${min_amount_usd} trouvées")
                        
                        # Traitement des transactions par sous-batch pour éviter trop d'appels API
                        inserted = self._process_transactions_batch(transactions, batch_size=10)
                        batch_transactions += inserted
                        total_transactions += inserted
                        
                        logger.info(f"  ✅ {inserted} transactions insérées")
                    else:
                        logger.info(f"  📭 Aucune transaction > ${min_amount_usd}")
                
                logger.info(f"\n📊 BATCH {batch_num} TERMINÉ: {batch_transactions} transactions ajoutées")
                
                # Pause entre batches pour éviter la surcharge API
                if batch_end < total_wallets:
                    logger.info(f"⏸️  Pause 5s avant batch suivant...")
                    time.sleep(5)
            
            logger.info(f"\n🎉 COLLECTE TERMINÉE")
            logger.info(f"📈 Total wallets traités: {total_wallets}")
            logger.info(f"📊 Total transactions collectées: {total_transactions}")
            return total_transactions
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la collecte: {e}")
            return 0
        finally:
            if self.db.connection:
                self.db.connection.close()
    
    def _create_table_if_not_exists(self):
        """Crée la table graph_wallet si elle n'existe pas"""
        with open(Path(__file__).parent.parent.parent / "db" / "create_graph_wallet_table.sql", 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        cursor = self.db.connection.cursor()
        cursor.executescript(sql_script)
        self.db.connection.commit()
        logger.info("✅ Table graph_wallet vérifiée/créée")
    
    def _get_smart_wallets(self):
        """Récupère la liste des smart wallets actifs"""
        query = """
        SELECT DISTINCT wallet_address 
        FROM smart_wallets 
        WHERE wallet_address IS NOT NULL
        ORDER BY score_final DESC
        """
        
        cursor = self.db.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        return [row[0] for row in results]
    
    def _get_high_value_transactions(self, wallet_mere, min_amount_usd):
        """
        Récupère les transactions > min_amount_usd directement depuis Zerion API
        pour avoir les vraies adresses from/to
        """
        logger.info(f"  🔍 Récupération transactions Zerion pour {wallet_mere}")
        
        # Utiliser l'API Zerion avec la même logique que wallet_balances_extractor.py
        transactions = self._get_zerion_transactions(wallet_mere, min_amount_usd)
        
        return transactions
    
    def _get_zerion_transactions(self, wallet_address, min_amount_usd):
        """
        Récupère les transactions depuis Zerion API avec from/to addresses
        Basé sur wallet_balances_extractor.py
        """
        if not API_KEY:
            logger.error("❌ Clé ZERION_API_KEY manquante")
            return []
        
        headers = {
            "accept": "application/json", 
            "authorization": f"Basic {API_KEY}"
        }
        
        # URL pour SEULEMENT les opérations send et receive sur 1 an
        url = f"https://api.zerion.io/v1/wallets/{wallet_address}/transactions/"
        params = {
            "filter[operation_types]": "send,receive",
            "filter[since]": "365d",  # 1 an d'historique
            "filter[trash]": "only_non_trash",  # Filtrer les scam tokens
            "currency": "usd", 
            "page[size]": 100
        }
        
        all_transactions = []
        page_cursor = None
        page_count = 0
        max_pages = 200  # Limiter pour éviter les timeouts
        
        # petites fonctions utilitaires de retry
        def _request_with_retry(u, h, p, timeout=15, max_retry=3, backoff=1.5):
            attempt = 0
            while True:
                try:
                    resp = requests.get(u, headers=h, params=p, timeout=timeout)
                    # Gestion simple du 429
                    if resp.status_code == 429:
                        raise requests.HTTPError("429 Too Many Requests")
                    resp.raise_for_status()
                    return resp
                except Exception as e:
                    attempt += 1
                    if attempt >= max_retry:
                        raise
                    sleep_s = backoff ** attempt
                    logger.warning(f"    ⚠️ Erreur réseau/API ({e}), retry {attempt}/{max_retry} dans {sleep_s:.1f}s...")
                    time.sleep(sleep_s)
        
        while page_count < max_pages:
            try:
                current_params = params.copy()
                if page_cursor:
                    current_params['page[after]'] = page_cursor
                
                logger.info(f"    🌐 Appel API Zerion page {page_count + 1}... cursor={page_cursor[:16] + '…' if page_cursor else '∅'}")
                response = _request_with_retry(url, headers, current_params, timeout=20)
                data = response.json()
                transactions = data.get("data", [])
                
                if not transactions:
                    logger.info(f"    📄 Fin pagination - aucune transaction page {page_count + 1}")
                    break
                
                logger.info(f"    📊 {len(transactions)} transactions reçues page {page_count + 1}")
                
                # --- Pagination: extraction robuste du curseur ---
                links = data.get("links", {}) or {}
                next_url = links.get("next")
                if next_url:
                    # On parse proprement l'URL; parse_qs retourne déjà des clés décodées (page[after])
                    try:
                        query = parse_qs(urlparse(next_url).query)
                        next_cursor_list = query.get('page[after]')
                        page_cursor = next_cursor_list[0] if next_cursor_list else None
                        if page_cursor:
                            logger.info(f"    🔗 Page suivante trouvée: cursor={page_cursor[:20]}...")
                        else:
                            logger.info(f"    📄 Dernière page atteinte (pas de cursor dans links.next)")
                    except Exception as e:
                        logger.warning(f"    ⚠️ Impossible de parser links.next ({next_url}): {e}")
                        page_cursor = None
                else:
                    page_cursor = None
                    logger.info(f"    📄 Dernière page atteinte (pas de lien next)")
                # --- fin extraction curseur ---
            
                # Traiter chaque transaction
                for tx in transactions:
                    attrs = tx.get('attributes', {}) or {}
                    transfers = attrs.get('transfers', []) or []
                    tx_date = attrs.get('mined_at', '')
                    tx_hash = attrs.get('hash', '')
                    
                    if not tx_hash:
                        continue
                    
                    # Récupérer l'operation_type de la transaction globale
                    operation_type = attrs.get('operation_type', '')
                    
                    # Analyser chaque transfer
                    for transfer in transfers:
                        direction = (transfer.get('direction') or '').lower()
                        # value en USD; certains transferts peuvent renvoyer None
                        try:
                            amount_usd = float(transfer.get('value') or 0.0)
                        except Exception:
                            amount_usd = 0.0
                        
                        # Filtrer par montant minimum
                        if amount_usd < min_amount_usd:
                            continue
                        
                        # Récupérer les infos du token
                        fungible_info = transfer.get('fungible_info') or {}
                        if not fungible_info:
                            continue
                        
                        # Récupérer les vraies adresses from/to du transfer
                        from_address = (transfer.get('from_address') or '').lower()
                        to_address = (transfer.get('to_address') or '').lower()
                        wallet_lower = wallet_address.lower()
                        
                        # LOGIQUE SIMPLE comme wallet_balances_extractor.py: seule la direction compte
                        if operation_type == 'send':
                            if direction == 'out':
                                transaction_direction = 'SEND'
                                wallet_fils = transfer.get('recipient', to_address) or to_address
                            elif direction == 'in':
                                # Cas rare: transaction "send" mais direction "in"
                                transaction_direction = 'RECEIVE'
                                wallet_fils = transfer.get('sender', from_address) or from_address
                            else:
                                continue
                        elif operation_type == 'receive':
                            if direction == 'in':
                                transaction_direction = 'RECEIVE'
                                wallet_fils = transfer.get('sender', from_address) or from_address
                            elif direction == 'out':
                                # Cas rare: transaction "receive" mais direction "out"
                                transaction_direction = 'SEND'
                                wallet_fils = transfer.get('recipient', to_address) or to_address
                            else:
                                continue
                        else:
                            # On ne traite que send/receive maintenant
                            continue
                        
                        if not wallet_fils:
                            continue
                        wallet_fils = wallet_fils.lower()
                        if wallet_fils == wallet_lower:
                            # éviter les boucles vers soi-même
                            continue
                        
                        # Quantité & prix unitaire
                        quantity_raw = (transfer.get('quantity') or {}).get('numeric', 0)
                        try:
                            quantity = float(quantity_raw or 0.0)
                        except Exception:
                            quantity = 0.0
                        price_per_token = (amount_usd / quantity) if quantity > 0 else 0.0
                        
                        # Récup token contract (première implémentation si dispo)
                        implementations = fungible_info.get('implementations') or []
                        token_contract = ''
                        if implementations and isinstance(implementations, list):
                            token_contract = (implementations[0] or {}).get('address', '') or ''
                        
                        # Construire l'entrée de transaction
                        tx_entry = {
                            'wallet_mere': wallet_address,
                            'wallet_fils': wallet_fils,
                            # 'wallet_fils_type' sera rempli dans _process_transactions_batch
                            'transaction_hash': tx_hash,
                            'transaction_date': tx_date,
                            'direction': transaction_direction,
                            'amount_usd': amount_usd,
                            'token_quantity': quantity,
                            'token_symbol': fungible_info.get('symbol', '') or '',
                            'token_contract': token_contract,
                            'price_per_token': price_per_token,
                            'operation_type': operation_type
                        }
                        
                        all_transactions.append(tx_entry)
                
                # Incrémenter le compteur de page
                page_count += 1
                
                # Si pas de page suivante, arrêter
                if not page_cursor:
                    logger.info(f"    ✅ Fin de pagination après {page_count} pages")
                    break
                    
                # Rate limiting entre pages
                time.sleep(0.5)
                
            except Exception as page_error:
                logger.error(f"    ❌ Erreur page {page_count + 1}: {page_error}")
                # on sort proprement; si tu préfères continuer, tu peux remplacer par 'continue'
                break
        
        logger.info(f"    ✅ {len(all_transactions)} transactions > ${min_amount_usd} extraites sur {page_count} pages")
        return all_transactions
    
    def _insert_graph_transactions(self, transactions):
        """Insère les transactions dans la table graph_wallet"""
        if not transactions:
            return 0
        
        insert_query = """
        INSERT OR IGNORE INTO graph_wallet (
            wallet_mere, wallet_fils, wallet_fils_type, direction,
            transaction_hash, transaction_date, amount_usd, token_quantity,
            token_symbol, token_contract, price_per_token, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        cursor = self.db.connection.cursor()
        inserted_count = 0
        
        for tx in transactions:
            try:
                cursor.execute(insert_query, (
                    tx['wallet_mere'],
                    tx['wallet_fils'],
                    tx.get('wallet_fils_type', None),
                    tx['direction'],
                    tx['transaction_hash'],
                    tx['transaction_date'],
                    tx['amount_usd'],
                    tx['token_quantity'],
                    tx['token_symbol'],
                    tx['token_contract'],
                    tx.get('price_per_token', 0),
                    datetime.now()
                ))
                inserted_count += 1
            except Exception as e:
                logger.warning(f"Erreur insertion transaction {tx.get('transaction_hash','?')}: {e}")
        
        self.db.connection.commit()
        return inserted_count
    
    def _wallet_already_processed(self, wallet_mere):
        """Vérifie si un wallet a déjà été traité"""
        query = "SELECT COUNT(*) FROM graph_wallet WHERE wallet_mere = ?"
        cursor = self.db.connection.cursor()
        cursor.execute(query, (wallet_mere,))
        count = cursor.fetchone()[0]
        return count > 0
    
    def _process_transactions_batch(self, transactions, batch_size=10):
        """Traite les transactions par sous-batch avec rate limiting"""
        total_inserted = 0
        total_tx = len(transactions)
        
        for batch_start in range(0, total_tx, batch_size):
            batch_end = min(batch_start + batch_size, total_tx)
            tx_batch = transactions[batch_start:batch_end]
            
            batch_num = (batch_start // batch_size) + 1
            total_batches = (total_tx + batch_size - 1) // batch_size
            
            logger.info(f"    🔄 Sous-batch {batch_num}/{total_batches} - {len(tx_batch)} transactions")
            
            # Vérifier les types de wallet pour ce batch
            for tx in tx_batch:
                wf = tx['wallet_fils']
                if wf not in self.wallet_checker.cache:
                    logger.info(f"      🔍 Vérification: {wf}")
                    tx['wallet_fils_type'] = self.wallet_checker.is_contract(wf)
                    logger.info(f"        → {tx['wallet_fils_type']}")
                else:
                    tx['wallet_fils_type'] = self.wallet_checker.cache[wf]
                    logger.info(f"      📋 Cache: {wf} → {tx['wallet_fils_type']}")
            
            # Insérer ce sous-batch
            inserted = self._insert_graph_transactions(tx_batch)
            total_inserted += inserted
            
            logger.info(f"    ✅ Sous-batch {batch_num}: {inserted} transactions insérées")
            
            # Pause entre sous-batches pour éviter rate limit
            if batch_end < total_tx:
                time.sleep(1)
        
        return total_inserted


def main():
    """Point d'entrée principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Collecteur Graph Wallet')
    parser.add_argument('--wallet', '-w', help='Wallet spécifique à traiter')
    parser.add_argument('--batch-size', '-b', type=int, default=3, help='Taille des batches (défaut: 3)')
    parser.add_argument('--min-amount', '-a', type=float, default=500.0, help='Montant minimum USD (défaut: 500)')
    
    args = parser.parse_args()
    
    logger.info("🚀 Démarrage de la collecte Graph Wallet")
    logger.info(f"⚙️  Configuration:")
    logger.info(f"   💰 Montant minimum: ${args.min_amount}")
    logger.info(f"   📦 Taille batch: {args.batch_size}")
    
    if args.wallet:
        logger.info(f"   🎯 Mode wallet unique: {args.wallet}")
    else:
        logger.info(f"   🔄 Mode batch complet")
    
    collector = GraphWalletCollector()
    
    # Collecte selon les paramètres
    total_transactions = collector.collect_high_value_transactions(
        min_amount_usd=args.min_amount,
        wallet_address=args.wallet,
        batch_size=args.batch_size
    )
    
    logger.info(f"\n📋 RÉSUMÉ FINAL: {total_transactions} transactions collectées")


if __name__ == "__main__":
    main()
