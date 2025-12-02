import os
import time
import requests
import json
import sqlite3
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Tuple
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from urllib.parse import quote

# === Chargement de l'API KEY ===
load_dotenv(dotenv_path=Path(__file__).parent.parent.parent / ".env")
API_KEY = os.getenv("ZERION_API_KEY_2")
if not API_KEY:
    raise ValueError("❌ Clé API manquante. Vérifie ton fichier .env (ZERION_API_KEY).")

# === Configuration ===
ROOT = Path(__file__).parent.parent.parent
DB_PATH = ROOT / "data" / "db" / "wit_database.db"

class SimpleWalletHistoryExtractor:
    """
    Extracteur d'historique complet par token avec sauvegarde en base de données
    Traite les wallets depuis la table wallets et sauvegarde dans tokens et transaction_history
    """
    
    def __init__(self):
        self.headers = {
            "accept": "application/json",
            "authorization": f"Basic {API_KEY}"
        }
        self.db_path = DB_PATH

    def get_wallets_to_process(self) -> List[str]:
        """Récupère la liste des wallets à traiter depuis la table wallets"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT wallet_address 
                    FROM wallets 
                    WHERE is_active = 1 AND transactions_extracted = 0
                """)
                wallets = [row[0] for row in cursor.fetchall()]
                print(f"📊 {len(wallets)} wallets à traiter trouvés en base")
                return wallets
        except sqlite3.Error as e:
            print(f"❌ Erreur lecture DB: {e}")
            return []

    def get_existing_tokens(self, wallet_address: str) -> Dict[str, bool]:
        """Récupère les tokens déjà en base pour ce wallet"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT fungible_id, in_portfolio
                    FROM tokens
                    WHERE wallet_address = ?
                """, (wallet_address,))
                return {row[0]: bool(row[1]) for row in cursor.fetchall()}
        except sqlite3.Error as e:
            print(f"❌ Erreur lecture tokens DB: {e}")
            return {}

    def save_token_to_db(self, wallet_address: str, token_info: Dict, in_portfolio: bool = False):
        """Sauvegarde un token dans la table tokens"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR IGNORE INTO tokens (
                        wallet_address, fungible_id, symbol, contract_address, 
                        chain, in_portfolio, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, (
                    wallet_address,
                    token_info['fungible_id'], 
                    token_info['symbol'],
                    token_info['contract_address'],
                    'ethereum',  # Chaîne par défaut
                    int(in_portfolio)
                ))
                conn.commit()
        except sqlite3.Error as e:
            print(f"❌ Erreur sauvegarde token DB: {e}")

    def save_transaction_to_db(self, wallet_address: str, token_info: Dict, transaction: Dict):
        """Sauvegarde une transaction dans la table transaction_history"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Convertir la date
                try:
                    tx_date = datetime.fromisoformat(transaction['date'].replace('Z', '+00:00'))
                except:
                    tx_date = datetime.now()
                
                cursor.execute("""
                    INSERT OR IGNORE INTO transaction_history (
                        wallet_address, fungible_id, symbol, date, hash, 
                        operation_type, action_type, swap_description, contract_address,
                        quantity, price_per_token, total_value_usd, direction
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    wallet_address,
                    token_info['fungible_id'],
                    token_info['symbol'],
                    tx_date,
                    transaction['transaction_hash'],
                    transaction['operation_type'],
                    transaction['action_type'],
                    None,  # swap_description
                    token_info['contract_address'],
                    transaction['quantity'],
                    transaction['price_per_token'],
                    transaction['value_usd'],
                    transaction.get('direction', '')  # Nouvelle colonne direction
                ))
                conn.commit()
        except sqlite3.Error as e:
            print(f"❌ Erreur sauvegarde transaction DB: {e}")

    def mark_wallet_processed(self, wallet_address: str):
        """Marque un wallet comme traité"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE wallets 
                    SET transactions_extracted = 1, last_sync = CURRENT_TIMESTAMP 
                    WHERE wallet_address = ?
                """, (wallet_address,))
                conn.commit()
        except sqlite3.Error as e:
            print(f"❌ Erreur mise à jour wallet DB: {e}")

    def get_complete_transaction_history(
        self, 
        wallet_address: str, 
        min_value_usd: float = 500,
        operation_types: List[str] = None,  # None = TOUS les types Zerion
        max_pages: int = 2000
    ) -> List[Dict]:
        """
        Récupère TOUT l'historique des transactions d'un wallet
        """
        
        print(f"  📡 API: récupération transactions...")
        
        # Vérifier d'abord le nombre de tokens dans le portefeuille actuel
        portfolio_url = f"https://api.zerion.io/v1/wallets/{wallet_address}/positions/?filter[positions]=only_simple&currency=usd&filter[trash]=only_non_trash"
        
        try:
            response = requests.get(portfolio_url, headers=self.headers, timeout=20)
            if response.status_code == 200:
                data = response.json()
                positions = data.get("data", [])
                token_count = len(positions)
                
                if token_count > 450:
                    print(f"  🚫 Wallet avec {token_count} tokens (>400) - SKIP (probablement un bot/airdrop farmer)")
                    # Marquer comme traité pour éviter de re-tenter
                    self.mark_wallet_processed(wallet_address)
                    return None
                
                print(f"  📊 Wallet avec {token_count} tokens - OK pour extraction")
            else:
                print(f"  ⚠️ Impossible de vérifier le portfolio ({response.status_code}) - continue l'extraction")
                
        except Exception as e:
            print(f"  ⚠️ Erreur vérification portfolio: {e} - continue l'extraction")
        
        # Construire l'URL de base avec filtre anti-trash uniquement
        base_url = f"https://api.zerion.io/v1/wallets/{wallet_address}/transactions/"
        # TOUS les types - pas de filtre operation_types, seulement anti-trash
        base_url += f"?filter%5Btrash%5D=only_non_trash&currency=usd&page%5Bsize%5D=100"
        
        all_transactions = []
        page_cursor = None
        page_count = 0
        seen_hashes = set()
        
        # Détection de patterns de bot
        transaction_times = []
        MAX_TRANSACTIONS = 10000  # Limite absolue
        
        while page_count < max_pages:
            # Construire URL avec pagination
            url = base_url
            if page_cursor:
                url += f"&page[after]={page_cursor}"
            
            try:
                response = requests.get(url, headers=self.headers, timeout=20)
                
                # Gestion spécifique des wallets non supportés
                if response.status_code == 400:
                    error_text = response.text
                    if "Malformed parameter was sent" in error_text:
                        print(f"    ❌ Wallet non supporté par Zerion - marqué comme traité")
                        # Marquer directement comme traité pour éviter les tentatives répétées
                        self.mark_wallet_processed(wallet_address)
                        return None
                    else:
                        print(f"    ❌ API Error 400: {error_text[:100]}...")
                        return None  # Retourner None pour erreur 400
                elif response.status_code != 200:
                    print(f"    ❌ API Error {response.status_code}: {response.text[:100]}...")
                    return None  # Retourner None pour toute erreur HTTP
                
                response.raise_for_status()
                
                data = response.json()
                transactions = data.get("data", [])
                
                # Log progression avec mise à jour sur la même ligne pour les pages suivantes
                if page_count == 0:  # Première page seulement
                    print(f"    📊 Page 1: {len(transactions)} transactions", end="")
                elif page_count % 5 == 0 or len(transactions) < 100:  # Update tous les 5 pages ou dernière page
                    print(f"\r    📈 Page {page_count + 1}: {len(all_transactions) + len(transactions)} transactions totales", end="")
                
                if not transactions:
                    if page_count > 0:
                        print()  # Nouvelle ligne après progression
                    break
                
                # Traiter chaque transaction
                new_transactions = 0
                valid_transactions = 0
                
                for tx in transactions:
                    attrs = tx.get("attributes", {})
                    hash_tx = attrs.get("hash", "")
                    
                    # Éviter les doublons
                    if hash_tx in seen_hashes:
                        continue
                    
                    seen_hashes.add(hash_tx)
                    new_transactions += 1
                    
                    # Vérifier limite absolue de transactions
                    if len(all_transactions) >= MAX_TRANSACTIONS:
                        print(f"\n  🚫 LIMITE ATTEINTE: {MAX_TRANSACTIONS} transactions - Probablement un bot/exchange")
                        self.mark_wallet_processed(wallet_address)
                        return None
                    
                    # Analyser la valeur de la transaction et le timing
                    total_value = self._calculate_transaction_value(attrs)
                    operation_type = attrs.get('operation_type', '')
                    tx_time = attrs.get('mined_at', '')
                    
                    # Collecter les timestamps pour détection de patterns
                    if tx_time:
                        try:
                            from datetime import datetime
                            parsed_time = datetime.fromisoformat(tx_time.replace('Z', '+00:00'))
                            transaction_times.append(parsed_time)
                        except:
                            pass
                    
                    # Détection de bot supprimée - les wallets très actifs peuvent être légitimes
                    
                    # Garder TOUTES les transactions pour analyse complète par token
                    # Le filtrage se fera plus tard au niveau du volume total par token
                    all_transactions.append(tx)
                    valid_transactions += 1
                
                # Vérifier pagination
                links = data.get("links", {})
                next_url = links.get("next")
                
                if not next_url:
                    if page_count > 0:
                        print()  # Nouvelle ligne après progression
                    break
                
                # Extraire le cursor
                if "page%5Bafter%5D=" in next_url:
                    page_cursor = next_url.split("page%5Bafter%5D=")[1].split("&")[0]
                elif "page[after]=" in next_url:
                    page_cursor = next_url.split("page[after]=")[1].split("&")[0]
                else:
                    if page_count > 0:
                        print()  # Nouvelle ligne après progression
                    break
                
                page_count += 1
                time.sleep(0.3)  # Rate limiting réduit pour vitesse
                
            except requests.exceptions.Timeout:
                print(f"    ⏰ API Timeout")
                time.sleep(5)
                break
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 500:
                    print(f"    ⚠️ HTTP 500 - Erreur serveur Zerion, retry impossible")
                    print(f"    📊 {len(all_transactions)} transactions récupérées avant erreur")
                    print(f"    📄 Page en cours: {page_count + 1}")
                    print(f"    🔗 URL: {url}")
                    
                    # Afficher le détail complet de l'erreur 500
                    try:
                        error_detail = e.response.text
                        print(f"    📋 Détail erreur 500:")
                        print(f"         Status: {e.response.status_code}")
                        print(f"         Headers: {dict(e.response.headers)}")
                        print(f"         Response: {error_detail[:500]}...")  # Limiter à 500 chars
                    except Exception as detail_error:
                        print(f"    ❌ Impossible d'afficher détail erreur: {detail_error}")
                    
                    # JAMAIS sauvegarder si erreur 500 (données toujours incomplètes)
                    print(f"    ❌ Erreur 500 détectée - AUCUNE sauvegarde")
                    return None  # Retourner None pour éviter toute sauvegarde
                else:
                    print(f"    ❌ HTTP {e.response.status_code}: {e.response.text[:200]}...")
                time.sleep(5)
                break
            except Exception as e:
                print(f"    ❌ API Error: {str(e)[:50]}...")
                time.sleep(5)
                break
        
        # Affichage final du résumé de récupération
        if all_transactions:
            print(f"\n    ✅ Récupération terminée: {len(all_transactions)} transactions sur {page_count + 1} pages")
        
        return all_transactions
    
    def _calculate_transaction_value(self, attributes: Dict) -> float:
        """Calcule la valeur totale d'une transaction"""
        transfers = attributes.get("transfers", [])
        total_value = 0
        
        for transfer in transfers:
            value = transfer.get("value", 0)
            if value:
                total_value += float(value)
        
        return total_value
    
    def _detect_bot_pattern(self, timestamps: list) -> bool:
        """
        Détecte si les transactions suivent un pattern de bot
        - Transactions trop fréquentes (< 30 secondes d'intervalle)
        - Pattern trop régulier (même intervalle répété)
        """
        if len(timestamps) < 10:
            return False
        
        # Trier les timestamps
        sorted_times = sorted(timestamps)
        intervals = []
        
        # Calculer les intervalles entre transactions consécutives
        for i in range(1, len(sorted_times)):
            interval = (sorted_times[i] - sorted_times[i-1]).total_seconds()
            intervals.append(interval)
        
        # Détecter transactions trop fréquentes (< 30 secondes)
        frequent_count = sum(1 for interval in intervals if interval < 30)
        frequent_ratio = frequent_count / len(intervals)
        
        # Si plus de 50% des transactions sont < 30s d'intervalle
        if frequent_ratio > 0.5:
            return True
        
        # Détecter pattern trop régulier (même intervalle ±10%)
        if len(intervals) >= 20:
            # Prendre les 20 derniers intervalles
            recent_intervals = intervals[-20:]
            avg_interval = sum(recent_intervals) / len(recent_intervals)
            
            # Compter combien d'intervalles sont dans la plage ±10% de la moyenne
            regular_count = 0
            for interval in recent_intervals:
                if abs(interval - avg_interval) / avg_interval < 0.1:  # ±10%
                    regular_count += 1
            
            # Si plus de 80% des intervalles récents sont réguliers
            if regular_count / len(recent_intervals) > 0.8:
                return True
        
        return False
    

    def extract_token_histories(self, wallet_address: str, transactions: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Extrait et regroupe l'historique par token avec sauvegarde en base
        """
        
        print(f"  💾 Analyse et sauvegarde tokens...")
        
        # Récupérer les tokens existants pour ce wallet
        existing_tokens = self.get_existing_tokens(wallet_address)
        
        token_histories = defaultdict(list)
        token_info = {}
        
        # Extraire TOUS les tokens - pas d'exclusion
        excluded_tokens = set()  # Vide - on garde tout
        
        for tx in transactions:
            attrs = tx.get('attributes', {})
            transfers = attrs.get('transfers', [])
            tx_date = attrs.get('mined_at', '')
            tx_hash = attrs.get('hash', '')
            
            # Grouper les transfers par token pour éviter de compter les frais séparément
            transfers_by_token = {}
            for transfer in transfers:
                fungible_info = transfer.get('fungible_info', {})
                if not fungible_info:
                    continue
                    
                fungible_id = fungible_info.get('id', '')
                if not fungible_id:
                    continue
                    
                # Grouper par token
                if fungible_id not in transfers_by_token:
                    transfers_by_token[fungible_id] = []
                transfers_by_token[fungible_id].append(transfer)
            
            # Traiter chaque token (en sommant TOUS les transfers, pas seulement le plus gros)
            for fungible_id, token_transfers in transfers_by_token.items():
                # Sommer TOUS les transfers du même token dans cette transaction
                total_quantity = 0
                total_value = 0
                direction = None
                main_transfer = None
                
                for transfer in token_transfers:
                    quantity = float(transfer.get('quantity', {}).get('numeric', 0) or 0)
                    value = float(transfer.get('value', 0) or 0)
                    
                    total_quantity += quantity
                    total_value += value
                    
                    # Garder le premier transfer pour les métadonnées (direction, fungible_info)
                    if main_transfer is None:
                        main_transfer = transfer
                        direction = transfer.get('direction', '')
                
                if main_transfer is None:
                    continue
                    
                fungible_info = main_transfer.get('fungible_info', {})
                if not fungible_info:
                    continue
                
                symbol = fungible_info.get('symbol', '').upper()
                name = fungible_info.get('name', '')
                fungible_id = fungible_info.get('id', '')
                
                # Skip si token exclu
                if symbol in excluded_tokens:
                    continue
                
                # Skip si nom contient des mots clés de trash (mais garder les stablecoins légitimes)
                trash_keywords = ['test', 'airdrop', 'scam', 'spam', 'fake', 'shit']
                if any(keyword in name.lower() for keyword in trash_keywords):
                    continue
                
                # Skip si symbole contient des mots suspects (mais garder USD pour stablecoins)
                symbol_trash = ['test', 'fake', 'scam', 'spam', 'lplz']
                if any(keyword in symbol.lower() for keyword in symbol_trash):
                    continue
                
                # Skip les tokens avec symbole suspect (trop long, caractères bizarres)
                if len(symbol) > 20 or not symbol.isalnum():
                    continue
                
                if not symbol or not fungible_id:
                    continue
                
                implementations = fungible_info.get('implementations', [])
                contract_address = implementations[0].get('address', '') if implementations else ''
                
                # Stocker les infos du token (on sauvegarde en base plus tard après filtrage)
                if fungible_id not in token_info:
                    token_info[fungible_id] = {
                        'symbol': symbol,
                        'name': name,
                        'contract_address': contract_address,
                        'fungible_id': fungible_id
                    }
                
                # Utiliser les valeurs sommées
                quantity = total_quantity
                transfer_value = total_value
                
                if quantity <= 0:
                    continue
                
                # Calculer le prix par token
                price_per_token = transfer_value / quantity if quantity > 0 else 0
                
                # Classifier le type de transaction - LOGIQUE SIMPLIFIÉE BASÉE SUR LA DIRECTION
                operation_type = attrs.get('operation_type', '')
                
                # LOGIQUE UNIVERSELLE: Utiliser la direction pour déterminer l'action
                if direction == 'in':
                    # Token ENTRE dans le wallet = ACHAT/RÉCEPTION
                    if operation_type in ['trade', 'swap']:
                        action_type = 'buy'
                    elif operation_type in ['execute', 'contract_interaction']:
                        action_type = 'buy'  # Contrat qui donne des tokens
                    else:
                        action_type = 'receive'  # Par défaut pour direction 'in'
                        
                elif direction == 'out':
                    # Token SORT du wallet = VENTE/ENVOI
                    if operation_type in ['trade', 'swap']:
                        action_type = 'sell'
                    elif operation_type in ['execute', 'contract_interaction']:
                        action_type = 'sell'  # Contrat qui prend des tokens
                    else:
                        action_type = 'send'  # Par défaut pour direction 'out'
                        
                else:
                    # Pas de direction claire - fallback sur operation_type
                    if operation_type == 'receive':
                        action_type = 'receive'
                    elif operation_type == 'send':
                        action_type = 'send'
                    elif operation_type in ['mint', 'claim']:
                        action_type = 'receive'
                    elif operation_type == 'burn':
                        action_type = 'send'
                    elif operation_type in ['approve', 'revoke', 'deploy']:
                        # Ignorer ces types qui ne changent pas les soldes
                        continue
                    else:
                        # Log et classifier par défaut
                        print(f"    ⚠️ Operation sans direction claire: {operation_type} - Classé comme réception")
                        action_type = 'receive'
                
                # Types simplifiés pour compatibilité
                transaction_type = operation_type or 'unknown'
                source_type = f"{operation_type}_{direction}" if direction else operation_type
                
                # Données simples de transaction
                token_tx_data = {
                    'transaction_hash': tx_hash,
                    'date': tx_date,
                    'transaction_type': transaction_type,
                    'direction': direction,
                    'source_type': source_type,
                    'action_type': action_type,
                    'quantity': quantity,
                    'price_per_token': price_per_token,
                    'value_usd': transfer_value,
                    'operation_type': operation_type
                }
                
                token_histories[fungible_id].append(token_tx_data)
        
        # Trier les transactions par date pour chaque token
        for token_id in token_histories:
            token_histories[token_id].sort(key=lambda x: x['date'])
        
        # FILTRER LES TOKENS PAR VOLUME TOTAL : seuil $500 minimum
        filtered_token_histories = {}
        filtered_token_info = {}
        
        MIN_TOKEN_VOLUME_THRESHOLD = 500  # $500 minimum d'activité totale
        
        print(f"  🔍 Filtrage tokens par volume (seuil: ${MIN_TOKEN_VOLUME_THRESHOLD})...")
        
        tokens_kept = 0
        tokens_rejected = 0
        total_transactions_saved = 0
        
        for token_id, transactions in token_histories.items():
            # Calculer le volume total d'activité sur ce token (tous types confondus)
            total_volume = sum(tx['value_usd'] for tx in transactions)
            symbol = token_info[token_id]['symbol']
            
            # Filtrer par volume total
            if total_volume >= MIN_TOKEN_VOLUME_THRESHOLD:
                # Token avec activité significative → GARDER
                filtered_token_histories[token_id] = transactions
                filtered_token_info[token_id] = token_info[token_id]
                tokens_kept += 1
                
                # MAINTENANT sauvegarder le token en base (seulement si il passe le filtre volume)
                if token_id not in existing_tokens:
                    self.save_token_to_db(wallet_address, token_info[token_id], in_portfolio=False)
                
                # Sauvegarder toutes les transactions pour ce token filtré
                for transaction in transactions:
                    self.save_transaction_to_db(wallet_address, token_info[token_id], transaction)
                    total_transactions_saved += 1
                
                print(f"    ✅ {symbol}: ${total_volume:,.0f} volume, {len(transactions)} txs")
            else:
                # Token avec volume insuffisant → REJETER
                tokens_rejected += 1
                print(f"    🚫 {symbol}: ${total_volume:,.0f} < ${MIN_TOKEN_VOLUME_THRESHOLD} - rejeté")
        
        print(f"  📊 Résultat filtrage: {tokens_kept} tokens gardés, {tokens_rejected} rejetés")
        print(f"  💾 {total_transactions_saved} transactions sauvegardées en base")
        
        
        return dict(filtered_token_histories), filtered_token_info
    

def extract_wallet_simple_history(wallet_address: str, min_value_usd: float = 500):
    """
    Fonction principale : extraction historique et sauvegarde en base
    """
    
    extractor = SimpleWalletHistoryExtractor()
    
    # 1. Récupérer toutes les transactions - TOUS les operation_types Zerion
    transactions = extractor.get_complete_transaction_history(
        wallet_address=wallet_address,
        min_value_usd=min_value_usd,
        operation_types=None,  # None = TOUS les types Zerion
        max_pages=2000
    )
    
    if not transactions:
        # NE PAS marquer comme traité si pas de transactions (erreur ou wallet vide)
        print(f"  ❌ Aucune transaction récupérée - wallet non marqué comme traité")
        return None
    
    # 2. Extraire l'historique par token
    token_histories, token_info = extractor.extract_token_histories(wallet_address, transactions)
    
    if not token_histories:
        # NE PAS marquer comme traité si aucun token valide trouvé
        print(f"  ❌ Aucun token valide trouvé - wallet non marqué comme traité")
        return None
    
    # 3. Marquer le wallet comme traité SEULEMENT si tout s'est bien passé
    extractor.mark_wallet_processed(wallet_address)
    
    print(f"  ✅ Terminé: {len(token_histories)} tokens sauvegardés")
    
    return True

def process_wallet_batch(wallet_batch: List[str], min_value_usd: float = 500) -> Tuple[int, int]:
    """Traite un batch de wallets - fonction helper pour parallélisation"""
    successful_wallets = 0
    failed_wallets = 0
    
    for wallet_address in wallet_batch:
        try:
            print(f"🔄 Traitement {wallet_address[:12]}...")
            result = extract_wallet_simple_history(wallet_address, min_value_usd)
            if result:
                successful_wallets += 1
                print(f"  ✅ {wallet_address[:12]}... terminé")
            else:
                failed_wallets += 1
                print(f"  ❌ {wallet_address[:12]}... échec")
                
            # Pause entre wallets (dans le même worker)
            time.sleep(2)  # Optimisé pour parallélisation
                
        except Exception as e:
            failed_wallets += 1
            print(f"  ❌ Erreur {wallet_address[:12]}...: {e}")
            
    return successful_wallets, failed_wallets

def process_all_wallets_from_db(min_value_usd: float = 500, batch_size: int = 10, batch_delay: int = 30):
    """
    Fonction principale : traite tous les wallets non traités par batches avec délais
    """
    
    extractor = SimpleWalletHistoryExtractor()
    
    # Récupérer la liste des wallets à traiter
    wallets_to_process = extractor.get_wallets_to_process()
    
    if not wallets_to_process:
        print("✅ Aucun wallet à traiter trouvé en base")
        return
    
    print(f"🚀 === TRAITEMENT PAR BATCHES DE {len(wallets_to_process)} WALLETS ===")
    print(f"🎯 Configuration: {batch_size} wallets par batch, {batch_delay}s entre batches")
    print("=" * 80)
    
    # Diviser les wallets en batches
    wallet_batches = []
    for i in range(0, len(wallets_to_process), batch_size):
        batch = wallets_to_process[i:i + batch_size]
        wallet_batches.append(batch)
    
    print(f"📦 {len(wallet_batches)} batches créés (taille: {batch_size})")
    print(f"⏱️ Temps estimé: ~{(len(wallet_batches) * batch_delay) // 60}min")
    print("=" * 80)
    
    total_successful = 0
    total_failed = 0
    start_time = datetime.now()
    
    # Traitement séquentiel des batches avec délais
    for batch_id, batch in enumerate(wallet_batches, 1):
        print(f"\n🔄 === BATCH {batch_id}/{len(wallet_batches)} ===")
        print(f"📋 {len(batch)} wallets à traiter")
        
        batch_start = datetime.now()
        batch_successful = 0
        batch_failed = 0
        
        # Traiter chaque wallet du batch
        for wallet_idx, wallet_address in enumerate(batch, 1):
            try:
                print(f"  🔄 [{wallet_idx}/{len(batch)}] {wallet_address[:12]}...")
                result = extract_wallet_simple_history(wallet_address, min_value_usd)
                
                if result:
                    batch_successful += 1
                    total_successful += 1
                    print(f"    ✅ Terminé")
                else:
                    batch_failed += 1
                    total_failed += 1
                    print(f"    ❌ Échec")
                
                # Délai entre wallets dans le batch
                if wallet_idx < len(batch):  # Pas de délai après le dernier wallet
                    time.sleep(3)
                    
            except Exception as e:
                batch_failed += 1
                total_failed += 1
                print(f"    ❌ Erreur: {str(e)[:50]}...")
        
        # Statistiques du batch
        batch_duration = datetime.now() - batch_start
        elapsed_total = datetime.now() - start_time
        
        print(f"\n📊 === RÉSUMÉ BATCH {batch_id} ===")
        print(f"✅ Succès: {batch_successful}/{len(batch)}")
        print(f"❌ Échecs: {batch_failed}/{len(batch)}")
        print(f"⏱️ Durée batch: {batch_duration}")
        print(f"⏱️ Temps écoulé total: {elapsed_total}")
        
        # Progression globale
        progress = (batch_id / len(wallet_batches)) * 100
        remaining_batches = len(wallet_batches) - batch_id
        eta_minutes = (remaining_batches * batch_delay) // 60
        
        print(f"📈 Progression globale: {progress:.1f}% ({total_successful + total_failed}/{len(wallets_to_process)} wallets)")
        print(f"🕐 ETA: ~{eta_minutes}min restantes")
        
        # Délai entre batches (sauf pour le dernier)
        if batch_id < len(wallet_batches):
            print(f"⏸️ Pause {batch_delay}s avant batch suivant...")
            time.sleep(batch_delay)
    
    # Résumé final
    total_duration = datetime.now() - start_time
    print(f"\n🏆 === RÉSULTAT FINAL ===")
    print(f"✅ Succès: {total_successful}/{len(wallets_to_process)} ({(total_successful/len(wallets_to_process)*100):.1f}%)")
    print(f"❌ Échecs: {total_failed}/{len(wallets_to_process)} ({(total_failed/len(wallets_to_process)*100):.1f}%)")
    print(f"⏱️ Durée totale: {total_duration}")
    print(f"📦 {len(wallet_batches)} batches traités")
    print("=" * 80)

def process_smart_wallets_only(min_value_usd: float = 500, batch_size: int = 5, batch_delay: int = 10):
    """
    Fonction pour traiter uniquement les smart wallets
    """
    
    extractor = SimpleWalletHistoryExtractor()
    
    # Récupérer la liste des smart wallets
    try:
        with sqlite3.connect(extractor.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT wallet_address 
                FROM smart_wallets
            """)
            smart_wallets = [row[0] for row in cursor.fetchall()]
            print(f"📊 {len(smart_wallets)} smart wallets trouvés en base")
    except sqlite3.Error as e:
        print(f"❌ Erreur lecture DB: {e}")
        return
    
    if not smart_wallets:
        print("✅ Aucun smart wallet à traiter")
        return
    
    print(f"🚀 === TRAITEMENT SMART WALLETS SEULEMENT ===")
    print(f"🎯 Configuration: {batch_size} wallets par batch, {batch_delay}s entre batches")
    print("=" * 80)
    
    # Diviser les wallets en batches
    wallet_batches = []
    for i in range(0, len(smart_wallets), batch_size):
        batch = smart_wallets[i:i + batch_size]
        wallet_batches.append(batch)
    
    print(f"📦 {len(wallet_batches)} batches créés (taille: {batch_size})")
    print(f"⏱️ Temps estimé: ~{(len(wallet_batches) * batch_delay) // 60}min")
    print("=" * 80)
    
    total_successful = 0
    total_failed = 0
    start_time = datetime.now()
    
    # Traitement séquentiel des batches avec délais
    for batch_id, batch in enumerate(wallet_batches, 1):
        print(f"\n🔄 === BATCH {batch_id}/{len(wallet_batches)} ===")
        print(f"📋 {len(batch)} smart wallets à traiter")
        
        batch_start = datetime.now()
        batch_successful = 0
        batch_failed = 0
        
        # Traiter chaque wallet du batch
        for wallet_idx, wallet_address in enumerate(batch, 1):
            try:
                print(f"  🔄 [{wallet_idx}/{len(batch)}] {wallet_address[:12]}...")
                result = extract_wallet_simple_history(wallet_address, min_value_usd)
                
                if result:
                    batch_successful += 1
                    total_successful += 1
                    print(f"    ✅ Terminé")
                else:
                    batch_failed += 1
                    total_failed += 1
                    print(f"    ❌ Échec")
                
                # Délai entre wallets dans le batch
                if wallet_idx < len(batch):  # Pas de délai après le dernier wallet
                    time.sleep(3)
                    
            except Exception as e:
                batch_failed += 1
                total_failed += 1
                print(f"    ❌ Erreur: {str(e)[:50]}...")
        
        # Statistiques du batch
        batch_duration = datetime.now() - batch_start
        elapsed_total = datetime.now() - start_time
        
        print(f"\n📊 === RÉSUMÉ BATCH {batch_id} ===")
        print(f"✅ Succès: {batch_successful}/{len(batch)}")
        print(f"❌ Échecs: {batch_failed}/{len(batch)}")
        print(f"⏱️ Durée batch: {batch_duration}")
        print(f"⏱️ Temps écoulé total: {elapsed_total}")
        
        # Progression globale
        progress = (batch_id / len(wallet_batches)) * 100
        remaining_batches = len(wallet_batches) - batch_id
        eta_minutes = (remaining_batches * batch_delay) // 60
        
        print(f"📈 Progression globale: {progress:.1f}% ({total_successful + total_failed}/{len(smart_wallets)} wallets)")
        print(f"🕐 ETA: ~{eta_minutes}min restantes")
        
        # Délai entre batches (sauf pour le dernier)
        if batch_id < len(wallet_batches):
            print(f"⏸️ Pause {batch_delay}s avant batch suivant...")
            time.sleep(batch_delay)
    
    # Résumé final
    total_duration = datetime.now() - start_time
    print(f"\n🏆 === RÉSULTAT FINAL ===")
    print(f"✅ Succès: {total_successful}/{len(smart_wallets)} ({(total_successful/len(smart_wallets)*100):.1f}%)")
    print(f"❌ Échecs: {total_failed}/{len(smart_wallets)} ({(total_failed/len(smart_wallets)*100):.1f}%)")
    print(f"⏱️ Durée totale: {total_duration}")
    print(f"📦 {len(wallet_batches)} batches traités")
    print("=" * 80)

# === Lancement direct ===
if __name__ == "__main__":
    # Traitement par batches avec suivi de progression
    #wallet='0x16b361681e7a8d1bfecbc88f3f087b6db40b2260'
    #extract_wallet_simple_history(wallet,min_value_usd=500)
    process_all_wallets_from_db(min_value_usd=500, batch_size=10, batch_delay=10)
