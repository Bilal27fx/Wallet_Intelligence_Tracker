import os
import time
import requests
import pandas as pd
import json
import sqlite3
import uuid
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

# === Configuration ===
load_dotenv(dotenv_path=Path(__file__).parent.parent.parent / ".env")

# Récupération des deux clés API
API_KEY_1 = os.getenv("ZERION_API_KEY")
API_KEY_2 = os.getenv("ZERION_API_KEY_2")

if not API_KEY_1:
    raise ValueError("❌ Clé API principale manquante. Vérifie ton fichier .env (ZERION_API_KEY).")
if not API_KEY_2:
    raise ValueError("❌ Clé API secondaire manquante. Vérifie ton fichier .env (ZERION_API_KEY_2).")

# Système de rotation des clés API
API_KEYS = [API_KEY_1, API_KEY_2]
api_key_index = 0

def get_current_api_key():
    """Retourne la clé API actuellement utilisée"""
    global api_key_index
    return API_KEYS[api_key_index]

def rotate_api_key():
    """Fait tourner vers la clé API suivante"""
    global api_key_index
    api_key_index = (api_key_index + 1) % len(API_KEYS)
    print(f"🔄 Rotation vers clé API {api_key_index + 1}")
    return API_KEYS[api_key_index]

# === Répertoires et Base de données ===
ROOT = Path(__file__).parent.parent.parent
DB_PATH = ROOT / "data" / "db" / "wit_database.db"

# === Fonctions SQL pour récupérer les wallets avec changements ===
def get_wallets_with_recent_changes(hours=24):
    """Récupère les wallets avec changements récents depuis la BDD"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Récupérer les wallets avec changements dans les dernières X heures
        query = """
            SELECT DISTINCT wpc.wallet_address, COUNT(*) as change_count
            FROM wallet_position_changes wpc
            WHERE wpc.detected_at >= datetime('now', '-{} hours')
            GROUP BY wpc.wallet_address
            ORDER BY change_count DESC
        """.format(hours)
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        wallets = df['wallet_address'].tolist()
        
        print(f"📊 {len(wallets)} wallets avec changements dans les {hours}h dernières")
        if not df.empty:
            total_changes = df['change_count'].sum()
            print(f"🔄 {total_changes} changements détectés au total")
        
        return wallets
        
    except Exception as e:
        print(f"❌ Erreur récupération wallets avec changements: {e}")
        return []

# Fonctions de snapshot supprimées - plus besoin avec la nouvelle logique

def get_tokens_for_wallet_from_db(wallet_address):
    """Récupère les tokens d'un wallet depuis la BDD (UNIQUEMENT les tokens en portefeuille actuel)"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Utiliser la table tokens avec in_portfolio=1 pour les positions actuelles
        query = """
            SELECT t.symbol as token, t.contract_address, t.fungible_id, 
                   t.current_amount as amount, t.current_usd_value as usd_value, 
                   t.current_price_per_token as price_per_token
            FROM tokens t
            WHERE t.wallet_address = ?
            AND t.in_portfolio = 1
            AND t.current_usd_value >= 500
            ORDER BY t.current_usd_value DESC
        """
        
        df = pd.read_sql_query(query, conn, params=[wallet_address])
        conn.close()
        
        tokens_data = df.to_dict('records')
        
        print(f"    💰 {len(tokens_data)} tokens EN PORTEFEUILLE trouvés pour {wallet_address}")
        return tokens_data
        
    except Exception as e:
        print(f"    ❌ Erreur récupération tokens BDD {wallet_address}: {e}")
        return []

# Fonctions de transactions générales supprimées - plus besoin avec la nouvelle logique

# === Fonctions de récupération de l'historique par token ===

def get_token_transaction_history_zerion_full(wallet_address, fungible_id, retries=3):
    """Récupération complète sans cache (fonction originale renommée)"""
    
    headers = {
        "accept": "application/json",
        "authorization": f"Basic {get_current_api_key()}"
    }
    
    all_transactions = []
    page_cursor = None
    page_count = 0
    max_pages = 25 # Limite de sécurité
    
    seen_transaction_hashes = set()
    should_stop_pagination = False
    
    while page_count < max_pages and not should_stop_pagination:
        # Construire l'URL avec pagination
        url = f"https://api.zerion.io/v1/wallets/{wallet_address}/transactions/?filter[fungible_ids]={fungible_id}&currency=usd&page[size]=100"
        if page_cursor:
            url += f"&page[after]={page_cursor}"
        
        for attempt in range(retries):
            try:
                response = requests.get(url, headers=headers, timeout=15)
                response.raise_for_status()
                
                data = response.json()
                transactions = data.get("data", [])
                page_count += 1
                
                # Si page vide, on s'arrête (fin des données)
                if not transactions:
                    print(f"      📄 Page {page_count}: vide - fin des données (total: {len(all_transactions)})")
                    should_stop_pagination = True
                    break
                
                # Vérifier les transactions dupliquées
                new_transactions = []
                duplicates_count = 0
                
                for tx in transactions:
                    tx_hash = tx.get("attributes", {}).get("hash", "")
                    if tx_hash and tx_hash not in seen_transaction_hashes:
                        seen_transaction_hashes.add(tx_hash)
                        new_transactions.append(tx)
                    else:
                        duplicates_count += 1
                
                # Si toutes les transactions sont des doublons, arrêter la pagination
                if duplicates_count == len(transactions) and len(transactions) > 0:
                    print(f"      📄 Page {page_count}: {duplicates_count} doublons détectés - fin de pagination (total: {len(all_transactions)})")
                    should_stop_pagination = True
                    break  # Sortir de la boucle retry
                
                # Si on arrive ici et qu'on n'a pas de nouvelles transactions, sortir aussi
                if not new_transactions:
                    print(f"      📄 Page {page_count}: aucune nouvelle transaction - fin de pagination")
                    should_stop_pagination = True
                    break  # Sortir de la boucle retry
                
                all_transactions.extend(new_transactions)
                if new_transactions:
                    print(f"      📄 Page {page_count}: +{len(new_transactions)} nouvelles transactions (total: {len(all_transactions)})")
                else:
                    print(f"      📄 Page {page_count}: aucune nouvelle transaction - fin de pagination")
                    should_stop_pagination = True
                    break
                
                # Vérifier s'il y a une page suivante dans les métadonnées
                links = data.get("links", {})
                next_url = links.get("next")
                
                if not next_url:
                    # Pas de page suivante dans les métadonnées = fin
                    print(f"      ✅ Fin de pagination - passage au token suivant")
                    should_stop_pagination = True
                    break
                
                # Extraire le cursor (format URL encodé)
                if "page%5Bafter%5D=" in next_url:
                    page_cursor = next_url.split("page%5Bafter%5D=")[1].split("&")[0]
                elif "page[after]=" in next_url:
                    page_cursor = next_url.split("page[after]=")[1].split("&")[0]
                else:
                    print(f"      ⚠️ Format cursor non reconnu, arrêt pagination")
                    should_stop_pagination = True
                    break
                
                # Délai entre les pages pour respecter rate limit
                time.sleep(2.0)  # Augmenté à 2s pour éviter rate limits
                break
                
            except Exception as e:
                if attempt < retries - 1:
                    print(f"      ⚠️ Retry page {page_count + 1}, tentative {attempt + 1}/{retries}")
                    time.sleep(2)
                else:
                    error_msg = str(e)
                    if "429" in error_msg or "rate limit" in error_msg.lower() or "too many requests" in error_msg.lower():
                        print(f"      🚧 Rate limit détecté pour pagination {wallet_address}, rotation clé API...")
                        rotate_api_key()
                        time.sleep(5)
                        # Retry avec la nouvelle clé API
                        print(f"      🔄 Retry transactions avec nouvelle clé pour {wallet_address}")
                        return get_token_transaction_history_zerion_full(wallet_address, fungible_id, retries)
                    else:
                        print(f"      ❌ Erreur pagination après {retries} tentatives: {e}")
                    return []
        else:
            # Si on arrive ici, toutes les tentatives ont échoué
            break
    
    if not all_transactions:
        return []
    
    print(f"✅ {len(all_transactions)} transactions récupérées sur {page_count} pages")
    
    return all_transactions

# === Fonction pour récupérer l'historique par token pour les wallets avec changements ===
def replace_complete_token_history(wallet_address, session_id, tokens_to_track):
    """Remplace complètement l'historique des tokens avec changements - APPROCHE SIMPLE"""
    
    print(f"    🔄 Remplacement complet historique pour {len(tokens_to_track)} tokens...")
    
    total_transactions_replaced = 0
    
    for token_data in tokens_to_track:
        token_symbol = token_data['token']
        fungible_id = token_data.get('fungible_id', '')
        contract_address = token_data.get('contract_address', '')
        
        if not fungible_id:
            print(f"        ⚠️ Pas de fungible_id pour {token_symbol}, skip")
            continue
        
        print(f"        🔄 {token_symbol} - Remplacement complet de l'historique...")
        
        # 1. Supprimer TOUT l'ancien historique de ce token pour ce wallet
        old_count = delete_token_history(wallet_address, token_symbol)
        if old_count > 0:
            print(f"            🗑️ {old_count} anciennes transactions supprimées")
        
        # 2. Récupérer TOUT le nouvel historique depuis Zerion
        raw_transactions = get_token_transaction_history_zerion_full(wallet_address, fungible_id)
        
        if raw_transactions:
            # 3. Analyser et stocker TOUT le nouvel historique (pas de filtre)
            new_tx_count = analyze_and_store_complete_transactions(
                session_id, wallet_address, token_symbol, fungible_id, 
                contract_address, raw_transactions
            )
            
            total_transactions_replaced += new_tx_count
            
            print(f"        ✅ {new_tx_count} transactions complètes ajoutées pour {token_symbol}")
            
            # 4. Nettoyer immédiatement le changement traité
            clean_processed_change(wallet_address, token_symbol)
            
        else:
            print(f"        ❌ Aucune transaction récupérée pour {token_symbol}")
            # Nettoyer quand même le changement pour éviter les re-tentatives
            clean_processed_change(wallet_address, token_symbol)
        
        # Pause pour éviter rate limiting
        time.sleep(1.5)
    
    print(f"    📊 Total: {total_transactions_replaced} transactions remplacées dans transaction_history")
    return total_transactions_replaced

def delete_token_history(wallet_address, token_symbol):
    """Supprime complètement l'historique d'un token pour un wallet"""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()
        
        # Compter d'abord combien on va supprimer
        cursor.execute("""
            SELECT COUNT(*) FROM transaction_history 
            WHERE wallet_address = ? AND symbol = ?
        """, (wallet_address, token_symbol))
        
        count = cursor.fetchone()[0]
        
        # Supprimer tout l'historique de ce token
        cursor.execute("""
            DELETE FROM transaction_history 
            WHERE wallet_address = ? AND symbol = ?
        """, (wallet_address, token_symbol))
        
        conn.commit()
        conn.close()
        
        return count
        
    except Exception as e:
        print(f"        ❌ Erreur suppression historique {token_symbol}: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return 0

def analyze_and_store_complete_transactions(session_id, wallet_address, token_symbol, fungible_id, 
                                          contract_address, raw_transactions):
    """Analyse et stocke TOUTES les transactions (pas de filtre incrémental)"""
    
    if not raw_transactions:
        return 0
    
    # Analyser et formater TOUTES les transactions
    formatted_transactions = []
    
    for tx in raw_transactions:
        attrs = tx.get("attributes", {})
        hash_tx = attrs.get("hash", "")
        date = attrs.get("mined_at", "")
        operation_type = attrs.get("operation_type", "")
        
        # Analyser les transfers pour ce token spécifique
        transfers = attrs.get("transfers", [])
        
        target_token_in = 0
        target_token_out = 0
        target_value_in = 0
        target_value_out = 0
        recipient_address = None
        sender_address = None

        for transfer in transfers:
            fungible_info = transfer.get("fungible_info", {})
            token_id = fungible_info.get("id", "")
            direction = transfer.get("direction", "")

            # Ignorer les transferts "self" (wallet s'envoie à lui-même)
            if direction == "self":
                continue

            # Si c'est le token qu'on analyse
            if token_id == fungible_id:
                quantity_data = transfer.get("quantity", {})
                amount = float(quantity_data.get("numeric", 0)) if isinstance(quantity_data, dict) else 0
                transfer_value = float(transfer.get("value", 0) or 0)

                # Capturer sender et recipient pour les migrations
                if direction == "out":
                    recipient_address = transfer.get("recipient")
                elif direction == "in":
                    sender_address = transfer.get("sender")

                if direction == "in":
                    target_token_in += amount
                    target_value_in += transfer_value
                elif direction == "out":
                    target_token_out += amount
                    target_value_out += transfer_value
        
        # Déterminer l'action et la quantité nette
        if target_token_in > 0 and target_token_out == 0:
            action_type = "buy"
            quantity = target_token_in
            swap_description = f"Achat: +{target_token_in:.6f} {token_symbol}"
        elif target_token_out > 0 and target_token_in == 0:
            action_type = "sell"
            quantity = -target_token_out
            swap_description = f"Vente: -{target_token_out:.6f} {token_symbol}"
        elif target_token_in > 0 and target_token_out > 0:
            net = target_token_in - target_token_out
            if net > 0:
                action_type = "buy"
                quantity = net
                swap_description = f"Achat net: +{net:.6f} {token_symbol}"
            else:
                action_type = "sell"
                quantity = net
                swap_description = f"Vente net: {net:.6f} {token_symbol}"
        else:
            continue  # Pas de mouvement sur ce token
        
        # Calculer la valeur totale et le prix
        total_value = target_value_in + target_value_out
        if operation_type == "trade" and target_value_in > 0 and target_value_out > 0:
            # Pour les swaps, diviser par 2 pour éviter double comptage
            balance_ratio = min(target_value_in, target_value_out) / max(target_value_in, target_value_out)
            if balance_ratio >= 0.8:
                total_value = total_value / 2
        
        price_per_token = total_value / abs(quantity) if quantity != 0 else 0
        
        # Calculer direction basée sur quantity
        direction = "in" if quantity > 0 else "out"
        
        formatted_transactions.append({
            "hash": hash_tx,
            "date": date,
            "operation_type": operation_type,
            "action_type": action_type,
            "swap_description": swap_description,
            "quantity": quantity,
            "price_per_token": price_per_token,
            "total_value_usd": total_value,
            "direction": direction,
            "recipient_address": recipient_address,
            "sender_address": sender_address
        })
    
    # Stocker TOUT dans transaction_history
    if formatted_transactions:
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30.0)
            conn.execute("PRAGMA journal_mode=WAL")
            cursor = conn.cursor()
            
            for tx in formatted_transactions:
                cursor.execute("""
                    INSERT OR REPLACE INTO transaction_history (
                        wallet_address, fungible_id, symbol, date, hash,
                        operation_type, action_type, swap_description, contract_address,
                        quantity, price_per_token, total_value_usd, direction,
                        recipient_address, sender_address
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    wallet_address, fungible_id, token_symbol, tx['date'], tx['hash'],
                    tx['operation_type'], tx['action_type'], tx['swap_description'],
                    contract_address, tx['quantity'], tx['price_per_token'], tx['total_value_usd'], tx['direction'],
                    tx.get('recipient_address'), tx.get('sender_address')
                ))
            
            conn.commit()
            conn.close()
            
            return len(formatted_transactions)
            
        except Exception as e:
            print(f"        ❌ Erreur stockage historique complet {token_symbol}: {e}")
            if conn:
                conn.rollback()
                conn.close()
            return 0
    
    return 0

def clean_processed_change(wallet_address, token_symbol):
    """Nettoie immédiatement un changement traité pour éviter les re-traitements"""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()
        
        # Supprimer tous les changements de ce token pour ce wallet
        cursor.execute("""
            DELETE FROM wallet_position_changes 
            WHERE wallet_address = ? AND symbol = ?
        """, (wallet_address, token_symbol))
        
        deleted_count = cursor.rowcount
        if deleted_count > 0:
            print(f"            🧹 {deleted_count} changements nettoyés pour {token_symbol}")
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        print(f"            ⚠️ Erreur nettoyage changement {token_symbol}: {e}")
        if conn:
            conn.rollback()
            conn.close()

def get_last_transaction_snapshot_per_token(wallet_address, tokens_to_check):
    """Récupère le dernier snapshot de transaction pour chaque token d'un wallet"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        snapshots = {}
        
        for token_data in tokens_to_check:
            token_symbol = token_data['token']
            fungible_id = token_data.get('fungible_id', '')
            
            # Récupérer la dernière transaction de ce token pour ce wallet
            query = """
                SELECT hash, date, quantity, total_value_usd
                FROM transaction_history 
                WHERE wallet_address = ? AND symbol = ?
                ORDER BY date DESC 
                LIMIT 1
            """
            
            cursor = conn.cursor()
            cursor.execute(query, (wallet_address, token_symbol))
            result = cursor.fetchone()
            
            if result:
                snapshots[token_symbol] = {
                    'last_hash': result[0],
                    'last_date': result[1],
                    'last_quantity': result[2],
                    'last_value': result[3],
                    'fungible_id': fungible_id
                }
            else:
                # Pas d'historique pour ce token
                snapshots[token_symbol] = {
                    'last_hash': None,
                    'last_date': None,
                    'last_quantity': 0,
                    'last_value': 0,
                    'fungible_id': fungible_id,
                    'is_new_token': True
                }
        
        conn.close()
        return snapshots
        
    except Exception as e:
        print(f"        ❌ Erreur récupération snapshots: {e}")
        return {}

def analyze_and_store_new_transactions(session_id, wallet_address, token_symbol, fungible_id, 
                                     contract_address, raw_transactions, last_known_hash=None):
    """Analyse les transactions brutes et stocke seulement les nouvelles dans transaction_history"""
    
    if not raw_transactions:
        return 0
    
    # Filtrer les nouvelles transactions si on a un hash de référence
    new_transactions = []
    
    if last_known_hash:
        # Chercher les transactions depuis le dernier hash connu
        for tx in raw_transactions:
            tx_hash = tx.get("attributes", {}).get("hash", "")
            if tx_hash == last_known_hash:
                break  # On a atteint la dernière transaction connue
            new_transactions.append(tx)
    else:
        # Pas de baseline, prendre toutes les transactions (première fois)
        new_transactions = raw_transactions
    
    if not new_transactions:
        return 0
    
    # Analyser et formater les transactions
    formatted_transactions = []
    
    for tx in new_transactions:
        attrs = tx.get("attributes", {})
        hash_tx = attrs.get("hash", "")
        date = attrs.get("mined_at", "")
        operation_type = attrs.get("operation_type", "")
        
        # Analyser les transfers pour ce token spécifique
        transfers = attrs.get("transfers", [])
        
        target_token_in = 0
        target_token_out = 0
        target_value_in = 0
        target_value_out = 0
        recipient_address = None
        sender_address = None

        for transfer in transfers:
            fungible_info = transfer.get("fungible_info", {})
            token_id = fungible_info.get("id", "")
            direction = transfer.get("direction", "")

            # Ignorer les transferts "self" (wallet s'envoie à lui-même)
            if direction == "self":
                continue

            # Si c'est le token qu'on analyse
            if token_id == fungible_id:
                quantity_data = transfer.get("quantity", {})
                amount = float(quantity_data.get("numeric", 0)) if isinstance(quantity_data, dict) else 0
                transfer_value = float(transfer.get("value", 0) or 0)

                # Capturer sender et recipient pour les migrations
                if direction == "out":
                    recipient_address = transfer.get("recipient")
                elif direction == "in":
                    sender_address = transfer.get("sender")

                if direction == "in":
                    target_token_in += amount
                    target_value_in += transfer_value
                elif direction == "out":
                    target_token_out += amount
                    target_value_out += transfer_value
        
        # Déterminer l'action et la quantité nette
        if target_token_in > 0 and target_token_out == 0:
            action_type = "buy"
            quantity = target_token_in
            swap_description = f"Achat: +{target_token_in:.6f} {token_symbol}"
        elif target_token_out > 0 and target_token_in == 0:
            action_type = "sell"
            quantity = -target_token_out
            swap_description = f"Vente: -{target_token_out:.6f} {token_symbol}"
        elif target_token_in > 0 and target_token_out > 0:
            net = target_token_in - target_token_out
            if net > 0:
                action_type = "buy"
                quantity = net
                swap_description = f"Achat net: +{net:.6f} {token_symbol}"
            else:
                action_type = "sell"
                quantity = net
                swap_description = f"Vente net: {net:.6f} {token_symbol}"
        else:
            continue  # Pas de mouvement sur ce token
        
        # Calculer la valeur totale et le prix
        total_value = target_value_in + target_value_out
        if operation_type == "trade" and target_value_in > 0 and target_value_out > 0:
            # Pour les swaps, diviser par 2 pour éviter double comptage
            balance_ratio = min(target_value_in, target_value_out) / max(target_value_in, target_value_out)
            if balance_ratio >= 0.8:
                total_value = total_value / 2
        
        price_per_token = total_value / abs(quantity) if quantity != 0 else 0
        
        # Calculer direction basée sur quantity
        direction = "in" if quantity > 0 else "out"
        
        formatted_transactions.append({
            "hash": hash_tx,
            "date": date,
            "operation_type": operation_type,
            "action_type": action_type,
            "swap_description": swap_description,
            "quantity": quantity,
            "price_per_token": price_per_token,
            "total_value_usd": total_value,
            "direction": direction,
            "recipient_address": recipient_address,
            "sender_address": sender_address
        })
    
    # Stocker dans transaction_history
    if formatted_transactions:
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30.0)
            conn.execute("PRAGMA journal_mode=WAL")
            cursor = conn.cursor()
            
            for tx in formatted_transactions:
                cursor.execute("""
                    INSERT OR IGNORE INTO transaction_history (
                        wallet_address, fungible_id, symbol, date, hash,
                        operation_type, action_type, swap_description, contract_address,
                        quantity, price_per_token, total_value_usd
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    wallet_address, fungible_id, token_symbol, tx['date'], tx['hash'],
                    tx['operation_type'], tx['action_type'], tx['swap_description'], 
                    contract_address, tx['quantity'], tx['price_per_token'], tx['total_value_usd']
                ))
            
            conn.commit()
            conn.close()
            
            print(f"        💾 {len(formatted_transactions)} nouvelles transactions stockées pour {token_symbol}")
            return len(formatted_transactions)
            
        except Exception as e:
            print(f"        ❌ Erreur stockage transactions {token_symbol}: {e}")
            if conn:
                conn.rollback()
                conn.close()
            return 0
    
    return 0

def run_optimized_transaction_tracking(min_usd=500, hours_lookback=24):
    """Extraction optimisée : transactions SEULEMENT pour wallets avec changements récents"""
    
    session_id = str(uuid.uuid4())[:8]
    
    print(f"\n🚀 === TRACKING TRANSACTIONS OPTIMISÉ - Session {session_id} ===")
    print(f"🎯 STRATÉGIE: Récupérer transactions SEULEMENT pour wallets avec changements")
    print(f"⏰ Période analysée: {hours_lookback}h dernières")
    print(f"💰 Seuil minimum: ${min_usd}")
    print(f"🗄️ Base de données: {DB_PATH}")
    
    # 1. Récupérer les wallets avec changements récents depuis la BDD
    wallets_with_changes = get_wallets_with_recent_changes(hours_lookback)
    
    if not wallets_with_changes:
        print("⚠️ Aucun wallet avec changements récents détecté")
        return
    
    print(f"🎯 {len(wallets_with_changes)} wallets avec changements à traiter")
    
    total_new_transactions = 0
    total_api_calls_saved = 0
    
    for i, wallet_address in enumerate(wallets_with_changes, 1):
        print(f"\n=== [{i}/{len(wallets_with_changes)}] {wallet_address} ===")
        
        # 2. Plus besoin de snapshot - on utilise directement les changements détectés
        
        # 3. Récupérer les changements récents pour ce wallet
        conn = sqlite3.connect(DB_PATH)
        changes_query = """
            SELECT wpc.symbol, wpc.change_type, wpc.amount_change, wpc.detected_at
            FROM wallet_position_changes wpc
            WHERE wpc.wallet_address = ?
            AND wpc.detected_at >= datetime('now', '-{} hours')
            ORDER BY wpc.detected_at DESC
        """.format(hours_lookback)
        
        changes_df = pd.read_sql_query(changes_query, conn, params=[wallet_address])
        conn.close()
        
        if changes_df.empty:
            print(f"    ⚠️ Aucun changement récent trouvé, skip")
            continue
        
        print(f"    🔄 {len(changes_df)} changements récents détectés:")
        for _, change in changes_df.head(3).iterrows():
            print(f"        • {change['symbol']}: {change['change_type']} ({change['detected_at'][:16]})")
        
        # 4. Récupérer les tokens affectés par les changements
        tokens_with_changes = changes_df['symbol'].unique().tolist()
        print(f"    🎯 Tokens avec changements: {', '.join(tokens_with_changes)}")
        
        # 5. Récupérer les infos des tokens depuis wallet_position_changes (plus fiable pour nouveaux tokens)
        tokens_to_track = []
        if tokens_with_changes:
            conn_temp = sqlite3.connect(DB_PATH)
            placeholders = ','.join(['?' for _ in tokens_with_changes])
            query = f"""
                SELECT DISTINCT symbol as token, contract_address, fungible_id
                FROM wallet_position_changes 
                WHERE wallet_address = ? AND symbol IN ({placeholders})
                AND fungible_id IS NOT NULL AND fungible_id != ''
                ORDER BY detected_at DESC
            """
            params = [wallet_address] + tokens_with_changes
            df = pd.read_sql_query(query, conn_temp, params=params)
            conn_temp.close()
            
            tokens_to_track = df.to_dict('records')
        
        print(f"    💰 {len(tokens_to_track)} tokens avec fungible_id valide depuis wallet_position_changes")
        
        if tokens_to_track:
            print(f"    📊 Remplacement historique complet pour {len(tokens_to_track)} tokens...")
            
            # 6. Remplacer complètement l'historique des tokens modifiés
            tx_replaced = replace_complete_token_history(wallet_address, session_id, tokens_to_track)
            
            if tx_replaced > 0:
                print(f"    ✅ {tx_replaced} transactions remplacées dans transaction_history")
                total_new_transactions += tx_replaced
            else:
                print(f"    ⚠️ Aucune transaction récupérée pour les tokens modifiés")
        
        # 7. Historique complet remplacé - pas besoin de nouvelles transactions générales
        print(f"    ✅ Historique complet traité pour les tokens modifiés")
        
        # Délai pour respecter rate limiting
        time.sleep(2)
    
    # Résumé final
    print(f"\n🎉 === TRACKING TRANSACTIONS TERMINÉ ===")
    print(f"🎯 Wallets traités: {len(wallets_with_changes)}")
    print(f"🆕 Nouvelles transactions: {total_new_transactions}")
    
    print(f"🗄️ Session: {session_id}")
    print(f"✅ Données mises à jour dans la table transaction_history")
    
    # Nettoyage final: vider complètement la table wallet_position_changes
    print(f"\n🧹 === NETTOYAGE FINAL ===")
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()
        
        # Supprimer complètement le contenu de la table
        cursor.execute("DELETE FROM wallet_position_changes")
        
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        
        print(f"🗑️ Table wallet_position_changes complètement vidée ({deleted_count} entrées supprimées)")
        
    except Exception as e:
        print(f"⚠️ Erreur nettoyage final: {e}")
        if conn:
            conn.rollback()
            conn.close()
    
    return True

# === Lancement direct ===
if __name__ == "__main__":
    run_optimized_transaction_tracking()