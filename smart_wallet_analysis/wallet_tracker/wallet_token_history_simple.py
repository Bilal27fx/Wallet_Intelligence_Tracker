import os
import time
import requests
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict
from collections import defaultdict

from smart_wallet_analysis.config import DB_PATH, WALLET_TRACKER, ENV_PATH
from smart_wallet_analysis.logger import get_logger

load_dotenv(dotenv_path=ENV_PATH)

logger = get_logger("wallet_tracker.history")

_WT = WALLET_TRACKER


class APIKeyManager:
    """Gestion et rotation des clés API Zerion"""
    def __init__(self):
        self.keys = [k for k in [os.getenv("ZERION_API_KEY"), os.getenv("ZERION_API_KEY_2")] if k]
        if not self.keys:
            raise ValueError("Aucune clé API Zerion trouvée dans .env")
        self.current_index = 0
        self.current_key = self.keys[self.current_index]

    def get_key(self):
        return self.current_key

    def rotate_key(self):
        if len(self.keys) <= 1:
            return False
        self.current_index = (self.current_index + 1) % len(self.keys)
        self.current_key = self.keys[self.current_index]
        logger.info(f"Rotation vers clé API #{self.current_index + 1}")
        return True


api_manager = APIKeyManager()


class SimpleWalletHistoryExtractor:
    """Extraction et sauvegarde de l'historique complet par token depuis Zerion"""

    def __init__(self):
        self.headers = {"accept": "application/json", "authorization": f"Basic {api_manager.get_key()}"}
        self.db_path = DB_PATH

    def _update_headers(self):
        self.headers["authorization"] = f"Basic {api_manager.get_key()}"

    def _handle_rate_limit(self, retry_fn=None):
        if api_manager.rotate_key():
            self._update_headers()
            time.sleep(_WT["RATE_LIMIT_SLEEP_SECONDS"])
            return True
        return False

    def get_wallets_to_process(self) -> List[str]:
        """Wallets actifs non encore extraits depuis la table wallets"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT wallet_address FROM wallets WHERE is_active = 1 AND transactions_extracted = 0")
                wallets = [row[0] for row in cursor.fetchall()]
                logger.info(f"{len(wallets)} wallets à traiter")
                return wallets
        except sqlite3.Error as e:
            logger.error(f"Erreur lecture DB: {e}")
            return []

    def get_existing_tokens(self, wallet_address: str) -> Dict[str, bool]:
        """Tokens déjà en base pour ce wallet"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT fungible_id, in_portfolio FROM tokens WHERE wallet_address = ?", (wallet_address,))
                return {row[0]: bool(row[1]) for row in cursor.fetchall()}
        except sqlite3.Error as e:
            logger.error(f"Erreur lecture tokens: {e}")
            return {}

    def save_token_to_db(self, wallet_address: str, token_info: Dict):
        """Sauvegarde un token dans la table tokens"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO tokens (wallet_address, fungible_id, symbol, contract_address, chain, in_portfolio, created_at, updated_at)
                    VALUES (?, ?, ?, ?, 'ethereum', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, (wallet_address, token_info['fungible_id'], token_info['symbol'], token_info['contract_address']))
        except sqlite3.Error as e:
            logger.error(f"Erreur sauvegarde token: {e}")

    def save_transaction_to_db(self, wallet_address: str, token_info: Dict, transaction: Dict):
        """Sauvegarde une transaction dans transaction_history"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                try:
                    tx_date = datetime.fromisoformat(transaction['date'].replace('Z', '+00:00'))
                except Exception:
                    tx_date = datetime.now()
                conn.execute("""
                    INSERT OR IGNORE INTO transaction_history (
                        wallet_address, fungible_id, symbol, date, hash,
                        operation_type, action_type, swap_description, contract_address,
                        quantity, price_per_token, total_value_usd, direction
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)
                """, (
                    wallet_address, token_info['fungible_id'], token_info['symbol'], tx_date,
                    transaction['transaction_hash'], transaction['operation_type'], transaction['action_type'],
                    token_info['contract_address'], transaction['quantity'],
                    transaction['price_per_token'], transaction['value_usd'], transaction.get('direction', '')
                ))
        except sqlite3.Error as e:
            logger.error(f"Erreur sauvegarde transaction: {e}")

    def mark_wallet_processed(self, wallet_address: str):
        """Marque le wallet comme traité"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("UPDATE wallets SET transactions_extracted = 1, last_sync = CURRENT_TIMESTAMP WHERE wallet_address = ?", (wallet_address,))
        except sqlite3.Error as e:
            logger.error(f"Erreur mise à jour wallet: {e}")

    def get_complete_transaction_history(self, wallet_address: str, max_pages: int = 2000) -> List[Dict]:
        """Récupère tout l'historique des transactions d'un wallet via Zerion"""
        try:
            resp = requests.get(
                f"https://api.zerion.io/v1/wallets/{wallet_address}/positions/?filter[positions]=only_simple&currency=usd&filter[trash]=only_non_trash",
                headers=self.headers, timeout=_WT["HTTP_TIMEOUT_SECONDS"]
            )
            if resp.status_code == 429 and self._handle_rate_limit():
                resp = requests.get(resp.url, headers=self.headers, timeout=_WT["HTTP_TIMEOUT_SECONDS"])
            if resp.status_code == 200 and len(resp.json().get("data", [])) > _WT["MAX_PORTFOLIO_TOKENS"]:
                logger.info(f">{_WT['MAX_PORTFOLIO_TOKENS']} tokens - bot/airdrop farmer, skip")
                self.mark_wallet_processed(wallet_address)
                return None
        except Exception:
            pass

        base_url = f"https://api.zerion.io/v1/wallets/{wallet_address}/transactions/?filter%5Btrash%5D=only_non_trash&currency=usd&page%5Bsize%5D=100"
        all_transactions, seen_hashes = [], set()
        page_cursor, page_count = None, 0

        while page_count < max_pages:
            url = base_url + (f"&page[after]={page_cursor}" if page_cursor else "")
            try:
                response = requests.get(url, headers=self.headers, timeout=_WT["HTTP_TIMEOUT_SECONDS"])

                if response.status_code == 429:
                    if self._handle_rate_limit():
                        continue
                    return None
                if response.status_code == 400:
                    if "Malformed parameter" in response.text:
                        self.mark_wallet_processed(wallet_address)
                    return None
                if response.status_code != 200:
                    return None

                data = response.json()
                transactions = data.get("data", [])
                if not transactions:
                    break

                for tx in transactions:
                    tx_hash = tx.get("attributes", {}).get("hash", "")
                    if tx_hash in seen_hashes:
                        continue
                    seen_hashes.add(tx_hash)
                    if len(all_transactions) >= _WT["MAX_TRANSACTIONS"]:
                        logger.info(f"Limite {_WT['MAX_TRANSACTIONS']} txs - bot/exchange, skip")
                        self.mark_wallet_processed(wallet_address)
                        return None
                    all_transactions.append(tx)

                next_url = data.get("links", {}).get("next")
                if not next_url:
                    break
                if "page%5Bafter%5D=" in next_url:
                    page_cursor = next_url.split("page%5Bafter%5D=")[1].split("&")[0]
                elif "page[after]=" in next_url:
                    page_cursor = next_url.split("page[after]=")[1].split("&")[0]
                else:
                    break

                page_count += 1
                time.sleep(_WT["PAGE_DELAY_SECONDS"])

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 500:
                    logger.error("Erreur 500 Zerion - aucune sauvegarde")
                    return None
                time.sleep(_WT["RATE_LIMIT_SLEEP_SECONDS"])
                break
            except Exception as e:
                logger.error(f"API Error: {str(e)[:50]}")
                time.sleep(_WT["RATE_LIMIT_SLEEP_SECONDS"])
                break

        logger.info(f"{len(all_transactions)} transactions sur {page_count + 1} pages")
        return all_transactions

    def extract_token_histories(self, wallet_address: str, transactions: List[Dict]):
        """Extrait, filtre par volume et sauvegarde l'historique par token"""
        existing_tokens = self.get_existing_tokens(wallet_address)
        token_histories = defaultdict(list)
        token_info = {}

        for tx in transactions:
            attrs = tx.get('attributes', {})
            operation_type = attrs.get('operation_type', '')
            tx_date = attrs.get('mined_at', '')
            tx_hash = attrs.get('hash', '')

            transfers_by_token = {}
            for transfer in attrs.get('transfers', []):
                finfo = transfer.get('fungible_info', {})
                fid = finfo.get('id', '')
                if fid:
                    transfers_by_token.setdefault(fid, []).append(transfer)

            for fungible_id, token_transfers in transfers_by_token.items():
                total_qty, total_val, direction, main = 0, 0, None, None
                for t in token_transfers:
                    total_qty += float(t.get('quantity', {}).get('numeric', 0) or 0)
                    total_val += float(t.get('value', 0) or 0)
                    if main is None:
                        main = t
                        direction = t.get('direction', '')

                if not main or total_qty <= 0 or direction == 'self':
                    continue

                finfo = main.get('fungible_info', {})
                symbol = finfo.get('symbol', '').upper()
                name = finfo.get('name', '')

                if not symbol or not fungible_id:
                    continue
                if len(symbol) > 20 or not symbol.isalnum():
                    continue
                if any(k in name.lower() for k in _WT["TRASH_NAMES"]):
                    continue
                if any(k in symbol.lower() for k in _WT["TRASH_SYMBOLS"]):
                    continue

                impls = finfo.get('implementations', [])
                contract = impls[0].get('address', '') if impls else ''

                token_info.setdefault(fungible_id, {'symbol': symbol, 'name': name, 'contract_address': contract, 'fungible_id': fungible_id})

                if direction == 'in':
                    action_type = 'buy' if operation_type in ['trade', 'swap', 'execute', 'contract_interaction'] else 'receive'
                elif direction == 'out':
                    action_type = 'sell' if operation_type in ['trade', 'swap', 'execute', 'contract_interaction'] else 'send'
                else:
                    if operation_type in ['approve', 'revoke', 'deploy']:
                        continue
                    action_type = 'receive' if operation_type in ['receive', 'mint', 'claim'] else 'send'

                token_histories[fungible_id].append({
                    'transaction_hash': tx_hash,
                    'date': tx_date,
                    'direction': direction,
                    'action_type': action_type,
                    'quantity': total_qty,
                    'price_per_token': total_val / total_qty,
                    'value_usd': total_val,
                    'operation_type': operation_type
                })

        for fid in token_histories:
            token_histories[fid].sort(key=lambda x: x['date'])

        filtered, kept, rejected, saved = {}, 0, 0, 0
        for fid, txs in token_histories.items():
            total_vol = sum(t['value_usd'] for t in txs)
            has_out = any(t['direction'] == 'out' for t in txs)
            if total_vol >= _WT["MIN_TOKEN_VOLUME_USD"] or has_out:
                filtered[fid] = txs
                kept += 1
                if fid not in existing_tokens:
                    self.save_token_to_db(wallet_address, token_info[fid])
                for t in txs:
                    self.save_transaction_to_db(wallet_address, token_info[fid], t)
                    saved += 1
            else:
                rejected += 1

        logger.info(f"{kept} tokens gardés, {rejected} rejetés, {saved} txs sauvegardées")
        return filtered, {k: v for k, v in token_info.items() if k in filtered}


def extract_wallet_simple_history(wallet_address: str, min_value_usd: float = None):
    """Extraction complète de l'historique d'un wallet et sauvegarde en base"""
    extractor = SimpleWalletHistoryExtractor()
    transactions = extractor.get_complete_transaction_history(wallet_address)
    if not transactions:
        logger.warning(f"Aucune transaction récupérée pour {wallet_address[:12]}...")
        return None
    token_histories, _ = extractor.extract_token_histories(wallet_address, transactions)
    if not token_histories:
        logger.warning(f"Aucun token valide trouvé pour {wallet_address[:12]}...")
        return None
    extractor.mark_wallet_processed(wallet_address)
    logger.info(f"{len(token_histories)} tokens sauvegardés pour {wallet_address[:12]}...")
    return True


def process_all_wallets_from_db(wallet_list: List[str] = None, min_value_usd: float = None, batch_size: int = 10, batch_delay: int = 30):
    """Traite une liste de wallets par batches. Si wallet_list=None, récupère depuis la table wallets."""
    extractor = SimpleWalletHistoryExtractor()
    wallets = wallet_list if wallet_list is not None else extractor.get_wallets_to_process()

    if not wallets:
        logger.info("Aucun wallet à traiter")
        return

    logger.info(f"{len(wallets)} wallets (batches={batch_size}, délai={batch_delay}s)")
    total_ok, total_fail = 0, 0

    for batch_id, i in enumerate(range(0, len(wallets), batch_size), 1):
        batch = wallets[i:i + batch_size]
        logger.info(f"Batch {batch_id}: {len(batch)} wallets")
        for idx, wallet in enumerate(batch, 1):
            try:
                logger.info(f"[{idx}/{len(batch)}] {wallet[:12]}...")
                if extract_wallet_simple_history(wallet):
                    total_ok += 1
                else:
                    total_fail += 1
                if idx < len(batch):
                    time.sleep(3)
            except Exception as e:
                total_fail += 1
                logger.error(str(e)[:50])

        if i + batch_size < len(wallets):
            time.sleep(batch_delay)

    logger.info(f"{total_ok}/{len(wallets)} réussis, {total_fail} échecs")


if __name__ == "__main__":
    process_all_wallets_from_db(batch_size=10, batch_delay=10)
