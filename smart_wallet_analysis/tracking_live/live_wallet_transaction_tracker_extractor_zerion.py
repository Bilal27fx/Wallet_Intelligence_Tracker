import os
import time
import requests
import pandas as pd
import sqlite3
import uuid
from datetime import datetime
from dotenv import load_dotenv

from smart_wallet_analysis.config import DB_PATH, ENV_PATH, TRACKING_LIVE
from smart_wallet_analysis.logger import get_logger

load_dotenv(dotenv_path=ENV_PATH)

logger = get_logger("tracking_live.transactions")

_TL = TRACKING_LIVE

API_KEY_1 = os.getenv("ZERION_API_KEY")
API_KEY_2 = os.getenv("ZERION_API_KEY_2")

if not API_KEY_1:
    raise ValueError("Clé API principale manquante (ZERION_API_KEY)")
if not API_KEY_2:
    raise ValueError("Clé API secondaire manquante (ZERION_API_KEY_2)")

API_KEYS = [API_KEY_1, API_KEY_2]
api_key_index = 0

TRADE_OPS = {'trade', 'swap', 'execute', 'contract_interaction'}


def get_current_api_key():
    """Retourne la clé API active."""
    return API_KEYS[api_key_index]


def rotate_api_key():
    """Bascule vers la clé API suivante."""
    global api_key_index
    api_key_index = (api_key_index + 1) % len(API_KEYS)
    logger.info(f"Rotation vers clé API {api_key_index + 1}")
    return API_KEYS[api_key_index]


def get_wallets_with_recent_changes(hours=24):
    """Récupère les wallets ayant des changements récents."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            df = pd.read_sql_query("""
                SELECT DISTINCT wpc.wallet_address, COUNT(*) as change_count
                FROM wallet_position_changes wpc
                WHERE wpc.detected_at >= datetime('now', '-{} hours')
                GROUP BY wpc.wallet_address
                ORDER BY change_count DESC
            """.format(hours), conn)
        wallets = df['wallet_address'].tolist()
        logger.info(f"{len(wallets)} wallets avec changements ({hours}h) — {df['change_count'].sum() if not df.empty else 0} changements")
        return wallets
    except Exception as e:
        logger.error(f"Erreur récupération wallets: {e}")
        return []


def _get_known_hashes(wallet_address, fungible_id):
    """Retourne les hashes déjà stockés pour un wallet+token."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT hash FROM transaction_history WHERE wallet_address = ? AND fungible_id = ?",
                (wallet_address, fungible_id)
            )
            return {row[0] for row in cursor.fetchall()}
    except Exception:
        return set()


def get_token_transaction_history_zerion_full(wallet_address, fungible_id, retries=None):
    """Récupère l'historique complet Zerion d'un token."""
    retries = _TL["TX_RETRIES"] if retries is None else retries
    known_hashes = _get_known_hashes(wallet_address, fungible_id)
    headers = {"accept": "application/json", "authorization": f"Basic {get_current_api_key()}"}
    all_transactions, seen_hashes = [], set()
    page_cursor = None

    while True:
        url = f"https://api.zerion.io/v1/wallets/{wallet_address}/transactions/?filter[fungible_ids]={fungible_id}&currency=usd&page[size]={_TL['TX_PAGE_SIZE']}"
        if page_cursor:
            url += f"&page[after]={page_cursor}"

        for attempt in range(retries):
            try:
                response = requests.get(url, headers=headers, timeout=_TL["TX_HTTP_TIMEOUT_SECONDS"])
                response.raise_for_status()
                data = response.json()
                transactions = data.get("data", [])

                if not transactions:
                    return all_transactions

                new_txs = [tx for tx in transactions if tx.get("attributes", {}).get("hash", "") not in seen_hashes]
                if not new_txs:
                    return all_transactions

                if known_hashes:
                    truly_new = [tx for tx in new_txs if tx.get("attributes", {}).get("hash", "") not in known_hashes]
                    if len(truly_new) < len(new_txs):
                        all_transactions.extend(truly_new)
                        return all_transactions
                    new_txs = truly_new

                seen_hashes.update(tx.get("attributes", {}).get("hash", "") for tx in new_txs)
                all_transactions.extend(new_txs)

                next_url = data.get("links", {}).get("next")
                if not next_url:
                    return all_transactions

                if "page%5Bafter%5D=" in next_url:
                    page_cursor = next_url.split("page%5Bafter%5D=")[1].split("&")[0]
                elif "page[after]=" in next_url:
                    page_cursor = next_url.split("page[after]=")[1].split("&")[0]
                else:
                    return all_transactions

                time.sleep(_TL["TX_PAGE_DELAY_SECONDS"])
                break

            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(_TL["TX_RETRY_DELAY_SECONDS"])
                else:
                    if "429" in str(e) or "rate limit" in str(e).lower():
                        rotate_api_key()
                        time.sleep(_TL["TX_RATE_LIMIT_SLEEP_SECONDS"])
                        return get_token_transaction_history_zerion_full(wallet_address, fungible_id, retries)
                    logger.error(f"Erreur pagination: {e}")
                    return []


def _parse_token_transactions(raw_transactions, fungible_id, token_symbol):
    """Parse les transactions Zerion vers le format DB."""
    formatted = []
    for tx in raw_transactions:
        attrs = tx.get("attributes", {})
        operation_type = attrs.get("operation_type", "")
        qty_in, qty_out, val_in, val_out = 0, 0, 0, 0
        recipient_address = sender_address = None

        for transfer in attrs.get("transfers", []):
            finfo = transfer.get("fungible_info", {})
            direction = transfer.get("direction", "")
            if direction == "self" or finfo.get("id", "") != fungible_id:
                continue

            qty_data = transfer.get("quantity", {})
            amount = float(qty_data.get("numeric", 0) if isinstance(qty_data, dict) else 0)
            val = float(transfer.get("value", 0) or 0)

            if direction == "out":
                qty_out += amount
                val_out += val
                recipient_address = transfer.get("recipient")
            elif direction == "in":
                qty_in += amount
                val_in += val
                sender_address = transfer.get("sender")

        if qty_in > 0 and qty_out == 0:
            action_type = "buy" if operation_type in TRADE_OPS else "receive"
            quantity = qty_in
            desc = f"{'Achat' if action_type == 'buy' else 'Réception'}: +{qty_in:.6f} {token_symbol}"
        elif qty_out > 0 and qty_in == 0:
            action_type = "sell" if operation_type in TRADE_OPS else "send"
            quantity = -qty_out
            desc = f"{'Vente' if action_type == 'sell' else 'Envoi'}: -{qty_out:.6f} {token_symbol}"
        elif qty_in > 0 and qty_out > 0:
            net = qty_in - qty_out
            action_type = "buy" if net > 0 else "sell"
            quantity = net
            desc = f"{'Achat' if net > 0 else 'Vente'} net: {net:+.6f} {token_symbol}"
        else:
            continue

        total_value = val_in + val_out
        if operation_type == "trade" and val_in > 0 and val_out > 0:
            ratio = min(val_in, val_out) / max(val_in, val_out)
            if ratio >= _TL["TX_SWAP_RATIO_THRESHOLD"]:
                total_value /= 2

        formatted.append({
            "hash": attrs.get("hash", ""),
            "date": attrs.get("mined_at", ""),
            "operation_type": operation_type,
            "action_type": action_type,
            "swap_description": desc,
            "quantity": quantity,
            "price_per_token": total_value / abs(quantity) if quantity != 0 else 0,
            "total_value_usd": total_value,
            "direction": "in" if quantity > 0 else "out",
            "recipient_address": recipient_address,
            "sender_address": sender_address
        })
    return formatted


def analyze_and_store_complete_transactions(session_id, wallet_address, token_symbol, fungible_id,
                                            contract_address, raw_transactions):
    """Formate et stocke toutes les transactions d'un token."""
    if not raw_transactions:
        return 0

    formatted = _parse_token_transactions(raw_transactions, fungible_id, token_symbol)
    if not formatted:
        return 0

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()
        for tx in formatted:
            cursor.execute("""
                INSERT OR IGNORE INTO transaction_history (
                    wallet_address, fungible_id, symbol, date, hash,
                    operation_type, action_type, swap_description, contract_address,
                    quantity, price_per_token, total_value_usd, direction,
                    recipient_address, sender_address
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                wallet_address, fungible_id, token_symbol, tx['date'], tx['hash'],
                tx['operation_type'], tx['action_type'], tx['swap_description'],
                contract_address, tx['quantity'], tx['price_per_token'], tx['total_value_usd'],
                tx['direction'], tx.get('recipient_address'), tx.get('sender_address')
            ))
        conn.commit()
        return len(formatted)
    except Exception as e:
        logger.error(f"Erreur stockage {token_symbol}: {e}")
        if conn:
            conn.rollback()
        return 0
    finally:
        if conn:
            conn.close()


def clean_processed_change(wallet_address, token_symbol):
    """Supprime les changements traités de wallet_position_changes."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()
        cursor.execute("DELETE FROM wallet_position_changes WHERE wallet_address = ? AND symbol = ?",
                       (wallet_address, token_symbol))
        conn.commit()
    except Exception as e:
        logger.warning(f"Erreur nettoyage {token_symbol}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def replace_complete_token_history(wallet_address, session_id, tokens_to_track):
    """Ajoute les nouvelles transactions pour chaque token."""
    total = 0
    for token_data in tokens_to_track:
        symbol = token_data['token']
        fungible_id = token_data.get('fungible_id', '')
        if not fungible_id:
            continue

        raw = get_token_transaction_history_zerion_full(wallet_address, fungible_id)
        if raw:
            count = analyze_and_store_complete_transactions(
                session_id, wallet_address, symbol, fungible_id,
                token_data.get('contract_address', ''), raw
            )
            total += count
            logger.info(f"{symbol}: {count} transactions ajoutées")
        else:
            logger.warning(f"{symbol}: aucune transaction récupérée")

        clean_processed_change(wallet_address, symbol)
        time.sleep(_TL["TX_TOKEN_DELAY_SECONDS"])

    logger.info(f"{total} transactions au total pour {wallet_address[:12]}...")
    return total


def run_optimized_transaction_tracking(min_usd=500, hours_lookback=24):
    """Mise à jour des transactions pour wallets avec changements récents."""
    session_id = str(uuid.uuid4())[:8]
    logger.info(f"TRACKING TRANSACTIONS — Session {session_id} ({hours_lookback}h, min ${min_usd})")

    wallets_with_changes = get_wallets_with_recent_changes(hours_lookback)
    if not wallets_with_changes:
        logger.warning("Aucun wallet avec changements récents")
        return True

    total_new_transactions = 0

    for i, wallet_address in enumerate(wallets_with_changes, 1):
        logger.info(f"[{i}/{len(wallets_with_changes)}] {wallet_address[:12]}...")

        with sqlite3.connect(DB_PATH) as conn:
            changes_df = pd.read_sql_query("""
                SELECT DISTINCT symbol as token, contract_address, fungible_id
                FROM wallet_position_changes
                WHERE wallet_address = ?
                AND detected_at >= datetime('now', '-{} hours')
                AND fungible_id IS NOT NULL AND fungible_id != ''
                ORDER BY detected_at DESC
            """.format(hours_lookback), conn, params=[wallet_address])

        if changes_df.empty:
            logger.warning(f"Aucun changement trouvé pour {wallet_address[:12]}..., skip")
            continue

        tokens_to_track = changes_df.to_dict('records')
        logger.info(f"{len(tokens_to_track)} tokens à traiter: {', '.join(t['token'] for t in tokens_to_track)}")

        tx_count = replace_complete_token_history(wallet_address, session_id, tokens_to_track)
        total_new_transactions += tx_count
        time.sleep(_TL["TX_WALLET_DELAY_SECONDS"])

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()
        cursor.execute("DELETE FROM wallet_position_changes")
        deleted = cursor.rowcount
        conn.commit()
        logger.info(f"wallet_position_changes vidée ({deleted} entrées)")
    except Exception as e:
        logger.warning(f"Erreur nettoyage final: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

    logger.info(f"{total_new_transactions} transactions ajoutées pour {len(wallets_with_changes)} wallets")
    return True


if __name__ == "__main__":
    run_optimized_transaction_tracking()
