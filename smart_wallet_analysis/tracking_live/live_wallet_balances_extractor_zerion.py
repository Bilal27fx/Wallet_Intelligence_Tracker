import os
import time
import requests
import pandas as pd
import sqlite3
import uuid
from datetime import datetime
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from smart_wallet_analysis.config import DB_PATH, ENV_PATH, TRACKING_LIVE
from smart_wallet_analysis.logger import get_logger

load_dotenv(dotenv_path=ENV_PATH)

logger = get_logger("tracking_live.balances")

_TL = TRACKING_LIVE

API_KEY_1 = os.getenv("ZERION_API_KEY")
API_KEY_2 = os.getenv("ZERION_API_KEY_2")

if not API_KEY_1:
    raise ValueError("Clé API principale manquante (ZERION_API_KEY)")
if not API_KEY_2:
    raise ValueError("Clé API secondaire manquante (ZERION_API_KEY_2)")

API_KEYS = [API_KEY_1, API_KEY_2]
api_key_index = 0


def get_current_api_key():
    """Retourne la clé API active."""
    return API_KEYS[api_key_index]


def rotate_api_key():
    """Bascule vers la clé API suivante."""
    global api_key_index
    api_key_index = (api_key_index + 1) % len(API_KEYS)
    logger.info(f"Rotation vers clé API {api_key_index + 1}")
    return API_KEYS[api_key_index]


def get_smart_wallets_from_db():
    """Récupère les smart wallets actifs."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            df = pd.read_sql_query("""
                SELECT wallet_address FROM smart_wallets
                WHERE optimal_threshold_tier > 0
                ORDER BY optimal_threshold_tier DESC
                LIMIT ?
            """, conn, params=[_TL["SMART_WALLETS_LIMIT"]])
        wallets = df['wallet_address'].tolist()
        logger.info(f"{len(wallets)} smart wallets chargés")
        return wallets
    except Exception as e:
        logger.error(f"Erreur récupération wallets: {e}")
        return []


def create_http_session():
    """Crée une session HTTP avec retry."""
    session = requests.Session()
    retry = Retry(
        total=_TL["HTTP_RETRY_TOTAL"],
        backoff_factor=_TL["HTTP_RETRY_BACKOFF"],
        status_forcelist=_TL["HTTP_RETRY_STATUS"],
        allowed_methods=_TL["HTTP_RETRY_METHODS"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_fungible_id_zerion(contract_address, chain, token_symbol="", session=None):
    """Récupère le fungible_id d'un token via Zerion."""
    if token_symbol.upper() == "ETH" and not contract_address:
        return "eth"
    if not contract_address or not chain:
        return ""

    if not session:
        session = create_http_session()

    headers = {"accept": "application/json", "authorization": f"Basic {get_current_api_key()}"}
    url = f"https://api.zerion.io/v1/fungibles/?filter[implementation_address]={contract_address.lower()}&filter[implementation_chain_id]={chain}"

    try:
        response = session.get(url, headers=headers, timeout=_TL["FUNGIBLE_TIMEOUT_SECONDS"])
        response.raise_for_status()
        fungibles = response.json().get("data", [])
        return fungibles[0].get("id", "") if fungibles else ""
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout fungible_id {contract_address}")
        return ""
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            rotate_api_key()
            time.sleep(_TL["API_KEY_ROTATION_SLEEP_SECONDS"])
        return ""
    except Exception as e:
        if "429" in str(e) or "rate limit" in str(e).lower():
            rotate_api_key()
            time.sleep(_TL["API_KEY_ROTATION_SLEEP_SECONDS"])
            return get_fungible_id_zerion(contract_address, chain, token_symbol, session)
        logger.warning(f"Erreur fungible_id {contract_address}: {e}")
        return ""


def get_token_balances_zerion(address):
    """Récupère les positions d'un wallet via Zerion."""
    session = create_http_session()
    headers = {"accept": "application/json", "authorization": f"Basic {get_current_api_key()}"}
    url = f"https://api.zerion.io/v1/wallets/{address}/positions/?filter[positions]=only_simple&currency=usd&filter[trash]=only_non_trash&sort=value"

    try:
        response = session.get(url, headers=headers, timeout=_TL["HTTP_TIMEOUT_SECONDS"])
        response.raise_for_status()

        filtered_tokens = []
        for pos in response.json().get("data", []):
            attrs = pos.get("attributes", {})
            finfo = attrs.get("fungible_info", {})

            qty = attrs.get("quantity", 0)
            amount = float(qty.get("numeric", 0) if isinstance(qty, dict) else qty or 0)
            val = attrs.get("value", 0)
            usd_value = float(val.get("numeric", 0) if isinstance(val, dict) else val or 0)

            if amount < _TL["MIN_TOKEN_QUANTITY"] or usd_value < _TL["MIN_TOKEN_VALUE_USD"]:
                continue

            impls = finfo.get("implementations", [])
            chain = impls[0].get("chain_id", "") if impls else ""
            contract = impls[0].get("address", "") if impls else ""
            fungible_id = get_fungible_id_zerion(contract, chain, finfo.get("symbol", ""), session)
            time.sleep(_TL["TOKEN_LOOKUP_DELAY"])

            filtered_tokens.append({
                "token": finfo.get("symbol", "UNKNOWN").strip().upper(),
                "amount": amount,
                "usd_value": usd_value,
                "chain": chain,
                "contract_address": contract,
                "contract_decimals": impls[0].get("decimals", "") if impls else "",
                "fungible_id": fungible_id
            })

        return pd.DataFrame(filtered_tokens)

    except requests.exceptions.Timeout:
        logger.warning(f"Timeout Zerion {address}")
        return pd.DataFrame()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            rotate_api_key()
            time.sleep(_TL["RATE_LIMIT_SLEEP_SECONDS"])
        return pd.DataFrame()
    except Exception as e:
        if "429" in str(e) or "rate limit" in str(e).lower():
            rotate_api_key()
            time.sleep(_TL["RATE_LIMIT_SLEEP_SECONDS"])
            return get_token_balances_zerion(address)
        logger.error(f"Erreur Zerion {address}: {e}")
        return pd.DataFrame()


def get_existing_wallet_tokens(wallet_address, filter_smart_wallets=True):
    """Récupère les tokens en portefeuille d'un wallet depuis la DB."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            if filter_smart_wallets:
                cursor.execute("""
                    SELECT t.symbol, t.current_amount, t.current_usd_value, t.contract_address,
                           t.chain, t.fungible_id, t.updated_at
                    FROM tokens t
                    WHERE t.wallet_address = ? AND t.in_portfolio = 1
                    AND EXISTS (SELECT 1 FROM smart_wallets sw WHERE sw.wallet_address = t.wallet_address AND sw.optimal_threshold_tier > 0)
                """, (wallet_address,))
            else:
                cursor.execute("""
                    SELECT t.symbol, t.current_amount, t.current_usd_value, t.contract_address,
                           t.chain, t.fungible_id, t.updated_at
                    FROM tokens t WHERE t.wallet_address = ? AND t.in_portfolio = 1
                """, (wallet_address,))

            return {
                row[0]: {"amount": row[1], "usd_value": row[2], "contract_address": row[3],
                         "chain": row[4], "fungible_id": row[5], "updated_at": row[6]}
                for row in cursor.fetchall()
            }
    except Exception as e:
        logger.warning(f"Erreur lecture DB {wallet_address}: {e}")
        return {}


def update_wallet_tokens_in_db(wallet_address, tokens_data):
    """Met à jour les tokens d'un wallet."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()
        now = datetime.now().isoformat()

        cursor.execute("UPDATE tokens SET in_portfolio = 0, updated_at = ? WHERE wallet_address = ? AND in_portfolio = 1",
                       (now, wallet_address))

        for t in tokens_data:
            cursor.execute("""
                INSERT OR REPLACE INTO tokens (
                    wallet_address, fungible_id, symbol, contract_address, chain,
                    current_amount, current_usd_value, current_price_per_token, updated_at, in_portfolio
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """, (
                wallet_address, t["fungible_id"], t["token"], t["contract_address"], t.get("chain", ""),
                t["amount"], t["usd_value"],
                t["usd_value"] / t["amount"] if t["amount"] > 0 else 0, now
            ))

        total_value = sum(t["usd_value"] for t in tokens_data)
        cursor.execute("""
            UPDATE wallets SET total_portfolio_value = ?, token_count = ?, last_sync = ?, updated_at = ?
            WHERE wallet_address = ?
        """, (total_value, len(tokens_data), now, now, wallet_address))

        if cursor.rowcount == 0:
            cursor.execute("""
                INSERT INTO wallets (wallet_address, total_portfolio_value, token_count, last_sync, created_at, updated_at, is_active)
                VALUES (?, ?, ?, ?, ?, ?, 1)
            """, (wallet_address, total_value, len(tokens_data), now, now, now))

        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Erreur mise à jour DB {wallet_address}: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()



def detect_position_changes_sql(wallet_address, current_tokens_data, session_id):
    """Détecte et enregistre les changements de positions."""
    changes = {"new_tokens": [], "accumulations": [], "reductions": [], "exits": []}
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()
        now = datetime.now().isoformat()

        cursor.execute("""
            SELECT t.symbol, t.current_amount, t.current_usd_value,
                   COALESCE(t.current_price_per_token, 0), t.contract_address, t.fungible_id
            FROM tokens t WHERE t.wallet_address = ? AND t.in_portfolio = 1
            AND EXISTS (SELECT 1 FROM smart_wallets sw WHERE sw.wallet_address = t.wallet_address AND sw.optimal_threshold_tier > 0)
        """, (wallet_address,))

        previous = {row[0]: {"amount": row[1] or 0, "usd_value": row[2] or 0, "price_per_token": row[3] or 0,
                              "contract_address": row[4] or "", "fungible_id": row[5] or ""}
                    for row in cursor.fetchall()}

        current = {t["token"]: t for t in current_tokens_data}
        cur_set, prev_set = set(current), set(previous)

        for symbol in cur_set - prev_set:
            pos = current[symbol]
            cursor.execute("SELECT COUNT(*) FROM tokens WHERE wallet_address = ? AND symbol = ? AND contract_address = ?",
                           (wallet_address, symbol, pos["contract_address"]))
            change_type = "RETOUR" if cursor.fetchone()[0] > 0 else "NEW"
            change = {**pos, "token": symbol, "wallet_address": wallet_address, "change_type": change_type}
            changes["new_tokens"].append(change)
            cursor.execute("""
                INSERT OR IGNORE INTO wallet_position_changes (
                    session_id, wallet_address, symbol, contract_address, change_type,
                    old_amount, new_amount, amount_change, change_percentage,
                    old_usd_value, new_usd_value, usd_change, detected_at, price_per_token, fungible_id
                ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, 100, 0, ?, ?, ?, ?, ?)
            """, (session_id, wallet_address, symbol, pos["contract_address"], change_type,
                  pos["amount"], pos["amount"], pos["usd_value"], pos["usd_value"], now,
                  pos.get("price_per_token", 0), pos["fungible_id"]))

        for symbol in cur_set & prev_set:
            cur, prev = current[symbol], previous[symbol]
            amt_chg = cur["amount"] - prev["amount"]
            usd_chg = cur["usd_value"] - prev["usd_value"]
            if abs(amt_chg) / max(prev["amount"], _TL["MIN_TOKEN_QUANTITY"]) <= _TL["POSITION_CHANGE_MIN_RATIO"] or abs(usd_chg) <= _TL["POSITION_CHANGE_MIN_USD"]:
                continue

            change_type = "ACCUMULATION" if amt_chg > 0 else "REDUCTION"
            change_pct = (amt_chg / prev["amount"]) * 100
            change = {"token": symbol, "old_amount": prev["amount"], "new_amount": cur["amount"],
                      "amount_change": amt_chg, "change_pct": change_pct,
                      "old_usd_value": prev["usd_value"], "new_usd_value": cur["usd_value"],
                      "usd_change": usd_chg, "wallet_address": wallet_address,
                      "change_type": change_type, "contract_address": cur["contract_address"],
                      "fungible_id": cur["fungible_id"]}
            changes["accumulations" if amt_chg > 0 else "reductions"].append(change)
            cursor.execute("""
                INSERT OR IGNORE INTO wallet_position_changes (
                    session_id, wallet_address, symbol, contract_address, change_type,
                    old_amount, new_amount, amount_change, change_percentage,
                    old_usd_value, new_usd_value, usd_change, detected_at, price_per_token, fungible_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (session_id, wallet_address, symbol, cur["contract_address"], change_type,
                  prev["amount"], cur["amount"], amt_chg, change_pct,
                  prev["usd_value"], cur["usd_value"], usd_chg, now,
                  cur.get("price_per_token", 0), cur["fungible_id"]))

        for symbol in prev_set - cur_set:
            prev = previous[symbol]
            change = {"token": symbol, "old_amount": prev["amount"] or 0, "old_usd_value": prev["usd_value"] or 0,
                      "wallet_address": wallet_address, "change_type": "EXIT",
                      "contract_address": prev.get("contract_address", ""), "fungible_id": prev.get("fungible_id", "")}
            changes["exits"].append(change)
            cursor.execute("""
                INSERT OR IGNORE INTO wallet_position_changes (
                    session_id, wallet_address, symbol, contract_address, change_type,
                    old_amount, new_amount, amount_change, change_percentage,
                    old_usd_value, new_usd_value, usd_change, detected_at, price_per_token, fungible_id
                ) VALUES (?, ?, ?, ?, 'EXIT', ?, 0, ?, -100, ?, 0, ?, ?, 0, ?)
            """, (session_id, wallet_address, symbol, prev.get("contract_address", ""),
                  prev["amount"] or 0, -(prev["amount"] or 0),
                  prev["usd_value"] or 0, -(prev["usd_value"] or 0), now, prev.get("fungible_id", "")))

        conn.commit()
    except Exception as e:
        logger.error(f"Erreur SQL détection changements {wallet_address}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

    return changes


def process_wallet_batch_sql(wallets, position_changes_found, session_id):
    """Traite un batch de wallets."""
    for address in wallets:
        logger.info(f"{address[:12]}...")
        df = get_token_balances_zerion(address)
        if df.empty:
            logger.warning(f"Aucun token détecté pour {address[:12]}...")
            continue

        current_tokens_data = df.to_dict('records')
        changes = detect_position_changes_sql(address, current_tokens_data, session_id)

        total = sum(len(v) for v in changes.values())
        if total > 0:
            logger.info(f"{total} changements: +{len(changes['new_tokens'])} new "
                        f"↗{len(changes['accumulations'])} accum ↘{len(changes['reductions'])} red "
                        f"🚪{len(changes['exits'])} exits")
            position_changes_found[address] = changes
        else:
            logger.info(f"Aucun changement pour {address[:12]}...")

        update_wallet_tokens_in_db(address, current_tokens_data)
        logger.info(f"${df['usd_value'].sum():,.0f} | {len(df)} tokens")
        time.sleep(_TL["WALLET_DELAY_SECONDS"])


def run_live_wallet_changes_tracker():
    """Pipeline principal de tracking des changements de positions."""
    session_id = str(uuid.uuid4())[:8]
    logger.info(f"TRACKING LIVE — Session {session_id} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    smart_wallets = get_smart_wallets_from_db()
    position_changes_found = {}
    batch_size = _TL["BATCH_SIZE"]

    for i in range(0, len(smart_wallets), batch_size):
        batch = smart_wallets[i:i + batch_size]
        logger.info(f"Batch {i // batch_size + 1}/{(len(smart_wallets) + batch_size - 1) // batch_size}")
        process_wallet_batch_sql(batch, position_changes_found, session_id)
        if i + batch_size < len(smart_wallets):
            time.sleep(_TL["DELAY_BETWEEN_BATCHES"])

    if position_changes_found:
        total = sum(sum(len(v) for v in c.values()) for c in position_changes_found.values())
        logger.info(f"{total} changements sur {len(position_changes_found)} wallets")
    else:
        logger.info("Aucun changement détecté")

    return True


if __name__ == "__main__":
    run_live_wallet_changes_tracker()
