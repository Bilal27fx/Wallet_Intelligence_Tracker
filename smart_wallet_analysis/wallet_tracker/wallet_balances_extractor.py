import os
import time
import requests
import sqlite3
import pandas as pd
from dotenv import load_dotenv

from db.database_utils import insert_wallet, insert_token, get_wallet
from smart_wallet_analysis.config import DB_PATH, WALLET_BALANCES, ENV_PATH
from smart_wallet_analysis.logger import get_logger

load_dotenv(dotenv_path=ENV_PATH)

logger = get_logger("wallet_tracker.balances")

_WB = WALLET_BALANCES


def _safe_float(value, default=0):
    """Convertit une valeur en float."""
    if value is None:
        return default
    if isinstance(value, dict):
        return float(value.get("numeric", default))
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _wallet_tag(address: str) -> str:
    """Retourne un identifiant court de wallet pour les logs."""
    if not address:
        return "unknown_wallet"
    if len(address) < 16:
        return address
    return f"{address[:10]}...{address[-6:]}"


def _fmt_usd(value: float) -> str:
    """Formate un montant USD pour les logs."""
    return f"${value:,.0f}"


def _log_wallet_line(tag: str, status: str, **fields):
    """Log compact et lisible sur une seule ligne par wallet."""
    ordered = []
    for key in sorted(fields.keys()):
        ordered.append(f"{key}={fields[key]}")
    suffix = " | " + " ".join(ordered) if ordered else ""
    logger.info("[%s] %s%s", tag, status, suffix)


class APIKeyManager:
    """Gestion et rotation des clés API Zerion."""
    def __init__(self):
        self.keys = [k for k in [os.getenv("ZERION_API_KEY"), os.getenv("ZERION_API_KEY_2")] if k]
        if not self.keys:
            raise ValueError("❌ Aucune clé API Zerion trouvée dans .env")
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


def get_wallet_period_mapping():
    """Récupère les wallets depuis la table wallet_brute."""
    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT wallet_address, temporality FROM wallet_brute ORDER BY wallet_address")
            results = cursor.fetchall()
        mapping = {addr: (temp if temp else "manual") for addr, temp in results}
        logger.info(f"{len(mapping)} wallets récupérés depuis wallet_brute")
        return mapping
    except Exception as e:
        logger.error(f"Erreur lecture wallet_brute: {e}")
        return {}


def get_fungible_id_zerion(contract_address, chain, token_symbol=""):
    """Récupère le fungible_id d'un token via l'API Zerion."""
    if token_symbol.upper() == "ETH" and not contract_address:
        return "eth"
    if not contract_address or not chain:
        return ""

    headers = {"accept": "application/json", "authorization": f"Basic {api_manager.get_key()}"}
    url = f"https://api.zerion.io/v1/fungibles/?filter[implementation_address]={contract_address.lower()}&filter[implementation_chain_id]={chain}"

    try:
        response = requests.get(url, headers=headers, timeout=_WB["FUNGIBLE_TIMEOUT_SECONDS"])
        if response.status_code == 429:
            if api_manager.rotate_key():
                time.sleep(_WB["RATE_LIMIT_SLEEP_SECONDS"])
                return get_fungible_id_zerion(contract_address, chain, token_symbol)
            return ""
        response.raise_for_status()
        fungibles = response.json().get("data", [])
        return fungibles[0].get("id", "") if fungibles else ""
    except Exception as e:
        logger.warning(f"Erreur fungible_id {contract_address}: {e}")
        return ""


def get_token_balances_zerion(address):
    """Récupère et filtre les positions d'un wallet via Zerion."""
    headers = {"accept": "application/json", "authorization": f"Basic {api_manager.get_key()}"}
    url = f"https://api.zerion.io/v1/wallets/{address}/positions/?filter[positions]=only_simple&currency=usd&filter[trash]=only_non_trash&sort=value"
    tag = _wallet_tag(address)

    try:
        response = requests.get(url, headers=headers, timeout=_WB["POSITIONS_TIMEOUT_SECONDS"])
        if response.status_code == 429:
            if api_manager.rotate_key():
                time.sleep(_WB["RATE_LIMIT_SLEEP_SECONDS"])
                return get_token_balances_zerion(address)
            return pd.DataFrame(), {"status": "SKIP", "reason": "rate_limit_no_spare_key"}
        response.raise_for_status()

        all_positions = response.json().get("data", [])
        total_value = sum(_safe_float(p.get("attributes", {}).get("value")) for p in all_positions)

        if total_value < _WB["MIN_WALLET_VALUE_USD"]:
            return pd.DataFrame(), {
                "status": "SKIP",
                "reason": "wallet_value_below_min",
                "wallet_value": _fmt_usd(total_value),
                "min_wallet_value": _fmt_usd(_WB["MIN_WALLET_VALUE_USD"]),
            }
        if total_value > _WB["MAX_WALLET_VALUE_USD"]:
            return pd.DataFrame(), {
                "status": "SKIP",
                "reason": "wallet_value_above_max",
                "wallet_value": _fmt_usd(total_value),
                "max_wallet_value": _fmt_usd(_WB["MAX_WALLET_VALUE_USD"]),
            }

        valid_positions, excluded_positions = [], []
        for pos in all_positions:
            attrs = pos.get("attributes", {})
            if _safe_float(attrs.get("value")) >= _WB["MIN_TOKEN_VALUE_USD"]:
                symbol = attrs.get("fungible_info", {}).get("symbol", "").upper()
                (excluded_positions if symbol in _WB["EXCLUDED_TOKENS"] else valid_positions).append(pos)

        if len(valid_positions) < _WB["MIN_TOKENS_PER_WALLET"]:
            return pd.DataFrame(), {
                "status": "SKIP",
                "reason": "valid_tokens_below_min",
                "wallet_value": _fmt_usd(total_value),
                "valid_tokens": len(valid_positions),
                "min_valid_tokens": _WB["MIN_TOKENS_PER_WALLET"],
                "excluded_tokens": len(excluded_positions),
            }

        all_valid = valid_positions + excluded_positions
        if len(all_valid) > _WB["MAX_TOKENS_PER_WALLET"]:
            return pd.DataFrame(), {
                "status": "SKIP",
                "reason": "total_tokens_above_max",
                "wallet_value": _fmt_usd(total_value),
                "total_tokens": len(all_valid),
                "max_tokens": _WB["MAX_TOKENS_PER_WALLET"],
                "excluded_tokens": len(excluded_positions),
            }

        tokens = []
        for pos in all_valid:
            attrs = pos.get("attributes", {})
            finfo = attrs.get("fungible_info", {})
            impls = finfo.get("implementations", [])
            chain = impls[0].get("chain_id", "") if impls else ""
            contract = impls[0].get("address", "") if impls else ""
            fungible_id = get_fungible_id_zerion(contract, chain, finfo.get("symbol", ""))
            time.sleep(_WB["TOKEN_LOOKUP_DELAY"])
            tokens.append({
                "token": finfo.get("symbol", "UNKNOWN").strip().upper(),
                "amount": _safe_float(attrs.get("quantity")),
                "usd_value": _safe_float(attrs.get("value")),
                "chain": chain,
                "contract_address": contract,
                "contract_decimals": impls[0].get("decimals", "") if impls else "",
                "fungible_id": fungible_id
            })

        return pd.DataFrame(tokens), {
            "status": "VALID",
            "wallet_value": _fmt_usd(total_value),
            "valid_tokens": len(valid_positions),
            "excluded_tokens": len(excluded_positions),
            "tokens_to_insert": len(tokens),
        }

    except Exception as e:
        logger.error("Erreur Zerion [%s]: %s", tag, e)
        return pd.DataFrame(), {"status": "ERROR", "reason": "zerion_request_failed"}


def process_wallet_batch(wallets, wallet_to_period):
    """Traite un batch de wallets : filtre, récupère balances et insère en BDD."""
    stats = {
        "processed": 0,
        "inserted": 0,
        "skip_reasons": {},
        "errors": 0,
    }
    for address in wallets:
        stats["processed"] += 1
        tag = _wallet_tag(address)
        periode = wallet_to_period.get(address, "manual")
        if periode not in _WB["PERIODS"]:
            periode = "manual"

        if get_wallet(address):
            reason = "already_in_db"
            stats["skip_reasons"][reason] = stats["skip_reasons"].get(reason, 0) + 1
            _log_wallet_line(tag, "SKIP", period=periode, reason=reason)
            continue

        df, decision = get_token_balances_zerion(address)
        if df.empty:
            reason = decision.get("reason", "empty_result")
            if decision.get("status") == "ERROR":
                stats["errors"] += 1
                _log_wallet_line(tag, "ERROR", period=periode, reason=reason)
            else:
                stats["skip_reasons"][reason] = stats["skip_reasons"].get(reason, 0) + 1
                fields = {k: v for k, v in decision.items() if k not in {"status", "reason"}}
                _log_wallet_line(tag, "SKIP", period=periode, reason=reason, **fields)
            continue

        _log_wallet_line(
            tag,
            "VALID",
            period=periode,
            wallet_value=decision.get("wallet_value", "n/a"),
            valid_tokens=decision.get("valid_tokens", 0),
            excluded_tokens=decision.get("excluded_tokens", 0),
            tokens_to_insert=decision.get("tokens_to_insert", 0),
        )

        total_value = df["usd_value"].sum()
        if not insert_wallet(address, periode, total_value):
            stats["errors"] += 1
            _log_wallet_line(tag, "ERROR", period=periode, reason="wallet_insert_failed")
            continue

        tokens_ok = sum(
            insert_token(
                wallet_address=address,
                fungible_id=row['fungible_id'],
                symbol=row['token'],
                contract_address=row['contract_address'],
                chain=row['chain'],
                amount=row['amount'],
                usd_value=row['usd_value'],
                price=row['usd_value'] / row['amount'] if row['amount'] > 0 else 0
            )
            for _, row in df.iterrows()
        )
        stats["inserted"] += 1
        _log_wallet_line(
            tag,
            "INSERTED",
            period=periode,
            wallet_total=_fmt_usd(total_value),
            tokens_inserted=f"{tokens_ok}/{len(df)}",
        )
    return stats


def run_wallet_balance_pipeline():
    """Pipeline principal : récupère les wallets depuis wallet_brute et insère en BDD."""
    wallet_to_period = get_wallet_period_mapping()
    addresses = list(wallet_to_period.keys())
    logger.info(f"{len(addresses)} wallets à traiter")

    batch_size = _WB["BATCH_SIZE"]
    total_batches = (len(addresses) + batch_size - 1) // batch_size if addresses else 0
    for i in range(0, len(addresses), batch_size):
        batch = addresses[i:i + batch_size]
        batch_idx = i // batch_size + 1
        logger.info(
            "Batch %s/%s | wallets %s-%s/%s",
            batch_idx,
            total_batches,
            i + 1,
            i + len(batch),
            len(addresses),
        )
        batch_stats = process_wallet_batch(batch, wallet_to_period)
        skips = ", ".join(
            f"{reason}:{count}" for reason, count in sorted(batch_stats["skip_reasons"].items())
        ) or "none"
        logger.info(
            "Batch %s summary | processed=%s inserted=%s errors=%s skips={%s}",
            batch_idx,
            batch_stats["processed"],
            batch_stats["inserted"],
            batch_stats["errors"],
            skips,
        )
        if i + batch_size < len(addresses):
            time.sleep(_WB["DELAY_BETWEEN_BATCHES"])

    logger.info("Tous les wallets traités.")


if __name__ == "__main__":
    run_wallet_balance_pipeline()
