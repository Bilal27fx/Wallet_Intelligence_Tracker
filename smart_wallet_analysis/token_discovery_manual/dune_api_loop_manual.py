import os
import json
import yaml
import time
import random
import requests
import pandas as pd
from dotenv import load_dotenv

from smart_wallet_analysis.config import DUNE_YML_PATH, ENV_PATH, TOKEN_DISCOVERY_MANUAL
from smart_wallet_analysis.logger import get_logger
from smart_wallet_analysis.token_discovery_manual.smart_contrat_remover import ContractChecker
from smart_wallet_analysis.token_discovery_manual.wallet_brute_dao import WalletBruteDAO

load_dotenv(dotenv_path=ENV_PATH)

logger = get_logger("token_discovery.manual")

_TDM = TOKEN_DISCOVERY_MANUAL
_DAO = WalletBruteDAO()

_DUNE_KEYS = [k for k in [os.getenv("DUNE_API_KEY"), os.getenv("DUNE_API_KEY_2")] if k]
_key_index = 0


def _next_headers() -> dict:
    """Retourne des headers avec rotation de clé et User-Agent aléatoire."""
    if not _DUNE_KEYS:
        raise RuntimeError("Aucune clé Dune trouvée (DUNE_API_KEY / DUNE_API_KEY_2)")
    global _key_index
    key = _DUNE_KEYS[_key_index % len(_DUNE_KEYS)]
    _key_index += 1
    return {
        "Content-Type": "application/json",
        "X-Dune-API-Key": key,
        "User-Agent": random.choice(_TDM["USER_AGENTS"]),
    }


def execute_dune_query(query_id, parameters):
    """Exécute une requête Dune et retourne un DataFrame."""
    exec_url = f"{_TDM['DUNE_BASE_URL']}/query/{query_id}/execute"
    res = requests.post(exec_url, headers=_next_headers(), json={"query_parameters": parameters})
    if res.status_code != 200:
        raise Exception(f"❌ Lancement échoué : {res.text}")

    execution_id = res.json()["execution_id"]
    logger.info(f"⏳ Execution ID : {execution_id}")

    status_url = f"{_TDM['DUNE_BASE_URL']}/execution/{execution_id}/status"
    result_url = f"{_TDM['DUNE_BASE_URL']}/execution/{execution_id}/results"

    waited = 0
    while waited < _TDM["MAX_WAIT_TIME_SECONDS"]:
        status = requests.get(status_url, headers=_next_headers()).json()
        state = status.get("state")
        logger.info(f"⌛ Status : {state} — {waited}s")

        if state == "QUERY_STATE_COMPLETED":
            break
        if state in ["QUERY_STATE_FAILED", "QUERY_STATE_ERRORED"]:
            raise Exception(f"❌ Erreur de requête : {state}")

        time.sleep(_TDM["SLEEP_INTERVAL_SECONDS"])
        waited += _TDM["SLEEP_INTERVAL_SECONDS"]

    if waited >= _TDM["MAX_WAIT_TIME_SECONDS"]:
        raise TimeoutError("⏰ Timeout dépassé")

    res = requests.get(result_url, headers=_next_headers())
    rows = res.json().get("result", {}).get("rows", [])
    return pd.DataFrame(rows)


def load_dune_config():
    """Charge la configuration Dune depuis le YAML."""
    with open(DUNE_YML_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def convert_period_to_hours(perf_window):
    """Convertit une période texte en nombre d'heures."""
    if perf_window.endswith("h"):
        return int(perf_window[:-1])
    if perf_window.endswith("j"):
        return int(perf_window[:-1]) * 24
    if perf_window.endswith("d"):
        return int(perf_window[:-1]) * 24
    try:
        return int(perf_window)
    except ValueError:
        raise ValueError(f"Format de période non reconnu: {perf_window}")


def load_cache():
    """Charge le cache des tokens déjà traités."""
    if _TDM["CACHE_PATH"].exists():
        try:
            return pd.read_csv(_TDM["CACHE_PATH"])
        except pd.errors.EmptyDataError:
            pass
    return pd.DataFrame(columns=["token_address", "chain", "perf_window"])


def update_cache(df_cache, token_address, chain, perf_window):
    """Ajoute un token au cache local."""
    df_cache.loc[len(df_cache)] = {
        "token_address": token_address,
        "chain": chain,
        "perf_window": perf_window
    }
    _TDM["CACHE_PATH"].parent.mkdir(parents=True, exist_ok=True)
    df_cache.to_csv(_TDM["CACHE_PATH"], index=False)


def filter_eoa_wallets(df):
    """Filtre les wallets pour ne garder que les EOA."""
    try:
        logger.info(f"🔍 Filtrage EOA sur {len(df)} wallets...")
        checker = ContractChecker()
        addresses = df["wallet"].unique().tolist()
        logger.info(f"📊 {len(addresses)} adresses uniques à vérifier")

        eoa_addresses = []
        for i, address in enumerate(addresses, 1):
            logger.info(f"  [{i}/{len(addresses)}] Vérification {address[:10]}...")
            is_contract = checker.is_contract_single(address)

            if is_contract is None:
                logger.warning("    ❌ Erreur API, exclusion par sécurité")
                continue
            if is_contract:
                logger.info("    🏗️ Smart contract détecté, exclusion")
                continue

            logger.info("    👤 EOA confirmé, conservation")
            eoa_addresses.append(address)

            if i < len(addresses):
                time.sleep(_TDM["EOA_CHECK_DELAY_SECONDS"])

        df_filtered = df[df["wallet"].isin(eoa_addresses)]
        logger.info(f"✅ Filtrage terminé: {len(df_filtered)}/{len(df)} wallets conservés (EOA uniquement)")
        return df_filtered

    except Exception as e:
        logger.error(f"❌ Erreur lors du filtrage EOA: {e}")
        logger.warning("⚠️ Conservation de tous les wallets par sécurité")
        return df


def insert_wallets_to_db(df, token_address, token_symbol, chain, temporality):
    """Insère les wallets dans la table wallet_brute après filtrage EOA."""
    try:
        df_eoa = filter_eoa_wallets(df)

        if df_eoa.empty:
            logger.warning("⚠️ Aucun EOA trouvé après filtrage, skip insertion")
            return True

        rows = []
        for _, row in df_eoa.iterrows():
            rows.append(
                {
                    "wallet_address": row["wallet"],
                    "token_address": token_address,
                    "token_symbol": token_symbol,
                    "contract_address": token_address,
                    "chain": chain,
                    "temporality": temporality,
                }
            )

        inserted = _DAO.insert_wallets_batch(rows)
        logger.info("💾 %s wallets EOA insérés dans wallet_brute", inserted)
        return True

    except Exception as e:
        logger.error(f"❌ Erreur insertion BDD: {e}")
        return False


def is_already_processed_db(token_address, chain, perf_window):
    """Vérifie si le token a déjà été traité en base."""
    try:
        return _DAO.token_already_processed(token_address, chain, perf_window)
    except Exception as e:
        logger.error(f"❌ Erreur vérification BDD: {e}")
        return False


def ensure_wallet_brute_table():
    """Crée la table wallet_brute si elle n'existe pas."""
    return _DAO.ensure_table()


def run_manual_token_discovery():
    """Exécute la découverte de tokens à partir du JSON manual."""
    if not _DUNE_KEYS:
        logger.error("❌ Aucune clé Dune configurée. Vérifie DUNE_API_KEY / DUNE_API_KEY_2 dans .env")
        return

    if not _TDM["INPUT_JSON_PATH"].exists():
        logger.error(f"❌ Fichier d'entrée non trouvé: {_TDM['INPUT_JSON_PATH']}")
        return

    if not ensure_wallet_brute_table():
        logger.error("❌ Impossible de créer/vérifier la table wallet_brute")
        return

    dune_config = load_dune_config()
    cache_df = load_cache()
    _TDM["EXPORT_DIR"].mkdir(parents=True, exist_ok=True)

    try:
        with open(_TDM["INPUT_JSON_PATH"], "r", encoding="utf-8") as f:
            tokens = json.load(f)
    except Exception as e:
        logger.error(f"❌ Erreur lecture JSON: {e}")
        return

    logger.info(f"🚀 Traitement de {len(tokens)} tokens explosifs")

    for i, token in enumerate(tokens, 1):
        try:
            token_address = token["token_address"].lower()
            perf_window_str = token["perf_window"]
            chain = token["chain"].lower()
            logger.info(f"\n[{i}/{len(tokens)}] 🎯 Token: {token_address}")
            logger.info(f"📊 Période: {perf_window_str} | Chaîne: {chain}")

        except KeyError as e:
            logger.warning(f"[SKIP] Clé manquante dans token {i}: {e}")
            continue

        if is_already_processed_db(token_address, chain, perf_window_str):
            logger.info(f"⏩ Déjà traité en BDD : {token_address} [{perf_window_str}]")
            continue

        try:
            perf_hours = convert_period_to_hours(perf_window_str)
            token_type = token.get("type", 1)
            extra_hours = _TDM["EARLY_WINDOW_HOURS_BY_TYPE"].get(token_type)

            if extra_hours is None:
                logger.warning(f"⚠️  Type {token_type} non reconnu, utilisation Type 1 par défaut")
                extra_hours = _TDM["EARLY_WINDOW_HOURS_BY_TYPE"].get(1, 24)

            early_hours = perf_hours + extra_hours

            logger.info(f"📅 Type {token_type} temporalité: accumulation {early_hours}h (perf + {extra_hours}h)")

        except ValueError as e:
            logger.warning(f"[SKIP] {e}")
            continue

        chain_key = _TDM["CHAIN_MAPPING"].get(chain)
        if not chain_key:
            logger.warning(f"[SKIP] Chaîne non supportée: {chain}")
            continue

        query_id = dune_config.get(chain_key, {}).get("top_wallet")
        if not query_id:
            logger.warning(f"[SKIP] Aucune query Dune pour {chain}")
            continue

        params = {
            "token_address": token_address,
            "perf_window": perf_hours,
            "early_window": early_hours
        }

        logger.info(f"[📡] Query ID : {query_id}")
        logger.info(f"[🔧] Params : perf={perf_hours}h, early={early_hours}h")

        try:
            df = execute_dune_query(query_id, params)
            if df.empty:
                logger.warning("⚠️ Aucun résultat — skip")
                update_cache(cache_df, token_address, chain, perf_window_str)
                continue

            token_symbol = token.get("symbol", "UNKNOWN")
            success = insert_wallets_to_db(df, token_address, token_symbol, chain, perf_window_str)

            if success:
                logger.info(f"📈 {len(df)} wallets stockés en BDD")

            update_cache(cache_df, token_address, chain, perf_window_str)

        except Exception as e:
            logger.error(f"[❌] Erreur requête : {e}")
            continue

    logger.info("✅ Traitement terminé !")


def _get_tokens_from_db():
    """Récupère les tokens explosifs depuis la DB avec leur fenêtre temporelle."""
    import sqlite3
    from smart_wallet_analysis.config import DB_PATH
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """SELECT token_address, chain, symbol, hours_since_now
               FROM explosive_tokens_detected
               WHERE hours_since_now IS NOT NULL AND (traite IS NULL OR traite = 0)"""
        ).fetchall()
    return [{"token_address": r[0], "chain": r[1], "symbol": r[2], "hours_since_now": r[3]} for r in rows]


def run_discovery_from_db():
    """Découverte automatique depuis explosive_tokens_detected (type 3, perf=hours_since_now)."""
    if not _DUNE_KEYS:
        logger.error("❌ Aucune clé Dune configurée. Vérifie DUNE_API_KEY / DUNE_API_KEY_2 dans .env")
        return

    if not ensure_wallet_brute_table():
        logger.error("Impossible de créer/vérifier la table wallet_brute")
        return

    dune_config = load_dune_config()
    tokens = _get_tokens_from_db()
    cache_df = load_cache()

    if not tokens:
        logger.warning("Aucun token avec hours_since_now dans explosive_tokens_detected")
        return

    extra_hours = _TDM["EARLY_WINDOW_HOURS_BY_TYPE"][3]
    logger.info("Découverte automatique: %s tokens | type 3 (%sh extra)", len(tokens), extra_hours)

    for i, token in enumerate(tokens, 1):
        token_address = token["token_address"].strip().lower()
        chain = token["chain"].lower()
        symbol = token.get("symbol", "UNKNOWN")
        perf_hours = round(token["hours_since_now"])
        early_hours = perf_hours + extra_hours
        perf_window_str = f"{perf_hours}h"

        logger.info("[%s/%s] %s (%s) | perf=%sh | early=%sh", i, len(tokens), symbol, chain, perf_hours, early_hours)

        already_cached = (
            not cache_df.empty
            and ((cache_df["token_address"] == token_address) & (cache_df["chain"] == chain)).any()
        )
        if already_cached:
            logger.info("Déjà dans le cache: %s (%s)", token_address, chain)
            continue

        chain_key = _TDM["CHAIN_MAPPING"].get(chain)
        if not chain_key:
            logger.warning("[SKIP] Chaîne non supportée: %s", chain)
            continue

        query_id = dune_config.get(chain_key, {}).get("top_wallet")
        if not query_id:
            logger.warning("[SKIP] Aucune query Dune pour %s", chain)
            continue

        params = {"token_address": token_address, "perf_window": perf_hours, "early_window": early_hours}

        try:
            df = execute_dune_query(query_id, params)
            if df.empty:
                logger.warning("Aucun résultat Dune pour %s", token_address)
            else:
                insert_wallets_to_db(df, token_address, symbol, chain, perf_window_str)

            import sqlite3 as _sqlite3
            from smart_wallet_analysis.config import DB_PATH as _DB_PATH
            with _sqlite3.connect(_DB_PATH) as conn:
                conn.execute(
                    "UPDATE explosive_tokens_detected SET traite = 1 WHERE token_address = ? AND chain = ?",
                    (token_address, chain),
                )
            update_cache(cache_df, token_address, chain, perf_window_str)
            logger.info("Marqué traité: %s (%s)", symbol, chain)
        except Exception as e:
            logger.error("Erreur Dune pour %s: %s", token_address, e)

    logger.info("Découverte automatique terminée")


if __name__ == "__main__":
    run_manual_token_discovery()
