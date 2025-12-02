import os
import json
import yaml
import time
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# === Chargement des variables d'environnement
load_dotenv()
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_BASE_URL = "https://api.dune.com/api/v1"

# === Constantes
DUNE_YML_PATH = "config/dune.yml"
DATA_DIR = Path("data/raw/json")
EXPORT_DIR = Path("data/raw/csv/top_wallets")
CACHE_PATH = Path("data/cache/early_wallets_extracted.csv")
SUPPORTED_PERIODS = ["14d", "30d", "200d", "360d"]
PERIOD_MAP = {"14d": 14, "30d": 30, "200d": 200, "360d": 360}
HEADERS = {
    "Content-Type": "application/json",
    "X-Dune-API-Key": DUNE_API_KEY
}
MAX_WAIT_TIME = 300
SLEEP_INTERVAL = 5


def execute_dune_query(query_id, parameters):
    exec_url = f"{DUNE_BASE_URL}/query/{query_id}/execute"
    res = requests.post(exec_url, headers=HEADERS, json={"query_parameters": parameters})
    if res.status_code != 200:
        raise Exception(f"❌ Lancement échoué : {res.text}")

    execution_id = res.json()["execution_id"]
    print(f"⏳ Execution ID : {execution_id}")

    status_url = f"{DUNE_BASE_URL}/execution/{execution_id}/status"
    result_url = f"{DUNE_BASE_URL}/execution/{execution_id}/results"

    waited = 0
    while waited < MAX_WAIT_TIME:
        status = requests.get(status_url, headers=HEADERS).json()
        state = status.get("state")
        print(f"⌛ Status : {state} — {waited}s")

        if state == "QUERY_STATE_COMPLETED":
            break
        elif state in ["QUERY_STATE_FAILED", "QUERY_STATE_ERRORED"]:
            raise Exception(f"❌ Erreur de requête : {state}")

        time.sleep(SLEEP_INTERVAL)
        waited += SLEEP_INTERVAL

    if waited >= MAX_WAIT_TIME:
        raise TimeoutError("⏰ Timeout dépassé")

    res = requests.get(result_url, headers=HEADERS)
    rows = res.json().get("result", {}).get("rows", [])
    return pd.DataFrame(rows)


def load_dune_config():
    with open(DUNE_YML_PATH, "r") as f:
        return yaml.safe_load(f)


def parse_temporal_tag(filename):
    for tag in SUPPORTED_PERIODS:
        if f"_{tag}_" in filename:
            return tag
    return None


def load_cache():
    if CACHE_PATH.exists():
        return pd.read_csv(CACHE_PATH)
    else:
        return pd.DataFrame(columns=["contract_address", "platform", "temporal_tag"])


def update_cache(df_cache, contract, platform, temporal_tag):
    df_cache.loc[len(df_cache)] = {
        "contract_address": contract,
        "platform": platform,
        "temporal_tag": temporal_tag
    }
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_cache.to_csv(CACHE_PATH, index=False)


def run_token_discovery():
    dune_config = load_dune_config()
    cache_df = load_cache()
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    for filename in os.listdir(DATA_DIR):
        if not (
            filename.startswith("top_tokens_contracts_")
            and filename.endswith("_evm.json")
            and any(f"_{tag}_" in filename for tag in SUPPORTED_PERIODS)
        ):
            continue

        temporal_tag = parse_temporal_tag(filename)
        if not temporal_tag or temporal_tag not in PERIOD_MAP:
            print(f"[WARN] Temporalité non supportée : {filename}")
            continue

        perf_days = PERIOD_MAP[temporal_tag]
        early_days = perf_days + 50

        filepath = DATA_DIR / filename
        try:
            with open(filepath, "r") as f:
                tokens = json.load(f)
        except Exception as e:
            print(f"[ERROR] Lecture JSON {filename} : {e}")
            continue

        for token in tokens:
            try:
                contract = token["contract"]
                symbol = token["symbol"].lower()
                platform = token["platform"].lower()
            except KeyError:
                print(f"[SKIP] Token invalide dans {filename}")
                continue

            if ((cache_df["contract_address"] == contract) &
                (cache_df["platform"] == platform) &
                (cache_df["temporal_tag"] == temporal_tag)).any():
                print(f"⏩ Déjà traité : {symbol} ({contract}) [{temporal_tag}]")
                continue

            query_id = dune_config.get(platform, {}).get("top_wallet")
            if not query_id:
                print(f"[SKIP] Aucune query Dune pour {platform}")
                continue

            params = {
                "token_address": contract.lower(),
                "perf_window": perf_days,
                "early_window": early_days
            }

            print(f"\n[🔍] {symbol.upper()} ({contract}) — {temporal_tag}")
            print(f"[📡] Query ID : {query_id} | Params : {params}")

            try:
                df = execute_dune_query(query_id, params)
                if df.empty:
                    print("⚠️ Aucun résultat — skip")
                    continue

                safe_symbol = symbol.replace(" ", "").replace("/", "").replace(":", "").replace(".", "")
                export_name = f"{contract.lower()}_{safe_symbol}_{temporal_tag}.csv"
                export_path = EXPORT_DIR / export_name
                df.to_csv(export_path, index=False)
                print(f"💾 Export : {export_path}")

                update_cache(cache_df, contract, platform, temporal_tag)

            except Exception as e:
                print(f"[❌] Erreur requête : {e}")
                continue

if __name__ == "__main__":
    run_token_discovery()