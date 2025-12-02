import os
import json
import requests
import time
from dotenv import load_dotenv
from pathlib import Path

# === Chargement clé API CMC
load_dotenv()
API_KEY = os.getenv("CG_API_KEY")
HEADERS = {"X-CMC_PRO_API_KEY": API_KEY}

# === Fichiers JSON d'entrée/sortie
INPUT_FILES = {
    "1y": Path("data/raw/json/top_tokens_1y.json"),
    "200d": Path("data/raw/json/top_tokens_200d.json"),
    "30d": Path("data/raw/json/top_tokens_30d.json"),
    "14d": Path("data/raw/json/top_tokens_14d.json"),

}

OUTPUT_FILES = {
    "1y": Path("data/raw/json/top_tokens_contracts_1y_all.json"),
    "200d": Path("data/raw/json/top_tokens_contracts_200d_all.json"),
    "30d": Path("data/raw/json/top_tokens_contracts_30d_all.json"),
    "14d": Path("data/raw/json/top_tokens_contracts_14d_all.json"),

}

# === CoinMarketCap : récupérer l'ID
def get_cmc_id(symbol):
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
    params = {"symbol": symbol.upper()}
    try:
        res = requests.get(url, headers=HEADERS, params=params, timeout=10)
        data = res.json()
        if "data" in data and data["data"]:
            return data["data"][0]["id"]
    except Exception as e:
        print(f"[ERROR] ID CMC pour {symbol} : {e}")
    return None

# === CoinMarketCap : récupérer le contrat principal
def get_main_contract_cmc(cmc_id):
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/info"
    params = {"id": cmc_id}
    try:
        res = requests.get(url, headers=HEADERS, params=params, timeout=10)
        data = res.json()
        if "data" in data and str(cmc_id) in data["data"]:
            platform = data["data"][str(cmc_id)].get("platform", {})
            if platform and platform.get("token_address"):
                return [{
                    "platform": platform.get("name", "").strip().lower(),
                    "contract": platform.get("token_address")
                }]
    except Exception as e:
        print(f"[ERROR] CMC contract pour ID {cmc_id} : {e}")
    return []

# === CoinGecko : fallback pour récupérer tous les contrats
def get_all_contracts_from_coingecko(gecko_id, retries=3):
    url = f"https://api.coingecko.com/api/v3/coins/{gecko_id.lower()}"
    for attempt in range(retries):
        try:
            res = requests.get(url, timeout=10)
            if res.status_code == 200:
                platforms = res.json().get("platforms", {})
                contracts = []
                for key, val in platforms.items():
                    if val and val.startswith("0x"):
                        contracts.append({
                            "platform": key.strip().lower(),
                            "contract": val
                        })
                return contracts
            elif res.status_code == 429:
                print(f"[WAIT] CoinGecko rate limit pour {gecko_id} → attente 15 sec...")
                time.sleep(15)
        except Exception as e:
            print(f"[RETRY] CoinGecko {gecko_id} : {e}")
            time.sleep(5)
    print(f"[FAIL] CoinGecko max retries pour {gecko_id}")
    return []

# === Traitement et export
def process_and_export(tokens, output_path):
    result = []
    for token in tokens:
        symbol = token.get("symbol")
        name = token.get("name")
        gecko_id = token.get("id")

        print(f"[INFO] Récupération de {symbol}...")

        contracts = []

        cmc_id = get_cmc_id(symbol)
        if cmc_id:
            contracts += get_main_contract_cmc(cmc_id)

        contracts += get_all_contracts_from_coingecko(gecko_id)

        seen = set()
        clean_contracts = []
        for entry in contracts:
            key = (entry["platform"], entry["contract"])
            if key not in seen:
                seen.add(key)
                clean_contracts.append(entry)

        if clean_contracts:
            result.append({
                "symbol": symbol,
                "name": name,
                "contracts": clean_contracts
            })
            print(f"[OK] {symbol} → {len(clean_contracts)} contrat(s)")
        else:
            print(f"[SKIP] {symbol} - aucun contrat trouvé")

        time.sleep(1.2)

    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"[✅] Export terminé → {output_path} ({len(result)} tokens)")

# === Fonction principale avec priorité 1y > 200d > 30d
def extract_and_save_simplified_data():
    all_tokens = {}
    for key, path in INPUT_FILES.items():
        with open(path, "r") as f:
            all_tokens[key] = json.load(f)

    tokens_1y = all_tokens["1y"]
    symbols_1y = {t["symbol"] for t in tokens_1y}

    tokens_200d = [t for t in all_tokens["200d"] if t["symbol"] not in symbols_1y]
    symbols_1y_200d = symbols_1y.union({t["symbol"] for t in tokens_200d})

    tokens_30d = [t for t in all_tokens["30d"] if t["symbol"] not in symbols_1y_200d]
    symbols_1y_200d_30d = symbols_1y_200d.union({t["symbol"] for t in tokens_30d})

    tokens_14d = [t for t in all_tokens["14d"] if t["symbol"] not in symbols_1y_200d_30d]

    print(f"[INFO] {len(tokens_14d)} tokens à traiter sur 14d")
    process_and_export(tokens_14d, OUTPUT_FILES["14d"])
    time.sleep(10)

    print(f"[INFO] {len(tokens_30d)} tokens à traiter sur 30d")
    process_and_export(tokens_30d, OUTPUT_FILES["30d"])
    time.sleep(10)

    print(f"[INFO] {len(tokens_200d)} tokens à traiter sur 200d")
    process_and_export(tokens_200d, OUTPUT_FILES["200d"])
    time.sleep(10)

    print(f"[INFO] {len(tokens_1y)} tokens à traiter sur 1y")
    process_and_export(tokens_1y, OUTPUT_FILES["1y"])

# === Exécution
if __name__ == "__main__":
    extract_and_save_simplified_data()