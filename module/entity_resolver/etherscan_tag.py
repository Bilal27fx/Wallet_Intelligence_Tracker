import requests
import os

# === Clé API Etherscan (à placer dans .env ou remplacer ici en dur)
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY") or "ta_clé_api_ici"

def get_etherscan_label(address: str) -> dict:
    label = {
        "address": address,
        "tag": None,
        "is_contract": False,
        "label": "unknown"
    }

    # === 1. Vérifie si c'est un contrat
    url_code = f"https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={ETHERSCAN_API_KEY}"
    res_code = requests.get(url_code).json()

    if res_code["status"] == "1" and res_code["result"]:
        code_info = res_code["result"][0]
        if code_info["SourceCode"]:
            label["is_contract"] = True
            label["label"] = "smart_contract"
        elif code_info["ABI"] != "Contract source code not verified":
            label["is_contract"] = True

    # === 2. Vérifie le solde pour détecter une whale potentielle
    url_balance = f"https://api.etherscan.io/api?module=account&action=balance&address={address}&tag=latest&apikey={ETHERSCAN_API_KEY}"
    res_balance = requests.get(url_balance).json()

    if res_balance.get("status") == "1" and "result" in res_balance:
        try:
            balance = int(res_balance["result"]) / 1e18
            if balance > 1_000:
                label["label"] = "whale_possible"
        except ValueError:
            pass

    # === 3. Scraping simple de la page publique Etherscan (pour tag comme Binance, etc.)
    try:
        html = requests.get(f"https://etherscan.io/address/{address}", timeout=10).text.lower()
        if "binance" in html:
            label["label"] = "cex"
            label["tag"] = "Binance"
        elif "kraken" in html:
            label["label"] = "cex"
            label["tag"] = "Kraken"
        elif "token creator" in html:
            label["label"] = "token_creator"
        elif "dex" in html or "swap" in html:
            if label["label"] == "unknown":
                label["label"] = "dex_trader"
    except Exception as e:
        print(f"⚠️ Erreur scraping Etherscan : {e}")

    return label


# === Exemple d'exécution
if __name__ == "__main__":
    address = "0xd551234ae421e3bcba99a0da6d736074f22192ff"  # ex : Binance
    result = get_etherscan_label(address)
    print("\n🔎 Résultat de l’analyse Etherscan :")
    for k, v in result.items():
        print(f"{k}: {v}")
