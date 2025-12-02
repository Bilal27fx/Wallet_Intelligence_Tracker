import requests
import pandas as pd
import time
from pathlib import Path

def safe_request(url, params, retries=5, delay=15):
    for attempt in range(retries):
        try:
            res = requests.get(url, params=params, timeout=10)
            if res.status_code == 200:
                return res
            elif res.status_code == 429:
                print(f"[RATE LIMIT] 429 reçu → attente {delay}s (tentative {attempt+1})")
                time.sleep(delay)
            else:
                print(f"[WARN] {res.status_code} → {url}")
                break
        except Exception as e:
            print(f"[ERROR] Exception → {e}")
            time.sleep(delay)
    return None

def get_top_tokens_by_period(period="1y", top_n=8, max_tokens=1000):
    assert period in ["1h", "24h", "7d", "14d", "30d", "200d", "1y"], "Période invalide"

    all_data = []
    pages_needed = (max_tokens // 250) + (1 if max_tokens % 250 > 0 else 0)

    for page in range(1, pages_needed + 1):
        print(f"[INFO] {period} → Page {page}")
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": page,
            "price_change_percentage": period
        }

        res = safe_request(url, params)
        if res and res.status_code == 200:
            all_data.extend(res.json())
        else:
            print(f"[FAIL] {period} → Échec récupération page {page}")
        time.sleep(1.5)

    if not all_data:
        print(f"[❌] {period} → Aucune donnée récupérée.")
        return None

    df = pd.DataFrame(all_data[:max_tokens])
    change_col = f"price_change_percentage_{period}_in_currency"

    if change_col not in df.columns:
        print(f"[ERROR] {period} → colonne manquante : {change_col}")
        print(f"[INFO] Colonnes présentes : {df.columns.tolist()}")
        return None

    df = df[["id", "symbol", "name", "current_price", change_col, "market_cap", "total_volume"]]

    # 💡 Filtrage sur le volume > 1M USD
    df = df[df["total_volume"] > 1000000]

    df = df.dropna(subset=[change_col])
    df = df.sort_values(by=change_col, ascending=False)
    top_df = df.head(top_n).copy()

    output_path = Path(f"data/raw/json/top_tokens_{period}.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    top_df.to_json(output_path, orient="records", indent=2)
    print(f"[✅ SUCCESS] Export {period} → {output_path}")

    return top_df

def process_periods(periods=["14d","30d", "200d", "1y"], top_n=8, max_tokens=3000, delay_between=15):
    for period in periods:
        print(f"\n[🔁] Traitement pour {period}")
        df = get_top_tokens_by_period(period=period, top_n=top_n, max_tokens=max_tokens)
        if df is not None:
            print(df[["symbol", "name", f"price_change_percentage_{period}_in_currency"]])
        else:
            print(f"[❌] Aucune donnée pour {period}")
        time.sleep(delay_between)

if __name__ == "__main__":
    process_periods()
