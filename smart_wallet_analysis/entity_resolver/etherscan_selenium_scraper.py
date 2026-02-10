import sqlite3
import os
import pandas as pd
from pathlib import Path

# === Chemins ===
DB_PATH = "data/db/wit_wallets.db"
WALLET_BALANCES_DIR = Path("data/processed/wallet_balances")
TOKEN_HISTORIES_DIR = Path("data/processed/token_histories")

# === Connexion base ===
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# === Injection des wallet_balances ===
print("📥 Injection des tokens détenus par wallet...")
for file in WALLET_BALANCES_DIR.glob("*.csv"):
    wallet_address = file.stem.lower()
    try:
        df = pd.read_csv(file)

        if "contract_address" not in df.columns:
            print(f"⚠️  {file.name} ignoré (pas de colonne 'contract_address')")
            continue

        df = df.dropna(subset=["contract_address"])

        for _, row in df.iterrows():
            token_address = str(row["contract_address"]).lower()
            symbol = str(row["token"])
            usd_value = float(row["usd_value"]) if not pd.isna(row["usd_value"]) else 0.0

            cursor.execute('''
                INSERT INTO wallet_tokens (
                    wallet_address, token_address, symbol,
                    first_seen, last_seen, volume_total_usd,
                    buy_count, sell_count
                ) VALUES (?, ?, ?, NULL, NULL, ?, NULL, NULL)
            ''', (
                wallet_address,
                token_address,
                symbol,
                usd_value
            ))

    except Exception as e:
        print(f"❌ Erreur lecture {file.name} : {e}")
        continue

# === Injection des token_histories ===
print("📥 Injection des transactions tokenisées...")
if TOKEN_HISTORIES_DIR.exists():
    for wallet_folder in TOKEN_HISTORIES_DIR.iterdir():
        if not wallet_folder.is_dir():
            continue
        wallet_address = wallet_folder.name.lower()
        for token_file in wallet_folder.glob("*.csv"):
            try:
                df = pd.read_csv(token_file)

                for _, row in df.iterrows():
                    timestamp = row.get("block_signed_at")
                    tx_hash = row.get("tx_hash")
                    from_address = str(row.get("from_address")).lower()
                    to_address = str(row.get("to_address")).lower()
                    token_address = str(row.get("contract_address")).lower()
                    amount = row.get("amount")

                    if pd.isna(amount):
                        print(f"⚠️  Ignoré (amount vide) : {token_file.name} ({wallet_address})")
                        continue

                    price_usd = float(row.get("price_usd")) if "price_usd" in row and not pd.isna(row["price_usd"]) else None
                    value_usd = float(row.get("value_usd")) if "value_usd" in row and not pd.isna(row["value_usd"]) else None

                    cursor.execute('''
                        INSERT OR IGNORE INTO wallet_transactions (
                            wallet_address, timestamp, tx_hash,
                            from_address, to_address,
                            token_address, amount,
                            price_usd, value_usd
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        wallet_address,
                        timestamp,
                        tx_hash,
                        from_address,
                        to_address,
                        token_address,
                        float(amount),
                        price_usd,
                        value_usd
                    ))

            except Exception as e:
                print(f"❌ Erreur lecture {token_file.name} ({wallet_address}) : {e}")
                continue

# === Finalisation ===
conn.commit()
conn.close()
print("✅ Données injectées avec succès dans wit_wallets.db")
