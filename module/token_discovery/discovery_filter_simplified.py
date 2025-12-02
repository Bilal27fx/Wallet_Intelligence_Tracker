import pandas as pd
from pathlib import Path

def process_new_wallets(
    raw_dir=None,
    existing_file=None,
    output_file=None,
    verbose=True
):
    # === Répertoires ===
    ROOT_DIR = Path(__file__).resolve().parents[2]
    RAW_DIR = Path(raw_dir) if raw_dir else ROOT_DIR / "data" / "raw" / "csv" / "top_wallets"
    EXISTING_FILE = Path(existing_file) if existing_file else ROOT_DIR / "data" / "processed" / "wallets_sources_mapping.csv"
    OUTPUT_FILE = Path(output_file) if output_file else ROOT_DIR / "data" / "processed" / "wallets_sources_new.csv"

    # === Chargement des wallets déjà connus ===
    try:
        df_existing = pd.read_csv(EXISTING_FILE)
        if df_existing.empty:
            raise pd.errors.EmptyDataError("Empty file")
        existing_wallets = set(df_existing["wallet"].str.lower())
    except (pd.errors.EmptyDataError, FileNotFoundError):
        if verbose: print("📁 Fichier mapping vide ou inexistant - création d'un nouveau mapping")
        df_existing = pd.DataFrame(columns=["wallet"])
        existing_wallets = set()

    # === Agrégation des wallets ===
    all_entries = []
    for file in RAW_DIR.glob("*_*_*.csv"):
        try:
            df = pd.read_csv(file)
            if "wallet" not in df.columns:
                if verbose: print(f"[SKIP] Pas de colonne 'wallet' dans {file.name}")
                continue

            parts = file.stem.split("_")
            if len(parts) < 3:
                if verbose: print(f"[WARN] Format inattendu pour {file.name}")
                continue

            for wallet in df["wallet"].dropna().unique():
                wallet_lc = wallet.lower()
                all_entries.append({
                    "wallet": wallet_lc
                })
        except Exception as e:
            if verbose: print(f"[ERROR] {file.name}: {e}")

    # === Filtrage des nouveaux wallets ===
    df_all = pd.DataFrame(all_entries).drop_duplicates(subset=["wallet"])
    df_new = df_all[~df_all["wallet"].str.lower().isin(existing_wallets)].copy()
    df_new.to_csv(OUTPUT_FILE, index=False)
    if verbose: print(f"✅ {len(df_new)} nouveaux wallets exportés dans : {OUTPUT_FILE}")

    # === Injection dans le mapping existant ===
    if verbose: print("➕ Injection dans wallets_sources_mapping.csv...")

    # Ajouter les nouveaux wallets au mapping existant
    df_existing = pd.concat([df_existing, df_new], ignore_index=True).drop_duplicates(subset=["wallet"])
    
    df_existing.to_csv(EXISTING_FILE, index=False)
    if verbose: print(f"🧠 Mapping mis à jour avec {len(df_new)} nouveaux wallets injectés.")

# Exemple d'appel
if __name__ == "__main__":
    process_new_wallets()