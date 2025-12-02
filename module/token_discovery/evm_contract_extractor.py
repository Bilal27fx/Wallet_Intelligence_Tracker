import json
from pathlib import Path

# === Plateformes EVM autorisées ===
EVM_PLATFORMS = {
    "ethereum", "binance smart chain", "base",
    "arbitrum one", "polygon pos", "optimistic ethereum",
    "avalanche", "fantom", "moonriver", "cronos",
    "bnb", "linea", "scroll", "zksync", "mantle", "blast"
}

# === Fichiers à traiter
FILES = {
    "14d": {
        "input": Path("data/raw/json/top_tokens_contracts_14d_all.json"),
        "output": Path("data/raw/json/top_tokens_contracts_14d_evm.json")
    },

    "30d": {
        "input": Path("data/raw/json/top_tokens_contracts_30d_all.json"),
        "output": Path("data/raw/json/top_tokens_contracts_30d_evm.json")
    },
    "200d": {
        "input": Path("data/raw/json/top_tokens_contracts_200d_all.json"),
        "output": Path("data/raw/json/top_tokens_contracts_200d_evm.json")
    },
    "1y": {
        "input": Path("data/raw/json/top_tokens_contracts_1y_all.json"),
        "output": Path("data/raw/json/top_tokens_contracts_1y_evm.json")
    }
}

def get_preferred_evm_contract(contracts):
    """Retourne le contrat Ethereum si présent, sinon premier contrat EVM valide, sinon None."""
    evm_contracts = [
        c for c in contracts
        if c.get("platform", "").strip().lower() in EVM_PLATFORMS
        and c.get("contract", "").startswith("0x")
    ]

    for c in evm_contracts:
        if c["platform"].strip().lower() == "ethereum":
            return c

    return evm_contracts[0] if evm_contracts else None

def process_file(input_path: Path, output_path: Path):
    with open(input_path, "r") as f:
        data = json.load(f)

    filtered = []
    for token in data:
        symbol = token.get("symbol")
        name = token.get("name")
        contracts = token.get("contracts", [])

        chosen = get_preferred_evm_contract(contracts)
        if chosen:
            filtered.append({
                "symbol": symbol,
                "name": name,
                "platform": chosen["platform"].strip().lower(),
                "contract": chosen["contract"]
            })
        else:
            print(f"[SKIP] {symbol} - aucun contrat EVM valide")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(filtered, f, indent=2)

    print(f"[✅] {output_path.name} : {len(filtered)} tokens EVM conservés")

def filter_all_evm_contracts():
    """Parcourt 30d, 200d, 1y et filtre un contrat EVM prioritaire par token."""
    for label, paths in FILES.items():
        print(f"\n[🔁] Traitement {label}")
        process_file(paths["input"], paths["output"])

# === Exécutable seul
if __name__ == "__main__":
    filter_all_evm_contracts()
