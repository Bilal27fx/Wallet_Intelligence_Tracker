#!/usr/bin/env python3
"""
Script pour lancer l'extraction d'historique des smart wallets uniquement
"""
import sys
from wallet_token_history_simple import process_smart_wallets_only

if __name__ == "__main__":
    # Forcer le flush de la sortie
    sys.stdout.reconfigure(line_buffering=True)

    print("🚀 Démarrage extraction smart wallets...")
    process_smart_wallets_only(min_value_usd=500, batch_size=5, batch_delay=10)
    print("✅ Traitement terminé")
