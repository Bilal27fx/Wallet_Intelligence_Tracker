#!/usr/bin/env python3
"""
EXPLOSIVE TOKENS RUNNER - PIPELINE SIMPLIFIÉ
Extrait uniquement les tokens explosifs avec leurs contrats EVM
"""

import sys
from pathlib import Path

# Ajouter le dossier au path
TOKEN_DISCOVERY_DIR = Path(__file__).parent
sys.path.insert(0, str(TOKEN_DISCOVERY_DIR))

from token_enrichment import run_token_enrichment


def main():
    """
    Pipeline simplifié : Extraction des tokens explosifs uniquement

    Étapes :
      1. Récupération des top tokens performants (CoinGecko)
      2. Enrichissement avec contrats (CMC + CoinGecko)
      3. Vérification EVM compatible
      4. Sauvegarde dans tokens_discovered
    """

    print("\n" + "=" * 80)
    print("🚀 EXTRACTION DES TOKENS EXPLOSIFS")
    print("=" * 80)
    print()
    print("Ce pipeline récupère les tokens les plus performants et leurs contrats.")
    print()

    # Configuration par défaut (optimisée pour éviter rate limit)
    config = {
        'periods': ["14d", "30d", "200d", "1y"],
        'top_n': 8,           # Top 8 tokens par période
        'max_tokens': 3000,    # Scraper jusqu'à 500 tokens max (évite rate limit)
        'delay_between': 30   # 30s entre chaque période
    }

    print("📋 Configuration :")
    print(f"   • Périodes : {', '.join(config['periods'])}")
    print(f"   • Top N par période : {config['top_n']}")
    print(f"   • Max tokens à scanner : {config['max_tokens']}")
    print(f"   • Délai entre périodes : {config['delay_between']}s")
    print()

    # Lancer l'extraction
    run_token_enrichment(
        periods=config['periods'],
        top_n=config['top_n'],
        max_tokens=config['max_tokens'],
        delay_between=config['delay_between']
    )

    print("\n" + "=" * 80)
    print("✅ EXTRACTION TERMINÉE")
    print("=" * 80)
    print()
    print("Les tokens explosifs sont maintenant disponibles dans tokens_discovered.")
    print()


if __name__ == "__main__":
    main()
