#!/usr/bin/env python3
"""Import des consensus du backtesting vers consensus_live pour éviter les doublons."""

import json
import sqlite3
from pathlib import Path
from datetime import datetime
from smart_wallet_analysis.config import DB_PATH, ROOT_DIR
from smart_wallet_analysis.logger import get_logger

logger = get_logger("consensus_live.import_backtesting")

BACKTESTING_DIR = ROOT_DIR / "data" / "backtesting" / "consensus_simple"

def _to_iso(value):
    """Convertit une date en ISO string."""
    if isinstance(value, str):
        return value
    return value.isoformat() if hasattr(value, "isoformat") else str(value)

def _json_default(value):
    """Sérialise les types non JSON natifs."""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)

def _to_json(value):
    """Sérialise un objet en JSON."""
    return json.dumps(value if value is not None else [], ensure_ascii=False, default=_json_default)

def get_latest_backtesting_file():
    """Récupère le fichier de backtesting le plus récent."""
    json_files = sorted(BACKTESTING_DIR.glob("consensus_simple_*.json"))
    if not json_files:
        logger.error("Aucun fichier de backtesting trouvé")
        return None
    latest = json_files[-1]
    logger.info(f"Fichier de backtesting le plus récent: {latest.name}")
    return latest

def import_consensus_from_backtesting(json_path=None):
    """Importe les consensus du backtesting dans consensus_live."""

    if json_path is None:
        json_path = get_latest_backtesting_file()
        if json_path is None:
            return 0

    logger.info(f"Chargement du fichier: {json_path}")

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    all_consensus = data.get('all_consensus', [])
    if not all_consensus:
        logger.warning("Aucun consensus trouvé dans le fichier")
        return 0

    logger.info(f"{len(all_consensus)} consensus trouvés dans le backtesting")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    imported_count = 0
    skipped_count = 0

    for consensus in all_consensus:
        symbol = consensus['symbol']
        contract_address = consensus['contract_address']

        cursor.execute("""
            SELECT COUNT(*) FROM consensus_live
            WHERE symbol = ? AND contract_address = ?
        """, (symbol, contract_address))

        exists = cursor.fetchone()[0] > 0

        if exists:
            logger.info(f"⏭️  Consensus {symbol} déjà présent, passage au suivant")
            skipped_count += 1
            continue

        whale_details = consensus.get('whale_details', [])
        detection_date = consensus.get('detection_date')
        period_start = consensus['consensus_period']['start']
        period_end = consensus['consensus_period']['end']

        wallet_addresses = [w.get('address') for w in whale_details if w.get('address')]
        detection_wallets = wallet_addresses[:3] if len(wallet_addresses) >= 3 else wallet_addresses
        detection_trigger_wallet = detection_wallets[-1] if detection_wallets else None

        formation_log = []
        for rank, whale in enumerate(whale_details, start=1):
            formation_log.append({
                "rank": rank,
                "wallet_address": whale.get('address'),
                "optimal_threshold_tier": whale.get('optimal_threshold_tier'),
                "threshold_status": whale.get('threshold_status'),
                "quality_score": whale.get('quality_score'),
                "optimal_roi": whale.get('optimal_roi'),
                "optimal_winrate": whale.get('optimal_winrate'),
                "investment_usd": whale.get('investment_usd'),
                "is_detection_step": rank == 3
            })

        perf = consensus.get('performance', {})

        cursor.execute("""
            INSERT INTO consensus_live (
                symbol, contract_address, whale_count, total_investment,
                first_buy, last_buy, detection_date, period_start, period_end,
                price_usd, wallet_details_json, formation_log_json,
                detection_wallets_json, detection_trigger_wallet, wallet_addresses_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol,
            contract_address,
            consensus['whale_count'],
            consensus['total_investment'],
            period_start,
            period_end,
            detection_date,
            period_start,
            period_end,
            perf.get('current_price'),
            _to_json(whale_details),
            _to_json(formation_log),
            _to_json(detection_wallets),
            detection_trigger_wallet,
            _to_json(wallet_addresses)
        ))

        imported_count += 1
        logger.info(f"✅ Importé: {symbol} ({consensus['whale_count']} wallets, ${consensus['total_investment']:,.0f})")

    conn.commit()
    conn.close()

    logger.info(f"\n📊 RÉSUMÉ:")
    logger.info(f"   • Consensus importés: {imported_count}")
    logger.info(f"   • Consensus déjà présents: {skipped_count}")
    logger.info(f"   • Total: {len(all_consensus)}")

    return imported_count

def main():
    """Point d'entrée du script."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Importe les consensus du backtesting dans consensus_live'
    )
    parser.add_argument(
        '--file',
        type=str,
        help='Chemin vers un fichier JSON spécifique (par défaut: le plus récent)'
    )

    args = parser.parse_args()

    json_path = Path(args.file) if args.file else None

    logger.info("🚀 Démarrage de l'import des consensus du backtesting")
    logger.info("=" * 80)

    count = import_consensus_from_backtesting(json_path)

    if count > 0:
        logger.info(f"\n✅ Import terminé avec succès: {count} consensus importés")
    else:
        logger.info(f"\n⚠️  Aucun nouveau consensus à importer")

if __name__ == "__main__":
    main()
