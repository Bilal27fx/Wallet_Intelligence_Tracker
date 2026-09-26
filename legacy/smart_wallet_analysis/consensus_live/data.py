#!/usr/bin/env python3
"""Accès DB et transactions pour consensus live."""

import json
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, timezone
from smart_wallet_analysis.config import DB_PATH, CONSENSUS_LIVE
from smart_wallet_analysis.logger import get_logger

logger = get_logger("consensus_live.data")

def _to_iso(value):
    """Convertit une date en ISO string."""
    return value.isoformat() if hasattr(value, "isoformat") else str(value)

def _json_default(value):
    """Sérialise les types non JSON natifs."""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)

def _to_json(value):
    """Sérialise un objet en JSON."""
    return json.dumps(value if value is not None else [], ensure_ascii=False, default=_json_default)

def _ensure_consensus_live_log_columns(cursor):
    """Ajoute les colonnes de logs de formation si absentes."""
    cursor.execute("PRAGMA table_info(consensus_live)")
    existing_columns = {row[1] for row in cursor.fetchall()}
    expected_columns = {
        "wallet_details_json": "TEXT",
        "formation_log_json": "TEXT",
        "detection_wallets_json": "TEXT",
        "detection_trigger_wallet": "TEXT",
        "wallet_addresses_json": "TEXT",
    }

    for column_name, column_type in expected_columns.items():
        if column_name in existing_columns:
            continue
        cursor.execute(
            f"ALTER TABLE consensus_live ADD COLUMN {column_name} {column_type}"
        )
        logger.info("Colonne ajoutée à consensus_live: %s", column_name)

def get_smart_wallets():
    """Récupère les wallets qualifiés depuis smart_wallets."""
    try:
        conn = sqlite3.connect(DB_PATH)

        query = """
            SELECT 
                wallet_address,
                optimal_threshold_tier,
                quality_score,
                threshold_status,
                optimal_roi,
                optimal_winrate
            FROM smart_wallets
            WHERE optimal_threshold_tier > 0
            AND threshold_status != 'NO_RELIABLE_TIERS'
            ORDER BY quality_score DESC
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        return df.set_index('wallet_address').to_dict('index')

    except Exception as e:
        logger.error(f"Erreur récupération smart wallets: {e}")
        return {}

def get_recent_transactions_live(smart_wallets):
    """Récupère les transactions des derniers jours."""
    try:
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=CONSENSUS_LIVE["PERIOD_DAYS"])

        conn = sqlite3.connect(DB_PATH)

        query = """
            SELECT 
                th.wallet_address,
                th.symbol,
                th.contract_address,
                th.quantity,
                th.total_value_usd as investment_usd,
                th.price_per_token,
                th.date
            FROM transaction_history th
            WHERE th.date BETWEEN ? AND ?
            AND th.action_type IN ('buy', 'receive')
            AND th.quantity > 0
            AND th.symbol NOT IN ({})
            AND th.wallet_address IN ({})
            ORDER BY th.date DESC
        """.format(
            ','.join(['?' for _ in CONSENSUS_LIVE["EXCLUDED_TOKENS"]]),
            ','.join(['?' for _ in smart_wallets.keys()])
        )

        params = [
            start_date.isoformat(),
            end_date.isoformat()
        ] + list(CONSENSUS_LIVE["EXCLUDED_TOKENS"]) + list(smart_wallets.keys())

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()

        if df.empty:
            return df

        df['date'] = pd.to_datetime(df['date'], utc=True, format='mixed')

        wallet_meta = pd.DataFrame.from_dict(smart_wallets, orient='index')
        wallet_meta.index.name = 'wallet_address'
        wallet_meta = wallet_meta.reset_index()
        df = df.merge(wallet_meta, on='wallet_address', how='left')

        df_grouped = df.groupby(['wallet_address', 'symbol'], as_index=False).agg({
            'investment_usd': 'sum',
            'optimal_threshold_tier': 'first',
            'quality_score': 'first',
            'threshold_status': 'first',
            'optimal_roi': 'first',
            'optimal_winrate': 'first'
        })

        df_grouped['threshold_usd'] = df_grouped['optimal_threshold_tier'].fillna(0) * 1000
        qualified = df_grouped[df_grouped['investment_usd'] >= df_grouped['threshold_usd']]

        logger.info(f"Seuils appliqués: {len(qualified)} wallet/token qualifiés sur {len(df_grouped)} combinaisons")

        if qualified.empty:
            return pd.DataFrame()

        df = df.merge(qualified[['wallet_address', 'symbol']], on=['wallet_address', 'symbol'], how='inner')
        return df

    except Exception as e:
        logger.error(f"Erreur récupération transactions live: {e}")
        return pd.DataFrame()

def get_existing_consensus_from_db():
    """Récupère les consensus déjà détectés depuis la BDD."""
    try:
        conn = sqlite3.connect(DB_PATH)

        query = """
            SELECT symbol, contract_address 
            FROM consensus_live 
            WHERE detection_date >= datetime('now', '-7 days')
            AND symbol IS NOT NULL 
            AND contract_address IS NOT NULL
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        existing = set()
        for _, row in df.iterrows():
            existing.add((row['symbol'], row['contract_address']))

        return existing

    except Exception as e:
        logger.warning(f"Erreur lecture consensus existants: {e}")
        return set()

def save_live_consensus_to_db(consensus_signals):
    """Sauvegarde les signaux de consensus live dans la base de données."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        _ensure_consensus_live_log_columns(cursor)

        cursor.execute("""
            DELETE FROM consensus_live 
            WHERE detection_date < datetime('now', '-7 days')
        """)

        for signal in consensus_signals:
            perf = signal.get('performance', {})
            token_info = signal.get('token_info', {})
            whale_details = signal.get("whale_details", [])
            formation_log = signal.get("formation_log", [])
            detection_wallets = signal.get("detection_wallets", [])
            detection_trigger_wallet = signal.get("detection_trigger_wallet")
            wallet_addresses = [
                wallet.get("address")
                for wallet in whale_details
                if wallet.get("address")
            ]

            cursor.execute("""
                INSERT OR REPLACE INTO consensus_live (
                    symbol, contract_address, whale_count, total_investment,
                    first_buy, last_buy, detection_date, period_start, period_end,
                    price_usd, market_cap_circulating, volume_24h, price_change_24h,
                    liquidity_usd, transactions_24h_buys, transactions_24h_sells,
                    wallet_details_json, formation_log_json, detection_wallets_json,
                    detection_trigger_wallet, wallet_addresses_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal['symbol'],
                signal['contract_address'],
                signal['whale_count'],
                signal['total_investment'],
                _to_iso(signal['period_start']),
                _to_iso(signal['period_end']),
                _to_iso(signal['detection_date']),
                _to_iso(signal['period_start']),
                _to_iso(signal['period_end']),
                perf.get('current_price'),
                token_info.get('market_cap', 0),
                token_info.get('volume_24h', 0),
                token_info.get('price_change_24h', 0),
                token_info.get('liquidity_usd', 0),
                token_info.get('txns_24h_buys', 0),
                token_info.get('txns_24h_sells', 0),
                _to_json(whale_details),
                _to_json(formation_log),
                _to_json(detection_wallets),
                detection_trigger_wallet,
                _to_json(wallet_addresses)
            ))

        conn.commit()
        conn.close()

        logger.info(f"{len(consensus_signals)} signaux sauvegardés dans consensus_live")

    except Exception as e:
        logger.error(f"Erreur sauvegarde consensus live: {e}")
