#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Algorithme de Backtesting Consensus Simplifié
Détecte les consensus entre whales qualifiés selon leurs seuils optimaux
SANS pondération ni scoring complexe - juste consensus pur
"""

import pandas as pd
import sqlite3
import requests
import time
import json
import numpy as np
from datetime import datetime, timedelta, timezone
from pathlib import Path
import argparse

from smart_wallet_analysis.logger import get_logger
from smart_wallet_analysis.backtesting_engine.trading_simulator import backtest_consensus_with_tp

# =============================================================================
# CONFIGURATION SIMPLIFIÉE
# =============================================================================

class SimpleBacktestConfig:
    """Configuration simplifiée pour le backtesting"""
    
    def __init__(self):
        # === PARAMÈTRES DE CONSENSUS SIMPLE ===
        self.min_whales_consensus = 2         # Nombre minimum de whales pour consensus
        self.exceptional_solo_signals = False # Signaux solo désactivés
        self.allow_mixed_signals = True       # Permettre signaux mixtes (exceptionnels + normaux)
        
        # === PARAMÈTRES TEMPORELS ===
        self.start_date = "2025-09-01"        # Date de début du backtesting
        self.period_days = 5             # Période d'analyse en jours
        
        # === FILTRES ===
        self.excluded_tokens = {               # Tokens à exclure
            'USDC', 'USDT', 'DAI', 'BUSD', 'ETH', 'WETH', 'BTC', 'BITCOIN', 'BNB', 'ETHEREUM'
        }
        
        # === PERFORMANCE ===
        self.price_check_delay = 0.5          # Délai entre les appels API prix
        
    def to_dict(self):
        """Convertit la config en dictionnaire"""
        return {
            'min_whales_consensus': self.min_whales_consensus,
            'exceptional_solo_signals': self.exceptional_solo_signals,
            'allow_mixed_signals': self.allow_mixed_signals,
            'start_date': self.start_date,
            'period_days': self.period_days,
            'excluded_tokens': list(self.excluded_tokens)
        }

# Configuration globale
config = SimpleBacktestConfig()

# Chemins
ROOT_DIR = Path(__file__).parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "db" / "wit_database.db"
OUTPUT_DIR = ROOT_DIR / "data" / "backtesting" / "consensus_simple"
logger = get_logger("backtesting.consensus_simple")

# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

def init_output_directory():
    """Initialise le dossier de sortie"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR

def get_current_price_dexscreener(contract_address, retries=2):
    """Récupère le prix actuel via DexScreener avec retry"""
    for attempt in range(retries):
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{contract_address}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            pairs = data.get("pairs", [])
            
            if pairs:
                # Prendre la paire avec le plus gros volume 24h
                best_pair = max(pairs, key=lambda x: float(x.get("volume", {}).get("h24", 0) or 0))
                price = float(best_pair.get("priceUsd", 0))
                return price if price > 0 else None
            
            return None
            
        except Exception as e:
            if attempt == retries - 1:
                logger.info(f"⚠️ Erreur prix pour {contract_address}: {e}")
                return None
            time.sleep(1)
    
    return None

def get_smart_wallets():
    """Récupère les top 15 wallets depuis wallet_scoring avec seuil 2K."""
    try:
        conn = sqlite3.connect(DB_PATH)
        query = """
            SELECT
                wallet_address,
                tier,
                score_final,
                win_rate,
                roi_1m,
                roi_12m,
                nb_trades
            FROM wallet_scoring
            ORDER BY score_final DESC
            LIMIT 15
        """
        df = pd.read_sql_query(query, conn)
        conn.close()

        # Transformer pour correspondre au format attendu
        wallets = {}
        for _, row in df.iterrows():
            wallets[row['wallet_address']] = {
                'optimal_threshold_tier': 0.5,  # Seuil fixe 2K ($2000)
                'quality_score': row['score_final'] / 100.0,  # Normaliser
                'threshold_status': 'TOP15',
                'optimal_roi': row['roi_12m'] if row['roi_12m'] else 0,
                'optimal_winrate': row['win_rate'] * 100 if row['win_rate'] else 0,
                'tier': row['tier'],
                'score_final': row['score_final'],
                'win_rate': row['win_rate'],
                'roi_1m': row['roi_1m']
            }
        return wallets

    except Exception as e:
        logger.info(f"❌ Erreur récupération wallets: {e}")
        return {}

def _to_utc_z(dt: datetime) -> str:
    """Formatte un datetime UTC en 'YYYY-MM-DDTHH:MM:SSZ'"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

def get_transactions_in_period_simple(start_date, end_date, smart_wallets):
    """Récupère les transactions en appliquant les seuils optimaux SIMPLES"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Récupérer toutes les transactions des wallets qualifiés
        query = """
            SELECT 
                th.wallet_address,
                th.symbol,
                th.contract_address,
                th.quantity,
                th.total_value_usd as investment_usd,
                th.price_per_token,
                th.date,
                th.hash as transaction_hash,
                th.operation_type,
                th.action_type,
                th.swap_description
            FROM transaction_history th
            WHERE th.date BETWEEN ? AND ?
            AND th.action_type IN ('buy', 'receive')
            AND th.quantity > 0
            AND th.symbol NOT IN ({})
            AND th.wallet_address IN ({})
            ORDER BY th.date ASC
        """.format(
            ','.join(['?' for _ in config.excluded_tokens]),
            ','.join(['?' for _ in smart_wallets.keys()])
        )
        
        params = [
            _to_utc_z(start_date), 
            _to_utc_z(end_date)
        ] + list(config.excluded_tokens) + list(smart_wallets.keys())
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        if df.empty:
            return df
            
        # Convertir les dates (tolérant ISO8601: Z, +00:00, etc.)
        df['date'] = pd.to_datetime(df['date'], utc=True, format='ISO8601', errors='coerce')
        # Fallback permissif si certaines valeurs ne sont pas ISO strict
        mask_na = df['date'].isna()
        if mask_na.any():
            df.loc[mask_na, 'date'] = pd.to_datetime(df.loc[mask_na, 'date'], utc=True, errors='coerce')
        # Supprimer les lignes inparsables
        df = df.dropna(subset=['date']).reset_index(drop=True)
        
        # Ajouter d'abord les métadonnées des wallets 
        logger.info(f"🔄 Application des seuils avec sommation des investissements...")
        
        wallet_data_list = []
        for _, row in df.iterrows():
            wallet_data = smart_wallets.get(row['wallet_address'], {})
            wallet_data_list.append({
                'optimal_threshold_tier': wallet_data.get('optimal_threshold_tier', 0),
                'quality_score': wallet_data.get('quality_score', 0.0),
                'threshold_status': wallet_data.get('threshold_status', 'UNKNOWN'),
                'optimal_roi': wallet_data.get('optimal_roi', 0.0),
                'optimal_winrate': wallet_data.get('optimal_winrate', 0.0)
            })
        
        # Ajouter les colonnes
        if wallet_data_list:
            for key in wallet_data_list[0].keys():
                df[key] = [w[key] for w in wallet_data_list]
        
        # Grouper par wallet + symbol et sommer les investissements
        df_grouped = df.groupby(['wallet_address', 'symbol']).agg({
            'investment_usd': 'sum',
            'optimal_threshold_tier': 'first',
            'quality_score': 'first',
            'threshold_status': 'first',
            'optimal_roi': 'first',
            'optimal_winrate': 'first'
        }).reset_index()
        
        # Filtrer selon les seuils optimaux avec sommation
        qualified_pairs = []
        for _, row in df_grouped.iterrows():
            threshold_usd = row['optimal_threshold_tier'] * 1000 if row['optimal_threshold_tier'] > 0 else 0
            if row['investment_usd'] >= threshold_usd:
                qualified_pairs.append((row['wallet_address'], row['symbol']))
        
        logger.info(f"🎯 Seuils avec sommation appliqués: {len(qualified_pairs)} wallet/token qualifiés")
        logger.info(f"   (sur {len(df_grouped)} combinaisons wallet/token au total)")
        
        # Filtrer les transactions originales pour ne garder que les paires qualifiées
        if qualified_pairs:
            mask_qualified = df.apply(
                lambda row: (row['wallet_address'], row['symbol']) in qualified_pairs, axis=1
            )
            df = df[mask_qualified].reset_index(drop=True)
        else:
            df = pd.DataFrame()
        
        return df
        
    except Exception as e:
        logger.info(f"❌ Erreur récupération transactions simples: {e}")
        return pd.DataFrame()

def detect_consensus_in_period(df_transactions, global_detected_tokens=None):
    """Détecte les consensus ≥2 wallets (sans signaux solo)"""
    if df_transactions.empty:
        return []
    
    if global_detected_tokens is None:
        global_detected_tokens = set()
    
    signals_detected = []
    processed_tokens = set()
    
    # Grouper par token
    for symbol, token_group in df_transactions.groupby('symbol'):
        if symbol in processed_tokens:
            continue
        
        # NOUVEAU: Vérifier si le token a déjà été détecté globalement
        if symbol in global_detected_tokens:
            logger.info(f"⏭️ Token {symbol} déjà détecté précédemment, passage au suivant")
            continue
            
        token_group = token_group.sort_values('date')
        
        # Analyser chaque transaction comme point de départ potentiel
        for idx, base_tx in token_group.iterrows():
            window_end = base_tx['date'] + timedelta(days=config.period_days)
            
            # Transactions dans la fenêtre
            window_txs = token_group[
                (token_group['date'] >= base_tx['date']) &
                (token_group['date'] <= window_end)
            ]
            
            # Analyser les wallets participants avec SOMMATION par wallet
            whale_analysis = {}

            # D'abord, grouper et sommer les investissements par wallet dans cette fenêtre
            wallet_sums = window_txs.groupby('wallet_address').agg({
                'investment_usd': 'sum',
                'optimal_threshold_tier': 'first',
                'quality_score': 'first',
                'threshold_status': 'first',
                'optimal_roi': 'first',
                'optimal_winrate': 'first'
            })

            # Vérifier quels wallets dépassent leur seuil optimal avec la somme
            qualified_wallets = set()
            for wallet_addr, wallet_data in wallet_sums.iterrows():
                threshold_usd = wallet_data['optimal_threshold_tier'] * 1000
                if wallet_data['investment_usd'] >= threshold_usd:
                    qualified_wallets.add(wallet_addr)

            # Maintenant analyser seulement les wallets qualifiés
            for _, tx in window_txs.iterrows():
                wallet_addr = tx['wallet_address']

                # Ignorer les wallets qui ne dépassent pas leur seuil avec la somme
                if wallet_addr not in qualified_wallets:
                    continue

                if wallet_addr not in whale_analysis:
                    whale_analysis[wallet_addr] = {
                        'transactions': [],
                        'total_investment': 0,
                        'wallet_data': {
                            'optimal_threshold_tier': tx['optimal_threshold_tier'],
                            'quality_score': tx['quality_score'],
                            'threshold_status': tx['threshold_status'],
                            'optimal_roi': tx['optimal_roi'],
                            'optimal_winrate': tx['optimal_winrate']
                        }
                    }

                whale_analysis[wallet_addr]['transactions'].append(tx)
                whale_analysis[wallet_addr]['total_investment'] += tx['investment_usd']

            # LOGIQUE DE DÉTECTION CONSENSUS SIMPLE
            unique_whales = len(whale_analysis)
            signal_valid = False
            signal_type = "CONSENSUS"

            # Consensus simple: >=2 wallets
            if unique_whales >= config.min_whales_consensus:
                signal_valid = True

            if signal_valid:

                # Signal détecté !
                signal_txs = window_txs
                
                signal_data = {
                    'symbol': symbol,
                    'contract_address': base_tx['contract_address'],
                    'detection_date': base_tx['date'],
                    'consensus_start': base_tx['date'],
                    'consensus_end': signal_txs['date'].max(),
                    'whale_count': unique_whales,
                    'signal_type': signal_type,
                    'total_investment': sum(data['total_investment'] for data in whale_analysis.values()),
                    'avg_entry_price': (signal_txs['investment_usd'] * signal_txs['price_per_token']).sum() / signal_txs['investment_usd'].sum(),
                    'transactions': signal_txs,
                    'whale_details': []
                }
                
                # Détails des wallets
                for wallet_addr, data in whale_analysis.items():
                    wallet_investment = data['total_investment']
                    wallet_data = data['wallet_data']
                    
                    signal_data['whale_details'].append({
                        'address': wallet_addr,
                        'optimal_threshold_tier': wallet_data['optimal_threshold_tier'],
                        'quality_score': wallet_data['quality_score'],
                        'threshold_status': wallet_data['threshold_status'],
                        'optimal_roi': wallet_data['optimal_roi'],
                        'optimal_winrate': wallet_data['optimal_winrate'],
                        'investment_usd': wallet_investment,
                        'transaction_count': len(data['transactions'])
                    })
                
                # Trier par investissement décroissant
                signal_data['whale_details'].sort(key=lambda x: -x['investment_usd'])
                
                signals_detected.append(signal_data)
                processed_tokens.add(symbol)
                # NOUVEAU: Ajouter au set global pour éviter re-détection
                global_detected_tokens.add(symbol)
                logger.info(f"✅ Token {symbol} ajouté aux tokens détectés globalement")
                break  # Prendre le premier signal pour ce token dans cette période
    
    return signals_detected

def calculate_performance(consensus_data):
    """Calcule la performance d'un consensus (identique)"""
    symbol = consensus_data['symbol']
    contract_address = consensus_data['contract_address']
    avg_entry_price = consensus_data['avg_entry_price']
    detection_date = consensus_data['detection_date']
    
    if not contract_address or avg_entry_price <= 0:
        return {
            'symbol': symbol,
            'entry_price': avg_entry_price,
            'current_price': None,
            'performance_pct': None,
            'days_held': (datetime.now(timezone.utc) - detection_date).days,
            'status': 'DONNÉES_INSUFFISANTES'
        }
    
    # Récupérer le prix actuel
    current_price = get_current_price_dexscreener(contract_address)
    
    if current_price:
        performance_pct = ((current_price - avg_entry_price) / avg_entry_price) * 100
        days_held = (datetime.now(timezone.utc) - detection_date).days
        
        # Classification
        if performance_pct >= 1000:
            status = "🚀 MOON SHOT"
        elif performance_pct >= 500:
            status = "🌟 EXCELLENT"
        elif performance_pct >= 100:
            status = "💚 TRÈS BON"
        elif performance_pct >= 50:
            status = "📈 BON"
        elif performance_pct >= 0:
            status = "🟡 POSITIF"
        elif performance_pct >= -30:
            status = "📉 NÉGATIF"
        else:
            status = "🔴 TRÈS NÉGATIF"
        
        return {
            'symbol': symbol,
            'entry_price': avg_entry_price,
            'current_price': current_price,
            'performance_pct': performance_pct,
            'days_held': days_held,
            'status': status,
            'annualized_return': (performance_pct / max(days_held, 1) * 365)
        }
    else:
        return {
            'symbol': symbol,
            'entry_price': avg_entry_price,
            'current_price': None,
            'performance_pct': None,
            'days_held': (datetime.now(timezone.utc) - detection_date).days,
            'status': 'PRIX_NON_DISPONIBLE'
        }

def run_simple_backtesting():
    """Lance le backtesting SIMPLE basé sur les seuils optimaux"""
    
    logger.info(f"🎯 BACKTESTING CONSENSUS SIMPLE")
    logger.info("=" * 80)
    logger.info(f"📅 Début: {config.start_date}")
    logger.info(f"⏰ Période d'analyse: {config.period_days} jours")
    logger.info(f"🐋 Consensus minimum: ≥{config.min_whales_consensus} wallets")
    logger.info("🎯 Règle qualité: au moins 1 wallet EXCELLENT/EXCEPTIONAL")
    logger.info(f"📊 Seuils optimaux: OUI (avec sommation)")
    logger.info(f"⚖️ Signaux solo: NON (supprimés)")
    logger.info("=" * 80)
    
    # Charger les smart wallets
    logger.info(f"📖 Chargement des smart wallets...")
    smart_wallets = get_smart_wallets()
    
    if not smart_wallets:
        logger.info("❌ Aucun smart wallet trouvé")
        return [], []
    
    logger.info(f"✅ {len(smart_wallets)} smart wallets chargés")
    
    # Afficher quelques exemples
    logger.info(f"\n📋 Exemples de smart wallets:")
    for i, (wallet, data) in enumerate(list(smart_wallets.items())[:5]):
        logger.info(f"   {wallet[:10]}...{wallet[-8:]}: "
              f"Seuil {data['optimal_threshold_tier']}K | "
              f"Qualité {data['quality_score']:.3f} | "
              f"ROI {data['optimal_roi']:.1f}% | "
              f"Status {data['threshold_status']}")
    
    # Dates de début et fin
    start_date = datetime.strptime(config.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_date = datetime.now(timezone.utc)
    
    # Résultats globaux
    all_consensus = []
    period_results = []
    detected_tokens = set()
    
    # Fenêtre glissante
    current_date = start_date
    period_number = 1
    
    while current_date < end_date:
        period_end = current_date + timedelta(days=config.period_days)
        if period_end > end_date:
            period_end = end_date
        
        logger.info(f"\n📊 PÉRIODE {period_number}: {current_date.strftime('%Y-%m-%d')} → {period_end.strftime('%Y-%m-%d')}")
        logger.info("-" * 60)
        
        # Récupérer les transactions avec seuils SIMPLES
        df_transactions = get_transactions_in_period_simple(current_date, period_end, smart_wallets)
        
        if df_transactions.empty:
            logger.info("❌ Aucune transaction qualifiée dans cette période")
            period_results.append({
                'period_number': period_number,
                'period_start': current_date,
                'period_end': period_end,
                'transactions_count': 0,
                'consensus_count': 0,
                'consensus_detected': []
            })
        else:
            logger.info(f"📈 {len(df_transactions)} transactions qualifiées trouvées")
            logger.info(f"🐋 {df_transactions['wallet_address'].nunique()} wallets actifs")
            logger.info(f"🪙 {df_transactions['symbol'].nunique()} tokens uniques")
            
            # Détecter les consensus
            consensus_detected = detect_consensus_in_period(df_transactions, detected_tokens)
            
            if consensus_detected:
                logger.info(f"✅ {len(consensus_detected)} consensus détectés:")
                for signal in consensus_detected:
                    # Emoji selon le type de signal
                    type_emoji = {
                        'EXCEPTIONAL_CONSENSUS': '🌟', 
                        'MIXED_CONSENSUS': '🎯',
                        'INVALID_CONSENSUS': '⛔'
                    }
                    emoji = type_emoji.get(signal['signal_type'], '🔍')
                    
                    logger.info(f"   {emoji} {signal['symbol']} ({signal['signal_type']}): "
                          f"{signal['whale_count']} wallets, ${signal['total_investment']:,.0f}")
                    logger.info(f"     📅 Détecté le: {signal['detection_date'].strftime('%Y-%m-%d %H:%M')}")
                    logger.info(f"     🐋 Wallets participants:")
                    
                    for whale in signal['whale_details']:
                        # Trouver la première transaction de cette whale pour ce token
                        whale_txs = signal['transactions'][signal['transactions']['wallet_address'] == whale['address']]
                        whale_date = whale_txs['date'].min().strftime('%Y-%m-%d %H:%M') if not whale_txs.empty else "N/A"

                        logger.info(f"        🔷 [{whale['threshold_status']}] "
                              f"Seuil {whale['optimal_threshold_tier']}K | "
                              f"Q={whale['quality_score']:.3f} | "
                              f"ROI {whale['optimal_roi']:+.1f}% | "
                              f"${whale['investment_usd']:,.0f} | "
                              f"{whale_date} | "
                              f"{whale['address'][:10]}...{whale['address'][-8:]}")
                    logger.info("")
            else:
                logger.info("❌ Aucun consensus détecté")
            
            # Stocker les résultats de la période
            period_results.append({
                'period_number': period_number,
                'period_start': current_date,
                'period_end': period_end,
                'transactions_count': len(df_transactions),
                'whale_count': df_transactions['wallet_address'].nunique(),
                'tokens_count': df_transactions['symbol'].nunique(),
                'consensus_count': len(consensus_detected),
                'consensus_detected': consensus_detected
            })
            
            all_consensus.extend(consensus_detected)
        
        # Avancer à la période suivante
        current_date = period_end
        period_number += 1
        time.sleep(0.1)
    
    logger.info(f"\n🎯 RÉSUMÉ GLOBAL:")
    logger.info("=" * 80)
    logger.info(f"📊 {period_number-1} périodes analysées")
    logger.info(f"🚀 {len(all_consensus)} consensus SIMPLES détectés au total")
    
    if all_consensus:
        # Calculer les performances
        logger.info(f"\n💹 CALCUL DES PERFORMANCES")
        logger.info("-" * 50)
        
        for consensus in all_consensus:
            perf = calculate_performance(consensus)
            consensus['performance'] = perf
            
            if perf['performance_pct'] is not None:
                logger.info(f"{perf['status']} {perf['symbol']}: "
                      f"${perf['entry_price']:.8f} → ${perf['current_price']:.8f} "
                      f"({perf['performance_pct']:+.1f}% | {perf['days_held']}j) "
                      f"[Whales: {consensus['whale_count']}]")
            else:
                logger.info(f"{perf['status']} {perf['symbol']}: ${perf['entry_price']:.8f}")
            
            time.sleep(config.price_check_delay)
    
    return all_consensus, period_results

def export_simple_results(all_consensus, period_results):
    """Exporte les résultats SIMPLES vers JSON"""
    
    init_output_directory()
    
    # Calculer les statistiques
    performances = []
    for consensus in all_consensus:
        perf = consensus.get('performance', {}).get('performance_pct')
        if perf is not None:
            performances.append(perf)
    
    stats = {}
    if performances:
        stats = {
            'total_consensus': len(all_consensus),
            'measurable_performances': len(performances),
            'average_performance': float(sum(performances) / len(performances)),
            'positive_count': int(sum(1 for p in performances if p > 0)),
            'success_rate': float(sum(1 for p in performances if p > 0) / len(performances) * 100),
            'best_performance': float(max(performances)),
            'worst_performance': float(min(performances)),
            'median_performance': float(np.median(performances)),
            'total_investment': float(sum(c['total_investment'] for c in all_consensus))
        }
    
    now_utc_z = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    # Préparer les données d'export
    export_data = {
        'metadata': {
            'timestamp': now_utc_z,
            'config': config.to_dict(),
            'periods_analyzed': len(period_results),
            'date_range': {
                'start': config.start_date,
                'end': now_utc_z[:10]
            },
            'simple_features': {
                'weighting_removed': True,
                'consensus_logic': 'pure_threshold_based',
                'scoring_complexity': 'none'
            }
        },
        'statistics': stats,
        'period_results': [],
        'all_consensus': []
    }
    
    # Convertir les résultats par période
    for period in period_results:
        period_data = {
            'period_number': period['period_number'],
            'period_start': period['period_start'].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'period_end': period['period_end'].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'transactions_count': period['transactions_count'],
            'whale_count': period.get('whale_count', 0),
            'tokens_count': period.get('tokens_count', 0),
            'consensus_count': period['consensus_count'],
            'consensus_symbols': [c['symbol'] for c in period['consensus_detected']]
        }
        export_data['period_results'].append(period_data)
    
    # Convertir tous les consensus
    for consensus in all_consensus:
        consensus_data = {
            'symbol': consensus['symbol'],
            'contract_address': consensus['contract_address'],
            'detection_date': consensus['detection_date'].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'consensus_period': {
                'start': consensus['consensus_start'].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'end': consensus['consensus_end'].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'duration_hours': float((consensus['consensus_end'] - consensus['consensus_start']).total_seconds() / 3600)
            },
            'whale_count': int(consensus['whale_count']),
            'signal_type': consensus.get('signal_type'),
            'total_investment': float(consensus['total_investment']),
            'avg_entry_price': float(consensus['avg_entry_price']),
            'whale_details': consensus['whale_details'],
            'performance': consensus.get('performance', {})
        }
        export_data['all_consensus'].append(consensus_data)
    
    # Trier par performance (None → très petit)
    def _perf_key(x):
        p = x['performance'].get('performance_pct')
        return p if p is not None else -1e18
    
    export_data['all_consensus'].sort(key=_perf_key, reverse=True)
    
    # Sauvegarder
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    output_file = OUTPUT_DIR / f"consensus_simple_{timestamp}.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
    
    logger.info(f"\n✅ Résultats SIMPLES exportés: {output_file}")
    
    # Afficher les statistiques
    if stats:
        logger.info(f"\n📊 STATISTIQUES FINALES SIMPLES:")
        logger.info(f"   • Consensus détectés: {stats['total_consensus']}")
        logger.info(f"   • Performances mesurables: {stats['measurable_performances']}")
        logger.info(f"   • Performance moyenne: {stats['average_performance']:+.1f}%")
        logger.info(f"   • Taux de succès: {stats['success_rate']:.1f}%")
        logger.info(f"   • Meilleure performance: {stats['best_performance']:+.1f}%")
        logger.info(f"   • Investment total: ${stats['total_investment']:,.0f}")
    
    return export_data

# =============================================================================
# EXÉCUTION PRINCIPALE
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Backtesting Consensus Simple')
    
    # Paramètres simples
    parser.add_argument('--min-whales', type=int, default=3, 
                       help='Nombre minimum de whales pour consensus')
    
    # Paramètres temporels
    parser.add_argument('--start-date', default="2026-01-01",
                       help='Date de début (YYYY-MM-DD)')
    parser.add_argument('--period-days', type=int, default=3,
                       help='Période d\'analyse en jours')
    
    args = parser.parse_args()
    
    # Appliquer les paramètres
    config.min_whales_consensus = args.min_whales
    config.start_date = args.start_date
    config.period_days = args.period_days
    
    # Lancer le backtesting SIMPLE
    all_consensus, period_results = run_simple_backtesting()

    if all_consensus:
        export_simple_results(all_consensus, period_results)

        # Simuler le trading avec paliers de TP
        logger.info(f"\n{'='*80}")
        logger.info("🎯 LANCEMENT DE LA SIMULATION DE TRADING")
        logger.info(f"{'='*80}")
        trading_results = backtest_consensus_with_tp(all_consensus)

        logger.info(f"\n🎉 BACKTESTING SIMPLE TERMINÉ AVEC SUCCÈS!")
    else:
        logger.info(f"\n❌ Aucun consensus simple détecté avec ces paramètres")

if __name__ == "__main__":
    main()
