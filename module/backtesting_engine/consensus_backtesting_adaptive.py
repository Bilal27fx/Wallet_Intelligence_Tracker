#!/usr/bin/env python3
"""
Algorithme de Backtesting Consensus Adaptatif
Utilise les seuils optimaux τ_w et qualités q_w de smart_wallets
pour adapter dynamiquement les critères de consensus selon chaque wallet
"""

import pandas as pd
import sqlite3
import requests
import time
import json
import os
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from pathlib import Path
import argparse

# =============================================================================
# CONFIGURATION ADAPTATIVE
# =============================================================================

class AdaptiveBacktestConfig:
    """Configuration adaptative basée sur les seuils optimaux"""
    
    def __init__(self):
        # === PARAMÈTRES DE CONSENSUS ADAPTATIF ===
        self.min_whales_consensus = 1         # Nombre minimum de whales pour former un consensus
        self.quality_threshold = 0.1          # Qualité minimum pour participer (q_w >= 0.1)
        self.use_optimal_thresholds = True    # Utiliser les seuils optimaux τ_w
        self.quality_weighting = True         # Pondérer par qualité q_w
        
        # === PARAMÈTRES TEMPORELS ===
        self.start_date = "2024-09-01"        # Date de début du backtesting
        self.period_days = 10                 # Période d'analyse en jours
        
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
            'quality_threshold': self.quality_threshold,
            'use_optimal_thresholds': self.use_optimal_thresholds,
            'quality_weighting': self.quality_weighting,
            'start_date': self.start_date,
            'period_days': self.period_days,
            'excluded_tokens': list(self.excluded_tokens)
        }

# Configuration globale
config = AdaptiveBacktestConfig()

# Chemins
ROOT_DIR = Path(__file__).parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "db" / "wit_database.db"
OUTPUT_DIR = ROOT_DIR / "data" / "backtesting" / "consensus_adaptive"

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
                print(f"⚠️ Erreur prix pour {contract_address}: {e}")
                return None
            time.sleep(1)
    
    return None

def get_smart_wallets_thresholds():
    """Récupère les seuils optimaux et qualités depuis smart_wallets"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        query = """
            SELECT 
                wallet_address,
                optimal_threshold_tier,
                quality_score,
                threshold_status,
                optimal_roi,
                optimal_winrate,
                global_roi,
                global_winrate
            FROM smart_wallets
            WHERE quality_score >= ? 
            AND threshold_status != 'NO_RELIABLE_TIERS'
            ORDER BY quality_score DESC
        """
        
        df = pd.read_sql_query(query, conn, params=[config.quality_threshold])
        conn.close()
        
        return df.set_index('wallet_address').to_dict('index')
        
    except Exception as e:
        print(f"❌ Erreur récupération seuils optimaux: {e}")
        return {}

def get_transactions_in_period_adaptive(start_date, end_date, wallet_thresholds):
    """Récupère les transactions en appliquant les seuils optimaux adaptatifs"""
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
            ','.join(['?' for _ in wallet_thresholds.keys()])
        )
        
        params = [
            start_date.isoformat(), 
            end_date.isoformat()
        ] + list(config.excluded_tokens) + list(wallet_thresholds.keys())
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        if df.empty:
            return df
            
        # Convertir les dates
        df['date'] = pd.to_datetime(df['date'], utc=True)
        
        # Filtrer selon les seuils optimaux adaptatifs
        if config.use_optimal_thresholds:
            def is_above_threshold(row):
                wallet_data = wallet_thresholds.get(row['wallet_address'], {})
                optimal_threshold = wallet_data.get('optimal_threshold_tier', 0)
                
                # Convertir le seuil en USD (ex: 3K = 3000)
                threshold_usd = optimal_threshold * 1000 if optimal_threshold > 0 else 0
                
                return row['investment_usd'] >= threshold_usd
            
            # Appliquer le filtre
            mask = df.apply(is_above_threshold, axis=1)
            df = df[mask]
            
            print(f"🎯 Seuils adaptatifs appliqués: {len(df)} transactions qualifiées")
        
        # Ajouter les métadonnées des wallets
        wallet_data_list = []
        for _, row in df.iterrows():
            wallet_data = wallet_thresholds.get(row['wallet_address'], {})
            wallet_data_list.append({
                'optimal_threshold_tier': wallet_data.get('optimal_threshold_tier', 0),
                'quality_score': wallet_data.get('quality_score', 0.0),
                'threshold_status': wallet_data.get('threshold_status', 'UNKNOWN'),
                'optimal_roi': wallet_data.get('optimal_roi', 0.0),
                'optimal_winrate': wallet_data.get('optimal_winrate', 0.0),
                'global_roi': wallet_data.get('global_roi', 0.0),
                'global_winrate': wallet_data.get('global_winrate', 0.0)
            })
        
        # Ajouter les colonnes
        for key in wallet_data_list[0].keys() if wallet_data_list else []:
            df[key] = [w[key] for w in wallet_data_list]
        
        return df
        
    except Exception as e:
        print(f"❌ Erreur récupération transactions adaptatives: {e}")
        return pd.DataFrame()

def calculate_consensus_weight(whale_data):
    """Calcule le poids d'un wallet dans le consensus basé sur sa qualité"""
    if not config.quality_weighting:
        return 1.0
    
    quality = whale_data.get('quality_score', 0.0)
    optimal_roi = whale_data.get('optimal_roi', 0.0)
    optimal_winrate = whale_data.get('optimal_winrate', 0.0)
    
    # Pondération simplifiée mais plus différenciée
    # Base: qualité x2 pour amplifier les différences
    base_weight = (quality / 0.15) * 2  # 0.15 = qualité typique → poids 2.0
    
    # Bonus ROI: +50% si ROI > 300%
    roi_bonus = 1.5 if optimal_roi > 300 else 1.0
    
    # Bonus WinRate: +25% si WinRate > 50%
    winrate_bonus = 1.25 if optimal_winrate > 50 else 1.0
    
    # Score final
    final_weight = base_weight * roi_bonus * winrate_bonus
    
    return max(0.5, min(3.0, final_weight))  # Entre 0.5 et 3.0

def detect_adaptive_consensus_in_period(df_transactions, global_detected_tokens=None):
    """Détecte les consensus en utilisant la logique adaptative"""
    if df_transactions.empty:
        return []
    
    if global_detected_tokens is None:
        global_detected_tokens = set()
    
    consensus_detected = []
    processed_tokens = set()
    
    # Grouper par token
    for symbol, token_group in df_transactions.groupby('symbol'):
        if symbol in processed_tokens or symbol in global_detected_tokens:
            continue
            
        token_group = token_group.sort_values('date')
        
        # Analyser chaque transaction comme point de départ potentiel
        for idx, base_tx in token_group.iterrows():
            consensus_end = base_tx['date'] + timedelta(days=config.period_days)
            
            # Transactions dans la fenêtre de consensus
            window_txs = token_group[
                (token_group['date'] >= base_tx['date']) &
                (token_group['date'] <= consensus_end)
            ]
            
            # Analyser les wallets participants avec pondération
            whale_analysis = {}
            total_weighted_investment = 0
            
            for _, tx in window_txs.iterrows():
                wallet_addr = tx['wallet_address']
                
                if wallet_addr not in whale_analysis:
                    whale_analysis[wallet_addr] = {
                        'transactions': [],
                        'total_investment': 0,
                        'weight': calculate_consensus_weight(tx),
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
                total_weighted_investment += tx['investment_usd'] * whale_analysis[wallet_addr]['weight']
            
            # Calcul du consensus pondéré
            weighted_whale_count = sum(data['weight'] for data in whale_analysis.values())
            unique_whales = len(whale_analysis)
            
            # Critères de consensus adaptatifs
            min_weighted_threshold = config.min_whales_consensus * 0.3  # Flexible
            consensus_valid = (
                unique_whales >= config.min_whales_consensus and
                weighted_whale_count >= min_weighted_threshold
            )
            
            if consensus_valid:
                # Consensus détecté !
                consensus_txs = window_txs
                
                consensus_data = {
                    'symbol': symbol,
                    'contract_address': base_tx['contract_address'],
                    'detection_date': base_tx['date'],
                    'consensus_start': base_tx['date'],
                    'consensus_end': consensus_txs['date'].max(),
                    'whale_count': unique_whales,
                    'weighted_whale_count': weighted_whale_count,
                    'total_investment': sum(data['total_investment'] for data in whale_analysis.values()),
                    'weighted_investment': total_weighted_investment,
                    'avg_entry_price': (consensus_txs['investment_usd'] * consensus_txs['price_per_token']).sum() / consensus_txs['investment_usd'].sum(),
                    'transactions': consensus_txs,
                    'whale_details': []
                }
                
                # Détails des whales avec métriques adaptatives
                for wallet_addr, data in whale_analysis.items():
                    wallet_investment = data['total_investment']
                    wallet_data = data['wallet_data']
                    
                    consensus_data['whale_details'].append({
                        'address': wallet_addr,
                        'optimal_threshold_tier': wallet_data['optimal_threshold_tier'],
                        'quality_score': wallet_data['quality_score'],
                        'threshold_status': wallet_data['threshold_status'],
                        'optimal_roi': wallet_data['optimal_roi'],
                        'optimal_winrate': wallet_data['optimal_winrate'],
                        'consensus_weight': data['weight'],
                        'investment_usd': wallet_investment,
                        'transaction_count': len(data['transactions']),
                        'weighted_contribution': wallet_investment * data['weight']
                    })
                
                # Trier par contribution pondérée
                consensus_data['whale_details'].sort(
                    key=lambda x: x['weighted_contribution'], reverse=True
                )
                
                consensus_detected.append(consensus_data)
                processed_tokens.add(symbol)
                global_detected_tokens.add(symbol)
                break  # Prendre le premier consensus pour ce token dans cette période
    
    return consensus_detected

def calculate_performance(consensus_data):
    """Calcule la performance d'un consensus (identique à la version originale)"""
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
            'annualized_return': (performance_pct / days_held * 365) if days_held > 0 else 0
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

def run_adaptive_backtesting():
    """Lance le backtesting adaptatif basé sur les seuils optimaux"""
    
    print(f"🎯 BACKTESTING CONSENSUS ADAPTATIF")
    print("=" * 80)
    print(f"📅 Début: {config.start_date}")
    print(f"⏰ Période d'analyse: {config.period_days} jours")
    print(f"🐋 Consensus minimum: {config.min_whales_consensus} whales")
    print(f"🎯 Seuils adaptatifs: {config.use_optimal_thresholds}")
    print(f"⚖️ Pondération qualité: {config.quality_weighting}")
    print(f"📊 Qualité minimum: {config.quality_threshold}")
    print("=" * 80)
    
    # Charger les seuils optimaux
    print(f"📖 Chargement des seuils optimaux...")
    wallet_thresholds = get_smart_wallets_thresholds()
    
    if not wallet_thresholds:
        print("❌ Aucun wallet avec seuil optimal trouvé")
        return [], []
    
    print(f"✅ {len(wallet_thresholds)} wallets avec seuils optimaux chargés")
    
    # Afficher quelques exemples
    print(f"\n📋 Exemples de seuils optimaux:")
    for i, (wallet, data) in enumerate(list(wallet_thresholds.items())[:5]):
        print(f"   {wallet[:10]}...{wallet[-8:]}: "
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
        
        print(f"\n📊 PÉRIODE {period_number}: {current_date.strftime('%Y-%m-%d')} → {period_end.strftime('%Y-%m-%d')}")
        print("-" * 60)
        
        # Récupérer les transactions avec seuils adaptatifs
        df_transactions = get_transactions_in_period_adaptive(current_date, period_end, wallet_thresholds)
        
        if df_transactions.empty:
            print("❌ Aucune transaction qualifiée dans cette période")
            period_results.append({
                'period_number': period_number,
                'period_start': current_date,
                'period_end': period_end,
                'transactions_count': 0,
                'consensus_count': 0,
                'consensus_detected': []
            })
        else:
            print(f"📈 {len(df_transactions)} transactions qualifiées trouvées")
            print(f"🐋 {df_transactions['wallet_address'].nunique()} wallets actifs")
            print(f"🪙 {df_transactions['symbol'].nunique()} tokens uniques")
            
            # Détecter les consensus adaptatifs
            consensus_detected = detect_adaptive_consensus_in_period(df_transactions, detected_tokens)
            
            if consensus_detected:
                print(f"✅ {len(consensus_detected)} consensus adaptatifs détectés:")
                for cons in consensus_detected:
                    print(f"   • {cons['symbol']}: {cons['whale_count']} whales "
                          f"(poids: {cons['weighted_whale_count']:.1f}), "
                          f"${cons['total_investment']:,.0f}")
                    print(f"     📅 Détecté le: {cons['detection_date'].strftime('%Y-%m-%d %H:%M')}")
                    print(f"     🐋 Whales participantes (par contribution pondérée):")
                    for whale in cons['whale_details']:
                        print(f"        [{whale['threshold_status']}] "
                              f"Seuil {whale['optimal_threshold_tier']}K | "
                              f"Q={whale['quality_score']:.3f} | "
                              f"Poids={whale['consensus_weight']:.2f} | "
                              f"ROI {whale['optimal_roi']:+.1f}% | "
                              f"${whale['investment_usd']:,.0f} | "
                              f"{whale['address'][:10]}...{whale['address'][-8:]}")
                    print()
            else:
                print("❌ Aucun consensus adaptatif détecté")
            
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
    
    print(f"\n🎯 RÉSUMÉ GLOBAL:")
    print("=" * 80)
    print(f"📊 {period_number-1} périodes analysées")
    print(f"🚀 {len(all_consensus)} consensus adaptatifs détectés au total")
    
    if all_consensus:
        # Calculer les performances
        print(f"\n💹 CALCUL DES PERFORMANCES")
        print("-" * 50)
        
        for consensus in all_consensus:
            perf = calculate_performance(consensus)
            consensus['performance'] = perf
            
            if perf['performance_pct'] is not None:
                print(f"{perf['status']} {perf['symbol']}: "
                      f"${perf['entry_price']:.8f} → ${perf['current_price']:.8f} "
                      f"({perf['performance_pct']:+.1f}% | {perf['days_held']}j) "
                      f"[Whales: {consensus['whale_count']}, Poids: {consensus['weighted_whale_count']:.1f}]")
            else:
                print(f"{perf['status']} {perf['symbol']}: ${perf['entry_price']:.8f}")
            
            time.sleep(config.price_check_delay)
    
    return all_consensus, period_results

def export_adaptive_results(all_consensus, period_results):
    """Exporte les résultats adaptatifs vers JSON"""
    
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
            'average_performance': sum(performances) / len(performances),
            'positive_count': sum(1 for p in performances if p > 0),
            'success_rate': sum(1 for p in performances if p > 0) / len(performances) * 100,
            'best_performance': max(performances),
            'worst_performance': min(performances),
            'median_performance': sorted(performances)[len(performances)//2],
            'total_investment': sum(c['total_investment'] for c in all_consensus),
            'total_weighted_investment': sum(c['weighted_investment'] for c in all_consensus)
        }
    
    # Préparer les données d'export
    export_data = {
        'metadata': {
            'timestamp': datetime.now().isoformat(),
            'config': config.to_dict(),
            'periods_analyzed': len(period_results),
            'date_range': {
                'start': config.start_date,
                'end': datetime.now().strftime('%Y-%m-%d')
            },
            'adaptive_features': {
                'optimal_thresholds_used': config.use_optimal_thresholds,
                'quality_weighting_used': config.quality_weighting,
                'quality_threshold': config.quality_threshold
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
            'period_start': period['period_start'].isoformat(),
            'period_end': period['period_end'].isoformat(),
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
            'detection_date': consensus['detection_date'].isoformat(),
            'consensus_period': {
                'start': consensus['consensus_start'].isoformat(),
                'end': consensus['consensus_end'].isoformat(),
                'duration_hours': (consensus['consensus_end'] - consensus['consensus_start']).total_seconds() / 3600
            },
            'whale_count': consensus['whale_count'],
            'weighted_whale_count': consensus['weighted_whale_count'],
            'total_investment': consensus['total_investment'],
            'weighted_investment': consensus['weighted_investment'],
            'avg_entry_price': consensus['avg_entry_price'],
            'whale_details': consensus['whale_details'],
            'performance': consensus.get('performance', {})
        }
        export_data['all_consensus'].append(consensus_data)
    
    # Trier par performance
    export_data['all_consensus'].sort(
        key=lambda x: x['performance'].get('performance_pct') or -999,
        reverse=True
    )
    
    # Sauvegarder
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = OUTPUT_DIR / f"consensus_adaptive_{timestamp}.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n✅ Résultats adaptatifs exportés: {output_file}")
    
    # Afficher les statistiques
    if stats:
        print(f"\n📊 STATISTIQUES FINALES ADAPTATIVES:")
        print(f"   • Consensus détectés: {stats['total_consensus']}")
        print(f"   • Performances mesurables: {stats['measurable_performances']}")
        print(f"   • Performance moyenne: {stats['average_performance']:+.1f}%")
        print(f"   • Taux de succès: {stats['success_rate']:.1f}%")
        print(f"   • Meilleure performance: {stats['best_performance']:+.1f}%")
        print(f"   • Investment total: ${stats['total_investment']:,.0f}")
        print(f"   • Investment pondéré: ${stats['total_weighted_investment']:,.0f}")
    
    return export_data

# =============================================================================
# EXÉCUTION PRINCIPALE
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Backtesting Consensus Adaptatif')
    
    # Paramètres adaptatifs
    parser.add_argument('--min-whales', type=int, default=1, 
                       help='Nombre minimum de whales pour consensus')
    parser.add_argument('--quality-threshold', type=float, default=0.1,
                       help='Qualité minimum requise (q_w)')
    parser.add_argument('--disable-thresholds', action='store_true',
                       help='Désactiver les seuils optimaux adaptatifs')
    parser.add_argument('--disable-weighting', action='store_true',
                       help='Désactiver la pondération par qualité')
    
    # Paramètres temporels
    parser.add_argument('--start-date', default="2025-07-01",
                       help='Date de début (YYYY-MM-DD)')
    parser.add_argument('--period-days', type=int, default=5,
                       help='Période d\'analyse en jours')
    
    args = parser.parse_args()
    
    # Appliquer les paramètres
    config.min_whales_consensus = args.min_whales
    config.quality_threshold = args.quality_threshold
    config.use_optimal_thresholds = not args.disable_thresholds
    config.quality_weighting = not args.disable_weighting
    config.start_date = args.start_date
    config.period_days = args.period_days
    
    # Lancer le backtesting adaptatif
    all_consensus, period_results = run_adaptive_backtesting()
    
    if all_consensus:
        export_adaptive_results(all_consensus, period_results)
        print(f"\n🎉 BACKTESTING ADAPTATIF TERMINÉ AVEC SUCCÈS!")
    else:
        print(f"\n❌ Aucun consensus adaptatif détecté avec ces paramètres")

if __name__ == "__main__":
    main()