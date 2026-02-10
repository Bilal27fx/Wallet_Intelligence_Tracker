#!/usr/bin/env python3
"""
Algorithme de Consensus LIVE - Détection en temps réel sur 5 jours
Reprend la logique du backtesting simple mais en mode live
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

# =============================================================================
# CONFIGURATION LIVE
# =============================================================================

class LiveConsensusConfig:
    """Configuration pour le consensus live"""
    
    def __init__(self):
        # === PARAMÈTRES DE CONSENSUS ===
        self.min_whales_consensus = 2         # Nombre minimum de whales pour consensus
        self.period_days = 5                  # Période d'analyse (5 jours)
        
        # === FILTRES MARKET CAP ===
        self.max_market_cap = 100_000_000     # Market cap maximum ($100M) - on veut les petites caps
        self.min_market_cap = 100_000         # Market cap minimum ($100K) - éviter les micro caps
        
        # === FILTRES ===
        self.excluded_tokens = {               # Tokens à exclure
            'USDC', 'USDT', 'DAI', 'BUSD', 'ETH', 'WETH', 'BTC', 'BITCOIN', 'BNB', 'ETHEREUM'
        }
        
        # === PERFORMANCE ===
        self.price_check_delay = 0.5          # Délai entre les appels API prix
        self.update_interval_hours = 6        # Intervalle de mise à jour (6 heures)
        
    def to_dict(self):
        """Convertit la config en dictionnaire"""
        return {
            'min_whales_consensus': self.min_whales_consensus,
            'period_days': self.period_days,
            'min_market_cap': self.min_market_cap,
            'max_market_cap': self.max_market_cap,
            'excluded_tokens': list(self.excluded_tokens),
            'update_interval_hours': self.update_interval_hours
        }

# Configuration globale
config = LiveConsensusConfig()

# Chemins
ROOT_DIR = Path(__file__).parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "db" / "wit_database.db"
OUTPUT_DIR = ROOT_DIR / "data" / "consensus_live"

# =============================================================================
# FONCTIONS UTILITAIRES (ADAPTÉES DU BACKTESTING)
# =============================================================================

def init_output_directory():
    """Initialise le dossier de sortie"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR

def get_token_info_dexscreener(contract_address, retries=2):
    """Récupère les informations complètes d'un token via DexScreener"""
    for attempt in range(retries):
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{contract_address}"
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            pairs = data.get("pairs", [])
            
            if pairs:
                # Prendre la paire avec le plus gros volume 24h
                best_pair = max(pairs, key=lambda x: float(x.get("volume", {}).get("h24", 0) or 0))
                
                # Extraire toutes les informations
                token_info = {
                    'price_usd': float(best_pair.get("priceUsd", 0)),
                    'market_cap': float(best_pair.get("marketCap", 0)),
                    'fdv': float(best_pair.get("fdv", 0)),  # Fully Diluted Valuation
                    'volume_24h': float(best_pair.get("volume", {}).get("h24", 0)),
                    'volume_6h': float(best_pair.get("volume", {}).get("h6", 0)),
                    'volume_1h': float(best_pair.get("volume", {}).get("h1", 0)),
                    'price_change_24h': float(best_pair.get("priceChange", {}).get("h24", 0)),
                    'price_change_6h': float(best_pair.get("priceChange", {}).get("h6", 0)),
                    'price_change_1h': float(best_pair.get("priceChange", {}).get("h1", 0)),
                    'liquidity_usd': float(best_pair.get("liquidity", {}).get("usd", 0)),
                    'pair_address': best_pair.get("pairAddress", ""),
                    'dex_id': best_pair.get("dexId", ""),
                    'chain_id': best_pair.get("chainId", ""),
                    'base_token': {
                        'address': best_pair.get("baseToken", {}).get("address", ""),
                        'name': best_pair.get("baseToken", {}).get("name", ""),
                        'symbol': best_pair.get("baseToken", {}).get("symbol", "")
                    },
                    'pair_created_at': best_pair.get("pairCreatedAt", 0),
                    'txns_24h_buys': best_pair.get("txns", {}).get("h24", {}).get("buys", 0),
                    'txns_24h_sells': best_pair.get("txns", {}).get("h24", {}).get("sells", 0),
                    'txns_6h_buys': best_pair.get("txns", {}).get("h6", {}).get("buys", 0),
                    'txns_6h_sells': best_pair.get("txns", {}).get("h6", {}).get("sells", 0),
                    'website': best_pair.get("info", {}).get("websites", [{}])[0].get("url", "") if best_pair.get("info", {}).get("websites") else "",
                    'twitter': best_pair.get("info", {}).get("socials", [{}])[0].get("url", "") if best_pair.get("info", {}).get("socials") else ""
                }
                
                return token_info
            
            return None
            
        except Exception as e:
            if attempt == retries - 1:
                print(f"⚠️ Erreur récupération infos pour {contract_address}: {e}")
                return None
            time.sleep(1)
    
    return None

def get_current_price_dexscreener(contract_address, retries=2):
    """Récupère le prix actuel via DexScreener avec retry (version simplifiée)"""
    token_info = get_token_info_dexscreener(contract_address, retries)
    if token_info:
        return token_info['price_usd'] if token_info['price_usd'] > 0 else None
    return None

def get_token_historical_data(contract_address, retries=2):
    """Récupère les données historiques 30j et 7j via DexScreener"""
    for attempt in range(retries):
        try:
            # DexScreener ne fournit pas directement l'historique 30j/7j
            # On utilise les variations 24h comme approximation et on calcule les tendances
            token_info = get_token_info_dexscreener(contract_address, retries=1)
            
            if token_info:
                # Simuler les données historiques basées sur les variations actuelles
                price_24h_change = token_info['price_change_24h']
                current_price = token_info['price_usd']
                
                # Estimation du prix il y a 24h
                price_24h_ago = current_price / (1 + price_24h_change / 100) if price_24h_change != 0 else current_price
                
                # Approximations pour 7j et 30j (basées sur la tendance 24h)
                # Note: Ces sont des estimations, DexScreener ne fournit pas l'historique complet
                estimated_7d_change = price_24h_change * 3  # Approximation grossière
                estimated_30d_change = price_24h_change * 10  # Approximation grossière
                
                return {
                    'price_current': current_price,
                    'price_24h_ago': price_24h_ago,
                    'change_24h': price_24h_change,
                    'change_7d_estimated': estimated_7d_change,
                    'change_30d_estimated': estimated_30d_change,
                    'volume_24h': token_info['volume_24h'],
                    'market_cap': token_info['market_cap']
                }
            
            return None
            
        except Exception as e:
            if attempt == retries - 1:
                print(f"⚠️ Erreur données historiques pour {contract_address}: {e}")
                return None
            time.sleep(1)
    
    return None

def get_smart_wallets():
    """Récupère les wallets qualifiés depuis smart_wallets"""
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
        print(f"❌ Erreur récupération smart wallets: {e}")
        return {}

def get_recent_transactions_live(smart_wallets):
    """Récupère les transactions des 5 derniers jours"""
    try:
        # Calculer la date de début (5 jours en arrière)
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=config.period_days)
        
        conn = sqlite3.connect(DB_PATH)
        
        # Récupérer toutes les transactions récentes des wallets qualifiés
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
                th.action_type
            FROM transaction_history th
            WHERE th.date BETWEEN ? AND ?
            AND th.action_type IN ('buy', 'receive')
            AND th.quantity > 0
            AND th.symbol NOT IN ({})
            AND th.wallet_address IN ({})
            ORDER BY th.date DESC
        """.format(
            ','.join(['?' for _ in config.excluded_tokens]),
            ','.join(['?' for _ in smart_wallets.keys()])
        )
        
        params = [
            start_date.isoformat(), 
            end_date.isoformat()
        ] + list(config.excluded_tokens) + list(smart_wallets.keys())
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        if df.empty:
            return df
            
        # Convertir les dates avec format flexible
        df['date'] = pd.to_datetime(df['date'], utc=True, format='mixed')
        
        # Ajouter les métadonnées des wallets
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
        
        print(f"🎯 Seuils appliqués: {len(qualified_pairs)} wallet/token qualifiés sur {len(df_grouped)} combinaisons")
        
        # Filtrer les transactions originales
        if qualified_pairs:
            mask_qualified = df.apply(
                lambda row: (row['wallet_address'], row['symbol']) in qualified_pairs, axis=1
            )
            df = df[mask_qualified]
        else:
            df = pd.DataFrame()
        
        return df
        
    except Exception as e:
        print(f"❌ Erreur récupération transactions live: {e}")
        return pd.DataFrame()

def get_existing_consensus_from_db():
    """Récupère les consensus déjà détectés depuis la BDD"""
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
        
        # Retourner un set des (symbol, contract_address) déjà détectés
        existing = set()
        for _, row in df.iterrows():
            symbol = row['symbol']
            contract_address = row['contract_address']
            existing.add((symbol, contract_address))
            
        print(f"🔍 Consensus récupérés de la BDD: {len(existing)} entrées")
        return existing
        
    except Exception as e:
        print(f"⚠️ Erreur lecture consensus existants: {e}")
        return set()

def detect_live_consensus(df_transactions):
    """Détecte les consensus actuels dans les 5 derniers jours avec filtrage market cap"""
    if df_transactions.empty:
        return []
    
    # Récupérer les consensus déjà détectés
    existing_consensus = get_existing_consensus_from_db()
    print(f"📋 {len(existing_consensus)} consensus déjà en BDD (ignorés)")
    if existing_consensus:
        print(f"🔍 Consensus existants: {list(existing_consensus)[:3]}...")  # Afficher les 3 premiers pour debug
    
    signals_detected = []
    processed_tokens = set()
    
    total_tokens = df_transactions['symbol'].nunique()
    print(f"🔍 Analyse de {total_tokens} tokens (market cap: ${config.min_market_cap:,.0f} - ${config.max_market_cap:,.0f})...")
    
    # Grouper par token
    for symbol, token_group in df_transactions.groupby('symbol'):
        if symbol in processed_tokens:
            continue
            
        token_group = token_group.sort_values('date')
        contract_address = token_group['contract_address'].iloc[0]
        
        # Vérifier si ce consensus existe déjà dans la BDD
        consensus_key = (symbol, contract_address)
        if consensus_key in existing_consensus:
            print(f"⏭️ Consensus déjà détecté pour {symbol} ({contract_address[:10]}...), passage au suivant")
            processed_tokens.add(symbol)
            continue
        else:
            print(f"🆕 Nouveau token à analyser: {symbol} ({contract_address[:10]}...)")
        
        # ÉTAPE 1: Vérifier le market cap via DexScreener (silencieux)
        token_info = get_token_info_dexscreener(contract_address)
        
        if not token_info:
            continue
        
        market_cap = token_info['market_cap']
        
        # Filtrer par market cap (silencieux)
        if market_cap < config.min_market_cap or market_cap > config.max_market_cap:
            continue
        
        # ÉTAPE 2: Récupérer les données historiques
        historical_data = get_token_historical_data(contract_address)
        
        # ÉTAPE 3: Analyser toute la période des 5 jours pour ce token
        whale_analysis = {}
        exceptional_whales = 0
        normal_whales = 0
        
        # Grouper et sommer les investissements par wallet
        wallet_sums = token_group.groupby('wallet_address').agg({
            'investment_usd': 'sum',
            'optimal_threshold_tier': 'first',
            'quality_score': 'first', 
            'threshold_status': 'first',
            'optimal_roi': 'first',
            'optimal_winrate': 'first'
        })
        
        # Vérifier quels wallets dépassent leur seuil optimal
        qualified_wallets = set()
        for wallet_addr, wallet_data in wallet_sums.iterrows():
            threshold_usd = wallet_data['optimal_threshold_tier'] * 1000
            if wallet_data['investment_usd'] >= threshold_usd:
                qualified_wallets.add(wallet_addr)
        
        # Analyser seulement les wallets qualifiés
        for _, tx in token_group.iterrows():
            wallet_addr = tx['wallet_address']
            
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
                
                # Compter les types de wallets (une fois par wallet)
                if tx['threshold_status'] == 'EXCEPTIONAL':
                    exceptional_whales += 1
                else:
                    normal_whales += 1
            
            whale_analysis[wallet_addr]['transactions'].append(tx)
            whale_analysis[wallet_addr]['total_investment'] += tx['investment_usd']
        
        # LOGIQUE DE DÉTECTION CONSENSUS
        unique_whales = len(whale_analysis)
        signal_valid = False
        signal_type = ""
        
        # Consensus ≥2 wallets
        if unique_whales >= config.min_whales_consensus:
            signal_valid = True
            if exceptional_whales >= 1 and normal_whales >= 1:
                signal_type = "MIXED_CONSENSUS"
            elif exceptional_whales >= 2:
                signal_type = "EXCEPTIONAL_CONSENSUS"
            else:
                signal_type = "NORMAL_CONSENSUS"
        
        if signal_valid:
            # CONSENSUS DÉTECTÉ !
            print(f"🎯 CONSENSUS DÉTECTÉ: {symbol} - {unique_whales} wallets ({signal_type})")
            
            # La date de détection est la date de formation du consensus (dernière transaction qui forme le consensus)
            consensus_formation_date = token_group['date'].max()
            
            signal_data = {
                'symbol': symbol,
                'contract_address': contract_address,
                'detection_date': consensus_formation_date,  # Date de formation du consensus
                'period_start': token_group['date'].min(),
                'period_end': token_group['date'].max(),
                'whale_count': unique_whales,
                'exceptional_count': exceptional_whales,
                'normal_count': normal_whales,
                'signal_type': signal_type,
                'total_investment': sum(data['total_investment'] for data in whale_analysis.values()),
                'avg_entry_price': (token_group['investment_usd'] * token_group['price_per_token']).sum() / token_group['investment_usd'].sum(),
                'transactions': token_group,
                'whale_details': [],
                # NOUVELLES DONNÉES ENRICHIES
                'token_info': token_info,
                'historical_data': historical_data
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
                    'transaction_count': len(data['transactions']),
                    'first_buy_date': min(tx['date'] for tx in data['transactions']),
                    'last_buy_date': max(tx['date'] for tx in data['transactions'])
                })
            
            # Trier par type de wallet puis par investissement
            signal_data['whale_details'].sort(
                key=lambda x: (x['threshold_status'] != 'EXCEPTIONAL', -x['investment_usd'])
            )
            
            signals_detected.append(signal_data)
            processed_tokens.add(symbol)
            
        # Délai pour éviter le rate limiting de DexScreener
        time.sleep(config.price_check_delay)
    
    return signals_detected

def calculate_live_performance(consensus_data):
    """Calcule la performance actuelle d'un consensus"""
    symbol = consensus_data['symbol']
    contract_address = consensus_data['contract_address']
    avg_entry_price = consensus_data['avg_entry_price']
    consensus_formation_date = consensus_data['detection_date']  # Date de formation du consensus
    
    if not contract_address or avg_entry_price <= 0:
        return {
            'symbol': symbol,
            'entry_price': avg_entry_price,
            'current_price': None,
            'performance_pct': None,
            'days_held': (datetime.now(timezone.utc) - consensus_formation_date).days,
            'status': 'DONNÉES_INSUFFISANTES'
        }
    
    # Récupérer le prix actuel
    current_price = get_current_price_dexscreener(contract_address)
    
    if current_price:
        performance_pct = ((current_price - avg_entry_price) / avg_entry_price) * 100
        days_held = (datetime.now(timezone.utc) - consensus_formation_date).days
        
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
            'days_held': (datetime.now(timezone.utc) - consensus_formation_date).days,
            'status': 'PRIX_NON_DISPONIBLE'
        }

def save_live_consensus_to_db(consensus_signals):
    """Sauvegarde les signaux de consensus live dans la base de données"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # La table consensus_live existe déjà avec son schéma
        
        # Nettoyer les anciens signaux (>7 jours)
        cursor.execute("""
            DELETE FROM consensus_live 
            WHERE detection_date < datetime('now', '-7 days')
        """)
        
        # Insérer les nouveaux signaux (utiliser les colonnes existantes)
        for signal in consensus_signals:
            perf = signal.get('performance', {})
            token_info = signal.get('token_info', {})
            
            cursor.execute("""
                INSERT OR REPLACE INTO consensus_live (
                    symbol, contract_address, whale_count, total_investment,
                    first_buy, last_buy, detection_date, period_start, period_end,
                    price_usd, market_cap_circulating, volume_24h, price_change_24h,
                    liquidity_usd, transactions_24h_buys, transactions_24h_sells
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal['symbol'],
                signal['contract_address'],
                signal['whale_count'],
                signal['total_investment'],
                signal['period_start'].isoformat() if hasattr(signal['period_start'], 'isoformat') else str(signal['period_start']),  # first_buy
                signal['period_end'].isoformat() if hasattr(signal['period_end'], 'isoformat') else str(signal['period_end']),    # last_buy
                signal['detection_date'].isoformat() if hasattr(signal['detection_date'], 'isoformat') else str(signal['detection_date']),
                signal['period_start'].isoformat() if hasattr(signal['period_start'], 'isoformat') else str(signal['period_start']),
                signal['period_end'].isoformat() if hasattr(signal['period_end'], 'isoformat') else str(signal['period_end']),
                perf.get('current_price'),
                token_info.get('market_cap', 0),
                token_info.get('volume_24h', 0),
                token_info.get('price_change_24h', 0),
                token_info.get('liquidity_usd', 0),
                token_info.get('txns_24h_buys', 0),
                token_info.get('txns_24h_sells', 0)
            ))
        
        conn.commit()
        conn.close()
        
        print(f"✅ {len(consensus_signals)} signaux sauvegardés dans consensus_live")
        
    except Exception as e:
        print(f"❌ Erreur sauvegarde consensus live: {e}")

def run_live_consensus_detection():
    """Lance la détection de consensus en temps réel"""
    
    print(f"🔴 CONSENSUS LIVE DETECTOR")
    print("=" * 80)
    print(f"⏰ Analyse des {config.period_days} derniers jours")
    print(f"🐋 Consensus minimum: ≥{config.min_whales_consensus} wallets")
    print(f"📊 Seuils optimaux: OUI (avec sommation)")
    print("=" * 80)
    
    # Charger les smart wallets
    smart_wallets = get_smart_wallets()
    
    if not smart_wallets:
        print("❌ Aucun smart wallet trouvé")
        return []
    
    print(f"✅ {len(smart_wallets)} smart wallets chargés")
    
    # Récupérer les transactions récentes
    print(f"\n🔄 Récupération des transactions des {config.period_days} derniers jours...")
    df_transactions = get_recent_transactions_live(smart_wallets)
    
    if df_transactions.empty:
        print("❌ Aucune transaction qualifiée trouvée")
        return []
    
    print(f"📈 {len(df_transactions)} transactions qualifiées trouvées")
    print(f"🐋 {df_transactions['wallet_address'].nunique()} wallets actifs")
    print(f"🪙 {df_transactions['symbol'].nunique()} tokens uniques")
    
    # Détecter les consensus
    print(f"\n🔍 Détection des consensus...")
    consensus_signals = detect_live_consensus(df_transactions)
    
    if not consensus_signals:
        analyzed_tokens = df_transactions['symbol'].nunique()
        print(f"❌ Aucun consensus détecté sur {analyzed_tokens} tokens analysés")
        return []
    
    print(f"✅ {len(consensus_signals)} consensus LIVE détectés:")
    
    # Calculer les performances et afficher
    for signal in consensus_signals:
        perf = calculate_live_performance(signal)
        signal['performance'] = perf
        
        # Données enrichies
        token_info = signal.get('token_info', {})
        historical_data = signal.get('historical_data', {})
        
        # Emoji selon le type de signal
        type_emoji = {
            'EXCEPTIONAL_CONSENSUS': '🌟', 
            'NORMAL_CONSENSUS': '🐋',
            'MIXED_CONSENSUS': '🎯'
        }
        emoji = type_emoji.get(signal['signal_type'], '🔍')
        
        print(f"\n{emoji} {signal['symbol']} ({signal['signal_type']})")
        print(f"   🐋 {signal['whale_count']} whales ({signal['exceptional_count']} exceptionnels + {signal['normal_count']} normaux)")
        print(f"   💰 ${signal['total_investment']:,.0f} investis")
        print(f"   📅 Période: {signal['period_start'].strftime('%m-%d %H:%M')} → {signal['period_end'].strftime('%m-%d %H:%M')}")
        
        # INFORMATIONS DEXSCREENER ENRICHIES
        if token_info:
            market_cap = token_info.get('market_cap', 0)
            volume_24h = token_info.get('volume_24h', 0)
            price_change_24h = token_info.get('price_change_24h', 0)
            liquidity = token_info.get('liquidity_usd', 0)
            
            print(f"   📊 Market Cap: ${market_cap:,.0f} | Volume 24h: ${volume_24h:,.0f}")
            print(f"   🔄 Variation 24h: {price_change_24h:+.1f}% | Liquidité: ${liquidity:,.0f}")
            
            # Transactions 24h
            buys_24h = token_info.get('txns_24h_buys', 0)
            sells_24h = token_info.get('txns_24h_sells', 0)
            if buys_24h > 0 or sells_24h > 0:
                total_txns = buys_24h + sells_24h
                buy_ratio = (buys_24h / total_txns * 100) if total_txns > 0 else 0
                print(f"   📈 Txns 24h: {buys_24h} achats | {sells_24h} ventes | Ratio achat: {buy_ratio:.1f}%")
        
        # DONNÉES HISTORIQUES (ESTIMÉES)
        if historical_data:
            change_7d = historical_data.get('change_7d_estimated', 0)
            change_30d = historical_data.get('change_30d_estimated', 0)
            print(f"   📈 Estimé 7j: {change_7d:+.1f}% | 30j: {change_30d:+.1f}%")
        
        # PERFORMANCE ACTUELLE
        if perf['performance_pct'] is not None:
            print(f"   💹 Performance consensus: {perf['performance_pct']:+.1f}% ({perf['days_held']}j) - {perf['status']}")
            print(f"   💵 Prix: ${perf['entry_price']:.8f} → ${perf['current_price']:.8f}")
        else:
            print(f"   ⚠️ {perf['status']}")
        
        # WALLETS PARTICIPANTS
        print(f"   🐋 Wallets participants:")
        for whale in signal['whale_details'][:5]:  # Top 5 seulement
            status_emoji = '⭐' if whale['threshold_status'] == 'EXCEPTIONAL' else '🔷'
            print(f"      {status_emoji} {whale['address'][:10]}...{whale['address'][-8:]} | "
                  f"Seuil {whale['optimal_threshold_tier']}K | "
                  f"${whale['investment_usd']:,.0f} | "
                  f"{whale['first_buy_date'].strftime('%m-%d %H:%M')}")
        
        time.sleep(config.price_check_delay)
    
    # Sauvegarder en base
    save_live_consensus_to_db(consensus_signals)
    
    return consensus_signals

def export_live_results(consensus_signals):
    """Exporte les résultats live vers JSON"""
    
    init_output_directory()
    
    # Préparer les données d'export
    export_data = {
        'metadata': {
            'timestamp': datetime.now().isoformat(),
            'detection_type': 'live_consensus',
            'period_days': config.period_days,
            'config': config.to_dict()
        },
        'summary': {
            'total_signals': len(consensus_signals),
            'exceptional_signals': sum(1 for s in consensus_signals if s['signal_type'] == 'EXCEPTIONAL_CONSENSUS'),
            'mixed_signals': sum(1 for s in consensus_signals if s['signal_type'] == 'MIXED_CONSENSUS'),
            'normal_signals': sum(1 for s in consensus_signals if s['signal_type'] == 'NORMAL_CONSENSUS'),
            'total_investment': sum(s['total_investment'] for s in consensus_signals)
        },
        'live_signals': []
    }
    
    # Convertir tous les signaux
    for signal in consensus_signals:
        token_info = signal.get('token_info', {})
        historical_data = signal.get('historical_data', {})
        
        signal_data = {
            'symbol': signal['symbol'],
            'contract_address': signal['contract_address'],
            'detection_date': signal['detection_date'].isoformat(),
            'period': {
                'start': signal['period_start'].isoformat(),
                'end': signal['period_end'].isoformat(),
                'duration_hours': (signal['period_end'] - signal['period_start']).total_seconds() / 3600
            },
            'consensus': {
                'type': signal['signal_type'],
                'whale_count': signal['whale_count'],
                'exceptional_count': signal['exceptional_count'],
                'normal_count': signal['normal_count']
            },
            'investment': {
                'total_usd': signal['total_investment'],
                'avg_entry_price': signal['avg_entry_price']
            },
            'performance': signal.get('performance', {}),
            'whale_details': signal['whale_details'],
            # NOUVELLES DONNÉES DEXSCREENER
            'token_metrics': {
                'market_cap': token_info.get('market_cap', 0),
                'fdv': token_info.get('fdv', 0),
                'volume_24h': token_info.get('volume_24h', 0),
                'volume_6h': token_info.get('volume_6h', 0),
                'volume_1h': token_info.get('volume_1h', 0),
                'price_change_24h': token_info.get('price_change_24h', 0),
                'price_change_6h': token_info.get('price_change_6h', 0),
                'price_change_1h': token_info.get('price_change_1h', 0),
                'liquidity_usd': token_info.get('liquidity_usd', 0),
                'pair_address': token_info.get('pair_address', ''),
                'dex_id': token_info.get('dex_id', ''),
                'chain_id': token_info.get('chain_id', '')
            },
            'trading_activity': {
                'txns_24h_buys': token_info.get('txns_24h_buys', 0),
                'txns_24h_sells': token_info.get('txns_24h_sells', 0),
                'txns_6h_buys': token_info.get('txns_6h_buys', 0),
                'txns_6h_sells': token_info.get('txns_6h_sells', 0),
                'buy_sell_ratio_24h': (token_info.get('txns_24h_buys', 0) / max(token_info.get('txns_24h_buys', 0) + token_info.get('txns_24h_sells', 0), 1)) * 100
            },
            'historical_estimates': {
                'change_7d_estimated': historical_data.get('change_7d_estimated', 0),
                'change_30d_estimated': historical_data.get('change_30d_estimated', 0),
                'price_24h_ago': historical_data.get('price_24h_ago', 0)
            },
            'token_info': {
                'name': token_info.get('base_token', {}).get('name', ''),
                'website': token_info.get('website', ''),
                'twitter': token_info.get('twitter', ''),
                'pair_created_at': token_info.get('pair_created_at', 0)
            }
        }
        export_data['live_signals'].append(signal_data)
    
    # Trier par performance
    export_data['live_signals'].sort(
        key=lambda x: x['performance'].get('performance_pct') or -999,
        reverse=True
    )
    
    # Sauvegarder
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = OUTPUT_DIR / f"consensus_live_{timestamp}.json"
    
    # Export JSON désactivé
    return export_data

def get_active_live_consensus():
    """Récupère les consensus actifs depuis la base de données"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        query = """
            SELECT *
            FROM consensus_live
            WHERE is_active = TRUE
            AND detection_date >= datetime('now', '-7 days')
            ORDER BY performance_pct DESC NULLS LAST, detection_date DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
        
    except Exception as e:
        print(f"❌ Erreur récupération consensus actifs: {e}")
        return pd.DataFrame()

# =============================================================================
# EXÉCUTION PRINCIPALE
# =============================================================================

def main():
    """Point d'entrée principal"""
    print(f"🚀 Lancement du Consensus Live Detector")
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Lancer la détection
    consensus_signals = run_live_consensus_detection()
    
    if consensus_signals:
        print(f"\n🎉 Détection terminée avec succès!")
        print(f"📊 {len(consensus_signals)} consensus actifs détectés")
        
        # Afficher résumé
        positive_signals = [s for s in consensus_signals 
                          if s.get('performance', {}).get('performance_pct', 0) > 0]
        if positive_signals:
            avg_perf = sum(s['performance']['performance_pct'] for s in positive_signals) / len(positive_signals)
            print(f"📈 {len(positive_signals)}/{len(consensus_signals)} signaux positifs (moyenne: {avg_perf:+.1f}%)")
    else:
        print(f"\n💤 Aucun consensus actif détecté pour le moment")
        print(f"🔄 Prochaine vérification dans {config.update_interval_hours}h")

if __name__ == "__main__":
    main()