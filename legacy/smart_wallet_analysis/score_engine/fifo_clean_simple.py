#!/usr/bin/env python3
"""Algorithme FIFO simple pour métriques token par wallet."""

import sqlite3
import requests
import time
from typing import Dict, List, Tuple, Optional
from datetime import datetime

from smart_wallet_analysis.config import DB_PATH, SCORE_ENGINE
from smart_wallet_analysis.logger import get_logger

logger = get_logger("score_engine.fifo")

_FIFO = SCORE_ENGINE["FIFO"]

class SimpleFIFOAnalyzer:
    def __init__(self):
        self.db_path = DB_PATH
    
    def _is_stablecoin(self, symbol: str) -> bool:
        """Détecte les stablecoins."""
        return symbol.upper() in _FIFO["STABLECOINS"] or symbol.upper().startswith(_FIFO["USD_PREFIX"])
    
    def _get_current_price(self, contract_address: str, symbol: str) -> float:
        """Prix actuel DexScreener ou $1 pour stablecoins."""
        if self._is_stablecoin(symbol):
            return 1.0
        
        if symbol.upper() in _FIFO["ETH_SYMBOLS"]:
            try:
                url = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd"
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    return float(data.get('ethereum', {}).get('usd', 0))
            except:
                return _FIFO["ETH_FALLBACK_PRICE"]
        
        if not contract_address or contract_address.lower() == 'none':
            return 0.0
        
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{contract_address}"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 429:
                return 0.0  # Skip sur rate limit
            
            if response.status_code == 200:
                data = response.json()
                pairs = data.get('pairs', [])
                if pairs and pairs[0].get('priceUsd'):
                    return float(pairs[0]['priceUsd'])
            
            return 0.0
        except:
            return 0.0
    
    def get_wallet_transactions(self, wallet_address: str) -> Dict[str, List[Dict]]:
        """Récupère transactions groupées par token."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT symbol, contract_address, date, direction, quantity,
                           price_per_token, total_value_usd,
                           inherited_price_per_token, is_inherited_from_wallet
                    FROM transaction_history
                    WHERE wallet_address = ?
                    ORDER BY date ASC
                """, (wallet_address,))
                
                transactions = cursor.fetchall()

                tokens = {}
                for tx in transactions:
                    symbol, contract, date, direction, qty, price, value, inherited_price, inherited_from = tx
                    token_key = f"{symbol}#{contract}"

                    if token_key not in tokens:
                        tokens[token_key] = {'symbol': symbol, 'contract': contract, 'txs': []}

                    effective_price = float(inherited_price) if inherited_price else float(price)
                    price_val = effective_price
                    value_val = float(value)

                    if inherited_price and direction == 'in':
                        value_val = abs(float(qty)) * effective_price

                    if price_val > _FIFO["MAX_PRICE_USD"] or value_val > _FIFO["MAX_VALUE_USD"]:
                        logger.warning(f"Valeur aberrante ignorée: {symbol} prix={price_val:.2e} valeur={value_val:.2e}")
                        continue

                    tokens[token_key]['txs'].append({
                        'date': date,
                        'direction': direction,
                        'quantity': float(qty),
                        'price': price_val,
                        'value': value_val,
                        'is_inherited': bool(inherited_price),
                        'inherited_from': inherited_from
                    })
                
                return tokens
        except Exception as e:
            logger.error(f"Erreur lecture transactions {wallet_address[:12]}...: {e}")
            return {}
    
    def calculate_fifo_metrics(self, token_data: Dict) -> Optional[Dict]:
        """Calcul FIFO pour un token."""
        symbol = token_data['symbol']
        contract = token_data['contract']
        transactions = token_data['txs']
        
        if not transactions:
            return None
        
        entries = [tx for tx in transactions if tx['direction'] == 'in']
        exits = [tx for tx in transactions if tx['direction'] == 'out']
        
        if not entries:
            return None
        
        total_bought = sum(tx['quantity'] for tx in entries)
        total_sold = sum(abs(tx['quantity']) for tx in exits)
        remaining_qty = total_bought - total_sold

        total_invested = sum(tx['value'] for tx in entries)
        total_realized = sum(abs(tx['value']) for tx in exits)
        
        avg_buy_price = total_invested / total_bought if total_bought > 0 else 0
        avg_sell_price = total_realized / total_sold if total_sold > 0 else 0
        
        current_price = self._get_current_price(contract, symbol)
        
        if current_price > _FIFO["MAX_PRICE_USD"]:
            logger.warning(f"Prix actuel aberrant ignoré: {symbol} prix=${current_price:.2e} → forcé à $0")
            current_price = 0.0
        
        current_value = remaining_qty * current_price if remaining_qty > 0 else 0
        
        total_gains = total_realized + current_value
        roi_percentage = ((total_gains - total_invested) / total_invested * 100) if total_invested > 0 else 0
        profit_loss = total_gains - total_invested
        
        is_airdrop = total_invested <= _FIFO["AIRDROP_MAX_INVESTED"]
        
        return {
            'wallet_address': None,
            'token_symbol': symbol,
            'contract_address': contract,
            'remaining_quantity': remaining_qty,
            'total_invested': total_invested,
            'total_realized': total_realized,
            'weighted_avg_buy_price': avg_buy_price,
            'weighted_avg_sell_price': avg_sell_price,
            'current_price': current_price,
            'current_value': current_value,
            'profit_loss': profit_loss,
            'roi_percentage': roi_percentage,
            'is_airdrop': 1 if is_airdrop else 0,
            'in_portfolio': 1 if remaining_qty > 0 else 0,
            'total_entries': len(entries),
            'total_exits': len(exits),
            'total_transactions': len(transactions)
        }
    
    def save_token_analytics(self, token_metrics: Dict) -> bool:
        """Sauvegarde métriques dans token_analytics."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO token_analytics (
                        wallet_address, token_symbol, contract_address,
                        remaining_quantity, total_invested, total_realized,
                        weighted_avg_buy_price, weighted_avg_sell_price,
                        current_price, current_value, profit_loss, roi_percentage,
                        is_airdrop, in_portfolio, total_entries, total_exits,
                        total_transactions, analysis_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    token_metrics['wallet_address'],
                    token_metrics['token_symbol'],
                    token_metrics['contract_address'],
                    token_metrics['remaining_quantity'],
                    token_metrics['total_invested'],
                    token_metrics['total_realized'],
                    token_metrics['weighted_avg_buy_price'],
                    token_metrics['weighted_avg_sell_price'],
                    token_metrics['current_price'],
                    token_metrics['current_value'],
                    token_metrics['profit_loss'],
                    token_metrics['roi_percentage'],
                    token_metrics['is_airdrop'],
                    token_metrics['in_portfolio'],
                    token_metrics['total_entries'],
                    token_metrics['total_exits'],
                    token_metrics['total_transactions'],
                    datetime.now().isoformat()
                ))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Erreur sauvegarde: {e}")
            return False
    
    def analyze_wallet(self, wallet_address: str) -> bool:
        """Analyse FIFO complète d'un wallet."""
        tokens = self.get_wallet_transactions(wallet_address)
        
        if not tokens:
            return False
        
        saved_tokens = 0
        for token_key, token_data in tokens.items():
            metrics = self.calculate_fifo_metrics(token_data)
            
            if metrics and metrics['total_invested'] >= _FIFO["AIRDROP_MAX_INVESTED"]:
                metrics['wallet_address'] = wallet_address
                if self.save_token_analytics(metrics):
                    saved_tokens += 1

        logger.info(f"{wallet_address[:12]}... | {saved_tokens} tokens analysés")
        return saved_tokens > 0
    
    def analyze_all_wallets(self) -> bool:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Wallets avec transactions dans transaction_history
                cursor.execute("""
                    SELECT DISTINCT wallet_address 
                    FROM transaction_history
                """)
                wallets_with_tx = set([row[0] for row in cursor.fetchall()])
                
                cursor.execute("""
                    SELECT DISTINCT wallet_address 
                    FROM token_analytics
                """)
                wallets_analyzed = set([row[0] for row in cursor.fetchall()])
                
                wallets_to_process = wallets_with_tx - wallets_analyzed
                wallets = list(wallets_to_process)
                
                logger.info(f"Wallets avec transactions: {len(wallets_with_tx)}")
                logger.info(f"Wallets déjà analysés: {len(wallets_analyzed)}")
                logger.info(f"Wallets à traiter: {len(wallets)}")

        except Exception as e:
            logger.error(f"Erreur récupération wallets: {e}")
            return False

        if not wallets:
            logger.warning("Aucun wallet avec transactions")
            return False

        logger.info(f"Analyse FIFO de {len(wallets)} wallets")

        successful = 0
        for i, wallet in enumerate(wallets, 1):
            if self.analyze_wallet(wallet):
                successful += 1

            if i % _FIFO["SLEEP_EVERY_WALLETS"] == 0:
                time.sleep(_FIFO["SLEEP_SECONDS"])

        logger.info(f"Terminé: {successful}/{len(wallets)} wallets analysés")
        return True
        

def run_smart_wallets_fifo():
    """Analyse FIFO pour les smart wallets uniquement."""
    analyzer = SimpleFIFOAnalyzer()
    
    try:
        with sqlite3.connect(analyzer.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT wallet_address FROM smart_wallets")
            smart_wallets = [row[0] for row in cursor.fetchall()]
            logger.info(f"{len(smart_wallets)} smart wallets trouvés")
    except sqlite3.Error as e:
        logger.error(f"Erreur lecture smart wallets: {e}")
        return False

    if not smart_wallets:
        logger.info("Aucun smart wallet à analyser")
        return True

    logger.info(f"Analyse FIFO de {len(smart_wallets)} smart wallets")

    successful = 0
    for i, wallet in enumerate(smart_wallets, 1):
        if analyzer.analyze_wallet(wallet):
            successful += 1

        if i % _FIFO["SLEEP_EVERY_WALLETS"] == 0:
            time.sleep(_FIFO["SLEEP_SECONDS"])

    logger.info(f"Terminé: {successful}/{len(smart_wallets)} smart wallets analysés")
    return True

def run_fifo_analysis():
    """Fonction principale."""
    analyzer = SimpleFIFOAnalyzer()
    return analyzer.analyze_all_wallets()



if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--smart_wallets":
        run_smart_wallets_fifo()
    else:
        run_fifo_analysis()
