#!/usr/bin/env python3
"""Simulateur de trading avec paliers de Take Profit pour les consensus."""

import sqlite3
import requests
import time
import numpy as np
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from smart_wallet_analysis.logger import get_logger

logger = get_logger("backtesting.trading_simulator")

ROOT_DIR = Path(__file__).parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "db" / "wit_database.db"

# Configuration du portefeuille
INITIAL_CAPITAL = 100000  # Capital de départ en USD
MAX_POSITION_SIZE_PCT = 20  # Maximum 20% du capital par trade
RISK_PER_TRADE_PCT = 5 # Risque maximum 2% du capital par trade

# Configuration des paliers de Take Profit (optimisée pour capter plus de gains)
TP_LEVELS = [
    {'level': 'TP1', 'target_pct': 50, 'sell_pct': 25},   # Vendre 25% à +50%
    {'level': 'TP2', 'target_pct': 100, 'sell_pct': 25},  # Vendre 25% à +100%
    {'level': 'TP3', 'target_pct': 200, 'sell_pct': 20},  # Vendre 20% à +200%
    {'level': 'TP4', 'target_pct': 500, 'sell_pct': 15},  # Vendre 15% à +500%
    {'level': 'TP5', 'target_pct': 700, 'sell_pct': 10},  # Vendre 10% à +700%
    {'level': 'TP6', 'target_pct': 1000, 'sell_pct': 5},  # Vendre 5% restant à +1000%
]

# Stop Loss
STOP_LOSS_PCT = -90  # Stop loss à -30%


class PortfolioTracker:
    """Suit l'évolution du portefeuille et calcule les métriques."""

    def __init__(self, initial_capital: float = INITIAL_CAPITAL):
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.trades_history = []
        self.equity_curve = [initial_capital]
        self.equity_dates = [datetime.now(timezone.utc)]

    def add_trade(self, trade_result: Dict):
        """Ajoute un trade à l'historique et met à jour le capital."""
        if trade_result['status'] == 'EXECUTED':
            profit = trade_result['total_profit_usd']
            self.current_capital += profit
            self.trades_history.append(trade_result)

            # Mettre à jour la courbe d'équité
            if trade_result['executions']:
                last_exec = trade_result['executions'][-1]
                self.equity_curve.append(self.current_capital)
                self.equity_dates.append(last_exec['date'])

    def calculate_metrics(self) -> Dict:
        """Calcule toutes les métriques de performance."""
        if not self.trades_history:
            return {}

        # Métriques de base
        total_trades = len(self.trades_history)
        winning_trades = sum(1 for t in self.trades_history if t['total_profit_usd'] > 0)
        losing_trades = total_trades - winning_trades

        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0

        # Profits et pertes
        gross_profit = sum(t['total_profit_usd'] for t in self.trades_history if t['total_profit_usd'] > 0)
        gross_loss = abs(sum(t['total_profit_usd'] for t in self.trades_history if t['total_profit_usd'] < 0))
        net_profit = self.current_capital - self.initial_capital

        # ROI global
        total_roi = (net_profit / self.initial_capital * 100) if self.initial_capital > 0 else 0

        # Profit factor
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float('inf')

        # Average win/loss
        avg_win = (gross_profit / winning_trades) if winning_trades > 0 else 0
        avg_loss = (gross_loss / losing_trades) if losing_trades > 0 else 0

        # Ratio gain/perte moyen
        avg_win_loss_ratio = (avg_win / avg_loss) if avg_loss > 0 else float('inf')

        # Drawdown maximum
        max_drawdown_pct = self._calculate_max_drawdown()

        # Sharpe ratio (simplifié, suppose 0% risk-free rate)
        sharpe_ratio = self._calculate_sharpe_ratio()

        # Expectancy (espérance de gain par trade)
        expectancy = (win_rate / 100 * avg_win) - ((100 - win_rate) / 100 * avg_loss)

        return {
            'initial_capital': self.initial_capital,
            'final_capital': self.current_capital,
            'net_profit': net_profit,
            'total_roi_pct': total_roi,
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate_pct': win_rate,
            'gross_profit': gross_profit,
            'gross_loss': gross_loss,
            'profit_factor': profit_factor,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'avg_win_loss_ratio': avg_win_loss_ratio,
            'max_drawdown_pct': max_drawdown_pct,
            'sharpe_ratio': sharpe_ratio,
            'expectancy': expectancy,
            'best_trade_pct': max(t['total_roi_pct'] for t in self.trades_history),
            'worst_trade_pct': min(t['total_roi_pct'] for t in self.trades_history),
        }

    def _calculate_max_drawdown(self) -> float:
        """Calcule le drawdown maximum."""
        if len(self.equity_curve) < 2:
            return 0.0

        peak = self.equity_curve[0]
        max_dd = 0.0

        for value in self.equity_curve:
            if value > peak:
                peak = value
            dd = ((peak - value) / peak * 100) if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd

        return max_dd

    def _calculate_sharpe_ratio(self) -> float:
        """Calcule le ratio de Sharpe."""
        if len(self.equity_curve) < 2:
            return 0.0

        # Calculer les rendements
        returns = []
        for i in range(1, len(self.equity_curve)):
            ret = (self.equity_curve[i] - self.equity_curve[i-1]) / self.equity_curve[i-1]
            returns.append(ret)

        if not returns:
            return 0.0

        mean_return = np.mean(returns)
        std_return = np.std(returns)

        if std_return == 0:
            return 0.0

        # Sharpe ratio annualisé (suppose ~250 jours de trading)
        sharpe = (mean_return / std_return) * np.sqrt(250)
        return sharpe


def init_consensus_price_table():
    """Crée la table consensus_price si elle n'existe pas."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS consensus_price (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contract_address TEXT NOT NULL,
                symbol TEXT,
                timestamp INTEGER NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(contract_address, timestamp)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_consensus_price_contract ON consensus_price(contract_address)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_consensus_price_date ON consensus_price(date)")
        conn.commit()
        conn.close()
        logger.info("✅ Table consensus_price initialisée")
    except Exception as e:
        logger.error(f"❌ Erreur création table consensus_price: {e}")


def _request_gecko(url: str, retries: int = 3) -> Optional[dict]:
    """Requête GeckoTerminal avec rate limiting et retry."""
    headers = {"Accept": "application/json;version=20230302"}

    for attempt in range(retries):
        time.sleep(1.5)  # Rate limit
        try:
            response = requests.get(url, headers=headers, timeout=15)

            if response.status_code == 429:
                wait_time = 60 if attempt < 2 else 120
                logger.warning(f"⏳ Rate limit GeckoTerminal, pause {wait_time}s...")
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            return response.json()

        except requests.RequestException as e:
            if attempt == retries - 1:
                logger.error(f"❌ Erreur requête: {e}")
                return None
            time.sleep(5)

    return None


def get_price_history_geckoterminal(contract_address: str, symbol: str, chain: str = "base") -> Optional[List[Dict]]:
    """Récupère l'historique des prix via GeckoTerminal et le sauvegarde en DB."""
    try:
        # Vérifier si on a déjà les données
        conn = sqlite3.connect(DB_PATH)
        existing = conn.execute(
            "SELECT COUNT(*) FROM consensus_price WHERE contract_address = ?",
            (contract_address,)
        ).fetchone()[0]

        if existing > 0:
            logger.info(f"📊 Historique déjà en cache pour {symbol}")
            # Récupérer depuis la DB
            rows = conn.execute("""
                SELECT timestamp, date, open, high, low, close, volume
                FROM consensus_price
                WHERE contract_address = ?
                ORDER BY timestamp ASC
            """, (contract_address,)).fetchall()
            conn.close()

            return [{
                'timestamp': row[0],
                'date': datetime.fromisoformat(row[1]),
                'open': row[2],
                'high': row[3],
                'low': row[4],
                'close': row[5],
                'volume': row[6]
            } for row in rows]

        conn.close()

        # Sinon, récupérer via API avec rate limiting
        logger.info(f"🔍 Récupération historique pour {symbol}")

        # Essayer plusieurs chains (Base d'abord, puis BSC, puis Ethereum)
        chains_to_try = ['base', 'bsc', 'eth'] if chain == 'base' else [chain, 'base', 'bsc', 'eth']
        pools_data = None
        found_chain = None

        for try_chain in chains_to_try:
            pools_url = f"https://api.geckoterminal.com/api/v2/networks/{try_chain}/tokens/{contract_address}/pools"
            pools_data = _request_gecko(pools_url)

            if pools_data and pools_data.get("data"):
                found_chain = try_chain
                logger.info(f"   ✅ Trouvé sur {try_chain.upper()}")
                break

        if not pools_data or not found_chain:
            logger.warning(f"⚠️ Impossible de trouver {symbol} sur Base/BSC/ETH")
            return None

        pools = pools_data.get("data", [])
        if not pools:
            logger.warning(f"⚠️ Aucun pool trouvé pour {symbol}")
            return None

        chain = found_chain  # Utiliser la chain trouvée pour les requêtes suivantes

        # Prendre le pool avec le plus de liquidité
        best_pool = max(pools, key=lambda x: float(x.get("attributes", {}).get("reserve_in_usd", 0) or 0))
        pool_address = best_pool.get("attributes", {}).get("address")

        if not pool_address:
            return None

        # Récupérer OHLCV 4h (plus de données)
        ohlcv_url = f"https://api.geckoterminal.com/api/v2/networks/{chain}/pools/{pool_address}/ohlcv/hour?aggregate=4&limit=200"
        ohlcv_data = _request_gecko(ohlcv_url)

        if not ohlcv_data:
            logger.warning(f"⚠️ Impossible de récupérer l'historique pour {symbol}")
            return None

        ohlcv_list = ohlcv_data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])

        if not ohlcv_list:
            logger.warning(f"⚠️ Aucune donnée OHLCV pour {symbol}")
            return None

        # Sauvegarder en DB
        conn = sqlite3.connect(DB_PATH)
        price_history = []

        for candle in ohlcv_list:
            timestamp = candle[0]
            open_price = candle[1]
            high = candle[2]
            low = candle[3]
            close = candle[4]
            volume = candle[5]

            dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            date_str = dt.isoformat()

            # Insérer dans la DB
            conn.execute("""
                INSERT OR IGNORE INTO consensus_price
                (contract_address, symbol, timestamp, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (contract_address, symbol, timestamp, date_str, open_price, high, low, close, volume))

            price_history.append({
                'timestamp': timestamp,
                'date': dt,
                'open': open_price,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume
            })

        conn.commit()
        conn.close()

        logger.info(f"✅ {len(price_history)} bougies (4h) sauvegardées pour {symbol}")
        return price_history

    except Exception as e:
        logger.error(f"❌ Erreur récupération prix {symbol}: {e}")
        return None


def simulate_tp_trading(entry_price: float, entry_date: datetime, price_history: List[Dict],
                        investment_usd: float, symbol: str) -> Dict:
    """Simule le trading avec paliers de TP."""

    if not price_history:
        return {
            'symbol': symbol,
            'status': 'NO_PRICE_DATA',
            'total_profit_usd': 0,
            'total_roi_pct': 0,
            'executions': []
        }

    # Filtrer l'historique après la date d'entrée
    relevant_history = [p for p in price_history if p['date'] >= entry_date]

    if not relevant_history:
        return {
            'symbol': symbol,
            'status': 'NO_RELEVANT_DATA',
            'total_profit_usd': 0,
            'total_roi_pct': 0,
            'executions': []
        }

    remaining_position = 100.0  # Pourcentage restant
    total_sold_usd = 0
    executions = []
    tp_hit = [False] * len(TP_LEVELS)
    stop_loss_hit = False

    for price_data in relevant_history:
        if remaining_position <= 0:
            break

        high = price_data['high']
        low = price_data['low']
        date = price_data['date']

        # Vérifier Stop Loss (sur low - si le prix touche le SL on est stoppé)
        if not stop_loss_hit and low <= entry_price * (1 + STOP_LOSS_PCT / 100):
            sl_price = entry_price * (1 + STOP_LOSS_PCT / 100)
            sold_pct = remaining_position
            sold_usd = investment_usd * (remaining_position / 100) * (sl_price / entry_price)

            executions.append({
                'type': 'STOP_LOSS',
                'date': date,
                'price': sl_price,
                'roi_pct': STOP_LOSS_PCT,
                'sold_pct': sold_pct,
                'sold_usd': sold_usd
            })

            total_sold_usd += sold_usd
            remaining_position = 0
            stop_loss_hit = True
            break

        # Vérifier chaque palier de TP (sur high - si le prix touche le TP on vend)
        # Vérifier du plus haut au plus bas pour capturer tous les TP intermédiaires
        for i in range(len(TP_LEVELS) - 1, -1, -1):  # Parcourir de TP6 vers TP1
            tp = TP_LEVELS[i]

            if tp_hit[i]:
                continue

            target_price = entry_price * (1 + tp['target_pct'] / 100)

            if high >= target_price:
                # TP atteint - marquer tous les TP inférieurs comme atteints aussi
                sell_pct = min(tp['sell_pct'], remaining_position)
                sold_usd = investment_usd * (sell_pct / 100) * (target_price / entry_price)

                executions.append({
                    'type': tp['level'],
                    'date': date,
                    'price': target_price,
                    'roi_pct': tp['target_pct'],
                    'sold_pct': sell_pct,
                    'sold_usd': sold_usd
                })

                total_sold_usd += sold_usd
                remaining_position -= sell_pct
                tp_hit[i] = True

                # Marquer tous les TP inférieurs (indices < i) comme atteints à cette même date
                for j in range(i):
                    if not tp_hit[j]:
                        lower_tp = TP_LEVELS[j]
                        lower_target_price = entry_price * (1 + lower_tp['target_pct'] / 100)
                        lower_sell_pct = min(lower_tp['sell_pct'], remaining_position)
                        lower_sold_usd = investment_usd * (lower_sell_pct / 100) * (lower_target_price / entry_price)

                        executions.append({
                            'type': lower_tp['level'],
                            'date': date,
                            'price': lower_target_price,
                            'roi_pct': lower_tp['target_pct'],
                            'sold_pct': lower_sell_pct,
                            'sold_usd': lower_sold_usd
                        })

                        total_sold_usd += lower_sold_usd
                        remaining_position -= lower_sell_pct
                        tp_hit[j] = True

                break  # Sortir de la boucle une fois qu'on a traité le TP le plus haut atteint

    # Si position restante, calculer avec le dernier prix
    if remaining_position > 0 and relevant_history:
        last_price = relevant_history[-1]['close']
        last_date = relevant_history[-1]['date']
        roi_pct = ((last_price - entry_price) / entry_price) * 100
        remaining_usd = investment_usd * (remaining_position / 100) * (last_price / entry_price)

        executions.append({
            'type': 'HOLD',
            'date': last_date,
            'price': last_price,
            'roi_pct': roi_pct,
            'sold_pct': remaining_position,
            'sold_usd': remaining_usd
        })

        total_sold_usd += remaining_usd
        remaining_position = 0

    total_profit_usd = total_sold_usd - investment_usd
    total_roi_pct = (total_profit_usd / investment_usd * 100) if investment_usd > 0 else 0

    return {
        'symbol': symbol,
        'status': 'EXECUTED',
        'entry_price': entry_price,
        'investment_usd': investment_usd,
        'total_sold_usd': total_sold_usd,
        'total_profit_usd': total_profit_usd,
        'total_roi_pct': total_roi_pct,
        'executions': executions,
        'tp_count': sum(tp_hit)
    }


def backtest_consensus_with_tp(consensus_data: List[Dict], initial_capital: float = INITIAL_CAPITAL) -> Dict:
    """Backteste tous les consensus avec le système de TP et gestion de capital."""

    init_consensus_price_table()
    portfolio = PortfolioTracker(initial_capital)

    max_exposure_per_trade = 0.0  # Tracker l'exposition maximum par trade
    max_cumulative_exposure = 0.0  # Tracker l'exposition cumulée maximum
    open_positions = []  # Liste des positions ouvertes: [(symbol, investment_usd, entry_date, exit_date)]

    logger.info(f"\n🎯 SIMULATION DE TRADING AVEC PALIERS DE TP")
    logger.info("=" * 80)
    logger.info(f"💰 Capital initial: ${initial_capital:,.0f}")
    logger.info(f"📊 Position max: {MAX_POSITION_SIZE_PCT}% du capital par trade")
    logger.info(f"🎲 Risque max: {RISK_PER_TRADE_PCT}% du capital par trade")
    logger.info(f"\n📊 Paliers configurés:")
    for tp in TP_LEVELS:
        logger.info(f"   {tp['level']}: Vendre {tp['sell_pct']}% à +{tp['target_pct']}%")
    logger.info(f"   Stop Loss: {STOP_LOSS_PCT}%")
    logger.info("=" * 80)

    for i, consensus in enumerate(consensus_data, 1):
        symbol = consensus['symbol']
        contract_address = consensus['contract_address']
        entry_price = consensus['avg_entry_price']
        entry_date = consensus['detection_date']

        # Calculer la taille de position basée sur le capital actuel
        max_position = portfolio.current_capital * (MAX_POSITION_SIZE_PCT / 100)

        # Ajuster selon le risque (stop loss)
        risk_amount = portfolio.current_capital * (RISK_PER_TRADE_PCT / 100)
        position_size_risk = risk_amount / (abs(STOP_LOSS_PCT) / 100)

        # Prendre le minimum des deux
        position_size = min(max_position, position_size_risk)

        logger.info(f"\n🔍 Trade {i}/{len(consensus_data)}: {symbol}")
        logger.info(f"   Capital disponible: ${portfolio.current_capital:,.0f}")
        logger.info(f"   Taille position: ${position_size:,.0f} ({position_size/portfolio.current_capital*100:.1f}%)")

        # Tracker l'exposition maximum par trade
        exposure_pct = (position_size / portfolio.current_capital) * 100
        if exposure_pct > max_exposure_per_trade:
            max_exposure_per_trade = exposure_pct

        # Récupérer l'historique des prix
        price_history = get_price_history_geckoterminal(contract_address, symbol)

        if not price_history:
            logger.warning(f"   ⚠️ Pas de données prix disponibles - SKIP")
            continue

        # Simuler le trading
        trade_result = simulate_tp_trading(entry_price, entry_date, price_history, position_size, symbol)

        # Ajouter au portefeuille
        portfolio.add_trade(trade_result)

        # Calculer la date de sortie (dernière exécution)
        if trade_result['status'] == 'EXECUTED' and trade_result['executions']:
            entry_dt = datetime.fromisoformat(entry_date.replace('Z', '+00:00')) if isinstance(entry_date, str) else entry_date
            exit_dt = trade_result['executions'][-1]['date']

            # Ajouter la position à la liste
            open_positions.append({
                'symbol': symbol,
                'investment': position_size,
                'entry_date': entry_dt,
                'exit_date': exit_dt
            })

            # Calculer l'exposition cumulée à chaque moment
            # Pour chaque position, vérifier combien d'autres positions sont ouvertes en même temps
            current_entry = entry_dt

            # Calculer l'exposition à la date d'entrée de cette position
            cumulative_at_entry = 0.0
            for pos in open_positions:
                # Si la position est ouverte pendant notre entrée
                if pos['entry_date'] <= current_entry <= pos['exit_date']:
                    cumulative_at_entry += pos['investment']

            # Mettre à jour le max
            cumulative_pct = (cumulative_at_entry / initial_capital) * 100
            if cumulative_pct > max_cumulative_exposure:
                max_cumulative_exposure = cumulative_pct

        # Afficher les résultats
        if trade_result['status'] == 'EXECUTED':
            logger.info(f"   Prix entrée: ${entry_price:.8f}")
            logger.info(f"   Profit: ${trade_result['total_profit_usd']:,.0f} ({trade_result['total_roi_pct']:+.1f}%)")
            logger.info(f"   Nouveau capital: ${portfolio.current_capital:,.0f}")
            logger.info(f"   TP atteints: {trade_result['tp_count']}/{len(TP_LEVELS)}")

            for exec in trade_result['executions']:
                logger.info(f"      {exec['type']}: {exec['sold_pct']:.1f}% @ ${exec['price']:.8f} ({exec['roi_pct']:+.1f}%) - {exec['date'].strftime('%Y-%m-%d')}")

        time.sleep(1.5)  # Rate limit

    # Calculer les métriques finales
    metrics = portfolio.calculate_metrics()

    # Afficher le rapport final
    logger.info(f"\n{'='*80}")
    logger.info(f"📊 RAPPORT FINAL DE PERFORMANCE")
    logger.info(f"{'='*80}")

    if metrics:
        logger.info(f"\n💰 CAPITAL")
        logger.info(f"   Capital initial: ${metrics['initial_capital']:,.0f}")
        logger.info(f"   Capital final: ${metrics['final_capital']:,.0f}")
        logger.info(f"   Profit net: ${metrics['net_profit']:,.0f}")
        logger.info(f"   ROI total: {metrics['total_roi_pct']:+.2f}%")

        logger.info(f"\n📈 STATISTIQUES DE TRADING")
        logger.info(f"   Total trades: {metrics['total_trades']}")
        logger.info(f"   Trades gagnants: {metrics['winning_trades']} ({metrics['win_rate_pct']:.1f}%)")
        logger.info(f"   Trades perdants: {metrics['losing_trades']}")
        logger.info(f"   Profit brut: ${metrics['gross_profit']:,.0f}")
        logger.info(f"   Perte brute: ${metrics['gross_loss']:,.0f}")
        logger.info(f"   Profit factor: {metrics['profit_factor']:.2f}")

        logger.info(f"\n💡 MOYENNES")
        logger.info(f"   Gain moyen: ${metrics['avg_win']:,.0f}")
        logger.info(f"   Perte moyenne: ${metrics['avg_loss']:,.0f}")
        logger.info(f"   Ratio gain/perte: {metrics['avg_win_loss_ratio']:.2f}")
        logger.info(f"   Expectancy: ${metrics['expectancy']:,.0f}")

        logger.info(f"\n📊 MÉTRIQUES DE RISQUE")
        logger.info(f"   Drawdown maximum: {metrics['max_drawdown_pct']:.2f}%")
        logger.info(f"   Sharpe ratio: {metrics['sharpe_ratio']:.2f}")
        logger.info(f"   Meilleur trade: {metrics['best_trade_pct']:+.1f}%")
        logger.info(f"   Pire trade: {metrics['worst_trade_pct']:+.1f}%")
        logger.info(f"\n💼 EXPOSITION DU CAPITAL")
        logger.info(f"   Exposition max par trade: {max_exposure_per_trade:.2f}% du capital")
        logger.info(f"   Exposition cumulée max: {max_cumulative_exposure:.2f}% du capital initial")

        # Analyse de la qualité de la stratégie
        logger.info(f"\n✅ ÉVALUATION DE LA STRATÉGIE")
        if metrics['total_roi_pct'] > 100 and metrics['win_rate_pct'] > 60:
            logger.info(f"   🌟 EXCELLENTE - ROI très élevé et bon win rate")
        elif metrics['total_roi_pct'] > 50 and metrics['win_rate_pct'] > 50:
            logger.info(f"   💚 BONNE - Performance positive et équilibrée")
        elif metrics['total_roi_pct'] > 0:
            logger.info(f"   🟡 MOYENNE - Performance positive mais à améliorer")
        else:
            logger.info(f"   🔴 FAIBLE - Performance négative")

        if metrics['profit_factor'] > 2:
            logger.info(f"   🎯 Profit factor excellent (>2)")
        elif metrics['profit_factor'] > 1.5:
            logger.info(f"   📈 Profit factor bon (>1.5)")
        else:
            logger.info(f"   ⚠️ Profit factor à améliorer (<1.5)")

    return {
        'portfolio': portfolio,
        'metrics': metrics,
        'trades': portfolio.trades_history
    }


if __name__ == "__main__":
    # Test
    init_consensus_price_table()
    logger.info("✅ Module trading_simulator initialisé")
