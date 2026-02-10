#!/usr/bin/env python3
"""
Trading Portfolio Simulator
Simule un portefeuille de trading avec des paramètres fixes
Utilise les données consensus_live pour les prix d'entrée et consensus_prices pour les prix futurs
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import json
import numpy as np

class TradingSimulator:
    """
    Simulateur de trading avec gestion de take profit par paliers
    """
    
    def __init__(self, initial_capital: float = 100000, risk_per_position: float = 0.02):
        """
        Initialise le simulateur
        
        Args:
            initial_capital: Capital initial (défaut: 100K)
            risk_per_position: Risque par position en % (défaut: 2%)
        """
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.risk_per_position = risk_per_position
        
        # Niveaux de take profit avec pourcentages de vente PROGRESSIFS (pas cumulés)
        self.tp_levels = {
            50: 0.15,    # 50% gain -> vendre 15% de l'initial
            100: 0.15,   # 100% gain -> vendre 15% de l'initial (total 30%)
            200: 0.20,   # 200% gain -> vendre 20% de l'initial (total 50%) 
            300: 0.15,   # 300% gain -> vendre 15% de l'initial (total 65%)
            500: 0.20,   # 500% gain -> vendre 20% de l'initial (total 85%)
            1000: 0.15,  # 1000% gain -> vendre 15% de l'initial (total 100%)
            5000: 0.00   # 5000% gain -> plus rien à vendre
        }
        
        # Portfolios
        self.positions = {}  # positions actives
        self.closed_positions = []  # positions fermées
        self.trades_history = []  # historique des trades
        
        # Base de données
        self.db_path = Path(__file__).parent.parent.parent / "data" / "db" / "wit_database.db"
        
        print(f"🚀 TRADING SIMULATOR INITIALISÉ")
        print(f"💰 Capital initial: ${self.initial_capital:,.2f}")
        print(f"⚠️  Risque par position: {self.risk_per_position*100}%")
        print(f"🎯 Niveaux TP: {list(self.tp_levels.keys())}%")
    
    def get_consensus_tokens(self) -> pd.DataFrame:
        """Récupère les tokens de consensus depuis consensus_backtesting"""
        
        query = """
        SELECT 
            symbol,
            contract_address,
            chain,
            avg_entry_price as entry_price,
            consensus_end_date,
            whale_count,
            total_investment
        FROM consensus_backtesting 
        ORDER BY total_investment DESC
        """
        
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"📊 {len(df)} tokens de consensus trouvés")
        return df
    
    def get_price_history(self, contract_address: str, chain: str) -> pd.DataFrame:
        """Récupère l'historique des prix depuis consensus_prices"""
        
        query = """
        SELECT 
            price_date,
            vwap_price_usd
        FROM consensus_prices 
        WHERE contract_address = ? AND vwap_price_usd IS NOT NULL AND vwap_price_usd > 0
        ORDER BY price_date ASC
        """
        
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(query, conn, params=(contract_address.lower(),))
        conn.close()
        
        return df
    
    
    def calculate_position_size(self, entry_price: float) -> float:
        """Calcule la taille de position basée sur le capital disponible"""
        
        # Montant à investir = capital restant * risque par position
        position_value = self.current_capital * self.risk_per_position
        
        # Calculer le nombre de tokens à acheter
        position_size = position_value / entry_price
        
        return position_size, position_value
    
    def open_position(self, symbol: str, contract: str, chain: str, entry_price: float, 
                     whale_count: int, total_investment: float) -> Dict:
        """Ouvre une nouvelle position"""
        
        position_size, position_value = self.calculate_position_size(entry_price)
        
        position = {
            'symbol': symbol,
            'contract': contract,
            'chain': chain,
            'entry_price': entry_price,
            'entry_date': datetime.now().strftime('%Y-%m-%d'),
            'initial_size': position_size,
            'current_size': position_size,
            'whale_count': whale_count,
            'total_investment': total_investment,
            
            # Tracking financier détaillé
            'initial_investment': position_value,  # Montant investi initialement
            'total_sold_tokens': 0,               # Nombre total de tokens vendus
            'total_cash_received': 0,             # Total encaissé en dollars
            'realized_pnl': 0,                    # PnL réalisé
            'unrealized_pnl': 0,                  # PnL non réalisé (calculé en temps réel)
            'current_token_value': position_value, # Valeur actuelle des tokens restants
            'last_price': entry_price,            # Dernier prix connu
            
            # Détails des trades
            'tp_levels_hit': [],
            'trades_detail': [],                  # Historique détaillé des ventes
            'total_return_pct': 0,                # Rendement total en %
            'roi_on_investment': 0                # ROI = (cash_received + current_value) / initial_investment - 1
        }
        
        self.positions[symbol] = position
        self.current_capital -= position_value
        
        print(f"📈 {symbol}: ${position_value:,.0f} @ ${entry_price:.6f}")
        
        return position
    
    def update_position_value(self, symbol: str, current_price: float):
        """Met à jour la valeur d'une position avec le prix actuel"""
        if symbol not in self.positions:
            return
        
        position = self.positions[symbol]
        position['last_price'] = current_price
        position['current_token_value'] = position['current_size'] * current_price
        
        # Calculer PnL non réalisé
        position['unrealized_pnl'] = position['current_size'] * (current_price - position['entry_price'])
        
        # Mettre à jour le ROI total
        total_portfolio_value = position['total_cash_received'] + position['current_token_value']
        position['roi_on_investment'] = (total_portfolio_value / position['initial_investment'] - 1) * 100
    
    def check_take_profits(self, symbol: str, current_price: float, price_date: str) -> List[Dict]:
        """Vérifie et exécute les take profits"""
        
        if symbol not in self.positions:
            return []
        
        position = self.positions[symbol]
        entry_price = position['entry_price']
        current_gain_percent = ((current_price / entry_price) - 1) * 100
        
        trades_executed = []
        
        # CORRECTION: Traiter les niveaux TP dans l'ordre croissant pour exécuter TOUS les niveaux atteints
        sorted_tp_levels = sorted(self.tp_levels.items())
        
        for tp_level, sell_percent in sorted_tp_levels:
            # Vérifier si ce niveau TP n'a pas déjà été touché
            if tp_level not in position['tp_levels_hit'] and current_gain_percent >= tp_level:
                
                # Calculer la quantité à vendre
                sell_quantity = position['initial_size'] * sell_percent
                
                # S'assurer qu'on ne vend pas plus que ce qu'on possède
                if sell_quantity > position['current_size']:
                    sell_quantity = position['current_size']
                
                if sell_quantity > 0:
                    # Exécuter la vente
                    sell_value = sell_quantity * current_price
                    profit = sell_quantity * (current_price - entry_price)
                    
                    # Mettre à jour la position avec tracking détaillé
                    position['current_size'] -= sell_quantity
                    position['total_sold_tokens'] += sell_quantity
                    position['total_cash_received'] += sell_value
                    position['realized_pnl'] += profit
                    position['tp_levels_hit'].append(tp_level)
                    position['last_price'] = current_price
                    
                    # Calculer la nouvelle valeur des tokens restants
                    position['current_token_value'] = position['current_size'] * current_price
                    
                    # Calculer le ROI total
                    total_portfolio_value = position['total_cash_received'] + position['current_token_value']
                    position['roi_on_investment'] = (total_portfolio_value / position['initial_investment'] - 1) * 100
                    
                    # Les gains restent en attente jusqu'à la fin du mois
                    # self.current_capital += sell_value  # Commenté - sera ajouté en fin de mois
                    
                    trade = {
                        'symbol': symbol,
                        'type': 'TAKE_PROFIT',
                        'tp_level': tp_level,
                        'date': price_date,
                        'price': current_price,
                        'quantity': sell_quantity,
                        'value': sell_value,
                        'profit': profit,
                        'gain_percent': current_gain_percent
                    }
                    
                    # Ajouter le trade aux historiques
                    trades_executed.append(trade)
                    self.trades_history.append(trade)
                    position['trades_detail'].append(trade)
                    
                    print(f"   🎯 {symbol} TP{tp_level}%: +${profit:,.0f}")
        
        # Fermer complètement la position si plus rien à vendre
        if position['current_size'] <= 0:
            self.close_position(symbol)
        
        return trades_executed
    
    def close_position(self, symbol: str) -> Optional[Dict]:
        """Ferme complètement une position"""
        
        if symbol not in self.positions:
            return None
        
        position = self.positions.pop(symbol)
        self.closed_positions.append(position)
        
        print(f"🔒 {symbol} fermée: {position['realized_pnl']:+,.0f}")
        
        return position
    
    def simulate_monthly_reinvestment(self) -> Dict:
        """Lance la simulation avec cycles mensuels et réinvestissement"""
        
        print(f"\n🚀 SIMULATION MENSUELLE AVEC RÉINVESTISSEMENT")
        print("=" * 60)
        
        # 1. Récupérer tous les tokens de consensus
        consensus_tokens = self.get_consensus_tokens()
        
        if consensus_tokens.empty:
            print("❌ Aucun token de consensus trouvé")
            return self.get_performance_summary()
        
        # 2. Grouper les consensus par mois de détection
        consensus_tokens['detection_date'] = pd.to_datetime(consensus_tokens['consensus_end_date'])
        consensus_tokens['month_year'] = consensus_tokens['detection_date'].dt.to_period('M')
        
        # Trier par mois
        monthly_groups = consensus_tokens.groupby('month_year')
        
        print(f"📊 {len(consensus_tokens)} tokens répartis sur {len(monthly_groups)} mois")
        
        # 3. Simulation mois par mois
        for month, month_tokens in monthly_groups:
            month_str = str(month)
            print(f"\n📅 === MOIS {month_str} ===")
            print(f"💰 Capital disponible: ${self.current_capital:,.0f}")
            print(f"🪙 {len(month_tokens)} nouveaux consensus ce mois")
            
            # Investir dans les nouveaux consensus du mois
            if self.current_capital > 1000:  # Seuil minimum pour investir
                for _, token in month_tokens.iterrows():
                    if self.current_capital > 1000:  # Vérifier qu'on a encore du capital
                        try:
                            position_value = self.current_capital * self.risk_per_position
                            if position_value >= 500:  # Montant minimum par position
                                self.open_position(
                                    symbol=token['symbol'],
                                    contract=token['contract_address'],
                                    chain=token['chain'],
                                    entry_price=token['entry_price'],
                                    whale_count=token['whale_count'],
                                    total_investment=token['total_investment']
                                )
                        except Exception as e:
                            print(f"❌ Erreur ouverture {token['symbol']}: {e}")
            
            # Simuler l'évolution des positions existantes pendant le mois
            self._simulate_month_evolution()
            
            # Bilan de fin de mois - Collecter tous les gains encaissés
            monthly_cash_collected = 0
            for pos in list(self.positions.values()) + self.closed_positions:
                monthly_cash_collected += pos.get('total_cash_received', 0)
            
            # Réinjecter les gains dans le capital pour le mois suivant
            old_capital = self.current_capital
            self.current_capital += monthly_cash_collected
            
            # Garder une trace des encaissements pour l'affichage mais reset pour éviter double comptage
            for pos in list(self.positions.values()) + self.closed_positions:
                # Sauvegarder pour l'affichage final
                if 'total_cash_received_display' not in pos:
                    pos['total_cash_received_display'] = 0
                pos['total_cash_received_display'] += pos.get('total_cash_received', 0)
                # Reset pour éviter double comptage dans le capital
                pos['total_cash_received'] = 0
            
            portfolio_value = self.current_capital
            for pos in self.positions.values():
                portfolio_value += pos['current_token_value']
            
            print(f"📊 Fin de mois:")
            print(f"   💰 Capital initial du mois: ${old_capital:,.0f}")
            print(f"   💸 Gains encaissés ce mois: ${monthly_cash_collected:,.0f}")
            print(f"   💰 Nouveau capital: ${self.current_capital:,.0f}")
            print(f"   📊 Valeur totale portefeuille: ${portfolio_value:,.0f}")
            print(f"   📊 Positions actives: {len(self.positions)}")
        
        print(f"\n✅ SIMULATION MENSUELLE TERMINÉE")
        return self.get_performance_summary()
    
    def _simulate_month_evolution(self):
        """Simule l'évolution des positions pendant un mois"""
        
        for symbol in list(self.positions.keys()):
            position = self.positions[symbol]
            
            # Récupérer l'historique des prix
            price_history = self.get_price_history(position['contract'], position['chain'])
            
            if price_history.empty:
                continue
            
            # Simuler jour par jour pendant le mois
            for _, price_data in price_history.iterrows():
                current_price = price_data['vwap_price_usd']
                
                if current_price and current_price > 0:
                    # Mettre à jour la valeur de la position
                    self.update_position_value(symbol, current_price)
                    
                    # Vérifier les take profits
                    self.check_take_profits(symbol, current_price, price_data['price_date'])
                    
                    # Arrêter si la position est fermée
                    if symbol not in self.positions:
                        break

    def simulate_trading(self) -> Dict:
        """Lance la simulation de trading complète"""
        
        print(f"\n🚀 DÉBUT DE LA SIMULATION")
        print("=" * 60)
        
        # 1. Récupérer les tokens de consensus
        consensus_tokens = self.get_consensus_tokens()
        
        if consensus_tokens.empty:
            print("❌ Aucun token de consensus trouvé")
            return self.get_performance_summary()
        
        # 2. Ouvrir des positions pour chaque token
        for _, token in consensus_tokens.iterrows():
            try:
                self.open_position(
                    symbol=token['symbol'],
                    contract=token['contract_address'],
                    chain=token['chain'],
                    entry_price=token['entry_price'],
                    whale_count=token['whale_count'],
                    total_investment=token['total_investment']
                )
            except Exception as e:
                print(f"❌ Erreur ouverture position {token['symbol']}: {e}")
        
        print(f"\n📊 {len(self.positions)} positions ouvertes")
        print(f"💰 Capital restant: ${self.current_capital:,.2f}")
        
        # 3. Simuler l'évolution des prix
        total_trades = 0
        for symbol in list(self.positions.keys()):
            position = self.positions[symbol]
            
            print(f"\n📈 Simulation {symbol}...")
            
            # Récupérer l'historique des prix
            price_history = self.get_price_history(position['contract'], position['chain'])
            
            if price_history.empty:
                print(f"   ⚠️  Pas de données de prix pour {symbol}")
                continue
            
            # Simuler jour par jour
            for _, price_data in price_history.iterrows():
                current_price = price_data['vwap_price_usd']
                
                if current_price and current_price > 0:
                    # Mettre à jour la valeur de la position
                    self.update_position_value(symbol, current_price)
                    
                    # Vérifier les take profits
                    trades = self.check_take_profits(symbol, current_price, price_data['price_date'])
                    total_trades += len(trades)
                    
                    # Arrêter si la position est fermée
                    if symbol not in self.positions:
                        break
        
        print(f"\n✅ SIMULATION TERMINÉE")
        print(f"📊 {total_trades} trades exécutés")
        
        return self.get_performance_summary()
    
    def get_performance_summary(self) -> Dict:
        """Calcule et retourne un résumé des performances"""
        
        # Calculer la valeur actuelle du portefeuille
        current_portfolio_value = self.current_capital
        
        # Ajouter la valeur des positions ouvertes
        for position in self.positions.values():
            # On prend le dernier prix connu ou le prix d'entrée
            position_value = position['current_size'] * position['entry_price']
            current_portfolio_value += position_value
        
        # Calculer le PnL total réalisé
        total_realized_pnl = sum(pos['realized_pnl'] for pos in self.closed_positions)
        total_realized_pnl += sum(pos['realized_pnl'] for pos in self.positions.values())
        
        # Calculer les statistiques
        total_return = current_portfolio_value - self.initial_capital
        return_percent = (total_return / self.initial_capital) * 100
        
        # Statistiques par position (token)
        all_positions = list(self.positions.values()) + self.closed_positions
        
        winning_positions = []
        losing_positions = []
        
        for pos in all_positions:
            total_pnl = pos['realized_pnl'] + pos['unrealized_pnl']
            if total_pnl > 0:
                winning_positions.append(pos)
            else:
                losing_positions.append(pos)
        
        summary = {
            'initial_capital': self.initial_capital,
            'current_portfolio_value': current_portfolio_value,
            'liquid_capital': self.current_capital,
            'total_return': total_return,
            'return_percent': return_percent,
            'total_realized_pnl': total_realized_pnl,
            'positions_opened': len(all_positions),
            'positions_closed': len(self.closed_positions),
            'positions_active': len(self.positions),
            'total_individual_trades': len(self.trades_history),  # Nombre d'exécutions TP/SL
            'winning_positions': len(winning_positions),
            'losing_positions': len(losing_positions),
            'win_rate': len(winning_positions) / len(all_positions) * 100 if all_positions else 0,
            'avg_win': sum(pos['realized_pnl'] + pos['unrealized_pnl'] for pos in winning_positions) / len(winning_positions) if winning_positions else 0,
            'avg_loss': sum(pos['realized_pnl'] + pos['unrealized_pnl'] for pos in losing_positions) / len(losing_positions) if losing_positions else 0,
        }
        
        return summary
    
    def calculate_advanced_metrics(self, summary: Dict) -> Dict:
        """Calcule les ratios de performance avancés"""
        
        # Collecter les rendements de chaque position
        all_positions = list(self.positions.values()) + self.closed_positions
        returns = []
        
        for pos in all_positions:
            total_pnl = pos['realized_pnl'] + pos['unrealized_pnl']
            position_return = total_pnl / pos['initial_investment']
            returns.append(position_return)
        
        if not returns:
            return {}
        
        returns = np.array(returns)
        
        # Métriques de base
        mean_return = np.mean(returns)
        std_return = np.std(returns, ddof=1) if len(returns) > 1 else 0
        
        # Portfolio metrics
        portfolio_return = summary['return_percent'] / 100
        
        # Estimation de la volatilité annualisée
        # Hypothèse: les positions sont tenues en moyenne 180 jours
        avg_holding_period = 180
        annualization_factor = np.sqrt(365 / avg_holding_period)
        annual_volatility = std_return * annualization_factor if std_return > 0 else 0
        
        # Ratio de Sharpe (en supposant un taux sans risque de 3%)
        risk_free_rate = 0.03
        excess_return = portfolio_return - risk_free_rate
        sharpe_ratio = excess_return / annual_volatility if annual_volatility > 0 else 0
        
        # Ratio de Sortino (seulement la volatilité des rendements négatifs)
        negative_returns = returns[returns < 0]
        downside_std = np.std(negative_returns, ddof=1) if len(negative_returns) > 1 else 0
        annual_downside_volatility = downside_std * annualization_factor if downside_std > 0 else 0
        sortino_ratio = excess_return / annual_downside_volatility if annual_downside_volatility > 0 else 0
        
        # Maximum Drawdown - calculé sur la valeur du portefeuille
        portfolio_values = []
        running_capital = self.initial_capital
        
        for pos in all_positions:
            # Simuler l'évolution de la valeur du portefeuille
            investment = pos['initial_investment']
            total_pnl = pos['realized_pnl'] + pos['unrealized_pnl']
            portfolio_values.append(running_capital + total_pnl)
            running_capital -= investment
        
        if portfolio_values:
            portfolio_values = np.array([self.initial_capital] + portfolio_values)
            running_max = np.maximum.accumulate(portfolio_values)
            drawdowns = (portfolio_values - running_max) / running_max
            max_drawdown = np.min(drawdowns) * 100  # En pourcentage
        else:
            max_drawdown = 0
        
        # Ratio de Calmar
        annual_return = portfolio_return  # Déjà annualisé si période < 1 an
        calmar_ratio = annual_return / abs(max_drawdown / 100) if max_drawdown != 0 else 0
        
        # Profit Factor
        winning_trades_pnl = sum(pos['realized_pnl'] + pos['unrealized_pnl'] for pos in all_positions if (pos['realized_pnl'] + pos['unrealized_pnl']) > 0)
        losing_trades_pnl = abs(sum(pos['realized_pnl'] + pos['unrealized_pnl'] for pos in all_positions if (pos['realized_pnl'] + pos['unrealized_pnl']) < 0))
        profit_factor = winning_trades_pnl / losing_trades_pnl if losing_trades_pnl > 0 else float('inf')
        
        # VaR 95% (Value at Risk)
        var_95 = np.percentile(returns, 5) * 100 if len(returns) > 0 else 0
        
        advanced_metrics = {
            'sharpe_ratio': sharpe_ratio,
            'sortino_ratio': sortino_ratio,
            'max_drawdown_pct': max_drawdown,
            'calmar_ratio': calmar_ratio,
            'annual_volatility_pct': annual_volatility * 100,
            'profit_factor': profit_factor,
            'var_95_pct': var_95,
            'mean_position_return_pct': mean_return * 100,
            'position_volatility_pct': std_return * 100
        }
        
        return advanced_metrics
    
    def print_performance_report(self, summary: Dict):
        """Affiche un rapport complet des performances"""
        
        print(f"\n📊 RÉSULTATS GLOBAUX")
        print("=" * 50)
        print(f"💰 Capital initial:    ${summary['initial_capital']:,.0f}")
        print(f"📈 Capital final:      ${summary['current_portfolio_value']:,.0f}")
        print(f"📊 Profit total:       ${summary['total_return']:+,.0f}")
        print(f"📈 Rendement:          {summary['return_percent']:+.1f}%")
        
        print(f"\n📋 POSITIONS")
        print("=" * 30)
        print(f"📊 Total:       {summary['positions_opened']}")
        print(f"✅ Gagnantes:   {summary['winning_positions']}")
        print(f"❌ Perdantes:   {summary['losing_positions']}")
        print(f"🎯 Taux réussite: {summary['win_rate']:.0f}%")
        
        # Calculer et afficher les métriques avancées
        advanced_metrics = self.calculate_advanced_metrics(summary)
        if advanced_metrics:
            print(f"\n📈 MÉTRIQUES DE PERFORMANCE")
            print("=" * 40)
            print(f"📊 Ratio de Sharpe:     {advanced_metrics['sharpe_ratio']:.2f}")
            print(f"📉 Ratio de Sortino:    {advanced_metrics['sortino_ratio']:.2f}")
            print(f"🔻 Drawdown maximum:    {advanced_metrics['max_drawdown_pct']:.1f}%")
            print(f"📈 Ratio de Calmar:     {advanced_metrics['calmar_ratio']:.2f}")
            print(f"⚡ Volatilité annuelle: {advanced_metrics['annual_volatility_pct']:.1f}%")
            print(f"💎 Profit Factor:       {advanced_metrics['profit_factor']:.2f}")
            print(f"⚠️  VaR 95%:             {advanced_metrics['var_95_pct']:.1f}%")
    
    def print_position_summary(self):
        """Affiche un résumé simple de toutes les positions"""
        
        print(f"\n📊 DÉTAIL PAR TOKEN")
        print("=" * 80)
        print(f"{'Token':<8} {'Investi':<10} {'Encaissé':<10} {'Restant':<10} {'Profit':<10} {'ROI':<8}")
        print("-" * 80)
        
        all_positions = list(self.positions.values()) + self.closed_positions
        
        for pos in all_positions:
            symbol = pos['symbol']
            invested = pos['initial_investment']
            cash_received = pos.get('total_cash_received_display', pos.get('total_cash_received', 0))
            current_value = pos['current_token_value']
            
            # CORRECTION: Calculer le ROI correctement basé sur les vrais gains
            total_portfolio_value = cash_received + current_value
            true_profit = total_portfolio_value - invested
            true_roi = (true_profit / invested * 100) if invested > 0 else 0
            
            print(f"{symbol:<8} ${invested:<9,.0f} ${cash_received:<9,.0f} ${current_value:<9,.0f} ${true_profit:<+9,.0f} {true_roi:+6.0f}%")
    
    def print_detailed_position_report(self):
        """Affiche un rapport détaillé pour chaque position"""
        
        print(f"\n📊 RAPPORT DÉTAILLÉ PAR POSITION")
        print("=" * 80)
        
        all_positions = list(self.positions.values()) + self.closed_positions
        
        for pos in all_positions:
            symbol = pos['symbol']
            status = "🔥 ACTIVE" if symbol in self.positions else "🔒 FERMÉE"
            
            print(f"\n{status} - {symbol}")
            print("-" * 60)
            
            # Informations de base
            print(f"💰 Investissement initial:     ${pos['initial_investment']:,.2f}")
            print(f"📦 Tokens achetés:             {pos['initial_size']:,.0f}")
            print(f"💵 Prix d'entrée:              ${pos['entry_price']:.6f}")
            
            # Ventes réalisées
            print(f"\n📈 VENTES RÉALISÉES:")
            print(f"   🪙 Tokens vendus:            {pos['total_sold_tokens']:,.0f}")
            print(f"   💸 Cash encaissé:            ${pos['total_cash_received']:,.2f}")
            print(f"   💎 PnL réalisé:              ${pos['realized_pnl']:,.2f}")
            
            # Position restante
            print(f"\n📊 POSITION RESTANTE:")
            print(f"   🪙 Tokens restants:          {pos['current_size']:,.0f}")
            print(f"   💵 Dernier prix:             ${pos['last_price']:.6f}")
            print(f"   💰 Valeur actuelle:          ${pos['current_token_value']:,.2f}")
            print(f"   📈 PnL non réalisé:          ${pos['unrealized_pnl']:,.2f}")
            
            # Performance totale
            total_value = pos['total_cash_received'] + pos['current_token_value']
            total_pnl = pos['realized_pnl'] + pos['unrealized_pnl']
            
            print(f"\n🎯 PERFORMANCE TOTALE:")
            print(f"   💰 Valeur totale portfolio:  ${total_value:,.2f}")
            print(f"   📊 PnL total:                ${total_pnl:,.2f}")
            print(f"   📈 ROI:                      {pos['roi_on_investment']:+.2f}%")
            
            # Détails des trades
            if pos['trades_detail']:
                print(f"\n🎯 HISTORIQUE DES VENTES:")
                for trade in pos['trades_detail']:
                    print(f"   TP {trade['tp_level']}%: {trade['quantity']:,.0f} tokens @ ${trade['price']:.6f} = ${trade['value']:,.2f} (Profit: ${trade['profit']:,.2f})")
    
    def save_results(self, summary: Dict, filename: str = None):
        """Sauvegarde les résultats de la simulation"""
        
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"trading_simulation_{timestamp}.json"
        
        results = {
            'simulation_params': {
                'initial_capital': self.initial_capital,
                'risk_per_position': self.risk_per_position,
                'tp_levels': self.tp_levels
            },
            'performance_summary': summary,
            'trades_history': self.trades_history,
            'active_positions': self.positions,
            'closed_positions': self.closed_positions
        }
        
        output_path = Path(__file__).parent / filename
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n💾 Résultats sauvegardés: {output_path}")
        return output_path


def main():
    """Fonction principale"""
    
    # Créer et lancer la simulation
    simulator = TradingSimulator(
        initial_capital=100000,  # 100K
        risk_per_position=0.1   # 2%
    )
    
    # Lancer la simulation mensuelle avec réinvestissement
    summary = simulator.simulate_monthly_reinvestment()
    
    # Afficher le rapport global
    simulator.print_performance_report(summary)
    
    # Afficher le détail par position
    simulator.print_position_summary()
    
    # Sauvegarder les résultats
    simulator.save_results(summary)


if __name__ == "__main__":
    main()