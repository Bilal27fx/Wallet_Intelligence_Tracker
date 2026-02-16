#!/usr/bin/env python3
"""Analyseur de seuil optimal par wallet."""

import sqlite3
import math
import numpy as np

from smart_wallet_analysis.config import DB_PATH, SCORE_ENGINE
from smart_wallet_analysis.logger import get_logger

logger = get_logger("score_engine.optimal_threshold")

_OT = SCORE_ENGINE["OPTIMAL_THRESHOLD"]

class OptimalThresholdAnalyzer:
    
    def __init__(self):
        self.alpha_bayesian = _OT["ALPHA_BAYESIAN"]
        self.min_trades_threshold = _OT["MIN_TRADES_THRESHOLD"]
        self.min_winrate_threshold = _OT["MIN_WINRATE_THRESHOLD"]
        self.stability_threshold = _OT["STABILITY_THRESHOLD"]
        self.quality_threshold = _OT["QUALITY_THRESHOLD"]
        self.min_trades_quality = _OT["MIN_TRADES_QUALITY"]
        self.filter_quality_min = _OT["FILTER_QUALITY_MIN"]
        
    def get_wallet_tier_data(self, wallet_address):
        """Récupère les données par palier depuis wallet_profiles."""
        
        conn = sqlite3.connect(DB_PATH)
        query = """
            SELECT
                tier_1k_roi, tier_1k_taux_reussite, tier_1k_nb_trades, tier_1k_gagnants, tier_1k_perdants, tier_1k_neutres,
                tier_2k_roi, tier_2k_taux_reussite, tier_2k_nb_trades, tier_2k_gagnants, tier_2k_perdants, tier_2k_neutres,
                tier_3k_roi, tier_3k_taux_reussite, tier_3k_nb_trades, tier_3k_gagnants, tier_3k_perdants, tier_3k_neutres,
                tier_4k_roi, tier_4k_taux_reussite, tier_4k_nb_trades, tier_4k_gagnants, tier_4k_perdants, tier_4k_neutres,
                tier_5k_roi, tier_5k_taux_reussite, tier_5k_nb_trades, tier_5k_gagnants, tier_5k_perdants, tier_5k_neutres,
                tier_6k_roi, tier_6k_taux_reussite, tier_6k_nb_trades, tier_6k_gagnants, tier_6k_perdants, tier_6k_neutres,
                tier_7k_roi, tier_7k_taux_reussite, tier_7k_nb_trades, tier_7k_gagnants, tier_7k_perdants, tier_7k_neutres,
                tier_8k_roi, tier_8k_taux_reussite, tier_8k_nb_trades, tier_8k_gagnants, tier_8k_perdants, tier_8k_neutres,
                tier_9k_roi, tier_9k_taux_reussite, tier_9k_nb_trades, tier_9k_gagnants, tier_9k_perdants, tier_9k_neutres,
                tier_10k_roi, tier_10k_taux_reussite, tier_10k_nb_trades, tier_10k_gagnants, tier_10k_perdants, tier_10k_neutres,
                tier_11k_roi, tier_11k_taux_reussite, tier_11k_nb_trades, tier_11k_gagnants, tier_11k_perdants, tier_11k_neutres,
                tier_12k_roi, tier_12k_taux_reussite, tier_12k_nb_trades, tier_12k_gagnants, tier_12k_perdants, tier_12k_neutres
            FROM wallet_profiles
            WHERE wallet_address = ?
        """

        result = conn.execute(query, [wallet_address]).fetchone()
        conn.close()

        if not result:
            return None

        tier_data = {}
        tiers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        
        for i, tier in enumerate(tiers):
            roi_idx = i * 6
            winrate_idx = i * 6 + 1
            trades_idx = i * 6 + 2
            gagnants_idx = i * 6 + 3
            perdants_idx = i * 6 + 4
            neutres_idx = i * 6 + 5
            
            tier_data[tier] = {
                'roi': result[roi_idx] or 0,
                'winrate': result[winrate_idx] or 0,
                'trades': result[trades_idx] or 0,
                'gagnants': result[gagnants_idx] or 0,
                'perdants': result[perdants_idx] or 0,
                'neutres': result[neutres_idx] or 0
            }
            
        return tier_data
    
    def filter_reliable_tiers(self, tier_data):
        """Filtre les paliers fiables selon les critères."""
        
        reliable_tiers = {}
        
        for tier, data in tier_data.items():
            if (data['roi'] > _OT["MIN_RELIABLE_ROI"] and
                data['winrate'] >= self.min_winrate_threshold and
                data['trades'] >= self.min_trades_threshold):
                reliable_tiers[tier] = data
                
        return reliable_tiers
    
    def calculate_bayesian_winrate(self, winrate, trades):
        """Calcul du WinRate lissé bayésien."""
        
        prior = 0.5
        successes = (winrate / 100) * trades
        smoothed = (successes + self.alpha_bayesian * prior) / (trades + self.alpha_bayesian)
        
        return smoothed * 100
    
    def normalize_roi(self, tier_data):
        """Normalisation min-max du ROI sur les paliers du wallet."""
        
        if not tier_data:
            return {}
            
        rois = [data['roi'] for data in tier_data.values()]
        roi_min = min(rois)
        roi_max = max(rois)
        
        if roi_max == roi_min:
            return {tier: 0.5 for tier in tier_data.keys()}
            
        normalized = {}
        for tier, data in tier_data.items():
            normalized[tier] = (data['roi'] - roi_min) / (roi_max - roi_min)
            
        return normalized
    
    def calculate_j_scores(self, tier_data):
        """Calcule le score J_t pour chaque palier."""
        
        if not tier_data:
            return {}
            
        roi_normalized = self.normalize_roi(tier_data)
        j_scores = {}
        
        for tier, data in tier_data.items():
            winrate_smooth = self.calculate_bayesian_winrate(data['winrate'], data['trades'])
            winrate_norm = winrate_smooth / 100
            
            jw = _OT["J_SCORE_WEIGHTS"]
            j_score = (jw["ROI"] * roi_normalized[tier] +
                       jw["WINRATE"] * winrate_norm +
                       jw["TRADES_LOG"] * math.log(1 + data['trades']))
            
            j_scores[tier] = j_score
            
        return j_scores
    
    def find_optimal_threshold(self, j_scores, tier_data):
        """Trouve le seuil optimal τ_w."""
        
        if not j_scores:
            return None
            
        scores = list(j_scores.values())
        percentile_60 = np.percentile(scores, _OT["PERCENTILE"])
        
        tiers_sorted = sorted(j_scores.keys())
        
        for i, tier in enumerate(tiers_sorted):
            current_score = j_scores[tier]
            
            if current_score >= percentile_60:
                
                if i + 1 < len(tiers_sorted):
                    next_tier = tiers_sorted[i + 1]
                    next_score = j_scores[next_tier]
                    
                    if (current_score - next_score) / current_score <= self.stability_threshold:
                        return tier
                else:
                    return tier
        
        best_tier = None
        best_adjusted_score = -float('inf')

        for tier in tiers_sorted:
            penalty = _OT["PENALTY_COEF"] * math.log(tier / 1.0)
            adjusted_score = j_scores[tier] - penalty
            
            if adjusted_score > best_adjusted_score:
                best_adjusted_score = adjusted_score
                best_tier = tier
                
        return best_tier
    
    def calculate_quality(self, wallet_address, optimal_threshold, tier_data):
        """Calcule la qualité q_w du wallet."""
        
        if not optimal_threshold or not tier_data:
            return 0.05
            
        optimal_data = tier_data.get(optimal_threshold)
        if not optimal_data:
            return 0.05
            
        total_trades_global = sum(data['trades'] for data in tier_data.values())
        
        if total_trades_global == 0:
            return 0.05
            
        optimal_roi = optimal_data['roi']
        optimal_trades = optimal_data['trades']
        optimal_gagnants = optimal_data['gagnants']
        optimal_perdants = optimal_data['perdants']
        optimal_neutres = optimal_data['neutres']

        optimal_winrate = (optimal_gagnants / optimal_trades) * 100 if optimal_trades > 0 else 0
        optimal_neutralrate = (optimal_neutres / optimal_trades) * 100 if optimal_trades > 0 else 0
        
        roi_score = min(1.0, max(0.0, optimal_roi / _OT["ROI_SCORE_MAX"]))
        winrate_score = min(1.0, max(0.0, optimal_winrate / _OT["WINRATE_SCORE_MAX"]))
        volume_score = min(1.0, math.log(1 + optimal_trades) / math.log(_OT["VOLUME_SCORE_MAX_TRADES"]))
        nrt = _OT["NEUTRAL_RATE_TARGET"]
        nrp = _OT["NEUTRAL_RATE_OVER_PENALTY"]
        neutral_score = min(1.0, optimal_neutralrate / nrt) if optimal_neutralrate <= nrt else max(0.0, 1.0 - (optimal_neutralrate - nrt) / nrp)

        jw = _OT["J_SCORE_WEIGHTS"]
        final_score = jw["ROI"] * roi_score + jw["WINRATE"] * winrate_score + 0.15 * volume_score + 0.05 * neutral_score

        quality = _OT["QUALITY_BASE"] + _OT["QUALITY_SCALE"] * min(1.0, max(0.0, final_score))
        
        return round(quality, 3)
    
    def analyze_wallet(self, wallet_address):
        """Analyse complète d'un wallet."""
        
        tier_data = self.get_wallet_tier_data(wallet_address)
        if not tier_data:
            return None
            
        reliable_tiers = self.filter_reliable_tiers(tier_data)
        if not reliable_tiers:
            return {
                'wallet_address': wallet_address,
                'optimal_threshold': None,
                'quality': 0.0,
                'status': 'NO_RELIABLE_TIERS',
                'reliable_tiers_count': 0,
                'j_scores': {},
                'tier_data': {}
            }
        
        j_scores = self.calculate_j_scores(reliable_tiers)
        
        optimal_threshold = self.find_optimal_threshold(j_scores, reliable_tiers)
        
        quality = self.calculate_quality(wallet_address, optimal_threshold, reliable_tiers)
        
        st = _OT["STATUS_THRESHOLDS"]
        if quality >= st["EXCEPTIONAL"]:
            status = 'EXCEPTIONAL'
        elif quality >= st["EXCELLENT"]:
            status = 'EXCELLENT'
        elif quality >= st["GOOD"]:
            status = 'GOOD'
        elif quality >= st["AVERAGE"]:
            status = 'AVERAGE'
        elif quality == st["NEUTRAL"]:
            status = 'NEUTRAL'
        else:
            status = 'POOR'
            
        return {
            'wallet_address': wallet_address,
            'optimal_threshold': optimal_threshold,
            'quality': round(quality, 3),
            'status': status,
            'reliable_tiers_count': len(reliable_tiers),
            'j_scores': {k: round(v, 3) for k, v in j_scores.items()},
            'tier_data': reliable_tiers
        }
    
    def analyze_all_qualified_wallets(self, quality_filter=None):
        """Analyse tous les wallets qualifiés avec filtre de qualité optionnel."""
        
        if quality_filter is not None:
            self.filter_quality_min = quality_filter
            
        logger.info(f"ANALYSE DES SEUILS OPTIMAUX | filtre qualité: {self.filter_quality_min}")

        conn = sqlite3.connect(DB_PATH)
        query = "SELECT wallet_address FROM wallet_qualified ORDER BY final_score DESC"
        qualified_wallets = conn.execute(query).fetchall()
        conn.close()

        if not qualified_wallets:
            logger.warning("Aucun wallet qualifié trouvé")
            return []

        logger.info(f"Analyse de {len(qualified_wallets)} wallets qualifiés")
        
        results = []
        filtered_results = []
        
        for wallet_row in qualified_wallets:
            wallet_address = wallet_row[0]
            result = self.analyze_wallet(wallet_address)
            
            if result:
                results.append(result)
                
                if result['quality'] >= self.filter_quality_min:
                    filtered_results.append(result)
                    
                    threshold_str = f"{result['optimal_threshold']}K" if result['optimal_threshold'] else "N/A"
                    logger.info(f"{wallet_address[:10]}...{wallet_address[-8:]} seuil={threshold_str} qualité={result['quality']:.3f} statut={result['status']} paliers_fiables={result['reliable_tiers_count']}")
                    if result['j_scores']:
                        scores_str = " | ".join([f"{k}K:{v:.2f}" for k, v in result['j_scores'].items()])
                        logger.info(f"  J_t: {scores_str}")
        
        self.save_to_smart_wallets(results)
        
        if self.filter_quality_min > 0:
            logger.info(f"Wallets exceptionnels (qualité ≥ {self.filter_quality_min}): {len(filtered_results)}/{len(results)} ({len(filtered_results)/len(results)*100:.1f}%)")
            self.display_global_stats(filtered_results)

        logger.info("STATISTIQUES GLOBALES")
        self.display_global_stats(results)
        
        return filtered_results if self.filter_quality_min > 0 else results
    
    def display_global_stats(self, results):
        """Affiche les statistiques globales."""
        if not results:
            return

        status_counts = {}
        qualities = []
        thresholds = []

        for result in results:
            status = result['status']
            status_counts[status] = status_counts.get(status, 0) + 1
            qualities.append(result['quality'])
            if result['optimal_threshold']:
                thresholds.append(result['optimal_threshold'])

        for status, count in sorted(status_counts.items()):
            pct = count / len(results) * 100
            logger.info(f"  {status}: {count} wallets ({pct:.1f}%)")

        if qualities:
            logger.info(f"Qualité moy={sum(qualities)/len(qualities):.3f} médiane={sorted(qualities)[len(qualities)//2]:.3f} max={max(qualities):.3f}")

        if thresholds:
            threshold_counts = {}
            for t in thresholds:
                threshold_counts[t] = threshold_counts.get(t, 0) + 1
            logger.info(f"Seuil moy={sum(thresholds)/len(thresholds):.1f}K médian={sorted(thresholds)[len(thresholds)//2]}K")
            for threshold in sorted(threshold_counts.keys()):
                count = threshold_counts[threshold]
                pct = count / len(thresholds) * 100
                logger.info(f"  {threshold}K: {count} wallets ({pct:.1f}%)")
    
    def save_to_smart_wallets(self, results):
        """Sauvegarde les résultats dans la table smart_wallets."""
        
        if not results:
            logger.warning("Aucun résultat à sauvegarder")
            return
            
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30.0)
            
            conn.execute("DELETE FROM smart_wallets")
            
            insert_query = """
                INSERT INTO smart_wallets (
                    wallet_address, optimal_threshold_tier, quality_score, threshold_status,
                    optimal_roi, optimal_winrate, optimal_trades, 
                    optimal_gagnants, optimal_perdants, optimal_neutres,
                    global_roi, global_winrate, global_trades,
                    j_score_max, j_score_avg, reliable_tiers_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            inserted_count = 0
            
            for result in results:
                if result['quality'] < _OT["MIN_INSERT_QUALITY"]:
                    continue
                j_scores = result.get('j_scores', {})
                j_score_max = max(j_scores.values()) if j_scores else 0.0
                j_score_avg = sum(j_scores.values()) / len(j_scores) if j_scores else 0.0
                
                optimal_roi = 0.0
                optimal_winrate = 0.0
                optimal_trades = 0
                optimal_gagnants = 0
                optimal_perdants = 0
                optimal_neutres = 0
                
                if result['optimal_threshold'] and result['tier_data']:
                    optimal_tier_data = result['tier_data'].get(result['optimal_threshold'])
                    if optimal_tier_data:
                        optimal_roi = optimal_tier_data['roi']
                        optimal_winrate = optimal_tier_data['winrate']
                        optimal_trades = optimal_tier_data['trades']
                        optimal_gagnants = optimal_tier_data['gagnants']
                        optimal_perdants = optimal_tier_data['perdants']
                        optimal_neutres = optimal_tier_data['neutres']

                if optimal_roi < _OT["MIN_OPTIMAL_ROI"] or optimal_winrate < _OT["MIN_OPTIMAL_WINRATE"]:
                    continue

                global_roi = 0.0
                global_winrate = 0.0
                global_trades = 0
                
                if result['tier_data']:
                    total_trades_global = sum(data['trades'] for data in result['tier_data'].values())
                    total_wins_global = sum((data['winrate'] / 100) * data['trades'] 
                                          for data in result['tier_data'].values())
                    all_rois = [data['roi'] for data in result['tier_data'].values()]
                    
                    global_trades = total_trades_global
                    global_winrate = (total_wins_global / total_trades_global * 100) if total_trades_global > 0 else 0.0
                    global_roi = sum(all_rois) / len(all_rois) if all_rois else 0.0
                
                optimal_threshold = result['optimal_threshold'] or 0
                
                insert_data = (
                    result['wallet_address'],
                    optimal_threshold,
                    result['quality'],
                    result['status'],
                    optimal_roi,
                    optimal_winrate,
                    optimal_trades,
                    optimal_gagnants,
                    optimal_perdants,
                    optimal_neutres,
                    global_roi,
                    global_winrate,
                    global_trades,
                    j_score_max,
                    j_score_avg,
                    result['reliable_tiers_count']
                )
                
                conn.execute(insert_query, insert_data)
                inserted_count += 1
            
            conn.commit()
            conn.close()
            
            filtered_count = len(results) - inserted_count
            logger.info(f"{inserted_count} wallets insérés dans smart_wallets (quality ≥ {_OT['MIN_INSERT_QUALITY']})")
            if filtered_count > 0:
                logger.info(f"{filtered_count} wallets filtrés (quality < {_OT['MIN_INSERT_QUALITY']})")

        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                logger.warning("Base verrouillée, abandon sauvegarde")
            else:
                logger.error(f"Erreur SQL: {e}")
        except Exception as e:
            logger.error(f"Erreur sauvegarde: {e}")
    
    def get_smart_wallets_threshold_stats(self):
        """Affiche les statistiques des seuils optimaux depuis smart_wallets."""
        
        try:
            conn = sqlite3.connect(DB_PATH)
            
            general_stats_query = """
                SELECT 
                    COUNT(*) as total,
                    AVG(quality_score) as avg_quality,
                    AVG(optimal_threshold_tier) as avg_threshold,
                    AVG(optimal_roi) as avg_optimal_roi,
                    AVG(optimal_winrate) as avg_optimal_winrate,
                    AVG(global_roi) as avg_global_roi,
                    AVG(global_winrate) as avg_global_winrate
                FROM smart_wallets
            """
            
            general_stats = conn.execute(general_stats_query).fetchone()
            
            status_query = """
                SELECT threshold_status, COUNT(*) as count
                FROM smart_wallets
                GROUP BY threshold_status
                ORDER BY count DESC
            """
            
            status_stats = conn.execute(status_query).fetchall()
            
            top_wallets_query = """
                SELECT wallet_address, optimal_threshold_tier, quality_score, 
                       optimal_roi, optimal_winrate, threshold_status
                FROM smart_wallets
                WHERE optimal_threshold_tier > 0
                ORDER BY quality_score DESC, optimal_roi DESC
                LIMIT 5
            """
            
            top_wallets = conn.execute(top_wallets_query).fetchall()
            
            conn.close()
            
            if general_stats and general_stats[0] > 0:
                logger.info(f"STATS smart_wallets: total={general_stats[0]} qualité_moy={general_stats[1]:.3f} seuil_moy={general_stats[2]:.1f}K roi_opt_moy={general_stats[3]:.1f}% wr_opt_moy={general_stats[4]:.1f}%")
                for status, count in status_stats:
                    pct = count / general_stats[0] * 100
                    logger.info(f"  {status}: {count} wallets ({pct:.1f}%)")
                if top_wallets:
                    logger.info("TOP 5 WALLETS:")
                    for i, wallet in enumerate(top_wallets, 1):
                        addr_short = wallet[0][:10] + "..." + wallet[0][-8:]
                        logger.info(f"  {i}. {addr_short} seuil={wallet[1]}K qualité={wallet[2]:.3f} roi={wallet[3]:.1f}% wr={wallet[4]:.1f}% {wallet[5]}")
            else:
                logger.warning("Aucune donnée dans smart_wallets")

        except Exception as e:
            logger.error(f"Erreur lecture stats smart_wallets: {e}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyseur de Seuils Optimaux')
    parser.add_argument('--quality-filter', type=float, default=0.0,
                       help='Filtre qualité minimum (0.9 pour exceptionnels uniquement)')
    parser.add_argument('--show-stats', action='store_true',
                       help='Afficher les statistiques de smart_wallets')
    
    args = parser.parse_args()
    
    analyzer = OptimalThresholdAnalyzer()
    results = analyzer.analyze_all_qualified_wallets(quality_filter=args.quality_filter)
    
    if args.show_stats:
        analyzer.get_smart_wallets_threshold_stats()
    
    if args.quality_filter > 0:
        logger.info(f"RÉSUMÉ: {len(results)} wallets exceptionnels (qualité ≥ {args.quality_filter})")
        for result in results[:5]:
            threshold_str = f"{result['optimal_threshold']}K" if result['optimal_threshold'] else "N/A"
            logger.info(f"  {result['wallet_address'][:10]}... {threshold_str} Q={result['quality']:.3f}")
    else:
        logger.info(f"RÉSUMÉ: {len(results)} wallets analysés au total")
