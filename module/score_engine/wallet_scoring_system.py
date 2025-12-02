#!/usr/bin/env python3
"""
SYSTÈME DE SCORING INTELLIGENT DES WALLETS
Calcule un score composite basé sur :
- ROI pondéré par investissement
- Nombre de trades (activité)
- Taux de réussite (>= 80% = gagnant)
Filtre les wallets avec < 50% ROI pondéré
Adapte le scoring selon le palier d'investissement
"""

import sqlite3
import math
from pathlib import Path

# Configuration
ROOT_DIR = Path(__file__).parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "db" / "wit_database.db"

def calculate_wallet_score(wallet_address):
    """
    Calcule le score d'un wallet directement depuis token_analytics
    
    Args:
        wallet_address: Adresse du wallet
    
    Returns:
        dict: Score et métriques détaillées ou None si non qualifié
    """
    
    conn = sqlite3.connect(DB_PATH)
    
    # Récupérer TOUS les trades du wallet (sans filtre de palier)
    query = """
        SELECT token_symbol, total_invested, roi_percentage
        FROM token_analytics 
        WHERE wallet_address = ?
        AND token_symbol NOT IN ('USDC', 'USDT', 'DAI', 'BUSD', 'ETH', 'WETH', 'BTC', 'WBTC', 'BNB')
        ORDER BY total_invested DESC
    """
    
    tokens = conn.execute(query, [wallet_address]).fetchall()
    conn.close()
    
    if not tokens:
        return None
    
    # Calculs des métriques de base
    nb_trades = len(tokens)
    total_invested = sum(t[1] for t in tokens)
    
    # ROI pondéré par investissement
    weighted_roi = sum(t[1] * t[2] for t in tokens) / total_invested if total_invested > 0 else 0
    
    # FILTRE: Éliminer les wallets avec ROI pondéré < 50%
    if weighted_roi < 50:
        return None
    
    # Classification des trades
    gagnants = sum(1 for t in tokens if t[2] >= 80)    # >= 80% ROI
    perdants = sum(1 for t in tokens if t[2] < 0)      # ROI négatif
    neutres = nb_trades - gagnants - perdants           # 0-80% ROI
    
    # Taux de réussite
    taux_reussite = (gagnants / nb_trades * 100) if nb_trades > 0 else 0
    
    # === CALCUL DU SCORE COMPOSITE ===
    
    # 1. Score ROI (0-100 points)
    # Normalisation: 50% = 0 points, 200% = 50 points, 500%+ = 100 points
    roi_score = min(100, max(0, (weighted_roi - 50) / 4.5))
    
    # 2. Score Activité (0-100 points) 
    # Normalisation logarithmique: 1 trade = 0, 5 trades = 50, 20+ trades = 100
    activity_score = min(100, max(0, math.log(nb_trades) / math.log(20) * 100)) if nb_trades > 0 else 0
    
    # 3. Score Taux de Réussite (0-100 points)
    # Normalisation: 0% = 0 points, 25% = 50 points, 50%+ = 100 points
    success_score = min(100, max(0, taux_reussite * 2))
    
    # 4. Bonus Qualité (0-50 points bonus)
    # Récompense les wallets avec beaucoup de gagnants et peu de perdants
    ratio_gagnants = gagnants / nb_trades if nb_trades > 0 else 0
    ratio_perdants = perdants / nb_trades if nb_trades > 0 else 0
    quality_bonus = (ratio_gagnants - ratio_perdants) * 50
    quality_bonus = max(0, min(50, quality_bonus))
    
    # === SCORE FINAL PONDÉRÉ ===
    # ROI (40%) + Activité (25%) + Réussite (25%) + Qualité (10%)
    final_score = (
        roi_score * 0.40 +
        activity_score * 0.25 +
        success_score * 0.25 +
        quality_bonus * 0.10
    )
    
    # Classification du wallet
    if final_score >= 80:
        classification = "ELITE"
    elif final_score >= 60:
        classification = "EXCELLENT"
    elif final_score >= 40:
        classification = "BON"
    elif final_score >= 20:
        classification = "MOYEN"
    else:
        classification = "FAIBLE"
    
    return {
        'wallet_address': wallet_address,
        'final_score': round(final_score, 2),
        'classification': classification,
        'weighted_roi': round(weighted_roi, 2),
        'nb_trades': nb_trades,
        'taux_reussite': round(taux_reussite, 2),
        'total_invested': total_invested,
        'gagnants': gagnants,
        'perdants': perdants,
        'neutres': neutres,
        'roi_score': round(roi_score, 2),
        'activity_score': round(activity_score, 2),
        'success_score': round(success_score, 2),
        'quality_bonus': round(quality_bonus, 2)
    }

def score_all_wallets(min_score=0):
    """
    Score tous les wallets et les classe par performance
    
    Args:
        min_score: Score minimum pour apparaître dans les résultats
    
    Returns:
        list: Liste des wallets scorés, triés par score décroissant
    """
    
    print(f"🚀 SCORING TOUS LES WALLETS")
    print("=" * 80)
    
    # Récupérer tous les wallets uniques
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT DISTINCT wallet_address
        FROM token_analytics 
        WHERE token_symbol NOT IN ('USDC', 'USDT', 'DAI', 'BUSD', 'ETH', 'WETH', 'BTC', 'WBTC', 'BNB')
    """
    wallets = conn.execute(query).fetchall()
    conn.close()
    
    print(f"📊 {len(wallets)} wallets candidats")
    
    # Scorer chaque wallet
    scored_wallets = []
    qualified_count = 0
    
    for wallet in wallets:
        score_data = calculate_wallet_score(wallet[0])
        if score_data and score_data['final_score'] >= min_score:
            scored_wallets.append(score_data)
            qualified_count += 1
    
    # Trier par score décroissant
    scored_wallets.sort(key=lambda x: x['final_score'], reverse=True)
    
    print(f"✅ {qualified_count} wallets qualifiés (ROI pondéré ≥ 50%)")
    print(f"📈 Score minimum: {min_score}")
    
    return scored_wallets

def display_top_wallets(scored_wallets, top_n=20):
    """Affiche le top N des wallets"""
    
    print(f"\n🏆 TOP {top_n} WALLETS")
    print("=" * 120)
    print(f"{'Rang':<4} {'Wallet':<45} {'Score':<6} {'Class':<9} {'ROI%':<7} {'Trades':<7} {'Réuss%':<7} {'G/P/N':<8}")
    print("=" * 120)
    
    for i, wallet in enumerate(scored_wallets[:top_n], 1):
        wallet_short = wallet['wallet_address'][:10] + "..." + wallet['wallet_address'][-8:]
        gpn = f"{wallet['gagnants']}/{wallet['perdants']}/{wallet['neutres']}"
        
        print(f"{i:<4} {wallet_short:<45} {wallet['final_score']:<6.1f} "
              f"{wallet['classification']:<9} {wallet['weighted_roi']:<7.1f} "
              f"{wallet['nb_trades']:<7} {wallet['taux_reussite']:<7.1f} {gpn:<8}")

def analyze_score_distribution(scored_wallets):
    """Analyse la distribution des scores"""
    
    if not scored_wallets:
        print("❌ Aucun wallet à analyser")
        return
    
    print(f"\n📊 DISTRIBUTION DES SCORES")
    print("=" * 50)
    
    # Statistiques générales
    scores = [w['final_score'] for w in scored_wallets]
    rois = [w['weighted_roi'] for w in scored_wallets]
    
    print(f"Score moyen: {sum(scores)/len(scores):.2f}")
    print(f"Score médian: {sorted(scores)[len(scores)//2]:.2f}")
    print(f"Score max: {max(scores):.2f}")
    print(f"ROI moyen: {sum(rois)/len(rois):.2f}%")
    
    # Distribution par classification
    classifications = {}
    for wallet in scored_wallets:
        classif = wallet['classification']
        classifications[classif] = classifications.get(classif, 0) + 1
    
    print(f"\nDistribution par classe:")
    for classif, count in sorted(classifications.items(), 
                                key=lambda x: ['ELITE', 'EXCELLENT', 'BON', 'MOYEN', 'FAIBLE'].index(x[0])):
        pct = count / len(scored_wallets) * 100
        print(f"  {classif}: {count} wallets ({pct:.1f}%)")

def save_qualified_wallets(scored_wallets):
    """Sauvegarde les wallets qualifiés dans la table wallet_qualified"""
    
    if not scored_wallets:
        print("❌ Aucun wallet à sauvegarder")
        return
    
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        
        # Vider la table avant d'insérer les nouveaux résultats
        conn.execute("DELETE FROM wallet_qualified")
        
        # Préparer les données pour l'insertion
        insert_query = """
            INSERT INTO wallet_qualified (
                wallet_address, final_score, classification,
                weighted_roi, nb_trades, taux_reussite, total_invested,
                gagnants, perdants, neutres,
                roi_score, activity_score, success_score, quality_bonus
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Insérer chaque wallet qualifié
        for wallet in scored_wallets:
            data = (
                wallet['wallet_address'],
                wallet['final_score'],
                wallet['classification'],
                wallet['weighted_roi'],
                wallet['nb_trades'],
                wallet['taux_reussite'],
                wallet['total_invested'],
                wallet['gagnants'],
                wallet['perdants'],
                wallet['neutres'],
                wallet['roi_score'],
                wallet['activity_score'],
                wallet['success_score'],
                wallet['quality_bonus']
            )
            conn.execute(insert_query, data)
        
        conn.commit()
        conn.close()
        
        print(f"✅ {len(scored_wallets)} wallets qualifiés sauvegardés dans wallet_qualified")
        
    except sqlite3.OperationalError as e:
        if "database is locked" in str(e):
            print(f"⚠️ Base verrouillée, abandon sauvegarde")
        else:
            print(f"❌ Erreur SQL: {e}")
    except Exception as e:
        print(f"❌ Erreur sauvegarde: {e}")

def get_qualified_wallets_stats():
    """Affiche les statistiques des wallets qualifiés en base"""
    
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Statistiques générales
        stats_query = """
            SELECT 
                COUNT(*) as total,
                AVG(final_score) as avg_score,
                AVG(weighted_roi) as avg_roi,
                AVG(nb_trades) as avg_trades,
                MAX(final_score) as max_score,
                MIN(final_score) as min_score
            FROM wallet_qualified
        """
        stats = conn.execute(stats_query).fetchone()
        
        # Distribution par classification
        classif_query = """
            SELECT classification, COUNT(*) as count
            FROM wallet_qualified
            GROUP BY classification
            ORDER BY 
                CASE classification
                    WHEN 'ELITE' THEN 1
                    WHEN 'EXCELLENT' THEN 2
                    WHEN 'BON' THEN 3
                    WHEN 'MOYEN' THEN 4
                    WHEN 'FAIBLE' THEN 5
                END
        """
        classifs = conn.execute(classif_query).fetchall()
        
        conn.close()
        
        if stats[0] > 0:
            print(f"\n📊 STATISTIQUES WALLET_QUALIFIED")
            print("=" * 50)
            print(f"Total wallets: {stats[0]}")
            print(f"Score moyen: {stats[1]:.2f}")
            print(f"ROI moyen: {stats[2]:.1f}%")
            print(f"Trades moyen: {stats[3]:.1f}")
            print(f"Score max: {stats[4]:.2f}")
            print(f"Score min: {stats[5]:.2f}")
            
            print(f"\nDistribution par classe:")
            for classif, count in classifs:
                pct = count / stats[0] * 100
                print(f"  {classif}: {count} wallets ({pct:.1f}%)")
        else:
            print("❌ Aucun wallet en base")
            
    except Exception as e:
        print(f"❌ Erreur lecture stats: {e}")

if __name__ == "__main__":
    # Analyse de tous les wallets sans palier
    scored_wallets = score_all_wallets(min_score=20)
    
    if scored_wallets:
        display_top_wallets(scored_wallets, top_n=20)
        analyze_score_distribution(scored_wallets)
        
        # Sauvegarder en base
        save_qualified_wallets(scored_wallets)
        
        # Vérifier la sauvegarde
        get_qualified_wallets_stats()