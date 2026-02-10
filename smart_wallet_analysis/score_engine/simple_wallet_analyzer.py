#!/usr/bin/env python3
"""
ANALYSEUR SIMPLE - PALIERS 3K-12K - WALLETS QUALIFIÉS UNIQUEMENT
Calcul et affichage pour chaque wallet QUALIFIÉ :
- ROI
- Taux de réussite
- Nombre de trades
- Trades gagnants (≥50%)
- Trades perdants (<-20%)
- Trades neutres (-20% à 50% - bruit de marché)
Pour chaque palier de 3K à 12K par pas de 1000€
Utilise uniquement les wallets de la table wallet_qualified
"""

import sqlite3
from pathlib import Path

# Configuration
ROOT_DIR = Path(__file__).parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "db" / "wit_database.db"

def analyze_wallet_simple(wallet_address):
    """Analyse simple d'un wallet qualifié sur tous les paliers"""
    
    # Récupérer les données du wallet
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT token_symbol, total_invested, roi_percentage
        FROM token_analytics
        WHERE wallet_address = ?
        AND token_symbol NOT IN ('USDC', 'USDT', 'DAI', 'USDAI', 'BUSD', 'ETH', 'WETH', 'BTC', 'WBTC', 'BNB')
        ORDER BY total_invested DESC
    """
    tokens = conn.execute(query, [wallet_address]).fetchall()
    conn.close()
    
    if not tokens:
        print("❌ Aucun token trouvé")
        return
    
    # Stocker les résultats pour sauvegarde
    tier_results = {}
    
    # Analyser chaque palier (commence à 1K au lieu de 3K)
    for tier in range(1000, 13000, 1000):
        # Filtrer tokens pour ce palier
        tier_tokens = [t for t in tokens if t[1] >= tier]
        
        if not tier_tokens:
            tier_results[f"tier_{tier//1000}k"] = {
                'roi': 0, 'taux_reussite': 0, 'nb_trades': 0,
                'gagnants': 0, 'perdants': 0, 'neutres': 0
            }
            continue
            
        # Calculs
        nb_trades = len(tier_tokens)

        # Classification
        gagnants = sum(1 for t in tier_tokens if t[2] >= 50)     # >= 50% ROI (vraies victoires)
        perdants = sum(1 for t in tier_tokens if t[2] < -20)     # < -20% ROI (vraies pertes)
        neutres = nb_trades - gagnants - perdants                 # -20% à 50% ROI (bruit de marché)
        
        # ROI moyen pondéré par investissement
        total_invested = sum(t[1] for t in tier_tokens)
        weighted_roi = sum(t[1] * t[2] for t in tier_tokens) / total_invested if total_invested > 0 else 0
        
        # Taux de réussite (sur le total de trades, neutres inclus)
        taux_reussite = (gagnants / nb_trades * 100) if nb_trades > 0 else 0
        
        # Stocker
        tier_results[f"tier_{tier//1000}k"] = {
            'roi': weighted_roi,
            'taux_reussite': taux_reussite,
            'nb_trades': nb_trades,
            'gagnants': gagnants,
            'perdants': perdants,
            'neutres': neutres
        }
        
        # Affichage
        print(f"   Palier {tier//1000}K: ROI={weighted_roi:+.1f}% | Taux={taux_reussite:.1f}% | "
              f"Trades={nb_trades} | G={gagnants} P={perdants} N={neutres}")
    
    # Sauvegarder en base (uniquement pour les wallets qualifiés)
    save_wallet_profile(wallet_address, tier_results)

def analyze_qualified_wallets():
    """Analyse uniquement les wallets qualifiés"""
    
    print("🚀 ANALYSE SIMPLE - WALLETS QUALIFIÉS UNIQUEMENT")
    print("=" * 80)
    
    # Récupérer UNIQUEMENT les wallets qualifiés
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT wallet_address, classification, final_score, weighted_roi
        FROM wallet_qualified 
        ORDER BY final_score DESC
    """
    qualified_wallets = conn.execute(query).fetchall()
    conn.close()
    
    if not qualified_wallets:
        print("❌ Aucun wallet qualifié trouvé. Exécutez d'abord wallet_scoring_system.py")
        return
    
    print(f"📊 {len(qualified_wallets)} wallets qualifiés à analyser")
    print(f"🏆 Score moyen des qualifiés: {sum(w[2] for w in qualified_wallets) / len(qualified_wallets):.1f}")
    print(f"💰 ROI moyen des qualifiés: {sum(w[3] for w in qualified_wallets) / len(qualified_wallets):.1f}%")
    print()
    
    # Analyser chaque wallet qualifié
    for wallet_data in qualified_wallets:
        wallet_address = wallet_data[0]
        classification = wallet_data[1]
        score = wallet_data[2]
        print(f"🔍 WALLET: {wallet_address} | {classification} | Score: {score:.1f}")
        analyze_wallet_simple(wallet_address)

def save_wallet_profile(wallet_address, tier_results):
    """Sauvegarde le profil d'un wallet en base"""
    
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        
        # Préparer les données pour l'insert/update
        data = [wallet_address]
        
        # Ajouter les données pour chaque palier (1k à 12k)
        for tier_num in range(1, 13):
            tier_key = f"tier_{tier_num}k"
            if tier_key in tier_results:
                results = tier_results[tier_key]
                data.extend([
                    results['roi'],
                    results['taux_reussite'],
                    results['nb_trades'],
                    results['gagnants'],
                    results['perdants'],
                    results['neutres']
                ])
            else:
                # Valeurs par défaut si pas de données pour ce palier
                data.extend([0, 0, 0, 0, 0, 0])
        
        # Requête INSERT OR REPLACE
        query = """
            INSERT OR REPLACE INTO wallet_profiles (
                wallet_address,
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
            ) VALUES (
                ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?
            )
        """
        
        conn.execute(query, data)
        conn.commit()
        conn.close()
        
        print(f"✅ Profil sauvegardé pour {wallet_address}")
        
    except sqlite3.OperationalError as e:
        if "database is locked" in str(e):
            print(f"⚠️ Base verrouillée pour {wallet_address}, abandon")
        else:
            print(f"❌ Erreur SQL pour {wallet_address}: {e}")
    except Exception as e:
        print(f"❌ Erreur pour {wallet_address}: {e}")

if __name__ == "__main__":
    analyze_qualified_wallets()