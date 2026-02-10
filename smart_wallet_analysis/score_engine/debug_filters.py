#!/usr/bin/env python3
"""Debug script pour voir où les wallets sont rejetés"""

import sqlite3
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent
DB_PATH = ROOT_DIR / "data" / "db" / "wit_database.db"

def debug_wallet_filters(wallet_address):
    """Debug pourquoi un wallet est rejeté"""

    conn = sqlite3.connect(DB_PATH)

    # Portfolio value
    portfolio_query = "SELECT total_portfolio_value FROM wallets WHERE wallet_address = ?"
    portfolio_result = conn.execute(portfolio_query, [wallet_address]).fetchone()
    portfolio_value = portfolio_result[0] if portfolio_result else 0

    # Tokens
    query = """
        SELECT token_symbol, total_invested, roi_percentage
        FROM token_analytics
        WHERE wallet_address = ?
        AND token_symbol NOT IN ('USDC', 'USDT', 'DAI', 'BUSD', 'ETH', 'WETH', 'BTC', 'WBTC', 'BNB')
        ORDER BY total_invested DESC
    """
    tokens = conn.execute(query, [wallet_address]).fetchall()
    conn.close()

    print(f"\n{'='*80}")
    print(f"🔍 WALLET: {wallet_address[:10]}...{wallet_address[-8:]}")
    print(f"{'='*80}")

    if not tokens:
        print("❌ REJET: Aucun token")
        return

    nb_trades = len(tokens)
    total_invested = sum(t[1] for t in tokens)

    print(f"📊 Nombre de trades: {nb_trades}")
    print(f"💰 Portfolio value: ${portfolio_value:,.0f}")
    print(f"💵 Total investi: ${total_invested:,.0f}")

    # FILTRE 1: Minimum 5 trades
    if nb_trades < 5:
        print(f"❌ REJET FILTRE 1: Seulement {nb_trades} trades (minimum 5)")
        return
    else:
        print(f"✅ FILTRE 1: {nb_trades} trades >= 5")

    # FILTRE 2: Au moins 3 trades gagnants (ROI > 50%)
    gagnants_significatifs = sum(1 for t in tokens if t[2] > 50)
    if gagnants_significatifs < 3:
        print(f"❌ REJET FILTRE 2: Seulement {gagnants_significatifs} trades gagnants (minimum 3 avec ROI > 50%)")
        return
    else:
        print(f"✅ FILTRE 2: {gagnants_significatifs} trades gagnants >= 3")

    # FILTRE 3: Concentration (uniquement trades positifs)
    roi_contributions_positive = []
    for t in tokens:
        if t[2] > 0:
            contribution = (t[1] * t[2])
            roi_contributions_positive.append(contribution)

    if len(roi_contributions_positive) >= 2:
        roi_contributions_sorted = sorted(roi_contributions_positive, reverse=True)
        total_positive_contribution = sum(roi_contributions_sorted)

        if total_positive_contribution > 0:
            top2_contribution = sum(roi_contributions_sorted[:2])
            concentration_ratio = top2_contribution / total_positive_contribution
            print(f"📈 Trades positifs: {len(roi_contributions_positive)}")
            print(f"📈 Concentration top 2 trades positifs: {concentration_ratio*100:.1f}%")
            if concentration_ratio > 0.70:
                print(f"❌ REJET FILTRE 3: Concentration {concentration_ratio*100:.1f}% > 70% (trop de chance)")
                return
            else:
                print(f"✅ FILTRE 3: Concentration {concentration_ratio*100:.1f}% <= 70%")
    else:
        print(f"⚠️ FILTRE 3: Moins de 2 trades positifs, skip")

    # FILTRE 4: Médiane vs Moyenne (uniquement si les deux positifs)
    roi_values = [t[2] for t in tokens]
    roi_values_sorted = sorted(roi_values)
    median_roi = roi_values_sorted[len(roi_values_sorted) // 2]
    mean_roi = sum(roi_values) / len(roi_values)

    print(f"📊 ROI médian: {median_roi:.1f}%")
    print(f"📊 ROI moyen: {mean_roi:.1f}%")

    if mean_roi > 0 and median_roi > 0:
        median_mean_ratio = median_roi / mean_roi
        print(f"📊 Ratio médiane/moyenne: {median_mean_ratio*100:.1f}%")
        if median_mean_ratio < 0.30:
            print(f"❌ REJET FILTRE 4: Ratio {median_mean_ratio*100:.1f}% < 30% (valeurs extrêmes)")
            return
        else:
            print(f"✅ FILTRE 4: Ratio {median_mean_ratio*100:.1f}% >= 30%")
    else:
        print(f"⚠️ FILTRE 4: Médiane ou moyenne négative, skip (le filtre 5 fera le tri)")

    # ROI pondéré
    weighted_roi = sum(t[1] * t[2] for t in tokens) / total_invested if total_invested > 0 else 0
    print(f"📈 ROI pondéré: {weighted_roi:.1f}%")

    # FILTRE 5: ROI >= 50%
    if weighted_roi < 50:
        print(f"❌ REJET FILTRE 5: ROI pondéré {weighted_roi:.1f}% < 50%")
        return
    else:
        print(f"✅ FILTRE 5: ROI pondéré {weighted_roi:.1f}% >= 50%")

    # FILTRE 6: Portfolio >= 150k
    if portfolio_value < 150000:
        print(f"❌ REJET FILTRE 6: Portfolio ${portfolio_value:,.0f} < $150,000")
        return
    else:
        print(f"✅ FILTRE 6: Portfolio ${portfolio_value:,.0f} >= $150,000")

    print(f"\n🎉 WALLET QUALIFIÉ !")

if __name__ == "__main__":
    # Récupérer tous les wallets
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT DISTINCT wallet_address
        FROM token_analytics
        WHERE token_symbol NOT IN ('USDC', 'USDT', 'DAI', 'BUSD', 'ETH', 'WETH', 'BTC', 'WBTC', 'BNB')
        LIMIT 10
    """
    wallets = conn.execute(query).fetchall()
    conn.close()

    print(f"🚀 DEBUG DE {len(wallets)} WALLETS")

    for wallet in wallets:
        debug_wallet_filters(wallet[0])
