import os
import time
import requests
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import sqlite3

# Import database utilities MANUELLES
import sys
sys.path.append(str(Path(__file__).parent.parent.parent / "db"))
from database_utils_manual import insert_wallet, insert_token, get_wallet, DatabaseManager

# === Configuration globale ===
load_dotenv(dotenv_path=Path(__file__).parent.parent.parent / ".env")
API_KEY = os.getenv("ZERION_API_KEY_2")
if not API_KEY:
    raise ValueError("❌ Clé API manquante. Vérifie ton fichier .env (ZERION_API_KEY).")

# Configuration simplifiée pour analyse manuelle (SANS FILTRES)
ROOT = Path(__file__).parent.parent.parent
DB_PATH = ROOT / "data" / "db" / "wit_database_manual.db"  # BDD MANUELLE séparée

def get_fungible_id_zerion(contract_address, chain, token_symbol=""):
    """Récupère le fungible_id d'un token via l'API Zerion /fungibles"""
    
    # Cas spécial : ETH natif (pas de contract_address)
    if token_symbol.upper() == "ETH" and not contract_address:
        return "eth"  # ID standard pour ETH natif sur toutes les chains
    
    # Cas normal : token avec contract_address
    if not contract_address or not chain:
        return ""
    
    url = f"https://api.zerion.io/v1/fungibles/?filter[implementation_address]={contract_address.lower()}&filter[implementation_chain_id]={chain}"
    
    headers = {
        "accept": "application/json",
        "authorization": f"Basic {API_KEY}"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        fungibles = data.get("data", [])
        
        if fungibles:
            # Prendre le premier résultat (devrait être unique)
            fungible_id = fungibles[0].get("id", "")
            return fungible_id
        else:
            return ""
            
    except Exception as e:
        print(f"⚠️ Erreur récupération fungible_id pour {contract_address}: {e}")
        return ""

def get_token_balances_manual(address):
    """Récupère les balances d'un wallet via l'API Zerion SANS FILTRES pour analyse manuelle"""
    url = f"https://api.zerion.io/v1/wallets/{address}/positions/?filter[positions]=only_simple&currency=usd&filter[trash]=only_non_trash&sort=value"
    
    headers = {
        "accept": "application/json",
        "authorization": f"Basic {API_KEY}"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        all_positions = data.get("data", [])
        
        # Fonction utilitaire pour gérer les valeurs None
        def safe_float(value, default=0):
            if value is None:
                return default
            if isinstance(value, dict):
                return float(value.get("numeric", default))
            try:
                return float(value)
            except (TypeError, ValueError):
                return default
        
        # Calculer la valeur totale du wallet
        total_wallet_value = sum(
            safe_float(pos.get("attributes", {}).get("value"))
            for pos in all_positions
        )
        
        print(f"💰 Wallet {address}: ${total_wallet_value:,.0f} total value")
        print(f"🪙 {len(all_positions)} total positions")
        
        # Traiter TOUS les tokens (pas de filtres)
        all_tokens = []
        for pos in all_positions:
            attrs = pos.get("attributes", {})
            fungible_info = attrs.get("fungible_info", {})
            
            # Quantité avec gestion des None
            amount = safe_float(attrs.get("quantity"))
            
            # Valeur USD avec gestion des None  
            usd_value = safe_float(attrs.get("value"))
            
            # Token info
            token = fungible_info.get("symbol", "UNKNOWN")
            
            # Chain et contrat
            implementations = fungible_info.get("implementations", [])
            if implementations:
                chain = implementations[0].get("chain_id", "")
                contract_address = implementations[0].get("address", "")
                contract_decimals = implementations[0].get("decimals", "")
            else:
                chain = ""
                contract_address = ""
                contract_decimals = ""
            
            # Récupérer le fungible_id (passer le token symbol pour ETH natif)
            fungible_id = get_fungible_id_zerion(contract_address, chain, token)
            
            # Petit délai pour éviter de surcharger l'API
            time.sleep(0.1)
            
            all_tokens.append({
                "token": token.strip().upper(),
                "amount": amount,
                "usd_value": usd_value,
                "chain": chain,
                "contract_address": contract_address,
                "contract_decimals": contract_decimals,
                "fungible_id": fungible_id
            })
        
        print(f"✅ {len(all_tokens)} tokens extraits pour analyse manuelle")
        return pd.DataFrame(all_tokens), total_wallet_value
        
    except Exception as e:
        print(f"❌ Erreur Zerion API pour {address}: {e}")
        return pd.DataFrame(), 0

def extract_wallet_transaction_history(address, token_data):
    """Récupère l'historique des transactions pour un wallet donné"""
    from .wallet_token_history_simple import extract_wallet_transactions_history

    try:
        print(f"📊 Extraction historique pour {address}...")
        # Utiliser la fonction existante d'extraction d'historique
        success = extract_wallet_transactions_history(address, db_path=DB_PATH)
        return success
    except Exception as e:
        print(f"❌ Erreur extraction historique pour {address}: {e}")
        return False

def run_fifo_scoring(wallet_address):
    """Lance le scoring FIFO pour un wallet analysé manuellement"""
    try:
        print(f"🧮 Analyse FIFO pour {wallet_address}...")

        # Importer le moteur FIFO
        sys.path.append(str(Path(__file__).parent.parent / "score_engine"))
        from fifo_clean_simple import SimpleFIFOAnalyzer

        # Créer un analyseur pointant vers la BDD manuelle
        analyzer = SimpleFIFOAnalyzer()
        analyzer.db_path = DB_PATH  # Override pour utiliser la BDD manuelle

        # Analyser le wallet
        success = analyzer.analyze_wallet(wallet_address)

        if success:
            print(f"✅ Scoring FIFO terminé pour {wallet_address[:12]}...")
            return True
        else:
            print(f"⚠️ Aucune donnée FIFO pour {wallet_address[:12]}...")
            return False

    except Exception as e:
        print(f"❌ Erreur scoring FIFO pour {wallet_address}: {e}")
        return False

def calculate_wallet_profile(wallet_address):
    """Calcule le profil complet du wallet et le sauvegarde en BDD"""
    try:
        print(f"📈 Calcul du profil wallet pour {wallet_address}...")

        # Connexion à la BDD manuelle
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()

        # Récupérer toutes les analytics de tokens pour ce wallet
        cursor.execute("""
            SELECT * FROM token_analytics
            WHERE wallet_address = ?
        """, (wallet_address,))

        columns = [desc[0] for desc in cursor.description]
        tokens_data = [dict(zip(columns, row)) for row in cursor.fetchall()]

        if not tokens_data:
            print(f"⚠️ Aucune donnée analytics pour {wallet_address}")
            conn.close()
            return False

        # Calculs globaux
        total_invested = sum(t.get('total_invested', 0) for t in tokens_data)
        total_realized = sum(t.get('total_realized', 0) for t in tokens_data)
        current_value = sum(t.get('current_value', 0) for t in tokens_data)
        total_gains = total_realized + current_value
        profit_net = total_gains - total_invested

        # ROI global
        roi_global = (profit_net / total_invested * 100) if total_invested > 0 else 0

        # Répartition des tokens
        tokens_gagnants = sum(1 for t in tokens_data if t.get('is_winning') == 1)
        tokens_perdants = sum(1 for t in tokens_data if t.get('is_winning') == 0)
        tokens_neutres = len(tokens_data) - tokens_gagnants - tokens_perdants
        tokens_airdrops = sum(1 for t in tokens_data if t.get('is_airdrop') == 1)

        # Taux de réussite
        total_non_airdrop = len([t for t in tokens_data if not t.get('is_airdrop')])
        taux_reussite = (tokens_gagnants / total_non_airdrop * 100) if total_non_airdrop > 0 else 0

        # Dernière activité
        cursor.execute("""
            SELECT MAX(date) as last_activity
            FROM transaction_history
            WHERE wallet_address = ?
        """, (wallet_address,))
        last_activity_row = cursor.fetchone()

        if last_activity_row and last_activity_row[0]:
            last_activity = datetime.fromisoformat(last_activity_row[0])
            days_since_last = (datetime.now() - last_activity).days
        else:
            days_since_last = 999

        # Gains airdrops vs trading
        gains_airdrops = sum(t.get('total_gains', 0) for t in tokens_data if t.get('is_airdrop'))
        gains_trading = sum(t.get('total_gains', 0) for t in tokens_data if not t.get('is_airdrop'))

        # Ratio skill/chance
        ratio_skill_chance = (gains_trading / total_gains * 100) if total_gains > 0 else 50

        # Calcul par tiers d'investissement
        # Tier 1: < 3K, Tier 2: 3K-12K, Tier 3: > 12K
        tiers = {'petits': [], 'gros': [], 'whales': []}

        for token in tokens_data:
            invested = token.get('total_invested', 0)
            if invested < 3000:
                tiers['petits'].append(token)
            elif invested < 12000:
                tiers['gros'].append(token)
            else:
                tiers['whales'].append(token)

        def calc_tier_stats(tier_tokens):
            if not tier_tokens:
                return {
                    'count': 0, 'gagnants': 0, 'roi': 0, 'reussite': 0,
                    'investi': 0, 'retour': 0
                }

            count = len(tier_tokens)
            gagnants = sum(1 for t in tier_tokens if t.get('is_winning') == 1)
            investi = sum(t.get('total_invested', 0) for t in tier_tokens)
            retour = sum(t.get('total_gains', 0) for t in tier_tokens)
            roi = ((retour - investi) / investi * 100) if investi > 0 else 0
            reussite = (gagnants / count * 100) if count > 0 else 0

            return {
                'count': count,
                'gagnants': gagnants,
                'roi': roi,
                'reussite': reussite,
                'investi': investi,
                'retour': retour
            }

        petits_stats = calc_tier_stats(tiers['petits'])
        gros_stats = calc_tier_stats(tiers['gros'])
        whales_stats = calc_tier_stats(tiers['whales'])

        # Calculer le score total (simplifié)
        score_roi = min(roi_global / 2, 40) if roi_global > 0 else 0  # Max 40pts
        score_winrate = min(taux_reussite * 0.3, 30)  # Max 30pts
        score_consistency = min(tokens_gagnants / len(tokens_data) * 30, 20)  # Max 20pts
        score_activity = 10 if days_since_last < 30 else 5 if days_since_last < 90 else 0  # Max 10pts

        total_score = score_roi + score_winrate + score_consistency + score_activity

        # Insérer dans wallet_profiles
        cursor.execute("""
            INSERT OR REPLACE INTO wallet_profiles (
                wallet_address, total_score, roi_global, taux_reussite, jours_derniere_activite,
                capital_investi, gains_realises, valeur_actuelle, gains_totaux, profit_net,
                total_tokens, tokens_gagnants, tokens_neutres, tokens_perdants, tokens_airdrops,
                gains_airdrops, gains_trading, ratio_skill_chance,
                petits_count, petits_gagnants, petits_roi, petits_reussite, petits_investi, petits_retour,
                gros_count, gros_gagnants, gros_roi, gros_reussite, gros_investi, gros_retour,
                whales_count, whales_gagnants, whales_roi, whales_reussite, whales_investi, whales_retour
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            wallet_address, total_score, roi_global, taux_reussite, days_since_last,
            total_invested, total_realized, current_value, total_gains, profit_net,
            len(tokens_data), tokens_gagnants, tokens_neutres, tokens_perdants, tokens_airdrops,
            gains_airdrops, gains_trading, ratio_skill_chance,
            petits_stats['count'], petits_stats['gagnants'], petits_stats['roi'], petits_stats['reussite'],
            petits_stats['investi'], petits_stats['retour'],
            gros_stats['count'], gros_stats['gagnants'], gros_stats['roi'], gros_stats['reussite'],
            gros_stats['investi'], gros_stats['retour'],
            whales_stats['count'], whales_stats['gagnants'], whales_stats['roi'], whales_stats['reussite'],
            whales_stats['investi'], whales_stats['retour']
        ))

        # Si score >= 40, ajouter aussi à smart_wallets
        if total_score >= 40:
            cursor.execute("""
                INSERT OR REPLACE INTO smart_wallets (
                    wallet_address, total_score, score_final, roi_global, taux_reussite, jours_derniere_activite,
                    capital_investi, gains_realises, valeur_actuelle, gains_totaux, profit_net, total_current_value,
                    total_tokens, tokens_gagnants, tokens_neutres, tokens_perdants, tokens_airdrops,
                    gains_airdrops, gains_trading, ratio_skill_chance,
                    petits_count, petits_gagnants, petits_roi, petits_reussite, petits_investi, petits_retour,
                    gros_count, gros_gagnants, gros_roi, gros_reussite, gros_investi, gros_retour,
                    whales_count, whales_gagnants, whales_roi, whales_reussite, whales_investi, whales_retour
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                wallet_address, total_score, total_score, roi_global, taux_reussite, days_since_last,
                total_invested, total_realized, current_value, total_gains, profit_net, current_value,
                len(tokens_data), tokens_gagnants, tokens_neutres, tokens_perdants, tokens_airdrops,
                gains_airdrops, gains_trading, ratio_skill_chance,
                petits_stats['count'], petits_stats['gagnants'], petits_stats['roi'], petits_stats['reussite'],
                petits_stats['investi'], petits_stats['retour'],
                gros_stats['count'], gros_stats['gagnants'], gros_stats['roi'], gros_stats['reussite'],
                gros_stats['investi'], gros_stats['retour'],
                whales_stats['count'], whales_stats['gagnants'], whales_stats['roi'], whales_stats['reussite'],
                whales_stats['investi'], whales_stats['retour']
            ))
            print(f"🎯 Smart wallet identifié ! Score: {total_score:.1f}/100")

        conn.commit()
        conn.close()

        print(f"✅ Profil wallet calculé (Score: {total_score:.1f}/100, ROI: {roi_global:.1f}%, Winrate: {taux_reussite:.1f}%)")
        return True

    except Exception as e:
        print(f"❌ Erreur calcul profil wallet: {e}")
        import traceback
        traceback.print_exc()
        return False

def store_manual_wallet_data(address, token_df, total_value):
    """Stocke les données d'un wallet analysé manuellement en BDD"""
    
    # Vérifier si wallet existe déjà en BDD
    existing_wallet = get_wallet(address)
    if existing_wallet:
        print(f"⚠️ Wallet {address} existe déjà en BDD")
        # On peut choisir de mettre à jour ou ignorer
        return True
    
    # 1. Insérer le wallet en BDD avec période "manual"
    success = insert_wallet(address, "manual", total_value)
    if not success:
        print(f"❌ Erreur insertion wallet {address}")
        return False
    
    print(f"✅ Wallet {address} inséré en BDD")

    # 2. Insérer chaque token en BDD
    tokens_inserted = 0
    for _, row in token_df.iterrows():
        token_success = insert_token(
            wallet_address=address,
            fungible_id=row['fungible_id'],
            symbol=row['token'],
            contract_address=row['contract_address'],
            chain=row['chain'],
            amount=row['amount'],
            usd_value=row['usd_value'],
            price=row['usd_value'] / row['amount'] if row['amount'] > 0 else 0
        )
        if token_success:
            tokens_inserted += 1
    
    print(f"✅ {tokens_inserted}/{len(token_df)} tokens insérés pour {address}")
    return True

def run_manual_analysis(wallet_address):
    """Pipeline complet pour analyse manuelle d'un wallet"""

    print(f"\n🎯 ANALYSE MANUELLE COMPLÈTE : {wallet_address}")
    print("=" * 80)

    # Étape 1: Extraction des balances (sans filtres)
    print("\n📊 [1/5] Extraction des balances actuelles...")
    token_df, total_value = get_token_balances_manual(wallet_address)

    if token_df.empty:
        print("❌ Aucun token trouvé ou erreur API")
        return False

    # Étape 2: Stockage en BDD manuelle
    print("\n💾 [2/5] Stockage en base de données manuelle...")
    storage_success = store_manual_wallet_data(wallet_address, token_df, total_value)

    if not storage_success:
        print("❌ Erreur stockage en BDD")
        return False

    # Étape 3: Extraction historique des transactions
    print("\n📈 [3/5] Extraction historique complet des transactions...")
    history_success = extract_wallet_transaction_history(wallet_address, token_df)

    if not history_success:
        print("⚠️ Historique partiellement extrait ou échoué")
        # On continue quand même si on a des données

    # Étape 4: Scoring FIFO (analyse token par token)
    print("\n🧮 [4/5] Analyse FIFO et calcul des métriques...")
    fifo_success = run_fifo_scoring(wallet_address)

    if not fifo_success:
        print("⚠️ Scoring FIFO incomplet")

    # Étape 5: Calcul du profil wallet global
    print("\n📊 [5/5] Calcul du profil wallet et scoring global...")
    profile_success = calculate_wallet_profile(wallet_address)

    if not profile_success:
        print("⚠️ Profil wallet incomplet")

    # Résumé final
    print(f"\n{'=' * 80}")
    print(f"🎉 ANALYSE TERMINÉE pour {wallet_address}")
    print(f"💰 Valeur totale du portefeuille: ${total_value:,.2f}")
    print(f"🪙 Nombre de tokens: {len(token_df)}")
    print(f"📊 Données stockées dans: wit_database_manual.db")
    print(f"✅ Wallet disponible dans le dashboard d'analyse manuelle")
    print(f"{'=' * 80}\n")

    return True

def run_manual_analysis_api(wallet_address):
    """Version API complète pour intégration avec le web app"""
    try:
        # Étape 1: Extraction balances
        token_df, total_value = get_token_balances_manual(wallet_address)

        if token_df.empty:
            return {
                "success": False,
                "error": "No tokens found or API error",
                "step": "balance_extraction"
            }

        # Étape 2: Stockage en BDD manuelle
        storage_success = store_manual_wallet_data(wallet_address, token_df, total_value)

        if not storage_success:
            return {
                "success": False,
                "error": "Database storage failed",
                "step": "storage"
            }

        # Étape 3: Extraction historique
        history_success = extract_wallet_transaction_history(wallet_address, token_df)

        # Étape 4: Scoring FIFO
        fifo_success = run_fifo_scoring(wallet_address)

        # Étape 5: Profil wallet
        profile_success = calculate_wallet_profile(wallet_address)

        # Récupérer le score final si disponible
        try:
            conn = sqlite3.connect(str(DB_PATH))
            cursor = conn.cursor()
            cursor.execute("""
                SELECT total_score, roi_global, taux_reussite
                FROM wallet_profiles
                WHERE wallet_address = ?
            """, (wallet_address,))
            result = cursor.fetchone()
            conn.close()

            if result:
                score, roi, winrate = result
            else:
                score, roi, winrate = 0, 0, 0
        except:
            score, roi, winrate = 0, 0, 0

        return {
            "success": True,
            "wallet_address": wallet_address,
            "total_value": total_value,
            "token_count": len(token_df),
            "score": score,
            "roi": roi,
            "winrate": winrate,
            "steps_completed": {
                "balance_extraction": True,
                "storage": storage_success,
                "history_extraction": history_success,
                "fifo_scoring": fifo_success,
                "profile_calculation": profile_success
            },
            "message": "Analysis completed successfully"
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "step": "unknown"
        }

# === Lancement direct pour tests
if __name__ == "__main__":
    # Test avec une adresse exemple
    test_address = "0x1234567890abcdef1234567890abcdef12345678"
    print("🧪 Test du pipeline d'analyse manuelle")
    run_manual_analysis(test_address)