#!/usr/bin/env python3
"""
Alpha Intelligence Live - Script de lancement simplifié
1. Tracking live des changements de positions
2. Détection automatique de consensus + envoi Telegram
"""

import sys
from pathlib import Path

# Ajouter le répertoire racine au path
ROOT_DIR = Path(__file__).parent
sys.path.append(str(ROOT_DIR))
sys.path.append(str(ROOT_DIR / "module" / "tracking_live"))

from module.consensus_live.consensus_live_detector import run_live_consensus_detection
from module.Telegram.telegram_bot import send_consensus_to_telegram
from module.tracking_live.run import run_complete_live_tracking
from datetime import datetime

def run_alpha_live():
    """Lance Alpha Intelligence Live avec tracking préalable"""
    
    try:
        print("🔄 PHASE 1: LIVE WALLET TRACKING")
        print("=" * 50)
        print("📊 Detecting wallet position changes...")
        print("🔍 Analyzing transaction patterns...")
        print("💾 Updating database with latest movements...")
        
        tracking_success = run_complete_live_tracking(
            enable_transaction_tracking=True,
            min_usd=500,
            hours_lookback=24
        )
        
        if tracking_success:
            print("✅ Phase 1 completed: Wallet tracking successful")
        else:
            print("⚠️ Phase 1 warning: Tracking completed with issues")
        
        print("\n🔍 PHASE 2: CONSENSUS DETECTION")
        print("=" * 50)
        print("🐋 Analyzing whale movements...")
        print("📈 Detecting consensus patterns...")
        
        consensus_signals = run_live_consensus_detection()
        
        if consensus_signals:
            print(f"✅ Phase 2 completed: {len(consensus_signals)} consensus detected")
        else:
            print("✅ Phase 2 completed: No new consensus patterns found")
        
        if consensus_signals:
            # 2. Préparer pour Telegram (format token-centric)
            telegram_data = {}
            for signal in consensus_signals:
                symbol = signal['symbol']
                telegram_data[symbol] = {
                    'symbol': symbol,
                    'total_investment': signal['total_investment'],
                    'contract_address': signal['contract_address'],
                    'detection_date': signal['detection_date'],
                    'token_info': signal.get('token_info', {}),
                    'performance': signal.get('performance', {}),
                    'whale_count': signal['whale_count'],
                    'signal_type': signal['signal_type']
                }
            
            print("\n📤 PHASE 3: TELEGRAM TRANSMISSION")
            print("=" * 50)
            print("🚀 Preparing signal messages...")
            print("📡 Sending to Telegram channel...")
            
            success = send_consensus_to_telegram(telegram_data)
            
            if success:
                print(f"✅ Phase 3 completed: {len(telegram_data)} signals transmitted successfully")
            else:
                print("❌ Phase 3 error: Failed to transmit signals")
            
            print("\n🎯 ALPHA SIGNALS DETECTED:")
            print("=" * 50)
            
            # 4. Afficher les signaux détectés
            for signal in consensus_signals:
                from module.Telegram.telegram_bot import AlphaIntelligenceBot
                bot = AlphaIntelligenceBot()
                message = bot.format_alpha_signal(telegram_data[signal['symbol']])
                print(message)
                print()  # Ligne vide entre les signaux s'il y en a plusieurs
        else:
            print("💤 No new Alpha signals detected")
            print("🤖 No Telegram notification sent (no signals detected)")
            return  # Pas de message Telegram si aucun signal
    
        
        # 5. Message de fin de scan (seulement si des signaux ont été détectés)
        print("\n🤖 PHASE 4: SCAN COMPLETION NOTIFICATION")
        print("=" * 50)
        try:
            from module.Telegram.telegram_bot import AlphaIntelligenceBot
            bot = AlphaIntelligenceBot()
            bot.send_scan_completion_message()
            print("✅ Scan completion message sent to Telegram")
        except Exception as telegram_error:
            print(f"⚠️ Failed to send completion message: {telegram_error}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        # En cas d'erreur, ne pas envoyer de message Telegram

if __name__ == "__main__":
    run_alpha_live()