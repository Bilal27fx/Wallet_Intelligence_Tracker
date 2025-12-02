#!/usr/bin/env python3
"""
Script d'intégration Alpha Intelligence Live
Combine la détection de consensus live avec l'envoi automatique Telegram
"""

import sys
import time
import schedule
from datetime import datetime, timezone
from pathlib import Path

# Ajouter le module à sys.path
ROOT_DIR = Path(__file__).parent
sys.path.append(str(ROOT_DIR))
sys.path.append(str(ROOT_DIR / "module" / "tracking_live"))

# Imports des modules
from module.consensus_live.consensus_live_detector import run_live_consensus_detection
from module.Telegram.telegram_bot import send_consensus_to_telegram
from module.tracking_live.run import run_complete_live_tracking
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(ROOT_DIR / "data" / "logs" / "alpha_live.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def ensure_log_directory():
    """Crée le dossier de logs s'il n'existe pas"""
    log_dir = ROOT_DIR / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

def convert_consensus_for_telegram(consensus_signals):
    """Convertit les signaux de consensus au format attendu par le bot Telegram"""
    telegram_data = {}
    
    for signal in consensus_signals:
        symbol = signal['symbol']
        
        # Reformater les données pour le bot Telegram
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
    
    return telegram_data

def run_alpha_intelligence_cycle():
    """Exécute un cycle complet de détection et d'envoi Alpha Intelligence"""
    try:
        logger.info("🚀 Démarrage du cycle Alpha Intelligence")
        
        # 0. Tracking live des changements
        logger.info("🔄 Phase 1: Tracking live des changements...")
        tracking_success = run_complete_live_tracking(
            enable_transaction_tracking=True,
            min_usd=500,
            hours_lookback=24
        )
        
        if tracking_success:
            logger.info("✅ Tracking live terminé avec succès")
        else:
            logger.warning("⚠️ Erreur tracking live - Poursuite avec données existantes")
        
        # 1. Détecter les consensus live
        logger.info("🔍 Phase 2: Détection des consensus live...")
        consensus_signals = run_live_consensus_detection()
        
        if not consensus_signals:
            logger.info("💤 Aucun nouveau consensus détecté")
            return
        
        logger.info(f"✅ {len(consensus_signals)} consensus détectés")
        
        # 2. Convertir au format Telegram
        telegram_data = convert_consensus_for_telegram(consensus_signals)
        
        # 3. Envoyer via Telegram
        logger.info("📤 Phase 3: Envoi des signaux via Telegram...")
        success = send_consensus_to_telegram(telegram_data)
        
        if success:
            logger.info(f"✅ {len(telegram_data)} signaux envoyés avec succès")
        else:
            logger.error("❌ Erreur lors de l'envoi Telegram")
        
        # 4. Résumé
        total_investment = sum(s['total_investment'] for s in consensus_signals)
        positive_count = sum(1 for s in consensus_signals 
                           if s.get('performance', {}).get('performance_pct', 0) > 0)
        
        logger.info(f"📊 Résumé cycle: {len(consensus_signals)} signaux, "
                   f"${total_investment:,.0f} investis, {positive_count} positifs")
        logger.info(f"📈 Phases: Tracking {'✅' if tracking_success else '⚠️'} | "
                   f"Consensus ✅ | Telegram {'✅' if success else '❌'}")
        
    except Exception as e:
        logger.error(f"❌ Erreur lors du cycle Alpha Intelligence: {e}")

def run_alpha_manual():
    """Lance une détection manuelle Alpha Intelligence"""
    print("🎯 ALPHA INTELLIGENCE - MANUAL RUN")
    print("=" * 60)
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        import subprocess
        import sys
        
        # Get the directory of this script
        script_dir = Path(__file__).parent
        alpha_live_path = script_dir / "alpha_live.py"
        
        print("🚀 ALPHA INTELLIGENCE CYCLE START")
        print("=" * 60)
        print("📊 Phase 1: Live wallet tracking (detecting position changes)...")
        print("🔍 Phase 2: Consensus detection (analyzing whale movements)...")  
        print("📤 Phase 3: Telegram signal transmission...")
        print("=" * 60)
        
        # Run alpha_live.py with real-time output
        result = subprocess.run([
            sys.executable, 
            str(alpha_live_path)
        ], cwd=script_dir)
        
        print("=" * 60)
        print("✅ Manual run completed")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Error: {e}")

def run_alpha_scheduler():
    """Lance alpha_live.py toutes les 2 heures"""
    print("🤖 ALPHA INTELLIGENCE - AUTO MODE (Every 2H)")
    print("=" * 60)
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🔄 Running alpha_live.py every 2 hours")
    print("⏹️  Ctrl+C to stop")
    print()
    
    def run_alpha_live_cycle():
        """Execute alpha_live.py with detailed progress tracking"""
        try:
            import subprocess
            import sys
            
            # Get the directory of this script
            script_dir = Path(__file__).parent
            alpha_live_path = script_dir / "alpha_live.py"
            
            print(f"🚀 {datetime.now().strftime('%H:%M:%S')} - ALPHA INTELLIGENCE CYCLE START")
            print("=" * 70)
            print("📊 Phase 1: Live wallet tracking (detecting position changes)...")
            print("🔍 Phase 2: Consensus detection (analyzing whale movements)...")
            print("📤 Phase 3: Telegram signal transmission...")
            print("=" * 70)
            
            # Run alpha_live.py
            result = subprocess.run([
                sys.executable, 
                str(alpha_live_path)
            ], capture_output=True, text=True, cwd=script_dir)
            
            # Print output with enhanced formatting
            if result.stdout:
                output_lines = result.stdout.strip().split('\n')
                for line in output_lines:
                    if line.strip():
                        # Add timestamp to each important line
                        if any(keyword in line for keyword in ['ALPHA', 'No new', '🧠', '💰', '⚡️']):
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] {line}")
                        else:
                            print(line)
            
            if result.stderr:
                print(f"❌ Error output: {result.stderr}")
            
            print("=" * 70)
            print(f"✅ {datetime.now().strftime('%H:%M:%S')} - ALPHA INTELLIGENCE CYCLE COMPLETED")
            print(f"⏱️  Next cycle in 2 hours...")
            print("=" * 70)
            print()
            
        except Exception as e:
            print(f"❌ Critical error running alpha_live.py: {e}")
            print("=" * 70)
    
    # Configure scheduling every 2 hours
    schedule.every(2).hours.do(run_alpha_live_cycle)
    
    # Run first cycle immediately
    print("🚀 Running first cycle...")
    run_alpha_live_cycle()
    
    # Main loop
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        print("\n🛑 Scheduler stopped by user")

def run_alpha_test():
    """Test des composants Alpha Intelligence"""
    print("🧪 ALPHA INTELLIGENCE - MODE TEST")
    print("=" * 60)
    
    try:
        # Test 1: Import des modules
        print("1️⃣ Test des imports...")
        from module.consensus_live.consensus_live_detector import get_smart_wallets
        from module.Telegram.telegram_bot import AlphaIntelligenceBot
        print("   ✅ Imports OK")
        
        # Test 2: Smart wallets
        print("2️⃣ Test récupération smart wallets...")
        smart_wallets = get_smart_wallets()
        print(f"   ✅ {len(smart_wallets)} smart wallets trouvés")
        
        # Test 3: Bot Telegram
        print("3️⃣ Test bot Telegram...")
        bot = AlphaIntelligenceBot()
        print("   ✅ Bot Telegram connecté")
        
        # Test 4: Formatage message
        print("4️⃣ Test formatage message...")
        test_data = {
            'symbol': 'TEST',
            'total_investment': 50000,
            'contract_address': '0x1234567890abcdef1234567890abcdef12345678',
            'detection_date': datetime.now(timezone.utc),
            'token_info': {
                'price_usd': 0.001,
                'market_cap': 5000000,
                'volume_24h': 100000,
                'price_change_24h': 5.2
            }
        }
        message = bot.format_alpha_signal(test_data)
        print("   ✅ Formatage message OK")
        print(f"   📝 Aperçu: {message[:100]}...")
        
        print("\n🎉 Tous les tests sont passés avec succès!")
        
    except Exception as e:
        print(f"\n❌ Erreur lors des tests: {e}")

def main():
    """Point d'entrée principal avec options"""
    ensure_log_directory()
    
    if len(sys.argv) < 2:
        print("🎯 ALPHA INTELLIGENCE LAB")
        print("=" * 40)
        print()
        print("Usage:")
        print("  python run_alpha_live.py manual      # Détection manuelle")
        print("  python run_alpha_live.py auto        # Mode automatique (1h)")
        print("  python run_alpha_live.py test        # Tests système")
        print()
        return
    
    mode = sys.argv[1].lower()
    
    if mode == "manual":
        run_alpha_manual()
    elif mode == "auto":
        run_alpha_scheduler()
    elif mode == "test":
        run_alpha_test()
    else:
        print(f"❌ Mode '{mode}' non reconnu")
        print("Modes disponibles: manual, auto, test")

if __name__ == "__main__":
    main()