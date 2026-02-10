#!/usr/bin/env python3
"""
Module Telegram Bot pour Alpha Intelligence Lab
Envoie automatiquement les signaux de consensus détectés
"""

import requests
import json
import time
from datetime import datetime, timezone
from pathlib import Path
import os
import logging
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
ROOT_DIR = Path(__file__).parent.parent.parent
ENV_PATH = ROOT_DIR / ".env"
load_dotenv(ENV_PATH)

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlphaIntelligenceBot:
    """Bot Telegram pour Alpha Intelligence Lab"""
    
    def __init__(self, bot_token=None, channel_id=None):
        """
        Initialise le bot Alpha Intelligence
        
        Args:
            bot_token: Token du bot (lu depuis .env si non fourni)
            channel_id: ID du canal (lu depuis .env si non fourni)
        """
        self.bot_token = bot_token or os.getenv('TELEGRAM_BOT_TOKEN')
        self.channel_id = channel_id or os.getenv('TELEGRAM_CHANNEL_ID')
        
        if not self.bot_token:
            raise ValueError("❌ TELEGRAM_BOT_TOKEN not found in .env")
        if not self.channel_id:
            raise ValueError("❌ TELEGRAM_CHANNEL_ID not found in .env")
        
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        
        # Test de connexion
        self.test_connection()
    
    def test_connection(self):
        """Teste la connexion au bot"""
        try:
            response = requests.get(f"{self.base_url}/getMe", timeout=10)
            if response.status_code == 200:
                return True
            else:
                return False
        except Exception as e:
            return False
    
    def send_message(self, message, parse_mode='HTML', disable_web_page_preview=True):
        """
        Envoie un message sur Alpha Intelligence Lab
        
        Args:
            message: Texte du message
            parse_mode: Format (HTML ou Markdown)
            disable_web_page_preview: Désactiver aperçu liens
            
        Returns:
            bool: Succès de l'envoi
        """
        try:
            payload = {
                'chat_id': self.channel_id,
                'text': message,
                'parse_mode': parse_mode,
                'disable_web_page_preview': disable_web_page_preview
            }
            
            response = requests.post(
                f"{self.base_url}/sendMessage", 
                json=payload, 
                timeout=30
            )
            
            if response.status_code == 200:
                return True
            else:
                return False
                
        except Exception as e:
            return False
    
    def format_alpha_signal(self, token_data):
        """
        Formate un signal alpha TOKEN FOCUS pour Alpha Intelligence Lab
        FOCUS: Token + Métriques uniquement (PAS de consensus/wallets)
        
        Args:
            token_data: Données du token détecté
            
        Returns:
            str: Message formaté token-centric
        """
        symbol = token_data.get('symbol', 'UNKNOWN')
        total_investment = token_data.get('total_investment', 0)
        contract_address = token_data.get('contract_address', 'N/A')
        detection_date = token_data.get('detection_date', datetime.now(timezone.utc))
        
        # Données DexScreener
        token_info = token_data.get('token_info', {})
        price = token_info.get('price_usd', 0)
        market_cap = token_info.get('market_cap', 0)
        liquidity = token_info.get('liquidity_usd', 0)
        volume_24h = token_info.get('volume_24h', 0)
        price_change_24h = token_info.get('price_change_24h', 0)
        
        # Déterminer la formation du signal
        if hasattr(detection_date, 'strftime'):
            formation_date = detection_date.strftime('%m/%d %H:%M')
        else:
            formation_date = str(detection_date)[:10]
        
        # Déterminier la qualité basée sur market cap et volume
        if market_cap > 50_000_000:
            quality = "ULTRA HIGH"
            quality_emoji = "⭐️⭐️⭐️"
        elif market_cap > 10_000_000:
            quality = "HIGH"  
            quality_emoji = "⭐️⭐️"
        elif market_cap > 1_000_000:
            quality = "MEDIUM"
            quality_emoji = "⭐️"
        else:
            quality = "EMERGING"
            quality_emoji = "🔍"
        
        # Formation style basée sur l'investissement
        if total_investment > 100_000:
            formation = "🚀 EXPLOSIVE"
        elif total_investment > 50_000:
            formation = "⚡️ RAPID"
        else:
            formation = "🕐 GRADUAL"
        
        # Message TOKEN FOCUS
        message = f"""🧠 <b>ALPHA SIGNAL DETECTED</b>

🪙 <b>TOKEN:</b> {symbol}
💰 <b>TOTAL INVESTMENT:</b> ${total_investment:,.0f}
⚡️ <b>FORMATION:</b> {formation}
📅 <b>DETECTED:</b> {formation_date}

📊 <b>MARKET METRICS:</b>"""
        
        if token_info:
            # Formatage du market cap
            if market_cap >= 1_000_000:
                mcap_display = f"${market_cap/1_000_000:.1f}M"
            elif market_cap >= 1_000:
                mcap_display = f"${market_cap/1_000:.1f}K"
            else:
                mcap_display = f"${market_cap:,.0f}"
                
            message += f"""
💲 <b>Price:</b> ${price:.8f}
📊 <b>Market Cap:</b> {mcap_display}
🌊 <b>Liquidity:</b> ${liquidity:,.0f}
📈 <b>Volume 24h:</b> ${volume_24h:,.0f}
🚀 <b>24h Change:</b> {price_change_24h:+.1f}%
{quality_emoji} <b>Quality:</b> {quality}"""
        else:
            message += "\n⚠️ <i>Market data loading...</i>"
        
        # Déterminer les liens (assumer Ethereum par défaut)
        chain_id = token_info.get('chain_id', 'ethereum')
        if chain_id == 'base' or 'base' in str(chain_id).lower():
            dex_link = f"https://dexscreener.com/base/{contract_address}"
            explorer_link = f"https://basescan.org/address/{contract_address}"
        elif chain_id == 'bsc' or 'bsc' in str(chain_id).lower():
            dex_link = f"https://dexscreener.com/bsc/{contract_address}"
            explorer_link = f"https://bscscan.com/address/{contract_address}"
        else:
            dex_link = f"https://dexscreener.com/ethereum/{contract_address}"
            explorer_link = f"https://etherscan.io/address/{contract_address}"
        
        message += f"""

🔗 <b>CONTRACT:</b> <code>{contract_address}</code>

🔍 <b>LINKS:</b>
• <a href="{dex_link}">📈 DexScreener</a>
• <a href="{explorer_link}">🔍 Explorer</a>

🤖 <b>Alpha Intelligence Lab</b>
🕐 <i>{datetime.now(timezone.utc).strftime('%m/%d %H:%M UTC')}</i>"""
        
        return message
    
    def send_alpha_signal(self, consensus_data):
        """
        Envoie un signal alpha sur le canal
        
        Args:
            consensus_data: Données du consensus
            
        Returns:
            bool: Succès de l'envoi
        """
        try:
            message = self.format_alpha_signal(consensus_data)
            return self.send_message(message)
        except Exception as e:
            logger.error(f"❌ Erreur formatage signal: {e}")
            return False
    
    def send_multiple_signals(self, consensus_list):
        """
        Envoie plusieurs signaux avec délai entre chaque
        
        Args:
            consensus_list: Liste des consensus détectés
            
        Returns:
            int: Nombre de signaux envoyés
        """
        sent_count = 0
        
        if not consensus_list:
            return 0
        
        for i, consensus in enumerate(consensus_list, 1):
            if self.send_alpha_signal(consensus):
                sent_count += 1
            
            # Délai anti-spam
            if i < len(consensus_list):
                time.sleep(3)
        return sent_count
    
    def send_lab_summary(self, consensus_count, total_investment=0):
        """
        Envoie un résumé de session d'analyse SIMPLIFIÉ
        
        Args:
            consensus_count: Nombre de consensus détectés
            total_investment: Capital total détecté
        """
        if consensus_count == 0:
            # Ne pas envoyer de message si aucun signal détecté
            return True
        else:
            message = f"""🤖 <b>Scan terminé</b>

✅ {consensus_count} signal{'s' if consensus_count > 1 else ''} détecté{'s' if consensus_count > 1 else ''}
💰 ${total_investment:,.0f} investis

<i>Prochain scan dans 1h</i>"""
        
        return self.send_message(message)
    
    def send_system_startup(self):
        """Envoie un message de démarrage du système SIMPLIFIÉ"""
        message = """🤖 <b>Alpha Intelligence Lab démarré</b>

Scan automatique activé

<i>Recherche de signaux en cours...</i>"""
        
        return self.send_message(message)
    
    def send_scan_completion_message(self):
        """Envoie un message futuriste IA de fin de scan blockchain"""
        
        message = """🤖 <b>ALPHA INTELLIGENCE NEURAL NETWORK</b>
        
🔍 <b>BLOCKCHAIN SCAN COMPLETED</b>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🧠 <i>AI Systems have successfully analyzed all wallet patterns</i>
⚡ <i>Neural pathways optimized for next consensus detection</i>
🛰️ <i>Quantum sensors entering hibernation mode</i>

⏰ <b>NEXT SCAN INITIATED IN: 1 HOUR</b>

🚀 <i>Stay connected to the future of DeFi intelligence...</i>

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
<i>Powered by Alpha Intelligence Lab</i>"""
        
        try:
            success = self.send_message(message)
            if success:
                logger.info("✅ Message de fin de scan envoyé")
            else:
                logger.error("❌ Échec envoi message de fin de scan")
            return success
        except Exception as e:
            logger.error(f"❌ Erreur message fin de scan: {e}")
            return False


# Fonction utilitaire pour intégration facile
def send_consensus_to_telegram(consensus_dict):
    """
    Fonction simple pour envoyer les consensus détectés (nouveau format)
    
    Args:
        consensus_dict: Dictionnaire des consensus {symbol: data}
        
    Returns:
        bool: Succès de l'opération
    """
    try:
        bot = AlphaIntelligenceBot()
        
        if consensus_dict:
            # Convertir le dictionnaire en liste avec symbol inclus
            consensus_list = []
            for symbol, data in consensus_dict.items():
                data['symbol'] = symbol  # Ajouter le symbol aux données
                consensus_list.append(data)
            
            sent_count = bot.send_multiple_signals(consensus_list)
            
            # PAS de résumé - seulement les signaux purs
            
            return sent_count > 0
        else:
            # Aucun message si pas de consensus
            return True
            
    except Exception as e:
        logger.error(f"❌ Erreur transmission Telegram: {e}")
        return False


# Test du module
if __name__ == "__main__":
    print("🧠 Alpha Intelligence Bot - Module de test")
    
    try:
        bot = AlphaIntelligenceBot()
        
        # Test de démarrage
        bot.send_system_startup()
       
        
    except Exception as e:
        print(f"❌ Erreur de test: {e}")