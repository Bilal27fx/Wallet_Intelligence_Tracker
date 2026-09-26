#!/usr/bin/env python3
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

from smart_wallet_analysis.config import TELEGRAM
from smart_wallet_analysis.logger import get_logger

# Charger les variables d'environnement depuis le fichier .env
ROOT_DIR = Path(__file__).parent.parent.parent
ENV_PATH = ROOT_DIR / ".env"
load_dotenv(ENV_PATH)
_TG = TELEGRAM

logger = get_logger("telegram.bot")


def _env_flag(name, default=False):
    """Retourne un booléen depuis une variable d'environnement."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _format_detection_date(detection_date):
    """Formate la date de détection."""
    if hasattr(detection_date, "strftime"):
        return detection_date.strftime("%m/%d %H:%M")
    return str(detection_date)[:10]


def _format_market_cap(market_cap):
    """Formate le market cap pour affichage."""
    if market_cap >= 1_000_000:
        return f"${market_cap/1_000_000:.1f}M"
    if market_cap >= 1_000:
        return f"${market_cap/1_000:.1f}K"
    return f"${market_cap:,.0f}"


def _quality_label(market_cap):
    """Retourne la qualité du token selon le market cap."""
    thresholds = _TG["QUALITY_MARKET_CAP_THRESHOLDS"]
    if market_cap > thresholds["ULTRA_HIGH"]:
        return "ULTRA HIGH", "⭐️⭐️⭐️"
    if market_cap > thresholds["HIGH"]:
        return "HIGH", "⭐️⭐️"
    if market_cap > thresholds["MEDIUM"]:
        return "MEDIUM", "⭐️"
    return "EMERGING", "🔍"


def _formation_label(total_investment):
    """Retourne le type de formation selon l'investissement."""
    thresholds = _TG["FORMATION_INVESTMENT_THRESHOLDS"]
    if total_investment > thresholds["EXPLOSIVE"]:
        return "🚀 EXPLOSIVE"
    if total_investment > thresholds["RAPID"]:
        return "⚡️ RAPID"
    return "🕐 GRADUAL"


def _build_links(contract_address, chain_id):
    """Construit les liens DexScreener et explorer selon la chaîne."""
    chain = (chain_id or _TG["DEFAULT_CHAIN_ID"]).lower()
    if "base" in chain:
        return (
            f"https://dexscreener.com/base/{contract_address}",
            f"https://basescan.org/address/{contract_address}",
        )
    if "bsc" in chain or "bnb" in chain:
        return (
            f"https://dexscreener.com/bsc/{contract_address}",
            f"https://bscscan.com/address/{contract_address}",
        )
    return (
        f"https://dexscreener.com/ethereum/{contract_address}",
        f"https://etherscan.io/address/{contract_address}",
    )


class AlphaIntelligenceBot:
    """Bot Telegram pour Alpha Intelligence Lab."""

    def __init__(self, bot_token=None, channel_id=None):
        """Initialise le bot Telegram."""
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN")
        self.channel_id = channel_id or os.getenv("TELEGRAM_CHANNEL_ID")
        self.notifications_enabled = _env_flag("TELEGRAM_NOTIFICATIONS_ENABLED", default=True)

        if not self.bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN not found in .env")
        if not self.channel_id:
            raise ValueError("TELEGRAM_CHANNEL_ID not found in .env")

        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        if not self.test_connection():
            logger.warning("Telegram getMe failed, send attempts may fail")

    def test_connection(self):
        """Teste la connexion au bot."""
        try:
            response = requests.get(
                f"{self.base_url}/getMe",
                timeout=_TG.get("PING_TIMEOUT_SECONDS", 10),
            )
            if response.status_code != 200:
                logger.warning("Telegram getMe error: %s - %s", response.status_code, response.text[:300])
                return False
            return True
        except requests.RequestException as exc:
            logger.warning("Telegram getMe request error: %s", exc)
            return False

    def send_message(self, message, parse_mode="HTML", disable_web_page_preview=True):
        """Envoie un message sur le canal Telegram."""
        if not self.notifications_enabled:
            logger.info("Telegram notifications disabled (TELEGRAM_NOTIFICATIONS_ENABLED=false)")
            return True

        payload = {
            "chat_id": self.channel_id,
            "text": message,
            "parse_mode": parse_mode,
            "disable_web_page_preview": disable_web_page_preview,
        }

        try:
            response = requests.post(
                f"{self.base_url}/sendMessage",
                json=payload,
                timeout=_TG.get("SEND_TIMEOUT_SECONDS", 30),
            )
            if response.status_code != 200:
                logger.error("Telegram sendMessage error: %s - %s", response.status_code, response.text[:300])
                return False
            return True
        except requests.RequestException as exc:
            logger.error("Telegram sendMessage request error: %s", exc)
            return False

    def format_alpha_signal(self, token_data):
        """Formate un signal token-centric."""
        symbol = token_data.get("symbol", "UNKNOWN")
        total_investment = token_data.get("total_investment", 0)
        contract_address = token_data.get("contract_address", "N/A")
        detection_date = token_data.get("detection_date", datetime.now(timezone.utc))

        token_info = token_data.get("token_info", {}) or {}
        price = token_info.get("price_usd", 0)
        market_cap = token_info.get("market_cap", 0)
        liquidity = token_info.get("liquidity_usd", 0)
        volume_24h = token_info.get("volume_24h", 0)
        price_change_24h = token_info.get("price_change_24h", 0)
        chain_id = token_info.get("chain_id", _TG["DEFAULT_CHAIN_ID"])

        quality, quality_emoji = _quality_label(market_cap)
        formation = _formation_label(total_investment)
        formation_date = _format_detection_date(detection_date)
        dex_link, explorer_link = _build_links(contract_address, chain_id)

        message = f"""🧠 <b>ALPHA SIGNAL DETECTED</b>

🪙 <b>TOKEN:</b> {symbol}
💰 <b>TOTAL INVESTMENT:</b> ${total_investment:,.0f}
⚡️ <b>FORMATION:</b> {formation}
📅 <b>DETECTED:</b> {formation_date}

📊 <b>MARKET METRICS:</b>"""

        if token_info:
            mcap_display = _format_market_cap(market_cap)
            message += f"""
💲 <b>Price:</b> ${price:.8f}
📊 <b>Market Cap:</b> {mcap_display}
🌊 <b>Liquidity:</b> ${liquidity:,.0f}
📈 <b>Volume 24h:</b> ${volume_24h:,.0f}
🚀 <b>24h Change:</b> {price_change_24h:+.1f}%
{quality_emoji} <b>Quality:</b> {quality}"""
        else:
            message += "\n⚠️ <i>Market data loading...</i>"

        message += f"""

🔗 <b>CONTRACT:</b> <code>{contract_address}</code>

🔍 <b>LINKS:</b>
• <a href="{dex_link}">📈 DexScreener</a>
• <a href="{explorer_link}">🔍 Explorer</a>

🤖 <b>Alpha Intelligence Lab</b>
🕐 <i>{datetime.now(timezone.utc).strftime('%m/%d %H:%M UTC')}</i>"""

        return message

    def send_alpha_signal(self, consensus_data):
        """Envoie un signal alpha unique."""
        try:
            message = self.format_alpha_signal(consensus_data)
            return self.send_message(message)
        except Exception as exc:
            logger.error("Erreur formatage signal: %s", exc)
            return False

    def send_multiple_signals(self, consensus_list):
        """Envoie plusieurs signaux avec délai anti-spam."""
        sent_count = 0
        if not consensus_list:
            return 0

        delay_seconds = float(_TG.get("SEND_DELAY_SECONDS", 3))
        for i, consensus in enumerate(consensus_list, 1):
            if self.send_alpha_signal(consensus):
                sent_count += 1
            if i < len(consensus_list):
                time.sleep(delay_seconds)
        return sent_count

    def send_lab_summary(self, consensus_count, total_investment=0):
        """Envoie un résumé de scan."""
        if consensus_count == 0:
            return True

        message = f"""🤖 <b>Scan terminé</b>

✅ {consensus_count} signal{'s' if consensus_count > 1 else ''} détecté{'s' if consensus_count > 1 else ''}
💰 ${total_investment:,.0f} investis

<i>Prochain scan dans 1h</i>"""

        return self.send_message(message)

    def send_system_startup(self):
        """Envoie un message de démarrage."""
        message = """🤖 <b>Alpha Intelligence Lab démarré</b>

Scan automatique activé

<i>Recherche de signaux en cours...</i>"""

        return self.send_message(message)

    def send_scan_completion_message(self):
        """Envoie un message de fin de scan."""
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
                logger.info("Message de fin de scan envoyé")
            else:
                logger.error("Échec envoi message de fin de scan")
            return success
        except Exception as exc:
            logger.error("Erreur message fin de scan: %s", exc)
            return False


def send_consensus_to_telegram(consensus_dict):
    """Envoie une collection de consensus vers Telegram."""
    try:
        bot = AlphaIntelligenceBot()

        if not consensus_dict:
            return True

        consensus_list = [
            {**data, "symbol": symbol}
            for symbol, data in consensus_dict.items()
        ]
        sent_count = bot.send_multiple_signals(consensus_list)
        return sent_count > 0
    except Exception as exc:
        logger.error("Erreur transmission Telegram: %s", exc)
        return False


if __name__ == "__main__":
    logger.info("Alpha Intelligence Bot - Test module")
    try:
        bot = AlphaIntelligenceBot()
        bot.send_system_startup()
    except Exception as exc:
        logger.error("Erreur de test: %s", exc)
