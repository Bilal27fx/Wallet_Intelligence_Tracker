"""Telegram bot service."""
import os
import requests
from django.conf import settings
import logging

logger = logging.getLogger(__name__)


class TelegramBot:
    """Telegram bot for sending alerts."""

    def __init__(self):
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN', '')
        self.channel_id = os.getenv('TELEGRAM_CHANNEL_ID', '')
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"

    def send_message(self, message: str, parse_mode: str = 'HTML'):
        """Send message to Telegram channel."""
        if not self.bot_token or not self.channel_id:
            logger.warning("Telegram not configured - skipping alert")
            return False

        url = f"{self.base_url}/sendMessage"
        data = {
            'chat_id': self.channel_id,
            'text': message,
            'parse_mode': parse_mode,
        }

        try:
            response = requests.post(url, json=data, timeout=10)
            response.raise_for_status()
            logger.info("Telegram message sent successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False


def send_consensus_alert(consensus_signal):
    """Send consensus alert to Telegram."""
    bot = TelegramBot()

    message = f"""
🚨 <b>CONSENSUS DETECTED</b> 🚨

Token: <b>{consensus_signal.get('symbol', 'Unknown')}</b>
Contract: <code>{consensus_signal.get('token_address', 'N/A')}</code>

📊 Wallets: {consensus_signal.get('wallet_count', 0)}
💰 Confidence: {consensus_signal.get('confidence_score', 0):.1%}

Detected: {consensus_signal.get('detected_at', 'N/A')}
"""

    return bot.send_message(message)
