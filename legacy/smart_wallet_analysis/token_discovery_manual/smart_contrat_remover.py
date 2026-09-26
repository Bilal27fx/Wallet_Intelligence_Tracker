"""Vérification EOA/contract via Etherscan."""

import os
import time
from typing import Optional

import requests
from dotenv import load_dotenv

from smart_wallet_analysis.config import ENV_PATH, TOKEN_DISCOVERY_MANUAL
from smart_wallet_analysis.logger import get_logger

load_dotenv(dotenv_path=ENV_PATH)

logger = get_logger("token_discovery.manual.contract_checker")
_TDM = TOKEN_DISCOVERY_MANUAL
_ETHERSCAN_API = os.getenv("ETHERSCAN_API_KEY")


class ContractChecker:
    """Checker Etherscan pour distinguer EOA et smart contracts."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or _ETHERSCAN_API
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "WIT-Contract-Checker/1.0"})
        if not self.api_key:
            logger.warning("ETHERSCAN_API_KEY manquante, le filtrage EOA retournera None")

    def is_contract_single(self, address: str, retry_count: int = 0) -> Optional[bool]:
        """Retourne True si l'adresse est un contrat, False si EOA, None si erreur."""
        if not self.api_key:
            return None

        params = {
            "chainid": _TDM["ETHERSCAN_CHAIN_ID"],
            "module": "proxy",
            "action": "eth_getCode",
            "address": address,
            "apikey": self.api_key,
        }

        try:
            response = self.session.get(
                _TDM["ETHERSCAN_BASE_URL"],
                params=params,
                timeout=_TDM["ETHERSCAN_TIMEOUT_SECONDS"],
            )
            response.raise_for_status()
            data = response.json()

            status = str(data.get("status", ""))
            message = str(data.get("message", "")).lower()
            result_str = str(data.get("result", ""))
            hit_rate_limit = (
                "rate limit" in message
                or "max rate limit" in message
                or (status == "0" and result_str.lower().startswith("max rate limit"))
            )
            if hit_rate_limit:
                if retry_count >= _TDM["ETHERSCAN_MAX_RETRIES"]:
                    logger.error("Rate limit persistant pour %s", address[:12])
                    return None
                time.sleep(_TDM["ETHERSCAN_RATE_LIMIT_SLEEP_SECONDS"])
                return self.is_contract_single(address, retry_count + 1)

            code = data.get("result", "")
            return bool(code and code != "0x")

        except requests.RequestException as e:
            if retry_count >= _TDM["ETHERSCAN_MAX_RETRIES"]:
                logger.error("Erreur Etherscan pour %s: %s", address[:12], e)
                return None
            backoff = _TDM["ETHERSCAN_RETRY_BACKOFF_SECONDS"] * (retry_count + 1)
            time.sleep(backoff)
            return self.is_contract_single(address, retry_count + 1)
