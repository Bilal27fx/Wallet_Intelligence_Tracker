"""Client CoinGecko : correspondance plateforme → chain id EVM."""

BASE_URL = "https://api.coingecko.com/api/v3"


class CoinGeckoClient:
    def __init__(self, http):
        self._http = http

    def platform_chain_ids(self) -> dict[str, int]:
        platforms = self._http.get("/asset_platforms")
        return {
            platform["id"]: int(platform["chain_identifier"])
            for platform in platforms
            if platform.get("chain_identifier")
        }
