"""Client Zerion : chaînes supportées (l'historique des wallets viendra plus tard)."""

BASE_URL = "https://api.zerion.io/v1"


class ZerionClient:
    def __init__(self, http):
        self._http = http

    def chain_ids(self) -> dict[int, str]:
        payload = self._http.get("/chains/")
        chains = {}
        for item in payload.get("data", []):
            external_id = item["attributes"].get("external_id")
            if external_id:
                chains[int(external_id, 16)] = item["id"]
        return chains
