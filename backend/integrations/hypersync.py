"""Client HyperSync (Envio) : chaînes supportées, blocs et transferts ERC-20."""

CHAINS_URL = "https://chains.hyperquery.xyz"


def hypersync_url(chain_id: int) -> str:
    return f"https://{chain_id}.hypersync.xyz"


class HyperSyncDirectory:
    def __init__(self, http):
        self._http = http

    def supported_chain_ids(self) -> set[int]:
        chains = self._http.get("/active_chains")
        return {
            chain["chain_id"]
            for chain in chains
            if chain.get("ecosystem") == "evm"
            and chain.get("tier") != "TESTNET"
            and chain.get("chain_id")
        }
