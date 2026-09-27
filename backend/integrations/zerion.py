"""Client Zerion : chaînes et prix uniquement (aucune donnée de wallet)."""

from dataclasses import dataclass

BASE_URL = "https://api.zerion.io/v1"
MAX_IMPLEMENTATIONS_PER_CALL = 25


@dataclass(frozen=True)
class ZerionChain:
    zerion_id: str
    evm_id: int
    rpc_url: str
    native_fungible_id: str
    wrapped_fungible_id: str


def _https_rpc(urls) -> str:
    return next((url for url in urls or [] if url.startswith("https://")), "")


def _relation_id(item: dict, name: str) -> str:
    relation = (item.get("relationships") or {}).get(name) or {}
    return (relation.get("data") or {}).get("id") or ""


class ZerionClient:
    def __init__(self, http):
        self._http = http

    def chains(self) -> list[ZerionChain]:
        payload = self._http.get("/chains/")
        chains = []
        for item in payload.get("data", []):
            attributes = item["attributes"]
            if not attributes.get("external_id"):
                continue
            chains.append(
                ZerionChain(
                    zerion_id=item["id"],
                    evm_id=int(attributes["external_id"], 16),
                    rpc_url=_https_rpc((attributes.get("rpc") or {}).get("public_servers_url")),
                    native_fungible_id=_relation_id(item, "native_fungible"),
                    wrapped_fungible_id=_relation_id(item, "wrapped_native_fungible"),
                )
            )
        return chains

    def chain_ids(self) -> dict[int, str]:
        return {chain.evm_id: chain.zerion_id for chain in self.chains()}
