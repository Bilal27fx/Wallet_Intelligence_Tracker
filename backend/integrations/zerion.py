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


@dataclass(frozen=True)
class Fungible:
    fungible_id: str
    symbol: str
    price: float | None
    implementations: dict[str, tuple[str, int]]


def _fungible(item: dict) -> Fungible:
    attributes = item["attributes"]
    implementations = {}
    for impl in attributes.get("implementations") or []:
        if impl.get("address"):
            implementations[impl["chain_id"]] = (
                impl["address"].lower(),
                int(impl.get("decimals") or 0),
            )
    price = (attributes.get("market_data") or {}).get("price")
    return Fungible(
        item["id"],
        attributes.get("symbol") or "",
        float(price) if price is not None else None,
        implementations,
    )


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

    def prices(self, implementations: list[tuple[str, str]]) -> dict[tuple[str, str], Fungible]:
        wanted = sorted({(chain, address.lower()) for chain, address in implementations})
        found: dict[tuple[str, str], Fungible] = {}
        for start in range(0, len(wanted), MAX_IMPLEMENTATIONS_PER_CALL):
            batch = wanted[start : start + MAX_IMPLEMENTATIONS_PER_CALL]
            requested = set(batch)
            payload = self._http.get(
                "/fungibles/",
                params={
                    "filter[fungible_implementations]": ",".join(f"{c}:{a}" for c, a in batch),
                    "currency": "usd",
                    "page[size]": 100,
                },
            )
            for item in payload.get("data", []):
                fungible = _fungible(item)
                for chain, (address, _) in fungible.implementations.items():
                    if (chain, address) in requested:
                        found[(chain, address)] = fungible
        return found

    def fungible(self, fungible_id: str) -> Fungible:
        payload = self._http.get(f"/fungibles/{fungible_id}", params={"currency": "usd"})
        return _fungible(payload["data"])

    def search(self, symbol: str) -> Fungible | None:
        payload = self._http.get(
            "/fungibles/",
            params={
                "filter[search_query]": symbol,
                "sort": "-market_data.market_cap",
                "currency": "usd",
                "page[size]": 1,
            },
        )
        data = payload.get("data", [])
        return _fungible(data[0]) if data else None

    def price_chart(self, fungible_id: str) -> list[tuple[int, float]]:
        payload = self._http.get(
            f"/fungibles/{fungible_id}/charts/year", params={"currency": "usd"}
        )
        return [(int(ts), float(price)) for ts, price in payload["data"]["attributes"]["points"]]
