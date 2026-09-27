"""Client Zerion : chaînes et prix uniquement (aucune donnée de wallet)."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from urllib.parse import parse_qs, urlparse

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


NATIVE = "native"
OPERATION_TYPES = "trade,send,receive,execute,mint,burn,claim"


@dataclass(frozen=True)
class ZerionTransfer:
    index: int
    chain: str
    token_address: str
    token_symbol: str
    token_decimals: int
    fungible_id: str
    direction: str
    amount: int
    quantity: Decimal
    price_usd: float | None
    value_usd: float | None
    sender: str
    recipient: str


@dataclass(frozen=True)
class ZerionTransaction:
    zerion_id: str
    chain: str
    tx_hash: str
    block: int | None
    mined_at: datetime
    operation_type: str
    status: str
    fee_usd: float | None
    transfers: list[ZerionTransfer]
    raw: dict


@dataclass(frozen=True)
class TransactionsPage:
    transactions: list[ZerionTransaction]
    next_cursor: str | None


@dataclass(frozen=True)
class Portfolio:
    total_usd: float
    by_chain: dict[str, float]


def _optional_float(value) -> float | None:
    return float(value) if value is not None else None


def _transfer(index: int, chain: str, item: dict) -> ZerionTransfer:
    info = item.get("fungible_info") or {}
    implementation = next(
        (i for i in info.get("implementations") or [] if i.get("chain_id") == chain), {}
    )
    quantity = item.get("quantity") or {}
    return ZerionTransfer(
        index=index,
        chain=chain,
        token_address=(implementation.get("address") or NATIVE).lower(),
        token_symbol=info.get("symbol") or "",
        token_decimals=int(quantity.get("decimals") or implementation.get("decimals") or 0),
        fungible_id=info.get("id") or "",
        direction=item.get("direction") or "",
        amount=int(quantity.get("int") or 0),
        quantity=Decimal(quantity.get("numeric") or "0"),
        price_usd=_optional_float(item.get("price")),
        value_usd=_optional_float(item.get("value")),
        sender=(item.get("sender") or "").lower(),
        recipient=(item.get("recipient") or "").lower(),
    )


def _transaction(item: dict) -> ZerionTransaction:
    attributes = item["attributes"]
    chain = item["relationships"]["chain"]["data"]["id"]
    return ZerionTransaction(
        zerion_id=item["id"],
        chain=chain,
        tx_hash=attributes.get("hash") or "",
        block=attributes.get("mined_at_block"),
        mined_at=datetime.fromisoformat(attributes["mined_at"].replace("Z", "+00:00")),
        operation_type=attributes.get("operation_type") or "",
        status=attributes.get("status") or "",
        fee_usd=_optional_float((attributes.get("fee") or {}).get("value")),
        transfers=[_transfer(i, chain, t) for i, t in enumerate(attributes.get("transfers") or [])],
        raw=item,
    )


def _cursor(next_url: str | None) -> str | None:
    if not next_url:
        return None
    return parse_qs(urlparse(next_url).query).get("page[after]", [None])[0]


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

    def transactions(
        self, address: str, since: datetime, cursor: str | None = None
    ) -> TransactionsPage:
        params = {
            "currency": "usd",
            "page[size]": 100,
            "filter[trash]": "only_non_trash",
            "filter[operation_types]": OPERATION_TYPES,
            "filter[min_mined_at]": int(since.timestamp() * 1000),
        }
        if cursor:
            params["page[after]"] = cursor
        payload = self._http.get(f"/wallets/{address}/transactions/", params=params)
        return TransactionsPage(
            [_transaction(item) for item in payload.get("data", [])],
            _cursor((payload.get("links") or {}).get("next")),
        )

    def portfolio(self, address: str) -> Portfolio:
        attributes = self._http.get(f"/wallets/{address}/portfolio", params={"currency": "usd"})[
            "data"
        ]["attributes"]
        return Portfolio(
            total_usd=float((attributes.get("total") or {}).get("positions") or 0.0),
            by_chain={
                k: float(v)
                for k, v in (attributes.get("positions_distribution_by_chain") or {}).items()
            },
        )
