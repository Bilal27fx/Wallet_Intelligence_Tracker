"""Client HyperSync (Envio) : chaînes supportées, blocs et transferts ERC-20."""

import asyncio
from dataclasses import dataclass

import hypersync
from hypersync import (
    BlockField,
    ClientConfig,
    FieldSelection,
    LogField,
    LogSelection,
    Query,
    TransactionField,
)

from integrations.errors import TooManyTransfers, UpstreamError

CHAINS_URL = "https://chains.hyperquery.xyz"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


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


@dataclass(frozen=True)
class Transfer:
    block: int
    timestamp: int
    tx_from: str
    sender: str
    recipient: str
    amount: int


def _int(value) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    return int(value, 16) if value.startswith("0x") else int(value)


def _topic_address(topic: str) -> str:
    return "0x" + topic[-40:].lower()


class HyperSyncClient:
    def __init__(self, chain_id: int, api_token: str, limiter, max_retries: int = 3, inner=None):
        self._inner = inner or hypersync.HypersyncClient(
            ClientConfig(
                url=hypersync_url(chain_id), bearer_token=api_token, max_num_retries=max_retries
            )
        )
        self._limiter = limiter
        self._timestamps: dict[int, int] = {}

    def height(self) -> int:
        self._limiter.acquire()
        return asyncio.run(self._inner.get_height())

    def block_timestamp(self, number: int) -> int:
        if number not in self._timestamps:
            query = Query(
                from_block=number,
                to_block=number + 1,
                include_all_blocks=True,
                field_selection=FieldSelection(block=[BlockField.NUMBER, BlockField.TIMESTAMP]),
            )
            response = self._get(query)
            blocks = {block.number: _int(block.timestamp) for block in response.data.blocks}
            if number not in blocks:
                raise UpstreamError(f"HyperSync : bloc {number} absent de la réponse")
            self._timestamps[number] = blocks[number]
        return self._timestamps[number]

    def transfers(
        self, token: str, from_block: int, to_block: int, max_transfers: int
    ) -> list[Transfer]:
        query = Query(
            from_block=from_block,
            to_block=to_block,
            logs=[LogSelection(address=[token], topics=[[TRANSFER_TOPIC]])],
            field_selection=FieldSelection(
                block=[BlockField.NUMBER, BlockField.TIMESTAMP],
                transaction=[TransactionField.HASH, TransactionField.FROM],
                log=[
                    LogField.BLOCK_NUMBER,
                    LogField.TRANSACTION_HASH,
                    LogField.DATA,
                    LogField.TOPIC0,
                    LogField.TOPIC1,
                    LogField.TOPIC2,
                ],
            ),
        )
        transfers: list[Transfer] = []
        while True:
            response = self._get(query)
            data = response.data
            timestamps = {block.number: _int(block.timestamp) for block in data.blocks}
            senders = {
                tx.hash: tx.from_.lower() for tx in data.transactions if tx.hash and tx.from_
            }
            for log in data.logs:
                topics = log.topics or []
                tx_from = senders.get(log.transaction_hash)
                # Les Transfer ERC-721 ont 4 topics et pas de data : on les ignore.
                if len(topics) != 3 or not log.data or log.data == "0x" or tx_from is None:
                    continue
                transfers.append(
                    Transfer(
                        block=log.block_number,
                        timestamp=timestamps.get(log.block_number, 0),
                        tx_from=tx_from,
                        sender=_topic_address(topics[1]),
                        recipient=_topic_address(topics[2]),
                        amount=_int(log.data),
                    )
                )
            if len(transfers) > max_transfers:
                raise TooManyTransfers(f"{token} : plus de {max_transfers} transferts")
            if response.next_block >= to_block:
                return transfers
            query.from_block = response.next_block

    def _get(self, query: Query):
        self._limiter.acquire()
        return asyncio.run(self._inner.get(query))
