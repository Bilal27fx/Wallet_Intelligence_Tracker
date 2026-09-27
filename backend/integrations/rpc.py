"""Client JSON-RPC minimal : solde natif d'une adresse sur le RPC public d'une chaîne."""

from integrations.errors import UpstreamError


class RpcClient:
    def __init__(self, http):
        self._http = http

    def native_balance(self, address: str) -> int:
        payload = self._http.post(
            "",
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getBalance",
                "params": [address, "latest"],
            },
        )
        if "result" not in payload:
            raise UpstreamError(f"eth_getBalance : {payload.get('error')}")
        return int(payload["result"], 16)
