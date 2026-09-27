import pytest
import respx

from integrations.errors import UpstreamError
from integrations.http import JsonHttpClient
from integrations.ratelimit import NoopLimiter
from integrations.rpc import RpcClient

RPC = "https://rpc.test/"


def client() -> RpcClient:
    return RpcClient(JsonHttpClient(RPC, limiter=NoopLimiter(), max_retries=0))


@respx.mock
def test_native_balance():
    route = respx.post(RPC).respond(json={"jsonrpc": "2.0", "id": 1, "result": "0xde0b6b3a7640000"})
    assert client().native_balance("0xabc") == 10**18
    assert b'"eth_getBalance"' in route.calls.last.request.content


@respx.mock
def test_rpc_error_raises():
    respx.post(RPC).respond(json={"jsonrpc": "2.0", "id": 1, "error": {"message": "nope"}})
    with pytest.raises(UpstreamError):
        client().native_balance("0xabc")
