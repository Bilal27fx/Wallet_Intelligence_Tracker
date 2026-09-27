import httpx
import pytest
import respx

from integrations.errors import BudgetExhausted, NotFound, RateLimited, UpstreamError
from integrations.http import JsonHttpClient
from integrations.ratelimit import NoopLimiter

BASE = "https://api.test"


def make_client(max_retries: int = 2) -> tuple[JsonHttpClient, list[float]]:
    sleeps: list[float] = []
    client = JsonHttpClient(
        BASE, limiter=NoopLimiter(), max_retries=max_retries, sleep=sleeps.append
    )
    return client, sleeps


@respx.mock
def test_returns_json():
    respx.get(f"{BASE}/items", params={"page": "1"}).respond(json={"ok": True})
    client, _ = make_client()
    assert client.get("/items", params={"page": 1}) == {"ok": True}


@respx.mock
def test_retries_server_errors_with_backoff():
    respx.get(f"{BASE}/items").mock(
        side_effect=[httpx.Response(500), httpx.Response(503), httpx.Response(200, json=[1])]
    )
    client, sleeps = make_client(max_retries=2)
    assert client.get("/items") == [1]
    assert sleeps == [1.0, 2.0]


@respx.mock
def test_raises_upstream_error_after_retries():
    respx.get(f"{BASE}/items").respond(500)
    client, sleeps = make_client(max_retries=2)
    with pytest.raises(UpstreamError):
        client.get("/items")
    assert len(sleeps) == 2


@respx.mock
def test_raises_rate_limited_after_retries():
    respx.get(f"{BASE}/items").respond(429)
    client, _ = make_client(max_retries=1)
    with pytest.raises(RateLimited):
        client.get("/items")


@respx.mock
def test_404_raises_not_found_without_retry():
    route = respx.get(f"{BASE}/items").respond(404)
    client, sleeps = make_client()
    with pytest.raises(NotFound):
        client.get("/items")
    assert route.call_count == 1
    assert sleeps == []


@respx.mock
def test_other_client_errors_are_not_retried():
    route = respx.get(f"{BASE}/items").respond(400)
    client, _ = make_client()
    with pytest.raises(UpstreamError):
        client.get("/items")
    assert route.call_count == 1


@respx.mock
def test_retries_transport_errors():
    respx.get(f"{BASE}/items").mock(
        side_effect=[httpx.ConnectError("down"), httpx.Response(200, json={"ok": 1})]
    )
    client, _ = make_client()
    assert client.get("/items") == {"ok": 1}


class CountingBudget:
    def __init__(self, allowed: int):
        self.allowed = allowed
        self.used = 0

    def consume(self):
        self.used += 1
        if self.used > self.allowed:
            raise BudgetExhausted("fini")


@respx.mock
def test_post_sends_json():
    route = respx.post(f"{BASE}/rpc").respond(json={"result": "0x1"})
    client, _ = make_client()
    assert client.post("/rpc", json={"method": "x"}) == {"result": "0x1"}
    assert route.calls.last.request.content == b'{"method":"x"}'


@respx.mock
def test_budget_is_consumed_per_attempt():
    respx.get(f"{BASE}/items").mock(side_effect=[httpx.Response(500), httpx.Response(200, json={})])
    budget = CountingBudget(allowed=10)
    client = JsonHttpClient(BASE, limiter=NoopLimiter(), budget=budget, sleep=lambda s: None)
    client.get("/items")
    assert budget.used == 2


@respx.mock
def test_exhausted_budget_stops_before_calling():
    route = respx.get(f"{BASE}/items").respond(json={})
    client = JsonHttpClient(BASE, limiter=NoopLimiter(), budget=CountingBudget(allowed=0))
    with pytest.raises(BudgetExhausted):
        client.get("/items")
    assert route.call_count == 0
