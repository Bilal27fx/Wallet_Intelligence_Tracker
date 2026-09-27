import respx

from integrations.http import JsonHttpClient
from integrations.ratelimit import NoopLimiter
from integrations.zerion import BASE_URL, ZerionClient

USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
XL = "0x1cdb289befdfac8af945a288bcdccc382cb34d32"


def client() -> ZerionClient:
    return ZerionClient(JsonHttpClient(BASE_URL, limiter=NoopLimiter(), max_retries=0))


def fungible(fid, symbol, price, impls):
    return {
        "id": fid,
        "attributes": {
            "symbol": symbol,
            "market_data": {"price": price},
            "implementations": [{"chain_id": c, "address": a, "decimals": d} for c, a, d in impls],
        },
    }


@respx.mock
def test_prices_batches_and_maps_back_to_requested_pairs():
    route = respx.get(f"{BASE_URL}/fungibles/").respond(
        json={
            "data": [
                fungible(
                    "usdc",
                    "USDC",
                    0.9998,
                    [("ethereum", "0xa0b8", 6), ("base", USDC_BASE.upper().replace("0X", "0x"), 6)],
                ),
                fungible("xl-id", "XL", 0.00012, [("robinhood", XL, 18)]),
                fungible("noprice", "SPAM", None, [("base", "0xspam", 18)]),
            ]
        }
    )
    prices = client().prices([("base", USDC_BASE), ("robinhood", XL), ("base", "0xspam")])
    assert prices[("base", USDC_BASE)].price == 0.9998
    assert prices[("base", USDC_BASE)].implementations["base"] == (USDC_BASE, 6)
    assert prices[("robinhood", XL)].symbol == "XL"
    assert prices[("base", "0xspam")].price is None
    params = route.calls.last.request.url.params
    assert (
        params["filter[fungible_implementations]"] == f"base:{USDC_BASE},base:0xspam,robinhood:{XL}"
    )


@respx.mock
def test_prices_splits_in_batches_of_25():
    route = respx.get(f"{BASE_URL}/fungibles/").respond(json={"data": []})
    client().prices([("base", f"0x{i:040x}") for i in range(30)])
    assert route.call_count == 2


def test_prices_without_input_makes_no_call():
    assert client().prices([]) == {}


@respx.mock
def test_search_returns_top_market_cap():
    route = respx.get(f"{BASE_URL}/fungibles/").respond(
        json={"data": [fungible("usdc", "USDC", 1.0, [("base", USDC_BASE, 6)])]}
    )
    found = client().search("USDC")
    assert found.implementations == {"base": (USDC_BASE, 6)}
    params = route.calls.last.request.url.params
    assert params["filter[search_query]"] == "USDC"
    assert params["sort"] == "-market_data.market_cap"


@respx.mock
def test_search_without_result():
    respx.get(f"{BASE_URL}/fungibles/").respond(json={"data": []})
    assert client().search("NOPE") is None


@respx.mock
def test_fungible_by_id():
    respx.get(f"{BASE_URL}/fungibles/0xweth").respond(
        json={
            "data": fungible(
                "0xweth",
                "WETH",
                2000.0,
                [("base", "0x4200000000000000000000000000000000000006", 18)],
            )
        }
    )
    assert client().fungible("0xweth").implementations["base"][1] == 18


@respx.mock
def test_price_chart_reads_year_points():
    respx.get(f"{BASE_URL}/fungibles/eth/charts/year").respond(
        json={"data": {"attributes": {"points": [[1759017600, 4018.26], [1759104000, 4140.23]]}}}
    )
    assert client().price_chart("eth") == [(1759017600, 4018.26), (1759104000, 4140.23)]
