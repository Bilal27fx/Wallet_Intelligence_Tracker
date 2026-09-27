from datetime import UTC, datetime
from decimal import Decimal

import respx

from integrations.http import JsonHttpClient
from integrations.ratelimit import NoopLimiter
from integrations.zerion import BASE_URL, NATIVE, OPERATION_TYPES, ZerionClient

W = "0x11edfaca715703cb91c384d84cd2551122ab3019"
TOKEN = "0xca7a1e31b36779cf32acb18714ab26982cf36b05"


def client() -> ZerionClient:
    return ZerionClient(JsonHttpClient(BASE_URL, limiter=NoopLimiter(), max_retries=0))


def transfer(
    direction, fungible_id, symbol, address, qty_int, numeric, value, price, sender, recipient
):
    return {
        "direction": direction,
        "quantity": {"int": qty_int, "decimals": 18, "float": float(numeric), "numeric": numeric},
        "value": value,
        "price": price,
        "sender": sender,
        "recipient": recipient,
        "fungible_info": {
            "id": fungible_id,
            "symbol": symbol,
            "implementations": [{"chain_id": "base", "address": address, "decimals": 18}],
        },
    }


TRADE = {
    "id": "1e7eb8fb",
    "attributes": {
        "operation_type": "trade",
        "hash": "0x5fa7",
        "mined_at_block": 51740628,
        "mined_at": "2026-09-24T17:23:23Z",
        "status": "confirmed",
        "fee": {"value": 0.0375},
        "transfers": [
            transfer(
                "in",
                "cat-id",
                "CATALYST",
                TOKEN.upper().replace("0X", "0x"),
                "743365907836862550998",
                "743.365907836862550998",
                1312.84,
                1.766,
                "0xPOOL",
                W,
            ),
            transfer(
                "out",
                "eth",
                "ETH",
                "",
                "500000000000000000",
                "0.5",
                1339.82,
                2679.65,
                W,
                "0xROUTER",
            ),
        ],
    },
    "relationships": {"chain": {"data": {"id": "base"}}},
}

NEXT = (
    f"{BASE_URL}/wallets/{W}/transactions/"
    "?currency=usd&page%5Bafter%5D=WyIyMDI2Il0%3D&page%5Bsize%5D=100"
)


@respx.mock
def test_transactions_parses_transfers_and_cursor():
    route = respx.get(f"{BASE_URL}/wallets/{W}/transactions/").respond(
        json={"data": [TRADE], "links": {"next": NEXT}}
    )
    page = client().transactions(W, datetime(2026, 3, 1, tzinfo=UTC))
    [tx] = page.transactions
    assert (tx.zerion_id, tx.chain, tx.tx_hash, tx.block) == (
        "1e7eb8fb",
        "base",
        "0x5fa7",
        51740628,
    )
    assert (tx.operation_type, tx.status, tx.fee_usd) == ("trade", "confirmed", 0.0375)
    assert tx.mined_at == datetime(2026, 9, 24, 17, 23, 23, tzinfo=UTC)
    buy, pay = tx.transfers
    assert (buy.index, buy.direction, buy.token_address, buy.token_symbol) == (
        0,
        "in",
        TOKEN,
        "CATALYST",
    )
    assert buy.amount == 743365907836862550998
    assert buy.quantity == Decimal("743.365907836862550998")
    assert (buy.price_usd, buy.value_usd, buy.sender, buy.recipient) == (
        1.766,
        1312.84,
        "0xpool",
        W,
    )
    assert (pay.index, pay.token_address, pay.fungible_id) == (1, NATIVE, "eth")
    assert tx.raw == TRADE
    assert page.next_cursor == "WyIyMDI2Il0="
    params = route.calls.last.request.url.params
    assert params["filter[operation_types]"] == OPERATION_TYPES
    assert params["filter[trash]"] == "only_non_trash"
    assert params["filter[min_mined_at]"] == str(
        int(datetime(2026, 3, 1, tzinfo=UTC).timestamp() * 1000)
    )
    assert "page[after]" not in params


@respx.mock
def test_transactions_sends_cursor_and_ends_without_next():
    route = respx.get(f"{BASE_URL}/wallets/{W}/transactions/").respond(
        json={"data": [], "links": {}}
    )
    page = client().transactions(W, datetime(2026, 3, 1, tzinfo=UTC), cursor="abc")
    assert page.transactions == [] and page.next_cursor is None
    assert route.calls.last.request.url.params["page[after]"] == "abc"


@respx.mock
def test_transfer_without_price():
    tx = {
        **TRADE,
        "attributes": {
            **TRADE["attributes"],
            "fee": None,
            "transfers": [
                transfer(
                    "in", "x", "OBSCURE", "0xabc", "5", "0.000000000000000005", None, None, "0xa", W
                )
            ],
        },
    }
    respx.get(f"{BASE_URL}/wallets/{W}/transactions/").respond(json={"data": [tx], "links": {}})
    [parsed] = client().transactions(W, datetime(2026, 3, 1, tzinfo=UTC)).transactions
    assert parsed.fee_usd is None
    assert (parsed.transfers[0].price_usd, parsed.transfers[0].value_usd) == (None, None)


def position(chain, symbol, address, fungible_id, numeric, price, value):
    return {
        "attributes": {
            "position_type": "wallet",
            "quantity": {"numeric": numeric},
            "price": price,
            "value": value,
            "fungible_info": {
                "symbol": symbol,
                "implementations": [{"chain_id": chain, "address": address}],
            },
        },
        "relationships": {
            "chain": {"data": {"id": chain}},
            "fungible": {"data": {"id": fungible_id}},
        },
    }


@respx.mock
def test_portfolio_from_positions():
    route = respx.get(f"{BASE_URL}/wallets/{W}/positions/").respond(
        json={
            "data": [
                position(
                    "robinhood",
                    "AI",
                    TOKEN.upper().replace("0X", "0x"),
                    "ai-id",
                    "4855000.5",
                    0.2124,
                    1031383.02,
                ),
                position("base", "ETH", None, "eth", "0.5", 2600.0, 1300.0),
            ],
            "links": {},
        }
    )
    portfolio = client().portfolio(W)
    assert portfolio.total_usd == 1032683.02
    assert portfolio.by_chain == {"robinhood": 1031383.02, "base": 1300.0}
    ai, eth = portfolio.positions
    assert (ai.chain, ai.token_address, ai.fungible_id, ai.quantity) == (
        "robinhood",
        TOKEN,
        "ai-id",
        Decimal("4855000.5"),
    )
    assert (eth.token_address, eth.symbol, eth.value_usd) == (NATIVE, "ETH", 1300.0)
    assert route.calls.last.request.url.params["filter[positions]"] == "no_filter"


@respx.mock
def test_operation_types_are_configurable():
    route = respx.get(f"{BASE_URL}/wallets/{W}/transactions/").respond(
        json={"data": [], "links": {}}
    )
    http = JsonHttpClient(BASE_URL, limiter=NoopLimiter(), max_retries=0)
    ZerionClient(http, operation_types="trade,deposit").transactions(
        W, datetime(2026, 3, 1, tzinfo=UTC)
    )
    assert route.calls.last.request.url.params["filter[operation_types]"] == "trade,deposit"
