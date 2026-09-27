from apps.wallets.services.classify import classify_all
from apps.wallets.services.pricing import STABLE, WRAPPED, QuoteAsset, price_trades
from integrations.hypersync import WalletTransfer

W = "0x" + "a" * 40
POOL = "0x" + "b" * 40
ROUTER = "0x" + "9" * 40
OTHER = "0x" + "8" * 40
TOK = "0x" + "1" * 40
TOK2 = "0x" + "3" * 40
USDC = "0x" + "c" * 40
WETH = "0x" + "d" * 40
QUOTES = {USDC: QuoteAsset(STABLE, 6), WETH: QuoteAsset(WRAPPED, 18)}


def tr(tx, idx, token, sender, recipient, amount, tx_to=ROUTER, value=0):
    return WalletTransfer(
        block=1,
        timestamp=100,
        tx_hash=tx,
        log_index=idx,
        token=token,
        sender=sender,
        recipient=recipient,
        amount=amount,
        tx_from=W,
        tx_to=tx_to,
        tx_value=value,
    )


def run(transfers, native=2000.0):
    return price_trades(classify_all(transfers, W), W, QUOTES, lambda ts: native)


def test_buy_paid_in_stablecoin():
    usd = run([tr("0x1", 0, USDC, W, POOL, 1_500 * 10**6), tr("0x1", 1, TOK, POOL, W, 10**18)])
    assert usd[("0x1", 1)] == 1500.0
    assert usd[("0x1", 0)] == 1500.0


def test_buy_paid_in_native():
    assert run([tr("0x2", 0, TOK, POOL, W, 10**18, value=10**18 // 4)])[("0x2", 0)] == 500.0


def test_buy_paid_in_wrapped_native():
    usd = run([tr("0x3", 0, WETH, W, POOL, 10**17), tr("0x3", 1, TOK, POOL, W, 10**18)])
    assert usd[("0x3", 1)] == 200.0


def test_sell_for_stablecoin():
    usd = run([tr("0x4", 0, TOK, W, POOL, 10**18), tr("0x4", 1, USDC, POOL, W, 800 * 10**6)])
    assert usd[("0x4", 0)] == 800.0


def test_split_between_legs_of_same_token():
    usd = run(
        [
            tr("0x5", 0, USDC, W, POOL, 400 * 10**6),
            tr("0x5", 1, TOK, POOL, W, 1 * 10**18),
            tr("0x5", 2, TOK, POOL, W, 3 * 10**18),
        ]
    )
    assert (usd[("0x5", 1)], usd[("0x5", 2)]) == (100.0, 300.0)


def test_unknown_when_two_different_tokens_bought():
    usd = run(
        [
            tr("0x6", 0, USDC, W, POOL, 100 * 10**6),
            tr("0x6", 1, TOK, POOL, W, 10**18),
            tr("0x6", 2, TOK2, POOL, W, 10**18),
        ]
    )
    assert (usd[("0x6", 1)], usd[("0x6", 2)]) == (None, None)


def test_transfer_has_no_price():
    assert run([tr("0x7", 0, TOK, W, OTHER, 10**18, tx_to=TOK)])[("0x7", 0)] is None


def test_native_payment_without_price_is_unknown():
    assert run([tr("0x8", 0, TOK, POOL, W, 10**18, value=10**18)], native=None)[("0x8", 0)] is None
