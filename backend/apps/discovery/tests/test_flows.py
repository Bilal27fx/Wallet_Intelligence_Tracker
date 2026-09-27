import pytest

from apps.discovery.services.flows import (
    DEPOSIT,
    HUB,
    VAULT,
    DEAD_ADDRESS,
    ZERO_ADDRESS,
    FlowScanner,
    classify_recipients,
    group_entities,
    resolve_flows,
    select_entities,
)
from integrations.geckoterminal import Candle
from integrations.hypersync import Transfer

UNIT = 10**18
POOL = "0x" + "b" * 40
A, B, C, D, H = ("0x" + c * 40 for c in "acdef")
SWEEPER = "0x" + "9" * 40
FEEDERS = ["0x" + c * 40 for c in "123"]
CANDLES = [Candle(0, 1, 1, 1, 1.0, 0)]
_counter = iter(range(10**6))


def tr(block, signer, sender, recipient, tokens):
    tx = f"0x{next(_counter):x}"
    return Transfer(block, block * 10, signer, sender, recipient, tokens * UNIT, tx, 0)


def buy(block, wallet, tokens):
    return tr(block, wallet, POOL, wallet, tokens)


def send(block, sender, recipient, tokens):
    return tr(block, sender, sender, recipient, tokens)


def hub_feeders():
    return [send(1, feeder, H, 1) for feeder in FEEDERS]


def scan(transfers, hub_min_senders=3):
    scanner = FlowScanner(
        candles=CANDLES, decimals=18, pools={POOL}, hub_min_senders=hub_min_senders
    )
    for transfer in transfers:
        scanner.add([transfer])
    return scanner


def resolve(scanner, big_pct=20, bots=frozenset(), known=frozenset()):
    kinds = classify_recipients(
        scanner, known=set(known), deposit_forward_pct=90, deposit_forward_hours=1
    )
    return kinds, resolve_flows(scanner, kinds, big_pct=big_pct, bots=bots)


def test_sell_to_pool_reduces_position():
    _, flows = resolve(scan([buy(1, A, 1000), send(2, A, POOL, 300)]))
    assert flows.holders[A].held == 700 * UNIT


def test_big_send_to_vault_moves_position_and_cost():
    kinds, flows = resolve(scan([buy(1, A, 1000), send(2, A, B, 800)]))
    assert kinds[B] == VAULT
    assert flows.holders[A].held == 200 * UNIT
    vault = flows.holders[B]
    assert (vault.held, vault.inherited_from) == (800 * UNIT, A)
    assert vault.inherited_cost == pytest.approx(800.0)
    assert vault.first_block == 1
    [link] = flows.links
    assert (link.sender, link.recipient, link.pct) == (A, B, 80.0)


def test_send_to_hub_is_an_exit():
    kinds, flows = resolve(scan(hub_feeders() + [buy(2, A, 1000), send(3, A, H, 900)]))
    assert kinds[H] == HUB
    assert flows.holders[A].held == 100 * UNIT and not flows.links


def test_burn_is_an_exit():
    kinds, flows = resolve(scan([buy(1, A, 1000), send(2, A, ZERO_ADDRESS, 900)]))
    assert kinds[ZERO_ADDRESS] == HUB and not flows.links


def test_send_to_dead_address_is_a_burn():
    kinds, flows = resolve(scan([buy(1, A, 1000), send(2, A, DEAD_ADDRESS, 900)]))
    assert kinds[DEAD_ADDRESS] == HUB and not flows.links
    assert flows.holders[A].held == 100 * UNIT


def test_deposit_address_forwarding_to_hub_is_an_exit():
    sweep = tr(4, SWEEPER, D, H, 900)
    kinds, flows = resolve(scan(hub_feeders() + [buy(2, A, 1000), send(3, A, D, 900), sweep]))
    assert kinds[D] == DEPOSIT
    assert not flows.links and D not in flows.holders
    assert flows.holders[A].held == 100 * UNIT


def test_small_send_is_an_exit():
    _, flows = resolve(scan([buy(1, A, 1000), send(2, A, B, 100)]))
    assert flows.holders[A].held == 900 * UNIT and not flows.links


def test_known_exchange_is_an_exit():
    _, flows = resolve(scan([buy(1, A, 1000), send(2, A, B, 800)]), known={B})
    assert not flows.links


def test_chain_of_vaults_keeps_first_buy_and_cost():
    _, flows = resolve(scan([buy(1, A, 1000), send(5, A, B, 1000), send(9, B, C, 1000)]))
    vault = flows.holders[C]
    assert (vault.held, vault.first_block) == (1000 * UNIT, 1)
    assert vault.inherited_cost == pytest.approx(1000.0)


def test_vault_that_buys_itself_keeps_both():
    _, flows = resolve(scan([buy(1, A, 1000), send(2, A, B, 1000), buy(3, B, 500)]))
    vault = flows.holders[B]
    assert (vault.held, vault.bought, vault.inherited) == (1500 * UNIT, 500 * UNIT, 1000 * UNIT)
    assert vault.first_block == 1


def test_bot_sender_creates_no_link_and_is_dropped():
    _, flows = resolve(scan([buy(1, A, 1000), send(2, A, B, 800)]), bots=frozenset({A}))
    assert not flows.links and A not in flows.holders and B not in flows.holders


def test_group_entities_sums_members():
    transfers = [buy(1, A, 1000), send(2, A, B, 800), buy(3, C, 500), send(4, C, B, 500)]
    _, flows = resolve(scan(transfers))
    [entity] = group_entities(flows, trough_price=2.0, scale=UNIT)
    assert entity.wallets == sorted([A, B, C])
    assert entity.held_usd == 3000.0


def select(scanner, bot_check, batch=10, max_entities=2):
    kinds = classify_recipients(
        scanner, known=set(), deposit_forward_pct=90, deposit_forward_hours=1
    )
    return select_entities(
        scanner,
        kinds,
        bot_check=bot_check,
        batch_size=batch,
        big_pct=20,
        trough_price=1.0,
        scale=UNIT,
        min_usd=500,
        max_entities=max_entities,
    )


def test_select_entities_replaces_bot_by_next():
    scanner = scan([buy(1, A, 5000), buy(2, B, 1000), buy(3, C, 800)])
    selection = select(
        scanner, lambda batch: {a: (a == A, 300.0 if a == A else 1.0) for a in batch}
    )
    assert [c.wallets for c in selection.selected] == [[B], [C]]
    assert selection.bots == {A: (300.0, 5000.0)}


def test_bot_checks_are_batched():
    scanner = scan([buy(1, A, 5000), buy(2, B, 1000), buy(3, C, 800)])
    batches = []

    def check(batch):
        batches.append(list(batch))
        return {a: (False, 1.0) for a in batch}

    select(scanner, check, batch=2, max_entities=3)
    assert batches == [[A, B], [C]]
