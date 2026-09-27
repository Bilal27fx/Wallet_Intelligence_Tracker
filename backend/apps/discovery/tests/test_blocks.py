import pytest

from apps.discovery.services.blocks import find_block_at, find_block_near


def regular(genesis: int = 1000, block_time: int = 2):
    calls = []

    def timestamp_of(block: int) -> int:
        calls.append(block)
        return genesis + block * block_time

    return timestamp_of, calls


@pytest.mark.parametrize(("target", "expected"), [(1000, 0), (1001, 1), (1002, 1), (21000, 10000)])
def test_finds_first_block_at_or_after_target(target, expected):
    timestamp_of, _ = regular()
    assert find_block_at(target, 0, 20_000_000, timestamp_of) == expected


def test_clamps_to_range():
    timestamp_of, _ = regular()
    assert find_block_at(0, 100, 200, timestamp_of) == 100
    assert find_block_at(10**12, 100, 200, timestamp_of) == 200


def test_converges_fast_on_regular_chains():
    timestamp_of, calls = regular()
    find_block_at(1000 + 2 * 12_345_678, 0, 20_000_000, timestamp_of)
    assert len(calls) <= 6


def test_stays_logarithmic_on_irregular_chains():
    # Blocs très lents au début puis très rapides : l'interpolation seule dégénère.
    def timestamp_of(block: int) -> int:
        return block * 1000 if block < 1000 else 1_000_000 + (block - 1000)

    calls = []

    def counting(block: int) -> int:
        calls.append(block)
        return timestamp_of(block)

    assert find_block_at(500_000, 0, 10_000_000, counting) == 500
    assert len(calls) <= 60


def test_find_block_near_uses_recent_block_rate():
    # Genèse à 0 (comme Robinhood) puis 2 s par bloc : l'interpolation sur [0, hauteur] échoue.
    def timestamp_of(block: int) -> int:
        return 0 if block == 0 else 1_600_000_000 + block * 2

    calls = []

    def counting(block: int) -> int:
        calls.append(block)
        return timestamp_of(block)

    height = 73_000_000
    target = timestamp_of(height) - 7 * 86_400
    assert find_block_near(target, height, counting) == height - 7 * 86_400 // 2
    # Le client HyperSync met les timestamps en cache : seuls les blocs distincts coûtent.
    assert len(set(calls)) <= 8


def test_find_block_near_after_top_returns_height():
    assert find_block_near(10**12, 100, lambda b: b * 10) == 100


def test_find_block_near_falls_back_on_irregular_chains():
    def timestamp_of(block: int) -> int:
        return block * 1000 if block < 1000 else 1_000_000 + (block - 1000)

    assert find_block_near(500_000, 10_000_000, timestamp_of) == 500


def test_find_block_near_target_before_chain_start():
    # Chaîne jeune : la cible (il y a un an) précède le premier bloc → bloc 0, jamais négatif.
    def timestamp_of(block: int) -> int:
        return 0 if block == 0 else 1_780_000_000 + block // 4

    height = 40_000_000
    target = timestamp_of(height) - 365 * 86_400
    assert find_block_near(target, height, timestamp_of) in (0, 1)
