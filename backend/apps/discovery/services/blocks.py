"""Conversion timestamp → numéro de bloc. Fonction pure : la source des timestamps est injectée."""

from collections.abc import Callable


def find_block_at(target_ts: int, low: int, high: int, timestamp_of: Callable[[int], int]) -> int:
    """Premier bloc de [low, high] dont le timestamp est >= target_ts.

    Alterne interpolation (rapide quand le temps de bloc est régulier) et dichotomie
    (garantit un nombre d'appels logarithmique quand il ne l'est pas).
    """
    low_ts, high_ts = timestamp_of(low), timestamp_of(high)
    if target_ts <= low_ts:
        return low
    if target_ts > high_ts:
        return high
    # Invariant : timestamp(low) < target_ts <= timestamp(high)
    step = 0
    while high - low > 1:
        if step % 2 == 0 and high_ts > low_ts:
            guess = low + (target_ts - low_ts) * (high - low) // (high_ts - low_ts)
        else:
            guess = (low + high) // 2
        middle = min(max(guess, low + 1), high - 1)
        middle_ts = timestamp_of(middle)
        if middle_ts < target_ts:
            low, low_ts = middle, middle_ts
        else:
            high, high_ts = middle, middle_ts
        step += 1
    return high


def find_block_near(
    target_ts: int, height: int, timestamp_of: Callable[[int], int], sample: int = 100_000
) -> int:
    """Premier bloc dont le timestamp est >= target_ts, en partant du rythme récent des blocs.

    Estime le bloc cible depuis le temps de bloc des `sample` derniers blocs, puis cherche dans
    une fenêtre étroite autour de l'estimation. Si la cible n'y est pas (chaîne irrégulière),
    retombe sur la recherche complète [0, height].
    """
    top_ts = timestamp_of(height)
    if target_ts >= top_ts:
        return height
    reference = max(height - sample, 0)
    reference_ts = timestamp_of(reference)
    if height > reference and top_ts > reference_ts:
        seconds_per_block = (top_ts - reference_ts) / (height - reference)
        guess = int(height - (top_ts - target_ts) / seconds_per_block)
        span = max(abs(height - guess) // 20, 1_000)
        low, high = max(guess - span, 0), min(guess + span, height)
        if timestamp_of(low) < target_ts <= timestamp_of(high):
            return find_block_at(target_ts, low, high, timestamp_of)
    return find_block_at(target_ts, 0, height, timestamp_of)
