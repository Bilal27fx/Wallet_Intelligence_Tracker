"""Fabriques de données de test."""

from apps.discovery.models import Candidate, Chain, Token


def make_chain(**overrides) -> Chain:
    values = {
        "gt_id": "base",
        "name": "Base",
        "evm_id": 8453,
        "zerion_id": "base",
        "hypersync_supported": True,
        "is_enabled": True,
    }
    values.update(overrides)
    return Chain.objects.create(**values)


def make_token(chain: Chain | None = None, address: str = "0x" + "1" * 40, **overrides) -> Token:
    values = {"symbol": "TKN", "decimals": 18}
    values.update(overrides)
    return Token.objects.create(chain=chain or make_chain(), address=address, **values)


def make_candidate(token: Token | None = None, **overrides) -> Candidate:
    return Candidate.objects.create(token=token or make_token(), **overrides)
