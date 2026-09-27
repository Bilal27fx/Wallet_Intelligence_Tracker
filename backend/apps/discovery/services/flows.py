"""Flux d'un token explosif : achats, ventes, sorties et transferts d'entité. Fonctions pures.

Vente = envoi vers un pool du token. Sortie = envoi vers un hub (router, exchange, burn), une
adresse de dépôt, une adresse connue, ou petit envoi. Transfert d'entité = gros envoi vers toute
autre adresse (un « coffre ») : la position, son prix de revient (coût moyen) et la date du
premier achat la suivent.
"""

from bisect import bisect_right
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field

from integrations.geckoterminal import Candle
from integrations.hypersync import Transfer

ZERO_ADDRESS = "0x" + "0" * 40
DEAD_ADDRESS = "0x" + "0" * 36 + "dead"
HUB, DEPOSIT, KNOWN, VAULT = "hub", "deposit", "known", "vault"


@dataclass
class PairFlow:
    amount: int
    first_block: int
    first_ts: int
    tx_hash: str


class FlowScanner:
    """Passe 1 : agrège page par page ; la mémoire suit le nombre d'adresses, pas de transferts."""

    def __init__(
        self, *, candles: list[Candle], decimals: int, pools: set[str], hub_min_senders: int
    ):
        self._candles = candles
        self._times = [candle.ts for candle in candles]
        self._scale = 10**decimals
        self.pools = set(pools)
        self.hub_min_senders = hub_min_senders
        self.bought: dict[str, int] = defaultdict(int)
        self.cost: dict[str, float] = defaultdict(float)
        self.first_buy: dict[str, tuple[int, int]] = {}
        self.sold: dict[str, int] = defaultdict(int)
        self.moves: dict[tuple[str, str], PairFlow] = {}
        self.senders: dict[str, set[str]] = defaultdict(set)
        self.received_at: dict[str, int] = {}

    def price_at(self, ts: int) -> float:
        if not self._candles:
            return 0.0
        index = bisect_right(self._times, ts) - 1
        return self._candles[max(index, 0)].close

    def _tracked(self, address: str) -> bool:
        return address in self.bought or address in self.received_at

    def add(self, transfers: list[Transfer]) -> None:
        for t in sorted(transfers, key=lambda t: (t.block, t.log_index)):
            signer = t.tx_from
            if t.recipient == signer and t.sender not in (signer, ZERO_ADDRESS):
                self.bought[signer] += t.amount
                self.cost[signer] += t.amount / self._scale * self.price_at(t.timestamp)
                self.first_buy.setdefault(signer, (t.block, t.timestamp))
                continue
            if t.recipient in self.pools:
                if self._tracked(t.sender):
                    self.sold[t.sender] += t.amount
                continue
            senders = self.senders[t.recipient]
            if len(senders) < self.hub_min_senders:
                senders.add(t.sender)
            if self._tracked(t.sender) and t.recipient != t.sender:
                flow = self.moves.get((t.sender, t.recipient))
                if flow is None:
                    flow = PairFlow(0, t.block, t.timestamp, t.tx_hash)
                    self.moves[(t.sender, t.recipient)] = flow
                flow.amount += t.amount
                self.received_at.setdefault(t.recipient, t.timestamp)


def hubs(scanner: FlowScanner) -> set[str]:
    found = {r for r, senders in scanner.senders.items() if len(senders) >= scanner.hub_min_senders}
    return found | {ZERO_ADDRESS, DEAD_ADDRESS}


def classify_recipients(
    scanner: FlowScanner,
    *,
    known: set[str],
    deposit_forward_pct: float,
    deposit_forward_hours: int,
) -> dict[str, str]:
    """Hub, adresse connue, adresse de dépôt (fait suivre vers un hub) ou coffre."""
    hub_set = hubs(scanner)
    received: dict[str, int] = defaultdict(int)
    outgoing: dict[str, list[tuple[str, PairFlow]]] = defaultdict(list)
    for (sender, recipient), flow in scanner.moves.items():
        received[recipient] += flow.amount
        outgoing[sender].append((recipient, flow))
    kinds = {}
    for recipient, total in received.items():
        if recipient in hub_set:
            kinds[recipient] = HUB
        elif recipient in known:
            kinds[recipient] = KNOWN
        else:
            start = scanner.received_at.get(recipient, 0)
            forwarded = sum(
                flow.amount
                for target, flow in outgoing[recipient]
                if target in hub_set and flow.first_ts - start <= deposit_forward_hours * 3600
            )
            is_deposit = total > 0 and forwarded * 100 >= deposit_forward_pct * total
            kinds[recipient] = DEPOSIT if is_deposit else VAULT
    return kinds


@dataclass
class Holder:
    address: str
    bought: int = 0
    cost: float = 0.0
    first_block: int = 0
    first_ts: int = 0
    out: int = 0
    inherited: int = 0
    inherited_cost: float = 0.0
    inherited_from: str = ""
    sent_to_vaults: int = 0

    @property
    def inflow(self) -> int:
        return self.bought + self.inherited

    @property
    def avg_cost(self) -> float:
        return (self.cost + self.inherited_cost) / self.inflow if self.inflow else 0.0

    @property
    def held(self) -> int:
        return max(self.inflow - self.out - self.sent_to_vaults, 0)

    @property
    def held_cost(self) -> float:
        return self.held * self.avg_cost


@dataclass(frozen=True)
class VaultLink:
    sender: str
    recipient: str
    amount: int
    pct: float
    first_block: int
    tx_hash: str


@dataclass
class Flows:
    holders: dict[str, Holder]
    links: list[VaultLink] = field(default_factory=list)


def resolve_flows(
    scanner: FlowScanner, kinds: dict[str, str], *, big_pct: float, bots=frozenset()
) -> Flows:
    """Positions au creux : les gros envois vers un coffre y déplacent quantité et coût."""
    holders: dict[str, Holder] = {}
    for address, amount in scanner.bought.items():
        block, ts = scanner.first_buy[address]
        holders[address] = Holder(address, amount, scanner.cost[address], block, ts)
    for address, amount in scanner.sold.items():
        holders.setdefault(address, Holder(address)).out += amount
    links = []
    ordered = sorted(scanner.moves.items(), key=lambda item: item[1].first_block)
    for (sender, recipient), flow in ordered:
        source = holders.get(sender)
        if source is None or source.inflow == 0:
            continue
        pct = flow.amount * 100 / source.inflow
        available = source.inflow - source.out - source.sent_to_vaults
        is_vault = (
            kinds.get(recipient) == VAULT
            and pct >= big_pct
            and sender not in bots
            and recipient not in bots
            and available > 0
        )
        if not is_vault:
            source.out += flow.amount
            continue
        amount = min(flow.amount, available)
        vault = holders.setdefault(recipient, Holder(recipient))
        vault.inherited += amount
        vault.inherited_cost += (source.cost + source.inherited_cost) * amount / source.inflow
        vault.inherited_from = vault.inherited_from or sender
        if source.first_block and (not vault.first_block or source.first_block < vault.first_block):
            vault.first_block, vault.first_ts = source.first_block, source.first_ts
        source.sent_to_vaults += amount
        links.append(
            VaultLink(sender, recipient, amount, round(pct, 2), flow.first_block, flow.tx_hash)
        )
    for bot in bots:
        holders.pop(bot, None)
    return Flows(holders, links)


@dataclass(frozen=True)
class EntityCandidate:
    wallets: list[str]
    held_amount: int
    held_usd: float
    first_ts: int


def group_entities(flows: Flows, *, trough_price: float, scale: int) -> list[EntityCandidate]:
    """Wallets reliés par des liens coffre = une entité ; triées par position au creux."""
    parent: dict[str, str] = {}

    def find(address: str) -> str:
        parent.setdefault(address, address)
        while parent[address] != address:
            parent[address] = parent[parent[address]]
            address = parent[address]
        return address

    for link in flows.links:
        parent[find(link.sender)] = find(link.recipient)
    groups: dict[str, list[str]] = defaultdict(list)
    for address, holder in flows.holders.items():
        if holder.held > 0 or address in parent:
            groups[find(address)].append(address)
    candidates = []
    for members in groups.values():
        holders = [flows.holders[a] for a in members if a in flows.holders]
        held = sum(h.held for h in holders)
        first = min((h.first_ts for h in holders if h.first_ts), default=0)
        usd = round(held / scale * trough_price, 2)
        candidates.append(EntityCandidate(sorted(members), held, usd, first))
    return sorted(candidates, key=lambda c: c.held_usd, reverse=True)


@dataclass
class Selection:
    flows: Flows
    selected: list[EntityCandidate]
    bots: dict[str, tuple[float, float]]


def select_entities(
    scanner: FlowScanner,
    kinds: dict[str, str],
    *,
    bot_check: Callable[[list[str]], dict[str, tuple[bool, float]]],
    batch_size: int,
    big_pct: float,
    trough_price: float,
    scale: int,
    min_usd: float,
    max_entities: int,
) -> Selection:
    """Top des entités au creux ; un bot trouvé est retiré et le calcul recommence sans lui.

    Les wallets sont vérifiés par lots (`bot_check` reçoit une liste d'adresses), dans l'ordre
    du classement, pour ne faire qu'une requête par lot.
    """
    bots: dict[str, tuple[float, float]] = {}
    checked: dict[str, tuple[bool, float]] = {}
    while True:
        flows = resolve_flows(scanner, kinds, big_pct=big_pct, bots=frozenset(bots))
        ranked = [
            candidate
            for candidate in group_entities(flows, trough_price=trough_price, scale=scale)
            if candidate.held_usd >= min_usd
        ]
        selected: list[EntityCandidate] = []
        found = False
        for index, candidate in enumerate(ranked):
            if any(wallet not in checked for wallet in candidate.wallets):
                pending: list[str] = []
                for later in ranked[index:]:
                    pending += [w for w in later.wallets if w not in checked and w not in pending]
                    if len(pending) >= batch_size:
                        break
                checked.update(bot_check(pending[: max(batch_size, len(candidate.wallets))]))
            for wallet in candidate.wallets:
                is_bot, per_day = checked.get(wallet, (False, 0.0))
                if is_bot:
                    holder = flows.holders.get(wallet)
                    usd = round(holder.held / scale * trough_price, 2) if holder else 0.0
                    bots[wallet] = (per_day, usd)
                    found = True
            if found:
                break
            selected.append(candidate)
            if max_entities and len(selected) >= max_entities:
                break
        if not found:
            return Selection(flows, selected, bots)
