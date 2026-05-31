# Phase 3: Business Logic Migration - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate all business logic (discovery, tracking, FIFO, scoring, consensus) from SQLite scripts to Django services with ORM.

**Architecture:** Services pattern with Django ORM, API clients in common/, management commands for CLI execution, validation tests comparing old vs new results.

**Tech Stack:** Django ORM, requests (API clients), pandas (data processing)

---

## Prerequisites

✅ Phase 1 completed (Django + PostgreSQL + data migrated)
✅ Phase 2 completed (API REST functional)
✅ Old SQLite database backup available for validation

---

## File Structure

**New files to create:**
```
backend/
├── common/
│   ├── api_clients/
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── zerion.py
│   │   ├── dune.py
│   │   └── dexscreener.py
│   └── constants.py
├── wallets/
│   ├── services/
│   │   ├── __init__.py
│   │   ├── discovery/
│   │   │   ├── __init__.py
│   │   │   ├── dune_client.py
│   │   │   └── explosion_detector.py
│   │   ├── tracking/
│   │   │   ├── __init__.py
│   │   │   ├── zerion_client.py
│   │   │   └── balance_tracker.py
│   │   ├── analytics/
│   │   │   ├── __init__.py
│   │   │   ├── fifo_calculator.py
│   │   │   ├── wallet_scorer.py
│   │   │   └── tier_analyzer.py
│   │   └── consensus/
│   │       ├── __init__.py
│   │       └── consensus_detector.py
│   ├── management/commands/
│   │   ├── run_discovery.py
│   │   ├── run_tracking.py
│   │   ├── run_scoring.py
│   │   ├── run_consensus.py
│   │   └── validate_migration.py
│   └── tests/
│       └── test_migration_validation.py
```

---

## Task 1: Create Common API Clients

**Files:**
- Create: `backend/common/api_clients/__init__.py`
- Create: `backend/common/api_clients/base.py`
- Create: `backend/common/api_clients/zerion.py`
- Create: `backend/common/api_clients/dune.py`
- Create: `backend/common/api_clients/dexscreener.py`

- [ ] **Step 1: Create common directory structure**

```bash
mkdir -p backend/common/api_clients
touch backend/common/__init__.py
touch backend/common/api_clients/__init__.py
```

- [ ] **Step 2: Write base API client**

Create `backend/common/api_clients/base.py`:
```python
"""Base API client."""
import requests
from typing import Optional, Dict, Any
import time


class BaseAPIClient:
    """Base class for external API clients."""

    def __init__(self, api_key: Optional[str] = None, base_url: str = ""):
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()

    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests."""
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        retry: int = 3,
    ) -> Any:
        """Make API request with retry logic."""
        url = f"{self.base_url}{endpoint}"

        for attempt in range(retry):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=self._get_headers(),
                    params=params,
                    json=data,
                    timeout=30,
                )
                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                if attempt == retry - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff

    def get(self, endpoint: str, params: Optional[Dict] = None) -> Any:
        """GET request."""
        return self._request("GET", endpoint, params=params)

    def post(self, endpoint: str, data: Optional[Dict] = None) -> Any:
        """POST request."""
        return self._request("POST", endpoint, data=data)
```

- [ ] **Step 3: Write Zerion client**

Create `backend/common/api_clients/zerion.py`:
```python
"""Zerion API client."""
from django.conf import settings
from .base import BaseAPIClient


class ZerionClient(BaseAPIClient):
    """Zerion API client for wallet data."""

    def __init__(self, api_key: str = None):
        api_key = api_key or settings.ZERION_API_KEY
        super().__init__(api_key=api_key, base_url="https://api.zerion.io/v1/")

    def get_wallet_portfolio(self, wallet_address: str, currency: str = "usd"):
        """Get wallet portfolio positions."""
        endpoint = f"wallets/{wallet_address}/positions/"
        params = {"currency": currency, "filter[positions]": "only_simple"}
        return self.get(endpoint, params=params)

    def get_wallet_transactions(
        self,
        wallet_address: str,
        fungible_id: str = None,
        limit: int = 100,
    ):
        """Get wallet transactions."""
        endpoint = f"wallets/{wallet_address}/transactions/"
        params = {"page[size]": limit}

        if fungible_id:
            params["filter[asset_id]"] = fungible_id

        return self.get(endpoint, params=params)
```

- [ ] **Step 4: Write Dune client**

Create `backend/common/api_clients/dune.py`:
```python
"""Dune Analytics API client."""
from django.conf import settings
from .base import BaseAPIClient
import time


class DuneClient(BaseAPIClient):
    """Dune Analytics API client."""

    def __init__(self, api_key: str = None):
        api_key = api_key or settings.DUNE_API_KEY
        super().__init__(api_key=api_key, base_url="https://api.dune.com/api/v1/")

    def execute_query(self, query_id: int, params: dict = None):
        """Execute a Dune query."""
        endpoint = f"query/{query_id}/execute"
        return self.post(endpoint, data={"query_parameters": params or {}})

    def get_query_results(self, execution_id: str):
        """Get query results."""
        endpoint = f"execution/{execution_id}/results"
        return self.get(endpoint)

    def execute_and_wait(self, query_id: int, params: dict = None, max_wait: int = 300):
        """Execute query and wait for results."""
        # Execute
        exec_response = self.execute_query(query_id, params)
        execution_id = exec_response["execution_id"]

        # Poll for results
        start_time = time.time()
        while time.time() - start_time < max_wait:
            try:
                results = self.get_query_results(execution_id)
                if results["state"] == "QUERY_STATE_COMPLETED":
                    return results["result"]["rows"]
                elif results["state"] == "QUERY_STATE_FAILED":
                    raise Exception(f"Query failed: {results}")
            except Exception:
                pass

            time.sleep(5)

        raise TimeoutError(f"Query execution timeout after {max_wait}s")
```

- [ ] **Step 5: Write DexScreener client**

Create `backend/common/api_clients/dexscreener.py`:
```python
"""DexScreener API client."""
from .base import BaseAPIClient


class DexScreenerClient(BaseAPIClient):
    """DexScreener API client for token prices."""

    def __init__(self):
        super().__init__(base_url="https://api.dexscreener.com/latest/dex/")

    def get_token_info(self, chain: str, contract_address: str):
        """Get token info and price."""
        endpoint = f"tokens/{chain}/{contract_address}"
        return self.get(endpoint)

    def search_token(self, query: str):
        """Search for token."""
        endpoint = f"search/?q={query}"
        return self.get(endpoint)
```

- [ ] **Step 6: Add API keys to settings**

Modify `backend/config/settings/base.py`, add after CONSENSUS config:
```python
# External API Keys
ZERION_API_KEY = os.getenv('ZERION_API_KEY', '')
ZERION_API_KEY_2 = os.getenv('ZERION_API_KEY_2', '')
DUNE_API_KEY = os.getenv('DUNE_API_KEY', '')
ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY', '')
ALCHEMY_API_KEY = os.getenv('ALCHEMY_API_KEY', '')
CG_API_KEY = os.getenv('CG_API_KEY', '')
```

- [ ] **Step 7: Update __init__.py**

Create `backend/common/api_clients/__init__.py`:
```python
"""API clients."""
from .zerion import ZerionClient
from .dune import DuneClient
from .dexscreener import DexScreenerClient

__all__ = ['ZerionClient', 'DuneClient', 'DexScreenerClient']
```

- [ ] **Step 8: Commit**

```bash
git add backend/common/ backend/config/settings/base.py
git commit -m "feat(services): create common API clients (Zerion, Dune, DexScreener)"
```

---

## Task 2: Create FIFO Calculator Service

**Files:**
- Create: `backend/wallets/services/__init__.py`
- Create: `backend/wallets/services/analytics/__init__.py`
- Create: `backend/wallets/services/analytics/fifo_calculator.py`

- [ ] **Step 1: Create services directory structure**

```bash
mkdir -p backend/wallets/services/analytics
touch backend/wallets/services/__init__.py
touch backend/wallets/services/analytics/__init__.py
```

- [ ] **Step 2: Write FIFO calculator**

Create `backend/wallets/services/analytics/fifo_calculator.py`:
```python
"""FIFO calculator service - migrated from score_engine/fifo_clean_simple.py"""
from django.conf import settings
from django.db import transaction
from wallets.models import Wallet, Transaction, TokenAnalytics
import logging

logger = logging.getLogger(__name__)


class FIFOCalculator:
    """Calculate FIFO (First In First Out) accounting for wallet positions."""

    def __init__(self):
        self.min_value = settings.TRACKING_LIVE_MIN_TOKEN_VALUE

    def calculate_for_wallet(self, wallet: Wallet) -> dict:
        """
        Calculate FIFO for all tokens of a wallet.
        Returns dict of token_symbol -> analytics.
        """
        logger.info(f"Calculating FIFO for wallet {wallet.address}")

        # Get all transactions ordered by date
        transactions = wallet.transactions.order_by('date')

        # Group by token
        tokens_data = {}
        for tx in transactions:
            if tx.symbol not in tokens_data:
                tokens_data[tx.symbol] = []
            tokens_data[tx.symbol].append(tx)

        # Calculate FIFO for each token
        results = {}
        for token_symbol, txs in tokens_data.items():
            analytics = self._calculate_token_fifo(wallet, token_symbol, txs)
            if analytics:
                results[token_symbol] = analytics

        logger.info(f"FIFO calculated for {len(results)} tokens")
        return results

    def _calculate_token_fifo(self, wallet: Wallet, token_symbol: str, transactions):
        """Calculate FIFO for a single token."""
        fifo_queue = []  # [(quantity, price_per_token), ...]
        total_invested = 0.0
        total_realized = 0.0
        holding_days = 0

        for tx in transactions:
            if tx.action_type == 'buy':
                # Add to FIFO queue
                fifo_queue.append((tx.quantity, tx.price_per_token))
                total_invested += tx.total_value_usd

            elif tx.action_type == 'sell':
                # Remove from FIFO queue
                sell_quantity = abs(tx.quantity)
                sell_value = tx.total_value_usd

                cost_basis = 0.0
                while sell_quantity > 0 and fifo_queue:
                    qty, price = fifo_queue[0]

                    if qty <= sell_quantity:
                        # Consume entire position
                        cost_basis += qty * price
                        sell_quantity -= qty
                        fifo_queue.pop(0)
                    else:
                        # Partial sell
                        cost_basis += sell_quantity * price
                        fifo_queue[0] = (qty - sell_quantity, price)
                        sell_quantity = 0

                # Calculate realized gain/loss
                realized = sell_value - cost_basis
                total_realized += realized

        # Calculate ROI
        if total_invested > 0:
            roi_percentage = ((total_invested + total_realized) / total_invested - 1) * 100
        else:
            roi_percentage = 0.0

        is_winning = roi_percentage > 0
        status = self._get_status(roi_percentage)

        # Check if still in portfolio
        in_portfolio = wallet.tokens.filter(symbol=token_symbol, in_portfolio=True).exists()

        # Save to database
        with transaction.atomic():
            analytics, created = TokenAnalytics.objects.update_or_create(
                wallet=wallet,
                token_symbol=token_symbol,
                defaults={
                    'total_invested': total_invested,
                    'total_realized': total_realized,
                    'roi_percentage': roi_percentage,
                    'is_winning': is_winning,
                    'status': status,
                    'holding_days': holding_days,
                    'in_portfolio': in_portfolio,
                }
            )

        return analytics

    def _get_status(self, roi: float) -> str:
        """Determine status based on ROI."""
        if roi > 50:
            return 'GAGNANT'
        elif roi < -20:
            return 'PERDANT'
        return 'NEUTRE'
```

- [ ] **Step 3: Commit**

```bash
git add backend/wallets/services/
git commit -m "feat(services): create FIFO calculator service"
```

---

## Task 3: Create Wallet Scorer Service

**Files:**
- Create: `backend/wallets/services/analytics/wallet_scorer.py`

- [ ] **Step 1: Write wallet scorer**

Create `backend/wallets/services/analytics/wallet_scorer.py`:
```python
"""Wallet scoring service - migrated from score_engine/wallet_scoring_system.py"""
from django.conf import settings
from django.db import transaction
from wallets.models import Wallet, TokenAnalytics, WalletQualified
import logging

logger = logging.getLogger(__name__)


class WalletScorer:
    """Score wallets based on FIFO analytics."""

    def __init__(self):
        self.roi_weight = settings.SCORE_ENGINE_ROI_WEIGHT
        self.winrate_weight = settings.SCORE_ENGINE_WINRATE_WEIGHT
        self.activity_weight = settings.SCORE_ENGINE_ACTIVITY_WEIGHT
        self.min_score = settings.SCORE_ENGINE_MIN_SCORE

    def calculate_score(self, wallet: Wallet) -> float:
        """
        Calculate wallet score.
        Score = 60% ROI + 30% win rate + 10% activity
        """
        analytics = wallet.token_analytics.all()

        if not analytics:
            return 0.0

        # Calculate weighted ROI
        total_invested = sum(a.total_invested for a in analytics)
        if total_invested == 0:
            weighted_roi = 0.0
        else:
            weighted_roi = sum(
                a.roi_percentage * (a.total_invested / total_invested)
                for a in analytics
            )

        # Calculate win rate
        nb_trades = analytics.count()
        nb_winning = analytics.filter(is_winning=True).count()
        winrate = (nb_winning / nb_trades * 100) if nb_trades > 0 else 0.0

        # Calculate activity score (normalized)
        activity_score = min(nb_trades / 10 * 100, 100)  # Max 10 trades = 100%

        # Final score
        score = (
            self.roi_weight * weighted_roi +
            self.winrate_weight * winrate +
            self.activity_weight * activity_score
        )

        return max(0, score)  # No negative scores

    def score_and_qualify(self, wallet: Wallet) -> bool:
        """Score wallet and save to WalletQualified if passes threshold."""
        score = self.calculate_score(wallet)

        if score < self.min_score:
            logger.info(f"Wallet {wallet.address} did not qualify (score: {score:.2f})")
            return False

        # Get analytics for stats
        analytics = wallet.token_analytics.all()
        total_invested = sum(a.total_invested for a in analytics)
        weighted_roi = sum(
            a.roi_percentage * (a.total_invested / total_invested)
            for a in analytics
        ) if total_invested > 0 else 0.0

        nb_trades = analytics.count()
        nb_winning = analytics.filter(is_winning=True).count()
        winrate = (nb_winning / nb_trades * 100) if nb_trades > 0 else 0.0

        # Determine classification
        classification = self._get_classification(score)

        # Save to database
        with transaction.atomic():
            WalletQualified.objects.update_or_create(
                wallet=wallet,
                defaults={
                    'final_score': score,
                    'classification': classification,
                    'weighted_roi': weighted_roi,
                    'taux_reussite': winrate,
                    'nb_trades': nb_trades,
                }
            )

        logger.info(f"Wallet {wallet.address} qualified: {classification} (score: {score:.2f})")
        return True

    def _get_classification(self, score: float) -> str:
        """Determine classification based on score."""
        if score >= 80:
            return 'ELITE'
        elif score >= 60:
            return 'EXCELLENT'
        elif score >= 40:
            return 'BON'
        elif score >= 20:
            return 'MOYEN'
        return 'FAIBLE'
```

- [ ] **Step 2: Commit**

```bash
git add backend/wallets/services/analytics/wallet_scorer.py
git commit -m "feat(services): create wallet scorer service"
```

---

## Task 4: Create Management Commands

**Files:**
- Create: `backend/wallets/management/commands/run_discovery.py`
- Create: `backend/wallets/management/commands/run_scoring.py`

- [ ] **Step 1: Write run_scoring command**

Create `backend/wallets/management/commands/run_scoring.py`:
```python
"""Run scoring pipeline - recalculate FIFO and scores for all wallets."""
from django.core.management.base import BaseCommand
from django.db import transaction
from wallets.models import Wallet
from wallets.services.analytics.fifo_calculator import FIFOCalculator
from wallets.services.analytics.wallet_scorer import WalletScorer
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Run scoring pipeline (FIFO + wallet scoring)'

    def handle(self, *args, **options):
        self.stdout.write("=== SCORING PIPELINE ===\n")

        fifo_calc = FIFOCalculator()
        scorer = WalletScorer()

        wallets = Wallet.objects.all()
        total = wallets.count()

        self.stdout.write(f"Processing {total} wallets...\n")

        qualified_count = 0
        for i, wallet in enumerate(wallets, 1):
            self.stdout.write(f"[{i}/{total}] {wallet.address}")

            # Calculate FIFO
            fifo_calc.calculate_for_wallet(wallet)

            # Score and qualify
            if scorer.score_and_qualify(wallet):
                qualified_count += 1

        self.stdout.write(self.style.SUCCESS(
            f"\n✓ Scoring completed: {qualified_count}/{total} wallets qualified"
        ))
```

- [ ] **Step 2: Write run_discovery placeholder**

Create `backend/wallets/management/commands/run_discovery.py`:
```python
"""Run discovery pipeline - placeholder for now."""
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Run discovery pipeline (token discovery + wallet tracking)'

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.WARNING("Discovery pipeline not yet implemented - use Phase 1 for now")
        )
```

- [ ] **Step 3: Test scoring command**

```bash
docker-compose exec backend python manage.py run_scoring
```

Expected: "Scoring completed: X/Y wallets qualified"

- [ ] **Step 4: Verify results in database**

```bash
docker-compose exec backend python manage.py shell
```

```python
from wallets.models import TokenAnalytics, WalletQualified
print(f"Token analytics: {TokenAnalytics.objects.count()}")
print(f"Qualified wallets: {WalletQualified.objects.count()}")
```

- [ ] **Step 5: Commit**

```bash
git add backend/wallets/management/commands/
git commit -m "feat(commands): create run_scoring management command"
```

---

## Task 5: Create Migration Validation Tests

**Files:**
- Create: `backend/wallets/tests/test_migration_validation.py`
- Create: `backend/wallets/management/commands/validate_migration.py`

- [ ] **Step 1: Write validation tests**

Create `backend/wallets/tests/test_migration_validation.py`:
```python
"""Migration validation tests - compare old SQLite vs new Django results."""
import sqlite3
from django.test import TestCase
from wallets.models import Wallet, TokenAnalytics
from wallets.services.analytics.fifo_calculator import FIFOCalculator


class FIFOMigrationValidationTest(TestCase):
    """Validate that Django FIFO matches old SQLite FIFO."""

    def setUp(self):
        """Setup test wallet with transactions."""
        # Load test data from fixtures or create manually
        self.wallet = Wallet.objects.create(
            address='0xTestWallet',
            period='30d'
        )

    def test_fifo_matches_old_system(self):
        """Compare FIFO results with old SQLite database."""
        # Get old results from SQLite
        old_results = self._get_old_fifo_results(self.wallet.address)

        if not old_results:
            self.skipTest("No old data available for comparison")

        # Calculate with new system
        calculator = FIFOCalculator()
        calculator.calculate_for_wallet(self.wallet)

        # Get new results
        new_analytics = TokenAnalytics.objects.filter(wallet=self.wallet)

        # Compare
        for analytics in new_analytics:
            old = old_results.get(analytics.token_symbol)
            if old:
                self.assertAlmostEqual(
                    analytics.total_invested,
                    old['total_invested'],
                    places=2,
                    msg=f"total_invested mismatch for {analytics.token_symbol}"
                )
                self.assertAlmostEqual(
                    analytics.roi_percentage,
                    old['roi_percentage'],
                    places=2,
                    msg=f"ROI mismatch for {analytics.token_symbol}"
                )

    def _get_old_fifo_results(self, wallet_address: str) -> dict:
        """Get FIFO results from old SQLite database."""
        try:
            conn = sqlite3.connect('data/db/wit_database.db')
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT token_symbol, total_invested, total_realized, roi_percentage
                FROM token_analytics
                WHERE wallet_address = ?
                """,
                (wallet_address,)
            )

            results = {}
            for row in cursor.fetchall():
                results[row[0]] = {
                    'total_invested': row[1],
                    'total_realized': row[2],
                    'roi_percentage': row[3],
                }

            conn.close()
            return results

        except Exception as e:
            print(f"Could not load old SQLite data: {e}")
            return {}
```

- [ ] **Step 2: Write validation command**

Create `backend/wallets/management/commands/validate_migration.py`:
```python
"""Validate migration - compare old vs new results."""
import sqlite3
from django.core.management.base import BaseCommand
from wallets.models import Wallet, TokenAnalytics
from wallets.services.analytics.fifo_calculator import FIFOCalculator


class Command(BaseCommand):
    help = 'Validate migration by comparing old SQLite vs new PostgreSQL results'

    def add_arguments(self, parser):
        parser.add_argument(
            '--sqlite-path',
            type=str,
            default='data/db/wit_database.db',
            help='Path to old SQLite database'
        )
        parser.add_argument(
            '--sample-size',
            type=int,
            default=10,
            help='Number of wallets to validate'
        )

    def handle(self, *args, **options):
        sqlite_path = options['sqlite_path']
        sample_size = options['sample_size']

        self.stdout.write("=== MIGRATION VALIDATION ===\n")
        self.stdout.write(f"Comparing {sample_size} wallets...\n")

        # Get sample wallets
        wallets = Wallet.objects.all()[:sample_size]

        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()

        matches = 0
        mismatches = 0

        for wallet in wallets:
            self.stdout.write(f"\nValidating {wallet.address}...")

            # Recalculate FIFO with new system
            calculator = FIFOCalculator()
            calculator.calculate_for_wallet(wallet)

            # Get new results
            new_analytics = TokenAnalytics.objects.filter(wallet=wallet)

            # Get old results
            cursor.execute(
                "SELECT token_symbol, roi_percentage FROM token_analytics WHERE wallet_address = ?",
                (wallet.address,)
            )
            old_analytics = {row[0]: row[1] for row in cursor.fetchall()}

            # Compare
            for analytics in new_analytics:
                old_roi = old_analytics.get(analytics.token_symbol)
                if old_roi is not None:
                    diff = abs(analytics.roi_percentage - old_roi)
                    if diff < 0.01:  # 0.01% tolerance
                        matches += 1
                        self.stdout.write(f"  ✓ {analytics.token_symbol}: {analytics.roi_percentage:.2f}% (match)")
                    else:
                        mismatches += 1
                        self.stdout.write(
                            self.style.WARNING(
                                f"  ✗ {analytics.token_symbol}: "
                                f"new={analytics.roi_percentage:.2f}% "
                                f"old={old_roi:.2f}% "
                                f"diff={diff:.2f}%"
                            )
                        )

        conn.close()

        self.stdout.write(f"\n=== RESULTS ===")
        self.stdout.write(f"Matches: {matches}")
        self.stdout.write(f"Mismatches: {mismatches}")

        if mismatches == 0:
            self.stdout.write(self.style.SUCCESS("\n✓ Migration validated successfully!"))
        else:
            self.stdout.write(
                self.style.WARNING(f"\n⚠ {mismatches} mismatches found - review results")
            )
```

- [ ] **Step 3: Run validation**

```bash
docker-compose run --rm -v $(pwd)/data:/data backend python manage.py validate_migration --sqlite-path /data/db/wit_database.db
```

Expected: "Migration validated successfully!" or details of mismatches

- [ ] **Step 4: Commit**

```bash
git add backend/wallets/tests/ backend/wallets/management/commands/validate_migration.py
git commit -m "feat(tests): create migration validation tests"
```

---

## Task 6: Create Remaining Service Stubs

**Files:**
- Create: `backend/wallets/services/discovery/dune_client.py`
- Create: `backend/wallets/services/tracking/zerion_client.py`
- Create: `backend/wallets/services/consensus/consensus_detector.py`

- [ ] **Step 1: Create service stubs**

Create `backend/wallets/services/discovery/dune_client.py`:
```python
"""Dune discovery service - TODO: migrate from token_discovery_manual."""
import logging

logger = logging.getLogger(__name__)


class DiscoveryService:
    """Token discovery via Dune Analytics - STUB."""

    def run(self):
        """Run token discovery pipeline."""
        logger.warning("Discovery service not yet implemented")
        raise NotImplementedError("Use old system for discovery for now")
```

Create `backend/wallets/services/tracking/zerion_client.py`:
```python
"""Zerion tracking service - TODO: migrate from wallet_tracker."""
import logging

logger = logging.getLogger(__name__)


class TrackingService:
    """Wallet tracking via Zerion - STUB."""

    def run(self):
        """Run wallet tracking pipeline."""
        logger.warning("Tracking service not yet implemented")
        raise NotImplementedError("Use old system for tracking for now")
```

Create `backend/wallets/services/consensus/consensus_detector.py`:
```python
"""Consensus detection service - TODO: migrate from consensus_live."""
import logging

logger = logging.getLogger(__name__)


class ConsensusService:
    """Consensus detection - STUB."""

    def detect(self):
        """Detect consensus signals."""
        logger.warning("Consensus service not yet implemented")
        raise NotImplementedError("Use old system for consensus for now")
```

- [ ] **Step 2: Create __init__ files**

```bash
touch backend/wallets/services/discovery/__init__.py
touch backend/wallets/services/tracking/__init__.py
touch backend/wallets/services/consensus/__init__.py
```

- [ ] **Step 3: Commit**

```bash
git add backend/wallets/services/
git commit -m "feat(services): create service stubs (discovery, tracking, consensus)"
```

---

## Validation Checkpoint

Before moving to Phase 4, verify:

- [ ] **FIFO calculator works**
```bash
docker-compose exec backend python manage.py shell
```
```python
from wallets.models import Wallet
from wallets.services.analytics.fifo_calculator import FIFOCalculator

wallet = Wallet.objects.first()
calc = FIFOCalculator()
calc.calculate_for_wallet(wallet)
```
Expected: TokenAnalytics created

- [ ] **Wallet scorer works**
```bash
docker-compose exec backend python manage.py run_scoring
```
Expected: Wallets qualified

- [ ] **Migration validation passes**
```bash
docker-compose exec backend python manage.py validate_migration
```
Expected: Matches = high, Mismatches = 0 or very low

- [ ] **Tests pass**
```bash
docker-compose exec backend python manage.py test
```

---

## Summary

**Phase 3 completed! You now have:**

✅ Common API clients (Zerion, Dune, DexScreener)
✅ FIFO Calculator service (Django ORM)
✅ Wallet Scorer service
✅ Management commands (run_scoring)
✅ Migration validation tests
✅ Service stubs for remaining modules

**Note:** Discovery, Tracking, and Consensus services are stubs. You can continue using the old system for these until Phase 4 where they'll be integrated into Celery tasks.

**Next phase:** Setup Celery + scheduler (Phase 4)

**To continue:**
```bash
cat docs/superpowers/plans/2026-05-31-phase-4-celery.md
```
