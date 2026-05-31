# Phase 2: API REST - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create complete REST API with Django REST Framework for frontend Next.js consumption.

**Architecture:** DRF ViewSets with serializers for wallets, analytics, and consensus. Read-only API with pagination, filtering, and comprehensive endpoints.

**Tech Stack:** Django REST Framework 3.14, drf-spectacular (OpenAPI docs)

---

## Prerequisites

✅ Phase 1 completed (Django + PostgreSQL + data migrated)
✅ Docker containers running
✅ Django Admin accessible

---

## File Structure

**New files to create:**
```
backend/
├── api/
│   ├── __init__.py
│   └── urls.py
├── wallets/
│   ├── serializers/
│   │   ├── __init__.py
│   │   ├── wallet.py
│   │   ├── analytics.py
│   │   └── consensus.py
│   ├── views/
│   │   ├── __init__.py
│   │   ├── wallets.py
│   │   ├── analytics.py
│   │   └── consensus.py
│   ├── urls.py
│   └── tests/
│       ├── __init__.py
│       └── test_api.py
```

---

## Task 1: Setup DRF and API Router

**Files:**
- Create: `backend/api/__init__.py`
- Create: `backend/api/urls.py`
- Modify: `backend/config/settings/base.py`
- Modify: `backend/config/urls.py`

- [ ] **Step 1: Create API directory**

```bash
mkdir -p backend/api
touch backend/api/__init__.py
```

- [ ] **Step 2: Install drf-spectacular**

Add to `backend/requirements/base.txt`:
```
drf-spectacular==0.27.0
```

Rebuild:
```bash
docker-compose build backend
```

- [ ] **Step 3: Update Django settings**

Modify `backend/config/settings/base.py`, add to INSTALLED_APPS:
```python
INSTALLED_APPS = [
    # ... existing apps ...
    'rest_framework',
    'drf_spectacular',  # Add this
    # ... WIT apps ...
]
```

Add REST Framework config (replace existing):
```python
# REST Framework
REST_FRAMEWORK = {
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.PageNumberPagination',
    'PAGE_SIZE': 50,
    'DEFAULT_SCHEMA_CLASS': 'drf_spectacular.openapi.AutoSchema',
    'DEFAULT_FILTER_BACKENDS': [
        'rest_framework.filters.SearchFilter',
        'rest_framework.filters.OrderingFilter',
    ],
}

# API Documentation
SPECTACULAR_SETTINGS = {
    'TITLE': 'WIT V1 API',
    'DESCRIPTION': 'Wallet Intelligence Toolkit API',
    'VERSION': '1.0.0',
}
```

- [ ] **Step 4: Create API router**

Create `backend/api/urls.py`:
```python
"""API URL router."""
from django.urls import path, include
from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView

urlpatterns = [
    # API Documentation
    path('schema/', SpectacularAPIView.as_view(), name='schema'),
    path('docs/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),

    # App endpoints
    path('v1/', include('wallets.urls')),
]
```

- [ ] **Step 5: Update main urls.py**

Modify `backend/config/urls.py`:
```python
"""URL configuration for WIT V1."""
from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', include('api.urls')),
]
```

- [ ] **Step 6: Verify API docs accessible**

```bash
docker-compose restart backend
```

Open browser: http://localhost:8000/api/docs/

Expected: Swagger UI visible (empty for now)

- [ ] **Step 7: Commit**

```bash
git add backend/api/ backend/config/settings/base.py backend/config/urls.py backend/requirements/
git commit -m "feat(api): setup DRF and API router with docs"
```

---

## Task 2: Create Wallet Serializers

**Files:**
- Create: `backend/wallets/serializers/__init__.py`
- Create: `backend/wallets/serializers/wallet.py`

- [ ] **Step 1: Create serializers directory**

```bash
mkdir -p backend/wallets/serializers
touch backend/wallets/serializers/__init__.py
```

- [ ] **Step 2: Write wallet serializers**

Create `backend/wallets/serializers/wallet.py`:
```python
"""Wallet serializers."""
from rest_framework import serializers
from wallets.models import Wallet, Token, Transaction, WalletPositionChange


class TokenSerializer(serializers.ModelSerializer):
    """Token position serializer."""

    class Meta:
        model = Token
        fields = [
            'id', 'fungible_id', 'symbol', 'contract_address',
            'chain', 'amount', 'usd_value', 'in_portfolio'
        ]


class TransactionSerializer(serializers.ModelSerializer):
    """Transaction serializer."""

    class Meta:
        model = Transaction
        fields = [
            'id', 'fungible_id', 'symbol', 'hash', 'date',
            'operation_type', 'action_type', 'swap_description',
            'contract_address', 'quantity', 'price_per_token',
            'total_value_usd', 'direction', 'recipient_address',
            'sender_address'
        ]


class WalletPositionChangeSerializer(serializers.ModelSerializer):
    """Position change serializer."""

    class Meta:
        model = WalletPositionChange
        fields = [
            'id', 'session_id', 'symbol', 'fungible_id',
            'contract_address', 'change_type', 'old_amount',
            'new_amount', 'usd_change', 'detected_at'
        ]


class WalletListSerializer(serializers.ModelSerializer):
    """Wallet list serializer (lightweight)."""

    class Meta:
        model = Wallet
        fields = [
            'address', 'period', 'total_portfolio_value',
            'is_smart_wallet', 'updated_at'
        ]


class WalletDetailSerializer(serializers.ModelSerializer):
    """Wallet detail serializer (with relations)."""
    tokens = TokenSerializer(many=True, read_only=True)

    class Meta:
        model = Wallet
        fields = [
            'address', 'period', 'total_portfolio_value',
            'is_smart_wallet', 'created_at', 'updated_at',
            'tokens'
        ]
```

- [ ] **Step 3: Update serializers __init__.py**

Create `backend/wallets/serializers/__init__.py`:
```python
"""Wallet serializers."""
from .wallet import (
    WalletListSerializer,
    WalletDetailSerializer,
    TokenSerializer,
    TransactionSerializer,
    WalletPositionChangeSerializer,
)

__all__ = [
    'WalletListSerializer',
    'WalletDetailSerializer',
    'TokenSerializer',
    'TransactionSerializer',
    'WalletPositionChangeSerializer',
]
```

- [ ] **Step 4: Commit**

```bash
git add backend/wallets/serializers/
git commit -m "feat(api): create wallet serializers"
```

---

## Task 3: Create Analytics Serializers

**Files:**
- Create: `backend/wallets/serializers/analytics.py`

- [ ] **Step 1: Write analytics serializers**

Create `backend/wallets/serializers/analytics.py`:
```python
"""Analytics serializers."""
from rest_framework import serializers
from wallets.models import (
    TokenAnalytics, WalletTierPerformance,
    WalletQualified, SmartWallet
)


class TokenAnalyticsSerializer(serializers.ModelSerializer):
    """Token analytics (FIFO results) serializer."""
    wallet_address = serializers.CharField(source='wallet.address', read_only=True)

    class Meta:
        model = TokenAnalytics
        fields = [
            'id', 'wallet_address', 'token_symbol',
            'total_invested', 'total_realized', 'roi_percentage',
            'is_winning', 'status', 'holding_days', 'in_portfolio'
        ]


class WalletTierPerformanceSerializer(serializers.ModelSerializer):
    """Wallet tier performance serializer."""
    wallet_address = serializers.CharField(source='wallet.address', read_only=True)

    class Meta:
        model = WalletTierPerformance
        fields = [
            'id', 'wallet_address', 'tier_usd', 'roi_percentage',
            'winrate', 'nb_trades', 'nb_gagnants', 'is_optimal_tier'
        ]


class WalletQualifiedSerializer(serializers.ModelSerializer):
    """Wallet qualified serializer."""
    wallet_address = serializers.CharField(source='wallet.address', read_only=True)

    class Meta:
        model = WalletQualified
        fields = [
            'wallet_address', 'final_score', 'classification',
            'weighted_roi', 'taux_reussite', 'nb_trades'
        ]


class SmartWalletSerializer(serializers.ModelSerializer):
    """Smart wallet serializer."""
    wallet_address = serializers.CharField(source='wallet.address', read_only=True)

    class Meta:
        model = SmartWallet
        fields = [
            'wallet_address', 'optimal_threshold_tier',
            'quality_score', 'threshold_status',
            'optimal_roi', 'optimal_winrate',
            'global_roi', 'global_winrate'
        ]


class WalletAnalyticsDetailSerializer(serializers.Serializer):
    """Combined wallet analytics (for /wallets/{address}/analytics/)."""
    token_analytics = TokenAnalyticsSerializer(many=True)
    tier_performance = WalletTierPerformanceSerializer(many=True)
    qualification = WalletQualifiedSerializer(allow_null=True)
    smart_wallet_data = SmartWalletSerializer(allow_null=True)
```

- [ ] **Step 2: Update serializers __init__.py**

Modify `backend/wallets/serializers/__init__.py`, add:
```python
from .analytics import (
    TokenAnalyticsSerializer,
    WalletTierPerformanceSerializer,
    WalletQualifiedSerializer,
    SmartWalletSerializer,
    WalletAnalyticsDetailSerializer,
)

__all__ = [
    # ... existing ...
    'TokenAnalyticsSerializer',
    'WalletTierPerformanceSerializer',
    'WalletQualifiedSerializer',
    'SmartWalletSerializer',
    'WalletAnalyticsDetailSerializer',
]
```

- [ ] **Step 3: Commit**

```bash
git add backend/wallets/serializers/
git commit -m "feat(api): create analytics serializers"
```

---

## Task 4: Create Consensus Serializers

**Files:**
- Create: `backend/wallets/serializers/consensus.py`

- [ ] **Step 1: Write consensus serializers**

Create `backend/wallets/serializers/consensus.py`:
```python
"""Consensus serializers."""
from rest_framework import serializers
from wallets.models import ConsensusSignal


class ConsensusSignalListSerializer(serializers.ModelSerializer):
    """Consensus signal list serializer."""
    nb_wallets = serializers.IntegerField()

    class Meta:
        model = ConsensusSignal
        fields = [
            'id', 'token_symbol', 'contract_address', 'chain',
            'nb_wallets', 'total_usd_invested', 'market_cap',
            'detected_at'
        ]


class ConsensusSignalDetailSerializer(serializers.ModelSerializer):
    """Consensus signal detail serializer (with wallets)."""
    wallet_addresses = serializers.SerializerMethodField()

    class Meta:
        model = ConsensusSignal
        fields = [
            'id', 'token_symbol', 'contract_address', 'chain',
            'nb_wallets', 'total_usd_invested', 'market_cap',
            'detected_at', 'wallet_addresses'
        ]

    def get_wallet_addresses(self, obj):
        """Return list of wallet addresses in this consensus."""
        return list(obj.wallets.values_list('address', flat=True))
```

- [ ] **Step 2: Update serializers __init__.py**

Modify `backend/wallets/serializers/__init__.py`, add:
```python
from .consensus import (
    ConsensusSignalListSerializer,
    ConsensusSignalDetailSerializer,
)

__all__ = [
    # ... existing ...
    'ConsensusSignalListSerializer',
    'ConsensusSignalDetailSerializer',
]
```

- [ ] **Step 3: Commit**

```bash
git add backend/wallets/serializers/
git commit -m "feat(api): create consensus serializers"
```

---

## Task 5: Create Wallet ViewSets

**Files:**
- Create: `backend/wallets/views/__init__.py`
- Create: `backend/wallets/views/wallets.py`

- [ ] **Step 1: Create views directory**

```bash
mkdir -p backend/wallets/views
touch backend/wallets/views/__init__.py
```

- [ ] **Step 2: Write wallet viewsets**

Create `backend/wallets/views/wallets.py`:
```python
"""Wallet viewsets."""
from rest_framework import viewsets, filters
from rest_framework.decorators import action
from rest_framework.response import Response
from wallets.models import Wallet, Transaction
from wallets.serializers import (
    WalletListSerializer,
    WalletDetailSerializer,
    TransactionSerializer,
    TokenSerializer,
    WalletAnalyticsDetailSerializer,
)


class WalletViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Wallet API.

    list: Get all wallets
    retrieve: Get wallet details
    positions: Get current token positions
    transactions: Get transaction history
    analytics: Get analytics (FIFO, ROI, scoring)
    """
    queryset = Wallet.objects.all()
    lookup_field = 'address'
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['address']
    ordering_fields = ['total_portfolio_value', 'updated_at']
    ordering = ['-total_portfolio_value']

    def get_serializer_class(self):
        if self.action == 'list':
            return WalletListSerializer
        return WalletDetailSerializer

    def get_queryset(self):
        """Filter smart wallets if requested."""
        queryset = super().get_queryset()
        smart_only = self.request.query_params.get('smart_only', None)

        if smart_only == 'true':
            queryset = queryset.filter(is_smart_wallet=True)

        return queryset

    @action(detail=True, methods=['get'])
    def positions(self, request, address=None):
        """Get current token positions."""
        wallet = self.get_object()
        tokens = wallet.tokens.filter(in_portfolio=True)
        serializer = TokenSerializer(tokens, many=True)
        return Response(serializer.data)

    @action(detail=True, methods=['get'])
    def transactions(self, request, address=None):
        """Get transaction history."""
        wallet = self.get_object()
        transactions = wallet.transactions.all()

        # Pagination
        page = self.paginate_queryset(transactions)
        if page is not None:
            serializer = TransactionSerializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = TransactionSerializer(transactions, many=True)
        return Response(serializer.data)

    @action(detail=True, methods=['get'])
    def analytics(self, request, address=None):
        """Get analytics (FIFO, ROI, scoring)."""
        wallet = self.get_object()

        data = {
            'token_analytics': wallet.token_analytics.all(),
            'tier_performance': wallet.tier_performance.all(),
            'qualification': getattr(wallet, 'qualification', None),
            'smart_wallet_data': getattr(wallet, 'smart_wallet_data', None),
        }

        serializer = WalletAnalyticsDetailSerializer(data)
        return Response(serializer.data)
```

- [ ] **Step 3: Update views __init__.py**

Create `backend/wallets/views/__init__.py`:
```python
"""Wallet views."""
from .wallets import WalletViewSet

__all__ = ['WalletViewSet']
```

- [ ] **Step 4: Commit**

```bash
git add backend/wallets/views/
git commit -m "feat(api): create wallet viewsets"
```

---

## Task 6: Create Analytics and Consensus ViewSets

**Files:**
- Create: `backend/wallets/views/analytics.py`
- Create: `backend/wallets/views/consensus.py`

- [ ] **Step 1: Write analytics viewsets**

Create `backend/wallets/views/analytics.py`:
```python
"""Analytics viewsets."""
from rest_framework import viewsets, filters
from rest_framework.decorators import action
from rest_framework.response import Response
from wallets.models import SmartWallet, WalletQualified
from wallets.serializers import SmartWalletSerializer, WalletQualifiedSerializer


class AnalyticsViewSet(viewsets.ViewSet):
    """
    Analytics API.

    performance: Get global performance stats
    tiers: Get tier analysis summary
    """

    @action(detail=False, methods=['get'])
    def performance(self, request):
        """Get global performance statistics."""
        qualified = WalletQualified.objects.all()

        stats = {
            'total_qualified': qualified.count(),
            'avg_score': qualified.aggregate(avg=models.Avg('final_score'))['avg'],
            'avg_roi': qualified.aggregate(avg=models.Avg('weighted_roi'))['avg'],
            'avg_winrate': qualified.aggregate(avg=models.Avg('taux_reussite'))['avg'],
            'classifications': {
                'ELITE': qualified.filter(classification='ELITE').count(),
                'EXCELLENT': qualified.filter(classification='EXCELLENT').count(),
                'BON': qualified.filter(classification='BON').count(),
                'MOYEN': qualified.filter(classification='MOYEN').count(),
                'FAIBLE': qualified.filter(classification='FAIBLE').count(),
            }
        }

        return Response(stats)

    @action(detail=False, methods=['get'])
    def tiers(self, request):
        """Get tier analysis summary."""
        from django.db.models import Avg, Count

        tiers = SmartWallet.objects.values('optimal_threshold_tier').annotate(
            count=Count('wallet'),
            avg_roi=Avg('optimal_roi'),
            avg_winrate=Avg('optimal_winrate'),
        ).order_by('optimal_threshold_tier')

        return Response(list(tiers))
```

- [ ] **Step 2: Write consensus viewsets**

Create `backend/wallets/views/consensus.py`:
```python
"""Consensus viewsets."""
from rest_framework import viewsets, filters
from rest_framework.decorators import action
from rest_framework.response import Response
from wallets.models import ConsensusSignal
from wallets.serializers import ConsensusSignalListSerializer, ConsensusSignalDetailSerializer


class ConsensusViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Consensus signals API.

    list: Get all consensus signals
    retrieve: Get consensus signal detail
    recent: Get recent signals (last 24h)
    """
    queryset = ConsensusSignal.objects.all()
    filter_backends = [filters.OrderingFilter]
    ordering_fields = ['detected_at', 'nb_wallets', 'total_usd_invested']
    ordering = ['-detected_at']

    def get_serializer_class(self):
        if self.action == 'retrieve':
            return ConsensusSignalDetailSerializer
        return ConsensusSignalListSerializer

    @action(detail=False, methods=['get'])
    def recent(self, request):
        """Get consensus signals from last 24 hours."""
        from django.utils import timezone
        from datetime import timedelta

        cutoff = timezone.now() - timedelta(hours=24)
        signals = self.queryset.filter(detected_at__gte=cutoff)

        serializer = self.get_serializer(signals, many=True)
        return Response(serializer.data)
```

- [ ] **Step 3: Update views __init__.py**

Modify `backend/wallets/views/__init__.py`:
```python
"""Wallet views."""
from .wallets import WalletViewSet
from .analytics import AnalyticsViewSet
from .consensus import ConsensusViewSet

__all__ = ['WalletViewSet', 'AnalyticsViewSet', 'ConsensusViewSet']
```

- [ ] **Step 4: Add missing import in analytics.py**

Modify `backend/wallets/views/analytics.py`, add at top:
```python
from django.db import models
```

- [ ] **Step 5: Commit**

```bash
git add backend/wallets/views/
git commit -m "feat(api): create analytics and consensus viewsets"
```

---

## Task 7: Create URL Routes

**Files:**
- Create: `backend/wallets/urls.py`

- [ ] **Step 1: Write wallets URLs**

Create `backend/wallets/urls.py`:
```python
"""Wallets API URLs."""
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from wallets.views import WalletViewSet, AnalyticsViewSet, ConsensusViewSet

router = DefaultRouter()
router.register(r'wallets', WalletViewSet, basename='wallet')
router.register(r'analytics', AnalyticsViewSet, basename='analytics')
router.register(r'consensus', ConsensusViewSet, basename='consensus')

urlpatterns = [
    path('', include(router.urls)),
]
```

- [ ] **Step 2: Test API endpoints**

```bash
docker-compose restart backend
```

Test endpoints:
```bash
# List wallets
curl http://localhost:8000/api/v1/wallets/ | jq

# Get wallet detail
curl http://localhost:8000/api/v1/wallets/{address}/ | jq

# Get wallet positions
curl http://localhost:8000/api/v1/wallets/{address}/positions/ | jq

# Get wallet transactions
curl http://localhost:8000/api/v1/wallets/{address}/transactions/ | jq

# Get wallet analytics
curl http://localhost:8000/api/v1/wallets/{address}/analytics/ | jq

# Get consensus signals
curl http://localhost:8000/api/v1/consensus/ | jq

# Get analytics performance
curl http://localhost:8000/api/v1/analytics/performance/ | jq
```

Expected: JSON responses with data

- [ ] **Step 3: Check API docs**

Open: http://localhost:8000/api/docs/

Expected: All endpoints visible in Swagger UI

- [ ] **Step 4: Commit**

```bash
git add backend/wallets/urls.py
git commit -m "feat(api): create API URL routes"
```

---

## Task 8: Create API Tests

**Files:**
- Create: `backend/wallets/tests/__init__.py`
- Create: `backend/wallets/tests/test_api.py`

- [ ] **Step 1: Create tests directory**

```bash
mkdir -p backend/wallets/tests
touch backend/wallets/tests/__init__.py
```

- [ ] **Step 2: Write API tests**

Create `backend/wallets/tests/test_api.py`:
```python
"""API tests."""
from django.test import TestCase
from rest_framework.test import APIClient
from wallets.models import Wallet, Token, SmartWallet, ConsensusSignal


class WalletAPITest(TestCase):
    """Test wallet API endpoints."""

    def setUp(self):
        self.client = APIClient()

        # Create test wallet
        self.wallet = Wallet.objects.create(
            address='0xTestWallet123',
            period='30d',
            total_portfolio_value=10000.0,
            is_smart_wallet=True
        )

        # Create test token
        Token.objects.create(
            wallet=self.wallet,
            fungible_id='eth',
            symbol='ETH',
            contract_address='0xEth',
            chain='ethereum',
            amount=5.0,
            usd_value=10000.0,
            in_portfolio=True
        )

    def test_wallet_list(self):
        """Test GET /api/v1/wallets/"""
        response = self.client.get('/api/v1/wallets/')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data['results']), 1)

    def test_wallet_detail(self):
        """Test GET /api/v1/wallets/{address}/"""
        response = self.client.get(f'/api/v1/wallets/{self.wallet.address}/')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['address'], self.wallet.address)

    def test_wallet_positions(self):
        """Test GET /api/v1/wallets/{address}/positions/"""
        response = self.client.get(f'/api/v1/wallets/{self.wallet.address}/positions/')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['symbol'], 'ETH')

    def test_wallet_filter_smart_only(self):
        """Test GET /api/v1/wallets/?smart_only=true"""
        # Create non-smart wallet
        Wallet.objects.create(
            address='0xNonSmart',
            period='30d',
            is_smart_wallet=False
        )

        response = self.client.get('/api/v1/wallets/?smart_only=true')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['is_smart_wallet'], True)


class ConsensusAPITest(TestCase):
    """Test consensus API endpoints."""

    def setUp(self):
        self.client = APIClient()

        # Create test consensus
        self.consensus = ConsensusSignal.objects.create(
            token_symbol='TEST',
            contract_address='0xTest',
            chain='ethereum',
            nb_wallets=3,
            total_usd_invested=50000.0,
            market_cap=1000000.0
        )

    def test_consensus_list(self):
        """Test GET /api/v1/consensus/"""
        response = self.client.get('/api/v1/consensus/')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data['results']), 1)

    def test_consensus_detail(self):
        """Test GET /api/v1/consensus/{id}/"""
        response = self.client.get(f'/api/v1/consensus/{self.consensus.id}/')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['token_symbol'], 'TEST')
```

- [ ] **Step 3: Run tests**

```bash
docker-compose exec backend python manage.py test wallets.tests.test_api
```

Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add backend/wallets/tests/
git commit -m "feat(api): create API tests"
```

---

## Task 9: Connect Frontend to API

**Files:**
- Modify: `frontend/lib/api.ts` (create if doesn't exist)
- Modify: `frontend/app/dashboard/page.tsx`

- [ ] **Step 1: Create API client**

Create `frontend/lib/api.ts`:
```typescript
const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000/api/v1';

export async function fetchWallets(smartOnly: boolean = true) {
  const url = `${API_BASE_URL}/wallets/${smartOnly ? '?smart_only=true' : ''}`;
  const response = await fetch(url);
  if (!response.ok) throw new Error('Failed to fetch wallets');
  return response.json();
}

export async function fetchWalletDetail(address: string) {
  const response = await fetch(`${API_BASE_URL}/wallets/${address}/`);
  if (!response.ok) throw new Error('Failed to fetch wallet detail');
  return response.json();
}

export async function fetchWalletAnalytics(address: string) {
  const response = await fetch(`${API_BASE_URL}/wallets/${address}/analytics/`);
  if (!response.ok) throw new Error('Failed to fetch analytics');
  return response.json();
}

export async function fetchConsensusSignals() {
  const response = await fetch(`${API_BASE_URL}/consensus/`);
  if (!response.ok) throw new Error('Failed to fetch consensus');
  return response.json();
}
```

- [ ] **Step 2: Update dashboard to use real API**

Modify `frontend/app/dashboard/page.tsx`, replace mock data with:
```typescript
'use client';

import { useState, useEffect } from 'react';
import { fetchConsensusSignals } from '@/lib/api';

export default function DashboardPage() {
  const [consensusData, setConsensusData] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function loadData() {
      try {
        const data = await fetchConsensusSignals();
        setConsensusData(data.results || []);
      } catch (error) {
        console.error('Failed to load consensus:', error);
      } finally {
        setLoading(false);
      }
    }
    loadData();
  }, []);

  if (loading) return <div>Loading...</div>;

  return (
    <div className="mx-auto max-w-7xl px-6 py-5">
      {/* Render consensus data */}
      {/* ... rest of component ... */}
    </div>
  );
}
```

- [ ] **Step 3: Add CORS to Django**

Modify `backend/requirements/base.txt`, add:
```
django-cors-headers==4.3.1
```

Modify `backend/config/settings/base.py`, add to INSTALLED_APPS:
```python
'corsheaders',
```

Add to MIDDLEWARE (at the top):
```python
MIDDLEWARE = [
    'corsheaders.middleware.CorsMiddleware',  # Add this
    'django.middleware.security.SecurityMiddleware',
    # ... rest ...
]
```

Add CORS config:
```python
# CORS
CORS_ALLOWED_ORIGINS = [
    'http://localhost:3000',  # Next.js dev
]
```

- [ ] **Step 4: Rebuild and test**

```bash
docker-compose build backend
docker-compose restart backend
cd frontend && npm run dev
```

Open: http://localhost:3000

Expected: Dashboard shows real data from Django API

- [ ] **Step 5: Commit**

```bash
git add frontend/lib/api.ts frontend/app/dashboard/page.tsx backend/config/settings/base.py backend/requirements/
git commit -m "feat(frontend): connect Next.js to Django API"
```

---

## Validation Checkpoint

Before moving to Phase 3, verify:

- [ ] **API accessible**
```bash
curl http://localhost:8000/api/v1/wallets/ | jq
```
Expected: JSON with wallets

- [ ] **All endpoints working**
```bash
curl http://localhost:8000/api/v1/wallets/{address}/
curl http://localhost:8000/api/v1/wallets/{address}/positions/
curl http://localhost:8000/api/v1/wallets/{address}/analytics/
curl http://localhost:8000/api/v1/consensus/
```

- [ ] **API docs accessible**
Open: http://localhost:8000/api/docs/
Expected: Swagger UI with all endpoints

- [ ] **Tests pass**
```bash
docker-compose exec backend python manage.py test
```
Expected: All tests pass

- [ ] **Frontend connected**
Open: http://localhost:3000
Expected: Real data from API visible

---

## Summary

**Phase 2 completed! You now have:**

✅ Complete REST API with DRF
✅ Serializers for all models
✅ ViewSets with custom actions
✅ API documentation (Swagger)
✅ API tests passing
✅ Frontend connected to API

**Next phase:** Migrate business logic to Django services (Phase 3)

**To continue:**
```bash
cat docs/superpowers/plans/2026-05-31-phase-3-business-logic.md
```
