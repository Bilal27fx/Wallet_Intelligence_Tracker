"""Consensus views."""
from rest_framework import viewsets, filters
from wallets.models import ConsensusSignal
from wallets.serializers import ConsensusSignalSerializer


class ConsensusSignalViewSet(viewsets.ReadOnlyModelViewSet):
    """Consensus Signal ViewSet - Smart money consensus signals."""
    queryset = ConsensusSignal.objects.all().prefetch_related('wallets')
    serializer_class = ConsensusSignalSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['token_symbol', 'contract_address', 'chain']
    ordering_fields = ['nb_wallets', 'total_usd_invested', 'market_cap', 'detected_at']
    ordering = ['-detected_at']
