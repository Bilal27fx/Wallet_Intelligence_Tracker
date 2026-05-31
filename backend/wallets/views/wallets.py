"""Wallet views."""
from rest_framework import viewsets, filters
from rest_framework.decorators import action
from rest_framework.response import Response
from wallets.models import Wallet, Token, Transaction, WalletPositionChange
from wallets.serializers import (
    WalletListSerializer,
    WalletDetailSerializer,
    TokenSerializer,
    TransactionSerializer,
    WalletPositionChangeSerializer,
)


class WalletViewSet(viewsets.ReadOnlyModelViewSet):
    """Wallet ViewSet - Read-only access to wallets."""
    queryset = Wallet.objects.all().prefetch_related('tokens', 'transactions')
    lookup_field = 'address'
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['address']
    ordering_fields = ['total_portfolio_value', 'created_at', 'updated_at']
    ordering = ['-total_portfolio_value']

    def get_serializer_class(self):
        """Use detail serializer for retrieve, list serializer for list."""
        if self.action == 'retrieve':
            return WalletDetailSerializer
        return WalletListSerializer

    @action(detail=True, methods=['get'])
    def tokens(self, request, address=None):
        """Get all tokens for a specific wallet."""
        wallet = self.get_object()
        tokens = wallet.tokens.filter(in_portfolio=True).order_by('-usd_value')
        serializer = TokenSerializer(tokens, many=True)
        return Response(serializer.data)

    @action(detail=True, methods=['get'])
    def transactions(self, request, address=None):
        """Get all transactions for a specific wallet."""
        wallet = self.get_object()
        transactions = wallet.transactions.all()[:100]  # Limit to 100 recent
        serializer = TransactionSerializer(transactions, many=True)
        return Response(serializer.data)


class TokenViewSet(viewsets.ReadOnlyModelViewSet):
    """Token ViewSet - Read-only access to tokens."""
    queryset = Token.objects.all().select_related('wallet')
    serializer_class = TokenSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['symbol', 'contract_address', 'fungible_id']
    ordering_fields = ['usd_value', 'amount']
    ordering = ['-usd_value']


class TransactionViewSet(viewsets.ReadOnlyModelViewSet):
    """Transaction ViewSet - Read-only access to transactions."""
    queryset = Transaction.objects.all().select_related('wallet')
    serializer_class = TransactionSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['symbol', 'hash', 'wallet__address']
    ordering_fields = ['date', 'total_value_usd']
    ordering = ['-date']


class WalletPositionChangeViewSet(viewsets.ReadOnlyModelViewSet):
    """Wallet Position Change ViewSet."""
    queryset = WalletPositionChange.objects.all().select_related('wallet')
    serializer_class = WalletPositionChangeSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['symbol', 'wallet__address', 'session_id']
    ordering_fields = ['detected_at', 'usd_change']
    ordering = ['-detected_at']
