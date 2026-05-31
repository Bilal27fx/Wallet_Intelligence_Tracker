"""Wallet serializers."""
from rest_framework import serializers
from wallets.models import Wallet, Token, Transaction, WalletPositionChange


class TokenSerializer(serializers.ModelSerializer):
    """Token position serializer."""

    class Meta:
        model = Token
        fields = [
            'id', 'fungible_id', 'symbol', 'contract_address', 'chain',
            'amount', 'usd_value', 'in_portfolio'
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
    """Wallet position change serializer."""

    class Meta:
        model = WalletPositionChange
        fields = [
            'id', 'session_id', 'symbol', 'fungible_id',
            'contract_address', 'change_type', 'old_amount',
            'new_amount', 'usd_change', 'detected_at'
        ]


class WalletListSerializer(serializers.ModelSerializer):
    """Wallet list serializer (lightweight)."""
    tokens_count = serializers.IntegerField(source='tokens.count', read_only=True)
    transactions_count = serializers.IntegerField(source='transactions.count', read_only=True)

    class Meta:
        model = Wallet
        fields = [
            'address', 'period', 'total_portfolio_value',
            'is_smart_wallet', 'tokens_count', 'transactions_count',
            'created_at', 'updated_at'
        ]


class WalletDetailSerializer(serializers.ModelSerializer):
    """Wallet detail serializer (with nested data)."""
    tokens = TokenSerializer(many=True, read_only=True)
    recent_transactions = serializers.SerializerMethodField()
    position_changes = WalletPositionChangeSerializer(many=True, read_only=True)

    class Meta:
        model = Wallet
        fields = [
            'address', 'period', 'total_portfolio_value',
            'is_smart_wallet', 'tokens', 'recent_transactions',
            'position_changes', 'created_at', 'updated_at'
        ]

    def get_recent_transactions(self, obj):
        """Get 10 most recent transactions."""
        recent = obj.transactions.all()[:10]
        return TransactionSerializer(recent, many=True).data
