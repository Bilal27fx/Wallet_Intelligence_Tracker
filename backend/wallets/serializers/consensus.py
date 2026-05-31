"""Consensus serializers."""
from rest_framework import serializers
from wallets.models import ConsensusSignal


class ConsensusSignalSerializer(serializers.ModelSerializer):
    """Consensus signal serializer."""
    wallet_addresses = serializers.SerializerMethodField()

    class Meta:
        model = ConsensusSignal
        fields = [
            'id', 'token_symbol', 'contract_address', 'chain',
            'nb_wallets', 'total_usd_invested', 'market_cap',
            'detected_at', 'wallet_addresses'
        ]

    def get_wallet_addresses(self, obj):
        """Get list of wallet addresses in this consensus."""
        return list(obj.wallets.values_list('address', flat=True))
