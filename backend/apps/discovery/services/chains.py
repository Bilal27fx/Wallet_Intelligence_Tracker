"""Synchronisation des chaînes : GeckoTerminal → CoinGecko (chain id) → HyperSync, Zerion."""

from apps.discovery.models import Chain


def sync_chains(gt, coingecko, directory, zerion) -> int:
    platform_ids = coingecko.platform_chain_ids()
    hypersync_ids = directory.supported_chain_ids()
    zerion_ids = zerion.chain_ids()
    count = 0
    for network in gt.networks():
        evm_id = (
            platform_ids.get(network.coingecko_platform_id)
            if network.coingecko_platform_id
            else None
        )
        Chain.objects.update_or_create(
            gt_id=network.gt_id,
            defaults={
                "name": network.name,
                "evm_id": evm_id,
                "zerion_id": zerion_ids.get(evm_id, "") if evm_id else "",
                "hypersync_supported": evm_id in hypersync_ids if evm_id else False,
            },
        )
        count += 1
    return count
