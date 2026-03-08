"use client";

import { useState, useCallback } from "react";
import { ConsensusCard } from "./components/consensus-card";
import { WalletDetailsModal } from "./components/wallet-details-modal";
import { mockConsensusData, getWalletDetails, getNetworkData } from "@/lib/mock-data";
import type { WalletDetails } from "@/lib/mock-data";

export default function DashboardPage() {
  const [selectedWallet, setSelectedWallet] = useState<WalletDetails | null>(null);
  const [modalOpen, setModalOpen] = useState(false);

  const handleWalletClick = useCallback((address: string, tokenSymbol: string) => {
    const details = getWalletDetails(address, tokenSymbol);
    setSelectedWallet(details);
    setModalOpen(true);
  }, []);

  const handleClose = useCallback(() => {
    setModalOpen(false);
    setTimeout(() => setSelectedWallet(null), 300);
  }, []);

  const networkData = selectedWallet ? getNetworkData(selectedWallet) : { nodes: [], links: [] };

  return (
    <div className="mx-auto max-w-7xl px-6 py-5">
      {/* Consensus Section Header */}
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-lg font-bold text-foreground text-balance">
          Consensus Smart Wallets
        </h2>
        <span className="text-xs text-muted-foreground">
          {mockConsensusData.length} active groups
        </span>
      </div>

      {/* Consensus Cards - compact accordion list */}
      <div className="space-y-2">
        {mockConsensusData.map((group) => (
          <ConsensusCard
            key={group.id}
            group={group}
            onWalletClick={handleWalletClick}
          />
        ))}
      </div>

      {/* Wallet Details Modal */}
      {selectedWallet && (
        <WalletDetailsModal
          isOpen={modalOpen}
          onClose={handleClose}
          wallet={selectedWallet}
          networkData={networkData}
        />
      )}
    </div>
  );
}
