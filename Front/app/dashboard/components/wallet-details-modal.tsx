"use client";

import { useState, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  X,
  Copy,
  ExternalLink,
  ChevronDown,
  ChevronRight,
  Check,
} from "lucide-react";
import { ScrollArea } from "@/components/ui/scroll-area";
import { NetworkGraph } from "./network-graph";
import type { WalletDetails, NetworkNode, NetworkLink } from "@/lib/mock-data";

interface WalletDetailsModalProps {
  isOpen: boolean;
  onClose: () => void;
  wallet: WalletDetails;
  networkData: {
    nodes: NetworkNode[];
    links: NetworkLink[];
  };
}

function formatCurrency(value: number): string {
  if (value >= 1000000) return `$${(value / 1000000).toFixed(1)}M`;
  if (value >= 1000) return `$${(value / 1000).toFixed(0)}K`;
  return `$${value.toFixed(2)}`;
}

function formatPrice(price: number): string {
  if (price < 0.001) return `$${price.toFixed(8)}`;
  if (price < 1) return `$${price.toFixed(4)}`;
  return `$${price.toFixed(2)}`;
}

export function WalletDetailsModal({
  isOpen,
  onClose,
  wallet,
  networkData,
}: WalletDetailsModalProps) {
  const [copied, setCopied] = useState(false);
  const [expandedSections, setExpandedSections] = useState<
    Record<string, boolean>
  >({
    buyWallets: true,
    dumpWallets: true,
    cex: true,
    unknown: false,
  });

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(wallet.address);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [wallet.address]);

  const toggleSection = (section: string) => {
    setExpandedSections((prev) => ({ ...prev, [section]: !prev[section] }));
  };

  const handleNodeClick = useCallback((nodeId: string) => {
    // scroll to corresponding section
    console.log("Node clicked:", nodeId);
  }, []);

  const strengthColor =
    wallet.qualityScore >= 85
      ? "#00ff88"
      : wallet.qualityScore >= 70
        ? "#ffc107"
        : "#f44336";

  return (
    <AnimatePresence>
      {isOpen && (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          className="fixed inset-0 z-[100] flex items-center justify-center"
          onClick={onClose}
        >
          {/* Backdrop */}
          <div className="absolute inset-0 bg-[rgba(10,10,10,0.98)] backdrop-blur-[15px]" />

          {/* Modal */}
          <motion.div
            initial={{ opacity: 0, scale: 0.95, y: 20 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.95, y: 20 }}
            transition={{ type: "spring", damping: 25, stiffness: 300 }}
            className="relative flex h-[85vh] w-[90vw] max-w-[1400px] overflow-hidden rounded-xl border border-[rgba(255,255,255,0.1)] bg-card shadow-2xl"
            onClick={(e) => e.stopPropagation()}
          >
            {/* Header */}
            <div className="absolute top-0 right-0 left-0 z-10 flex items-center justify-between border-b border-[rgba(255,255,255,0.1)] bg-card px-6 py-4">
              <div className="flex items-center gap-3">
                <h2 className="text-base font-bold text-foreground">
                  WALLET DETAILS:
                </h2>
                <span className="font-mono text-sm text-wit-green">
                  {wallet.address}
                </span>
              </div>
              <button
                onClick={onClose}
                className="flex h-8 w-8 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-[rgba(255,255,255,0.1)] hover:text-foreground"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            {/* Content */}
            <div className="flex h-full w-full pt-[56px]">
              {/* Left Sidebar (30%) */}
              <ScrollArea className="h-full w-[30%] min-w-[340px] border-r border-[rgba(255,255,255,0.1)]">
                <div className="space-y-6 p-6">
                  {/* Quality Score */}
                  <div className="flex flex-col items-center">
                    <div
                      className="flex h-24 w-24 flex-col items-center justify-center rounded-full animate-pulse-glow"
                      style={{
                        background: `${strengthColor}15`,
                        border: `2px solid ${strengthColor}60`,
                        boxShadow: `0 0 30px ${strengthColor}40`,
                      }}
                    >
                      <span className="text-4xl font-bold text-foreground">
                        {wallet.qualityScore}
                      </span>
                      <span
                        className="text-[10px] font-semibold uppercase tracking-wider"
                        style={{ color: strengthColor }}
                      >
                        QUALITY
                      </span>
                    </div>

                    <div className="mt-3 flex items-center gap-1">
                      <span
                        className="h-2 w-2 rounded-full"
                        style={{ background: strengthColor }}
                      />
                      <span
                        className="text-xs font-medium"
                        style={{ color: strengthColor }}
                      >
                        Smart Wallet
                      </span>
                    </div>
                  </div>

                  {/* Address + actions */}
                  <div>
                    <p className="mb-1 text-xs text-muted-foreground">
                      Full Address
                    </p>
                    <p className="mb-2 break-all font-mono text-xs text-foreground">
                      {wallet.address}
                    </p>
                    <div className="flex gap-2">
                      <button
                        onClick={handleCopy}
                        className="flex items-center gap-1 rounded-md bg-[rgba(255,255,255,0.05)] px-3 py-1.5 text-xs text-muted-foreground transition-colors hover:bg-[rgba(255,255,255,0.1)] hover:text-foreground"
                      >
                        {copied ? (
                          <Check className="h-3 w-3 text-wit-green" />
                        ) : (
                          <Copy className="h-3 w-3" />
                        )}
                        {copied ? "Copied" : "Copy"}
                      </button>
                      <a
                        href={`https://etherscan.io/address/${wallet.address}`}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex items-center gap-1 rounded-md bg-[rgba(255,255,255,0.05)] px-3 py-1.5 text-xs text-muted-foreground transition-colors hover:bg-[rgba(255,255,255,0.1)] hover:text-foreground"
                      >
                        <ExternalLink className="h-3 w-3" />
                        Etherscan
                      </a>
                    </div>
                  </div>

                  <Divider />

                  {/* Performance Metrics */}
                  <Section title="PERFORMANCE METRICS">
                    <div className="space-y-3">
                      <div>
                        <div className="mb-1 flex justify-between text-xs">
                          <span className="text-muted-foreground">
                            Win Rate
                          </span>
                          <span className="font-medium text-foreground">
                            {wallet.winRate}%
                          </span>
                        </div>
                        <div className="h-1.5 overflow-hidden rounded-full bg-[rgba(255,255,255,0.1)]">
                          <div
                            className="h-full rounded-full bg-wit-green"
                            style={{ width: `${wallet.winRate}%` }}
                          />
                        </div>
                      </div>
                      <MetricRow
                        label="ROI"
                        value={`+${wallet.roi}%`}
                        color="#00ff88"
                      />
                      <MetricRow
                        label="Total Invested"
                        value={formatCurrency(wallet.metrics.totalInvested)}
                      />
                      <MetricRow
                        label="Total Realized"
                        value={formatCurrency(wallet.metrics.totalRealized)}
                        color="#00ff88"
                      />
                      <MetricRow
                        label="Current Holdings"
                        value={formatCurrency(wallet.metrics.currentHoldings)}
                      />
                    </div>
                  </Section>

                  <Divider />

                  {/* Trade Statistics */}
                  <Section title="TRADE STATISTICS">
                    <div className="space-y-2">
                      <MetricRow
                        label="Total Trades"
                        value={String(wallet.metrics.totalTrades)}
                      />
                      <MetricRow
                        label="Winning Trades"
                        value={String(wallet.metrics.winningTrades)}
                        color="#00ff88"
                      />
                      <MetricRow
                        label="Losing Trades"
                        value={String(wallet.metrics.losingTrades)}
                        color="#f44336"
                      />
                      <MetricRow
                        label="Avg Win"
                        value={`$${wallet.metrics.avgWin}`}
                        color="#00ff88"
                      />
                      <MetricRow
                        label="Avg Loss"
                        value={`$${wallet.metrics.avgLoss}`}
                        color="#f44336"
                      />
                      <MetricRow
                        label="Best Trade"
                        value={`+$${wallet.metrics.bestTrade.toLocaleString()}`}
                        color="#00ff88"
                      />
                      <MetricRow
                        label="Worst Trade"
                        value={`$${wallet.metrics.worstTrade}`}
                        color="#f44336"
                      />
                    </div>
                  </Section>

                  <Divider />

                  {/* Position */}
                  <Section
                    title={`POSITION IN ${wallet.currentPosition.token}`}
                  >
                    <div className="space-y-2">
                      <MetricRow
                        label="Entry Price"
                        value={formatPrice(wallet.currentPosition.entryPrice)}
                      />
                      <MetricRow
                        label="Current Price"
                        value={formatPrice(
                          wallet.currentPosition.currentPrice
                        )}
                        color="#00ff88"
                      />
                      <MetricRow
                        label="Position Size"
                        value={formatCurrency(wallet.currentPosition.size)}
                      />
                      <MetricRow
                        label="Unrealized P&L"
                        value={`+${formatCurrency(wallet.currentPosition.unrealizedPnl)}`}
                        color="#00ff88"
                      />
                      <MetricRow
                        label="Entry Date"
                        value={new Date(
                          wallet.currentPosition.entryDate
                        ).toLocaleDateString("en-US", {
                          month: "short",
                          day: "numeric",
                        })}
                      />
                      <MetricRow
                        label="Holding Time"
                        value={wallet.currentPosition.holdingTime}
                      />
                    </div>
                  </Section>

                  <Divider />

                  {/* Interaction Breakdown */}
                  <Section title="INTERACTION BREAKDOWN">
                    <div className="space-y-3">
                      {/* Buy Wallets */}
                      <CollapsibleGroup
                        title={`Buy Wallets (${wallet.interactions.buyWallets.length})`}
                        color="#2196f3"
                        isOpen={expandedSections.buyWallets}
                        onToggle={() => toggleSection("buyWallets")}
                      >
                        {wallet.interactions.buyWallets.map((w) => (
                          <InteractionItem
                            key={w.address}
                            address={w.address}
                            details={[
                              `${w.txCount} txs, ${formatCurrency(w.volume)} vol`,
                              `Role: ${w.role}`,
                            ]}
                          />
                        ))}
                      </CollapsibleGroup>

                      {/* Dump Wallets */}
                      <CollapsibleGroup
                        title={`Dump Wallets (${wallet.interactions.dumpWallets.length})`}
                        color="#f44336"
                        isOpen={expandedSections.dumpWallets}
                        onToggle={() => toggleSection("dumpWallets")}
                      >
                        {wallet.interactions.dumpWallets.map((w) => (
                          <InteractionItem
                            key={w.address}
                            address={w.address}
                            details={[
                              `${w.txCount} txs, ${formatCurrency(w.volume)} vol`,
                              `Role: ${w.role}`,
                            ]}
                          />
                        ))}
                      </CollapsibleGroup>

                      {/* CEX */}
                      <CollapsibleGroup
                        title={`CEX (${wallet.interactions.cex.length})`}
                        color="#ffc107"
                        isOpen={expandedSections.cex}
                        onToggle={() => toggleSection("cex")}
                      >
                        {wallet.interactions.cex.map((w) => (
                          <InteractionItem
                            key={w.address}
                            address={w.address}
                            details={[
                              `${w.name} deposit`,
                              `${w.txCount} tx, ${formatCurrency(w.volume)}`,
                            ]}
                          />
                        ))}
                      </CollapsibleGroup>

                      {/* Unknown */}
                      <CollapsibleGroup
                        title={`Unknown (${wallet.interactions.unknown.length})`}
                        color="#666666"
                        isOpen={expandedSections.unknown ?? false}
                        onToggle={() => toggleSection("unknown")}
                      >
                        {wallet.interactions.unknown.map((w) => (
                          <InteractionItem
                            key={w.address}
                            address={w.address}
                            details={[
                              `${w.txCount} txs, ${formatCurrency(w.volume)} vol`,
                            ]}
                          />
                        ))}
                      </CollapsibleGroup>
                    </div>
                  </Section>

                  <div className="pb-4">
                    <button className="w-full rounded-md border border-[rgba(255,255,255,0.1)] py-2 text-xs font-medium text-muted-foreground transition-colors hover:bg-[rgba(255,255,255,0.05)] hover:text-foreground">
                      View Full History
                    </button>
                  </div>
                </div>
              </ScrollArea>

              {/* Right Side - Network Graph (70%) */}
              <div className="flex h-full flex-1 flex-col">
                <div className="border-b border-[rgba(255,255,255,0.1)] px-6 py-3">
                  <h3 className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">
                    Network Graph
                  </h3>
                  <p className="text-[10px] text-muted-foreground">
                    {networkData.nodes.length} connected wallets | Drag nodes to
                    rearrange | Click node for details
                  </p>
                </div>
                <div className="flex-1">
                  <NetworkGraph
                    centerWallet={wallet.address}
                    data={networkData}
                    onNodeClick={handleNodeClick}
                  />
                </div>
              </div>
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}

function Divider() {
  return <div className="h-px bg-[rgba(255,255,255,0.1)]" />;
}

function Section({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <h4 className="mb-3 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
        {title}
      </h4>
      {children}
    </div>
  );
}

function MetricRow({
  label,
  value,
  color,
}: {
  label: string;
  value: string;
  color?: string;
}) {
  return (
    <div className="flex items-center justify-between text-xs">
      <span className="text-muted-foreground">{label}</span>
      <span className="font-medium" style={{ color: color || "#fff" }}>
        {value}
      </span>
    </div>
  );
}

function CollapsibleGroup({
  title,
  color,
  isOpen,
  onToggle,
  children,
}: {
  title: string;
  color: string;
  isOpen: boolean;
  onToggle: () => void;
  children: React.ReactNode;
}) {
  return (
    <div
      className="overflow-hidden rounded-lg"
      style={{ border: `1px solid ${color}30` }}
    >
      <button
        onClick={onToggle}
        className="flex w-full items-center justify-between px-3 py-2 text-xs font-medium transition-colors hover:bg-[rgba(255,255,255,0.03)]"
        style={{ color }}
      >
        <span className="flex items-center gap-2">
          <span
            className="h-2 w-2 rounded-full"
            style={{ background: color }}
          />
          {title}
        </span>
        {isOpen ? (
          <ChevronDown className="h-3 w-3" />
        ) : (
          <ChevronRight className="h-3 w-3" />
        )}
      </button>
      <AnimatePresence>
        {isOpen && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="overflow-hidden"
          >
            <div className="space-y-2 px-3 pb-3">{children}</div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

function InteractionItem({
  address,
  details,
}: {
  address: string;
  details: string[];
}) {
  return (
    <div className="rounded-md bg-[rgba(255,255,255,0.03)] p-2">
      <p className="mb-1 font-mono text-[11px] text-foreground">{address}</p>
      {details.map((d, i) => (
        <p key={i} className="text-[10px] text-muted-foreground">
          {d}
        </p>
      ))}
    </div>
  );
}
