"use client";

import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { ChevronDown, Eye, Copy } from "lucide-react";
import {
  Area,
  AreaChart,
  ResponsiveContainer,
  YAxis,
} from "recharts";
import type { ConsensusGroup } from "@/lib/mock-data";

interface ConsensusCardProps {
  group: ConsensusGroup;
  onWalletClick: (address: string, tokenSymbol: string) => void;
}

function scoreColor(score: number) {
  if (score >= 80) return { ring: "#00ff88", bg: "rgba(0,255,136,0.1)" };
  if (score >= 60) return { ring: "#ffc107", bg: "rgba(255,193,7,0.1)" };
  return { ring: "#f44336", bg: "rgba(244,67,54,0.1)" };
}

function strengthStyle(s: string) {
  if (s === "STRONG") return "bg-wit-green/10 text-wit-green";
  if (s === "MODERATE") return "bg-wit-yellow/10 text-wit-yellow";
  return "bg-wit-red/10 text-wit-red";
}

function fmt(v: number) {
  if (v >= 1e6) return `$${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `$${(v / 1e3).toFixed(0)}K`;
  return `$${v.toFixed(2)}`;
}

function fmtPrice(p: number) {
  if (p < 0.001) return `$${p.toFixed(8)}`;
  if (p < 1) return `$${p.toFixed(4)}`;
  return `$${p.toFixed(2)}`;
}

export function ConsensusCard({ group, onWalletClick }: ConsensusCardProps) {
  const [expanded, setExpanded] = useState(false);
  const [copied, setCopied] = useState(false);
  const sc = scoreColor(group.score);

  const handleCopy = (e: React.MouseEvent) => {
    e.stopPropagation();
    navigator.clipboard.writeText(group.token.contractAddress);
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  };

  const contract = group.token.contractAddress;
  const shortContract = `${contract.slice(0, 6)}...${contract.slice(-4)}`;

  return (
    <div className="overflow-hidden rounded-lg border border-border/60 bg-card">
      {/* Collapsed row */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex w-full items-center gap-5 px-5 py-4 text-left transition-colors hover:bg-[rgba(255,255,255,0.03)]"
      >
        {/* Score circle */}
        <div
          className="flex h-12 w-12 shrink-0 items-center justify-center rounded-full text-base font-bold"
          style={{
            background: sc.bg,
            border: `2px solid ${sc.ring}`,
            color: sc.ring,
          }}
        >
          {group.score}
        </div>

        {/* Left block: token identity */}
        <div className="flex min-w-0 flex-1 flex-col gap-1">
          <div className="flex items-center gap-2">
            <span className="text-base font-bold text-foreground">{group.token.symbol}</span>
            <span className="rounded bg-muted px-1.5 py-0.5 text-[10px] font-medium text-muted-foreground">
              {group.token.chain}
            </span>
            <span className={`rounded-full px-2 py-0.5 text-[10px] font-semibold ${strengthStyle(group.strength)}`}>
              {group.strength}
            </span>
            <span className="rounded bg-muted px-1.5 py-0.5 text-[10px] text-muted-foreground">
              {group.metrics.consensusType}
            </span>
          </div>
          <div className="flex items-center gap-1.5">
            <span className="font-mono text-[11px] text-muted-foreground">{shortContract}</span>
            <button
              onClick={handleCopy}
              className="rounded p-0.5 text-muted-foreground transition-colors hover:text-foreground"
            >
              <Copy className="h-3 w-3" />
            </button>
            {copied && <span className="text-[10px] text-wit-green">Copied</span>}
          </div>
        </div>

        {/* Center: key data points as a clean grid */}
        <div className="hidden gap-x-8 gap-y-1 lg:grid lg:grid-cols-4">
          <DataCell label="Wallets" value={String(group.metrics.walletsCount)} />
          <DataCell label="Capital" value={fmt(group.metrics.totalCapital)} />
          <DataCell label="Best ROI" value={`+${group.metrics.bestROI}%`} accent="green" />
          <DataCell label="Token Age" value={group.token.age} />
          <DataCell label="Avg Entry" value={fmtPrice(group.metrics.avgEntryPrice)} />
          <DataCell label="Interval" value={`${group.metrics.intervalMin} - ${group.metrics.intervalMax}`} />
          <DataCell label="Worst ROI" value={`+${group.metrics.worstROI}%`} accent="yellow" />
          <DataCell label="Detected" value={group.detectedAt} />
        </div>

        {/* Sparkline + chevron */}
        <div className="flex shrink-0 items-center gap-3">
          <div className="hidden h-9 w-20 md:block">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={group.performance7d}>
                <defs>
                  <linearGradient id={`sp-${group.id}`} x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor={sc.ring} stopOpacity={0.3} />
                    <stop offset="95%" stopColor={sc.ring} stopOpacity={0} />
                  </linearGradient>
                </defs>
                <YAxis hide domain={["dataMin", "dataMax"]} />
                <Area
                  type="monotone"
                  dataKey="value"
                  stroke={sc.ring}
                  strokeWidth={1.5}
                  fill={`url(#sp-${group.id})`}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
          <motion.div animate={{ rotate: expanded ? 180 : 0 }} transition={{ duration: 0.2 }}>
            <ChevronDown className="h-4 w-4 text-muted-foreground" />
          </motion.div>
        </div>
      </button>

      {/* Mobile data (shown always on small screens, hidden on lg+) */}
      <div className="grid grid-cols-4 gap-x-4 gap-y-2 border-t border-border/40 px-5 py-3 lg:hidden">
        <DataCell label="Wallets" value={String(group.metrics.walletsCount)} />
        <DataCell label="Capital" value={fmt(group.metrics.totalCapital)} />
        <DataCell label="Best ROI" value={`+${group.metrics.bestROI}%`} accent="green" />
        <DataCell label="Token Age" value={group.token.age} />
        <DataCell label="Avg Entry" value={fmtPrice(group.metrics.avgEntryPrice)} />
        <DataCell label="Interval" value={`${group.metrics.intervalMin} - ${group.metrics.intervalMax}`} />
        <DataCell label="Worst ROI" value={`+${group.metrics.worstROI}%`} accent="yellow" />
        <DataCell label="Detected" value={group.detectedAt} />
      </div>

      {/* Expanded */}
      <AnimatePresence>
        {expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.25 }}
            className="overflow-hidden"
          >
            <div className="border-t border-border/40 px-5 py-4">
              {/* Extra details row */}
              <div className="mb-4 grid grid-cols-2 gap-3 rounded-lg bg-muted/30 px-4 py-3 md:grid-cols-4">
                <DataCell label="Contract" value={shortContract} mono />
                <DataCell label="Chain" value={group.token.chain} />
                <DataCell label="Token Created" value={group.token.createdAt} />
                <DataCell label="Detection Date" value={group.metrics.detectionDate} />
              </div>

              {/* Performance chart */}
              <div className="mb-4 h-28 w-full rounded-lg bg-muted/20 p-3">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={group.performance7d}>
                    <defs>
                      <linearGradient id={`ch-${group.id}`} x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor={sc.ring} stopOpacity={0.2} />
                        <stop offset="95%" stopColor={sc.ring} stopOpacity={0} />
                      </linearGradient>
                    </defs>
                    <YAxis hide domain={["dataMin - 5", "dataMax + 5"]} />
                    <Area
                      type="monotone"
                      dataKey="value"
                      stroke={sc.ring}
                      strokeWidth={1.5}
                      fill={`url(#ch-${group.id})`}
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>

              {/* Wallets */}
              <div className="grid gap-2 md:grid-cols-3 lg:grid-cols-4">
                {group.wallets.map((w) => (
                  <button
                    key={w.address}
                    onClick={() => onWalletClick(w.address, group.token.symbol)}
                    className="group flex items-center gap-2 rounded-md border border-border/40 bg-muted/20 px-3 py-2.5 text-left transition-colors hover:border-wit-green/30 hover:bg-muted/40"
                  >
                    <div className="min-w-0 flex-1">
                      <p className="truncate font-mono text-[11px] text-foreground">{w.address}</p>
                      <div className="mt-1 flex items-center gap-3 text-[10px] text-muted-foreground">
                        <span>Q: <span className="text-foreground">{w.qualityScore}</span></span>
                        <span>WR: <span className="text-wit-green">{w.winRate}%</span></span>
                        <span>ROI: <span className="text-wit-green">+{w.roi}%</span></span>
                      </div>
                    </div>
                    <Eye className="h-3.5 w-3.5 shrink-0 text-muted-foreground opacity-0 transition-opacity group-hover:opacity-100" />
                  </button>
                ))}
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

/* Small reusable data cell */
function DataCell({
  label,
  value,
  accent,
  mono,
}: {
  label: string;
  value: string;
  accent?: "green" | "yellow" | "red";
  mono?: boolean;
}) {
  const valColor =
    accent === "green"
      ? "text-wit-green"
      : accent === "yellow"
        ? "text-wit-yellow"
        : accent === "red"
          ? "text-wit-red"
          : "text-foreground";

  return (
    <div className="flex flex-col">
      <span className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</span>
      <span className={`text-xs font-semibold ${valColor} ${mono ? "font-mono" : ""}`}>{value}</span>
    </div>
  );
}
