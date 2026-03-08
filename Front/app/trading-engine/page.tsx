"use client";

import { useState, useMemo } from "react";
import { motion } from "framer-motion";
import {
  Play,
  RotateCcw,
  TrendingUp,
  DollarSign,
  Target,
  BarChart3,
  Lock,
  Wallet,
  PieChart,
} from "lucide-react";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Slider } from "@/components/ui/slider";

// Mock backtest results
const equityCurveData = [
  { date: "Jan", value: 10000 },
  { date: "Feb", value: 12400 },
  { date: "Mar", value: 15800 },
  { date: "Apr", value: 14200 },
  { date: "May", value: 19600 },
  { date: "Jun", value: 24500 },
  { date: "Jul", value: 28900 },
  { date: "Aug", value: 32100 },
  { date: "Sep", value: 38700 },
  { date: "Oct", value: 45200 },
  { date: "Nov", value: 56800 },
  { date: "Dec", value: 72400 },
  { date: "Jan 2", value: 89500 },
  { date: "Feb 2", value: 105200 },
  { date: "Mar 2", value: 139600 },
];

const roiByParamData = [
  { name: "Set A", roi: 1295.7 },
  { name: "Set B", roi: 987.3 },
  { name: "Set C", roi: 834.2 },
  { name: "Set D", roi: 612.8 },
  { name: "Set E", roi: 489.1 },
];

const topParamSets = [
  {
    name: "Set A",
    roi: 1295.7,
    winRate: 81.6,
    trades: 228,
    maxDrawdown: -12.3,
    sharpe: 3.42,
  },
  {
    name: "Set B",
    roi: 987.3,
    winRate: 78.2,
    trades: 195,
    maxDrawdown: -15.8,
    sharpe: 2.98,
  },
  {
    name: "Set C",
    roi: 834.2,
    winRate: 75.4,
    trades: 312,
    maxDrawdown: -18.2,
    sharpe: 2.54,
  },
  {
    name: "Set D",
    roi: 612.8,
    winRate: 72.1,
    trades: 167,
    maxDrawdown: -22.1,
    sharpe: 2.11,
  },
  {
    name: "Set E",
    roi: 489.1,
    winRate: 69.8,
    trades: 284,
    maxDrawdown: -25.4,
    sharpe: 1.87,
  },
];

export default function TradingEnginePage() {
  const [minWallets, setMinWallets] = useState([3]);
  const [timeWindow, setTimeWindow] = useState([24]);
  const [minInvestment, setMinInvestment] = useState([5000]);
  const [maxGap, setMaxGap] = useState([4]);
  const [capital, setCapital] = useState([10000]);
  const [riskPerPos, setRiskPerPos] = useState([5]);
  const [takeProfit, setTakeProfit] = useState([150]);
  const [stopLoss, setStopLoss] = useState([25]);
  const [maxPositions, setMaxPositions] = useState([5]);
  const [isRunning, setIsRunning] = useState(false);
  const [hasResults, setHasResults] = useState(true);

  const winLossData = useMemo(
    () => [
      { name: "Winning", count: 186, color: "#00ff88" },
      { name: "Losing", count: 42, color: "#f44336" },
    ],
    []
  );

  const handleRunBacktest = () => {
    setIsRunning(true);
    setTimeout(() => {
      setIsRunning(false);
      setHasResults(true);
    }, 2000);
  };

  const handleReset = () => {
    setMinWallets([3]);
    setTimeWindow([24]);
    setMinInvestment([5000]);
    setMaxGap([4]);
    setCapital([10000]);
    setRiskPerPos([5]);
    setTakeProfit([150]);
    setStopLoss([25]);
    setMaxPositions([5]);
  };

  return (
    <div className="mx-auto max-w-7xl px-6 py-8">
      {/* Page Title */}
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-foreground text-balance">
          Backtest Optimizer
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Optimize your trading parameters using historical consensus data
        </p>
      </div>

      {/* Main Content */}
      <div className="flex gap-6">
        {/* Left Sidebar - Parameters */}
        <div className="w-[300px] shrink-0">
          <div className="sticky top-[120px] space-y-6">
            {/* Consensus Filters */}
            <div className="rounded-xl border border-[rgba(255,255,255,0.1)] bg-card p-5 shadow-[0_4px_6px_rgba(0,0,0,0.3)]">
              <h3 className="mb-4 text-xs font-semibold uppercase tracking-widest text-muted-foreground">
                Consensus Filters
              </h3>
              <div className="space-y-5">
                <ParamSlider
                  label="Min Wallets"
                  value={minWallets}
                  onChange={setMinWallets}
                  min={2}
                  max={10}
                  step={1}
                  format={(v) => `${v} wallets`}
                />
                <ParamSlider
                  label="Time Window"
                  value={timeWindow}
                  onChange={setTimeWindow}
                  min={1}
                  max={72}
                  step={1}
                  format={(v) => `${v}h`}
                />
                <ParamSlider
                  label="Min Investment"
                  value={minInvestment}
                  onChange={setMinInvestment}
                  min={1000}
                  max={100000}
                  step={1000}
                  format={(v) => `$${(v / 1000).toFixed(0)}K`}
                />
                <ParamSlider
                  label="Max Gap"
                  value={maxGap}
                  onChange={setMaxGap}
                  min={1}
                  max={24}
                  step={1}
                  format={(v) => `${v}h`}
                />
              </div>
            </div>

            {/* Trading Params */}
            <div className="rounded-xl border border-[rgba(255,255,255,0.1)] bg-card p-5 shadow-[0_4px_6px_rgba(0,0,0,0.3)]">
              <h3 className="mb-4 text-xs font-semibold uppercase tracking-widest text-muted-foreground">
                Trading Parameters
              </h3>
              <div className="space-y-5">
                <ParamSlider
                  label="Capital"
                  value={capital}
                  onChange={setCapital}
                  min={1000}
                  max={100000}
                  step={1000}
                  format={(v) => `$${(v / 1000).toFixed(0)}K`}
                />
                <ParamSlider
                  label="Risk / Position"
                  value={riskPerPos}
                  onChange={setRiskPerPos}
                  min={1}
                  max={20}
                  step={1}
                  format={(v) => `${v}%`}
                />
                <ParamSlider
                  label="Take Profit"
                  value={takeProfit}
                  onChange={setTakeProfit}
                  min={10}
                  max={500}
                  step={10}
                  format={(v) => `${v}%`}
                />
                <ParamSlider
                  label="Stop Loss"
                  value={stopLoss}
                  onChange={setStopLoss}
                  min={5}
                  max={50}
                  step={5}
                  format={(v) => `${v}%`}
                />
                <ParamSlider
                  label="Max Positions"
                  value={maxPositions}
                  onChange={setMaxPositions}
                  min={1}
                  max={20}
                  step={1}
                  format={(v) => `${v}`}
                />
              </div>
            </div>

            {/* Action Buttons */}
            <div className="flex gap-3">
              <button
                onClick={handleRunBacktest}
                disabled={isRunning}
                className="flex flex-1 items-center justify-center gap-2 rounded-lg bg-wit-green py-3 text-sm font-semibold text-[#0a0a0a] transition-all hover:bg-wit-green/90 disabled:opacity-50"
              >
                {isRunning ? (
                  <div className="h-4 w-4 animate-spin rounded-full border-2 border-[#0a0a0a] border-t-transparent" />
                ) : (
                  <Play className="h-4 w-4" />
                )}
                {isRunning ? "Running..." : "Run Backtest"}
              </button>
              <button
                onClick={handleReset}
                className="flex items-center justify-center rounded-lg border border-[rgba(255,255,255,0.1)] px-4 py-3 text-sm text-muted-foreground transition-colors hover:bg-[rgba(255,255,255,0.05)] hover:text-foreground"
              >
                <RotateCcw className="h-4 w-4" />
              </button>
            </div>
          </div>
        </div>

        {/* Right Content - Results */}
        <div className="flex-1 space-y-6">
          {hasResults ? (
            <>
              {/* Performance Metrics */}
              <div className="grid grid-cols-4 gap-4">
                <MetricCard
                  icon={<TrendingUp className="h-4 w-4" />}
                  label="ROI"
                  value="+1,295.7%"
                  color="#00ff88"
                />
                <MetricCard
                  icon={<DollarSign className="h-4 w-4" />}
                  label="Total P&L"
                  value="$129.6K"
                  color="#00ff88"
                />
                <MetricCard
                  icon={<Target className="h-4 w-4" />}
                  label="Win Rate"
                  value="81.6%"
                  color="#ffc107"
                />
                <MetricCard
                  icon={<BarChart3 className="h-4 w-4" />}
                  label="Total Trades"
                  value="228"
                  color="#2196f3"
                />
              </div>

              {/* Equity Curve */}
              <div className="rounded-xl border border-[rgba(255,255,255,0.1)] bg-card p-5 shadow-[0_4px_6px_rgba(0,0,0,0.3)]">
                <h3 className="mb-4 text-xs font-semibold uppercase tracking-widest text-muted-foreground">
                  Equity Curve
                </h3>
                <div className="h-[280px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={equityCurveData}>
                      <defs>
                        <linearGradient
                          id="equityGradient"
                          x1="0"
                          y1="0"
                          x2="0"
                          y2="1"
                        >
                          <stop
                            offset="5%"
                            stopColor="#00ff88"
                            stopOpacity={0.3}
                          />
                          <stop
                            offset="95%"
                            stopColor="#00ff88"
                            stopOpacity={0}
                          />
                        </linearGradient>
                      </defs>
                      <CartesianGrid
                        strokeDasharray="3 3"
                        stroke="rgba(255,255,255,0.05)"
                      />
                      <XAxis
                        dataKey="date"
                        tick={{ fontSize: 10, fill: "#888" }}
                        axisLine={false}
                        tickLine={false}
                      />
                      <YAxis
                        tick={{ fontSize: 10, fill: "#888" }}
                        axisLine={false}
                        tickLine={false}
                        tickFormatter={(v) => `$${(v / 1000).toFixed(0)}K`}
                      />
                      <Tooltip
                        contentStyle={{
                          background: "#1a1a1a",
                          border: "1px solid rgba(255,255,255,0.1)",
                          borderRadius: "8px",
                          color: "#fff",
                          fontSize: "12px",
                        }}
                        formatter={(value: number) => [
                          `$${value.toLocaleString()}`,
                          "Equity",
                        ]}
                      />
                      <Area
                        type="monotone"
                        dataKey="value"
                        stroke="#00ff88"
                        strokeWidth={2}
                        fill="url(#equityGradient)"
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              </div>

              {/* ROI by Parameter Set + Win/Loss */}
              <div className="grid grid-cols-2 gap-6">
                {/* ROI by Parameter Set */}
                <div className="rounded-xl border border-[rgba(255,255,255,0.1)] bg-card p-5 shadow-[0_4px_6px_rgba(0,0,0,0.3)]">
                  <h3 className="mb-4 text-xs font-semibold uppercase tracking-widest text-muted-foreground">
                    ROI by Parameter Set
                  </h3>
                  <div className="h-[200px]">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={roiByParamData}>
                        <CartesianGrid
                          strokeDasharray="3 3"
                          stroke="rgba(255,255,255,0.05)"
                        />
                        <XAxis
                          dataKey="name"
                          tick={{ fontSize: 10, fill: "#888" }}
                          axisLine={false}
                          tickLine={false}
                        />
                        <YAxis
                          tick={{ fontSize: 10, fill: "#888" }}
                          axisLine={false}
                          tickLine={false}
                          tickFormatter={(v) => `${v}%`}
                        />
                        <Tooltip
                          contentStyle={{
                            background: "#1a1a1a",
                            border: "1px solid rgba(255,255,255,0.1)",
                            borderRadius: "8px",
                            color: "#fff",
                            fontSize: "12px",
                          }}
                          formatter={(value: number) => [`${value}%`, "ROI"]}
                        />
                        <Bar dataKey="roi" radius={[4, 4, 0, 0]}>
                          {roiByParamData.map((_, index) => (
                            <Cell
                              key={`cell-${index}`}
                              fill={
                                index === 0
                                  ? "#00ff88"
                                  : `rgba(0,255,136,${0.7 - index * 0.12})`
                              }
                            />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>

                {/* Win/Loss Breakdown */}
                <div className="rounded-xl border border-[rgba(255,255,255,0.1)] bg-card p-5 shadow-[0_4px_6px_rgba(0,0,0,0.3)]">
                  <h3 className="mb-4 text-xs font-semibold uppercase tracking-widest text-muted-foreground">
                    Win/Loss Breakdown
                  </h3>
                  <div className="flex flex-col justify-center gap-6 py-4">
                    {winLossData.map((item) => (
                      <div key={item.name}>
                        <div className="mb-1 flex items-center justify-between text-xs">
                          <span className="text-muted-foreground">
                            {item.name}
                          </span>
                          <span
                            className="font-medium"
                            style={{ color: item.color }}
                          >
                            {item.count} trades (
                            {(
                              (item.count /
                                winLossData.reduce(
                                  (a, b) => a + b.count,
                                  0
                                )) *
                              100
                            ).toFixed(1)}
                            %)
                          </span>
                        </div>
                        <div className="h-6 overflow-hidden rounded bg-[rgba(255,255,255,0.05)]">
                          <motion.div
                            initial={{ width: 0 }}
                            animate={{
                              width: `${
                                (item.count /
                                  winLossData.reduce(
                                    (a, b) => a + b.count,
                                    0
                                  )) *
                                100
                              }%`,
                            }}
                            transition={{ duration: 1, delay: 0.3 }}
                            className="h-full rounded"
                            style={{ background: item.color }}
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>

              {/* Top Parameter Sets Table */}
              <div className="rounded-xl border border-[rgba(255,255,255,0.1)] bg-card p-5 shadow-[0_4px_6px_rgba(0,0,0,0.3)]">
                <h3 className="mb-4 text-xs font-semibold uppercase tracking-widest text-muted-foreground">
                  Top Parameter Sets
                </h3>
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="border-b border-[rgba(255,255,255,0.1)]">
                        <th className="px-4 py-3 text-left text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                          Name
                        </th>
                        <th className="px-4 py-3 text-right text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                          ROI
                        </th>
                        <th className="px-4 py-3 text-right text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                          Win Rate
                        </th>
                        <th className="px-4 py-3 text-right text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                          Trades
                        </th>
                        <th className="px-4 py-3 text-right text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                          Max DD
                        </th>
                        <th className="px-4 py-3 text-right text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                          Sharpe
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {topParamSets.map((row, idx) => (
                        <tr
                          key={row.name}
                          className="border-b border-[rgba(255,255,255,0.05)] transition-colors hover:bg-[rgba(255,255,255,0.03)]"
                        >
                          <td className="px-4 py-3 text-sm font-medium text-foreground">
                            <span className="flex items-center gap-2">
                              {idx === 0 && (
                                <span className="h-1.5 w-1.5 rounded-full bg-wit-green" />
                              )}
                              {row.name}
                            </span>
                          </td>
                          <td className="px-4 py-3 text-right text-sm font-medium text-wit-green">
                            +{row.roi}%
                          </td>
                          <td className="px-4 py-3 text-right text-sm text-foreground">
                            {row.winRate}%
                          </td>
                          <td className="px-4 py-3 text-right text-sm text-foreground">
                            {row.trades}
                          </td>
                          <td className="px-4 py-3 text-right text-sm text-wit-red">
                            {row.maxDrawdown}%
                          </td>
                          <td className="px-4 py-3 text-right text-sm text-foreground">
                            {row.sharpe}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </>
          ) : (
            <div className="flex h-[500px] items-center justify-center rounded-xl border border-[rgba(255,255,255,0.1)] bg-card">
              <div className="text-center">
                <BarChart3 className="mx-auto mb-4 h-12 w-12 text-muted-foreground" />
                <h3 className="mb-2 text-lg font-semibold text-foreground">
                  No Results Yet
                </h3>
                <p className="text-sm text-muted-foreground">
                  Configure your parameters and run a backtest to see results
                </p>
              </div>
            </div>
          )}

          {/* Coming Soon Cards */}
          <div className="grid grid-cols-2 gap-6">
            <ComingSoonCard
              icon={<Wallet className="h-5 w-5" />}
              title="Connect Wallet"
            />
            <ComingSoonCard
              icon={<PieChart className="h-5 w-5" />}
              title="Portfolio Management"
            />
          </div>
        </div>
      </div>
    </div>
  );
}

function ParamSlider({
  label,
  value,
  onChange,
  min,
  max,
  step,
  format,
}: {
  label: string;
  value: number[];
  onChange: (val: number[]) => void;
  min: number;
  max: number;
  step: number;
  format: (val: number) => string;
}) {
  return (
    <div>
      <div className="mb-2 flex items-center justify-between">
        <span className="text-xs text-muted-foreground">{label}</span>
        <span className="text-xs font-medium text-foreground">
          {format(value[0])}
        </span>
      </div>
      <Slider
        value={value}
        onValueChange={onChange}
        min={min}
        max={max}
        step={step}
        className="[&_[role=slider]]:h-3.5 [&_[role=slider]]:w-3.5 [&_[role=slider]]:border-wit-green [&_[role=slider]]:bg-wit-green [&_[data-disabled]]:opacity-50 [&_.relative]:bg-[rgba(255,255,255,0.1)] [&_[data-orientation=horizontal]>[data-orientation=horizontal]]:bg-wit-green"
      />
    </div>
  );
}

function MetricCard({
  icon,
  label,
  value,
  color,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  color: string;
}) {
  return (
    <div className="rounded-xl border border-[rgba(255,255,255,0.1)] bg-card p-4 shadow-[0_4px_6px_rgba(0,0,0,0.3)]">
      <div
        className="mb-2 flex h-8 w-8 items-center justify-center rounded-lg"
        style={{ background: `${color}20`, color }}
      >
        {icon}
      </div>
      <p className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className="mt-1 text-xl font-bold" style={{ color }}>
        {value}
      </p>
    </div>
  );
}

function ComingSoonCard({
  icon,
  title,
}: {
  icon: React.ReactNode;
  title: string;
}) {
  return (
    <div className="flex items-center justify-between rounded-xl border border-[rgba(255,255,255,0.1)] bg-card p-5 shadow-[0_4px_6px_rgba(0,0,0,0.3)]">
      <div className="flex items-center gap-3">
        <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-[rgba(255,255,255,0.05)] text-muted-foreground">
          {icon}
        </div>
        <div>
          <h3 className="text-sm font-semibold text-foreground">{title}</h3>
          <p className="text-xs text-muted-foreground">Coming Soon</p>
        </div>
      </div>
      <Lock className="h-4 w-4 text-muted-foreground" />
    </div>
  );
}
