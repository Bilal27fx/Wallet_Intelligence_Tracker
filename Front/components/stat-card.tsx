"use client";

import { TrendingUp, TrendingDown } from "lucide-react";

interface StatCardProps {
  label: string;
  value: string;
  trend?: "up" | "down";
  trendValue?: string;
}

export function StatCard({ label, value, trend, trendValue }: StatCardProps) {
  return (
    <div className="rounded-xl border border-[rgba(255,255,255,0.1)] bg-card p-6 shadow-[0_4px_6px_rgba(0,0,0,0.3)]">
      <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className="mt-2 text-3xl font-bold text-foreground">{value}</p>
      {trend && trendValue && (
        <div className="mt-2 flex items-center gap-1">
          {trend === "up" ? (
            <TrendingUp className="h-3 w-3 text-wit-green" />
          ) : (
            <TrendingDown className="h-3 w-3 text-wit-red" />
          )}
          <span
            className={`text-xs font-medium ${
              trend === "up" ? "text-wit-green" : "text-wit-red"
            }`}
          >
            {trendValue}
          </span>
        </div>
      )}
    </div>
  );
}
