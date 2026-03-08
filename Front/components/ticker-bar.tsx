"use client";

import { tickerData } from "@/lib/mock-data";

export function TickerBar() {
  const items = [...tickerData, ...tickerData];

  return (
    <div className="fixed top-[60px] left-0 right-0 z-40 flex h-10 items-center overflow-hidden border-b border-[rgba(255,255,255,0.1)] bg-[rgba(26,26,26,0.8)] backdrop-blur-sm">
      <div className="animate-marquee flex whitespace-nowrap">
        {items.map((item, i) => (
          <span key={i} className="mx-4 text-sm font-medium">
            <span className="text-foreground">{item.symbol}</span>{" "}
            <span
              className={
                item.change >= 0 ? "text-wit-green" : "text-wit-red"
              }
            >
              {item.change >= 0 ? "+" : ""}
              {item.change.toFixed(1)}%
            </span>
            {i < items.length - 1 && (
              <span className="ml-4 text-wit-gray">{"·"}</span>
            )}
          </span>
        ))}
      </div>
    </div>
  );
}
