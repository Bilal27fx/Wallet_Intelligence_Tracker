"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";

export function Header() {
  const pathname = usePathname();

  const isActive = (path: string) => {
    if (path === "/dashboard") return pathname === "/" || pathname === "/dashboard";
    return pathname.startsWith(path);
  };

  return (
    <header className="fixed top-0 left-0 right-0 z-50">
      <div className="flex h-[60px] items-center justify-between border-b border-[rgba(255,255,255,0.1)] bg-[#0a0a0a] px-6">
        <div className="flex items-center gap-8">
          <Link href="/dashboard" className="text-xl font-bold text-wit-green">
            WIT
          </Link>
          <nav className="flex items-center gap-2">
            <Link
              href="/dashboard"
              className={cn(
                "rounded-full px-4 py-1.5 text-sm font-medium transition-all",
                isActive("/dashboard")
                  ? "bg-wit-green text-[#0a0a0a]"
                  : "text-wit-gray hover:text-foreground"
              )}
            >
              Dashboard
            </Link>
            <Link
              href="/trading-engine"
              className={cn(
                "rounded-full px-4 py-1.5 text-sm font-medium transition-all",
                isActive("/trading-engine")
                  ? "bg-wit-green text-[#0a0a0a]"
                  : "text-wit-gray hover:text-foreground"
              )}
            >
              Trading Engine
            </Link>
          </nav>
        </div>
        <div className="flex items-center gap-2">
          <span className="relative flex h-2 w-2">
            <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-wit-green opacity-75" />
            <span className="relative inline-flex h-2 w-2 rounded-full bg-wit-green" />
          </span>
          <span className="text-sm font-medium text-foreground">Live</span>
        </div>
      </div>
    </header>
  );
}
