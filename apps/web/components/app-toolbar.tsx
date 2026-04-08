"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  Activity,
  BellRing,
  BriefcaseBusiness,
  CandlestickChart,
  Sparkles,
  Wifi,
  WifiOff,
} from "lucide-react";

import { useRealtimeActivity } from "@/components/providers";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

const PAGE_ITEMS = [
  {
    href: "/",
    label: "Sessions",
    caption: "slots + board",
    icon: CandlestickChart,
  },
  {
    href: "/generator",
    label: "Generator",
    caption: "single-symbol ideas",
    icon: Sparkles,
  },
  {
    href: "/alerts",
    label: "Alerts",
    caption: "delivery feed",
    icon: BellRing,
  },
  {
    href: "/jobs",
    label: "Jobs",
    caption: "runtime health",
    icon: BriefcaseBusiness,
  },
];

export function AppToolbar() {
  const pathname = usePathname();
  const { connectionState, latestSummary } = useRealtimeActivity();

  return (
    <div className="sticky top-0 z-40 border-b border-border/70 bg-background/88 backdrop-blur-xl">
      <div className="mx-auto flex max-w-[1680px] flex-col gap-3 px-4 py-3 lg:px-6">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="min-w-0">
            <div className="text-[11px] uppercase tracking-[0.28em] text-muted-foreground">
              Spreads operator
            </div>
            <div className="truncate text-sm text-foreground/80">
              Sessions, generator, alerts, and runtime diagnostics.
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Badge
              variant="outline"
              className={cn(
                "rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em]",
                connectionState === "connected"
                  ? "border-emerald-200 bg-emerald-100 text-emerald-900"
                  : "border-amber-200 bg-amber-100 text-amber-900",
              )}
            >
              {connectionState === "connected" ? (
                <Wifi className="size-3.5" />
              ) : (
                <WifiOff className="size-3.5" />
              )}
              {connectionState}
            </Badge>
            {latestSummary ? (
              <Badge
                variant="outline"
                className="hidden rounded-full border-border/70 bg-card/80 px-2.5 py-1 text-[11px] uppercase tracking-[0.16em] text-muted-foreground md:inline-flex"
              >
                <Activity className="size-3.5" />
                {latestSummary}
              </Badge>
            ) : null}
          </div>
        </div>
        <nav className="flex gap-2 overflow-x-auto pb-1">
          {PAGE_ITEMS.map((item) => {
            const isActive =
              item.href === "/"
                ? pathname === "/"
                : pathname === item.href || pathname.startsWith(`${item.href}/`);
            const Icon = item.icon;

            return (
              <Link
                key={item.href}
                href={item.href}
                className={cn(
                  "group min-w-[148px] rounded-2xl border px-3 py-2 transition-colors",
                  isActive
                    ? "border-stone-900 bg-stone-900 text-stone-50"
                    : "border-border/70 bg-card/80 text-foreground hover:bg-accent/60",
                )}
              >
                <div className="flex items-center gap-2">
                  <Icon className="size-4 shrink-0" />
                  <span className="text-sm font-medium">{item.label}</span>
                </div>
                <div
                  className={cn(
                    "mt-1 text-[11px] uppercase tracking-[0.18em]",
                    isActive ? "text-stone-300" : "text-muted-foreground",
                  )}
                >
                  {item.caption}
                </div>
              </Link>
            );
          })}
        </nav>
      </div>
    </div>
  );
}
