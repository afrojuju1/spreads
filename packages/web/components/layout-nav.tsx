"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Activity, BriefcaseBusiness, CandlestickChart, Radar } from "lucide-react";

import { cn } from "@/lib/utils";

export const LAYOUT_NAV_ITEMS = [
  {
    href: "/pipelines",
    label: "Pipelines",
    caption: "runtime control",
    icon: CandlestickChart,
  },
  {
    href: "/opportunities",
    label: "Opportunities",
    caption: "trade ideas",
    icon: Radar,
  },
  {
    href: "/positions",
    label: "Positions",
    caption: "open risk",
    icon: BriefcaseBusiness,
  },
  {
    href: "/account",
    label: "Account",
    caption: "broker state",
    icon: Activity,
  },
];

export function resolveActiveLayoutItem(pathname: string) {
  if (pathname === "/sessions" || pathname.startsWith("/sessions/")) {
    return {
      href: "/sessions",
      label: "Sessions",
      caption: "compatibility history",
      icon: CandlestickChart,
    };
  }
  return (
    LAYOUT_NAV_ITEMS.find((item) =>
      pathname === item.href || pathname.startsWith(`${item.href}/`),
    ) ?? LAYOUT_NAV_ITEMS[0]
  );
}

export function LayoutNav({
  compact = false,
  onNavigate,
}: {
  compact?: boolean;
  onNavigate?: () => void;
}) {
  const pathname = usePathname();

  return (
    <nav className="flex flex-col gap-1.5">
      {LAYOUT_NAV_ITEMS.map((item) => {
        const isActive =
          pathname === item.href || pathname.startsWith(`${item.href}/`);
        const Icon = item.icon;

        return (
          <Link
            key={item.href}
            href={item.href}
            onClick={onNavigate}
            aria-current={isActive ? "page" : undefined}
            className={cn(
              "group flex items-center gap-3 rounded-2xl border transition-all",
              compact ? "justify-center px-2 py-3" : "px-3 py-3",
              isActive
                ? "border-primary bg-primary text-primary-foreground shadow-[0_18px_48px_-34px_rgba(28,25,23,0.55)] dark:shadow-[0_18px_48px_-34px_rgba(0,0,0,0.78)]"
                : "border-border/70 bg-background/70 text-foreground hover:border-border hover:bg-accent/60",
            )}
            title={compact ? item.label : undefined}
          >
            <div
              className={cn(
                "flex items-center justify-center rounded-xl",
                isActive
                  ? "bg-primary-foreground/12 text-primary-foreground"
                  : "bg-accent/80 text-foreground/80 group-hover:text-foreground",
                compact ? "size-10" : "size-9",
              )}
            >
              <Icon />
            </div>
            <div
              className={cn(
                "min-w-0 flex-1 transition-[opacity,transform,width] duration-200",
                compact
                  ? "w-0 -translate-x-2 overflow-hidden opacity-0"
                  : "w-auto translate-x-0 opacity-100",
              )}
            >
              <div className="text-sm font-medium tracking-[0.01em]">
                {item.label}
              </div>
              <div
                className={cn(
                  "mt-1 text-[11px] uppercase tracking-[0.18em]",
                  isActive
                    ? "text-primary-foreground/70"
                    : "text-muted-foreground",
                )}
              >
                {item.caption}
              </div>
            </div>
          </Link>
        );
      })}
    </nav>
  );
}
