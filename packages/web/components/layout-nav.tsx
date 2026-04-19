"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  Activity,
  Bot,
  BriefcaseBusiness,
  CandlestickChart,
  Radar,
} from "lucide-react";

import { cn } from "@/lib/utils";

export const LAYOUT_NAV_ITEMS = [
  {
    href: "/automations",
    label: "Automations",
    caption: "runtime control",
    icon: Bot,
  },
  {
    href: "/pipelines",
    label: "Discovery",
    caption: "collector sessions",
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
    <nav className={cn("flex flex-col gap-1", compact && "items-center")}>
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
            aria-label={compact ? item.label : undefined}
            className={cn(
              "group relative text-sm transition-colors",
              compact
                ? "mx-auto grid size-11 place-items-center rounded-2xl"
                : "flex min-h-11 items-center gap-3 rounded-xl px-3.5",
              isActive
                ? "bg-sidebar-accent/90 text-foreground"
                : "text-muted-foreground hover:bg-sidebar-accent/55 hover:text-foreground",
            )}
            title={compact ? item.label : undefined}
          >
            {compact ? (
              <Icon
                className={cn(
                  "size-4 shrink-0",
                  isActive ? "text-foreground" : "text-current",
                )}
              />
            ) : (
              <>
              <span
                className={cn(
                  "absolute left-1 top-1/2 h-5 w-0.5 -translate-y-1/2 rounded-full transition-opacity",
                  isActive ? "opacity-100" : "opacity-0 group-hover:opacity-40",
                )}
              />
              <Icon
                className={cn(
                  "size-4 shrink-0",
                  isActive ? "text-foreground" : "text-current",
                )}
              />
              <div className="min-w-0 flex-1">
                <div className="truncate font-medium tracking-[0.01em]">
                  {item.label}
                </div>
              </div>
              </>
            )}
          </Link>
        );
      })}
    </nav>
  );
}
