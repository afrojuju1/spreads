"use client";

import { usePathname } from "next/navigation";
import { Activity, PanelLeftClose, PanelLeftOpen } from "lucide-react";

import { resolveActiveLayoutItem } from "@/components/layout-nav";
import { useLayoutChrome, useRealtimeActivity } from "@/components/providers";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";

export function LayoutHeader() {
  const pathname = usePathname();
  const activeItem = resolveActiveLayoutItem(pathname);
  const { connectionState, latestSummary } = useRealtimeActivity();
  const {
    layoutNavOpen,
    toggleLayoutNav,
    setMobileLayoutNavOpen,
  } = useLayoutChrome();

  return (
    <header className="sticky top-0 z-40 border-b border-border/70 bg-background/82 backdrop-blur-xl">
      <div className="flex items-center gap-3 px-4 py-3 lg:px-6">
        <Button
          type="button"
          variant="outline"
          size="icon-sm"
          className="lg:hidden"
          aria-label="Open navigation"
          onClick={() => setMobileLayoutNavOpen(true)}
        >
          <PanelLeftOpen />
        </Button>
        <Button
          type="button"
          variant="outline"
          size="icon-sm"
          className="hidden lg:inline-flex"
          aria-label={layoutNavOpen ? "Collapse navigation" : "Expand navigation"}
          onClick={toggleLayoutNav}
        >
          {layoutNavOpen ? <PanelLeftClose /> : <PanelLeftOpen />}
        </Button>

        <div className="min-w-0 flex-1">
          <div className="text-[11px] uppercase tracking-[0.28em] text-muted-foreground">
            {activeItem.label}
          </div>
          <div className="truncate text-sm text-foreground/80">
            {activeItem.caption}
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Badge
            variant="outline"
            className="rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em]"
          >
            {connectionState}
          </Badge>
          {latestSummary ? (
            <Badge
              variant="outline"
              className="hidden rounded-full border-border/70 bg-card/80 px-2.5 py-1 text-[11px] uppercase tracking-[0.16em] text-muted-foreground xl:inline-flex"
            >
              <Activity />
              {latestSummary}
            </Badge>
          ) : null}
        </div>
      </div>
    </header>
  );
}
