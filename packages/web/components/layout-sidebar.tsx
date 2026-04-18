"use client";

import { Activity } from "lucide-react";

import { LayoutNav } from "@/components/layout-nav";
import { useLayoutChrome, useRealtimeActivity } from "@/components/providers";
import { Badge } from "@/components/ui/badge";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { cn } from "@/lib/utils";

function ConnectionBadge({
  connected,
  label,
}: {
  connected: boolean;
  label: string;
}) {
  return (
    <Badge
      variant="outline"
      className={cn(
        "rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em]",
        connected
          ? "border-emerald-200 bg-emerald-100 text-emerald-900 dark:border-emerald-900/80 dark:bg-emerald-950/55 dark:text-emerald-100"
          : "border-amber-200 bg-amber-100 text-amber-900 dark:border-amber-900/80 dark:bg-amber-950/55 dark:text-amber-100",
      )}
    >
      {label}
    </Badge>
  );
}

export function LayoutSidebar() {
  const {
    layoutNavOpen,
    mobileLayoutNavOpen,
    setMobileLayoutNavOpen,
  } = useLayoutChrome();
  const { connectionState, latestSummary } = useRealtimeActivity();

  return (
    <>
      <aside
        className={cn(
          "sticky top-0 hidden h-dvh shrink-0 border-r border-border/70 bg-sidebar/85 backdrop-blur-xl lg:flex lg:flex-col",
          "transition-[width,padding] duration-200 ease-out",
          layoutNavOpen ? "w-[18rem] px-4 py-4" : "w-[5.5rem] px-3 py-4",
        )}
      >
        <div className={cn("flex items-start gap-3", layoutNavOpen ? "" : "justify-center")}>
          <div className="brand-mark flex size-11 shrink-0 items-center justify-center">
            <Activity />
          </div>
          <div
            className={cn(
              "min-w-0 transition-[opacity,transform,width] duration-200",
              layoutNavOpen
                ? "w-auto translate-x-0 opacity-100"
                : "w-0 -translate-x-2 overflow-hidden opacity-0",
            )}
          >
            <div className="text-[11px] uppercase tracking-[0.28em] text-muted-foreground">
              Spreads
            </div>
            <div className="mt-1 text-lg font-semibold tracking-[0.01em]">
              Operator
            </div>
            <div className="mt-1 text-sm text-foreground/70">
              Automations, discovery, positions, and broker state.
            </div>
          </div>
        </div>

        <div className="mt-6">
          <LayoutNav compact={!layoutNavOpen} />
        </div>

        <div className="mt-auto flex flex-col gap-3">
          <ConnectionBadge
            connected={connectionState === "connected"}
            label={connectionState}
          />
          <div
            className={cn(
              "rounded-2xl border border-sidebar-border/80 bg-background/70 px-3 py-3 transition-[opacity,transform,height] duration-200",
              layoutNavOpen
                ? "translate-y-0 opacity-100"
                : "pointer-events-none h-0 -translate-y-2 overflow-hidden border-transparent p-0 opacity-0",
            )}
          >
            <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
              Live summary
            </div>
            <div className="mt-2 text-sm leading-6 text-foreground/75">
              {latestSummary ?? "Waiting for the next operator event."}
            </div>
          </div>
        </div>
      </aside>

      <Sheet open={mobileLayoutNavOpen} onOpenChange={setMobileLayoutNavOpen}>
        <SheetContent
          side="left"
          className="w-[22rem] border-r border-border/70 bg-sidebar/96 p-0 sm:max-w-none"
        >
          <SheetHeader className="border-b border-border/70 px-5 py-5">
            <SheetTitle>Navigation</SheetTitle>
            <SheetDescription>
              Automations, discovery, positions, and broker state.
            </SheetDescription>
          </SheetHeader>
          <div className="flex h-full flex-col px-5 py-5">
            <div className="flex items-start gap-3">
              <div className="brand-mark flex size-11 shrink-0 items-center justify-center">
                <Activity />
              </div>
              <div className="min-w-0">
                <div className="text-[11px] uppercase tracking-[0.28em] text-muted-foreground">
                  Spreads
                </div>
                <div className="mt-1 text-lg font-semibold tracking-[0.01em]">
                  Operator
                </div>
                <div className="mt-1 text-sm text-foreground/70">
                  One layout for automation control, discovery diagnostics, open
                  risk, and broker state.
                </div>
              </div>
            </div>

            <div className="mt-6">
              <LayoutNav onNavigate={() => setMobileLayoutNavOpen(false)} />
            </div>

            <div className="mt-auto flex flex-col gap-3">
              <ConnectionBadge
                connected={connectionState === "connected"}
                label={connectionState}
              />
              <div className="rounded-2xl border border-sidebar-border/80 bg-background/70 px-3 py-3">
                <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                  Live summary
                </div>
                <div className="mt-2 text-sm leading-6 text-foreground/75">
                  {latestSummary ?? "Waiting for the next operator event."}
                </div>
              </div>
            </div>
          </div>
        </SheetContent>
      </Sheet>
    </>
  );
}
