"use client";

import { Activity, PanelLeftClose, PanelLeftOpen } from "lucide-react";

import { LayoutNav } from "@/components/layout-nav";
import { useLayoutChrome } from "@/components/providers";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { cn } from "@/lib/utils";

export function LayoutSidebar() {
  const {
    layoutNavOpen,
    mobileLayoutNavOpen,
    setMobileLayoutNavOpen,
    toggleLayoutNav,
  } = useLayoutChrome();

  return (
    <>
      <aside
        className={cn(
          "sticky top-0 hidden h-dvh shrink-0 border-r border-border/70 bg-sidebar/85 backdrop-blur-xl lg:flex lg:flex-col",
          "transition-[width,padding] duration-200 ease-out",
          layoutNavOpen ? "w-[15rem] px-3 py-4" : "w-[5rem] px-2 py-4",
        )}
      >
        <div
          className={cn(
            "flex items-center",
            layoutNavOpen ? "justify-between gap-3" : "justify-center gap-1",
          )}
        >
          <div
            className={cn(
              "brand-mark flex shrink-0 items-center justify-center",
              layoutNavOpen ? "size-9" : "size-8",
            )}
          >
            <Activity />
          </div>
          {layoutNavOpen ? (
            <div className="min-w-0 flex-1 truncate text-sm font-medium tracking-[0.01em]">
              spreads
            </div>
          ) : null}
          <Button
            type="button"
            variant="ghost"
            size={layoutNavOpen ? "icon-sm" : "icon-xs"}
            className="text-muted-foreground hover:text-foreground"
            aria-label={layoutNavOpen ? "Collapse navigation" : "Expand navigation"}
            title={layoutNavOpen ? "Collapse navigation" : "Expand navigation"}
            onClick={toggleLayoutNav}
          >
            {layoutNavOpen ? <PanelLeftClose /> : <PanelLeftOpen />}
          </Button>
        </div>

        <div className="mt-6 flex-1">
          <LayoutNav compact={!layoutNavOpen} />
        </div>
      </aside>

      <Sheet open={mobileLayoutNavOpen} onOpenChange={setMobileLayoutNavOpen}>
        <SheetContent
          side="left"
          className="w-[18rem] border-r border-border/70 bg-sidebar/96 p-0 sm:max-w-none"
        >
          <SheetHeader className="sr-only">
            <SheetTitle>Navigation</SheetTitle>
            <SheetDescription>
              Operator routes for automations, opportunities, positions, discovery, and account.
            </SheetDescription>
          </SheetHeader>
          <div className="flex h-full flex-col px-4 py-4">
            <div className="flex items-center gap-3">
              <div className="brand-mark flex size-9 shrink-0 items-center justify-center">
                <Activity />
              </div>
              <div className="truncate text-sm font-medium tracking-[0.01em]">
                spreads
              </div>
            </div>

            <div className="mt-6">
              <LayoutNav onNavigate={() => setMobileLayoutNavOpen(false)} />
            </div>
          </div>
        </SheetContent>
      </Sheet>
    </>
  );
}
