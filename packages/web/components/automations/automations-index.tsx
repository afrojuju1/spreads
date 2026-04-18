"use client";

import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";
import { Bot, RefreshCw, Rows3 } from "lucide-react";

import { DataTable } from "@/components/data-table";
import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import {
  buildAutomationHref,
  buildPipelineHref,
  getAutomations,
  type AutomationRuntimeListItem,
} from "@/lib/api";
import {
  formatDate,
  formatNullableCurrency,
  LoadingState,
  MetricTile,
  readString,
  SectionSurface,
} from "@/components/sessions/workspace-primitives";

type AutomationListRow = {
  id: string;
  botId: string;
  automationId: string;
  botName: string;
  strategyFamily: string;
  automationType: string;
  executionMode: string;
  symbols: string[];
  marketDate: string;
  latestDiscoveryLabel: string;
  latestDiscoveryPipelineId: string | null;
  latestDiscoveryDate: string | null;
  opportunityCount: number;
  liveOpportunityCount: number;
  selectedCount: number;
  intentCount: number;
  openPositionCount: number;
  dailyTotalPnl: number;
};

function buildAutomationRows(
  automations: AutomationRuntimeListItem[],
): AutomationListRow[] {
  return automations.map((automation) => ({
    id: `${automation.bot_id}:${automation.automation_id}`,
    botId: automation.bot_id,
    automationId: automation.automation_id,
    botName: automation.bot_name,
    strategyFamily: readString(automation.strategy_family, "unscoped"),
    automationType: automation.automation_type,
    executionMode: readString(automation.execution_mode, "unknown"),
    symbols: automation.symbols ?? [],
    marketDate: readString(automation.market_date, ""),
    latestDiscoveryLabel: readString(automation.latest_discovery?.label, "—"),
    latestDiscoveryPipelineId:
      automation.latest_discovery?.pipeline_id ?? null,
    latestDiscoveryDate: automation.latest_discovery?.session_date ?? null,
    opportunityCount: automation.opportunity_count ?? 0,
    liveOpportunityCount: automation.live_opportunity_count ?? 0,
    selectedCount: automation.decision_state_counts?.selected ?? 0,
    intentCount: automation.intent_count ?? 0,
    openPositionCount: automation.open_position_count ?? 0,
    dailyTotalPnl: automation.daily_total_pnl ?? 0,
  }));
}

const AUTOMATION_COLUMNS: ColumnDef<AutomationListRow>[] = [
  {
    accessorKey: "botName",
    header: "Automation",
    cell: ({ row }) => (
      <div className="min-w-[260px]">
        <div className="font-semibold">{row.original.botName}</div>
        <div className="text-xs text-muted-foreground">
          {row.original.automationId} · {row.original.strategyFamily.replaceAll("_", " ")}
        </div>
        <div className="mt-1 font-mono text-[11px] text-foreground/75">
          {row.original.symbols.length ? row.original.symbols.join(", ") : "No symbols"}
        </div>
      </div>
    ),
  },
  {
    accessorKey: "automationType",
    header: "Runtime",
    cell: ({ row }) => (
      <div className="space-y-1">
        <Badge variant="outline">{row.original.automationType}</Badge>
        <div className="text-xs text-muted-foreground">
          {row.original.executionMode.replaceAll("_", " ")}
        </div>
      </div>
    ),
  },
  {
    accessorKey: "latestDiscoveryLabel",
    header: "Discovery",
    cell: ({ row }) =>
      row.original.latestDiscoveryPipelineId ? (
        <Link
          href={buildPipelineHref(
            row.original.latestDiscoveryPipelineId,
            row.original.latestDiscoveryDate,
          )}
          className="inline-flex flex-col text-sm underline-offset-4 hover:underline"
        >
          <span>{row.original.latestDiscoveryLabel}</span>
          <span className="text-xs text-muted-foreground">
            {row.original.latestDiscoveryDate
              ? formatDate(row.original.latestDiscoveryDate)
              : "No date"}
          </span>
        </Link>
      ) : (
        <div className="text-sm text-muted-foreground">
          {row.original.latestDiscoveryLabel}
        </div>
      ),
  },
  {
    accessorKey: "opportunityCount",
    header: "Opportunities",
    cell: ({ row }) => (
      <div className="space-y-1 font-mono text-xs">
        <div>Total {row.original.opportunityCount}</div>
        <div className="text-muted-foreground">
          Live {row.original.liveOpportunityCount}
        </div>
      </div>
    ),
  },
  {
    accessorKey: "selectedCount",
    header: "Decisions",
    cell: ({ row }) => (
      <div className="space-y-1 font-mono text-xs">
        <div>Selected {row.original.selectedCount}</div>
        <div className="text-muted-foreground">
          Intents {row.original.intentCount}
        </div>
      </div>
    ),
  },
  {
    accessorKey: "openPositionCount",
    header: "Risk",
    cell: ({ row }) => (
      <div className="space-y-1">
        <div className="font-mono text-xs">
          Open {row.original.openPositionCount}
        </div>
        <div className="text-xs text-muted-foreground">
          {formatNullableCurrency(row.original.dailyTotalPnl)}
        </div>
      </div>
    ),
  },
  {
    id: "actions",
    header: "",
    cell: ({ row }) => (
      <Link
        href={buildAutomationHref(
          row.original.botId,
          row.original.automationId,
          row.original.marketDate || null,
        )}
        className={buttonVariants({ variant: "outline", size: "sm" })}
      >
        Open
      </Link>
    ),
  },
];

export function AutomationsIndexPageContent() {
  const automationsQuery = useQuery({
    queryKey: ["automations"],
    queryFn: () => getAutomations({ limit: 120 }),
  });

  if (automationsQuery.isLoading) {
    return <LoadingState />;
  }

  const automations = automationsQuery.data?.automations ?? [];
  const automationRows = buildAutomationRows(automations);
  const latestAutomation = automations[0] ?? null;
  const liveOpportunityTotal = automations.reduce(
    (total, row) => total + (row.live_opportunity_count ?? 0),
    0,
  );
  const openPositionTotal = automations.reduce(
    (total, row) => total + (row.open_position_count ?? 0),
    0,
  );
  const dailyPnlTotal = automations.reduce(
    (total, row) => total + (row.daily_total_pnl ?? 0),
    0,
  );

  return (
    <div className="flex flex-col gap-4">
      <div className="app-hero">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-2">
              <Badge
                variant="outline"
                className="rounded-full border-border/70 bg-background/80 px-2.5 py-1 text-[11px] uppercase tracking-[0.18em] text-muted-foreground"
              >
                <Bot data-icon="inline-start" />
                Automations
              </Badge>
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">
              Automation runtimes
            </div>
            <div className="mt-2 text-sm text-foreground/70">
              Operate the owner plane first: bot and automation state,
              decisions, positions, and PnL. Discovery sessions remain under
              Pipelines for collector diagnostics.
            </div>
          </div>
          <Button
            type="button"
            variant="outline"
            onClick={() => void automationsQuery.refetch()}
          >
            <RefreshCw data-icon="inline-start" />
            Refresh
          </Button>
        </div>
      </div>

      {automationsQuery.isError ? (
        <div className="app-tone-error rounded-2xl border px-4 py-3 text-sm">
          Automation runtimes could not be loaded.
        </div>
      ) : null}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <MetricTile
          label="Automations"
          value={String(automations.length)}
          note="Active runtime configs"
        />
        <MetricTile
          label="Live opportunities"
          value={String(liveOpportunityTotal)}
          note={latestAutomation?.bot_name ?? "No runtime loaded"}
        />
        <MetricTile
          label="Open positions"
          value={String(openPositionTotal)}
          note="Owner-attributed inventory"
        />
        <MetricTile
          label="Daily PnL"
          value={formatNullableCurrency(dailyPnlTotal)}
          note="Realized plus open estimate"
        />
        <MetricTile
          label="Latest discovery"
          value={latestAutomation?.latest_discovery?.label ?? "—"}
          note={
            latestAutomation?.latest_discovery?.session_date
              ? formatDate(latestAutomation.latest_discovery.session_date)
              : "No discovery linkage"
          }
        />
      </div>

      <SectionSurface
        title="Automation List"
        description="Open a runtime to inspect automation runs, linked discovery lineage, opportunities, execution intents, and positions."
      >
        {!automationRows.length ? (
          <div className="flex flex-col items-center justify-center gap-3 px-6 py-16 text-center">
            <Rows3 className="size-10 text-muted-foreground" />
            <div className="text-lg font-medium">No automations found</div>
            <div className="max-w-[34rem] text-sm text-muted-foreground">
              Active bot-owned runtimes will appear here after the configured
              automation set is loaded.
            </div>
          </div>
        ) : (
          <DataTable
            columns={AUTOMATION_COLUMNS}
            data={automationRows}
            getRowId={(row) => row.id}
            emptyMessage="No automation runtimes matched the current query."
          />
        )}
      </SectionSurface>

      <div className="grid gap-4 xl:grid-cols-2">
        <SectionSurface
          title="Primary plane"
          description="Use this surface for runtime-owned state and bot operations."
        >
          <div className="grid gap-3 sm:grid-cols-2">
            <MetricTile
              label="Decisions"
              value={String(
                automations.reduce(
                  (total, row) => total + (row.decision_count ?? 0),
                  0,
                ),
              )}
              note="Owner-scoped opportunity decisions"
            />
            <MetricTile
              label="Intents"
              value={String(
                automations.reduce(
                  (total, row) => total + (row.intent_count ?? 0),
                  0,
                ),
              )}
              note="Execution intents"
            />
          </div>
        </SectionSurface>

        <SectionSurface
          title="Supporting plane"
          description="Discovery remains visible, but it is no longer the primary operator noun."
        >
          <div className="grid gap-3 sm:grid-cols-2">
            <MetricTile
              label="Discovery sessions"
              value={String(
                new Set(
                  automations.flatMap((row) =>
                    row.latest_discovery?.pipeline_id
                      ? [row.latest_discovery.pipeline_id]
                      : [],
                  ),
                ).size,
              )}
              note="Collector-linked sessions"
            />
            <MetricTile
              label="Linked discovery dates"
              value={String(
                new Set(
                  automations.flatMap((row) =>
                    row.latest_discovery?.session_date
                      ? [row.latest_discovery.session_date]
                      : [],
                  ),
                ).size,
              )}
              note="Recent discovery coverage"
            />
          </div>
        </SectionSurface>
      </div>
    </div>
  );
}
