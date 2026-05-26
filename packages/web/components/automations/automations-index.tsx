"use client";

import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";
import { Bot, RefreshCw, Rows3 } from "lucide-react";

import { DataTable } from "@/components/data-table";
import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import {
  buildRuntimeHref,
  buildPipelineHref,
  getExecutionRuntimes,
  getRuntimes,
  type AutomationRuntimeListItem,
} from "@/lib/api";
import {
  formatDate,
  formatNullableCurrency,
  LoadingState,
  MetricTile,
  readString,
  SectionSurface,
} from "@/components/operator/operator-primitives";

type AutomationListRow = {
  id: string;
  botId: string;
  automationId: string;
  botName: string;
  strategyFamily: string;
  automationType: string;
  executionMode: string;
  executionRuntime: string;
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
    executionRuntime: readString(automation.execution_runtime, "alpaca_direct"),
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
    header: "Runtime",
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
        <div className="font-mono text-[11px] text-foreground/75">
          {row.original.executionRuntime}
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
        href={buildRuntimeHref(
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
  const runtimesQuery = useQuery({
    queryKey: ["runtimes"],
    queryFn: () => getRuntimes({ limit: 120 }),
  });
  const executionRuntimesQuery = useQuery({
    queryKey: ["execution-runtimes"],
    queryFn: getExecutionRuntimes,
  });

  if (runtimesQuery.isLoading) {
    return <LoadingState />;
  }

  const automations = runtimesQuery.data?.automations ?? [];
  const automationRows = buildAutomationRows(automations);
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
  const nautilusRuntime = executionRuntimesQuery.data?.runtimes.find(
    (row) => row.runtime === "nautilus",
  );
  const nautilusStatus = readString(nautilusRuntime?.status, "unknown");

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
                Runtime
              </Badge>
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">
              Runtime catalog
            </div>
            <div className="mt-2 text-sm text-foreground/70">
              Inspect bot-owned runtime configurations when you need schedule,
              caps, recent decisions, or linked diagnostics outside a specific
              opportunity or position.
            </div>
          </div>
          <Button
            type="button"
            variant="outline"
            onClick={() => void runtimesQuery.refetch()}
          >
            <RefreshCw data-icon="inline-start" />
            Refresh
          </Button>
        </div>
      </div>

      {runtimesQuery.isError ? (
        <div className="app-tone-error rounded-2xl border px-4 py-3 text-sm">
          Runtime catalog could not be loaded.
        </div>
      ) : null}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <MetricTile
          label="Runtimes"
          value={String(automations.length)}
          note="Active runtime configs"
        />
        <MetricTile
          label="Live opportunities"
          value={String(liveOpportunityTotal)}
          note="Across all runtimes"
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
          label="Nautilus"
          value={nautilusStatus}
          note={`${nautilusRuntime?.entry_automation_count ?? 0} routed entries`}
        />
      </div>

      <SectionSurface
        title="Runtime List"
        description="Open a runtime to inspect recent runs, linked diagnostics, opportunities, execution intents, and positions."
      >
        {!automationRows.length ? (
          <div className="flex flex-col items-center justify-center gap-3 px-6 py-16 text-center">
            <Rows3 className="size-10 text-muted-foreground" />
            <div className="text-lg font-medium">No runtimes found</div>
            <div className="max-w-[34rem] text-sm text-muted-foreground">
              Active bot-owned runtimes will appear here after the configured
              runtime set is loaded.
            </div>
          </div>
        ) : (
          <DataTable
            columns={AUTOMATION_COLUMNS}
            data={automationRows}
            getRowId={(row) => row.id}
            emptyMessage="No runtimes matched the current query."
          />
        )}
      </SectionSurface>
    </div>
  );
}
