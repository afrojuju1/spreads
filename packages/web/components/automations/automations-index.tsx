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
  getOpsTradingHealth,
  getRuntimes,
  type AutomationRuntimeListItem,
} from "@/lib/api";
import {
  formatDate,
  formatNullableCurrency,
  formatTimestamp,
  LoadingState,
  MetricTile,
  readNumber,
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

type DetailRecord = Record<string, unknown>;

function readRecord(value: unknown): DetailRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as DetailRecord)
    : {};
}

function readRecordList(value: unknown): DetailRecord[] {
  return Array.isArray(value)
    ? value
        .filter((item) => typeof item === "object" && item !== null && !Array.isArray(item))
        .map((item) => item as DetailRecord)
    : [];
}

function humanizeToken(value: unknown, fallback = "unknown"): string {
  return readString(value, fallback).replaceAll("_", " ");
}

function proofTone(value: unknown): string {
  switch (readString(value, "unknown")) {
    case "proved":
      return "border-emerald-200 bg-emerald-100 text-emerald-900 dark:border-emerald-900/80 dark:bg-emerald-950/55 dark:text-emerald-100";
    case "proved_with_direct_close":
    case "open_observed":
      return "border-amber-200 bg-amber-100 text-amber-900 dark:border-amber-900/80 dark:bg-amber-950/55 dark:text-amber-100";
    case "not_observed":
      return "border-border/70 bg-background text-muted-foreground";
    default:
      return "border-rose-200 bg-rose-100 text-rose-900 dark:border-rose-900/80 dark:bg-rose-950/55 dark:text-rose-100";
  }
}

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
  const tradingHealthQuery = useQuery({
    queryKey: ["ops-trading-health"],
    queryFn: getOpsTradingHealth,
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
  const tradingHealth = tradingHealthQuery.data;
  const tradingSummary = readRecord(tradingHealth?.summary);
  const tradingDetails = readRecord(tradingHealth?.details);
  const nautilusLifecycle = readRecord(tradingDetails.nautilus_lifecycle);
  const lifecycleSummary = readRecord(nautilusLifecycle.summary);
  const proofByFamily = readRecord(nautilusLifecycle.proof_by_family);
  const proofRows = Object.entries(proofByFamily).map(([family, value]) => ({
    family,
    ...readRecord(value),
  }));
  const directOrUnsetCloseCount = proofRows.reduce(
    (total, row) => total + readNumber(row.direct_or_unset_close_count),
    0,
  );
  const recentAttempts = readRecordList(nautilusLifecycle.recent_attempts);
  const missingCloseRuntimeCount = readNumber(
    lifecycleSummary.missing_close_runtime_count,
    readNumber(tradingSummary.nautilus_missing_close_runtime_count),
  );
  const fullLifecycleCount = readNumber(
    lifecycleSummary.nautilus_full_lifecycle_count,
    readNumber(tradingSummary.nautilus_full_lifecycle_count),
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
        title="Nautilus Lifecycle Proof"
        description="Runtime handoff, broker order/fill, session position, and close proof by strategy family."
      >
        {tradingHealthQuery.isError ? (
          <div className="app-tone-error rounded-2xl border px-4 py-3 text-sm">
            Nautilus lifecycle proof could not be loaded.
          </div>
        ) : null}

        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
          <MetricTile
            label="Bridge"
            value={readString(tradingSummary.nautilus_bridge_status, nautilusStatus)}
            note={`${readNumber(tradingSummary.nautilus_entry_automation_count, nautilusRuntime?.entry_automation_count ?? 0)} routed entries`}
          />
          <MetricTile
            label="Attempts"
            value={String(readNumber(lifecycleSummary.attempt_count))}
            note={`${readNumber(lifecycleSummary.nautilus_attempt_count)} Nautilus`}
          />
          <MetricTile
            label="Full Lifecycle"
            value={String(fullLifecycleCount)}
            note={`${readNumber(lifecycleSummary.closed_matched_position_count)} closed and matched`}
          />
          <MetricTile
            label="Legacy Close"
            value={String(directOrUnsetCloseCount)}
            note={`${missingCloseRuntimeCount} missing runtime proofs`}
          />
          <MetricTile
            label="Bridge Payload"
            value={String(readNumber(lifecycleSummary.bridge_payload_count))}
            note={`${readNumber(lifecycleSummary.alpaca_order_count)} orders · ${readNumber(lifecycleSummary.alpaca_fill_count)} fills`}
          />
        </div>

        <div className="mt-4 grid gap-4 xl:grid-cols-[1.05fr_1fr]">
          <div className="min-w-0">
            <div className="mb-2 text-sm font-medium">Proof by strategy family</div>
            <div className="divide-y divide-border/70 rounded-lg border border-border/70">
              {proofRows.length ? (
                proofRows.map((row) => (
                  <div
                    key={readString(row.family)}
                    className="grid gap-3 px-3 py-2 text-sm lg:grid-cols-[minmax(130px,1.2fr)_minmax(120px,1fr)_minmax(150px,1.2fr)]"
                  >
                    <div className="min-w-0">
                      <div className="truncate font-medium">
                        {humanizeToken(row.family)}
                      </div>
                      <div className="truncate text-xs text-muted-foreground">
                        {readString(row.market_date)}
                      </div>
                    </div>
                    <div>
                      <Badge
                        variant="outline"
                        className={proofTone(row.status)}
                      >
                        {humanizeToken(row.status)}
                      </Badge>
                    </div>
                    <div className="grid grid-cols-2 gap-x-3 gap-y-1 font-mono text-[11px] text-muted-foreground">
                      <span>open {readNumber(row.open_count)}</span>
                      <span>closed {readNumber(row.closed_matched_count)}</span>
                      <span>naut {readNumber(row.nautilus_close_count)}</span>
                      <span>legacy {readNumber(row.direct_or_unset_close_count)}</span>
                    </div>
                  </div>
                ))
              ) : (
                <div className="px-3 py-3 text-sm text-muted-foreground">
                  No Nautilus strategy-family proof is available yet.
                </div>
              )}
            </div>
          </div>

          <div className="min-w-0">
            <div className="mb-2 text-sm font-medium">Recent lifecycle attempts</div>
            <div className="divide-y divide-border/70 rounded-lg border border-border/70">
              {recentAttempts.length ? (
                recentAttempts.slice(0, 6).map((row) => (
                  <div
                    key={readString(row.execution_attempt_id)}
                    className="grid gap-3 px-3 py-2 text-sm lg:grid-cols-[minmax(80px,0.8fr)_minmax(130px,1fr)_minmax(140px,1.2fr)]"
                  >
                    <div className="min-w-0">
                      <div className="truncate font-medium">
                        {readString(row.underlying_symbol)}
                      </div>
                      <div className="truncate text-xs text-muted-foreground">
                        {humanizeToken(row.trade_intent)}
                      </div>
                    </div>
                    <div className="min-w-0 text-xs text-muted-foreground">
                      <div className="truncate">
                        {humanizeToken(row.execution_runtime)} →{" "}
                        {humanizeToken(row.expected_runtime)}
                      </div>
                      <div className="truncate">
                        {readNumber(row.alpaca_order_count)}/
                        {readNumber(row.alpaca_fill_count)} orders/fills
                      </div>
                    </div>
                    <div className="min-w-0 text-xs text-muted-foreground">
                      <div className="truncate">
                        {humanizeToken(row.attempt_status)} ·{" "}
                        {humanizeToken(row.session_position_status)}
                      </div>
                      <div className="truncate">
                        {readString(row.bridge_status, "bridge pending")} ·{" "}
                        {formatTimestamp(readString(row.requested_at, ""))}
                      </div>
                    </div>
                    {row.missing_close_runtime ? (
                      <Badge variant="outline" className="w-fit border-amber-200 bg-amber-100 text-amber-900 dark:border-amber-900/80 dark:bg-amber-950/55 dark:text-amber-100">
                        legacy direct close
                      </Badge>
                    ) : null}
                  </div>
                ))
              ) : (
                <div className="px-3 py-3 text-sm text-muted-foreground">
                  No Nautilus attempts have been observed for today.
                </div>
              )}
            </div>
          </div>
        </div>
      </SectionSurface>

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
