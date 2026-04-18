"use client";

import Link from "next/link";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";
import {
  Radar,
  RefreshCw,
  RotateCw,
  Rows3,
  ShieldAlert,
} from "lucide-react";
import { useMemo } from "react";

import { DataTable } from "@/components/data-table";
import {
  buildPipelineHref,
  closePosition,
  executeOpportunity,
  getPipelineDetail,
  getPipelines,
  refreshExecution,
  type LiveCandidate,
  type PipelineListItem,
  type SessionPortfolioPosition,
} from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import {
  AutoExecutionStatusBadge,
  CaptureStatusBadge,
  ExecutionStatusBadge,
  formatDate,
  formatNullableCurrency,
  formatQuantity,
  formatSignedCurrency,
  formatTimestamp,
  LoadingState,
  MetricTile,
  readNumber,
  readString,
  SectionSurface,
  SessionStatusBadge,
  TradeabilityBadge,
} from "@/components/sessions/workspace-primitives";

type PipelineDetailPageContentProps = {
  pipelineId: string;
  marketDate?: string;
};

type OpportunityRow = {
  id: string;
  symbol: string;
  strategy: string;
  state: string;
  score: number;
  credit: number;
  opportunityId: string | null;
};

type ExecutionRow = {
  id: string;
  symbol: string;
  strategy: string;
  intent: string;
  status: string;
  requestedAt: string;
};

type PositionRow = {
  id: string;
  symbol: string;
  strategy: string;
  status: string;
  remainingQuantity: number;
  realizedPnl: number | null | undefined;
  unrealizedPnl: number | null | undefined;
};

function autoExecutionTarget(summary: Record<string, unknown> | null | undefined): string {
  const symbol = readString(summary?.selected_symbol, "");
  if (symbol) {
    return symbol;
  }
  return readString(summary?.selected_opportunity_id ?? summary?.top_opportunity_id, "—");
}

function autoExecutionTargetNote(summary: Record<string, unknown> | null | undefined): string {
  const strategy = readString(summary?.selected_strategy_family, "");
  if (strategy) {
    return strategy.replaceAll("_", " ");
  }
  return readString(summary?.decision_reason ?? summary?.reason, "No opportunity selected");
}

function autoExecutionBlockers(summary: Record<string, unknown> | null | undefined): string {
  const blockers = Array.isArray(summary?.execution_blockers)
    ? summary.execution_blockers.map((value) => String(value)).filter(Boolean)
    : [];
  if (!blockers.length) {
    return readString(summary?.message, "No blockers recorded.");
  }
  return blockers.join(", ");
}

function buildOpportunityRows(opportunities: LiveCandidate[]): OpportunityRow[] {
  return opportunities.map((row) => ({
    id: String(row.candidate_id),
    symbol: readString(row.underlying_symbol, "—"),
    strategy: row.strategy,
    state: row.selection_state,
    score: readNumber(row.quality_score),
    credit: readNumber(row.midpoint_credit),
    opportunityId:
      typeof row.opportunity_id === "string" && row.opportunity_id
        ? row.opportunity_id
        : null,
  }));
}

function buildExecutionRows(
  executions: {
    execution_attempt_id: string;
    underlying_symbol: string;
    strategy: string;
    trade_intent: string;
    status: string;
    requested_at: string;
  }[],
): ExecutionRow[] {
  return executions.map((row) => ({
    id: row.execution_attempt_id,
    symbol: row.underlying_symbol,
    strategy: row.strategy,
    intent: row.trade_intent,
    status: row.status,
    requestedAt: row.requested_at,
  }));
}

function buildPositionRows(
  positions: SessionPortfolioPosition[],
): PositionRow[] {
  return positions.map((row) => ({
    id: row.position_id,
    symbol: row.underlying_symbol,
    strategy: row.strategy,
    status: row.position_status,
    remainingQuantity: readNumber(row.remaining_quantity),
    realizedPnl: row.realized_pnl,
    unrealizedPnl: row.unrealized_pnl,
  }));
}

export function PipelineDetailPageContent({
  pipelineId,
  marketDate,
}: PipelineDetailPageContentProps) {
  const queryClient = useQueryClient();
  const pipelinesQuery = useQuery({
    queryKey: ["pipelines"],
    queryFn: () => getPipelines({ limit: 120 }),
  });
  const detailQuery = useQuery({
    queryKey: ["pipelines", pipelineId, marketDate ?? ""],
    queryFn: () => getPipelineDetail(pipelineId, { marketDate }),
  });

  const executeMutation = useMutation({
    mutationFn: (opportunityId: string) => executeOpportunity(opportunityId, {}),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["pipelines"] }),
        queryClient.invalidateQueries({ queryKey: ["pipelines", pipelineId] }),
        queryClient.invalidateQueries({ queryKey: ["opportunities"] }),
        queryClient.invalidateQueries({ queryKey: ["positions"] }),
      ]);
    },
  });

  const closeMutation = useMutation({
    mutationFn: (positionId: string) => closePosition(positionId, {}),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["pipelines"] }),
        queryClient.invalidateQueries({ queryKey: ["pipelines", pipelineId] }),
        queryClient.invalidateQueries({ queryKey: ["positions"] }),
      ]);
    },
  });

  const refreshMutation = useMutation({
    mutationFn: (executionAttemptId: string) => refreshExecution(executionAttemptId),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["pipelines"] }),
        queryClient.invalidateQueries({ queryKey: ["pipelines", pipelineId] }),
      ]);
    },
  });

  const pipelineRows = pipelinesQuery.data?.pipelines ?? [];
  const detail = detailQuery.data;
  const opportunityRows = useMemo(
    () => buildOpportunityRows(detail?.opportunities ?? []),
    [detail?.opportunities],
  );
  const executionRows = useMemo(
    () => buildExecutionRows(detail?.executions ?? []),
    [detail?.executions],
  );
  const positionRows = useMemo(
    () => buildPositionRows(detail?.portfolio?.positions ?? []),
    [detail?.portfolio?.positions],
  );

  const opportunityColumns = useMemo<ColumnDef<OpportunityRow>[]>(
    () => [
      {
        accessorKey: "symbol",
        header: "Symbol",
        cell: ({ row }) => (
          <div>
            <div className="font-semibold">{row.original.symbol}</div>
            <div className="text-xs text-muted-foreground">
              {row.original.strategy}
            </div>
          </div>
        ),
      },
      {
        accessorKey: "state",
        header: "State",
        cell: ({ getValue }) => <Badge variant="outline">{String(getValue())}</Badge>,
      },
      {
        accessorKey: "score",
        header: "Score",
        cell: ({ getValue }) => <span className="font-mono">{Number(getValue()).toFixed(1)}</span>,
      },
      {
        accessorKey: "credit",
        header: "Credit",
        cell: ({ getValue }) => <span className="font-mono">{Number(getValue()).toFixed(2)}</span>,
      },
      {
        id: "actions",
        header: "",
        cell: ({ row }) => (
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={!row.original.opportunityId || executeMutation.isPending}
            onClick={() => {
              if (row.original.opportunityId) {
                executeMutation.mutate(row.original.opportunityId);
              }
            }}
          >
            Execute
          </Button>
        ),
      },
    ],
    [executeMutation],
  );

  const executionColumns = useMemo<ColumnDef<ExecutionRow>[]>(
    () => [
      {
        accessorKey: "symbol",
        header: "Execution",
        cell: ({ row }) => (
          <div>
            <div className="font-semibold">{row.original.symbol}</div>
            <div className="text-xs text-muted-foreground">
              {row.original.strategy} · {row.original.intent}
            </div>
          </div>
        ),
      },
      {
        accessorKey: "status",
        header: "Status",
        cell: ({ getValue }) => <ExecutionStatusBadge value={String(getValue())} />,
      },
      {
        accessorKey: "requestedAt",
        header: "Requested",
        cell: ({ getValue }) => (
          <span className="font-mono text-[12px]">
            {formatTimestamp(String(getValue()))}
          </span>
        ),
      },
      {
        id: "actions",
        header: "",
        cell: ({ row }) => (
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={refreshMutation.isPending}
            onClick={() => refreshMutation.mutate(row.original.id)}
          >
            Refresh
          </Button>
        ),
      },
    ],
    [refreshMutation],
  );

  const positionColumns = useMemo<ColumnDef<PositionRow>[]>(
    () => [
      {
        accessorKey: "symbol",
        header: "Position",
        cell: ({ row }) => (
          <div>
            <div className="font-semibold">{row.original.symbol}</div>
            <div className="text-xs text-muted-foreground">
              {row.original.strategy}
            </div>
          </div>
        ),
      },
      {
        accessorKey: "status",
        header: "Status",
        cell: ({ getValue }) => <Badge variant="outline">{String(getValue())}</Badge>,
      },
      {
        accessorKey: "remainingQuantity",
        header: "Remaining",
        cell: ({ getValue }) => <span className="font-mono">{formatQuantity(Number(getValue()))}</span>,
      },
      {
        accessorKey: "realizedPnl",
        header: "Realized",
        cell: ({ getValue }) => formatSignedCurrency(getValue() as number | null | undefined),
      },
      {
        accessorKey: "unrealizedPnl",
        header: "Unrealized",
        cell: ({ getValue }) => formatSignedCurrency(getValue() as number | null | undefined),
      },
      {
        id: "actions",
        header: "",
        cell: ({ row }) => (
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={closeMutation.isPending || row.original.status === "closed"}
            onClick={() => closeMutation.mutate(row.original.id)}
          >
            Close
          </Button>
        ),
      },
    ],
    [closeMutation],
  );

  if (detailQuery.isLoading) {
    return <LoadingState />;
  }

  if (detailQuery.isError || !detail) {
    return (
      <div className="flex flex-col gap-4">
        <div className="app-tone-error rounded-2xl border px-4 py-3 text-sm">
          Discovery session detail could not be loaded.
        </div>
        <Link href="/pipelines" className={buttonVariants({ variant: "outline" })}>
          Back to discovery
        </Link>
      </div>
    );
  }

  const latestPipeline =
    pipelineRows.find((row) => row.pipeline_id === pipelineId) ?? null;

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
                <Radar data-icon="inline-start" />
                Discovery workspace
              </Badge>
              <SessionStatusBadge value={detail.status} />
              <CaptureStatusBadge
                value={readString(detail.latest_slot?.capture_status, "") || undefined}
              />
              <TradeabilityBadge value={detail.tradeability_state} />
              {detail.latest_auto_execution ? (
                <AutoExecutionStatusBadge value={detail.latest_auto_execution.status} />
              ) : null}
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">
              {detail.label}
            </div>
            <div className="mt-2 text-sm text-foreground/70">
              Market date {formatDate(detail.market_date)}. Use this workspace
              to inspect collector and cycle state first; linked automation
              decisions, executions, and positions remain available here for
              compatibility.
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            <Button
              type="button"
              variant="outline"
              onClick={() => void detailQuery.refetch()}
            >
              <RefreshCw data-icon="inline-start" />
              Refresh
            </Button>
            <Link href="/pipelines" className={buttonVariants({ variant: "outline" })}>
              All discovery sessions
            </Link>
          </div>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <MetricTile
          label="Discovery"
          value={detail.label}
          note={latestPipeline?.style_profile ?? "runtime"}
        />
        <MetricTile
          label="Promotable"
          value={String(detail.selection_counts.promotable)}
          note="Current cycle"
        />
        <MetricTile
          label="Monitor"
          value={String(detail.selection_counts.monitor)}
          note="Current cycle"
        />
        <MetricTile
          label="Open Positions"
          value={String(detail.portfolio.summary.open_position_count)}
          note={formatNullableCurrency(detail.portfolio.summary.net_pnl_total)}
        />
        <MetricTile
          label="Risk"
          value={readString(detail.risk_status)}
          note={readString(detail.risk_note)}
        />
      </div>

      <SectionSurface
        title="Latest Auto Execution"
        description="Most recent owner-plane decision linked to this discovery session."
      >
        {detail.latest_auto_execution ? (
          <div className="grid gap-4 xl:grid-cols-[0.95fr_1.05fr]">
            <div className="rounded-2xl border border-border/70 bg-background/75 p-4">
              <div className="flex flex-wrap items-center gap-2">
                <AutoExecutionStatusBadge value={detail.latest_auto_execution.status} />
                <span className="text-xs uppercase tracking-[0.16em] text-muted-foreground">
                  {readString(detail.latest_auto_execution.reason, "latest decision")}
                </span>
              </div>
              <div className="mt-3 text-lg font-medium">
                {autoExecutionTarget(detail.latest_auto_execution)}
              </div>
              <div className="mt-1 text-sm text-muted-foreground">
                {autoExecutionTargetNote(detail.latest_auto_execution)}
              </div>
              <div className="mt-3 text-sm text-foreground/80">
                {readString(detail.latest_auto_execution.message, "No auto execution result has been recorded yet.")}
              </div>
            </div>
            <div className="grid gap-3 sm:grid-cols-3">
              <MetricTile
                label="Allocation"
                value={
                  detail.latest_auto_execution.allocation_score == null
                    ? "—"
                    : formatQuantity(detail.latest_auto_execution.allocation_score)
                }
                note="portfolio-adjusted score"
              />
              <MetricTile
                label="Planner"
                value={`${readNumber(detail.latest_auto_execution.candidate_count)}/${readNumber(detail.latest_auto_execution.allocation_count)}/${readNumber(detail.latest_auto_execution.execution_intent_count)}`}
                note="candidates / allocations / intents"
              />
              <MetricTile
                label="Selected"
                value={readString(detail.latest_auto_execution.selected_opportunity_id, "—")}
                note="canonical opportunity id"
              />
              <div className="sm:col-span-3 rounded-2xl border border-border/70 bg-background/75 p-4 text-sm text-foreground/80">
                <div className="text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
                  Execution blockers
                </div>
                <div className="mt-2">{autoExecutionBlockers(detail.latest_auto_execution)}</div>
              </div>
            </div>
          </div>
        ) : (
          <div className="rounded-2xl border border-dashed border-border/70 bg-background/60 px-4 py-3 text-sm text-muted-foreground">
            No auto execution decision has been recorded for the latest slot yet.
          </div>
        )}
      </SectionSurface>

      <SectionSurface
        title="Discovery Dates"
        description="Switch between persisted discovery dates or inspect the latest runtime cycles."
      >
        {!pipelineRows.length ? (
          <div className="text-sm text-muted-foreground">
            No persisted discovery sessions were found.
          </div>
        ) : (
          <div className="flex flex-wrap gap-2">
            {pipelineRows.map((row: PipelineListItem) => (
              <Link
                key={`${row.pipeline_id}:${row.latest_market_date}`}
                href={buildPipelineHref(row.pipeline_id, row.latest_market_date)}
                className={buttonVariants({
                  variant:
                    row.pipeline_id === pipelineId &&
                    row.latest_market_date === detail.market_date
                      ? "default"
                      : "outline",
                  size: "sm",
                })}
              >
                {row.label} · {formatDate(row.latest_market_date)}
              </Link>
            ))}
          </div>
        )}
      </SectionSurface>

      <div className="grid gap-4 xl:grid-cols-2">
        <SectionSurface
          title="Opportunities"
          description="These are the linked promotable and monitor ideas for this discovery date."
        >
          {!opportunityRows.length ? (
            <div className="flex flex-col items-center justify-center gap-3 px-6 py-12 text-center">
              <Rows3 className="size-10 text-muted-foreground" />
              <div className="text-sm text-muted-foreground">
                No opportunities are available for this cycle.
              </div>
            </div>
          ) : (
            <DataTable
              columns={opportunityColumns}
              data={opportunityRows}
              getRowId={(row) => row.id}
              emptyMessage="No opportunities were available."
            />
          )}
        </SectionSurface>

        <SectionSurface
          title="Executions"
          description="Refresh broker state without leaving the discovery workspace."
        >
          {!executionRows.length ? (
            <div className="flex flex-col items-center justify-center gap-3 px-6 py-12 text-center">
              <RotateCw className="size-10 text-muted-foreground" />
              <div className="text-sm text-muted-foreground">
                No executions have been recorded yet.
              </div>
            </div>
          ) : (
            <DataTable
              columns={executionColumns}
              data={executionRows}
              getRowId={(row) => row.id}
              emptyMessage="No executions were available."
            />
          )}
        </SectionSurface>
      </div>

      <SectionSurface
        title="Positions"
        description="Open and partially closed positions linked to this discovery date."
      >
        {!positionRows.length ? (
          <div className="flex flex-col items-center justify-center gap-3 px-6 py-12 text-center">
            <ShieldAlert className="size-10 text-muted-foreground" />
            <div className="text-sm text-muted-foreground">
              No positions are open for this pipeline date.
            </div>
          </div>
        ) : (
          <DataTable
            columns={positionColumns}
            data={positionRows}
            getRowId={(row) => row.id}
            emptyMessage="No positions were available."
          />
        )}
      </SectionSurface>

      <SectionSurface
        title="Cycle Timeline"
        description="Recent collector cycles for this discovery session."
      >
        {!detail.cycles.length ? (
          <div className="text-sm text-muted-foreground">
            No pipeline cycles were recorded.
          </div>
        ) : (
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            {detail.cycles.slice(0, 6).map((row) => (
              <div
                key={String(row.cycle_id)}
                className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3"
              >
                <div className="text-sm font-medium">
                  {formatTimestamp(readString(row.generated_at, ""))}
                </div>
                <div className="mt-1 text-xs text-muted-foreground">
                  {readString(row.strategy_mode, "strategy")} ·{" "}
                  {readString(row.legacy_profile, "profile")}
                </div>
                <div className="mt-3 text-xs text-muted-foreground">
                  Symbols {readNumber((row.summary_json as Record<string, unknown> | undefined)?.candidate_count)}
                </div>
              </div>
            ))}
          </div>
        )}
      </SectionSurface>
    </div>
  );
}
