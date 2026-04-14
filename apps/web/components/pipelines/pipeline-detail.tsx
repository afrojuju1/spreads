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
  TriangleAlert,
} from "lucide-react";
import { useMemo, useState } from "react";

import { DataTable } from "@/components/data-table";
import {
  buildPipelineHref,
  closePosition,
  executeOpportunity,
  getPipelineDetail,
  getPipelines,
  type ReplayDeploymentSlice,
  type ReplayGroup,
  refreshExecution,
  type LiveCandidate,
  type PipelineListItem,
  type SessionPortfolioPosition,
} from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
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

type DeploymentSource = "allocator_selected" | "actual_deployed";

type DeploymentBucketKey =
  | "by_entry_return_on_risk_bucket"
  | "by_midpoint_credit_bucket"
  | "by_width_bucket"
  | "by_dte_bucket";

type ReplayBucketRow = ReplayGroup;

type DeploymentSummaryCardProps = {
  title: string;
  slice: ReplayDeploymentSlice | null | undefined;
  note: string;
};

type DeploymentQualityPanelProps = {
  title: string;
  description: string;
  warnings: string[];
  allocatorSlice: ReplayDeploymentSlice | null | undefined;
  actualSlice: ReplayDeploymentSlice | null | undefined;
};

const deploymentSourceOptions: Array<{
  value: DeploymentSource;
  label: string;
}> = [
  { value: "allocator_selected", label: "Allocator selected" },
  { value: "actual_deployed", label: "Actual deployed" },
];

const deploymentBucketDefinitions: Array<{
  key: DeploymentBucketKey;
  title: string;
  description: string;
}> = [
  {
    key: "by_entry_return_on_risk_bucket",
    title: "Entry RoR",
    description: "Bucketed by scanned return on risk at entry.",
  },
  {
    key: "by_midpoint_credit_bucket",
    title: "Credit",
    description: "Bucketed by midpoint entry credit.",
  },
  {
    key: "by_width_bucket",
    title: "Width",
    description: "Bucketed by spread width.",
  },
  {
    key: "by_dte_bucket",
    title: "DTE",
    description: "Bucketed by days to expiration.",
  },
];

function formatReturnOnRisk(value: number | null | undefined): string {
  return value == null ? "—" : `${(value * 100).toFixed(1)}%`;
}

function deploymentSliceCount(slice: ReplayDeploymentSlice | null | undefined): number {
  return readNumber(slice?.count);
}

function deploymentSummaryNote(
  slice: ReplayDeploymentSlice | null | undefined,
  fallback: string,
): string {
  if (!slice) {
    return fallback;
  }
  const coverage = slice.coverage_rate;
  if (coverage == null) {
    return fallback;
  }
  return `${(coverage * 100).toFixed(0)}% matched`;
}

function deploymentBucketColumns(): ColumnDef<ReplayBucketRow>[] {
  return [
    {
      accessorKey: "group_value",
      header: "Bucket",
      cell: ({ getValue }) => (
        <span className="font-mono text-[12px]">{String(getValue())}</span>
      ),
    },
    {
      accessorKey: "count",
      header: "Count",
      cell: ({ getValue }) => (
        <span className="font-mono">{formatQuantity(Number(getValue()))}</span>
      ),
    },
    {
      accessorKey: "coverage_rate",
      header: "Coverage",
      cell: ({ getValue }) => formatReturnOnRisk(getValue() as number | null | undefined),
    },
    {
      accessorKey: "pooled_estimated_close_return_on_risk",
      header: "Modeled Close",
      cell: ({ getValue }) => formatReturnOnRisk(getValue() as number | null | undefined),
    },
    {
      accessorKey: "pooled_actual_net_return_on_risk",
      header: "Actual",
      cell: ({ getValue }) => formatReturnOnRisk(getValue() as number | null | undefined),
    },
  ];
}

function DeploymentSummaryCard({
  title,
  slice,
  note,
}: DeploymentSummaryCardProps) {
  const count = deploymentSliceCount(slice);

  return (
    <div className="rounded-2xl border border-border/70 bg-card/70 p-4">
      <div className="text-xs uppercase tracking-[0.16em] text-muted-foreground">
        {title}
      </div>
      <div className="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <MetricTile
          label="Count"
          value={String(count)}
          note={note}
        />
        <MetricTile
          label="Modeled Close"
          value={formatReturnOnRisk(slice?.pooled_estimated_close_return_on_risk)}
          note="pooled pnl / max loss"
        />
        <MetricTile
          label="Modeled Final"
          value={formatReturnOnRisk(slice?.pooled_estimated_final_return_on_risk)}
          note="pooled pnl / max loss"
        />
        <MetricTile
          label="Actual"
          value={formatReturnOnRisk(slice?.pooled_actual_net_return_on_risk)}
          note="pooled pnl / max loss"
        />
      </div>
    </div>
  );
}

function DeploymentQualityPanel({
  title,
  description,
  warnings,
  allocatorSlice,
  actualSlice,
}: DeploymentQualityPanelProps) {
  const defaultSource =
    deploymentSliceCount(allocatorSlice) > 0
      ? "allocator_selected"
      : "actual_deployed";
  const [selectedSource, setSelectedSource] =
    useState<DeploymentSource>(defaultSource);
  const selectedSlice =
    selectedSource === "actual_deployed" ? actualSlice : allocatorSlice;
  const bucketColumns = useMemo(() => deploymentBucketColumns(), []);

  return (
    <div className="rounded-2xl border border-border/70 bg-card/60 p-4">
      <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
        <div>
          <div className="text-sm font-semibold">{title}</div>
          <div className="mt-1 text-sm text-muted-foreground">{description}</div>
        </div>
        <Select
          value={selectedSource}
          onValueChange={(value) => setSelectedSource(value as DeploymentSource)}
        >
          <SelectTrigger size="sm" className="w-[180px]">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {deploymentSourceOptions.map((option) => (
              <SelectItem key={option.value} value={option.value}>
                {option.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="mt-4 grid gap-4">
        <DeploymentSummaryCard
          title="Allocator selected"
          slice={allocatorSlice}
          note={deploymentSummaryNote(
            allocatorSlice,
            "Replay-selected opportunities",
          )}
        />
        <DeploymentSummaryCard
          title="Actual deployed"
          slice={actualSlice}
          note={deploymentSummaryNote(
            actualSlice,
            "Filled positions matched to replay",
          )}
        />
      </div>

      {warnings.length ? (
        <div className="mt-4 space-y-2">
          {warnings.map((warning) => (
            <div
              key={warning}
              className="rounded-xl border border-amber-300/70 bg-amber-100/80 px-3 py-2 text-sm text-amber-950 dark:border-amber-900/70 dark:bg-amber-950/40 dark:text-amber-100"
            >
              <div className="flex items-start gap-2">
                <TriangleAlert className="mt-0.5 size-4 shrink-0" />
                <span>{warning}</span>
              </div>
            </div>
          ))}
        </div>
      ) : null}

      <div className="mt-4 grid gap-4 xl:grid-cols-2">
        {deploymentBucketDefinitions.map((bucket) => (
          <div
            key={bucket.key}
            className="rounded-2xl border border-border/70 bg-background/70 p-4"
          >
            <div className="text-sm font-semibold">{bucket.title}</div>
            <div className="mt-1 text-xs text-muted-foreground">
              {bucket.description}
            </div>
            <div className="mt-3">
              <DataTable
                columns={bucketColumns}
                data={(selectedSlice?.[bucket.key] ?? []) as ReplayBucketRow[]}
                emptyMessage="No replay rows were available for this bucket."
                getRowId={(row) => `${bucket.key}:${row.group_value}`}
                pageSize={6}
              />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
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
    queryKey: ["pipelines", pipelineId, marketDate ?? "", "both"],
    queryFn: () =>
      getPipelineDetail(pipelineId, {
        marketDate,
        includeReplay: "both",
      }),
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
  const replay = detail?.replay ?? null;
  const currentDeploymentQuality =
    replay?.current?.scorecard?.deployment_quality ?? null;
  const recentDeploymentQuality =
    replay?.recent?.aggregate?.deployment_quality ?? null;
  const replayWarnings = replay?.warnings ?? [];
  const currentReplayWarnings = replay?.current?.warnings ?? [];
  const recentReplayWarnings = replay?.recent?.warnings ?? [];

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
          Pipeline detail could not be loaded.
        </div>
        <Link href="/pipelines" className={buttonVariants({ variant: "outline" })}>
          Back to pipelines
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
                Pipeline workspace
              </Badge>
              <SessionStatusBadge value={detail.status} />
              <CaptureStatusBadge value={detail.latest_slot?.capture_status} />
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">
              {detail.label}
            </div>
            <div className="mt-2 text-sm text-foreground/70">
              Market date {formatDate(detail.market_date)}. Use this workspace
              to inspect the latest cycle, execute opportunities, refresh broker
              state, and manage open positions.
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
              All pipelines
            </Link>
          </div>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <MetricTile
          label="Pipeline"
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
        title="Deployment Quality"
        description="Replay-backed pooled return-on-risk for this pipeline date and the recent label sample."
      >
        {replayWarnings.length ? (
          <div className="mb-4 space-y-2">
            {replayWarnings.map((warning) => (
              <div
                key={warning}
                className="rounded-xl border border-amber-300/70 bg-amber-100/80 px-3 py-2 text-sm text-amber-950 dark:border-amber-900/70 dark:bg-amber-950/40 dark:text-amber-100"
              >
                <div className="flex items-start gap-2">
                  <TriangleAlert className="mt-0.5 size-4 shrink-0" />
                  <span>{warning}</span>
                </div>
              </div>
            ))}
          </div>
        ) : null}
        <div className="grid gap-4 xl:grid-cols-2">
          <DeploymentQualityPanel
            title="Current session"
            description={`Replay for ${formatDate(detail.market_date)}.`}
            allocatorSlice={currentDeploymentQuality?.allocator_selected}
            actualSlice={currentDeploymentQuality?.actual_deployed}
            warnings={currentReplayWarnings}
          />
          <DeploymentQualityPanel
            title="Recent label sample"
            description={`Latest ${readNumber(replay?.recent_limit ?? 20)} replayable sessions for ${detail.label}.`}
            allocatorSlice={recentDeploymentQuality?.allocator_selected}
            actualSlice={recentDeploymentQuality?.actual_deployed}
            warnings={recentReplayWarnings}
          />
        </div>
      </SectionSurface>

      <SectionSurface
        title="Pipeline Runs"
        description="Switch between pipeline dates or inspect the latest runtime cycles."
      >
        {!pipelineRows.length ? (
          <div className="text-sm text-muted-foreground">
            No persisted pipelines were found.
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
          description="These are the current promotable and monitor ideas for this pipeline date."
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
          description="Refresh broker state without leaving the pipeline workspace."
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
        description="Open and partially closed positions created by this pipeline date."
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
        description="Recent collector cycles for this pipeline."
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
