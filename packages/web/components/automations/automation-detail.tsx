"use client";

import Link from "next/link";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";
import {
  Bot,
  BriefcaseBusiness,
  Radar,
  RefreshCw,
  RotateCw,
  Rows3,
} from "lucide-react";

import { DataTable } from "@/components/data-table";
import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import {
  buildOpportunitiesHref,
  buildPositionsHref,
  buildPipelineHref,
  closePosition,
  executeOpportunity,
  getAutomationDetail,
  type Opportunity,
  type Position,
} from "@/lib/api";
import {
  formatDate,
  formatNullableCurrency,
  formatQuantity,
  formatSignedCurrency,
  formatTimestamp,
  LoadingState,
  MetricTile,
  readString,
  SectionSurface,
} from "@/components/sessions/workspace-primitives";

type AutomationDetailPageContentProps = {
  botId: string;
  automationId: string;
  marketDate?: string;
};

type IntentRow = {
  id: string;
  actionType: string;
  state: string;
  createdAt: string;
  opportunityId: string;
};

type AutomationRunRow = {
  id: string;
  status: string;
  startedAt: string;
  sessionDate: string;
  opportunityCount: number;
};

function readIntentRows(rows: Record<string, unknown>[]): IntentRow[] {
  return rows.map((row, index) => ({
    id: readString(row.execution_intent_id, `intent-${index}`),
    actionType: readString(row.action_type, "unknown"),
    state: readString(row.state, "unknown"),
    createdAt: readString(row.created_at, ""),
    opportunityId: readString(row.opportunity_id, "—"),
  }));
}

function readAutomationRunRows(rows: Record<string, unknown>[]): AutomationRunRow[] {
  return rows.map((row, index) => {
    const result =
      typeof row.result === "object" && row.result !== null && !Array.isArray(row.result)
        ? (row.result as Record<string, unknown>)
        : {};
    return {
      id: readString(row.automation_run_id, `run-${index}`),
      status: readString(row.status, "unknown"),
      startedAt: readString(row.started_at, ""),
      sessionDate: readString(row.session_date, ""),
      opportunityCount:
        typeof result.opportunity_count === "number" ? result.opportunity_count : 0,
    };
  });
}

function buildOpportunityColumns(
  onExecute: (opportunityId: string) => void,
  executePending: boolean,
): ColumnDef<Opportunity>[] {
  return [
    {
      accessorKey: "underlying_symbol",
      header: "Opportunity",
      cell: ({ row }) => (
        <div className="min-w-[220px]">
          <div className="font-semibold">{row.original.underlying_symbol}</div>
          <div className="text-xs text-muted-foreground">
            {row.original.strategy_family.replaceAll("_", " ")}
          </div>
          <div className="mt-1 text-xs text-foreground/80">
            Discovery · {readString(row.original.discovery?.label, "—")}
          </div>
        </div>
      ),
    },
    {
      accessorKey: "selection_state",
      header: "Selection",
      cell: ({ row }) => (
        <div className="space-y-1">
          <Badge variant="outline">{row.original.selection_state}</Badge>
          <div className="text-xs text-muted-foreground">
            {row.original.lifecycle_state}
          </div>
        </div>
      ),
    },
    {
      accessorKey: "market_date",
      header: "Timing",
      cell: ({ row }) => (
        <div className="space-y-1">
          <div className="text-sm">{formatDate(row.original.market_date)}</div>
          <div className="text-xs text-muted-foreground">
            Rank {row.original.selection_rank ?? "—"}
          </div>
        </div>
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
          disabled={executePending}
          onClick={() => onExecute(row.original.opportunity_id)}
        >
          Execute
        </Button>
      ),
    },
  ];
}

function buildPositionColumns(
  onClose: (positionId: string) => void,
  closePending: boolean,
): ColumnDef<Position>[] {
  return [
    {
      accessorKey: "root_symbol",
      header: "Position",
      cell: ({ row }) => (
        <div className="min-w-[220px]">
          <div className="font-semibold">{row.original.root_symbol}</div>
          <div className="text-xs text-muted-foreground">
            {row.original.strategy_family}
          </div>
          <div className="mt-1 text-xs text-foreground/80">
            Discovery · {readString(row.original.discovery?.label, "—")}
          </div>
        </div>
      ),
    },
    {
      accessorKey: "position_status",
      header: "Status",
      cell: ({ getValue }) => <Badge variant="outline">{String(getValue())}</Badge>,
    },
    {
      accessorKey: "remaining_quantity",
      header: "Remaining",
      cell: ({ getValue }) => (
        <span className="font-mono">{formatQuantity(getValue() as number)}</span>
      ),
    },
    {
      accessorKey: "realized_pnl",
      header: "Realized",
      cell: ({ getValue }) =>
        formatSignedCurrency(getValue() as number | null | undefined),
    },
    {
      accessorKey: "unrealized_pnl",
      header: "Unrealized",
      cell: ({ getValue }) =>
        formatSignedCurrency(getValue() as number | null | undefined),
    },
    {
      id: "actions",
      header: "",
      cell: ({ row }) => (
        <Button
          type="button"
          size="sm"
          variant="outline"
          disabled={closePending || row.original.position_status === "closed"}
          onClick={() => onClose(row.original.position_id)}
        >
          Close
        </Button>
      ),
    },
  ];
}

const INTENT_COLUMNS: ColumnDef<IntentRow>[] = [
  {
    accessorKey: "actionType",
    header: "Intent",
    cell: ({ row }) => (
      <div>
        <div className="font-medium">{row.original.actionType}</div>
        <div className="text-xs text-muted-foreground">
          {row.original.opportunityId}
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
    accessorKey: "createdAt",
    header: "Created",
    cell: ({ getValue }) => (
      <span className="font-mono text-[12px]">
        {formatTimestamp(String(getValue()))}
      </span>
    ),
  },
];

const AUTOMATION_RUN_COLUMNS: ColumnDef<AutomationRunRow>[] = [
  {
    accessorKey: "sessionDate",
    header: "Run",
    cell: ({ row }) => (
      <div>
        <div className="font-medium">{formatDate(row.original.sessionDate)}</div>
        <div className="text-xs text-muted-foreground">
          {row.original.id}
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
    accessorKey: "opportunityCount",
    header: "Opportunities",
    cell: ({ getValue }) => <span className="font-mono">{String(getValue())}</span>,
  },
  {
    accessorKey: "startedAt",
    header: "Started",
    cell: ({ getValue }) => (
      <span className="font-mono text-[12px]">
        {formatTimestamp(String(getValue()))}
      </span>
    ),
  },
];

function DetailField({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <div className="rounded-2xl border border-border/70 bg-background/70 p-4">
      <div className="text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
        {label}
      </div>
      <div className="mt-2 text-sm text-foreground/90">{value}</div>
    </div>
  );
}

export function AutomationDetailPageContent({
  botId,
  automationId,
  marketDate,
}: AutomationDetailPageContentProps) {
  const queryClient = useQueryClient();
  const detailQuery = useQuery({
    queryKey: ["automations", botId, automationId, marketDate ?? ""],
    queryFn: () =>
      getAutomationDetail(botId, automationId, { marketDate, limit: 200 }),
  });
  const executeMutation = useMutation({
    mutationFn: (opportunityId: string) => executeOpportunity(opportunityId, {}),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["automations"] }),
        queryClient.invalidateQueries({ queryKey: ["opportunities"] }),
        queryClient.invalidateQueries({ queryKey: ["positions"] }),
        queryClient.invalidateQueries({ queryKey: ["pipelines"] }),
      ]);
    },
  });
  const closeMutation = useMutation({
    mutationFn: (positionId: string) => closePosition(positionId, {}),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["automations"] }),
        queryClient.invalidateQueries({ queryKey: ["positions"] }),
        queryClient.invalidateQueries({ queryKey: ["pipelines"] }),
      ]);
    },
  });
  const opportunityColumns = buildOpportunityColumns(
    (opportunityId) => executeMutation.mutate(opportunityId),
    executeMutation.isPending,
  );
  const positionColumns = buildPositionColumns(
    (positionId) => closeMutation.mutate(positionId),
    closeMutation.isPending,
  );

  if (detailQuery.isLoading) {
    return <LoadingState />;
  }

  const detail = detailQuery.data;
  if (detailQuery.isError || !detail) {
    return (
      <div className="flex flex-col gap-4">
        <div className="app-tone-error rounded-2xl border px-4 py-3 text-sm">
          Automation runtime detail could not be loaded.
        </div>
        <Link href="/automations" className={buttonVariants({ variant: "outline" })}>
          Back to automations
        </Link>
      </div>
    );
  }

  const intentRows = readIntentRows(
    detail.intents as Record<string, unknown>[],
  );
  const automationRunRows = readAutomationRunRows(
    detail.automation_runs as Record<string, unknown>[],
  );
  const discoveryHref = detail.latest_discovery?.pipeline_id
    ? buildPipelineHref(
        detail.latest_discovery.pipeline_id,
        detail.latest_discovery.session_date ?? detail.market_date ?? null,
      )
    : null;

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
                Automation runtime
              </Badge>
              <Badge variant="outline">{detail.automation_type}</Badge>
              <Badge variant="outline">
                {readString(detail.execution_mode, "unknown").replaceAll("_", " ")}
              </Badge>
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">
              {detail.bot_name}
            </div>
            <div className="mt-2 text-sm text-foreground/70">
              {detail.automation_id} ·{" "}
              {readString(detail.strategy_family, "unscoped").replaceAll("_", " ")}.
              Use this workspace for owner-scoped decisions, intents,
              positions, and the latest linked discovery session.
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
            <Link href="/automations" className={buttonVariants({ variant: "outline" })}>
              All automations
            </Link>
          </div>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <MetricTile
          label="Live opportunities"
          value={String(detail.live_opportunity_count)}
          note={`Total ${detail.opportunity_count}`}
        />
        <MetricTile
          label="Decisions"
          value={String(detail.decision_count)}
          note={`Selected ${detail.decision_state_counts.selected ?? 0}`}
        />
        <MetricTile
          label="Open positions"
          value={String(detail.open_position_count)}
          note={formatNullableCurrency(detail.open_unrealized_pnl)}
        />
        <MetricTile
          label="Daily PnL"
          value={formatNullableCurrency(detail.daily_total_pnl)}
          note={formatNullableCurrency(detail.daily_realized_pnl)}
        />
        <MetricTile
          label="Discovery"
          value={detail.latest_discovery?.label ?? "—"}
          note={
            detail.latest_discovery?.session_date
              ? formatDate(detail.latest_discovery.session_date)
              : "No linked session"
          }
        />
      </div>

      <SectionSurface
        title="Runtime Summary"
        description="Configuration stays owner-first, with discovery lineage linked instead of leading."
      >
        <div className="grid gap-3 lg:grid-cols-3">
          <DetailField
            label="Symbols"
            value={detail.symbols.length ? detail.symbols.join(", ") : "No symbols"}
          />
          <DetailField
            label="Schedule"
            value={JSON.stringify(detail.schedule ?? {})}
          />
          <DetailField
            label="Trigger policy"
            value={JSON.stringify(detail.trigger_policy ?? {})}
          />
          <DetailField
            label="Strategy config"
            value={readString(detail.strategy_config_id)}
          />
          <DetailField
            label="Latest automation run"
            value={
              detail.latest_automation_run?.started_at
                ? formatTimestamp(String(detail.latest_automation_run.started_at))
                : "No automation run"
            }
          />
          <DetailField
            label="Latest discovery"
            value={
              detail.latest_discovery?.label
                ? `${detail.latest_discovery.label}${detail.latest_discovery.session_date ? ` · ${detail.latest_discovery.session_date}` : ""}`
                : "No discovery linkage"
            }
          />
        </div>
        {discoveryHref ? (
          <div className="mt-4 flex flex-wrap gap-2">
            <Link href={discoveryHref} className={buttonVariants({ variant: "outline" })}>
              Open linked discovery session
            </Link>
            <Link
              href={buildOpportunitiesHref({
                marketDate: detail.market_date ?? null,
                botId: detail.bot_id,
                automationId: detail.automation_id,
                strategyConfigId: detail.strategy_config_id ?? null,
              })}
              className={buttonVariants({ variant: "outline" })}
            >
              Open filtered opportunities
            </Link>
            <Link
              href={buildPositionsHref({
                marketDate: detail.market_date ?? null,
                botId: detail.bot_id,
                automationId: detail.automation_id,
                strategyConfigId: detail.strategy_config_id ?? null,
              })}
              className={buttonVariants({ variant: "outline" })}
            >
              Open filtered positions
            </Link>
          </div>
        ) : (
          <div className="mt-4 flex flex-wrap gap-2">
            <Link
              href={buildOpportunitiesHref({
                marketDate: detail.market_date ?? null,
                botId: detail.bot_id,
                automationId: detail.automation_id,
                strategyConfigId: detail.strategy_config_id ?? null,
              })}
              className={buttonVariants({ variant: "outline" })}
            >
              Open filtered opportunities
            </Link>
            <Link
              href={buildPositionsHref({
                marketDate: detail.market_date ?? null,
                botId: detail.bot_id,
                automationId: detail.automation_id,
                strategyConfigId: detail.strategy_config_id ?? null,
              })}
              className={buttonVariants({ variant: "outline" })}
            >
              Open filtered positions
            </Link>
          </div>
        )}
      </SectionSurface>

      <div className="grid gap-4 xl:grid-cols-2">
        <SectionSurface
          title="Automation Runs"
          description="Recent planner/manager runs recorded for this owner scope."
        >
          {!automationRunRows.length ? (
            <div className="flex flex-col items-center justify-center gap-3 px-6 py-12 text-center">
              <RotateCw className="size-10 text-muted-foreground" />
              <div className="text-sm text-muted-foreground">
                No automation runs were recorded yet.
              </div>
            </div>
          ) : (
            <DataTable
              columns={AUTOMATION_RUN_COLUMNS}
              data={automationRunRows}
              getRowId={(row) => row.id}
              emptyMessage="No automation runs were available."
            />
          )}
        </SectionSurface>

        <SectionSurface
          title="Execution Intents"
          description="Recent execution intents attributed directly to this automation runtime."
        >
          {!intentRows.length ? (
            <div className="flex flex-col items-center justify-center gap-3 px-6 py-12 text-center">
              <Rows3 className="size-10 text-muted-foreground" />
              <div className="text-sm text-muted-foreground">
                No execution intents were recorded yet.
              </div>
            </div>
          ) : (
            <DataTable
              columns={INTENT_COLUMNS}
              data={intentRows}
              getRowId={(row) => row.id}
              emptyMessage="No intents were available."
            />
          )}
        </SectionSurface>
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <SectionSurface
          title="Opportunities"
          description="Owner-scoped opportunities stay primary; discovery lineage is shown inline for context."
        >
          {!detail.opportunities.length ? (
            <div className="flex flex-col items-center justify-center gap-3 px-6 py-12 text-center">
              <Radar className="size-10 text-muted-foreground" />
              <div className="text-sm text-muted-foreground">
                No opportunities were available for this automation runtime.
              </div>
            </div>
          ) : (
            <DataTable
              columns={opportunityColumns}
              data={detail.opportunities}
              getRowId={(row) => row.opportunity_id}
              emptyMessage="No opportunities were available."
            />
          )}
        </SectionSurface>

        <SectionSurface
          title="Positions"
          description="Open risk is attributed directly to the automation owner, with discovery shown second."
        >
          {!detail.positions.length ? (
            <div className="flex flex-col items-center justify-center gap-3 px-6 py-12 text-center">
              <BriefcaseBusiness className="size-10 text-muted-foreground" />
              <div className="text-sm text-muted-foreground">
                No positions were open for this automation runtime.
              </div>
            </div>
          ) : (
            <DataTable
              columns={positionColumns}
              data={detail.positions}
              getRowId={(row) => row.position_id}
              emptyMessage="No positions were available."
            />
          )}
        </SectionSurface>
      </div>
    </div>
  );
}
