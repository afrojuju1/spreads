"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";
import { startTransition, useMemo, useState } from "react";
import { BriefcaseBusiness, RefreshCw, Send, XCircle } from "lucide-react";

import { DataTable } from "@/components/data-table";
import {
  buildRuntimeHref,
  buildPositionsHref,
  buildPipelineHref,
  cancelExecution,
  closePosition,
  getPositions,
  submitEquityOrder,
  type EquityOrderRequest,
  type Position,
} from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  formatNullableCurrency,
  formatQuantity,
  formatSignedCurrency,
  LoadingState,
  MetricTile,
  readNumber,
  readString,
  SectionSurface,
} from "@/components/operator/operator-primitives";

function positionRecord(position: Position): Record<string, unknown> {
  return position as Record<string, unknown>;
}

function readRecord(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function readRecordList(value: unknown): Record<string, unknown>[] {
  return Array.isArray(value)
    ? value
        .filter((item) => typeof item === "object" && item !== null && !Array.isArray(item))
        .map((item) => item as Record<string, unknown>)
    : [];
}

function readOptionalNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function getPositionOwner(position: Position): Record<string, unknown> {
  return readRecord(positionRecord(position).owner);
}

function getPositionDiscovery(position: Position): Record<string, unknown> {
  return readRecord(positionRecord(position).discovery);
}

function hasPositionAutomationOwner(position: Position): boolean {
  const owner = getPositionOwner(position);
  return Boolean(
    typeof owner.bot_id === "string" &&
      owner.bot_id &&
      typeof owner.automation_id === "string" &&
      owner.automation_id,
  );
}

function getPositionRuntimeLabel(position: Position): string {
  const owner = getPositionOwner(position);
  const botId = readString(owner.bot_id, "");
  const automationId = readString(owner.automation_id, "");
  if (botId && automationId) {
    return `${botId} / ${automationId}`;
  }
  return "—";
}

function getPositionDiscoveryLabel(position: Position): string {
  const discovery = getPositionDiscovery(position);
  return readString(discovery.label, readString(position.pipeline_id));
}

function getPositionRuntimeHref(position: Position): string {
  const owner = getPositionOwner(position);
  const botId = typeof owner.bot_id === "string" ? owner.bot_id : null;
  const automationId =
    typeof owner.automation_id === "string" ? owner.automation_id : null;
  return buildRuntimeHref(botId, automationId, position.market_date);
}

function getPositionDiscoveryHref(position: Position): string {
  const discovery = getPositionDiscovery(position);
  const pipelineId =
    typeof discovery.pipeline_id === "string"
      ? discovery.pipeline_id
      : position.pipeline_id;
  return buildPipelineHref(pipelineId, position.market_date);
}

const TERMINAL_ATTEMPT_STATUSES = new Set([
  "canceled",
  "cancelled",
  "expired",
  "failed",
  "filled",
  "rejected",
  "revoked",
]);

function getOpenExecutionAttempt(position: Position): Record<string, unknown> {
  return readRecord(positionRecord(position).open_execution_attempt);
}

function getOpenExecutionAttemptId(position: Position): string {
  const attempt = getOpenExecutionAttempt(position);
  return readString(
    attempt.execution_attempt_id,
    readString(position.open_execution_attempt_id, ""),
  );
}

function getOpenExecutionAttemptStatus(position: Position): string {
  const attempt = getOpenExecutionAttempt(position);
  return readString(attempt.status, "");
}

function canCancelOpenExecutionAttempt(position: Position): boolean {
  const attemptId = getOpenExecutionAttemptId(position);
  const status = getOpenExecutionAttemptStatus(position).toLowerCase();
  return Boolean(attemptId && status && !TERMINAL_ATTEMPT_STATUSES.has(status));
}

const POSITION_COLUMNS: ColumnDef<Position>[] = [
  {
    accessorKey: "root_symbol",
    header: "Position",
    cell: ({ row }) => (
      <div>
        <div className="font-semibold">{row.original.root_symbol}</div>
        <div className="text-xs text-muted-foreground">
          {row.original.strategy_family}
        </div>
        {hasPositionAutomationOwner(row.original) ? (
          <Link
            href={getPositionRuntimeHref(row.original)}
            className="mt-1 inline-block text-xs text-foreground underline-offset-4 hover:underline"
          >
            Runtime · {getPositionRuntimeLabel(row.original)}
          </Link>
        ) : null}
        <Link
          href={getPositionDiscoveryHref(row.original)}
          className="mt-1 inline-block text-xs text-muted-foreground underline-offset-4 hover:underline"
        >
          Diagnostics · {getPositionDiscoveryLabel(row.original)}
        </Link>
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
    cell: ({ getValue }) => formatSignedCurrency(getValue() as number | null | undefined),
  },
  {
    accessorKey: "unrealized_pnl",
    header: "Unrealized",
    cell: ({ getValue }) => formatSignedCurrency(getValue() as number | null | undefined),
  },
  {
    id: "pipeline",
    header: "",
    cell: ({ row }) => (
      <span className="text-xs text-muted-foreground">
        {hasPositionAutomationOwner(row.original)
          ? "Owner-attributed"
          : "Diagnostics lineage only"}
      </span>
    ),
  },
];

export function PositionsIndexPageContent({
  marketDate,
  botId,
  automationId,
  strategyConfigId,
  label,
}: {
  marketDate?: string;
  botId?: string;
  automationId?: string;
  strategyConfigId?: string;
  label?: string;
}) {
  const router = useRouter();
  const queryClient = useQueryClient();
  const closeRuntime = "alpaca_direct";
  const [equitySymbol, setEquitySymbol] = useState("AAPL");
  const [equitySide, setEquitySide] = useState<EquityOrderRequest["side"]>("buy");
  const [equityQuantity, setEquityQuantity] = useState("1");
  const [equityLimitPrice, setEquityLimitPrice] = useState("");
  const [equityMessage, setEquityMessage] = useState<string | null>(null);
  const hasOwnerScope = Boolean(botId && automationId);
  const ownerScopeLabel = hasOwnerScope
    ? `Runtime · ${botId} / ${automationId}`
    : label
      ? `Diagnostics · ${label}`
      : "All runtimes";
  const positionsQuery = useQuery({
    queryKey: [
      "positions",
      marketDate ?? "",
      botId ?? "",
      automationId ?? "",
      strategyConfigId ?? "",
      label ?? "",
    ],
    queryFn: () =>
      getPositions({
        marketDate,
        botId,
        automationId,
        strategyConfigId,
        label,
        limit: 200,
      }),
  });
  const closeMutation = useMutation({
    mutationFn: ({
      positionId,
      executionRuntime,
    }: {
      positionId: string;
      executionRuntime: string;
    }) =>
      closePosition(positionId, {
        execution_runtime: executionRuntime,
      }),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["runtimes"] }),
        queryClient.invalidateQueries({ queryKey: ["positions"] }),
        queryClient.invalidateQueries({ queryKey: ["pipelines"] }),
      ]);
    },
  });
  const cancelMutation = useMutation({
    mutationFn: (executionAttemptId: string) => cancelExecution(executionAttemptId),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["runtimes"] }),
        queryClient.invalidateQueries({ queryKey: ["positions"] }),
        queryClient.invalidateQueries({ queryKey: ["pipelines"] }),
      ]);
    },
  });
  const equityOrder = useMemo(() => {
    const quantity = Number(equityQuantity);
    const limitPrice = Number(equityLimitPrice);
    const symbol = equitySymbol.trim().toUpperCase();
    if (
      !symbol ||
      !Number.isInteger(quantity) ||
      quantity <= 0 ||
      !Number.isFinite(limitPrice) ||
      limitPrice <= 0
    ) {
      return null;
    }
    return {
      symbol,
      side: equitySide,
      quantity,
      limit_price: limitPrice,
      time_in_force: "day" as const,
      label: label ?? "manual_equity",
      market_date: marketDate,
      execution_runtime: "alpaca_direct",
    };
  }, [equityLimitPrice, equityQuantity, equitySide, equitySymbol, label, marketDate]);
  const equityMutation = useMutation({
    mutationFn: (payload: EquityOrderRequest) => submitEquityOrder(payload),
    onSuccess: async (result) => {
      setEquityMessage(result.message);
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["runtimes"] }),
        queryClient.invalidateQueries({ queryKey: ["positions"] }),
        queryClient.invalidateQueries({ queryKey: ["pipelines"] }),
      ]);
    },
    onError: (error) => {
      setEquityMessage(error instanceof Error ? error.message : "Equity order failed.");
    },
  });

  if (positionsQuery.isLoading) {
    return <LoadingState />;
  }

  const positions = positionsQuery.data?.positions ?? [];
  const summary = positionsQuery.data?.summary ?? {};
  const closeLifecycle = readRecord(summary.close_lifecycle);
  const latestFailure = readRecord(closeLifecycle.latest_failure);
  const latestFilledCloses = readRecordList(closeLifecycle.latest_filled_closes);
  const closeProofRows = readRecordList(closeLifecycle.position_close_proof);

  function clearOwnerScope() {
    startTransition(() => {
      router.replace(
        buildPositionsHref({
          marketDate: marketDate ?? null,
        }),
        { scroll: false },
      );
    });
  }

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
                <BriefcaseBusiness data-icon="inline-start" />
                Positions
              </Badge>
              {hasOwnerScope ? (
                <Badge variant="outline">{ownerScopeLabel}</Badge>
              ) : null}
              {!hasOwnerScope && label ? (
                <Badge variant="outline">{ownerScopeLabel}</Badge>
              ) : null}
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">
              Open risk inventory
            </div>
            <div className="mt-2 text-sm text-foreground/70">
              Inspect current risk first. Runtime ownership and diagnostics
              lineage stay attached to each row, but this surface stays focused
              on inventory and exits. Current workspace scope: {ownerScopeLabel}.
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            <Badge variant="outline">Close runtime · Alpaca</Badge>
            <Button
              type="button"
              variant="outline"
              onClick={() => void positionsQuery.refetch()}
            >
              <RefreshCw data-icon="inline-start" />
              Refresh
            </Button>
            {hasOwnerScope || label ? (
              <Button type="button" variant="outline" onClick={clearOwnerScope}>
                Clear scope
              </Button>
            ) : null}
          </div>
        </div>
      </div>

      <SectionSurface
        title="Equity Ticket"
        description="Submit a small Alpaca-direct equity limit order into the execution ledger."
      >
        <div className="grid gap-3 md:grid-cols-[minmax(120px,180px)_minmax(120px,160px)_minmax(100px,140px)_minmax(120px,160px)_auto]">
          <div className="flex flex-col gap-2">
            <div className="text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
              Symbol
            </div>
            <Input
              value={equitySymbol}
              onChange={(event) => setEquitySymbol(event.target.value.toUpperCase())}
              placeholder="AAPL"
            />
          </div>
          <div className="flex flex-col gap-2">
            <div className="text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
              Side
            </div>
            <div className="flex h-8 rounded-lg border border-border/70 p-0.5">
              <Button
                type="button"
                size="sm"
                variant={equitySide === "buy" ? "default" : "ghost"}
                className="flex-1"
                onClick={() => setEquitySide("buy")}
              >
                Buy
              </Button>
              <Button
                type="button"
                size="sm"
                variant={equitySide === "sell" ? "default" : "ghost"}
                className="flex-1"
                onClick={() => setEquitySide("sell")}
              >
                Sell
              </Button>
            </div>
          </div>
          <div className="flex flex-col gap-2">
            <div className="text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
              Qty
            </div>
            <Input
              type="number"
              min={1}
              step={1}
              value={equityQuantity}
              onChange={(event) => setEquityQuantity(event.target.value)}
            />
          </div>
          <div className="flex flex-col gap-2">
            <div className="text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
              Limit
            </div>
            <Input
              type="number"
              min={0.01}
              step={0.01}
              value={equityLimitPrice}
              onChange={(event) => setEquityLimitPrice(event.target.value)}
              placeholder="0.00"
            />
          </div>
          <div className="flex items-end">
            <Button
              type="button"
              disabled={equityMutation.isPending || equityOrder == null}
              onClick={() => {
                if (equityOrder) {
                  setEquityMessage(null);
                  equityMutation.mutate(equityOrder);
                }
              }}
            >
              <Send data-icon="inline-start" />
              Submit
            </Button>
          </div>
        </div>
        {equityMessage ? (
          <div className="mt-3 text-sm text-muted-foreground">{equityMessage}</div>
        ) : null}
      </SectionSurface>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricTile
          label="Positions"
          value={String(summary.position_count ?? 0)}
          note="Current runtime inventory"
        />
        <MetricTile
          label="Open"
          value={String(summary.open_position_count ?? 0)}
          note="Open or partial"
        />
        <MetricTile
          label="Closed"
          value={String(summary.closed_position_count ?? 0)}
          note="Completed exits"
        />
        <MetricTile
          label="Running PnL"
          value={formatNullableCurrency(
            positions.reduce(
              (total, row) => total + (row.net_pnl ?? 0),
              0,
            ),
          )}
          note={closeMutation.isPending ? "Close queued" : "Live estimate"}
        />
      </div>

      <SectionSurface
        title="Close Lifecycle"
        description="Recent close attempts, pending close work, and filled close proof."
      >
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
          <MetricTile
            label="Status"
            value={readString(closeLifecycle.status, "unknown")}
            note="Current close health"
          />
          <MetricTile
            label="Attempts"
            value={String(readNumber(closeLifecycle.recent_close_attempt_count))}
            note="Recent close requests"
          />
          <MetricTile
            label="Active"
            value={String(readNumber(closeLifecycle.active_close_attempt_count))}
            note="Working close attempts"
          />
          <MetricTile
            label="Pending"
            value={String(readNumber(closeLifecycle.pending_close_intent_count))}
            note="Close intents"
          />
          <MetricTile
            label="Anomalies"
            value={String(readNumber(closeLifecycle.anomaly_count))}
            note={`Failed ${readNumber(closeLifecycle.failed_close_attempt_count)} · stale ${readNumber(closeLifecycle.stale_reconciliation_skip_count)} · mismatch ${readNumber(closeLifecycle.intent_mismatch_reject_count)}`}
          />
        </div>

        {Object.keys(latestFailure).length > 0 ? (
          <div className="mt-4 rounded-lg border border-border/70 p-3">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="text-sm font-medium">Latest close failure</div>
              <Badge variant="outline">{readString(latestFailure.status)}</Badge>
            </div>
            <div className="mt-2 grid gap-2 text-xs text-muted-foreground md:grid-cols-3">
              <div className="truncate">
                {readString(latestFailure.root_symbol)} ·{" "}
                {readString(latestFailure.requested_at)}
              </div>
              <div className="truncate">
                {readString(latestFailure.execution_attempt_id)}
              </div>
              <div className="truncate">
                {readString(latestFailure.error_text)}
              </div>
            </div>
          </div>
        ) : null}

        <div className="mt-4 grid gap-4 xl:grid-cols-2">
          <div className="min-w-0">
            <div className="mb-2 text-sm font-medium">Latest filled closes</div>
            <div className="divide-y divide-border/70 rounded-lg border border-border/70">
              {latestFilledCloses.length ? (
                latestFilledCloses.slice(0, 5).map((row) => {
                  const latestClose = readRecord(row.latest_close);
                  return (
                    <div
                      key={readString(row.position_id)}
                      className="grid gap-2 px-3 py-2 text-sm md:grid-cols-[minmax(80px,1fr)_minmax(120px,1.4fr)_minmax(90px,0.8fr)]"
                    >
                      <div className="min-w-0">
                        <div className="truncate font-medium">
                          {readString(row.root_symbol)}
                        </div>
                        <div className="truncate text-xs text-muted-foreground">
                          {readString(row.status)}
                        </div>
                      </div>
                      <div className="min-w-0 truncate text-xs text-muted-foreground">
                        {readString(latestClose.closed_at)}
                      </div>
                      <div className="text-right">
                        {formatNullableCurrency(
                          readOptionalNumber(latestClose.realized_pnl),
                        )}
                      </div>
                    </div>
                  );
                })
              ) : (
                <div className="px-3 py-3 text-sm text-muted-foreground">
                  No filled closes in scope.
                </div>
              )}
            </div>
          </div>

          <div className="min-w-0">
            <div className="mb-2 text-sm font-medium">Close proof by position</div>
            <div className="divide-y divide-border/70 rounded-lg border border-border/70">
              {closeProofRows.length ? (
                closeProofRows.slice(0, 5).map((row) => {
                  const latestClose = readRecord(row.latest_close);
                  return (
                    <div
                      key={readString(row.position_id)}
                      className="grid gap-2 px-3 py-2 text-sm md:grid-cols-[minmax(80px,1fr)_minmax(80px,0.8fr)_minmax(120px,1.3fr)]"
                    >
                      <div className="min-w-0">
                        <div className="truncate font-medium">
                          {readString(row.root_symbol)}
                        </div>
                        <div className="truncate text-xs text-muted-foreground">
                          {readString(row.strategy_family)}
                        </div>
                      </div>
                      <div>
                        <Badge variant="outline">{readString(row.status)}</Badge>
                      </div>
                      <div className="min-w-0 text-xs text-muted-foreground">
                        <div className="truncate">
                          closes {readNumber(row.close_count)} ·{" "}
                          {readString(row.reconciliation_status)}
                        </div>
                        <div className="truncate">
                          {readString(
                            latestClose.closed_at,
                            readString(row.last_exit_reason),
                          )}
                        </div>
                      </div>
                    </div>
                  );
                })
              ) : (
                <div className="px-3 py-3 text-sm text-muted-foreground">
                  No close proof rows in scope.
                </div>
              )}
            </div>
          </div>
        </div>
      </SectionSurface>

      <SectionSurface
        title="Position List"
        description="Use the owning runtime for limits and execution context, or close directly from here."
      >
        <DataTable
          columns={[
            ...POSITION_COLUMNS,
            {
              id: "actions",
              header: "",
              cell: ({ row }) => {
                const attemptId = getOpenExecutionAttemptId(row.original);
                const canCancel = canCancelOpenExecutionAttempt(row.original);
                return (
                  <div className="flex flex-wrap justify-end gap-2">
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      disabled={
                        closeMutation.isPending ||
                        row.original.position_status === "closed"
                      }
                      onClick={() =>
                        closeMutation.mutate({
                          positionId: row.original.position_id,
                          executionRuntime: closeRuntime,
                        })
                      }
                    >
                      Close
                    </Button>
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      disabled={!canCancel || cancelMutation.isPending}
                      onClick={() => {
                        if (attemptId) {
                          cancelMutation.mutate(attemptId);
                        }
                      }}
                    >
                      <XCircle data-icon="inline-start" />
                      Cancel
                    </Button>
                  </div>
                );
              },
            },
          ]}
          data={positions}
          getRowId={(row) => row.position_id}
          emptyMessage="No positions were available."
        />
      </SectionSurface>
    </div>
  );
}
