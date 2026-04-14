"use client";

import Link from "next/link";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";
import {
  Activity,
  BellRing,
  CandlestickChart,
  LoaderCircle,
  RefreshCw,
  Rows3,
} from "lucide-react";
import { useRouter } from "next/navigation";
import { startTransition, useEffect, useState } from "react";

import { DataTable } from "@/components/data-table";
import {
  buildSessionHref,
  closeSessionPosition,
  createSessionExecution,
  type AlertRecord,
  type ExecutionAttempt,
  type JobRun,
  type LiveCandidate,
  type LiveEvent,
  getSessionDetail,
  getSessions,
  refreshSessionExecution,
  type SessionDetail,
  type SessionPortfolioPosition,
} from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
} from "@/components/ui/select";
import {
  CaptureStatusBadge,
  ExecutionStatusBadge,
  formatCurrency,
  formatDate,
  formatDuration,
  formatNullableCurrency,
  formatQuantity,
  formatScore,
  formatSignedCurrency,
  formatTime,
  formatTimestamp,
  LoadingState,
  MetricTile,
  PortfolioStatusBadge,
  readNumber,
  readString,
  ReconciliationStatusBadge,
  RiskStatusBadge,
  SectionSurface,
  SessionStatusBadge,
  valueTone,
} from "@/components/sessions/workspace-primitives";
import { cn } from "@/lib/utils";

type CandidateRow = {
  id: string;
  symbol: string;
  strategy: string;
  expirationDate: string;
  strikes: string;
  score: number;
  credit: number;
  raw: LiveCandidate;
};

type SlotRow = {
  id: string;
  slotAt: string;
  status: string;
  captureStatus: string;
  streamCount: number;
  baselineCount: number;
  recoveryCount: number;
  retryCount: number;
  duration: string;
  workerName: string;
};

type AlertRow = {
  id: string;
  createdAt: string;
  symbol: string;
  alertType: string;
  status: string;
};

type EventRow = {
  id: string;
  generatedAt: string;
  symbol: string;
  eventType: string;
  message: string;
};

type SessionPortfolioPositionRow = {
  id: string;
  symbol: string;
  strategy: string;
  status: string;
  brokerStatus: string;
  riskStatus: string;
  riskNote: string | null | undefined;
  reconciliationStatus: string;
  reconciliationNote: string | null | undefined;
  lastReconciledAt: string | null | undefined;
  openedQuantity: number | null | undefined;
  remainingQuantity: number | null | undefined;
  entryCredit: number | null | undefined;
  closeMark: number | null | undefined;
  realizedPnl: number | null | undefined;
  unrealizedPnl: number | null | undefined;
  maxLoss: number | null | undefined;
  openedAt: string | null | undefined;
  closedAt: string | null | undefined;
  raw: SessionPortfolioPosition;
};

const TERMINAL_EXECUTION_STATUSES = new Set([
  "filled",
  "canceled",
  "done_for_day",
  "expired",
  "failed",
  "rejected",
]);

function buildCandidateRows(candidates: LiveCandidate[]): CandidateRow[] {
  return candidates.map((candidate) => ({
    id: String(candidate.candidate_id),
    symbol: candidate.underlying_symbol,
    strategy: candidate.strategy,
    expirationDate: candidate.expiration_date,
    strikes: `${candidate.candidate.short_strike.toFixed(2)} / ${candidate.candidate.long_strike.toFixed(2)}`,
    score: candidate.quality_score,
    credit: candidate.midpoint_credit,
    raw: candidate,
  }));
}

function quoteCapture(run: JobRun): Record<string, unknown> {
  const rootValue = run.result;
  if (!rootValue || typeof rootValue !== "object") {
    return {};
  }
  const candidate = (rootValue as Record<string, unknown>).quote_capture;
  return candidate && typeof candidate === "object"
    ? (candidate as Record<string, unknown>)
    : {};
}

function readStreamQuoteCount(payload: Record<string, unknown>): number {
  return readNumber(
    payload.stream_quote_events_saved ?? payload.websocket_quote_events_saved,
  );
}

function buildSlotRows(slotRuns: JobRun[]): SlotRow[] {
  return slotRuns.map((run) => {
    const capture = quoteCapture(run);
    return {
      id: run.job_run_id,
      slotAt: readString(
        run.slot_at ?? run.scheduled_for ?? run.started_at ?? run.finished_at,
        "",
      ),
      status: run.status,
      captureStatus: readString(capture.capture_status, "unknown"),
      streamCount: readStreamQuoteCount(capture),
      baselineCount: readNumber(capture.baseline_quote_events_saved),
      recoveryCount: readNumber(capture.recovery_quote_events_saved),
      retryCount: readNumber(run.retry_count),
      duration: formatDuration(run),
      workerName: readString(run.worker_name, "—"),
    };
  });
}

function buildAlertRows(alerts: AlertRecord[]): AlertRow[] {
  return alerts.map((alert) => ({
    id: String(alert.alert_id),
    createdAt: alert.created_at,
    symbol: alert.symbol,
    alertType: alert.alert_type,
    status: alert.status,
  }));
}

function buildEventRows(events: LiveEvent[]): EventRow[] {
  return [...events].reverse().map((event, index) => ({
    id: `${event.cycle_id}:${event.symbol}:${event.generated_at}:${index}`,
    generatedAt: event.generated_at,
    symbol: event.symbol,
    eventType: event.event_type,
    message: event.message,
  }));
}

function buildSessionPortfolioPositionRows(
  positions: SessionPortfolioPosition[],
): SessionPortfolioPositionRow[] {
  return positions.map((position) => ({
    id: position.position_id,
    symbol: position.underlying_symbol,
    strategy: position.strategy,
    status: position.position_status,
    brokerStatus: position.broker_status,
    riskStatus: readString(position.risk_status, "unknown"),
    riskNote: position.risk_note,
    reconciliationStatus: readString(position.reconciliation_status, "unknown"),
    reconciliationNote: position.reconciliation_note,
    lastReconciledAt: position.last_reconciled_at,
    openedQuantity: position.opened_quantity ?? position.filled_quantity,
    remainingQuantity: position.remaining_quantity ?? position.filled_quantity,
    entryCredit: position.entry_credit,
    closeMark: position.spread_mark_close,
    realizedPnl: position.realized_pnl,
    unrealizedPnl: position.unrealized_pnl ?? position.estimated_close_pnl,
    maxLoss: position.max_loss,
    openedAt: position.opened_at,
    closedAt: position.closed_at,
    raw: position,
  }));
}

function isWorkingExecutionAttempt(attempt: ExecutionAttempt): boolean {
  return !TERMINAL_EXECUTION_STATUSES.has(readString(attempt.status, "unknown"));
}

function buildOperatorFocus({
  session,
  promotableCount,
  monitorCount,
  selectedPromotable,
}: {
  session: SessionDetail | null;
  promotableCount: number;
  monitorCount: number;
  selectedPromotable: LiveCandidate | null;
}): {
  title: string;
  detail: string;
} {
  const openPositionCount = session?.portfolio.summary.open_position_count ?? 0;
  const workingExecutionCount = (session?.executions ?? []).filter(
    isWorkingExecutionAttempt,
  ).length;

  if (openPositionCount > 0) {
    return {
      title: "Manage open risk",
      detail: `${openPositionCount} open position${openPositionCount === 1 ? "" : "s"} and ${workingExecutionCount} working execution${workingExecutionCount === 1 ? "" : "s"} are still active in this session.`,
    };
  }

  if (workingExecutionCount > 0) {
    return {
      title: "Watch broker progress",
      detail: `${workingExecutionCount} execution attempt${workingExecutionCount === 1 ? "" : "s"} still need broker resolution before the session is clean.`,
    };
  }

  if (selectedPromotable) {
    return {
      title: `${selectedPromotable.underlying_symbol} is ready to act`,
      detail: `${promotableCount} promotable ${promotableCount === 1 ? "opportunity is" : "opportunities are"} available; ${selectedPromotable.strategy.replaceAll("_", " ")} is currently selected for execution.`,
    };
  }

  if (monitorCount > 0) {
    return {
      title: "Monitor live flow",
      detail: `${monitorCount} monitor ${monitorCount === 1 ? "opportunity remains" : "opportunities remain"}, but nothing is promotable right now.`,
    };
  }

  return {
    title: "Collector is quiet",
    detail: "No promotable or monitor opportunities are currently persisted for this session.",
  };
}

const CANDIDATE_COLUMNS: ColumnDef<CandidateRow>[] = [
  {
    accessorKey: "symbol",
    header: "Symbol",
    cell: ({ row }) => (
      <span className="font-semibold tracking-[0.04em]">
        {row.original.symbol}
      </span>
    ),
  },
  {
    accessorKey: "strategy",
    header: "Side",
    cell: ({ getValue }) => (
      <span className="capitalize text-foreground/80">
        {String(getValue())}
      </span>
    ),
  },
  {
    accessorKey: "expirationDate",
    header: "Expiry",
    cell: ({ getValue }) => (
      <span className="text-muted-foreground">
        {formatDate(String(getValue()))}
      </span>
    ),
  },
  {
    accessorKey: "strikes",
    header: "Strikes",
    cell: ({ getValue }) => (
      <span className="font-mono text-[12px]">{String(getValue())}</span>
    ),
  },
  {
    accessorKey: "score",
    header: "Score",
    cell: ({ getValue }) => (
      <span className="font-mono">{formatScore(Number(getValue()))}</span>
    ),
  },
  {
    accessorKey: "credit",
    header: "Credit",
    cell: ({ getValue }) => (
      <span className="font-mono">{formatCurrency(Number(getValue()))}</span>
    ),
  },
];

const SLOT_COLUMNS: ColumnDef<SlotRow>[] = [
  {
    accessorKey: "slotAt",
    header: "Slot",
    cell: ({ getValue }) => (
      <span className="font-mono text-[12px]">
        {formatTimestamp(String(getValue()))}
      </span>
    ),
  },
  {
    accessorKey: "status",
    header: "Run",
    cell: ({ getValue }) => <SessionStatusBadge value={String(getValue())} />,
  },
  {
    accessorKey: "captureStatus",
    header: "Capture",
    cell: ({ getValue }) => <CaptureStatusBadge value={String(getValue())} />,
  },
  {
    accessorKey: "streamCount",
    header: "Stream",
    cell: ({ getValue }) => <span className="font-mono">{String(getValue())}</span>,
  },
  {
    accessorKey: "baselineCount",
    header: "Base",
    cell: ({ getValue }) => <span className="font-mono">{String(getValue())}</span>,
  },
  {
    accessorKey: "recoveryCount",
    header: "Recovery",
    cell: ({ getValue }) => <span className="font-mono">{String(getValue())}</span>,
  },
  {
    accessorKey: "retryCount",
    header: "Retries",
    cell: ({ getValue }) => <span className="font-mono">{String(getValue())}</span>,
  },
  {
    accessorKey: "duration",
    header: "Duration",
    cell: ({ getValue }) => (
      <span className="font-mono text-muted-foreground">{String(getValue())}</span>
    ),
  },
  {
    accessorKey: "workerName",
    header: "Worker",
    cell: ({ getValue }) => (
      <span className="text-muted-foreground">{String(getValue())}</span>
    ),
  },
];

const ALERT_COLUMNS: ColumnDef<AlertRow>[] = [
  {
    accessorKey: "createdAt",
    header: "Time",
    cell: ({ getValue }) => (
      <span className="font-mono text-[12px]">
        {formatTimestamp(String(getValue()))}
      </span>
    ),
  },
  {
    accessorKey: "symbol",
    header: "Symbol",
    cell: ({ getValue }) => <span className="font-semibold">{String(getValue())}</span>,
  },
  {
    accessorKey: "alertType",
    header: "Type",
    cell: ({ getValue }) => (
      <span className="text-foreground/80">
        {String(getValue()).replaceAll("_", " ")}
      </span>
    ),
  },
  {
    accessorKey: "status",
    header: "Status",
    cell: ({ getValue }) => <SessionStatusBadge value={String(getValue())} />,
  },
];

const EVENT_COLUMNS: ColumnDef<EventRow>[] = [
  {
    accessorKey: "generatedAt",
    header: "Time",
    cell: ({ getValue }) => (
      <span className="font-mono text-[12px]">
        {formatTimestamp(String(getValue()))}
      </span>
    ),
  },
  {
    accessorKey: "symbol",
    header: "Symbol",
    cell: ({ getValue }) => <span className="font-semibold">{String(getValue())}</span>,
  },
  {
    accessorKey: "eventType",
    header: "Event",
    cell: ({ getValue }) => (
      <span className="text-foreground/80">
        {String(getValue()).replaceAll("_", " ")}
      </span>
    ),
  },
  {
    accessorKey: "message",
    header: "Message",
    cell: ({ getValue }) => (
      <span className="text-foreground/70">{String(getValue())}</span>
    ),
  },
];

function buildSessionPortfolioPositionColumns({
  closingPositionId,
  onClose,
}: {
  closingPositionId: string | null;
  onClose: (position: SessionPortfolioPosition) => void;
}): ColumnDef<SessionPortfolioPositionRow>[] {
  return [
    {
      accessorKey: "symbol",
      header: "Underlying",
      cell: ({ row }) => (
        <div className="space-y-1">
          <div className="font-semibold tracking-[0.04em]">
            {row.original.symbol}
          </div>
          <div className="text-[11px] text-muted-foreground">
            {row.original.raw.short_symbol} / {row.original.raw.long_symbol}
          </div>
          {row.original.reconciliationStatus === "mismatch" &&
          row.original.reconciliationNote ? (
            <div className="rounded-lg border border-rose-200 bg-rose-50 px-2 py-1 text-[11px] text-rose-900">
              {row.original.reconciliationNote}
            </div>
          ) : null}
        </div>
      ),
    },
    {
      accessorKey: "strategy",
      header: "Setup",
      cell: ({ row }) => (
        <div>
          <div className="capitalize text-foreground/80">
            {row.original.strategy.replaceAll("_", " ")}
          </div>
          <div className="text-[11px] text-muted-foreground">
            expires {formatDate(row.original.raw.expiration_date)}
          </div>
        </div>
      ),
    },
    {
      accessorKey: "status",
      header: "Status",
      cell: ({ row }) => (
        <div className="flex flex-col gap-1">
          <PortfolioStatusBadge value={row.original.status} />
          <ExecutionStatusBadge value={row.original.brokerStatus} />
          <RiskStatusBadge value={row.original.riskStatus} />
          <ReconciliationStatusBadge value={row.original.reconciliationStatus} />
        </div>
      ),
    },
    {
      accessorKey: "remainingQuantity",
      header: "Qty",
      cell: ({ row }) => (
        <div className="font-mono">
          <div>{formatQuantity(row.original.remainingQuantity)}</div>
          <div className="text-[11px] text-muted-foreground">
            open {formatQuantity(row.original.openedQuantity)}
          </div>
        </div>
      ),
    },
    {
      accessorKey: "entryCredit",
      header: "Entry Credit",
      cell: ({ getValue }) => (
        <span className="font-mono">
          {formatNullableCurrency(getValue<number | null | undefined>())}
        </span>
      ),
    },
    {
      accessorKey: "closeMark",
      header: "Close Mark",
      cell: ({ row }) => (
        <div className="font-mono">
          <div>{formatNullableCurrency(row.original.closeMark)}</div>
          <div className="text-[11px] text-muted-foreground">
            {readString(row.original.raw.mark_source, "unquoted")}
          </div>
        </div>
      ),
    },
    {
      accessorKey: "realizedPnl",
      header: "P&L",
      cell: ({ row }) => (
        <div className="font-mono">
          <div className={valueTone(row.original.realizedPnl)}>
            realized {formatSignedCurrency(row.original.realizedPnl)}
          </div>
          <div className={cn("text-[11px]", valueTone(row.original.unrealizedPnl))}>
            open {formatSignedCurrency(row.original.unrealizedPnl)}
          </div>
        </div>
      ),
    },
    {
      accessorKey: "maxLoss",
      header: "Max Loss",
      cell: ({ getValue }) => (
        <span className="font-mono">
          {formatNullableCurrency(getValue<number | null | undefined>())}
        </span>
      ),
    },
    {
      accessorKey: "openedAt",
      header: "Opened",
      cell: ({ row }) => (
        <div className="font-mono text-[12px]">
          <div>{formatTimestamp(row.original.openedAt)}</div>
          <div className="text-muted-foreground">
            {formatTimestamp(row.original.closedAt)}
          </div>
        </div>
      ),
    },
    {
      id: "actions",
      header: "",
      cell: ({ row }) => {
        const canClose =
          row.original.status !== "closed" &&
          (row.original.remainingQuantity ?? 0) > 0;
        return canClose ? (
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={closingPositionId === row.original.id}
            onClick={() => onClose(row.original.raw)}
          >
            {closingPositionId === row.original.id ? (
              <LoaderCircle className="size-3.5 animate-spin" />
            ) : null}
            Close
          </Button>
        ) : (
          <span className="text-xs text-muted-foreground">Settled</span>
        );
      },
    },
  ];
}

function SessionPortfolioSection({
  session,
  closingPositionId,
  onClosePosition,
  closeError,
  closeMessage,
}: {
  session: SessionDetail;
  closingPositionId: string | null;
  onClosePosition: (position: SessionPortfolioPosition) => void;
  closeError: string | null;
  closeMessage: string | null;
}) {
  const portfolio = session.portfolio;
  const summary = portfolio.summary;
  const positionRows = buildSessionPortfolioPositionRows(portfolio.positions);
  const mismatchRows = positionRows.filter(
    (row) => row.reconciliationStatus === "mismatch",
  );
  const columns = buildSessionPortfolioPositionColumns({
    closingPositionId,
    onClose: onClosePosition,
  });

  return (
    <SectionSurface
      title="Open Risk & PnL"
      description="Persisted session positions, close controls, and quote-backed PnL for the risk that is still sitting on the book."
    >
      {!positionRows.length ? (
        <div className="rounded-2xl border border-dashed border-border/70 px-4 py-8 text-sm text-muted-foreground">
          No session positions were opened for this session yet.
        </div>
      ) : (
        <div className="flex flex-col gap-4">
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
            <MetricTile
              label="Open positions"
              value={String(summary.open_position_count)}
              note={`${summary.partial_close_position_count ?? 0} partial / ${summary.quoted_position_count} quoted`}
            />
            <MetricTile
              label="Closed today"
              value={String(summary.closed_position_count ?? 0)}
              note={`${formatQuantity(summary.remaining_contract_count)} contracts still open`}
            />
            <MetricTile
              label="Realized PnL"
              value={formatSignedCurrency(summary.realized_pnl_total)}
              note={`Net ${formatSignedCurrency(summary.net_pnl_total)}`}
            />
            <MetricTile
              label="Open PnL"
              value={formatSignedCurrency(summary.unrealized_pnl_total)}
              note={`Marks as of ${formatTimestamp(summary.retrieved_at)}`}
            />
            <MetricTile
              label="Risk status"
              value={readString(session.risk_status, "unknown").toUpperCase()}
              note={readString(session.risk_note, "No session risk note")}
            />
            <MetricTile
              label="Reconciliation"
              value={readString(session.reconciliation_status, "unknown").toUpperCase()}
              note={readString(session.reconciliation_note, "No reconciliation note")}
            />
          </div>
          {summary.mark_error ? (
            <div className="rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-950">
              Quote enrichment was partial: {summary.mark_error}
            </div>
          ) : null}
          {mismatchRows.map((row) => (
            <div
              key={`mismatch-${row.id}`}
              className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-900"
            >
              {row.symbol} {row.strategy.replaceAll("_", " ")} mismatch:{" "}
              {readString(
                row.reconciliationNote,
                "Broker state does not match the local session position.",
              )}
              {row.lastReconciledAt
                ? ` · last checked ${formatTimestamp(row.lastReconciledAt)}`
                : ""}
            </div>
          ))}
          {closeError ? (
            <div className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-900">
              {closeError}
            </div>
          ) : null}
          {closeMessage ? (
            <div className="rounded-2xl border border-border/70 bg-background/70 px-4 py-3 text-sm text-foreground/70">
              {closeMessage}
            </div>
          ) : null}
          <DataTable
            columns={columns}
            data={positionRows}
            getRowId={(row) => row.id}
            emptyMessage="No session positions were recorded in the execution ledger."
          />
        </div>
      )}
    </SectionSurface>
  );
}

function SessionActionDesk({
  session,
  promotableRows,
  selectedPromotable,
  selectedPromotablePayload,
  selectedPromotableId,
  onSelectPromotable,
  onExecuteSelected,
  submitPending,
  submitError,
  submitMessage,
  refreshingAttemptId,
  onRefreshExecution,
  refreshError,
  refreshMessage,
}: {
  session: SessionDetail;
  promotableRows: CandidateRow[];
  selectedPromotable: LiveCandidate | null;
  selectedPromotablePayload: Record<string, unknown> | null;
  selectedPromotableId: string | null;
  onSelectPromotable: (candidateId: string) => void;
  onExecuteSelected: (candidate: LiveCandidate) => void;
  submitPending: boolean;
  submitError: string | null;
  submitMessage: string | null;
  refreshingAttemptId: string | null;
  onRefreshExecution: (executionAttemptId: string) => void;
  refreshError: string | null;
  refreshMessage: string | null;
}) {
  const workingExecutionCount = session.executions.filter(
    isWorkingExecutionAttempt,
  ).length;
  const filledExecutionCount = session.executions.filter(
    (attempt) => readString(attempt.status, "unknown") === "filled",
  ).length;

  return (
    <SectionSurface
      title="Action Desk"
      description="Act on the current best opportunity and keep broker-working executions in view from the same workspace."
    >
      <div className="flex flex-col gap-4">
        <div className="grid gap-4 xl:grid-cols-[1.1fr_0.9fr]">
          <div className="rounded-2xl border border-border/70 bg-background/70 p-4">
            <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
              Promotable queue
            </div>
            <div className="mt-3">
              <DataTable
                columns={CANDIDATE_COLUMNS}
                data={promotableRows}
                getRowId={(row) => row.id}
                selectedId={selectedPromotableId ?? undefined}
                onSelect={(row) => onSelectPromotable(row.id)}
                emptyMessage="No promotable opportunities were persisted for this session."
              />
            </div>
          </div>

          <div className="rounded-2xl border border-border/70 bg-background/70 px-4 py-4">
            <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
              Selected action
            </div>
            {selectedPromotable ? (
              <div className="mt-3 flex flex-col gap-4">
                <div>
                  <div className="text-lg font-medium">
                    {selectedPromotable.underlying_symbol} ·{" "}
                    {selectedPromotable.strategy.replaceAll("_", " ")}
                  </div>
                  <div className="mt-1 text-sm text-muted-foreground">
                    {selectedPromotable.short_symbol} /{" "}
                    {selectedPromotable.long_symbol} · expires{" "}
                    {formatDate(selectedPromotable.expiration_date)}
                  </div>
                </div>
                <div className="grid gap-3 sm:grid-cols-3">
                  <MetricTile
                    label="Score"
                    value={formatScore(selectedPromotable.quality_score)}
                  />
                  <MetricTile
                    label="Credit"
                    value={formatCurrency(selectedPromotable.midpoint_credit)}
                  />
                  <MetricTile
                    label="Limit"
                    value={formatCurrency(
                      readNumber(
                        selectedPromotablePayload?.limit_price ??
                          selectedPromotablePayload?.midpoint_credit,
                      ),
                    )}
                    note="Persisted candidate payload"
                  />
                </div>
                <div className="flex flex-col gap-2">
                  <Button
                    type="button"
                    variant="secondary"
                    disabled={submitPending}
                    onClick={() => onExecuteSelected(selectedPromotable)}
                  >
                    {submitPending ? (
                      <LoaderCircle className="size-4 animate-spin" />
                    ) : null}
                    Execute 1x
                  </Button>
                  <div className="text-sm text-muted-foreground">
                    Uses the candidate&apos;s persisted Alpaca multi-leg order
                    payload and saved limit price.
                  </div>
                </div>
              </div>
            ) : (
              <div className="mt-3 rounded-2xl border border-dashed border-border/70 px-4 py-8 text-sm text-muted-foreground">
                No promotable opportunity is ready for execution right now.
              </div>
            )}
            {submitError ? (
              <div className="mt-3 text-sm text-rose-700">{submitError}</div>
            ) : null}
            {submitMessage ? (
              <div className="mt-3 text-sm text-foreground/70">
                {submitMessage}
              </div>
            ) : null}
          </div>
        </div>

        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <MetricTile
            label="Working"
            value={String(workingExecutionCount)}
            note="Non-terminal broker attempts"
          />
          <MetricTile
            label="Filled"
            value={String(filledExecutionCount)}
            note="Attempts that reached filled"
          />
          <MetricTile
            label="Total attempts"
            value={String(session.executions.length)}
            note="Persisted execution attempts"
          />
          <MetricTile
            label="Linked positions"
            value={String(session.portfolio.positions.length)}
            note="Session positions from filled opens"
          />
        </div>

        <div className="rounded-2xl border border-border/70 bg-background/70 p-4">
          <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
            Execution ledger
          </div>
          {!session.executions.length ? (
            <div className="mt-3 rounded-2xl border border-dashed border-border/70 px-4 py-8 text-sm text-muted-foreground">
              No execution attempts were recorded for this session yet.
            </div>
          ) : (
            <div className="mt-3 flex flex-col gap-3">
              {session.executions.map((attempt) => (
                <ExecutionAttemptCard
                  key={attempt.execution_attempt_id}
                  attempt={attempt}
                  refreshing={refreshingAttemptId === attempt.execution_attempt_id}
                  onRefresh={() => onRefreshExecution(attempt.execution_attempt_id)}
                />
              ))}
            </div>
          )}
        </div>

        {refreshError ? (
          <div className="text-sm text-rose-700">{refreshError}</div>
        ) : null}
        {refreshMessage ? (
          <div className="text-sm text-foreground/70">{refreshMessage}</div>
        ) : null}
      </div>
    </SectionSurface>
  );
}

function SessionLiveFlow({
  monitorRows,
  alertRows,
  eventRows,
}: {
  monitorRows: CandidateRow[];
  alertRows: AlertRow[];
  eventRows: EventRow[];
}) {
  return (
    <SectionSurface
      title="Live Flow"
      description="Keep the monitor queue, alerts, and collector events together so the session narrative stays in one place."
    >
      <div className="grid gap-4 xl:grid-cols-[1.05fr_0.95fr]">
        <div className="rounded-2xl border border-border/70 bg-background/70 p-4">
          <div className="mb-3 flex items-center gap-2 text-sm font-medium">
            <Rows3 className="size-4 text-muted-foreground" />
            Monitor queue
          </div>
          <DataTable
            columns={CANDIDATE_COLUMNS}
            data={monitorRows}
            getRowId={(row) => row.id}
            emptyMessage="No monitor opportunities were persisted for this session."
          />
        </div>

        <div className="flex flex-col gap-4">
          <div className="rounded-2xl border border-border/70 bg-background/70 p-4">
            <div className="mb-3 flex items-center gap-2 text-sm font-medium">
              <BellRing className="size-4 text-muted-foreground" />
              Alerts
            </div>
            <DataTable
              columns={ALERT_COLUMNS}
              data={alertRows}
              getRowId={(row) => row.id}
              emptyMessage="No alert records were found for this session."
            />
          </div>
          <div className="rounded-2xl border border-border/70 bg-background/70 p-4">
            <div className="mb-3 flex items-center gap-2 text-sm font-medium">
              <Activity className="size-4 text-muted-foreground" />
              Events
            </div>
            <DataTable
              columns={EVENT_COLUMNS}
              data={eventRows}
              getRowId={(row) => row.id}
              emptyMessage="No collector events were found for this session."
            />
          </div>
        </div>
      </div>
    </SectionSurface>
  );
}

function SessionCollectorHealth({
  session,
  latestSlotCapture,
  slotRows,
}: {
  session: SessionDetail;
  latestSlotCapture: Record<string, unknown>;
  slotRows: SlotRow[];
}) {
  const currentCycle = session.current_cycle;

  return (
    <SectionSurface
      title="Collector Health"
      description="Slot timing, quote-capture mix, and current-cycle context for the collector that fed this session."
    >
      <div className="flex flex-col gap-4">
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
          <MetricTile
            label="Latest slot"
            value={
              session.latest_slot
                ? formatTime(
                    readString(
                      session.latest_slot.slot_at ??
                        session.latest_slot.scheduled_for ??
                        session.latest_slot.finished_at,
                      "",
                    ),
                  )
                : "—"
            }
            note={readString(session.latest_slot?.status, "No slot")}
          />
          <MetricTile
            label="Capture mix"
            value={`${readStreamQuoteCount(latestSlotCapture)} stream`}
            note={`${readNumber(latestSlotCapture.baseline_quote_events_saved)} baseline / ${readNumber(latestSlotCapture.recovery_quote_events_saved)} recovery`}
          />
          <MetricTile
            label="Updated"
            value={formatTime(session.updated_at)}
            note={formatDate(session.updated_at)}
          />
          <MetricTile
            label="Slot runs"
            value={String(slotRows.length)}
            note={`${session.events.length} recorded events`}
          />
          <MetricTile
            label="Current cycle"
            value={currentCycle ? formatTime(currentCycle.generated_at) : "—"}
            note={
              currentCycle
                ? `p ${currentCycle.selection_counts.promotable} / m ${currentCycle.selection_counts.monitor}`
                : "No live cycle cached"
            }
          />
        </div>
        <DataTable
          columns={SLOT_COLUMNS}
          data={slotRows}
          getRowId={(row) => row.id}
          emptyMessage="No slot runs were persisted for this session."
        />
      </div>
    </SectionSurface>
  );
}

function SessionAnalysis({ session }: { session: SessionDetail }) {
  if (!session.analysis) {
    return (
      <div className="rounded-2xl border border-dashed border-border/70 px-4 py-8 text-sm text-muted-foreground">
        Post-market analysis is not available for this session yet.
      </div>
    );
  }

  const strongest = session.analysis.tuning?.strongest_signals ?? [];
  const weakest = session.analysis.tuning?.weakest_signals ?? [];

  return (
    <div className="flex flex-col gap-4">
      <div className="grid gap-3 md:grid-cols-4">
        <MetricTile
          label="Ideas"
          value={String(session.analysis.outcomes?.idea_count ?? 0)}
          note="Persisted promotable + monitor ideas"
        />
        <MetricTile
          label="Cycles"
          value={String(session.analysis.cycle_count ?? 0)}
          note="Collector cycles in this session"
        />
        <MetricTile
          label="Run samples"
          value={String(readNumber(session.analysis.run_overview?.total_runs))}
          note="Scan runs included in analysis"
        />
        <MetricTile
          label="Quote events"
          value={String(readNumber(session.analysis.quote_overview?.quote_events))}
          note="Persisted intraday quote rows"
        />
      </div>
      <div className="grid gap-4 xl:grid-cols-2">
        <div className="rounded-2xl border border-border/70 bg-background/70 p-4">
          <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
            Strongest signals
          </div>
          <div className="mt-3 flex flex-col gap-2">
            {strongest.length ? (
              strongest.slice(0, 5).map((row, index) => (
                <div
                  key={`${readString(row.dimension, "signal")}-${row.bucket}-${index}`}
                  className="rounded-xl border border-border/70 px-3 py-2"
                >
                  <div className="text-sm font-medium">
                    {readString(row.dimension, "signal")}: {row.bucket}
                  </div>
                  <div className="mt-1 text-xs text-muted-foreground">
                    count {row.count ?? 0} | resolved {row.resolved_count ?? 0} |
                    avg pnl {row.average_estimated_pnl ?? "n/a"}
                  </div>
                </div>
              ))
            ) : (
              <div className="text-sm text-muted-foreground">
                No tuned signals were available.
              </div>
            )}
          </div>
        </div>
        <div className="rounded-2xl border border-border/70 bg-background/70 p-4">
          <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
            Weakest signals
          </div>
          <div className="mt-3 flex flex-col gap-2">
            {weakest.length ? (
              weakest.slice(0, 5).map((row, index) => (
                <div
                  key={`${readString(row.dimension, "signal")}-${row.bucket}-${index}`}
                  className="rounded-xl border border-border/70 px-3 py-2"
                >
                  <div className="text-sm font-medium">
                    {readString(row.dimension, "signal")}: {row.bucket}
                  </div>
                  <div className="mt-1 text-xs text-muted-foreground">
                    count {row.count ?? 0} | resolved {row.resolved_count ?? 0} |
                    avg pnl {row.average_estimated_pnl ?? "n/a"}
                  </div>
                </div>
              ))
            ) : (
              <div className="text-sm text-muted-foreground">
                No weak signals were available.
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

function ExecutionAttemptCard({
  attempt,
  refreshing,
  onRefresh,
}: {
  attempt: ExecutionAttempt;
  refreshing: boolean;
  onRefresh: () => void;
}) {
  const primaryOrder =
    attempt.orders.find((order) => !order.parent_broker_order_id) ??
    attempt.orders[0] ??
    null;
  const fillSummary = attempt.fills.length
    ? `${attempt.fills.length} fill${attempt.fills.length === 1 ? "" : "s"} recorded`
    : "No fills recorded yet";

  return (
    <div className="rounded-2xl border border-border/70 bg-background/70 px-4 py-4">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <span className="font-medium">
              {attempt.underlying_symbol} · {attempt.strategy.replaceAll("_", " ")}
            </span>
            <ExecutionStatusBadge value={attempt.status} />
            <Badge
              variant="outline"
              className="rounded-full border-border/70 bg-background/80 px-2.5 py-1 text-[11px] uppercase tracking-[0.16em] text-muted-foreground"
            >
              {attempt.trade_intent}
            </Badge>
          </div>
          <div className="mt-1 text-sm text-muted-foreground">
            {attempt.short_symbol} / {attempt.long_symbol} · qty {attempt.quantity} ·
            limit {formatCurrency(attempt.limit_price)}
          </div>
          <div className="mt-2 flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted-foreground">
            <span>requested {formatTimestamp(attempt.requested_at)}</span>
            <span>
              submitted{" "}
              {attempt.submitted_at ? formatTimestamp(attempt.submitted_at) : "—"}
            </span>
            <span>
              broker order{" "}
              {primaryOrder?.broker_order_id ?? attempt.broker_order_id ?? "pending"}
            </span>
            {attempt.session_position_id ? (
              <span>position {attempt.session_position_id}</span>
            ) : null}
          </div>
        </div>
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={refreshing}
          onClick={onRefresh}
        >
          {refreshing ? <LoaderCircle className="size-3.5 animate-spin" /> : null}
          Refresh broker status
        </Button>
      </div>
      <div className="mt-3 grid gap-3 sm:grid-cols-3">
        <MetricTile
          label="broker status"
          value={readString(primaryOrder?.order_status, attempt.status)}
        />
        <MetricTile label="fills" value={fillSummary} />
        <MetricTile
          label="completed"
          value={attempt.completed_at ? formatTime(attempt.completed_at) : "—"}
          note={
            attempt.completed_at
              ? formatDate(attempt.completed_at)
              : "Awaiting terminal state"
          }
        />
      </div>
      {attempt.orders.length > 1 ? (
        <div className="mt-3 text-sm text-muted-foreground">
          Legs:{" "}
          {attempt.orders
            .filter((order) => order.parent_broker_order_id)
            .map(
              (order) =>
                `${readString(order.leg_symbol, "leg")} ${readString(order.order_status, "unknown")}`,
            )
            .join(" · ")}
        </div>
      ) : null}
      {attempt.fills.length ? (
        <div className="mt-3 flex flex-col gap-2">
          {attempt.fills.slice(0, 4).map((fill) => (
            <div
              key={fill.broker_fill_id}
              className="rounded-xl border border-border/70 px-3 py-2 text-sm text-foreground/80"
            >
              {fill.symbol} · {readString(fill.fill_type, "fill").replaceAll("_", " ")} ·
              qty {fill.quantity} @{" "}
              {fill.price == null ? "—" : formatCurrency(fill.price)} ·{" "}
              {formatTimestamp(fill.filled_at)}
            </div>
          ))}
        </div>
      ) : null}
      {attempt.error_text ? (
        <div className="mt-3 text-sm text-rose-700">{attempt.error_text}</div>
      ) : null}
    </div>
  );
}

export function SessionDetailPageContent({
  sessionId,
}: {
  sessionId: string;
}) {
  const router = useRouter();
  const queryClient = useQueryClient();
  const [selectedPromotableId, setSelectedPromotableId] = useState<string | null>(
    null,
  );
  const [refreshingAttemptId, setRefreshingAttemptId] = useState<string | null>(
    null,
  );
  const [closingPositionId, setClosingPositionId] = useState<string | null>(
    null,
  );

  const sessionsQuery = useQuery({
    queryKey: ["sessions"],
    queryFn: () => getSessions({ limit: 120 }),
  });

  const sessions = sessionsQuery.data?.sessions ?? [];
  const selectedSession =
    sessions.find((session) => session.session_id === sessionId) ??
    sessions[0] ??
    null;
  const selectedSessionId = selectedSession?.session_id ?? null;

  useEffect(() => {
    if (!selectedSessionId || selectedSessionId === sessionId) {
      return;
    }
    startTransition(() => {
      router.replace(buildSessionHref(selectedSessionId), { scroll: false });
    });
  }, [router, selectedSessionId, sessionId]);

  const sessionDetailQuery = useQuery({
    queryKey: ["session", selectedSessionId],
    queryFn: () => getSessionDetail(selectedSessionId ?? ""),
    enabled: Boolean(selectedSessionId),
    refetchInterval: 30_000,
  });

  const session = sessionDetailQuery.data ?? null;
  const latestSlotCapture = session?.latest_slot
    ? quoteCapture(session.latest_slot)
    : {};
  const liveOpportunities = (session?.opportunities ?? []).filter(
    (row) => readString(row.eligibility, "live") === "live",
  );
  const promotableRows = buildCandidateRows(
    liveOpportunities.filter((row) => row.selection_state === "promotable"),
  );
  const monitorRows = buildCandidateRows(
    liveOpportunities.filter((row) => row.selection_state === "monitor"),
  );
  const slotRows = buildSlotRows(session?.slot_runs ?? []);
  const alertRows = buildAlertRows(session?.alerts ?? []);
  const eventRows = buildEventRows(session?.events ?? []);
  const selectedPromotable =
    (selectedPromotableId
      ? promotableRows.find((row) => row.id === selectedPromotableId)?.raw
      : null) ??
    promotableRows[0]?.raw ??
    null;
  const selectedPromotablePayload =
    selectedPromotable && typeof selectedPromotable.candidate === "object"
      ? (selectedPromotable.candidate as Record<string, unknown>)
      : null;
  const workingExecutionCount = (session?.executions ?? []).filter(
    isWorkingExecutionAttempt,
  ).length;
  const filledExecutionCount = (session?.executions ?? []).filter(
    (attempt) => readString(attempt.status, "unknown") === "filled",
  ).length;
  const operatorFocus = buildOperatorFocus({
    session,
    promotableCount: promotableRows.length,
    monitorCount: monitorRows.length,
    selectedPromotable,
  });

  const invalidateSessionQueries = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["account-overview"] }),
      queryClient.invalidateQueries({ queryKey: ["sessions"] }),
      queryClient.invalidateQueries({ queryKey: ["session", selectedSessionId] }),
    ]);
  };

  const submitExecutionMutation = useMutation({
    mutationFn: (candidate: LiveCandidate) =>
      createSessionExecution(selectedSessionId ?? "", {
        candidate_id: candidate.candidate_id,
      }),
    onSuccess: invalidateSessionQueries,
  });

  const refreshExecutionMutation = useMutation({
    mutationFn: (executionAttemptId: string) =>
      refreshSessionExecution(selectedSessionId ?? "", executionAttemptId),
    onMutate: (executionAttemptId) => {
      setRefreshingAttemptId(executionAttemptId);
    },
    onSuccess: invalidateSessionQueries,
    onSettled: () => {
      setRefreshingAttemptId(null);
    },
  });

  const closePositionMutation = useMutation({
    mutationFn: (position: SessionPortfolioPosition) =>
      closeSessionPosition(selectedSessionId ?? "", position.position_id),
    onMutate: (position) => {
      setClosingPositionId(position.position_id);
    },
    onSuccess: invalidateSessionQueries,
    onSettled: () => {
      setClosingPositionId(null);
    },
  });

  const selectSession = (nextSessionId: string) => {
    if (!nextSessionId || nextSessionId === selectedSessionId) {
      return;
    }
    startTransition(() => {
      router.push(buildSessionHref(nextSessionId), { scroll: false });
    });
  };

  if (sessionsQuery.isLoading) {
    return <LoadingState />;
  }

  return (
    <div className="flex flex-col gap-4">
      <div className="rounded-[32px] border border-border/70 bg-[radial-gradient(circle_at_top_left,rgba(120,113,108,0.16),transparent_34%),linear-gradient(145deg,rgba(255,255,255,0.97),rgba(245,245,244,0.9))] px-5 py-5 shadow-[0_40px_120px_-72px_rgba(15,23,42,0.7)] lg:px-6">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-2">
              <Badge
                variant="outline"
                className="rounded-full border-border/70 bg-background/80 px-2.5 py-1 text-[11px] uppercase tracking-[0.18em] text-muted-foreground"
              >
                <CandlestickChart data-icon="inline-start" />
                Session detail
              </Badge>
              {selectedSession ? (
                <SessionStatusBadge value={selectedSession.status} />
              ) : null}
              {selectedSession ? (
                <CaptureStatusBadge value={selectedSession.latest_capture_status} />
              ) : null}
            </div>
            <div className="mt-4 flex flex-col gap-3">
              <div className="flex flex-col gap-3 sm:flex-row sm:items-end">
                <div className="min-w-0 flex-1">
                  <div className="text-[11px] uppercase tracking-[0.24em] text-muted-foreground">
                    Existing sessions
                  </div>
                  <Select
                    value={selectedSessionId ?? undefined}
                    onValueChange={(value) => {
                      if (value) {
                        selectSession(value);
                      }
                    }}
                  >
                    <SelectTrigger className="mt-2 h-auto w-full min-w-0 rounded-2xl border-border/70 bg-background/80 px-4 py-3 text-left">
                      <div className="min-w-0 flex-1">
                        <div className="truncate text-base font-semibold tracking-[0.02em]">
                          {selectedSession?.label ?? "Select a session"}
                        </div>
                        <div className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-muted-foreground">
                          <span>
                            {selectedSession
                              ? formatDate(selectedSession.session_date)
                              : "No persisted sessions were found."}
                          </span>
                          {selectedSession ? (
                            <span className="font-mono">
                              {selectedSession.session_id}
                            </span>
                          ) : null}
                        </div>
                      </div>
                    </SelectTrigger>
                    <SelectContent className="max-h-96">
                      {sessions.map((item) => (
                        <SelectItem key={item.session_id} value={item.session_id}>
                          <span className="flex min-w-0 flex-col">
                            <span className="truncate font-medium">
                              {item.label}
                            </span>
                            <span className="text-xs text-muted-foreground">
                              {formatDate(item.session_date)} | {item.status} |
                              slot {formatTime(item.latest_slot_at)}
                            </span>
                          </span>
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <Button
                  type="button"
                  variant="outline"
                  onClick={() => void sessionsQuery.refetch()}
                >
                  <RefreshCw data-icon="inline-start" />
                  Refresh
                </Button>
                <Link
                  href="/sessions"
                  className={buttonVariants({ variant: "outline" })}
                >
                  All Sessions
                </Link>
              </div>
              <div className="text-sm text-foreground/70">
                {sessions.length
                  ? `${sessions.length} persisted sessions available in storage.`
                  : "No persisted sessions were found in storage."}
              </div>
              <div className="rounded-2xl border border-border/70 bg-background/80 px-4 py-4">
                <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                  Operator focus
                </div>
                <div className="mt-2 text-lg font-medium">
                  {operatorFocus.title}
                </div>
                <div className="mt-1 text-sm text-foreground/70">
                  {operatorFocus.detail}
                </div>
              </div>
            </div>
          </div>
          <div className="grid flex-1 gap-3 sm:grid-cols-2 xl:max-w-[860px] xl:grid-cols-3">
            <MetricTile
              label="Latest slot"
              value={selectedSession ? formatTime(selectedSession.latest_slot_at) : "—"}
              note={selectedSession?.latest_slot_status ?? "No slot"}
            />
            <MetricTile
              label="Open positions"
              value={String(session?.portfolio.summary.open_position_count ?? 0)}
              note={
                session
                  ? `Net ${formatSignedCurrency(session.portfolio.summary.net_pnl_total)}`
                  : "Waiting for session detail"
              }
            />
            <MetricTile
              label="Working execs"
              value={String(workingExecutionCount)}
              note={`${filledExecutionCount} filled / ${session?.executions.length ?? 0} total`}
            />
            <MetricTile
              label="Promotable"
              value={String(promotableRows.length)}
              note={
                selectedPromotable
                  ? `${selectedPromotable.underlying_symbol} selected`
                  : "No promotable action ready"
              }
            />
            <MetricTile
              label="Risk"
              value={readString(session?.risk_status, "unknown").toUpperCase()}
              note={readString(session?.risk_note, "No session risk note")}
            />
            <MetricTile
              label="Reconciliation"
              value={readString(
                session?.reconciliation_status,
                "unknown",
              ).toUpperCase()}
              note={readString(
                session?.reconciliation_note,
                "No reconciliation note",
              )}
            />
          </div>
        </div>
        {sessionDetailQuery.isLoading ? (
          <div className="mt-5 flex items-center gap-2 text-sm text-muted-foreground">
            <LoaderCircle className="size-4 animate-spin" />
            Loading session detail…
          </div>
        ) : null}
        {sessionDetailQuery.isError ? (
          <div className="mt-5 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-900">
            Session detail could not be loaded.
          </div>
        ) : null}
      </div>

      {session ? (
        <>
          <SessionPortfolioSection
            session={session}
            closingPositionId={closingPositionId}
            onClosePosition={(position) => closePositionMutation.mutate(position)}
            closeError={
              closePositionMutation.isError
                ? closePositionMutation.error instanceof Error
                  ? closePositionMutation.error.message
                  : "Could not submit the session close execution."
                : null
            }
            closeMessage={closePositionMutation.data?.message ?? null}
          />

          <SessionActionDesk
            session={session}
            promotableRows={promotableRows}
            selectedPromotable={selectedPromotable}
            selectedPromotablePayload={selectedPromotablePayload}
            selectedPromotableId={
              selectedPromotable ? String(selectedPromotable.candidate_id) : null
            }
            onSelectPromotable={setSelectedPromotableId}
            onExecuteSelected={(candidate) =>
              submitExecutionMutation.mutate(candidate)
            }
            submitPending={submitExecutionMutation.isPending}
            submitError={
              submitExecutionMutation.isError
                ? submitExecutionMutation.error instanceof Error
                  ? submitExecutionMutation.error.message
                  : "Could not submit this live execution."
                : null
            }
            submitMessage={submitExecutionMutation.data?.message ?? null}
            refreshingAttemptId={refreshingAttemptId}
            onRefreshExecution={(executionAttemptId) =>
              refreshExecutionMutation.mutate(executionAttemptId)
            }
            refreshError={
              refreshExecutionMutation.isError
                ? refreshExecutionMutation.error instanceof Error
                  ? refreshExecutionMutation.error.message
                  : "Could not refresh broker execution status."
                : null
            }
            refreshMessage={refreshExecutionMutation.data?.message ?? null}
          />

          <SessionLiveFlow
            monitorRows={monitorRows}
            alertRows={alertRows}
            eventRows={eventRows}
          />

          <div className="grid gap-4 xl:grid-cols-2">
            <SessionCollectorHealth
              session={session}
              latestSlotCapture={latestSlotCapture}
              slotRows={slotRows}
            />
            <SectionSurface
              title="Post-Market Context"
              description="Persisted post-close signal context and tuning for this session."
            >
              <SessionAnalysis session={session} />
            </SectionSurface>
          </div>
        </>
      ) : (
        <SectionSurface
          title="No session selected"
          description="Select a persisted session to inspect slot health, current opportunities, and analysis."
        >
          <div className="flex flex-col items-center justify-center gap-3 px-6 py-16 text-center">
            <Rows3 className="size-10 text-muted-foreground" />
            <div className="text-lg font-medium">No persisted sessions found</div>
            <div className="max-w-[34rem] text-sm text-muted-foreground">
              This page only shows sessions that already exist in storage. Once
              live collector slots persist, they will appear here automatically.
            </div>
          </div>
        </SectionSurface>
      )}
    </div>
  );
}
