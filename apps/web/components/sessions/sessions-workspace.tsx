"use client";

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ColumnDef } from "@tanstack/react-table";
import {
  Activity,
  BellRing,
  CandlestickChart,
  LoaderCircle,
  RefreshCw,
  Rows3,
  Wallet,
} from "lucide-react";
import { trim } from "lodash-es";
import { useRouter, useSearchParams } from "next/navigation";
import { startTransition, useEffect, useState } from "react";

import { DataTable } from "@/components/data-table";
import {
  type AccountHistory,
  type AccountHistoryPoint,
  type AccountHistoryRange,
  type AccountOverview,
  type AccountPosition,
  type AlertRecord,
  buildSessionHref,
  closeSessionPosition,
  createSessionExecution,
  type ExecutionAttempt,
  type JobRun,
  type LiveCandidate,
  type LiveEvent,
  getAccountOverview,
  refreshSessionExecution,
  type SessionDetail,
  type SessionPortfolioPosition,
  getSessionDetail,
  getSessions,
} from "@/lib/api";
import {
  formatCalendarDate,
  formatElapsedDuration,
  formatLocalDateTime,
  formatLocalTime,
} from "@/lib/date";
import { cn } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";

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
  websocketCount: number;
  baselineCount: number;
  recoveryCount: number;
  retryCount: number;
  duration: string;
  workerName: string;
  raw: JobRun;
};

type AlertRow = {
  id: string;
  createdAt: string;
  symbol: string;
  alertType: string;
  status: string;
  raw: AlertRecord;
};

type EventRow = {
  id: string;
  generatedAt: string;
  symbol: string;
  eventType: string;
  message: string;
  raw: LiveEvent;
};

type AccountPositionRow = {
  id: string;
  symbol: string;
  side: string;
  quantity: number | null | undefined;
  averageEntryPrice: number | null | undefined;
  currentPrice: number | null | undefined;
  marketValue: number | null | undefined;
  intradayPnl: number | null | undefined;
  intradayPnlPercent: number | null | undefined;
  openPnl: number | null | undefined;
  openPnlPercent: number | null | undefined;
  raw: AccountPosition;
};

type SessionPortfolioPositionRow = {
  id: string;
  symbol: string;
  strategy: string;
  status: string;
  brokerStatus: string;
  openedQuantity: number | null | undefined;
  remainingQuantity: number | null | undefined;
  entryCredit: number | null | undefined;
  closeMark: number | null | undefined;
  realizedPnl: number | null | undefined;
  unrealizedPnl: number | null | undefined;
  netPnl: number | null | undefined;
  maxLoss: number | null | undefined;
  openedAt: string | null | undefined;
  closedAt: string | null | undefined;
  raw: SessionPortfolioPosition;
};

const ACCOUNT_HISTORY_RANGES: AccountHistoryRange[] = ["1D", "1W", "1M"];

function readNumber(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }
  return fallback;
}

function readString(value: unknown, fallback = "—"): string {
  return typeof value === "string" && trim(value) !== "" ? value : fallback;
}

function formatDate(value: string | null | undefined): string {
  return formatCalendarDate(value);
}

function formatTime(value: string | null | undefined): string {
  return formatLocalTime(value);
}

function formatTimestamp(value: string | null | undefined): string {
  return formatLocalDateTime(value);
}

function formatCurrency(value: number): string {
  return new Intl.NumberFormat(undefined, {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

function formatNullableCurrency(value: number | null | undefined): string {
  return value == null ? "—" : formatCurrency(value);
}

function formatSignedCurrency(value: number | null | undefined): string {
  if (value == null) {
    return "—";
  }
  return new Intl.NumberFormat(undefined, {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
    signDisplay: "exceptZero",
  }).format(value);
}

function formatScore(value: number): string {
  return value.toFixed(1);
}

function formatQuantity(value: number | null | undefined): string {
  if (value == null) {
    return "—";
  }
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: 2,
  }).format(value);
}

function formatSignedPercent(value: number | null | undefined): string {
  if (value == null) {
    return "—";
  }
  const percent = value * 100;
  return `${percent > 0 ? "+" : ""}${percent.toFixed(2)}%`;
}

function formatDuration(raw: JobRun): string {
  return formatElapsedDuration(raw.started_at, raw.finished_at);
}

function statusTone(value: string): string {
  switch (value) {
    case "running":
      return "border-sky-200 bg-sky-100 text-sky-900";
    case "healthy":
    case "succeeded":
      return "border-emerald-200 bg-emerald-100 text-emerald-900";
    case "degraded":
    case "skipped":
      return "border-amber-200 bg-amber-100 text-amber-900";
    case "failed":
    case "empty":
      return "border-rose-200 bg-rose-100 text-rose-900";
    default:
      return "border-border/70 bg-card text-foreground";
  }
}

function captureTone(value: string): string {
  switch (value) {
    case "healthy":
      return "border-emerald-200 bg-emerald-100 text-emerald-900";
    case "baseline_only":
      return "border-amber-200 bg-amber-100 text-amber-900";
    case "recovery_only":
      return "border-orange-200 bg-orange-100 text-orange-900";
    case "empty":
      return "border-rose-200 bg-rose-100 text-rose-900";
    default:
      return "border-border/70 bg-card text-foreground";
  }
}

function valueTone(value: number | null | undefined): string {
  if (value == null) {
    return "text-muted-foreground";
  }
  if (value > 0) {
    return "text-emerald-700";
  }
  if (value < 0) {
    return "text-rose-700";
  }
  return "text-foreground/80";
}

function SessionStatusBadge({ value }: { value: string | null | undefined }) {
  const resolved = readString(value, "idle");
  return (
    <Badge variant="outline" className={cn("rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em]", statusTone(resolved))}>
      {resolved.replaceAll("_", " ")}
    </Badge>
  );
}

function CaptureStatusBadge({ value }: { value: string | null | undefined }) {
  const resolved = readString(value, "unknown");
  return (
    <Badge variant="outline" className={cn("rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em]", captureTone(resolved))}>
      {resolved.replaceAll("_", " ")}
    </Badge>
  );
}

function executionTone(value: string): string {
  switch (value) {
    case "filled":
      return "border-emerald-200 bg-emerald-100 text-emerald-900";
    case "partially_filled":
      return "border-sky-200 bg-sky-100 text-sky-900";
    case "canceled":
    case "done_for_day":
    case "expired":
      return "border-amber-200 bg-amber-100 text-amber-900";
    case "failed":
    case "rejected":
      return "border-rose-200 bg-rose-100 text-rose-900";
    default:
      return "border-border/70 bg-card text-foreground";
  }
}

function ExecutionStatusBadge({ value }: { value: string | null | undefined }) {
  const resolved = readString(value, "unknown");
  return (
    <Badge variant="outline" className={cn("rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em]", executionTone(resolved))}>
      {resolved.replaceAll("_", " ")}
    </Badge>
  );
}

function portfolioTone(value: string): string {
  switch (value) {
    case "open":
      return "border-emerald-200 bg-emerald-100 text-emerald-900";
    case "partial_close":
    case "partial_open":
      return "border-sky-200 bg-sky-100 text-sky-900";
    case "closed":
      return "border-stone-200 bg-stone-100 text-stone-900";
    default:
      return "border-border/70 bg-card text-foreground";
  }
}

function PortfolioStatusBadge({ value }: { value: string | null | undefined }) {
  const resolved = readString(value, "unknown");
  return (
    <Badge variant="outline" className={cn("rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em]", portfolioTone(resolved))}>
      {resolved.replaceAll("_", " ")}
    </Badge>
  );
}

function AccountEnvironmentBadge({ value }: { value: string | null | undefined }) {
  const resolved = readString(value, "custom");
  const tone =
    resolved === "paper"
      ? "border-sky-200 bg-sky-100 text-sky-900"
      : resolved === "live"
        ? "border-amber-200 bg-amber-100 text-amber-900"
        : "border-border/70 bg-card text-foreground";
  return (
    <Badge variant="outline" className={cn("rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em]", tone)}>
      {resolved}
    </Badge>
  );
}

function MetricTile({
  label,
  value,
  note,
}: {
  label: string;
  value: string;
  note?: string;
}) {
  return (
    <div className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
      <div className="text-[11px] uppercase tracking-[0.2em] text-muted-foreground">{label}</div>
      <div className="mt-2 text-xl font-semibold">{value}</div>
      {note ? <div className="mt-1 text-xs text-muted-foreground">{note}</div> : null}
    </div>
  );
}

function SectionSurface({
  title,
  description,
  children,
}: {
  title: string;
  description?: string;
  children: React.ReactNode;
}) {
  return (
    <section className="rounded-[28px] border border-border/70 bg-card/80 shadow-[0_30px_90px_-54px_rgba(15,23,42,0.55)]">
      <div className="border-b border-border/70 px-5 py-4">
        <div className="text-[11px] uppercase tracking-[0.22em] text-muted-foreground">{title}</div>
        {description ? <div className="mt-1 text-sm text-foreground/70">{description}</div> : null}
      </div>
      <div className="px-4 py-4 md:px-5">{children}</div>
    </section>
  );
}

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
  return candidate && typeof candidate === "object" ? (candidate as Record<string, unknown>) : {};
}

function buildSlotRows(slotRuns: JobRun[]): SlotRow[] {
  return slotRuns.map((run) => {
    const capture = quoteCapture(run);
    return {
      id: run.job_run_id,
      slotAt: readString(run.slot_at ?? run.scheduled_for ?? run.started_at ?? run.finished_at, ""),
      status: run.status,
      captureStatus: readString(capture.capture_status, "unknown"),
      websocketCount: readNumber(capture.websocket_quote_events_saved),
      baselineCount: readNumber(capture.baseline_quote_events_saved),
      recoveryCount: readNumber(capture.recovery_quote_events_saved),
      retryCount: readNumber(run.retry_count),
      duration: formatDuration(run),
      workerName: readString(run.worker_name, "—"),
      raw: run,
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
    raw: alert,
  }));
}

function buildEventRows(events: LiveEvent[]): EventRow[] {
  return [...events]
    .reverse()
    .map((event, index) => ({
      id: `${event.cycle_id}:${event.symbol}:${event.generated_at}:${index}`,
      generatedAt: event.generated_at,
      symbol: event.symbol,
      eventType: event.event_type,
      message: event.message,
      raw: event,
    }));
}

function buildAccountPositionRows(positions: AccountPosition[]): AccountPositionRow[] {
  return positions.map((position) => ({
    id: position.asset_id ?? position.symbol,
    symbol: position.symbol,
    side: readString(position.side, "flat"),
    quantity: position.qty,
    averageEntryPrice: position.avg_entry_price,
    currentPrice: position.current_price,
    marketValue: position.market_value,
    intradayPnl: position.unrealized_intraday_pl,
    intradayPnlPercent: position.unrealized_intraday_plpc,
    openPnl: position.unrealized_pl,
    openPnlPercent: position.unrealized_plpc,
    raw: position,
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
    openedQuantity: position.opened_quantity ?? position.filled_quantity,
    remainingQuantity: position.remaining_quantity ?? position.filled_quantity,
    entryCredit: position.entry_credit,
    closeMark: position.spread_mark_close,
    realizedPnl: position.realized_pnl,
    unrealizedPnl: position.unrealized_pnl ?? position.estimated_close_pnl,
    netPnl: position.net_pnl,
    maxLoss: position.max_loss,
    openedAt: position.opened_at,
    closedAt: position.closed_at,
    raw: position,
  }));
}

const CANDIDATE_COLUMNS: ColumnDef<CandidateRow>[] = [
  {
    accessorKey: "symbol",
    header: "Symbol",
    cell: ({ row }) => <span className="font-semibold tracking-[0.04em]">{row.original.symbol}</span>,
  },
  {
    accessorKey: "strategy",
    header: "Side",
    cell: ({ getValue }) => <span className="capitalize text-foreground/80">{String(getValue())}</span>,
  },
  {
    accessorKey: "expirationDate",
    header: "Expiry",
    cell: ({ getValue }) => <span className="text-muted-foreground">{formatDate(String(getValue()))}</span>,
  },
  {
    accessorKey: "strikes",
    header: "Strikes",
    cell: ({ getValue }) => <span className="font-mono text-[12px]">{String(getValue())}</span>,
  },
  {
    accessorKey: "score",
    header: "Score",
    cell: ({ getValue }) => <span className="font-mono">{formatScore(Number(getValue()))}</span>,
  },
  {
    accessorKey: "credit",
    header: "Credit",
    cell: ({ getValue }) => <span className="font-mono">{formatCurrency(Number(getValue()))}</span>,
  },
];

const SLOT_COLUMNS: ColumnDef<SlotRow>[] = [
  {
    accessorKey: "slotAt",
    header: "Slot",
    cell: ({ getValue }) => <span className="font-mono text-[12px]">{formatTimestamp(String(getValue()))}</span>,
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
    accessorKey: "websocketCount",
    header: "WS",
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
    cell: ({ getValue }) => <span className="font-mono text-muted-foreground">{String(getValue())}</span>,
  },
  {
    accessorKey: "workerName",
    header: "Worker",
    cell: ({ getValue }) => <span className="text-muted-foreground">{String(getValue())}</span>,
  },
];

const ALERT_COLUMNS: ColumnDef<AlertRow>[] = [
  {
    accessorKey: "createdAt",
    header: "Time",
    cell: ({ getValue }) => <span className="font-mono text-[12px]">{formatTimestamp(String(getValue()))}</span>,
  },
  {
    accessorKey: "symbol",
    header: "Symbol",
    cell: ({ getValue }) => <span className="font-semibold">{String(getValue())}</span>,
  },
  {
    accessorKey: "alertType",
    header: "Type",
    cell: ({ getValue }) => <span className="text-foreground/80">{String(getValue()).replaceAll("_", " ")}</span>,
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
    cell: ({ getValue }) => <span className="font-mono text-[12px]">{formatTimestamp(String(getValue()))}</span>,
  },
  {
    accessorKey: "symbol",
    header: "Symbol",
    cell: ({ getValue }) => <span className="font-semibold">{String(getValue())}</span>,
  },
  {
    accessorKey: "eventType",
    header: "Event",
    cell: ({ getValue }) => <span className="text-foreground/80">{String(getValue()).replaceAll("_", " ")}</span>,
  },
  {
    accessorKey: "message",
    header: "Message",
    cell: ({ getValue }) => <span className="text-foreground/70">{String(getValue())}</span>,
  },
];

const ACCOUNT_POSITION_COLUMNS: ColumnDef<AccountPositionRow>[] = [
  {
    accessorKey: "symbol",
    header: "Symbol",
    cell: ({ row }) => <span className="font-mono text-[12px] font-semibold">{row.original.symbol}</span>,
  },
  {
    accessorKey: "side",
    header: "Side",
    cell: ({ getValue }) => <span className="capitalize text-foreground/80">{String(getValue())}</span>,
  },
  {
    accessorKey: "quantity",
    header: "Qty",
    cell: ({ getValue }) => <span className="font-mono">{formatQuantity(getValue<number | null | undefined>())}</span>,
  },
  {
    accessorKey: "averageEntryPrice",
    header: "Entry",
    cell: ({ getValue }) => <span className="font-mono">{formatNullableCurrency(getValue<number | null | undefined>())}</span>,
  },
  {
    accessorKey: "currentPrice",
    header: "Mark",
    cell: ({ getValue }) => <span className="font-mono">{formatNullableCurrency(getValue<number | null | undefined>())}</span>,
  },
  {
    accessorKey: "marketValue",
    header: "Market Value",
    cell: ({ getValue }) => <span className="font-mono">{formatNullableCurrency(getValue<number | null | undefined>())}</span>,
  },
  {
    accessorKey: "intradayPnl",
    header: "Day PnL",
    cell: ({ row }) => (
      <div className={cn("font-mono", valueTone(row.original.intradayPnl))}>
        <div>{formatSignedCurrency(row.original.intradayPnl)}</div>
        <div className="text-[11px]">{formatSignedPercent(row.original.intradayPnlPercent)}</div>
      </div>
    ),
  },
  {
    accessorKey: "openPnl",
    header: "Open PnL",
    cell: ({ row }) => (
      <div className={cn("font-mono", valueTone(row.original.openPnl))}>
        <div>{formatSignedCurrency(row.original.openPnl)}</div>
        <div className="text-[11px]">{formatSignedPercent(row.original.openPnlPercent)}</div>
      </div>
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
      <div>
        <div className="font-semibold tracking-[0.04em]">{row.original.symbol}</div>
        <div className="text-[11px] text-muted-foreground">
          {row.original.raw.short_symbol} / {row.original.raw.long_symbol}
        </div>
      </div>
    ),
  },
  {
    accessorKey: "strategy",
    header: "Setup",
    cell: ({ row }) => (
      <div>
        <div className="capitalize text-foreground/80">{row.original.strategy.replaceAll("_", " ")}</div>
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
      <span className="font-mono">{formatNullableCurrency(getValue<number | null | undefined>())}</span>
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
      <span className="font-mono">{formatNullableCurrency(getValue<number | null | undefined>())}</span>
    ),
  },
  {
    accessorKey: "openedAt",
    header: "Opened",
    cell: ({ row }) => (
      <div className="font-mono text-[12px]">
        <div>{formatTimestamp(row.original.openedAt)}</div>
        <div className="text-muted-foreground">{formatTimestamp(row.original.closedAt)}</div>
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

function LoadingShell() {
  return (
    <div className="flex flex-col gap-4">
      <Skeleton className="h-48 w-full rounded-[28px]" />
      <Skeleton className="h-72 w-full rounded-[28px]" />
      <Skeleton className="h-72 w-full rounded-[28px]" />
    </div>
  );
}

function maskAccountNumber(value: string | null | undefined): string {
  const normalized = trim(value ?? "");
  if (!normalized) {
    return "—";
  }
  if (normalized.length <= 4) {
    return normalized;
  }
  return `••••${normalized.slice(-4)}`;
}

function buildChartCoordinates(points: AccountHistoryPoint[], width: number, height: number, padding: number) {
  const validPoints = points.filter(
    (point): point is AccountHistoryPoint & { equity: number } => typeof point.equity === "number" && Number.isFinite(point.equity),
  );
  if (!validPoints.length) {
    return null;
  }
  const min = Math.min(...validPoints.map((point) => point.equity));
  const max = Math.max(...validPoints.map((point) => point.equity));
  const range = max - min || 1;
  const step = validPoints.length === 1 ? 0 : (width - padding * 2) / (validPoints.length - 1);
  const coordinates = validPoints.map((point, index) => {
    const x = padding + step * index;
    const y = height - padding - ((point.equity - min) / range) * (height - padding * 2);
    return { x, y, point };
  });
  const line = coordinates.map(({ x, y }) => `${x},${y}`).join(" ");
  const area = [
    `${padding},${height - padding}`,
    ...coordinates.map(({ x, y }) => `${x},${y}`),
    `${coordinates[coordinates.length - 1]?.x ?? width - padding},${height - padding}`,
  ].join(" ");
  return {
    min,
    max,
    start: validPoints[0].equity,
    end: validPoints[validPoints.length - 1].equity,
    line,
    area,
  };
}

function AccountHistoryChart({ history }: { history: AccountHistory }) {
  const width = 720;
  const height = 220;
  const padding = 16;
  const chart = buildChartCoordinates(history.points, width, height, padding);

  if (!chart) {
    return (
      <div className="rounded-2xl border border-dashed border-border/70 px-4 py-8 text-sm text-muted-foreground">
        Alpaca did not return equity history points for this range.
      </div>
    );
  }

  return (
    <div className="rounded-2xl border border-border/70 bg-background/70 px-4 py-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Equity curve</div>
          <div className={cn("mt-2 text-2xl font-semibold", valueTone(chart.end - chart.start))}>
            {formatSignedCurrency(chart.end - chart.start)}
          </div>
          <div className="mt-1 text-sm text-muted-foreground">
            {history.range} range · {readString(history.timeframe, "—")} candles
          </div>
        </div>
        <div className="grid gap-2 sm:grid-cols-3">
          <MetricTile label="start" value={formatNullableCurrency(chart.start)} />
          <MetricTile label="high" value={formatNullableCurrency(chart.max)} />
          <MetricTile label="low" value={formatNullableCurrency(chart.min)} />
        </div>
      </div>
      <div className="mt-4 overflow-hidden rounded-2xl border border-border/70 bg-gradient-to-b from-stone-100/80 to-background">
        <svg viewBox={`0 0 ${width} ${height}`} className="h-56 w-full">
          <defs>
            <linearGradient id="account-history-fill" x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor="rgb(41 37 36 / 0.22)" />
              <stop offset="100%" stopColor="rgb(41 37 36 / 0.02)" />
            </linearGradient>
          </defs>
          <polyline points={chart.area} fill="url(#account-history-fill)" stroke="none" />
          <polyline
            points={chart.line}
            fill="none"
            stroke="rgb(68 64 60)"
            strokeWidth="3"
            strokeLinejoin="round"
            strokeLinecap="round"
          />
        </svg>
      </div>
    </div>
  );
}

function AccountOverviewSection({
  overview,
  historyRange,
  onHistoryRangeChange,
  refreshing,
  onRefresh,
  error,
}: {
  overview: AccountOverview | null;
  historyRange: AccountHistoryRange;
  onHistoryRangeChange: (nextRange: AccountHistoryRange) => void;
  refreshing: boolean;
  onRefresh: () => void;
  error: string | null;
}) {
  const positionRows = buildAccountPositionRows(overview?.positions ?? []);

  return (
    <SectionSurface title="Broker Account" description="Read-only Alpaca balances, open positions, and portfolio equity history.">
      {overview ? (
        <div className="flex flex-col gap-4">
          <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="outline" className="rounded-full border-border/70 bg-background/80 px-2.5 py-1 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                  <Wallet data-icon="inline-start" />
                  Broker snapshot
                </Badge>
                <AccountEnvironmentBadge value={overview.environment} />
                {overview.account.status ? <SessionStatusBadge value={overview.account.status.toLowerCase()} /> : null}
              </div>
              <div className="mt-3 text-sm text-foreground/70">
                Account {maskAccountNumber(overview.account.account_number)} · refreshed {formatTimestamp(overview.retrieved_at)}
              </div>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              {ACCOUNT_HISTORY_RANGES.map((range) => (
                <Button
                  key={range}
                  type="button"
                  size="sm"
                  variant={historyRange === range ? "secondary" : "outline"}
                  onClick={() => onHistoryRangeChange(range)}
                >
                  {range}
                </Button>
              ))}
              <Button type="button" variant="outline" size="sm" disabled={refreshing} onClick={onRefresh}>
                {refreshing ? <LoaderCircle className="size-3.5 animate-spin" /> : <RefreshCw className="size-3.5" />}
                Refresh
              </Button>
            </div>
          </div>

          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
            <MetricTile
              label="equity"
              value={formatNullableCurrency(overview.account.equity)}
              note={`Prev close ${formatNullableCurrency(overview.account.last_equity)}`}
            />
            <MetricTile
              label="day pnl"
              value={formatSignedCurrency(overview.pnl.day_change)}
              note={formatSignedPercent(overview.pnl.day_change_percent)}
            />
            <MetricTile
              label="buying power"
              value={formatNullableCurrency(overview.account.buying_power)}
              note={`Options ${formatNullableCurrency(overview.account.options_buying_power)}`}
            />
            <MetricTile
              label="cash"
              value={formatNullableCurrency(overview.account.cash)}
              note={`Day trades ${overview.account.daytrade_count ?? "—"}`}
            />
            <MetricTile
              label="positions"
              value={String(overview.positions.length)}
              note="Currently open broker positions"
            />
            <MetricTile
              label="account status"
              value={readString(overview.account.status, "—").toUpperCase()}
              note={overview.account.trading_blocked ? "Trading blocked" : "Trading enabled"}
            />
          </div>

          <div className="grid gap-4 xl:grid-cols-[minmax(0,1.1fr)_minmax(0,1fr)]">
            <AccountHistoryChart history={overview.history} />
            <div className="rounded-2xl border border-border/70 bg-background/70 px-4 py-4">
              <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Open positions</div>
              <div className="mt-3">
                <DataTable
                  columns={ACCOUNT_POSITION_COLUMNS}
                  data={positionRows}
                  getRowId={(row) => row.id}
                  emptyMessage="No open broker positions were reported by Alpaca."
                />
              </div>
            </div>
          </div>
        </div>
      ) : error ? (
        <div className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-900">
          {error}
        </div>
      ) : (
        <div className="grid gap-3 md:grid-cols-3">
          <Skeleton className="h-28 rounded-2xl" />
          <Skeleton className="h-28 rounded-2xl" />
          <Skeleton className="h-28 rounded-2xl" />
          <Skeleton className="h-72 rounded-2xl md:col-span-2" />
          <Skeleton className="h-72 rounded-2xl" />
        </div>
      )}
    </SectionSurface>
  );
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
  const columns = buildSessionPortfolioPositionColumns({
    closingPositionId,
    onClose: onClosePosition,
  });

  return (
    <SectionSurface
      title="Trade PnL"
      description="Day-scoped session positions backed by persisted executions, with realized PnL from closes and live quote marks layered on top."
    >
      {!positionRows.length ? (
        <div className="rounded-2xl border border-dashed border-border/70 px-4 py-8 text-sm text-muted-foreground">
          No session positions were opened for this session yet.
        </div>
      ) : (
        <div className="flex flex-col gap-4">
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
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
          </div>
          {summary.mark_error ? (
            <div className="rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-950">
              Quote enrichment was partial: {summary.mark_error}
            </div>
          ) : null}
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
          note="Persisted board + watchlist ideas"
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
          <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Strongest signals</div>
          <div className="mt-3 flex flex-col gap-2">
            {strongest.length ? (
              strongest.slice(0, 5).map((row, index) => (
                <div key={`${readString(row.dimension, "signal")}-${row.bucket}-${index}`} className="rounded-xl border border-border/70 px-3 py-2">
                  <div className="text-sm font-medium">{readString(row.dimension, "signal")}: {row.bucket}</div>
                  <div className="mt-1 text-xs text-muted-foreground">
                    count {row.count ?? 0} | resolved {row.resolved_count ?? 0} | avg pnl {row.average_estimated_pnl ?? "n/a"}
                  </div>
                </div>
              ))
            ) : (
              <div className="text-sm text-muted-foreground">No tuned signals were available.</div>
            )}
          </div>
        </div>
        <div className="rounded-2xl border border-border/70 bg-background/70 p-4">
          <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Weakest signals</div>
          <div className="mt-3 flex flex-col gap-2">
            {weakest.length ? (
              weakest.slice(0, 5).map((row, index) => (
                <div key={`${readString(row.dimension, "signal")}-${row.bucket}-${index}`} className="rounded-xl border border-border/70 px-3 py-2">
                  <div className="text-sm font-medium">{readString(row.dimension, "signal")}: {row.bucket}</div>
                  <div className="mt-1 text-xs text-muted-foreground">
                    count {row.count ?? 0} | resolved {row.resolved_count ?? 0} | avg pnl {row.average_estimated_pnl ?? "n/a"}
                  </div>
                </div>
              ))
            ) : (
              <div className="text-sm text-muted-foreground">No weak signals were available.</div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export function SessionsWorkspace() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const queryClient = useQueryClient();
  const [accountHistoryRange, setAccountHistoryRange] = useState<AccountHistoryRange>("1D");
  const [selectedBoardId, setSelectedBoardId] = useState<string | null>(null);
  const [refreshingAttemptId, setRefreshingAttemptId] = useState<string | null>(null);
  const [closingPositionId, setClosingPositionId] = useState<string | null>(null);

  const accountOverviewQuery = useQuery({
    queryKey: ["account-overview", accountHistoryRange],
    queryFn: () => getAccountOverview(accountHistoryRange),
    refetchInterval: 30_000,
  });

  const sessionsQuery = useQuery({
    queryKey: ["sessions"],
    queryFn: () => getSessions({ limit: 120 }),
  });

  const accountOverview = accountOverviewQuery.data ?? null;
  const sessions = sessionsQuery.data?.sessions ?? [];
  const requestedSessionId = searchParams.get("session_id");
  const selectedSession =
    sessions.find((session) => session.session_id === requestedSessionId) ?? sessions[0] ?? null;
  const selectedSessionId = selectedSession?.session_id ?? null;

  useEffect(() => {
    if (!selectedSessionId || selectedSessionId === requestedSessionId) {
      return;
    }
    startTransition(() => {
      router.replace(buildSessionHref(selectedSessionId), { scroll: false });
    });
  }, [requestedSessionId, router, selectedSessionId]);

  const sessionDetailQuery = useQuery({
    queryKey: ["session", selectedSessionId],
    queryFn: () => getSessionDetail(selectedSessionId ?? ""),
    enabled: Boolean(selectedSessionId),
    refetchInterval: 30_000,
  });

  const session = sessionDetailQuery.data ?? null;
  const latestSlotCapture = session?.latest_slot ? quoteCapture(session.latest_slot) : {};
  const boardRows = buildCandidateRows(session?.board_candidates ?? []);
  const watchlistRows = buildCandidateRows(session?.watchlist_candidates ?? []);
  const slotRows = buildSlotRows(session?.slot_runs ?? []);
  const alertRows = buildAlertRows(session?.alerts ?? []);
  const eventRows = buildEventRows(session?.events ?? []);
  const selectedBoardCandidate =
    (selectedBoardId ? boardRows.find((row) => row.id === selectedBoardId)?.raw : null) ??
    boardRows[0]?.raw ??
    null;
  const selectedBoardCandidatePayload =
    selectedBoardCandidate && typeof selectedBoardCandidate.candidate === "object"
      ? (selectedBoardCandidate.candidate as Record<string, unknown>)
      : null;

  const submitExecutionMutation = useMutation({
    mutationFn: (candidate: LiveCandidate) =>
      createSessionExecution(selectedSessionId ?? "", { candidate_id: candidate.candidate_id }),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["account-overview"] }),
        queryClient.invalidateQueries({ queryKey: ["sessions"] }),
        queryClient.invalidateQueries({ queryKey: ["session", selectedSessionId] }),
      ]);
    },
  });

  const refreshExecutionMutation = useMutation({
    mutationFn: (executionAttemptId: string) =>
      refreshSessionExecution(selectedSessionId ?? "", executionAttemptId),
    onMutate: (executionAttemptId) => {
      setRefreshingAttemptId(executionAttemptId);
    },
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["account-overview"] }),
        queryClient.invalidateQueries({ queryKey: ["sessions"] }),
        queryClient.invalidateQueries({ queryKey: ["session", selectedSessionId] }),
      ]);
    },
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
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["account-overview"] }),
        queryClient.invalidateQueries({ queryKey: ["sessions"] }),
        queryClient.invalidateQueries({ queryKey: ["session", selectedSessionId] }),
      ]);
    },
    onSettled: () => {
      setClosingPositionId(null);
    },
  });

  const selectSession = (sessionId: string) => {
    if (!sessionId || sessionId === selectedSessionId) {
      return;
    }
    startTransition(() => {
      router.push(buildSessionHref(sessionId), { scroll: false });
    });
  };

  if (sessionsQuery.isLoading) {
    return (
      <main className="mx-auto max-w-[1680px] px-4 py-6 lg:px-6">
        <LoadingShell />
      </main>
    );
  }

  return (
    <main className="mx-auto max-w-[1680px] px-4 py-6 lg:px-6">
      <div className="flex flex-col gap-4">
          <div className="rounded-[32px] border border-border/70 bg-[radial-gradient(circle_at_top_left,rgba(120,113,108,0.16),transparent_34%),linear-gradient(145deg,rgba(255,255,255,0.97),rgba(245,245,244,0.9))] px-5 py-5 shadow-[0_40px_120px_-72px_rgba(15,23,42,0.7)] lg:px-6">
            <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
              <div className="min-w-0">
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant="outline" className="rounded-full border-border/70 bg-background/80 px-2.5 py-1 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                    <CandlestickChart data-icon="inline-start" />
                    Sessions workspace
                  </Badge>
                  {selectedSession ? <SessionStatusBadge value={selectedSession.status} /> : null}
                  {selectedSession ? <CaptureStatusBadge value={selectedSession.latest_capture_status} /> : null}
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
                              <span>{selectedSession ? formatDate(selectedSession.session_date) : "No persisted sessions were found."}</span>
                              {selectedSession ? <span className="font-mono">{selectedSession.session_id}</span> : null}
                            </div>
                          </div>
                        </SelectTrigger>
                        <SelectContent className="max-h-96">
                          {sessions.map((item) => (
                            <SelectItem key={item.session_id} value={item.session_id}>
                              <span className="flex min-w-0 flex-col">
                                <span className="truncate font-medium">{item.label}</span>
                                <span className="text-xs text-muted-foreground">
                                  {formatDate(item.session_date)} | {item.status} | slot {formatTime(item.latest_slot_at)}
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
                      className={buttonVariants({ variant: "outline" })}
                      onClick={() => void sessionsQuery.refetch()}
                    >
                      <RefreshCw data-icon="inline-start" />
                      Refresh
                    </Button>
                  </div>
                  <div className="text-sm text-foreground/70">
                    {sessions.length
                      ? `${sessions.length} persisted sessions available in storage.`
                      : "No persisted sessions were found in storage."}
                  </div>
                </div>
              </div>
              <div className="grid flex-1 gap-3 sm:grid-cols-2 xl:max-w-[720px] xl:grid-cols-4">
                <MetricTile
                  label="Latest slot"
                  value={selectedSession ? formatTime(selectedSession.latest_slot_at) : "—"}
                  note={selectedSession?.latest_slot_status ?? "No slot"}
                />
                <MetricTile
                  label="Board"
                  value={String(selectedSession?.board_count ?? 0)}
                  note="Current board candidates"
                />
                <MetricTile
                  label="Watchlist"
                  value={String(selectedSession?.watchlist_count ?? 0)}
                  note="Current watchlist candidates"
                />
                <MetricTile
                  label="Alerts"
                  value={String(selectedSession?.alert_count ?? 0)}
                  note="Recorded alert events"
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

          <AccountOverviewSection
            overview={accountOverview}
            historyRange={accountHistoryRange}
            onHistoryRangeChange={(nextRange) => {
              startTransition(() => {
                setAccountHistoryRange(nextRange);
              });
            }}
            refreshing={accountOverviewQuery.isFetching}
            onRefresh={() => {
              void accountOverviewQuery.refetch();
            }}
            error={
              accountOverviewQuery.isError
                ? "Broker account data could not be loaded from Alpaca."
                : null
            }
          />

          {session ? (
            <>
              <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                <MetricTile
                  label="Capture mix"
                  value={`${readNumber(latestSlotCapture.websocket_quote_events_saved)} ws`}
                  note={`${readNumber(latestSlotCapture.baseline_quote_events_saved)} baseline / ${readNumber(latestSlotCapture.recovery_quote_events_saved)} recovery`}
                />
                <MetricTile
                  label="Updated"
                  value={formatTime(session.updated_at)}
                  note={formatDate(session.updated_at)}
                />
                <MetricTile
                  label="Slot runs"
                  value={String(session.slot_runs.length)}
                  note="Persisted intraday slots"
                />
                <MetricTile
                  label="Events"
                  value={String(session.events.length)}
                  note="Collector events for this session"
                />
              </div>

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

              <SectionSurface title="Board" description="Current board candidates from the latest successful cycle.">
                <div className="flex flex-col gap-4">
                  <DataTable
                    columns={CANDIDATE_COLUMNS}
                    data={boardRows}
                    getRowId={(row) => row.id}
                    selectedId={selectedBoardCandidate ? String(selectedBoardCandidate.candidate_id) : undefined}
                    onSelect={(row) => setSelectedBoardId(row.id)}
                    emptyMessage="No board candidates were persisted for this session."
                  />
                  {selectedBoardCandidate ? (
                    <div className="rounded-2xl border border-border/70 bg-background/70 px-4 py-4">
                      <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                        <div className="min-w-0">
                          <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                            Selected live candidate
                          </div>
                          <div className="mt-2 text-lg font-medium">
                            {selectedBoardCandidate.underlying_symbol} · {selectedBoardCandidate.strategy.replaceAll("_", " ")}
                          </div>
                          <div className="mt-1 text-sm text-muted-foreground">
                            {selectedBoardCandidate.short_symbol} / {selectedBoardCandidate.long_symbol} · expires {formatDate(selectedBoardCandidate.expiration_date)}
                          </div>
                          <div className="mt-3 grid gap-2 sm:grid-cols-3">
                            <MetricTile label="score" value={formatScore(selectedBoardCandidate.quality_score)} />
                            <MetricTile label="credit" value={formatCurrency(selectedBoardCandidate.midpoint_credit)} />
                            <MetricTile
                              label="limit"
                              value={formatCurrency(
                                readNumber(
                                  selectedBoardCandidatePayload?.limit_price ??
                                    selectedBoardCandidatePayload?.midpoint_credit,
                                ),
                              )}
                              note="Persisted candidate payload"
                            />
                          </div>
                        </div>
                        <div className="flex flex-col gap-2 lg:items-end">
                          <Button
                            type="button"
                            variant="secondary"
                            disabled={submitExecutionMutation.isPending || !selectedSessionId}
                            onClick={() => submitExecutionMutation.mutate(selectedBoardCandidate)}
                          >
                            {submitExecutionMutation.isPending ? <LoaderCircle className="size-4 animate-spin" /> : null}
                            Execute 1x
                          </Button>
                          <div className="max-w-sm text-sm text-muted-foreground lg:text-right">
                            Uses the candidate&apos;s persisted Alpaca multi-leg order payload and saved limit price.
                          </div>
                        </div>
                      </div>
                      {submitExecutionMutation.isError ? (
                        <div className="mt-3 text-sm text-rose-700">
                          {submitExecutionMutation.error instanceof Error
                            ? submitExecutionMutation.error.message
                            : "Could not submit this live execution."}
                        </div>
                      ) : null}
                      {submitExecutionMutation.data ? (
                        <div className="mt-3 text-sm text-foreground/70">
                          {submitExecutionMutation.data.message}
                        </div>
                      ) : null}
                    </div>
                  ) : null}
                </div>
              </SectionSurface>

              <SectionSurface title="Executions" description="Persisted trade attempts, broker orders, and any fills linked to this session.">
                {!session.executions.length ? (
                  <div className="rounded-2xl border border-dashed border-border/70 px-4 py-8 text-sm text-muted-foreground">
                    No execution attempts were recorded for this session yet.
                  </div>
                ) : (
                  <div className="flex flex-col gap-3">
                    {session.executions.map((attempt) => (
                      <ExecutionAttemptCard
                        key={attempt.execution_attempt_id}
                        attempt={attempt}
                        refreshing={refreshingAttemptId === attempt.execution_attempt_id}
                        onRefresh={() => refreshExecutionMutation.mutate(attempt.execution_attempt_id)}
                      />
                    ))}
                  </div>
                )}
                {refreshExecutionMutation.isError ? (
                  <div className="mt-3 text-sm text-rose-700">
                    {refreshExecutionMutation.error instanceof Error
                      ? refreshExecutionMutation.error.message
                      : "Could not refresh broker execution status."}
                  </div>
                ) : null}
                {refreshExecutionMutation.data ? (
                  <div className="mt-3 text-sm text-foreground/70">
                    {refreshExecutionMutation.data.message}
                  </div>
                ) : null}
              </SectionSurface>

              <SectionSurface title="Watchlist" description="Current watchlist candidates from the latest successful cycle.">
                <DataTable
                  columns={CANDIDATE_COLUMNS}
                  data={watchlistRows}
                  getRowId={(row) => row.id}
                  emptyMessage="No watchlist candidates were persisted for this session."
                />
              </SectionSurface>

              <SectionSurface title="Slots" description="One row per persisted collector slot for this session.">
                <DataTable
                  columns={SLOT_COLUMNS}
                  data={slotRows}
                  getRowId={(row) => row.id}
                  emptyMessage="No slot runs were persisted for this session."
                />
              </SectionSurface>

              <div className="grid gap-4 xl:grid-cols-2">
                <SectionSurface title="Alerts & Events" description="Recent alerts and collector events scoped to this session.">
                  <div className="flex flex-col gap-4">
                    <div>
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
                    <div>
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
                </SectionSurface>

                <SectionSurface title="Analysis" description="Persisted post-market analysis and signal tuning for this session.">
                  <SessionAnalysis session={session} />
                </SectionSurface>
              </div>
            </>
          ) : (
            <SectionSurface title="No session selected" description="Select a persisted session to inspect slot health, board state, and analysis.">
              <div className="flex flex-col items-center justify-center gap-3 px-6 py-16 text-center">
                <Rows3 className="size-10 text-muted-foreground" />
                <div className="text-lg font-medium">No persisted sessions found</div>
                <div className="max-w-[34rem] text-sm text-muted-foreground">
                  The root workspace only shows real sessions that already exist in storage. Once live collector slots persist, they will appear here automatically.
                </div>
              </div>
            </SectionSurface>
          )}
      </div>
    </main>
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
  const primaryOrder = attempt.orders.find((order) => !order.parent_broker_order_id) ?? attempt.orders[0] ?? null;
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
            <Badge variant="outline" className="rounded-full border-border/70 bg-background/80 px-2.5 py-1 text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
              {attempt.trade_intent}
            </Badge>
          </div>
          <div className="mt-1 text-sm text-muted-foreground">
            {attempt.short_symbol} / {attempt.long_symbol} · qty {attempt.quantity} · limit {formatCurrency(attempt.limit_price)}
          </div>
          <div className="mt-2 flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted-foreground">
            <span>requested {formatTimestamp(attempt.requested_at)}</span>
            <span>submitted {attempt.submitted_at ? formatTimestamp(attempt.submitted_at) : "—"}</span>
            <span>broker order {primaryOrder?.broker_order_id ?? attempt.broker_order_id ?? "pending"}</span>
            {attempt.session_position_id ? <span>position {attempt.session_position_id}</span> : null}
          </div>
        </div>
        <Button type="button" variant="outline" size="sm" disabled={refreshing} onClick={onRefresh}>
          {refreshing ? <LoaderCircle className="size-3.5 animate-spin" /> : null}
          Refresh broker status
        </Button>
      </div>
      <div className="mt-3 grid gap-3 sm:grid-cols-3">
        <MetricTile label="broker status" value={readString(primaryOrder?.order_status, attempt.status)} />
        <MetricTile label="fills" value={fillSummary} />
        <MetricTile
          label="completed"
          value={attempt.completed_at ? formatTime(attempt.completed_at) : "—"}
          note={attempt.completed_at ? formatDate(attempt.completed_at) : "Awaiting terminal state"}
        />
      </div>
      {attempt.orders.length > 1 ? (
        <div className="mt-3 text-sm text-muted-foreground">
          Legs:{" "}
          {attempt.orders
            .filter((order) => order.parent_broker_order_id)
            .map((order) => `${readString(order.leg_symbol, "leg")} ${readString(order.order_status, "unknown")}`)
            .join(" · ")}
        </div>
      ) : null}
      {attempt.fills.length ? (
        <div className="mt-3 flex flex-col gap-2">
          {attempt.fills.slice(0, 4).map((fill) => (
            <div key={fill.broker_fill_id} className="rounded-xl border border-border/70 px-3 py-2 text-sm text-foreground/80">
              {fill.symbol} · {readString(fill.fill_type, "fill").replaceAll("_", " ")} · qty {fill.quantity} @{" "}
              {fill.price == null ? "—" : formatCurrency(fill.price)} · {formatTimestamp(fill.filled_at)}
            </div>
          ))}
        </div>
      ) : null}
      {attempt.error_text ? (
        <div className="mt-3 text-sm text-rose-700">
          {attempt.error_text}
        </div>
      ) : null}
    </div>
  );
}
