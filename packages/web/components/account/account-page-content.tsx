"use client";

import { useQuery } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";
import { LoaderCircle, RefreshCw, Wallet } from "lucide-react";
import { startTransition, useState } from "react";

import { DataTable } from "@/components/data-table";
import {
  getAccountOverview,
  type AccountHistory,
  type AccountHistoryPoint,
  type AccountHistoryRange,
  type AccountOverview,
  type AccountPosition,
} from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  AccountEnvironmentBadge,
  formatNullableCurrency,
  formatSignedCurrency,
  formatSignedPercent,
  formatTimestamp,
  formatQuantity,
  LoadingState,
  MetricTile,
  readString,
  SectionSurface,
  SessionStatusBadge,
  valueTone,
} from "@/components/sessions/workspace-primitives";
import { cn } from "@/lib/utils";

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
};

const ACCOUNT_HISTORY_RANGES: AccountHistoryRange[] = ["1D", "1W", "1M"];

function buildAccountPositionRows(
  positions: AccountPosition[],
): AccountPositionRow[] {
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
  }));
}

const ACCOUNT_POSITION_COLUMNS: ColumnDef<AccountPositionRow>[] = [
  {
    accessorKey: "symbol",
    header: "Symbol",
    cell: ({ row }) => (
      <span className="font-mono text-[12px] font-semibold">
        {row.original.symbol}
      </span>
    ),
  },
  {
    accessorKey: "side",
    header: "Side",
    cell: ({ getValue }) => (
      <span className="capitalize text-foreground/80">
        {String(getValue())}
      </span>
    ),
  },
  {
    accessorKey: "quantity",
    header: "Qty",
    cell: ({ getValue }) => (
      <span className="font-mono">
        {formatQuantity(getValue<number | null | undefined>())}
      </span>
    ),
  },
  {
    accessorKey: "averageEntryPrice",
    header: "Entry",
    cell: ({ getValue }) => (
      <span className="font-mono">
        {formatNullableCurrency(getValue<number | null | undefined>())}
      </span>
    ),
  },
  {
    accessorKey: "currentPrice",
    header: "Mark",
    cell: ({ getValue }) => (
      <span className="font-mono">
        {formatNullableCurrency(getValue<number | null | undefined>())}
      </span>
    ),
  },
  {
    accessorKey: "marketValue",
    header: "Market Value",
    cell: ({ getValue }) => (
      <span className="font-mono">
        {formatNullableCurrency(getValue<number | null | undefined>())}
      </span>
    ),
  },
  {
    accessorKey: "intradayPnl",
    header: "Day PnL",
    cell: ({ row }) => (
      <div className={cn("font-mono", valueTone(row.original.intradayPnl))}>
        <div>{formatSignedCurrency(row.original.intradayPnl)}</div>
        <div className="text-[11px]">
          {formatSignedPercent(row.original.intradayPnlPercent)}
        </div>
      </div>
    ),
  },
  {
    accessorKey: "openPnl",
    header: "Open PnL",
    cell: ({ row }) => (
      <div className={cn("font-mono", valueTone(row.original.openPnl))}>
        <div>{formatSignedCurrency(row.original.openPnl)}</div>
        <div className="text-[11px]">
          {formatSignedPercent(row.original.openPnlPercent)}
        </div>
      </div>
    ),
  },
];

function maskAccountNumber(value: string | null | undefined): string {
  const normalized = (value ?? "").trim();
  if (!normalized) {
    return "—";
  }
  if (normalized.length <= 4) {
    return normalized;
  }
  return `••••${normalized.slice(-4)}`;
}

function buildChartCoordinates(
  points: AccountHistoryPoint[],
  width: number,
  height: number,
  padding: number,
) {
  const validPoints = points.filter(
    (point): point is AccountHistoryPoint & { equity: number } =>
      typeof point.equity === "number" && Number.isFinite(point.equity),
  );
  if (!validPoints.length) {
    return null;
  }

  const min = Math.min(...validPoints.map((point) => point.equity));
  const max = Math.max(...validPoints.map((point) => point.equity));
  const range = max - min || 1;
  const step =
    validPoints.length === 1
      ? 0
      : (width - padding * 2) / (validPoints.length - 1);
  const coordinates = validPoints.map((point, index) => {
    const x = padding + step * index;
    const y =
      height -
      padding -
      ((point.equity - min) / range) * (height - padding * 2);
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
          <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
            Equity curve
          </div>
          <div
            className={cn(
              "mt-2 text-2xl font-semibold",
              valueTone(chart.end - chart.start),
            )}
          >
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
      <div className="mt-4 overflow-hidden rounded-2xl border border-border/70 bg-gradient-to-b from-muted/70 to-background">
        <svg viewBox={`0 0 ${width} ${height}`} className="h-56 w-full">
          <defs>
            <linearGradient id="account-history-fill" x1="0" x2="0" y1="0" y2="1">
              <stop
                offset="0%"
                style={{
                  stopColor: "color-mix(in oklab, var(--primary) 26%, transparent)",
                }}
              />
              <stop
                offset="100%"
                style={{
                  stopColor: "color-mix(in oklab, var(--primary) 3%, transparent)",
                }}
              />
            </linearGradient>
          </defs>
          <polyline
            points={chart.area}
            fill="url(#account-history-fill)"
            stroke="none"
          />
          <polyline
            points={chart.line}
            fill="none"
            stroke="var(--primary)"
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
    <SectionSurface
      title="Broker Account"
      description="Read-only Alpaca balances, open positions, and portfolio equity history."
    >
      {overview ? (
        <div className="flex flex-col gap-4">
          <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <Badge
                  variant="outline"
                  className="rounded-full border-border/70 bg-background/80 px-2.5 py-1 text-[11px] uppercase tracking-[0.18em] text-muted-foreground"
                >
                  <Wallet data-icon="inline-start" />
                  Broker snapshot
                </Badge>
                <AccountEnvironmentBadge value={overview.environment} />
                {overview.account.status ? (
                  <SessionStatusBadge
                    value={overview.account.status.toLowerCase()}
                  />
                ) : null}
                {overview.sync ? (
                  <SessionStatusBadge value={overview.sync.status} />
                ) : null}
              </div>
              <div className="mt-3 text-sm text-foreground/70">
                Account {maskAccountNumber(overview.account.account_number)} ·
                refreshed {formatTimestamp(overview.retrieved_at)}
                {overview.sync?.updated_at
                  ? ` · background sync ${formatTimestamp(overview.sync.updated_at)}`
                  : ""}
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
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={refreshing}
                onClick={onRefresh}
              >
                {refreshing ? (
                  <LoaderCircle className="size-3.5 animate-spin" />
                ) : (
                  <RefreshCw className="size-3.5" />
                )}
                Refresh
              </Button>
            </div>
          </div>

          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-7">
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
              label="broker sync"
              value={readString(overview.sync?.status, "unknown").toUpperCase()}
              note={
                overview.sync?.updated_at
                  ? `Last sync ${formatTimestamp(overview.sync.updated_at)}`
                  : "Background sync has not run yet"
              }
            />
            <MetricTile
              label="account status"
              value={readString(overview.account.status, "—").toUpperCase()}
              note={
                overview.account.trading_blocked
                  ? "Trading blocked"
                  : "Trading enabled"
              }
            />
          </div>

          <div className="grid gap-4 xl:grid-cols-[minmax(0,1.1fr)_minmax(0,1fr)]">
            <AccountHistoryChart history={overview.history} />
            <div className="rounded-2xl border border-border/70 bg-background/70 px-4 py-4">
              <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                Open positions
              </div>
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
        <div className="app-tone-error rounded-2xl border px-4 py-3 text-sm">
          {error}
        </div>
      ) : (
        <div className="grid gap-3 md:grid-cols-3">
          <LoadingState />
        </div>
      )}
    </SectionSurface>
  );
}

export function AccountPageContent() {
  const [accountHistoryRange, setAccountHistoryRange] =
    useState<AccountHistoryRange>("1D");
  const accountOverviewQuery = useQuery({
    queryKey: ["account-overview", accountHistoryRange],
    queryFn: () => getAccountOverview(accountHistoryRange),
    refetchInterval: 30_000,
  });

  if (accountOverviewQuery.isLoading && !accountOverviewQuery.data) {
    return <LoadingState />;
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
                <Wallet data-icon="inline-start" />
                Account
              </Badge>
              {accountOverviewQuery.data?.account.status ? (
                <SessionStatusBadge
                  value={accountOverviewQuery.data.account.status.toLowerCase()}
                />
              ) : null}
              {accountOverviewQuery.data?.sync ? (
                <SessionStatusBadge value={accountOverviewQuery.data.sync.status} />
              ) : null}
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">
              Broker account
            </div>
            <div className="mt-2 text-sm text-foreground/70">
              Read-only Alpaca balances, open positions, equity history, and sync
              status.
            </div>
          </div>
          <div className="grid w-full gap-3 sm:grid-cols-2 xl:max-w-[760px] xl:grid-cols-4">
            <MetricTile
              label="Equity"
              value={formatNullableCurrency(accountOverviewQuery.data?.account.equity)}
              note={`Cash ${formatNullableCurrency(accountOverviewQuery.data?.account.cash)}`}
            />
            <MetricTile
              label="Buying Power"
              value={formatNullableCurrency(accountOverviewQuery.data?.account.buying_power)}
              note={`Options ${formatNullableCurrency(accountOverviewQuery.data?.account.options_buying_power)}`}
            />
            <MetricTile
              label="Positions"
              value={String(accountOverviewQuery.data?.positions.length ?? 0)}
              note="Currently open broker positions"
            />
            <MetricTile
              label="Sync"
              value={readString(accountOverviewQuery.data?.sync?.status, "unknown").toUpperCase()}
              note={
                accountOverviewQuery.data?.sync?.updated_at
                  ? formatTimestamp(accountOverviewQuery.data.sync.updated_at)
                  : "Awaiting background sync"
              }
            />
          </div>
        </div>
      </div>

      <AccountOverviewSection
        overview={accountOverviewQuery.data ?? null}
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
    </div>
  );
}
