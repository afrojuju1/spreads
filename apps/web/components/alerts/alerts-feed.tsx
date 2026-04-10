"use client";

import { useQuery } from "@tanstack/react-query";
import { ColumnDef } from "@tanstack/react-table";
import { BellRing, LoaderCircle, RefreshCw } from "lucide-react";
import { trim } from "lodash-es";
import { useDeferredValue, useState } from "react";

import { DataTable } from "@/components/data-table";
import { type AlertRecord, getAlerts } from "@/lib/api";
import { formatCalendarDate, formatLocalDateTime } from "@/lib/date";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

type AlertRow = {
  id: string;
  createdAt: string;
  sessionDate: string;
  label: string;
  symbol: string;
  alertType: string;
  status: string;
  raw: AlertRecord;
};

function formatDate(value: string): string {
  return formatCalendarDate(value, "MMM d");
}

function formatTimestamp(value: string): string {
  return formatLocalDateTime(value);
}

function statusTone(value: string): string {
  switch (value) {
    case "delivered":
    case "sent":
    case "succeeded":
    case "created":
      return "border-emerald-200 bg-emerald-100 text-emerald-900";
    case "pending":
    case "dispatching":
    case "retry_wait":
      return "border-sky-200 bg-sky-100 text-sky-900";
    case "suppressed":
    case "skipped":
      return "border-amber-200 bg-amber-100 text-amber-900";
    case "dead_letter":
    case "failed":
      return "border-rose-200 bg-rose-100 text-rose-900";
    default:
      return "border-border/70 bg-card text-foreground";
  }
}

const ALERT_COLUMNS: ColumnDef<AlertRow>[] = [
  {
    accessorKey: "createdAt",
    header: "Time",
    cell: ({ getValue }) => <span className="font-mono text-[12px]">{formatTimestamp(String(getValue()))}</span>,
  },
  {
    accessorKey: "sessionDate",
    header: "Session",
    cell: ({ row }) => (
      <div>
        <div className="font-semibold tracking-[0.04em]">{row.original.label}</div>
        <div className="text-xs text-muted-foreground">{formatDate(row.original.sessionDate)}</div>
      </div>
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
    cell: ({ getValue }) => <span className="text-foreground/80">{String(getValue()).replaceAll("_", " ")}</span>,
  },
  {
    accessorKey: "status",
    header: "Status",
    cell: ({ getValue }) => (
      <Badge variant="outline" className={`rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em] ${statusTone(String(getValue()))}`}>
        {String(getValue())}
      </Badge>
    ),
  },
];

export function AlertsFeed() {
  const [filterText, setFilterText] = useState("");
  const deferredFilterText = useDeferredValue(filterText);
  const alertsQuery = useQuery({
    queryKey: ["alerts-latest"],
    queryFn: () => getAlerts(200),
  });

  const rows: AlertRow[] = (alertsQuery.data?.alerts ?? []).map((alert) => ({
    id: String(alert.alert_id),
    createdAt: alert.created_at,
    sessionDate: alert.session_date,
    label: alert.label,
    symbol: alert.symbol,
    alertType: alert.alert_type,
    status: alert.status,
    raw: alert,
  }));

  const normalizedFilter = trim(deferredFilterText).toLowerCase();
  const filteredRows = normalizedFilter
    ? rows.filter((row) =>
        `${row.label} ${row.symbol} ${row.alertType}`.toLowerCase().includes(normalizedFilter),
      )
    : rows;

  return (
    <main className="mx-auto max-w-[1680px] px-4 py-6 lg:px-6">
      <section className="rounded-[32px] border border-border/70 bg-card/80 shadow-[0_30px_90px_-54px_rgba(15,23,42,0.55)]">
        <div className="flex flex-col gap-4 border-b border-border/70 px-5 py-5 lg:flex-row lg:items-end lg:justify-between lg:px-6">
          <div>
            <Badge variant="outline" className="rounded-full border-border/70 bg-background/80 px-2.5 py-1 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
              <BellRing data-icon="inline-start" />
              Alerts feed
            </Badge>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">Alert delivery history</div>
            <div className="mt-2 text-sm text-foreground/70">
              Recent persisted alerts across all sessions.
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Input
              value={filterText}
              onChange={(event) => setFilterText(event.target.value)}
              placeholder="Filter by label, symbol, or type"
              className="h-10 w-[280px] rounded-xl"
            />
            <Button type="button" variant="outline" onClick={() => void alertsQuery.refetch()}>
              <RefreshCw data-icon="inline-start" />
              Refresh
            </Button>
          </div>
        </div>
        <div className="px-4 py-4 md:px-5">
          {alertsQuery.isLoading ? (
            <div className="flex items-center gap-2 px-2 py-8 text-sm text-muted-foreground">
              <LoaderCircle className="size-4 animate-spin" />
              Loading alerts…
            </div>
          ) : null}
          {alertsQuery.isError ? (
            <div className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-900">
              Alert history could not be loaded.
            </div>
          ) : null}
          {!alertsQuery.isLoading && !alertsQuery.isError ? (
            <DataTable
              columns={ALERT_COLUMNS}
              data={filteredRows}
              getRowId={(row) => row.id}
              emptyMessage="No alerts matched the current filter."
            />
          ) : null}
        </div>
      </section>
    </main>
  );
}
