"use client";

import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";
import { CandlestickChart, RefreshCw, Rows3 } from "lucide-react";

import { DataTable } from "@/components/data-table";
import {
  buildSessionHref,
  getSessions,
  type SessionListItem,
} from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import {
  CaptureStatusBadge,
  formatDate,
  formatTime,
  formatTimestamp,
  LoadingState,
  MetricTile,
  readString,
  SectionSurface,
  SessionStatusBadge,
} from "@/components/sessions/workspace-primitives";

type SessionListRow = {
  id: string;
  label: string;
  sessionDate: string;
  status: string;
  captureStatus: string;
  latestSlotAt: string;
  promotableCount: number;
  monitorCount: number;
  alertCount: number;
};

function buildSessionListRows(sessions: SessionListItem[]): SessionListRow[] {
  return sessions.map((session) => ({
    id: session.session_id,
    label: session.label,
    sessionDate: session.session_date,
    status: session.status,
    captureStatus: readString(session.latest_capture_status, "unknown"),
    latestSlotAt: readString(session.latest_slot_at, ""),
    promotableCount: session.promotable_count,
    monitorCount: session.monitor_count,
    alertCount: session.alert_count,
  }));
}

const SESSION_LIST_COLUMNS: ColumnDef<SessionListRow>[] = [
  {
    accessorKey: "label",
    header: "Session",
    cell: ({ row }) => (
      <div>
        <div className="font-semibold tracking-[0.04em]">
          {row.original.label}
        </div>
        <div className="text-xs text-muted-foreground">
          {formatDate(row.original.sessionDate)}
        </div>
      </div>
    ),
  },
  {
    accessorKey: "status",
    header: "Status",
    cell: ({ getValue }) => <SessionStatusBadge value={String(getValue())} />,
  },
  {
    accessorKey: "captureStatus",
    header: "Capture",
    cell: ({ getValue }) => <CaptureStatusBadge value={String(getValue())} />,
  },
  {
    accessorKey: "latestSlotAt",
    header: "Latest Slot",
    cell: ({ getValue }) => (
      <span className="font-mono text-[12px]">
        {formatTimestamp(String(getValue()))}
      </span>
    ),
  },
  {
    accessorKey: "promotableCount",
    header: "Promotable",
    cell: ({ getValue }) => <span className="font-mono">{String(getValue())}</span>,
  },
  {
    accessorKey: "monitorCount",
    header: "Monitor",
    cell: ({ getValue }) => <span className="font-mono">{String(getValue())}</span>,
  },
  {
    accessorKey: "alertCount",
    header: "Alerts",
    cell: ({ getValue }) => <span className="font-mono">{String(getValue())}</span>,
  },
  {
    id: "actions",
    header: "",
    cell: ({ row }) => (
      <Link
        href={buildSessionHref(row.original.id)}
        className={buttonVariants({ variant: "outline", size: "sm" })}
      >
        Open
      </Link>
    ),
  },
];

export function SessionsIndexPageContent() {
  const sessionsQuery = useQuery({
    queryKey: ["sessions"],
    queryFn: () => getSessions({ limit: 120 }),
  });

  if (sessionsQuery.isLoading) {
    return <LoadingState />;
  }

  const sessions = sessionsQuery.data?.sessions ?? [];
  const latestSession = sessions[0] ?? null;
  const sessionRows = buildSessionListRows(sessions);

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
                <CandlestickChart data-icon="inline-start" />
                Sessions
              </Badge>
              {latestSession ? (
                <SessionStatusBadge value={latestSession.status} />
              ) : null}
              {latestSession ? (
                <CaptureStatusBadge value={latestSession.latest_capture_status} />
              ) : null}
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">
              Persisted sessions
            </div>
            <div className="mt-2 text-sm text-foreground/70">
              Browse historical and active sessions, then open a single session
              detail page.
            </div>
          </div>
          <Button
            type="button"
            variant="outline"
            onClick={() => void sessionsQuery.refetch()}
          >
            <RefreshCw data-icon="inline-start" />
            Refresh
          </Button>
        </div>
      </div>

      {sessionsQuery.isError ? (
        <div className="app-tone-error rounded-2xl border px-4 py-3 text-sm">
          Session list could not be loaded.
        </div>
      ) : null}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <MetricTile
          label="Sessions"
          value={String(sessions.length)}
          note="Persisted sessions in storage"
        />
        <MetricTile
          label="Latest Session"
          value={latestSession?.label ?? "—"}
          note={latestSession ? formatDate(latestSession.session_date) : "No sessions"}
        />
        <MetricTile
          label="Latest Slot"
          value={latestSession ? formatTime(latestSession.latest_slot_at) : "—"}
          note={latestSession?.latest_slot_status ?? "No slot"}
        />
        <MetricTile
          label="Promotable"
          value={String(latestSession?.promotable_count ?? 0)}
          note="Latest session snapshot"
        />
        <MetricTile
          label="Monitor"
          value={String(latestSession?.monitor_count ?? 0)}
          note={`Alerts ${latestSession?.alert_count ?? 0}`}
        />
      </div>

      <SectionSurface
        title="Session List"
        description="Open a session to inspect opportunities, executions, slots, and post-market analysis."
      >
        {!sessions.length ? (
          <div className="flex flex-col items-center justify-center gap-3 px-6 py-16 text-center">
            <Rows3 className="size-10 text-muted-foreground" />
            <div className="text-lg font-medium">No persisted sessions found</div>
            <div className="max-w-[34rem] text-sm text-muted-foreground">
              Session history will appear here after live collector slots are
              persisted.
            </div>
          </div>
        ) : (
          <DataTable
            columns={SESSION_LIST_COLUMNS}
            data={sessionRows}
            getRowId={(row) => row.id}
            emptyMessage="No sessions matched the current query."
          />
        )}
      </SectionSurface>
    </div>
  );
}
