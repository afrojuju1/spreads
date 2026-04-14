"use client";

import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";
import { CandlestickChart, RefreshCw, Rows3 } from "lucide-react";

import { DataTable } from "@/components/data-table";
import {
  buildPipelineHref,
  getPipelines,
  type PipelineListItem,
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

type PipelineListRow = {
  id: string;
  label: string;
  marketDate: string;
  status: string;
  captureStatus: string;
  latestSlotAt: string;
  promotableCount: number;
  monitorCount: number;
  alertCount: number;
};

function buildPipelineRows(pipelines: PipelineListItem[]): PipelineListRow[] {
  return pipelines.map((pipeline) => ({
    id: pipeline.pipeline_id,
    label: pipeline.label,
    marketDate: pipeline.latest_market_date,
    status: pipeline.status,
    captureStatus: readString(pipeline.latest_capture_status, "unknown"),
    latestSlotAt: readString(pipeline.latest_slot_at, ""),
    promotableCount: pipeline.promotable_count,
    monitorCount: pipeline.monitor_count,
    alertCount: pipeline.alert_count,
  }));
}

const PIPELINE_COLUMNS: ColumnDef<PipelineListRow>[] = [
  {
    accessorKey: "label",
    header: "Pipeline",
    cell: ({ row }) => (
      <div>
        <div className="font-semibold tracking-[0.04em]">
          {row.original.label}
        </div>
        <div className="text-xs text-muted-foreground">
          {formatDate(row.original.marketDate)}
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
        href={buildPipelineHref(row.original.id, row.original.marketDate)}
        className={buttonVariants({ variant: "outline", size: "sm" })}
      >
        Open
      </Link>
    ),
  },
];

export function PipelinesIndexPageContent() {
  const pipelinesQuery = useQuery({
    queryKey: ["pipelines"],
    queryFn: () => getPipelines({ limit: 120 }),
  });

  if (pipelinesQuery.isLoading) {
    return <LoadingState />;
  }

  const pipelines = pipelinesQuery.data?.pipelines ?? [];
  const latestPipeline = pipelines[0] ?? null;
  const pipelineRows = buildPipelineRows(pipelines);

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
                Pipelines
              </Badge>
              {latestPipeline ? (
                <SessionStatusBadge value={latestPipeline.status} />
              ) : null}
              {latestPipeline ? (
                <CaptureStatusBadge value={latestPipeline.latest_capture_status} />
              ) : null}
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">
              Live runtime pipelines
            </div>
            <div className="mt-2 text-sm text-foreground/70">
              Browse active runtimes, then open a single pipeline workspace for
              one market date.
            </div>
          </div>
          <Button
            type="button"
            variant="outline"
            onClick={() => void pipelinesQuery.refetch()}
          >
            <RefreshCw data-icon="inline-start" />
            Refresh
          </Button>
        </div>
      </div>

      {pipelinesQuery.isError ? (
        <div className="app-tone-error rounded-2xl border px-4 py-3 text-sm">
          Pipeline list could not be loaded.
        </div>
      ) : null}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <MetricTile
          label="Pipelines"
          value={String(pipelines.length)}
          note="Persisted runtime configs"
        />
        <MetricTile
          label="Latest Pipeline"
          value={latestPipeline?.label ?? "—"}
          note={
            latestPipeline
              ? formatDate(latestPipeline.latest_market_date)
              : "No pipelines"
          }
        />
        <MetricTile
          label="Latest Slot"
          value={
            latestPipeline ? formatTime(latestPipeline.latest_slot_at) : "—"
          }
          note={latestPipeline?.latest_slot_status ?? "No slot"}
        />
        <MetricTile
          label="Promotable"
          value={String(latestPipeline?.promotable_count ?? 0)}
          note="Latest pipeline snapshot"
        />
        <MetricTile
          label="Monitor"
          value={String(latestPipeline?.monitor_count ?? 0)}
          note={`Alerts ${latestPipeline?.alert_count ?? 0}`}
        />
      </div>

      <SectionSurface
        title="Pipeline List"
        description="Open a pipeline to inspect its latest cycle, opportunities, executions, and open positions."
      >
        {!pipelines.length ? (
          <div className="flex flex-col items-center justify-center gap-3 px-6 py-16 text-center">
            <Rows3 className="size-10 text-muted-foreground" />
            <div className="text-lg font-medium">No pipelines found</div>
            <div className="max-w-[34rem] text-sm text-muted-foreground">
              Runtime pipelines will appear here after collector cycles are
              persisted.
            </div>
          </div>
        ) : (
          <DataTable
            columns={PIPELINE_COLUMNS}
            data={pipelineRows}
            getRowId={(row) => row.id}
            emptyMessage="No pipelines matched the current query."
          />
        )}
      </SectionSurface>
    </div>
  );
}
