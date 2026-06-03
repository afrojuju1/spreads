"use client";

import { useQuery } from "@tanstack/react-query";
import { Database, ExternalLink, HardDrive, RefreshCw, Server } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import { getOpsLiveDoctor, getOpsRetention, getOpsStatus } from "@/lib/api";
import { grafanaTradingLogsUrl } from "@/lib/grafana";
import {
  formatBytes,
  formatCompactNumber,
  formatQuantity,
  formatTimestamp,
  MetricTile,
  readNumber,
  readRecord,
  readRecordList,
  readString,
  RuntimeStatusBadge,
} from "@/components/operator/operator-primitives";

function humanizeToken(value: unknown, fallback = "unknown"): string {
  return readString(value, fallback).replaceAll("_", " ");
}

function latestJobRows(rows: Record<string, unknown>[]) {
  return rows.slice(0, 8);
}

function firstPresent(...values: unknown[]): unknown {
  return values.find((value) => value !== undefined && value !== null && value !== "");
}

export function OpsPageContent() {
  const statusQuery = useQuery({
    queryKey: ["ops-status"],
    queryFn: getOpsStatus,
  });
  const liveDoctorQuery = useQuery({
    queryKey: ["ops-live-doctor", "ops-page"],
    queryFn: () => getOpsLiveDoctor({ feedId: "finviz_momentum", limit: 8 }),
  });
  const retentionQuery = useQuery({
    queryKey: ["ops-retention", "pending-counts"],
    queryFn: () => getOpsRetention({ includePendingCounts: true }),
  });

  const loading = statusQuery.isLoading || liveDoctorQuery.isLoading || retentionQuery.isLoading;
  const statusPayload = statusQuery.data;
  const statusSummary = readRecord(statusPayload?.summary);
  const statusDetails = readRecord(statusPayload?.details);
  const liveDoctor = liveDoctorQuery.data;
  const liveSummary = readRecord(liveDoctor?.summary);
  const liveDetails = readRecord(liveDoctor?.details);
  const checks = readRecordList(liveDetails.checks);
  const retentionPayload = retentionQuery.data;
  const retentionSummary = readRecord(retentionPayload?.summary);
  const retentionDetails = readRecord(retentionPayload?.details);
  const retentionTables = readRecordList(retentionDetails.tables);
  const workers = readRecordList(statusDetails.workers);
  const runningJobs = latestJobRows(readRecordList(statusDetails.running_jobs));
  const queuedJobs = latestJobRows(readRecordList(statusDetails.queued_jobs));
  const quoteTable = retentionTables.find((row) => readString(row.name, "") === "option_quote_events") ?? {};
  const eventLogTable = retentionTables.find((row) => readString(row.physical_table, "") === "event_log") ?? {};
  const hasQueryError = statusQuery.isError || liveDoctorQuery.isError || retentionQuery.isError;
  const scheduler = readRecord(statusDetails.scheduler);
  const marketSessionStatus = firstPresent(liveSummary.market_session_status, statusSummary.market_session_status);
  const schedulerStatus = firstPresent(liveSummary.scheduler_status, scheduler.status);
  const vacuumFullTables = Array.isArray(retentionSummary.vacuum_full_pending_tables)
    ? retentionSummary.vacuum_full_pending_tables.map(String).join(", ") || "no pending tables"
    : readString(retentionSummary.vacuum_full_pending_tables, "no pending tables");

  const refreshAll = () => {
    void statusQuery.refetch();
    void liveDoctorQuery.refetch();
    void retentionQuery.refetch();
  };

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
                <Server data-icon="inline-start" />
                Ops
              </Badge>
              <RuntimeStatusBadge value={statusPayload?.status ?? liveDoctor?.status} />
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">Resources and jobs</div>
            <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-sm text-foreground/70">
              <span>{humanizeToken(marketSessionStatus)}</span>
              <span>scheduler {humanizeToken(schedulerStatus)}</span>
              <span>updated {formatTimestamp(readString(statusPayload?.generated_at, ""))}</span>
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            <Button type="button" variant="outline" onClick={refreshAll}>
              <RefreshCw data-icon="inline-start" />
              Refresh
            </Button>
            <a href={grafanaTradingLogsUrl()} target="_blank" rel="noreferrer" className={buttonVariants({ variant: "outline" })}>
              <ExternalLink data-icon="inline-start" />
              Trading logs
            </a>
          </div>
        </div>
      </div>

      {hasQueryError ? (
        <div className="app-tone-error rounded-2xl border px-4 py-3 text-sm">
          One or more ops feeds could not be loaded.
        </div>
      ) : null}

      {loading ? (
        <div className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3 text-sm text-muted-foreground">
          Loading live ops feeds.
        </div>
      ) : null}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricTile
          label="Workers"
          value={String(readNumber(statusSummary.worker_count, workers.length))}
          note={`${readNumber(liveSummary.worker_lane_count)} lanes · ${readNumber(liveSummary.blocked_worker_lane_count)} blocked`}
        />
        <MetricTile
          label="Jobs"
          value={`${readNumber(statusSummary.running_job_count)} running`}
          note={`${readNumber(statusSummary.queued_job_count)} queued · ${readNumber(statusSummary.recent_failure_count)} recent failures`}
        />
        <MetricTile
          label="Quote Events"
          value={formatCompactNumber(readNumber(quoteTable.estimated_live_rows))}
          note={formatBytes(readNumber(quoteTable.total_size_bytes))}
        />
        <MetricTile
          label="Retention"
          value={humanizeToken(retentionSummary.latest_run_status)}
          note={`latest ${formatTimestamp(readString(retentionSummary.latest_run_at, ""))}`}
        />
      </div>

      <div className="grid gap-4 xl:grid-cols-[0.9fr_1.1fr]">
        <section className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
          <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
            <Server className="size-4" />
            Health Checks
          </div>
          <div className="divide-y divide-border/60">
            {checks.map((row) => (
              <div key={readString(row.name)} className="grid gap-2 py-2 text-sm md:grid-cols-[minmax(120px,0.45fr)_1fr_auto] md:items-center">
                <div className="font-medium">{readString(row.name)}</div>
                <div className="min-w-0 truncate text-muted-foreground">{readString(row.message)}</div>
                <RuntimeStatusBadge value={row.status} />
              </div>
            ))}
          </div>
        </section>

        <section className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
          <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
            <HardDrive className="size-4" />
            Runtime Workers
          </div>
          <div className="grid gap-2 md:grid-cols-2">
            {workers.map((row) => (
              <div key={readString(row.lease_key)} className="rounded-lg border border-border/70 px-3 py-2 text-sm">
                <div className="flex items-center justify-between gap-3">
                  <span className="truncate font-medium">{humanizeToken(readRecord(row.lease_state).lane)}</span>
                  <Badge variant="outline">{readString(row.owner)}</Badge>
                </div>
                <div className="mt-1 truncate text-xs text-muted-foreground">
                  expires {formatTimestamp(readString(row.expires_at, ""))}
                </div>
              </div>
            ))}
          </div>
        </section>
      </div>

      <section className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
        <div className="mb-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Active Jobs</div>
        <div className="grid gap-4 xl:grid-cols-2">
          <div className="min-w-0">
            <div className="mb-2 text-sm font-medium">Running</div>
            <div className="divide-y divide-border/60 rounded-lg border border-border/70">
              {runningJobs.length ? (
                runningJobs.map((row) => (
                  <div key={readString(row.job_run_id)} className="px-3 py-2 text-sm">
                    <div className="flex items-center justify-between gap-3">
                      <span className="min-w-0 truncate font-medium">{readString(row.job_type)}</span>
                      <RuntimeStatusBadge value={row.status} />
                    </div>
                    <div className="mt-1 truncate text-xs text-muted-foreground">{readString(row.job_run_id)}</div>
                  </div>
                ))
              ) : (
                <div className="px-3 py-3 text-sm text-muted-foreground">No running jobs.</div>
              )}
            </div>
          </div>

          <div className="min-w-0">
            <div className="mb-2 text-sm font-medium">Queued</div>
            <div className="divide-y divide-border/60 rounded-lg border border-border/70">
              {queuedJobs.length ? (
                queuedJobs.map((row) => (
                  <div key={readString(row.job_run_id)} className="px-3 py-2 text-sm">
                    <div className="flex items-center justify-between gap-3">
                      <span className="min-w-0 truncate font-medium">{readString(row.job_type)}</span>
                      <RuntimeStatusBadge value={row.status} />
                    </div>
                    <div className="mt-1 truncate text-xs text-muted-foreground">{readString(row.job_run_id)}</div>
                  </div>
                ))
              ) : (
                <div className="px-3 py-3 text-sm text-muted-foreground">No queued jobs.</div>
              )}
            </div>
          </div>
        </div>
      </section>

      <section className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
        <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
          <Database className="size-4" />
          Storage Retention
        </div>
        <div className="grid gap-4 md:grid-cols-3">
          <MetricTile
            label="Vacuum Full"
            value={retentionSummary.vacuum_full_pending ? "Pending" : "Clear"}
            note={vacuumFullTables}
          />
          <MetricTile
            label="Event Log"
            value={formatBytes(readNumber(eventLogTable.total_size_bytes))}
            note={`${formatCompactNumber(readNumber(eventLogTable.estimated_live_rows))} rows`}
          />
          <MetricTile
            label="Schedule"
            value={readString(retentionSummary.schedule)}
            note={retentionSummary.market_hours_safe ? "market-hours safe" : "review timing"}
          />
        </div>
        <div className="mt-4 overflow-hidden rounded-lg border border-border/70">
          <div className="grid grid-cols-[1.2fr_0.8fr_0.8fr_0.8fr] gap-3 border-b border-border/70 bg-background/70 px-3 py-2 text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
            <span>Table</span>
            <span>Rows</span>
            <span>Size</span>
            <span>Prune</span>
          </div>
          <div className="divide-y divide-border/60">
            {retentionTables.map((row) => (
              <div key={readString(row.name)} className="grid grid-cols-[1.2fr_0.8fr_0.8fr_0.8fr] gap-3 px-3 py-2 text-sm">
                <span className="min-w-0 truncate font-medium">{readString(row.name)}</span>
                <span>{formatCompactNumber(readNumber(row.estimated_live_rows))}</span>
                <span>{formatBytes(readNumber(row.total_size_bytes))}</span>
                <span>{formatQuantity(readNumber(row.pending_prune_count))}</span>
              </div>
            ))}
          </div>
        </div>
      </section>
    </div>
  );
}
