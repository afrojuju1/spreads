"use client";

import { useQuery } from "@tanstack/react-query";
import { Database, ExternalLink, HardDrive, RefreshCw, Server } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import { getStorageOpsState, getTradingOpsState } from "@/lib/api";
import { grafanaTradingLogsUrl } from "@/lib/grafana";
import {
  formatBytes,
  formatCompactNumber,
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
  const tradingOpsQuery = useQuery({
    queryKey: ["trading-ops-state", "ops-page"],
    queryFn: () => getTradingOpsState(),
  });
  const storageOpsQuery = useQuery({
    queryKey: ["storage-ops-state"],
    queryFn: getStorageOpsState,
  });

  const loading = tradingOpsQuery.isLoading || storageOpsQuery.isLoading;
  const tradingState = tradingOpsQuery.data;
  const tradingSummary = readRecord(tradingState?.summary);
  const tradingDetails = readRecord(tradingState?.details);
  const storageState = storageOpsQuery.data;
  const storageSummary = readRecord(storageState?.summary);
  const storageDetails = readRecord(storageState?.details);
  const storageTables = readRecordList(storageDetails.tables);
  const workers = readRecordList(tradingDetails.workers);
  const workerLanes = readRecordList(tradingDetails.worker_lanes);
  const runningJobs = latestJobRows(readRecordList(tradingDetails.running_jobs));
  const queuedJobs = latestJobRows(readRecordList(tradingDetails.queued_jobs));
  const engine = readRecord(tradingDetails.engine);
  const engineSummary = readRecord(engine.summary);
  const quoteTable = storageTables.find((row) => readString(row.name, "") === "option_quote_events") ?? {};
  const eventLogTable = storageTables.find((row) => readString(row.physical_table, "") === "event_log") ?? {};
  const attention = [...readRecordList(tradingState?.attention), ...readRecordList(storageState?.attention)];
  const hasQueryError = tradingOpsQuery.isError || storageOpsQuery.isError;
  const scheduler = readRecord(tradingDetails.scheduler);
  const marketSessionStatus = tradingSummary.market_session_status;
  const schedulerStatus = firstPresent(tradingSummary.scheduler_status, scheduler.status);
  const vacuumFullTables = Array.isArray(storageSummary.vacuum_full_pending_tables)
    ? storageSummary.vacuum_full_pending_tables.map(String).join(", ") || "no pending tables"
    : readString(storageSummary.vacuum_full_pending_tables, "no pending tables");

  const refreshAll = () => {
    void tradingOpsQuery.refetch();
    void storageOpsQuery.refetch();
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
              <RuntimeStatusBadge value={tradingState?.status ?? storageState?.status ?? (loading ? "loading" : "idle")} />
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">Engine and runtime</div>
            <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-sm text-foreground/70">
              <span>{humanizeToken(marketSessionStatus, loading ? "loading" : "unknown")}</span>
              <span>scheduler {humanizeToken(schedulerStatus, loading ? "loading" : "unknown")}</span>
              <span>updated {formatTimestamp(readString(tradingState?.generated_at, ""))}</span>
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

      {hasQueryError ? <div className="app-tone-error rounded-2xl border px-4 py-3 text-sm">One or more ops state feeds could not be loaded.</div> : null}

      {loading ? (
        <div className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3 text-sm text-muted-foreground">Loading live ops state.</div>
      ) : null}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <MetricTile
          label="Engine"
          value={humanizeToken(firstPresent(tradingSummary.engine_status, engine.status), loading ? "loading" : "idle")}
          note={`${formatCompactNumber(readNumber(engineSummary.signal_count))} signals · ${formatCompactNumber(readNumber(engineSummary.selected_count))} selected`}
        />
        <MetricTile
          label="Workers"
          value={String(readNumber(tradingSummary.worker_lane_count, workerLanes.length))}
          note={`${readNumber(tradingSummary.blocked_worker_lane_count)} blocked · ${workers.length} leases`}
        />
        <MetricTile
          label="Jobs"
          value={`${runningJobs.length} running`}
          note={`${queuedJobs.length} queued · ${readNumber(tradingSummary.actionable_failed_job_count)} failed`}
        />
        <MetricTile
          label="Quote Events"
          value={formatCompactNumber(readNumber(quoteTable.estimated_live_rows))}
          note={formatBytes(readNumber(quoteTable.total_size_bytes))}
        />
        <MetricTile
          label="Retention"
          value={humanizeToken(storageSummary.latest_run_status, loading ? "loading" : "unknown")}
          note={`latest ${formatTimestamp(readString(storageSummary.latest_run_at, ""))}`}
        />
      </div>

      <section className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
        <div className="mb-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Engine Spine</div>
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <MetricTile
            label="Ticker Sources"
            value={formatCompactNumber(readNumber(engineSummary.ticker_source_run_count))}
            note={`${formatCompactNumber(readNumber(engineSummary.candidate_run_count))} candidate runs`}
          />
          <MetricTile
            label="Candidates"
            value={formatCompactNumber(readNumber(engineSummary.trade_candidate_count))}
            note={`${formatCompactNumber(readNumber(engineSummary.signal_count))} signals`}
          />
          <MetricTile
            label="Decisions"
            value={formatCompactNumber(readNumber(engineSummary.decision_count))}
            note={`${formatCompactNumber(readNumber(engineSummary.selected_count))} selected`}
          />
          <MetricTile
            label="Capture"
            value={formatCompactNumber(readNumber(engineSummary.capture_active_target_count))}
            note={humanizeToken(engineSummary.capture_status, loading ? "loading" : "idle")}
          />
        </div>
        <div className="mt-3 grid gap-3 text-sm md:grid-cols-3">
          <div className="rounded-lg border border-border/70 px-3 py-2">
            <div className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Intents</div>
            <div className="mt-1 font-medium">
              {formatCompactNumber(readNumber(engineSummary.entry_intent_count))} entry ·{" "}
              {formatCompactNumber(readNumber(engineSummary.management_intent_count))} management
            </div>
          </div>
          <div className="rounded-lg border border-border/70 px-3 py-2">
            <div className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Positions</div>
            <div className="mt-1 font-medium">{formatCompactNumber(readNumber(engineSummary.open_position_count))} open</div>
          </div>
          <div className="rounded-lg border border-border/70 px-3 py-2">
            <div className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Latest Capture</div>
            <div className="mt-1 truncate font-medium">{readString(engineSummary.latest_capture_summary_id)}</div>
          </div>
        </div>
      </section>

      <div className="grid gap-4 xl:grid-cols-[0.9fr_1.1fr]">
        <section className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
          <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
            <Server className="size-4" />
            Attention
          </div>
          {attention.length ? (
            <div className="space-y-2">
              {attention.slice(0, 10).map((row, index) => (
                <div key={`${readString(row.code, "attention")}-${index}`} className="rounded-lg border border-border/70 px-3 py-2 text-sm">
                  <div className="flex items-center justify-between gap-3">
                    <span className="font-medium">{humanizeToken(row.code ?? row.severity)}</span>
                    <RuntimeStatusBadge value={row.severity ?? "degraded"} />
                  </div>
                  <div className="mt-1 text-xs text-muted-foreground">{readString(row.message)}</div>
                </div>
              ))}
            </div>
          ) : (
            <div className="text-sm text-muted-foreground">No actionable attention items.</div>
          )}
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
                <div className="mt-1 truncate text-xs text-muted-foreground">expires {formatTimestamp(readString(row.expires_at, ""))}</div>
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
          <MetricTile label="Vacuum Full" value={storageSummary.vacuum_full_pending ? "Pending" : "Clear"} note={vacuumFullTables} />
          <MetricTile
            label="Event Log"
            value={formatBytes(readNumber(eventLogTable.total_size_bytes))}
            note={`${formatCompactNumber(readNumber(eventLogTable.estimated_live_rows))} rows`}
          />
          <MetricTile
            label="Schedule"
            value={readString(storageSummary.schedule)}
            note={storageSummary.market_hours_safe ? "market-hours safe" : "review timing"}
          />
        </div>
        <div className="mt-4 overflow-hidden rounded-lg border border-border/70">
          <div className="grid grid-cols-[1.2fr_0.8fr_0.8fr_0.8fr] gap-3 border-b border-border/70 bg-background/70 px-3 py-2 text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
            <span>Table</span>
            <span>Rows</span>
            <span>Size</span>
            <span>Vacuum</span>
          </div>
          <div className="divide-y divide-border/60">
            {storageTables.map((row) => {
              const vacuumFull = readRecord(row.vacuum_full);
              return (
                <div key={readString(row.name)} className="grid grid-cols-[1.2fr_0.8fr_0.8fr_0.8fr] gap-3 px-3 py-2 text-sm">
                  <span className="min-w-0 truncate font-medium">{readString(row.name)}</span>
                  <span>{formatCompactNumber(readNumber(row.estimated_live_rows))}</span>
                  <span>{formatBytes(readNumber(row.total_size_bytes))}</span>
                  <span>{vacuumFull.pending ? "pending" : "ok"}</span>
                </div>
              );
            })}
          </div>
        </div>
      </section>
    </div>
  );
}
