"use client";

import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { Activity, ExternalLink, Gauge, RefreshCw, ShieldCheck } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import {
  getExecutionRuntimes,
  getOpsFinvizLedger,
  getOpsLiveDoctor,
  getOpsTradingHealth,
} from "@/lib/api";
import { grafanaTradingLogsUrl } from "@/lib/grafana";
import {
  formatNullableCurrency,
  formatQuantity,
  formatTimestamp,
  MetricTile,
  readNumber,
  readRecord,
  readRecordList,
  readString,
  RuntimeStatusBadge,
  valueTone,
} from "@/components/operator/operator-primitives";

function humanizeToken(value: unknown, fallback = "unknown"): string {
  return readString(value, fallback).replaceAll("_", " ");
}

function formatAge(seconds: unknown): string {
  const value = readNumber(seconds, -1);
  if (value < 0) {
    return "—";
  }
  if (value < 60) {
    return `${Math.round(value)}s`;
  }
  if (value < 60 * 60) {
    return `${Math.round(value / 60)}m`;
  }
  return `${(value / 3600).toFixed(1)}h`;
}

function findCheck(checks: Record<string, unknown>[], name: string): Record<string, unknown> {
  return checks.find((row) => readString(row.name, "").toLowerCase() === name.toLowerCase()) ?? {};
}

function reasonRows(value: unknown): { reason: string; count: number }[] {
  return Object.entries(readRecord(value))
    .map(([reason, count]) => ({ reason, count: readNumber(count) }))
    .filter((row) => row.count > 0)
    .sort((left, right) => right.count - left.count);
}

function hasMetricValue(value: unknown): boolean {
  return value !== undefined && value !== null && value !== "";
}

function formatNumberMetric(value: unknown, fallback: string): string {
  return hasMetricValue(value) ? formatQuantity(readNumber(value)) : fallback;
}

function firstPresent(...values: unknown[]): unknown {
  return values.find(hasMetricValue);
}

function StatusLine({
  label,
  value,
  note,
}: {
  label: string;
  value: unknown;
  note?: string;
}) {
  return (
    <div className="flex min-w-0 items-center justify-between gap-3 border-b border-border/60 py-2 last:border-b-0">
      <div className="min-w-0">
        <div className="truncate text-sm font-medium">{label}</div>
        {note ? <div className="mt-0.5 truncate text-xs text-muted-foreground">{note}</div> : null}
      </div>
      <RuntimeStatusBadge value={value} />
    </div>
  );
}

export function TodayCommandCenter() {
  const liveDoctorQuery = useQuery({
    queryKey: ["ops-live-doctor", "finviz_momentum"],
    queryFn: () => getOpsLiveDoctor({ feedId: "finviz_momentum", limit: 8 }),
  });
  const tradingHealthQuery = useQuery({
    queryKey: ["ops-trading-health"],
    queryFn: getOpsTradingHealth,
  });
  const finvizLedgerQuery = useQuery({
    queryKey: ["ops-finviz-ledger", "finviz_momentum"],
    queryFn: () => getOpsFinvizLedger({ feedId: "finviz_momentum", limit: 8 }),
  });
  const executionRuntimesQuery = useQuery({
    queryKey: ["execution-runtimes"],
    queryFn: getExecutionRuntimes,
  });

  const loading =
    liveDoctorQuery.isLoading ||
    tradingHealthQuery.isLoading ||
    finvizLedgerQuery.isLoading ||
    executionRuntimesQuery.isLoading;

  const liveDoctor = liveDoctorQuery.data;
  const liveSummary = readRecord(liveDoctor?.summary);
  const liveDetails = readRecord(liveDoctor?.details);
  const checks = readRecordList(liveDetails.checks);
  const trading = tradingHealthQuery.data;
  const tradingSummary = readRecord(trading?.summary);
  const finvizSummary = readRecord(finvizLedgerQuery.data?.summary);
  const finvizFeedCheck = findCheck(checks, "Finviz Feed");
  const finvizDirectCheck = findCheck(checks, "Finviz Direct");
  const finvizDirectMetrics = readRecord(finvizDirectCheck.metrics);
  const directReasonRows = reasonRows(finvizDirectMetrics.reason_counts ?? finvizSummary.latest_lifecycle_decision_state_counts);
  const runtimeRows = executionRuntimesQuery.data?.runtimes ?? [];
  const attention = [
    ...readRecordList(liveDoctor?.attention),
    ...readRecordList(trading?.attention),
  ];
  const hasQueryError =
    liveDoctorQuery.isError ||
    tradingHealthQuery.isError ||
    finvizLedgerQuery.isError ||
    executionRuntimesQuery.isError;

  const pendingLabel = loading ? "Loading" : "—";
  const generatedAt = firstPresent(liveDoctor?.generated_at, trading?.generated_at, finvizLedgerQuery.data?.generated_at);
  const marketSessionStatus = firstPresent(liveSummary.market_session_status, tradingSummary.market_session_status);
  const tradingAllowed = firstPresent(liveSummary.trading_allowed, tradingSummary.trading_allowed);
  const environment = firstPresent(liveSummary.environment, tradingSummary.environment);
  const controlMode = firstPresent(liveSummary.control_mode, tradingSummary.control_mode);
  const directCandidateTotal = directReasonRows.reduce((total, row) => total + row.count, 0);
  const directCandidateValue = firstPresent(
    liveSummary.finviz_direct_candidate_count,
    directCandidateTotal > 0 ? directCandidateTotal : undefined,
  );
  const marketDate = readString(liveSummary.market_date, readString(finvizSummary.market_date, ""));
  const openPositions = formatNumberMetric(
    firstPresent(liveSummary.open_position_count, tradingSummary.open_position_count, finvizSummary.open_position_count),
    pendingLabel,
  );
  const maxOpenPositions = formatNumberMetric(liveSummary.max_open_positions, pendingLabel);
  const activeIntents = formatNumberMetric(
    firstPresent(liveSummary.active_intent_count, tradingSummary.automation_intent_count, finvizSummary.active_intent_count),
    pendingLabel,
  );
  const remainingEntries = formatNumberMetric(liveSummary.remaining_daily_entries, pendingLabel);
  const maxDailyEntries = formatNumberMetric(liveSummary.max_daily_entries, pendingLabel);
  const directCandidateCount = formatNumberMetric(directCandidateValue, pendingLabel);
  const netPnl = hasMetricValue(liveSummary.net_pnl)
    ? readNumber(liveSummary.net_pnl)
    : hasMetricValue(finvizSummary.net_pnl)
      ? readNumber(finvizSummary.net_pnl)
      : null;
  const realizedPnl = hasMetricValue(liveSummary.realized_pnl)
    ? readNumber(liveSummary.realized_pnl)
    : null;

  const refreshAll = () => {
    void liveDoctorQuery.refetch();
    void tradingHealthQuery.refetch();
    void finvizLedgerQuery.refetch();
    void executionRuntimesQuery.refetch();
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
                <Activity data-icon="inline-start" />
                Today
              </Badge>
              <RuntimeStatusBadge value={liveDoctor?.status ?? trading?.status ?? finvizLedgerQuery.data?.status ?? (loading ? "loading" : "idle")} />
              <Badge variant="outline">{readString(environment, "paper")}</Badge>
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">Command center</div>
            <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-sm text-foreground/70">
              <span>{marketDate || "current market date"}</span>
              <span>{humanizeToken(marketSessionStatus, loading ? "loading" : "unknown")}</span>
              <span>generated {formatTimestamp(readString(generatedAt, ""))}</span>
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            <Button type="button" variant="outline" onClick={refreshAll}>
              <RefreshCw data-icon="inline-start" />
              Refresh
            </Button>
            <a
              href={grafanaTradingLogsUrl()}
              target="_blank"
              rel="noreferrer"
              className={buttonVariants({ variant: "outline" })}
            >
              <ExternalLink data-icon="inline-start" />
              Trading logs
            </a>
          </div>
        </div>
      </div>

      {hasQueryError ? (
        <div className="app-tone-error rounded-2xl border px-4 py-3 text-sm">
          One or more command-center feeds could not be loaded.
        </div>
      ) : null}

      {loading ? (
        <div className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3 text-sm text-muted-foreground">
          Loading live command-center feeds.
        </div>
      ) : null}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricTile
          label="Trading Gate"
          value={
            hasMetricValue(tradingAllowed)
              ? readString(tradingAllowed) === "true" || tradingAllowed === true
                ? "Allowed"
                : "Blocked"
              : pendingLabel
          }
          note={`${humanizeToken(controlMode, pendingLabel)} control · ${humanizeToken(marketSessionStatus, pendingLabel)}`}
        />
        <MetricTile
          label="Finviz"
          value={directCandidateCount === pendingLabel ? pendingLabel : `${directCandidateCount} candidates`}
          note={`feed ${formatAge(liveSummary.finviz_feed_age_seconds)} · direct ${formatAge(liveSummary.finviz_direct_age_seconds)}`}
        />
        <MetricTile
          label="Capacity"
          value={`${openPositions}/${maxOpenPositions} positions`}
          note={`${remainingEntries}/${maxDailyEntries} entries left · ${activeIntents} active intents`}
        />
        <MetricTile
          label="Net PnL"
          value={formatNullableCurrency(netPnl)}
          note={`realized ${formatNullableCurrency(realizedPnl)}`}
        />
      </div>

      <div className="grid gap-4 xl:grid-cols-[1.1fr_0.9fr]">
        <section className="grid gap-4 md:grid-cols-2">
          <div className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
            <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
              <ShieldCheck className="size-4" />
              Live Checks
            </div>
            <StatusLine
              label="Scheduler"
              value={liveSummary.scheduler_status ?? (loading ? "loading" : "idle")}
              note={`${formatNumberMetric(liveSummary.worker_lane_count, pendingLabel)} worker lanes`}
            />
            <StatusLine
              label="Broker Sync"
              value={liveSummary.broker_sync_status ?? (loading ? "loading" : "idle")}
              note={`${formatAge(liveSummary.broker_sync_age_seconds)} old`}
            />
            <StatusLine
              label="Execution"
              value={tradingSummary.execution_health_status ?? "healthy"}
              note={`${readNumber(liveSummary.active_close_attempt_count)} active closes`}
            />
            <StatusLine
              label="Close Lifecycle"
              value={liveSummary.close_lifecycle_status ?? (loading ? "loading" : "idle")}
              note={`${formatNumberMetric(liveSummary.failed_close_attempt_count, pendingLabel)} failed closes`}
            />
          </div>

          <div className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
            <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
              <Gauge className="size-4" />
              Finviz Flow
            </div>
            <StatusLine
              label="Feed"
              value={liveSummary.finviz_feed_status ?? (loading ? "loading" : "idle")}
              note={`${formatNumberMetric(liveSummary.finviz_feed_symbol_count, pendingLabel)} symbols · ${readString(readRecord(finvizFeedCheck.metrics).job_run_id)}`}
            />
            <StatusLine
              label="Direct Job"
              value={liveSummary.finviz_direct_status ?? (loading ? "loading" : "idle")}
              note={`${formatNumberMetric(finvizDirectMetrics.created_count, pendingLabel)} created · ${formatNumberMetric(finvizDirectMetrics.entry_candidates, pendingLabel)} candidates`}
            />
            <div className="mt-3 flex flex-wrap gap-2">
              <Link
                href={`/opportunities?marketDate=${encodeURIComponent(marketDate)}`}
                className={buttonVariants({ variant: "outline", size: "sm" })}
              >
                Opportunities
              </Link>
              <Link href="/positions" className={buttonVariants({ variant: "outline", size: "sm" })}>
                Positions
              </Link>
              <Link href="/automations" className={buttonVariants({ variant: "outline", size: "sm" })}>
                Automations
              </Link>
            </div>
          </div>
        </section>

        <section className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
          <div className="mb-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Decision Reasons</div>
          {directReasonRows.length ? (
            <div className="space-y-2">
              {directReasonRows.slice(0, 6).map((row) => (
                <div key={row.reason} className="flex items-center justify-between gap-3 text-sm">
                  <div className="min-w-0 truncate">{humanizeToken(row.reason)}</div>
                  <Badge variant="outline">{formatQuantity(row.count)}</Badge>
                </div>
              ))}
            </div>
          ) : (
            <div className="text-sm text-muted-foreground">No decision reasons reported.</div>
          )}
          <div className="mt-4 border-t border-border/60 pt-3">
            <div className="mb-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Execution Runtime</div>
            {runtimeRows.map((row) => (
              <div key={row.runtime} className="flex items-center justify-between gap-3 text-sm">
                <span>{humanizeToken(row.runtime)}</span>
                <RuntimeStatusBadge value={row.status} />
              </div>
            ))}
          </div>
        </section>
      </div>

      <section className="grid gap-4 xl:grid-cols-[0.85fr_1.15fr]">
        <div className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
          <div className="mb-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Attention</div>
          {attention.length ? (
            <div className="space-y-2">
              {attention.slice(0, 8).map((row, index) => (
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
        </div>

        <div className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
          <div className="mb-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Health Checks</div>
          <div className="divide-y divide-border/60">
            {checks.slice(0, 10).map((row) => (
              <div key={readString(row.name)} className="grid gap-2 py-2 text-sm md:grid-cols-[minmax(120px,0.4fr)_1fr_auto] md:items-center">
                <div className="font-medium">{readString(row.name)}</div>
                <div className="min-w-0 truncate text-muted-foreground">{readString(row.message)}</div>
                <RuntimeStatusBadge value={row.status} />
              </div>
            ))}
          </div>
        </div>
      </section>

      <div className="grid gap-4 md:grid-cols-3">
        <Link href="/ops" className={buttonVariants({ variant: "outline" })}>
          Ops resources
        </Link>
        <Link href="/account" className={buttonVariants({ variant: "outline" })}>
          Broker account
        </Link>
        <a href={grafanaTradingLogsUrl({ from: "now-6h" })} target="_blank" rel="noreferrer" className={buttonVariants({ variant: "outline" })}>
          <ExternalLink data-icon="inline-start" />
          Six-hour logs
        </a>
      </div>

      <div className={`text-xs ${valueTone(netPnl)}`}>
        Session PnL: {formatNullableCurrency(netPnl)}
      </div>
    </div>
  );
}
