"use client";

import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { Activity, ExternalLink, Gauge, RefreshCw, ShieldCheck } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import { getTradingOpsState } from "@/lib/api";
import { useGrafanaTradingLogsUrl } from "@/lib/grafana";
import {
  formatNullableCurrency,
  formatQuantity,
  formatTimestamp,
  EntryQualityWaterfallSummary,
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
    return "-";
  }
  if (value < 60) {
    return `${Math.round(value)}s`;
  }
  if (value < 60 * 60) {
    return `${Math.round(value / 60)}m`;
  }
  return `${(value / 3600).toFixed(1)}h`;
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

function countEntries(value: unknown, limit = 5): Array<[string, number]> {
  return Object.entries(readRecord(value))
    .map(([key, rawValue]) => [key, readNumber(rawValue)] as [string, number])
    .filter(([key, count]) => key.length > 0 && Number.isFinite(count) && count > 0)
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, limit);
}

function groupLabels(value: unknown, limit = 3): string {
  const labels = readRecordList(value)
    .map((row) => {
      const label = humanizeToken(row.label ?? row.group, "");
      const count = readNumber(row.count, Number.NaN);
      return label && Number.isFinite(count) ? `${label} ${formatQuantity(count)}` : label;
    })
    .filter((value) => value.length > 0)
    .slice(0, limit);
  return labels.length ? labels.join(" · ") : "-";
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
  const tradingOpsQuery = useQuery({
    queryKey: ["trading-ops-state"],
    queryFn: () => getTradingOpsState(),
  });

  const loading = tradingOpsQuery.isLoading;
  const state = tradingOpsQuery.data;
  const summary = readRecord(state?.summary);
  const details = readRecord(state?.details);
  const engine = readRecord(details.engine);
  const engineSummary = readRecord(engine.summary);
  const executionContract = readRecord(details.execution_contract);
  const primaryExecutionContract = readRecord(executionContract.primary_strategy_contract);
  const primaryFlow = readRecord(details.primary_trading_flow);
  const sourceState = readRecord(primaryFlow.source_state);
  const candidateState = readRecord(primaryFlow.candidate_state);
  const entryPosture = readRecord(primaryFlow.entry_posture);
  const intentState = readRecord(primaryFlow.intent_state);
  const capacity = readRecord(primaryFlow.capacity);
  const brokerExposure = readRecord(details.broker_exposure);
  const executionRuntimes = readRecord(details.execution_runtimes);
  const runtimeRows = readRecordList(executionRuntimes.runtimes);
  const attention = readRecordList(state?.attention);
  const sourceSymbols = Array.isArray(sourceState.symbols) ? sourceState.symbols.map(String).slice(0, 12) : [];
  const latestTickerSourceRun = readRecord(sourceState.latest_run);
  const latestCandidateRun = readRecord(candidateState.latest_run);
  const topCandidateBlockers = countEntries(candidateState.top_rejection_counts);
  const candidateDiagnostics = readRecordList(candidateState.diagnostics).slice(0, 10);
  const qualityWaterfall = readRecord(candidateState.quality_waterfall);
  const hasQualityWaterfall =
    readString(qualityWaterfall.profile_id, "") !== "" ||
    Object.keys(readRecord(qualityWaterfall.stage_counts)).length > 0;

  const pendingLabel = loading ? "Loading" : "-";
  const marketDate = readString(summary.market_date, "");
  const marketSessionStatus = summary.market_session_status;
  const tradingAllowed = summary.trading_allowed;
  const environment = firstPresent(summary.broker_environment, executionContract.broker_environment);
  const executionPosture = firstPresent(summary.execution_posture, primaryExecutionContract.execution_posture);
  const executionContractStatus = firstPresent(summary.execution_contract_status, executionContract.status);
  const controlMode = summary.control_mode;
  const generatedAt = state?.generated_at;
  const openPositions = formatNumberMetric(firstPresent(summary.open_position_count, capacity.open_position_count), pendingLabel);
  const maxOpenPositions = formatNumberMetric(firstPresent(summary.max_open_positions, capacity.max_open_positions), pendingLabel);
  const activeIntents = formatNumberMetric(firstPresent(summary.active_intent_count, intentState.active_intent_count), pendingLabel);
  const remainingEntries = formatNumberMetric(firstPresent(summary.remaining_daily_entries, capacity.remaining_daily_entries), pendingLabel);
  const maxDailyEntries = formatNumberMetric(firstPresent(summary.max_daily_entries, capacity.max_daily_entries), pendingLabel);
  const engineSignalCount = formatNumberMetric(firstPresent(summary.engine_signal_count, engineSummary.signal_count), pendingLabel);
  const engineCandidateCount = formatNumberMetric(firstPresent(summary.engine_trade_candidate_count, engineSummary.trade_candidate_count), pendingLabel);
  const engineDecisionCount = formatNumberMetric(firstPresent(summary.engine_decision_count, engineSummary.decision_count), pendingLabel);
  const brokerOptionPositions = formatNumberMetric(summary.broker_option_position_count, pendingLabel);
  const externalBrokerOptionPositions = formatNumberMetric(summary.external_manual_broker_option_position_count, pendingLabel);
  const managedBrokerOptionPositions = formatNumberMetric(summary.spreads_managed_broker_option_position_count, pendingLabel);
  const netPnl = hasMetricValue(summary.net_pnl) ? readNumber(summary.net_pnl) : null;
  const realizedPnl = hasMetricValue(summary.realized_pnl) ? readNumber(summary.realized_pnl) : null;
  const tradingLogsUrl = useGrafanaTradingLogsUrl();
  const sixHourLogsUrl = useGrafanaTradingLogsUrl({ from: "now-6h" });

  const refreshAll = () => {
    void tradingOpsQuery.refetch();
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
              <RuntimeStatusBadge value={state?.status ?? (loading ? "loading" : "idle")} />
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
            <a href={tradingLogsUrl} target="_blank" rel="noreferrer" className={buttonVariants({ variant: "outline" })}>
              <ExternalLink data-icon="inline-start" />
              Trading logs
            </a>
          </div>
        </div>
      </div>

      {tradingOpsQuery.isError ? (
        <div className="app-tone-error rounded-2xl border px-4 py-3 text-sm">Trading ops state could not be loaded.</div>
      ) : null}

      {loading ? (
        <div className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3 text-sm text-muted-foreground">
          Loading live command-center state.
        </div>
      ) : null}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-6">
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
          label="Execution Mode"
          value={humanizeToken(executionPosture, pendingLabel)}
          note={`${humanizeToken(environment, pendingLabel)} · ${humanizeToken(executionContractStatus, pendingLabel)}`}
        />
        <MetricTile
          label="Entry Engine"
          value={engineSignalCount === pendingLabel ? pendingLabel : `${engineSignalCount} signals`}
          note={`${engineCandidateCount} candidates · ${engineDecisionCount} decisions`}
        />
        <MetricTile
          label="Entry Posture"
          value={humanizeToken(firstPresent(summary.primary_entry_state, entryPosture.state), pendingLabel)}
          note={readString(firstPresent(summary.primary_entry_message, entryPosture.message), pendingLabel)}
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
              Live Gates
            </div>
            <StatusLine
              label="Schedules"
              value={summary.temporal_schedule_status ?? (loading ? "loading" : "idle")}
              note={`${formatNumberMetric(summary.task_queue_count, pendingLabel)} task queues`}
            />
            <StatusLine
              label="Broker Sync"
              value={summary.broker_sync_status ?? (loading ? "loading" : "idle")}
              note={`${formatAge(summary.broker_sync_age_seconds)} old`}
            />
            <StatusLine
              label="Broker Exposure"
              value={brokerExposure.status ?? (loading ? "loading" : "clear")}
              note={`${brokerOptionPositions} option legs · ${externalBrokerOptionPositions} external · ${managedBrokerOptionPositions} managed`}
            />
            <StatusLine
              label="Mode Contract"
              value={executionContractStatus ?? (loading ? "loading" : "unknown")}
              note={`${humanizeToken(executionPosture, pendingLabel)} · ${humanizeToken(environment, pendingLabel)}`}
            />
            <StatusLine
              label="Execution"
              value={summary.execution_health_status ?? "healthy"}
              note={`${activeIntents} active intents`}
            />
            <StatusLine
              label="Marks"
              value={summary.mark_health_status ?? "healthy"}
              note={`${formatNumberMetric(summary.open_position_count, pendingLabel)} open positions`}
            />
          </div>

          <div className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
            <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
              <Gauge className="size-4" />
              Momentum Calls
            </div>
            <StatusLine
              label="Ticker Source"
              value={sourceState.status ?? (loading ? "loading" : "idle")}
              note={`${formatNumberMetric(sourceState.symbol_count, pendingLabel)} symbols · ${formatAge(sourceState.age_seconds)} old`}
            />
            <StatusLine
              label="Candidates"
              value={candidateState.status ?? (loading ? "loading" : "idle")}
              note={`${formatNumberMetric(candidateState.candidate_count, pendingLabel)} candidates · ${humanizeToken(candidateState.diagnostic_status, pendingLabel)}`}
            />
            <StatusLine
              label="Entry Posture"
              value={entryPosture.status ?? (loading ? "loading" : "idle")}
              note={readString(entryPosture.message, pendingLabel)}
            />
            <StatusLine
              label="Flow"
              value={primaryFlow.status ?? (loading ? "loading" : "idle")}
              note={`${readString(primaryFlow.trading_strategy_id, "strategy")} · ${readString(primaryFlow.trade_structure, "calls")}`}
            />
            <StatusLine
              label="Capture"
              value={summary.capture_status ?? (loading ? "loading" : "idle")}
              note={`${formatNumberMetric(summary.capture_active_target_count, pendingLabel)} active targets`}
            />
            {topCandidateBlockers.length ? (
              <div className="mt-3 border-t border-border/60 pt-3">
                <div className="mb-2 text-[11px] uppercase tracking-[0.16em] text-muted-foreground">Top blockers</div>
                <div className="flex flex-wrap gap-2">
                  {readRecordList(entryPosture.blocker_groups).length ? (
                    <Badge variant="outline">{groupLabels(entryPosture.blocker_groups)}</Badge>
                  ) : null}
                  {topCandidateBlockers.map(([name, count]) => (
                    <Badge key={name} variant="outline">
                      {humanizeToken(name)} {formatQuantity(count)}
                    </Badge>
                  ))}
                </div>
              </div>
            ) : null}
            {hasQualityWaterfall ? (
              <div className="mt-3">
                <EntryQualityWaterfallSummary value={qualityWaterfall} compact />
              </div>
            ) : null}
            <div className="mt-3 flex flex-wrap gap-2">
              <Link href="/ops" className={buttonVariants({ variant: "outline", size: "sm" })}>
                Ops
              </Link>
              <Link href="/positions" className={buttonVariants({ variant: "outline", size: "sm" })}>
                Positions
              </Link>
            </div>
          </div>
        </section>

        <section className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
          <div className="mb-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Engine Runtime</div>
          <div className="space-y-2 text-sm">
            <div className="flex items-center justify-between gap-3">
              <span>Selected today</span>
              <Badge variant="outline">{formatNumberMetric(summary.engine_selected_count, pendingLabel)}</Badge>
            </div>
            <div className="flex items-center justify-between gap-3">
              <span>Entry intents</span>
              <Badge variant="outline">{formatNumberMetric(summary.engine_entry_intent_count, pendingLabel)}</Badge>
            </div>
            <div className="flex items-center justify-between gap-3">
              <span>Management intents</span>
              <Badge variant="outline">{formatNumberMetric(summary.engine_management_intent_count, pendingLabel)}</Badge>
            </div>
            <div className="flex items-center justify-between gap-3">
              <span>Capture targets</span>
              <Badge variant="outline">{formatNumberMetric(summary.capture_active_target_count, pendingLabel)}</Badge>
            </div>
          </div>
          <div className="mt-4 border-t border-border/60 pt-3">
            <div className="mb-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Execution Runtime</div>
            {runtimeRows.map((row) => (
              <div key={readString(row.runtime)} className="flex items-center justify-between gap-3 text-sm">
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
          <div className="mb-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Flow Evidence</div>
          <div className="grid gap-3 text-sm md:grid-cols-2">
            <div className="rounded-lg border border-border/70 px-3 py-2">
              <div className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Latest Ticker Source</div>
              <div className="mt-1 truncate font-medium">{readString(latestTickerSourceRun.ticker_source_run_id, "-")}</div>
              <div className="mt-1 text-xs text-muted-foreground">{formatTimestamp(readString(latestTickerSourceRun.generated_at, ""))}</div>
            </div>
            <div className="rounded-lg border border-border/70 px-3 py-2">
              <div className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Latest Candidates</div>
              <div className="mt-1 truncate font-medium">{readString(latestCandidateRun.candidate_run_id, "-")}</div>
              <div className="mt-1 text-xs text-muted-foreground">{formatTimestamp(readString(latestCandidateRun.generated_at, ""))}</div>
            </div>
          </div>
          <div className="mt-3 flex flex-wrap gap-2">
            {sourceSymbols.length ? (
              sourceSymbols.map((symbol) => (
                <Badge key={symbol} variant="outline">
                  {symbol}
                </Badge>
              ))
            ) : (
              <span className="text-sm text-muted-foreground">No source symbols loaded.</span>
            )}
          </div>
          {candidateDiagnostics.length ? (
            <div className="mt-3 border-t border-border/60 pt-3">
              <div className="mb-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Candidate Diagnostics</div>
              <div className="grid gap-2 sm:grid-cols-2">
                {candidateDiagnostics.map((row) => {
                  const symbol = readString(row.underlying_symbol, "-");
                  const status = humanizeToken(row.diagnostic_status, "-");
                  const blockers = countEntries(readRecord(row.rejection_counts).top, 2)
                    .map(([name, count]) => `${humanizeToken(name)} ${formatQuantity(count)}`)
                    .join(" · ");
                  return (
                    <div key={symbol} className="rounded-lg border border-border/70 px-3 py-2 text-sm">
                      <div className="flex items-center justify-between gap-3">
                        <span className="font-medium">{symbol}</span>
                        <Badge variant="outline">{status}</Badge>
                      </div>
                      <div className="mt-1 truncate text-xs text-muted-foreground">{blockers || "No blockers"}</div>
                    </div>
                  );
                })}
              </div>
            </div>
          ) : null}
        </div>
      </section>

      <div className="grid gap-4 md:grid-cols-3">
        <Link href="/ops" className={buttonVariants({ variant: "outline" })}>
          Ops resources
        </Link>
        <Link href="/account" className={buttonVariants({ variant: "outline" })}>
          Broker account
        </Link>
        <a href={sixHourLogsUrl} target="_blank" rel="noreferrer" className={buttonVariants({ variant: "outline" })}>
          <ExternalLink data-icon="inline-start" />
          Six-hour logs
        </a>
      </div>

      <div className={`text-xs ${valueTone(netPnl)}`}>Session PnL: {formatNullableCurrency(netPnl)}</div>
    </div>
  );
}
