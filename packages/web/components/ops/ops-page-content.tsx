"use client";

import { useQuery } from "@tanstack/react-query";
import { Database, ExternalLink, HardDrive, RefreshCw, Server } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import { getStorageOpsState, getTradingOpsState } from "@/lib/api";
import { useGrafanaTradingLogsUrl } from "@/lib/grafana";
import {
  formatBytes,
  formatCompactNumber,
  formatTimestamp,
  EntryQualityWaterfallSummary,
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

function formatOptionalCompact(value: unknown): string {
  return value === undefined || value === null ? "-" : formatCompactNumber(readNumber(value));
}

function groupLabels(value: unknown, limit = 3): string {
  const labels = readRecordList(value)
    .map((row) => {
      const label = humanizeToken(row.label ?? row.group, "");
      const count = readNumber(row.count, Number.NaN);
      return label && Number.isFinite(count) ? `${label} ${formatCompactNumber(count)}` : label;
    })
    .filter((value) => value.length > 0)
    .slice(0, limit);
  return labels.length ? labels.join(" · ") : "-";
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
  const executionContract = readRecord(tradingDetails.execution_contract);
  const primaryExecutionContract = readRecord(executionContract.primary_strategy_contract);
  const primaryFlow = readRecord(tradingDetails.primary_trading_flow);
  const entryPosture = readRecord(primaryFlow.entry_posture);
  const brokerExposure = readRecord(tradingDetails.broker_exposure);
  const workers = readRecordList(tradingDetails.workers);
  const workerLanes = readRecordList(tradingDetails.worker_lanes);
  const tradingFlows = readRecordList(tradingDetails.trading_flows);
  const strategyBreadth = readRecord(tradingDetails.strategy_breadth);
  const strategyBreadthSummary = readRecord(strategyBreadth.summary);
  const strategyBreadthRows = readRecordList(strategyBreadth.strategies);
  const tradingFlowsWithQuality = tradingFlows.filter((flow) => {
    const waterfall = readRecord(readRecord(flow.candidate_state).quality_waterfall);
    return readString(waterfall.profile_id, "") !== "" || Object.keys(readRecord(waterfall.stage_counts)).length > 0;
  });
  const runningJobs = latestJobRows(readRecordList(tradingDetails.running_jobs));
  const queuedJobs = latestJobRows(readRecordList(tradingDetails.queued_jobs));
  const engine = readRecord(tradingDetails.engine);
  const engineSummary = readRecord(engine.summary);
  const quoteTable = storageTables.find((row) => readString(row.name, "") === "option_quote_ticks") ?? {};
  const tradeTable = storageTables.find((row) => readString(row.name, "") === "option_trade_ticks") ?? {};
  const marketDataReady = Boolean(storageSummary.market_data_tables_ready);
  const attention = [...readRecordList(tradingState?.attention), ...readRecordList(storageState?.attention)];
  const hasQueryError = tradingOpsQuery.isError || storageOpsQuery.isError;
  const scheduler = readRecord(tradingDetails.scheduler);
  const marketSessionStatus = tradingSummary.market_session_status;
  const schedulerStatus = firstPresent(tradingSummary.scheduler_status, scheduler.status);
  const authoredStrategyCount = firstPresent(tradingSummary.strategy_count, strategyBreadthSummary.strategy_count);
  const activeStrategyCount = firstPresent(tradingSummary.active_strategy_count, strategyBreadthSummary.active_strategy_count);
  const availableStrategyCount = firstPresent(tradingSummary.available_strategy_count, strategyBreadthSummary.available_strategy_count);
  const availableShadowStrategyCount = firstPresent(tradingSummary.available_shadow_strategy_count, strategyBreadthSummary.available_shadow_strategy_count);
  const availablePaperStrategyCount = firstPresent(tradingSummary.available_paper_strategy_count, strategyBreadthSummary.available_paper_strategy_count);
  const tradingLogsUrl = useGrafanaTradingLogsUrl();

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
            <a href={tradingLogsUrl} target="_blank" rel="noreferrer" className={buttonVariants({ variant: "outline" })}>
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

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricTile
          label="Execution Mode"
          value={humanizeToken(firstPresent(tradingSummary.execution_posture, primaryExecutionContract.execution_posture), loading ? "loading" : "unknown")}
          note={`${humanizeToken(firstPresent(tradingSummary.broker_environment, executionContract.broker_environment), loading ? "loading" : "unknown")} · ${humanizeToken(
            firstPresent(tradingSummary.execution_contract_status, executionContract.status),
            loading ? "loading" : "unknown",
          )}`}
        />
        <MetricTile
          label="Engine"
          value={humanizeToken(firstPresent(tradingSummary.engine_status, engine.status), loading ? "loading" : "idle")}
          note={`${formatCompactNumber(readNumber(engineSummary.signal_count))} signals · ${formatCompactNumber(readNumber(engineSummary.selected_count))} selected`}
        />
        <MetricTile
          label="Entry Posture"
          value={humanizeToken(firstPresent(tradingSummary.primary_entry_state, entryPosture.state), loading ? "loading" : "unknown")}
          note={readString(firstPresent(tradingSummary.primary_entry_message, entryPosture.message), "-")}
        />
        <MetricTile
          label="Broker Exposure"
          value={`${formatOptionalCompact(tradingSummary.broker_option_position_count)} option legs`}
          note={`${formatOptionalCompact(tradingSummary.external_manual_broker_option_position_count)} external · ${formatOptionalCompact(
            tradingSummary.spreads_managed_broker_option_position_count,
          )} managed`}
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
          label="Quote Ticks"
          value={formatCompactNumber(readNumber(quoteTable.estimated_live_rows))}
          note={formatBytes(readNumber(quoteTable.total_size_bytes))}
        />
        <MetricTile
          label="Storage"
          value={marketDataReady ? "Ready" : "Review"}
          note={`capture ${formatTimestamp(readString(storageSummary.latest_captured_at, ""))}`}
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

      {strategyBreadthRows.length ? (
        <section className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
          <div className="mb-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Strategy Breadth</div>
          <div className="grid gap-4 md:grid-cols-3">
            <MetricTile
              label="Authored"
              value={formatOptionalCompact(authoredStrategyCount)}
              note={`${formatOptionalCompact(activeStrategyCount)} active`}
            />
            <MetricTile
              label="Available"
              value={formatOptionalCompact(availableStrategyCount)}
              note={`${formatOptionalCompact(availableShadowStrategyCount)} shadow · ${formatOptionalCompact(availablePaperStrategyCount)} paper`}
            />
            <MetricTile
              label="Execution"
              value={humanizeToken(firstPresent(tradingSummary.execution_posture, primaryExecutionContract.execution_posture), loading ? "loading" : "unknown")}
              note={humanizeToken(firstPresent(tradingSummary.execution_contract_status, executionContract.status), loading ? "loading" : "unknown")}
            />
          </div>
          <div className="mt-4 overflow-x-auto rounded-lg border border-border/70">
            <div className="min-w-[900px]">
              <div className="grid grid-cols-[1.2fr_0.8fr_1fr_0.7fr_0.8fr_1fr] gap-3 border-b border-border/70 bg-background/70 px-3 py-2 text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
                <span>Strategy</span>
                <span>Structure</span>
                <span>Posture</span>
                <span>Execution</span>
                <span>Evidence</span>
                <span>Reason</span>
              </div>
              <div className="divide-y divide-border/60">
                {strategyBreadthRows.slice(0, 12).map((row) => {
                  const isActive = row.active === true;
                  const latestObservation = readRecord(row.latest_observation);
                  return (
                    <div
                      key={readString(row.trading_strategy_id, "strategy")}
                      className="grid grid-cols-[1.2fr_0.8fr_1fr_0.7fr_0.8fr_1fr] gap-3 px-3 py-2 text-sm"
                    >
                      <span className="min-w-0 truncate font-medium">{readString(row.trading_strategy_id, "-")}</span>
                      <span className="min-w-0 truncate">{humanizeToken(row.trade_structure, "-")}</span>
                      <span className="min-w-0 truncate">{humanizeToken(row.ops_posture, "-")}</span>
                      <span className="min-w-0 truncate">{humanizeToken(row.execution_mode, "-")}</span>
                      <span className="min-w-0 truncate">
                        {formatOptionalCompact(latestObservation.candidate_count)} / {formatOptionalCompact(latestObservation.signal_count)}
                      </span>
                      <span className="min-w-0 truncate">{humanizeToken(row.not_active_reason, isActive ? "active" : "-")}</span>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        </section>
      ) : null}

      {tradingFlowsWithQuality.length ? (
        <section className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
          <div className="mb-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Entry Quality</div>
          <div className="grid gap-3">
            {tradingFlowsWithQuality.map((flow) => {
              const candidateState = readRecord(flow.candidate_state);
              const flowEntryPosture = readRecord(flow.entry_posture);
              return (
                <div key={readString(flow.trading_strategy_id, "flow")} className="min-w-0">
                  <div className="mb-2 flex flex-wrap items-center gap-2 text-sm">
                    <span className="font-medium">{readString(flow.trading_strategy_id, "strategy")}</span>
                    <RuntimeStatusBadge value={flow.status} />
                    <Badge variant="outline">{humanizeToken(flowEntryPosture.state, "entry")}</Badge>
                  </div>
                  <div className="mb-3 rounded-lg border border-border/70 px-3 py-2 text-sm">
                    <div className="font-medium">{readString(flowEntryPosture.message, "-")}</div>
                    <div className="mt-1 text-xs text-muted-foreground">{groupLabels(flowEntryPosture.blocker_groups)}</div>
                  </div>
                  <EntryQualityWaterfallSummary value={candidateState.quality_waterfall} />
                </div>
              );
            })}
          </div>
        </section>
      ) : null}

      {readRecordList(brokerExposure.positions).length ? (
        <section className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
          <div className="mb-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Broker Exposure Ownership</div>
          <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-4">
            {readRecordList(brokerExposure.positions)
              .slice(0, 8)
              .map((row) => (
                <div key={readString(row.symbol)} className="rounded-lg border border-border/70 px-3 py-2 text-sm">
                  <div className="flex items-center justify-between gap-3">
                    <span className="truncate font-medium">{readString(row.symbol)}</span>
                    <Badge variant="outline">{humanizeToken(row.ownership)}</Badge>
                  </div>
                  <div className="mt-1 truncate text-xs text-muted-foreground">
                    {humanizeToken(row.asset_class)} · {humanizeToken(row.side)} · {readString(row.spreads_position_id, "external")}
                  </div>
                </div>
              ))}
          </div>
        </section>
      ) : null}

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
          Market Data Storage
        </div>
        <div className="grid gap-4 md:grid-cols-3">
          <MetricTile label="ClickHouse" value={marketDataReady ? "Ready" : "Review"} note={readString(storageSummary.market_data_database, "-")} />
          <MetricTile
            label="Raw Ticks"
            value={formatBytes(readNumber(quoteTable.total_size_bytes) + readNumber(tradeTable.total_size_bytes))}
            note={`${formatCompactNumber(readNumber(quoteTable.estimated_live_rows) + readNumber(tradeTable.estimated_live_rows))} rows`}
          />
          <MetricTile
            label="Latest Capture"
            value={humanizeToken(storageSummary.latest_capture_status, "unknown")}
            note={`${formatCompactNumber(readNumber(storageSummary.latest_quote_rows_saved))} quotes / ${formatCompactNumber(readNumber(storageSummary.latest_trade_rows_saved))} trades`}
          />
        </div>
        <div className="mt-4 overflow-hidden rounded-lg border border-border/70">
          <div className="grid grid-cols-[1.2fr_0.8fr_0.6fr_0.8fr_0.8fr] gap-3 border-b border-border/70 bg-background/70 px-3 py-2 text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
            <span>Table</span>
            <span>Engine</span>
            <span>Parts</span>
            <span>Rows</span>
            <span>Size</span>
          </div>
          <div className="divide-y divide-border/60">
            {storageTables.map((row) => {
              return (
                <div key={readString(row.name)} className="grid grid-cols-[1.2fr_0.8fr_0.6fr_0.8fr_0.8fr] gap-3 px-3 py-2 text-sm">
                  <span className="min-w-0 truncate font-medium">{readString(row.name)}</span>
                  <span>{readString(row.engine, "-")}</span>
                  <span>{formatOptionalCompact(row.active_part_count)}</span>
                  <span>{formatOptionalCompact(row.estimated_live_rows)}</span>
                  <span>{formatBytes(readNumber(row.total_size_bytes))}</span>
                </div>
              );
            })}
          </div>
        </div>
      </section>
    </div>
  );
}
