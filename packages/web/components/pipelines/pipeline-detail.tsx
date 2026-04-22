"use client";

import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { Activity, Radar, RefreshCw } from "lucide-react";

import {
  buildPipelineHref,
  getPipelineDetail,
  getPipelines,
  type PipelineListItem,
} from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import {
  AutoExecutionStatusBadge,
  CaptureStatusBadge,
  formatDate,
  formatNullableCurrency,
  formatQuantity,
  LoadingState,
  MetricTile,
  readNumber,
  readString,
  SectionSurface,
  RuntimeStatusBadge,
  TradeabilityBadge,
} from "@/components/operator/operator-primitives";

type PipelineDetailPageContentProps = {
  pipelineId: string;
  marketDate?: string;
};

type DetailRecord = Record<string, unknown>;

function readRecord(value: unknown): DetailRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as DetailRecord)
    : {};
}

function readRecordRows(value: unknown): DetailRecord[] {
  return Array.isArray(value)
    ? value.filter(
        (item): item is DetailRecord =>
          typeof item === "object" && item !== null && !Array.isArray(item),
      )
    : [];
}

function readOptionalNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function formatCompactPercent(value: number | null): string {
  return value == null ? "—" : `${(value * 100).toFixed(0)}%`;
}

function formatCompactScore(value: number | null): string {
  return value == null ? "—" : value.toFixed(1);
}

function formatUoaState(value: unknown): string {
  return readString(value, "unknown").replaceAll("_", " ");
}

function UoaRootList({
  title,
  rows,
  emptyMessage,
}: {
  title: string;
  rows: DetailRecord[];
  emptyMessage: string;
}) {
  return (
    <div className="rounded-2xl border border-border/70 bg-background/70 p-4">
      <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
        {title}
      </div>
      {!rows.length ? (
        <div className="mt-3 text-sm text-muted-foreground">{emptyMessage}</div>
      ) : (
        <div className="mt-3 flex flex-col gap-3">
          {rows.slice(0, 3).map((row, index) => (
            <div
              key={`${readString(row.underlying_symbol, "root")}:${index}`}
              className="rounded-2xl border border-border/70 bg-card/70 px-4 py-3"
            >
              <div className="flex flex-wrap items-start justify-between gap-2">
                <div className="min-w-0">
                  <div className="font-medium">
                    {readString(row.underlying_symbol, "—")}
                  </div>
                  <div className="mt-1 text-xs text-muted-foreground">
                    {readString(row.dominant_flow, "mixed")} flow ·{" "}
                    {formatCompactPercent(readOptionalNumber(row.dominant_flow_ratio))}
                  </div>
                </div>
                {row.decision_state ? (
                  <Badge variant="outline">{formatUoaState(row.decision_state)}</Badge>
                ) : null}
              </div>
              <div className="mt-3 grid gap-2 sm:grid-cols-3">
                <MetricTile
                  label="score"
                  value={formatCompactScore(
                    readOptionalNumber(row.decision_score) ??
                      readOptionalNumber(row.root_score),
                  )}
                  note={`quality ${readString(row.quality_state, "unknown")}`}
                />
                <MetricTile
                  label="premium"
                  value={formatNullableCurrency(readOptionalNumber(row.scoreable_premium))}
                  note={`${readNumber(row.scoreable_trade_count)} trades`}
                />
                <MetricTile
                  label="contracts"
                  value={String(readNumber(row.scoreable_contract_count))}
                  note={`best vol/oi ${formatCompactPercent(readOptionalNumber(row.max_volume_oi_ratio))}`}
                />
              </div>
              {row.explanation ? (
                <div className="mt-3 text-sm text-foreground/75">
                  {readString(row.explanation)}
                </div>
              ) : null}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function UoaContractList({ rows }: { rows: DetailRecord[] }) {
  return (
    <div className="rounded-2xl border border-border/70 bg-background/70 p-4">
      <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
        Top contracts
      </div>
      {!rows.length ? (
        <div className="mt-3 text-sm text-muted-foreground">
          No scoreable contracts were captured for this cycle.
        </div>
      ) : (
        <div className="mt-3 flex flex-col gap-3">
          {rows.slice(0, 4).map((row, index) => (
            <div
              key={`${readString(row.option_symbol, "contract")}:${index}`}
              className="rounded-2xl border border-border/70 bg-card/70 px-4 py-3"
            >
              <div className="font-mono break-all text-[12px] font-medium">
                {readString(row.option_symbol, "—")}
              </div>
              <div className="mt-1 text-xs text-muted-foreground">
                {readString(row.underlying_symbol, "—")} ·{" "}
                {readString(row.option_type, "option")} · DTE {readNumber(row.dte)}
              </div>
              <div className="mt-3 grid gap-2 sm:grid-cols-3">
                <MetricTile
                  label="premium"
                  value={formatNullableCurrency(readOptionalNumber(row.scoreable_premium))}
                  note={`${readNumber(row.scoreable_trade_count)} trades`}
                />
                <MetricTile
                  label="quality"
                  value={formatCompactScore(readOptionalNumber(row.quality_score))}
                  note={readString(row.quality_state, "unknown")}
                />
                <MetricTile
                  label="vol/oi"
                  value={formatCompactScore(readOptionalNumber(row.volume_oi_ratio))}
                  note={`age ${formatQuantity(readOptionalNumber(row.quote_age_seconds))}s`}
                />
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function autoExecutionTarget(summary: Record<string, unknown> | null | undefined): string {
  const symbol = readString(summary?.selected_symbol, "");
  if (symbol) {
    return symbol;
  }
  return readString(summary?.selected_opportunity_id ?? summary?.top_opportunity_id, "—");
}

function autoExecutionTargetNote(summary: Record<string, unknown> | null | undefined): string {
  const strategy = readString(summary?.selected_strategy_family, "");
  if (strategy) {
    return strategy.replaceAll("_", " ");
  }
  return readString(summary?.decision_reason ?? summary?.reason, "No opportunity selected");
}

function autoExecutionBlockers(summary: Record<string, unknown> | null | undefined): string {
  const blockers = Array.isArray(summary?.execution_blockers)
    ? summary.execution_blockers.map((value) => String(value)).filter(Boolean)
    : [];
  if (!blockers.length) {
    return readString(summary?.message, "No blockers recorded.");
  }
  return blockers.join(", ");
}

export function PipelineDetailPageContent({
  pipelineId,
  marketDate,
}: PipelineDetailPageContentProps) {
  const pipelinesQuery = useQuery({
    queryKey: ["pipelines"],
    queryFn: () => getPipelines({ limit: 120 }),
  });
  const detailQuery = useQuery({
    queryKey: ["pipelines", pipelineId, marketDate ?? ""],
    queryFn: () => getPipelineDetail(pipelineId, { marketDate }),
  });

  const pipelineRows = pipelinesQuery.data?.pipelines ?? [];
  const detail = detailQuery.data;
  const quoteCapture = readRecord(detail?.quote_capture);
  const tradeCapture = readRecord(detail?.trade_capture);
  const uoaSummary = readRecord(detail?.uoa_summary);
  const uoaQuoteSummary = readRecord(detail?.uoa_quote_summary);
  const uoaDecisions = readRecord(detail?.uoa_decisions);
  const uoaOverview = readRecord(uoaSummary.overview);
  const uoaQuoteOverview = readRecord(uoaQuoteSummary.overview);
  const uoaDecisionOverview = readRecord(uoaDecisions.overview);
  const topPromotableRoots = readRecordRows(uoaDecisions.top_promotable_roots);
  const topHighRoots = readRecordRows(uoaDecisions.top_high_roots);
  const topMonitorRoots = readRecordRows(uoaDecisions.top_monitor_roots);
  const topContracts = readRecordRows(uoaSummary.top_contracts);
  const secondaryRootRows = topHighRoots.length ? topHighRoots : topMonitorRoots;
  const secondaryRootTitle = topHighRoots.length
    ? "Top high roots"
    : "Top monitor roots";
  const hasUoaData =
    Object.keys(uoaSummary).length > 0 ||
    Object.keys(uoaQuoteSummary).length > 0 ||
    Object.keys(uoaDecisions).length > 0;

  if (detailQuery.isLoading) {
    return <LoadingState />;
  }

  if (detailQuery.isError || !detail) {
    return (
      <div className="flex flex-col gap-4">
        <div className="app-tone-error rounded-2xl border px-4 py-3 text-sm">
          Discovery diagnostics could not be loaded.
        </div>
        <Link href="/pipelines" className={buttonVariants({ variant: "outline" })}>
          Back to diagnostics
        </Link>
      </div>
    );
  }

  const latestPipeline =
    pipelineRows.find((row) => row.pipeline_id === pipelineId) ?? null;

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
                <Radar data-icon="inline-start" />
                Diagnostics
              </Badge>
              <RuntimeStatusBadge value={detail.status} />
              <CaptureStatusBadge
                value={readString(detail.latest_slot?.capture_status, "") || undefined}
              />
              <TradeabilityBadge value={detail.tradeability_state} />
              {detail.latest_auto_execution ? (
                <AutoExecutionStatusBadge value={detail.latest_auto_execution.status} />
              ) : null}
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">
              {detail.label}
            </div>
            <div className="mt-2 text-sm text-foreground/70">
              Market date {formatDate(detail.market_date)}. Use this view for
              read-only discovery-run diagnostics, cycle state, and linked
              runtime-linked outcomes.
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            <Button
              type="button"
              variant="outline"
              onClick={() => void detailQuery.refetch()}
            >
              <RefreshCw data-icon="inline-start" />
              Refresh
            </Button>
            <Link href="/pipelines" className={buttonVariants({ variant: "outline" })}>
              All diagnostics
            </Link>
          </div>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <MetricTile
          label="Diagnostics"
          value={detail.label}
          note={latestPipeline?.style_profile ?? "runtime"}
        />
        <MetricTile
          label="Promotable"
          value={String(detail.selection_counts.promotable)}
          note="Current cycle"
        />
        <MetricTile
          label="Monitor"
          value={String(detail.selection_counts.monitor)}
          note="Current cycle"
        />
        <MetricTile
          label="Open Positions"
          value={String(detail.portfolio.summary.open_position_count)}
          note={formatNullableCurrency(detail.portfolio.summary.net_pnl_total)}
        />
        <MetricTile
          label="Risk"
          value={readString(detail.risk_status)}
          note={readString(detail.risk_note)}
        />
      </div>

      <SectionSurface
        title="UOA"
        description="Cycle-local unusual options activity, quote quality, and root decision diagnostics for this discovery run."
      >
        {!hasUoaData ? (
          <div className="rounded-2xl border border-dashed border-border/70 bg-background/60 px-4 py-3 text-sm text-muted-foreground">
            No UOA summary was recorded for this cycle.
          </div>
        ) : (
          <div className="flex flex-col gap-4">
            <div className="flex flex-wrap items-center gap-2">
              <Badge
                variant="outline"
                className="rounded-full border-border/70 bg-background/80 px-2.5 py-1 text-[11px] uppercase tracking-[0.18em] text-muted-foreground"
              >
                <Activity data-icon="inline-start" />
                UOA
              </Badge>
              {quoteCapture.capture_status ? (
                <CaptureStatusBadge value={String(quoteCapture.capture_status)} />
              ) : null}
              {tradeCapture.capture_status ? (
                <CaptureStatusBadge value={String(tradeCapture.capture_status)} />
              ) : null}
              {uoaOverview.summary_status ? (
                <Badge variant="outline">{formatUoaState(uoaOverview.summary_status)}</Badge>
              ) : null}
              {uoaDecisionOverview.decision_status ? (
                <Badge variant="outline">
                  {formatUoaState(uoaDecisionOverview.decision_status)}
                </Badge>
              ) : null}
            </div>

            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
              <MetricTile
                label="Trades"
                value={String(readNumber(uoaOverview.scoreable_trade_count))}
                note={`Raw ${readNumber(uoaOverview.raw_trade_count)} · Excluded ${readNumber(uoaOverview.excluded_trade_count)}`}
              />
              <MetricTile
                label="Roots"
                value={String(readNumber(uoaDecisionOverview.root_count))}
                note={`${readNumber(uoaDecisionOverview.monitor_count)} monitor · ${readNumber(uoaDecisionOverview.promotable_count)} promotable`}
              />
              <MetricTile
                label="Contracts"
                value={String(readNumber(uoaOverview.observed_contract_count))}
                note={`${readNumber(uoaQuoteOverview.fresh_contract_count)} fresh · ${readNumber(uoaQuoteOverview.liquid_contract_count)} liquid`}
              />
              <MetricTile
                label="Top decision"
                value={readString(uoaDecisionOverview.top_decision_symbol, "—")}
                note={`${formatUoaState(uoaDecisionOverview.top_decision_state)} · score ${formatCompactScore(readOptionalNumber(uoaDecisionOverview.top_decision_score))}`}
              />
              <MetricTile
                label="Premium"
                value={formatNullableCurrency(readOptionalNumber(uoaOverview.scoreable_premium))}
                note={`${readNumber(uoaOverview.scoreable_root_count)} scoreable roots`}
              />
            </div>

            <div className="grid gap-4 xl:grid-cols-3">
              <UoaRootList
                title="Top promotable roots"
                rows={topPromotableRoots}
                emptyMessage="No promotable roots were recorded."
              />
              <UoaRootList
                title={secondaryRootTitle}
                rows={secondaryRootRows}
                emptyMessage={`No ${secondaryRootTitle.toLowerCase()} were recorded.`}
              />
              <UoaContractList rows={topContracts} />
            </div>
          </div>
        )}
      </SectionSurface>

      <SectionSurface
        title="Latest Auto Execution"
        description="Most recent owner-plane decision linked to this diagnostic view."
      >
        {detail.latest_auto_execution ? (
          <div className="grid gap-4 xl:grid-cols-[0.95fr_1.05fr]">
            <div className="rounded-2xl border border-border/70 bg-background/75 p-4">
              <div className="flex flex-wrap items-center gap-2">
                <AutoExecutionStatusBadge value={detail.latest_auto_execution.status} />
                <span className="text-xs uppercase tracking-[0.16em] text-muted-foreground">
                  {readString(detail.latest_auto_execution.reason, "latest decision")}
                </span>
              </div>
              <div className="mt-3 text-lg font-medium">
                {autoExecutionTarget(detail.latest_auto_execution)}
              </div>
              <div className="mt-1 text-sm text-muted-foreground">
                {autoExecutionTargetNote(detail.latest_auto_execution)}
              </div>
              <div className="mt-3 text-sm text-foreground/80">
                {readString(detail.latest_auto_execution.message, "No auto execution result has been recorded yet.")}
              </div>
            </div>
            <div className="grid gap-3 sm:grid-cols-3">
              <MetricTile
                label="Allocation"
                value={
                  detail.latest_auto_execution.allocation_score == null
                    ? "—"
                    : formatQuantity(detail.latest_auto_execution.allocation_score)
                }
                note="portfolio-adjusted score"
              />
              <MetricTile
                label="Planner"
                value={`${readNumber(detail.latest_auto_execution.candidate_count)}/${readNumber(detail.latest_auto_execution.allocation_count)}/${readNumber(detail.latest_auto_execution.execution_intent_count)}`}
                note="candidates / allocations / intents"
              />
              <MetricTile
                label="Selected"
                value={readString(detail.latest_auto_execution.selected_opportunity_id, "—")}
                note="canonical opportunity id"
              />
              <div className="sm:col-span-3 rounded-2xl border border-border/70 bg-background/75 p-4 text-sm text-foreground/80">
                <div className="text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
                  Execution blockers
                </div>
                <div className="mt-2">{autoExecutionBlockers(detail.latest_auto_execution)}</div>
              </div>
            </div>
          </div>
        ) : (
          <div className="rounded-2xl border border-dashed border-border/70 bg-background/60 px-4 py-3 text-sm text-muted-foreground">
            No auto execution decision has been recorded for the latest slot yet.
          </div>
        )}
      </SectionSurface>

      <SectionSurface
        title="Captured Dates"
        description="Switch between persisted diagnostic dates for this discovery run. Use Opportunities, Positions, and Runtime links for active operator work."
      >
        {!pipelineRows.length ? (
          <div className="text-sm text-muted-foreground">
            No persisted diagnostics were found.
          </div>
        ) : (
          <div className="flex flex-wrap gap-2">
            {pipelineRows.map((row: PipelineListItem) => (
              <Link
                key={`${row.pipeline_id}:${row.latest_market_date}`}
                href={buildPipelineHref(row.pipeline_id, row.latest_market_date)}
                className={buttonVariants({
                  variant:
                    row.pipeline_id === pipelineId &&
                    row.latest_market_date === detail.market_date
                      ? "default"
                      : "outline",
                  size: "sm",
                })}
              >
                {row.label} · {formatDate(row.latest_market_date)}
              </Link>
            ))}
          </div>
        )}
      </SectionSurface>
    </div>
  );
}
