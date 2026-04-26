"use client";

import Link from "next/link";
import { useEffect, useEffectEvent, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { differenceInCalendarDays, parseISO } from "date-fns";
import { usePathname, useRouter } from "next/navigation";
import {
  CandlestickChart,
  LoaderCircle,
  Radar,
  RefreshCw,
  Search,
} from "lucide-react";
import { startCase, trim } from "lodash-es";

import {
  buildPipelineHref,
  getPipelineDetail,
  getPipelines,
  startPipelineRun,
  type GlobalRealtimeEvent,
  type Opportunity,
  type PipelineRunResponse,
} from "@/lib/api";
import {
  CaptureStatusBadge,
  LoadingState,
  MetricTile,
  RuntimeStatusBadge,
  SectionSurface,
  TradeabilityBadge,
  formatDate,
  formatNullableCurrency,
  formatScore,
  formatTimestamp,
  readString,
} from "@/components/operator/operator-primitives";
import { useRealtimeActivity } from "@/components/providers";
import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

const MANUAL_LABEL_PREFIX = "manual_";
const STRATEGY_OPTIONS = [
  { value: "auto", label: "Auto" },
  { value: "call_credit_spread", label: "Call Credit Spread" },
  { value: "put_credit_spread", label: "Put Credit Spread" },
  { value: "call_debit_spread", label: "Call Debit Spread" },
  { value: "put_debit_spread", label: "Put Debit Spread" },
  { value: "iron_condor", label: "Iron Condor" },
  { value: "long_call", label: "Long Call" },
  { value: "long_put", label: "Long Put" },
] as const;
const TERMINAL_JOB_STATUSES = new Set(["failed", "skipped", "succeeded"]);

type ActiveRunState = {
  jobRunId: string;
  pipelineId: string;
  label: string;
  status: string;
  message: string | null;
  errorText: string | null;
  scheduledFor: string;
  startedAt: string | null;
  finishedAt: string | null;
};

function readInitialQueryValue(
  explicitValue: string | undefined,
  key: string,
): string | null {
  const directValue = readEventText(explicitValue);
  if (directValue != null) {
    return directValue;
  }
  if (typeof window === "undefined") {
    return null;
  }
  return readEventText(new URLSearchParams(window.location.search).get(key));
}

function humanizeStrategy(value: string | null | undefined): string {
  const normalized = readString(value, "unknown").replaceAll("_", " ");
  return startCase(normalized);
}

function readRecord(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function readOptionalNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function opportunityStrategy(candidate: Opportunity): string {
  return humanizeStrategy(candidate.strategy_family);
}

function opportunityDte(candidate: Opportunity): string {
  const expirationDate = readString(candidate.expiration_date, "");
  const marketDate = readString(candidate.market_date, "");
  if (expirationDate === "" || marketDate === "") {
    return "DTE —";
  }
  const dte = differenceInCalendarDays(parseISO(expirationDate), parseISO(marketDate));
  return `${dte} DTE`;
}

function opportunityMidpoint(candidate: Opportunity): number | null {
  return readOptionalNumber(readRecord(candidate.economics).midpoint_credit);
}

function opportunityReturnOnRisk(candidate: Opportunity): string {
  const value = readOptionalNumber(readRecord(candidate.economics).return_on_risk);
  if (value == null) {
    return "ROR —";
  }
  return `ROR ${(value * 100).toFixed(1)}%`;
}

function opportunityScore(candidate: Opportunity): number | null {
  return candidate.promotion_score ?? candidate.execution_score ?? null;
}

function readEventText(value: unknown): string | null {
  return typeof value === "string" && trim(value) !== "" ? trim(value) : null;
}

function buildInitialRunState(run: PipelineRunResponse): ActiveRunState {
  return {
    jobRunId: run.job_run_id,
    pipelineId: run.pipeline_id,
    label: run.label,
    status: run.status,
    message: "Queued for discovery-run execution.",
    errorText: null,
    scheduledFor: run.scheduled_for,
    startedAt: null,
    finishedAt: null,
  };
}

function cycleSummaryCount(
  cycle: Record<string, unknown>,
  key: string,
): string {
  const summary = cycle.summary;
  if (summary == null || typeof summary !== "object") {
    return "0";
  }
  const value = (summary as Record<string, unknown>)[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  return readString(value, "0");
}

function cycleGeneratedAt(cycle: Record<string, unknown>): string {
  return readString(cycle.generated_at, cycle.market_date);
}

function OpportunityCard({
  candidate,
  rank,
}: {
  candidate: Opportunity;
  rank: number;
}) {
  return (
    <div className="rounded-2xl border border-border/70 bg-background/70 px-4 py-4">
      <div className="flex flex-wrap items-center gap-2">
        <Badge variant="outline">#{rank}</Badge>
        <Badge variant="outline">{candidate.underlying_symbol}</Badge>
        <Badge variant="outline">{opportunityStrategy(candidate)}</Badge>
        <Badge variant="outline">{opportunityDte(candidate)}</Badge>
      </div>
      <div className="mt-3 grid gap-3 md:grid-cols-4">
        <MetricTile
          label="Score"
          value={formatScore(opportunityScore(candidate))}
          note={readString(candidate.state_reason, "Selection summary")}
        />
        <MetricTile
          label="Midpoint"
          value={formatNullableCurrency(opportunityMidpoint(candidate))}
          note={readString(candidate.expiration_date, "No expiration")}
        />
        <MetricTile
          label="Return On Risk"
          value={opportunityReturnOnRisk(candidate)}
          note={readString(candidate.selection_state, "unknown")}
        />
        <MetricTile
          label="Profile"
          value={humanizeStrategy(candidate.style_profile)}
          note={readString(candidate.lifecycle_state, "candidate")}
        />
      </div>
    </div>
  );
}

type ManualScanPageContentProps = {
  initialPipelineId?: string;
  initialCycleId?: string;
};

export function ManualScanPageContent({
  initialPipelineId,
  initialCycleId,
}: ManualScanPageContentProps) {
  const router = useRouter();
  const pathname = usePathname();
  const queryClient = useQueryClient();
  const { subscribeRealtimeEvent } = useRealtimeActivity();
  const [symbol, setSymbol] = useState("SPY");
  const [strategy, setStrategy] = useState("auto");
  const [activeRun, setActiveRun] = useState<ActiveRunState | null>(null);
  const [selectedPipelineId, setSelectedPipelineId] = useState<string | null>(
    readInitialQueryValue(initialPipelineId, "pipelineId"),
  );
  const [selectedCycleId, setSelectedCycleId] = useState<string | null>(
    readInitialQueryValue(initialCycleId, "cycleId"),
  );

  const pipelinesQuery = useQuery({
    queryKey: ["pipelines", "manual-scan"],
    queryFn: () => getPipelines({ limit: 120 }),
  });

  const manualPipelines = useMemo(
    () =>
      (pipelinesQuery.data?.pipelines ?? []).filter((pipeline) =>
        readString(pipeline.label, "").startsWith(MANUAL_LABEL_PREFIX),
      ),
    [pipelinesQuery.data?.pipelines],
  );
  const effectiveSelectedPipelineId =
    selectedPipelineId ?? manualPipelines[0]?.pipeline_id ?? null;
  const effectiveSelectedCycleId = selectedCycleId ?? undefined;

  const selectedPipelineQuery = useQuery({
    queryKey: [
      "pipelines",
      effectiveSelectedPipelineId ?? "",
      effectiveSelectedCycleId ?? "",
      "manual-detail",
    ],
    queryFn: () =>
      getPipelineDetail(String(effectiveSelectedPipelineId), {
        cycleId: effectiveSelectedCycleId,
      }),
    enabled:
      effectiveSelectedPipelineId != null &&
      (effectiveSelectedCycleId != null ||
        activeRun == null ||
        activeRun.pipelineId !== effectiveSelectedPipelineId ||
        TERMINAL_JOB_STATUSES.has(activeRun.status)),
    retry: false,
  });

  const startRunMutation = useMutation({
    mutationFn: () =>
      startPipelineRun({
        symbol: symbol.trim().toUpperCase(),
        strategy_mode: strategy === "auto" ? "auto" : "manual",
        strategy_family: strategy === "auto" ? null : strategy,
      }),
    onSuccess: async (run) => {
      setActiveRun(buildInitialRunState(run));
      setSelectedPipelineId(run.pipeline_id);
      setSelectedCycleId(null);
      await queryClient.invalidateQueries({ queryKey: ["pipelines"] });
    },
  });

  const handleRealtimeEvent = useEffectEvent((event: GlobalRealtimeEvent) => {
    if (activeRun == null) {
      return;
    }
    if (event.topic !== "job.run.updated") {
      return;
    }
    const eventPayload = event.payload;
    if (readEventText(eventPayload.job_run_id) !== activeRun.jobRunId) {
      return;
    }
    const status = readString(eventPayload.status, activeRun.status);
    setActiveRun((current) =>
      current == null
        ? current
        : {
            ...current,
            status,
            message:
              readEventText(eventPayload.error_text) ??
              readEventText((eventPayload.result as Record<string, unknown> | null)?.message) ??
              current.message,
            errorText: readEventText(eventPayload.error_text),
            startedAt: readEventText(eventPayload.started_at),
            finishedAt: readEventText(eventPayload.finished_at),
          },
    );
    if (TERMINAL_JOB_STATUSES.has(status)) {
      void queryClient.invalidateQueries({ queryKey: ["pipelines"] });
      if (status === "succeeded") {
        setSelectedCycleId(null);
        void queryClient.invalidateQueries({ queryKey: ["pipelines", activeRun.pipelineId] });
      }
    }
  });

  useEffect(() => {
    if (activeRun == null || TERMINAL_JOB_STATUSES.has(activeRun.status)) {
      return;
    }
    return subscribeRealtimeEvent(handleRealtimeEvent);
  }, [activeRun, subscribeRealtimeEvent]);

  useEffect(() => {
    if (!pathname) {
      return;
    }
    const params = new URLSearchParams();
    if (effectiveSelectedPipelineId) {
      params.set("pipelineId", effectiveSelectedPipelineId);
    }
    if (effectiveSelectedPipelineId && selectedCycleId) {
      params.set("cycleId", selectedCycleId);
    }
    const query = params.toString();
    const nextHref = query ? `${pathname}?${query}` : pathname;
    const currentHref =
      typeof window === "undefined"
        ? pathname
        : `${window.location.pathname}${window.location.search}`;
    if (nextHref !== currentHref) {
      router.replace(nextHref, { scroll: false });
    }
  }, [effectiveSelectedPipelineId, pathname, router, selectedCycleId]);

  if (pipelinesQuery.isLoading) {
    return <LoadingState />;
  }

  const pipelineDetail = selectedPipelineQuery.data;
  const liveOpportunities = pipelineDetail?.opportunities ?? [];
  const analysisOnly = pipelineDetail?.analysis_only_opportunities ?? [];
  const currentCycleId = readString(pipelineDetail?.current_cycle?.cycle_id, "");
  const topCandidate = liveOpportunities[0] ?? null;
  const alternatives = topCandidate == null ? liveOpportunities : liveOpportunities.slice(1);

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
                Manual Scan
              </Badge>
              {activeRun ? <RuntimeStatusBadge value={activeRun.status} /> : null}
              {pipelineDetail ? (
                <TradeabilityBadge value={pipelineDetail.tradeability_state} />
              ) : null}
              {pipelineDetail ? (
                <CaptureStatusBadge value={pipelineDetail.quote_capture.capture_status} />
              ) : null}
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">
              Frontend scanner
            </div>
            <div className="mt-2 text-sm text-foreground/70">
              Start an ad hoc discovery-run for one symbol, follow the job over the
              existing websocket stream, then inspect the resulting pipeline output.
            </div>
          </div>
          <Button
            type="button"
            variant="outline"
            onClick={() => {
              void pipelinesQuery.refetch();
              void selectedPipelineQuery.refetch();
            }}
          >
            <RefreshCw data-icon="inline-start" />
            Refresh
          </Button>
        </div>
      </div>

      <div className="grid gap-4 xl:grid-cols-[minmax(0,1.1fr)_minmax(0,0.9fr)]">
        <SectionSurface
          title="Run Scan"
          description="Use Auto to fan out across the supported families, or pin one strategy family directly."
        >
          <div className="grid gap-4 md:grid-cols-[minmax(0,1fr)_minmax(220px,280px)_auto]">
            <div className="flex flex-col gap-2">
              <div className="text-[11px] uppercase tracking-[0.2em] text-muted-foreground">
                Symbol
              </div>
              <Input
                value={symbol}
                onChange={(event) => setSymbol(event.target.value.toUpperCase())}
                placeholder="SPY"
              />
            </div>
            <div className="flex flex-col gap-2">
              <div className="text-[11px] uppercase tracking-[0.2em] text-muted-foreground">
                Strategy
              </div>
              <Select value={strategy} onValueChange={setStrategy}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {STRATEGY_OPTIONS.map((option) => (
                    <SelectItem key={option.value} value={option.value}>
                      {option.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="flex items-end">
              <Button
                type="button"
                onClick={() => startRunMutation.mutate()}
                disabled={startRunMutation.isPending || trim(symbol) === ""}
              >
                {startRunMutation.isPending ? (
                  <LoaderCircle data-icon="inline-start" className="animate-spin" />
                ) : (
                  <Search data-icon="inline-start" />
                )}
                Run Scan
              </Button>
            </div>
          </div>
          {startRunMutation.isError ? (
            <div className="mt-4 rounded-2xl border border-rose-300/70 bg-rose-100/80 px-4 py-3 text-sm text-rose-950 dark:border-rose-900/80 dark:bg-rose-950/40 dark:text-rose-100">
              {startRunMutation.error.message}
            </div>
          ) : null}
        </SectionSurface>

        <SectionSurface
          title="Active Run"
          description="The page watches the queued discovery-run over the existing websocket stream."
        >
          {activeRun == null ? (
            <div className="text-sm text-muted-foreground">
              No active manual scan yet.
            </div>
          ) : (
            <div className="flex flex-col gap-4">
              <div className="flex flex-wrap items-center gap-2">
                <RuntimeStatusBadge value={activeRun.status} />
                <Badge variant="outline">{activeRun.label}</Badge>
              </div>
              <div className="grid gap-3 md:grid-cols-3">
                <MetricTile
                  label="Job Run"
                  value={activeRun.jobRunId}
                  note={activeRun.message ?? "Waiting for updates"}
                />
                <MetricTile
                  label="Queued"
                  value={formatTimestamp(activeRun.scheduledFor)}
                />
                <MetricTile
                  label="Finished"
                  value={formatTimestamp(activeRun.finishedAt)}
                  note={readString(activeRun.errorText, "No terminal error")}
                />
              </div>
            </div>
          )}
        </SectionSurface>
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        <MetricTile
          label="Manual Pipelines"
          value={String(manualPipelines.length)}
          note="Recent ad hoc discovery sessions"
        />
        <MetricTile
          label="Live Opportunities"
          value={String(liveOpportunities.length)}
          note="Current selected pipeline"
        />
        <MetricTile
          label="Analysis Only"
          value={String(analysisOnly.length)}
          note="Blocked or non-live candidates"
        />
      </div>

      <SectionSurface
        title="Recent Pipelines"
        description="Manual scans reuse the existing discovery diagnostics surface, grouped by symbol and strategy."
      >
        {!manualPipelines.length ? (
          <div className="text-sm text-muted-foreground">
            No manual scan pipelines have completed yet.
          </div>
        ) : (
          <div className="flex flex-col gap-3">
            {manualPipelines.slice(0, 6).map((pipeline) => (
              <div
                key={pipeline.pipeline_id}
                className="flex flex-col gap-3 rounded-2xl border border-border/70 bg-background/70 px-4 py-4 md:flex-row md:items-center md:justify-between"
              >
                <div className="min-w-0">
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant="outline">{pipeline.label}</Badge>
                    <RuntimeStatusBadge value={pipeline.status} />
                  </div>
                  <div className="mt-2 text-sm text-muted-foreground">
                    Market date {formatDate(pipeline.latest_market_date)}. Promotable{" "}
                    {pipeline.promotable_count}. Monitor {pipeline.monitor_count}.
                  </div>
                </div>
                <div className="flex flex-wrap gap-2">
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => {
                      setSelectedPipelineId(pipeline.pipeline_id);
                      setSelectedCycleId(null);
                    }}
                  >
                    Select
                  </Button>
                  <Link
                    href={buildPipelineHref(
                      pipeline.pipeline_id,
                      pipeline.latest_market_date,
                    )}
                    className={buttonVariants({ variant: "outline", size: "default" })}
                  >
                    Open Diagnostics
                  </Link>
                </div>
              </div>
            ))}
          </div>
        )}
      </SectionSurface>

      <SectionSurface
        title="Run History"
        description="Past manual runs for the selected symbol and strategy live as discovery cycles on the same pipeline."
      >
        {effectiveSelectedPipelineId == null ? (
          <div className="text-sm text-muted-foreground">
            Select a manual pipeline to inspect its recent runs.
          </div>
        ) : selectedPipelineQuery.isLoading ? (
          <div className="text-sm text-muted-foreground">
            Loading run history for {effectiveSelectedPipelineId}.
          </div>
        ) : selectedPipelineQuery.isError || pipelineDetail == null ? (
          <div className="text-sm text-muted-foreground">
            Run history is not available yet.
          </div>
        ) : !(pipelineDetail.cycles?.length ?? 0) ? (
          <div className="text-sm text-muted-foreground">
            No past runs have been recorded for this manual pipeline yet.
          </div>
        ) : (
          <div className="flex flex-col gap-3">
            {pipelineDetail.cycles.slice(0, 8).map((rawCycle, index) => {
              const cycle = rawCycle as Record<string, unknown>;
              const cycleId = readString(
                cycle.cycle_id,
                `${pipelineDetail.pipeline_id}:${index}`,
              );
              const cycleMarketDate = readString(
                cycle.market_date,
                pipelineDetail.market_date,
              );
              const isSelected = cycleId !== "" && cycleId === currentCycleId;
              return (
                <div
                  key={cycleId}
                  className="flex flex-col gap-3 rounded-2xl border border-border/70 bg-background/70 px-4 py-4 md:flex-row md:items-center md:justify-between"
                >
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <Badge variant="outline">
                        {formatTimestamp(cycleGeneratedAt(cycle))}
                      </Badge>
                      <Badge variant="outline">
                        {humanizeStrategy(readString(cycle.strategy_mode, "unknown"))}
                      </Badge>
                      <Badge variant="outline">
                        {readString(cycle.legacy_profile, "unknown")}
                      </Badge>
                    </div>
                    <div className="mt-2 text-sm text-muted-foreground">
                      Candidates {cycleSummaryCount(cycle, "candidate_count")}. Promotable{" "}
                      {cycleSummaryCount(cycle, "promotable_count")}. Monitor{" "}
                      {cycleSummaryCount(cycle, "monitor_count")}. Failures{" "}
                      {cycleSummaryCount(cycle, "failure_count")}.
                    </div>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <Badge variant="outline">
                      {cycleId}
                    </Badge>
                    <Button
                      type="button"
                      variant={isSelected ? "default" : "outline"}
                      onClick={() => setSelectedCycleId(cycleId)}
                    >
                      Inspect Run
                    </Button>
                    <Link
                      href={buildPipelineHref(
                        pipelineDetail.pipeline_id,
                        cycleMarketDate,
                        cycleId,
                      )}
                      className={buttonVariants({ variant: "outline", size: "default" })}
                    >
                      Open Diagnostics
                    </Link>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </SectionSurface>

      <SectionSurface
        title="Results"
        description="Completed pipeline output, split into live-eligible and analysis-only opportunities."
      >
        {effectiveSelectedPipelineId == null ? (
          <div className="text-sm text-muted-foreground">
            Start a scan or select a recent manual pipeline.
          </div>
        ) : selectedPipelineQuery.isLoading ? (
          <div className="text-sm text-muted-foreground">
            Loading pipeline detail for {effectiveSelectedPipelineId}.
          </div>
        ) : selectedPipelineQuery.isError ? (
          <div className="rounded-2xl border border-amber-300/70 bg-amber-100/80 px-4 py-3 text-sm text-amber-950 dark:border-amber-900/80 dark:bg-amber-950/40 dark:text-amber-100">
            Pipeline detail is not available yet.
          </div>
        ) : pipelineDetail == null ? (
          <div className="text-sm text-muted-foreground">
            No pipeline detail was returned.
          </div>
        ) : (
          <div className="flex flex-col gap-4">
            <div className="flex flex-wrap items-center gap-2">
              <Badge variant="outline">{pipelineDetail.label}</Badge>
              {currentCycleId ? <Badge variant="outline">{currentCycleId}</Badge> : null}
              <RuntimeStatusBadge value={pipelineDetail.status} />
              <TradeabilityBadge value={pipelineDetail.tradeability_state} />
              <CaptureStatusBadge value={pipelineDetail.quote_capture.capture_status} />
              {selectedCycleId != null ? (
                <Button
                  type="button"
                  variant="outline"
                  onClick={() => setSelectedCycleId(null)}
                >
                  View Latest
                </Button>
              ) : null}
            </div>

            {topCandidate == null ? (
              <div className="rounded-2xl border border-border/70 bg-background/70 px-4 py-4 text-sm text-muted-foreground">
                No live-eligible opportunity is available right now.
                {pipelineDetail.tradeability_message ? (
                  <div className="mt-2">{pipelineDetail.tradeability_message}</div>
                ) : null}
              </div>
            ) : (
              <div className="flex flex-col gap-3">
                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.2em] text-muted-foreground">
                  <CandlestickChart className="size-4" />
                  Top Ranked Right Now
                </div>
                <OpportunityCard candidate={topCandidate} rank={1} />
              </div>
            )}

            {alternatives.length ? (
              <div className="flex flex-col gap-3">
                <div className="text-[11px] uppercase tracking-[0.2em] text-muted-foreground">
                  Alternatives
                </div>
                <div className="grid gap-3">
                  {alternatives.map((candidate, index) => (
                    <OpportunityCard
                      key={candidate.opportunity_id}
                      candidate={candidate}
                      rank={index + 2}
                    />
                  ))}
                </div>
              </div>
            ) : null}

            {analysisOnly.length ? (
              <div className="flex flex-col gap-3">
                <div className="text-[11px] uppercase tracking-[0.2em] text-muted-foreground">
                  Analysis Only
                </div>
                <div className="grid gap-3">
                  {analysisOnly.map((candidate, index) => (
                    <OpportunityCard
                      key={`${candidate.opportunity_id}:analysis`}
                      candidate={candidate}
                      rank={index + 1}
                    />
                  ))}
                </div>
              </div>
            ) : null}

            <div className="flex justify-end">
              <Link
                href={buildPipelineHref(
                  pipelineDetail.pipeline_id,
                  pipelineDetail.market_date,
                  currentCycleId || null,
                )}
                className={buttonVariants({ variant: "outline" })}
              >
                Open Full Diagnostics
              </Link>
            </div>
          </div>
        )}
      </SectionSurface>
    </div>
  );
}
