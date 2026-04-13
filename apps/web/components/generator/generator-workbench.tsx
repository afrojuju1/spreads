"use client";

import Link from "next/link";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ColumnDef } from "@tanstack/react-table";
import { LoaderCircle, Sparkles } from "lucide-react";
import { uniq } from "lodash-es";
import { startTransition, useDeferredValue, useMemo, useState } from "react";

import { DataTable } from "@/components/data-table";
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
import { Separator } from "@/components/ui/separator";
import { cn } from "@/lib/utils";
import {
  CandidateDetail,
  createGeneratorCandidateAction,
  GeneratorResponse,
  GeneratorJob,
  createGeneratorJob,
  getJobs,
  getGeneratorJobs,
  getGeneratorJob,
  getGeneratorSymbols,
} from "@/lib/api";
import { formatLocalDateTime } from "@/lib/date";
import {
  DEFAULT_GENERATOR_REQUEST,
  type GeneratorJobRequestPayload,
  normalizeGeneratorJobRequestRecord,
} from "@/lib/generator-request";

type CandidateRow = {
  id: string;
  strategy: string;
  strikes: string;
  expirationDate: string;
  score: number;
  credit: number;
  setupStatus: string;
  calendarStatus: string;
  raw: CandidateDetail;
};

export const CANDIDATE_COLUMNS: ColumnDef<CandidateRow>[] = [
  {
    accessorKey: "strategy",
    header: "Side",
    cell: ({ getValue }) => <StrategyBadge strategy={String(getValue())} />,
  },
  {
    accessorKey: "strikes",
    header: "Strikes",
    cell: ({ getValue }) => <span className="mono">{String(getValue())}</span>,
  },
  {
    accessorKey: "expirationDate",
    header: "Expiry",
    cell: ({ getValue }) => <span className="mono text-muted-foreground">{String(getValue())}</span>,
  },
  {
    accessorKey: "score",
    header: "Score",
    cell: ({ getValue }) => <span className="mono">{Number(getValue()).toFixed(1)}</span>,
  },
  {
    accessorKey: "credit",
    header: "Credit",
    cell: ({ getValue }) => <span className="mono">${Number(getValue()).toFixed(2)}</span>,
  },
  {
    accessorKey: "setupStatus",
    header: "Setup",
    cell: ({ getValue }) => <StatusBadge value={String(getValue())} tone="setup" />,
  },
  {
    accessorKey: "calendarStatus",
    header: "Calendar",
    cell: ({ getValue }) => <StatusBadge value={String(getValue())} tone="calendar" />,
  },
];

export function GeneratorWorkbench({
  initialRequest = DEFAULT_GENERATOR_REQUEST,
}: {
  initialRequest?: GeneratorJobRequestPayload;
}) {
  const [symbol, setSymbol] = useState(initialRequest.symbol);
  const [profile, setProfile] = useState(initialRequest.profile);
  const [strategy, setStrategy] = useState(initialRequest.strategy);
  const [greeksSource, setGreeksSource] = useState(initialRequest.greeks_source);
  const [top, setTop] = useState(String(initialRequest.top));
  const [minCredit, setMinCredit] = useState(
    initialRequest.min_credit === undefined ? "" : String(initialRequest.min_credit),
  );
  const [shortDeltaMax, setShortDeltaMax] = useState(
    initialRequest.short_delta_max === undefined ? "" : String(initialRequest.short_delta_max),
  );
  const [shortDeltaTarget, setShortDeltaTarget] = useState(
    initialRequest.short_delta_target === undefined ? "" : String(initialRequest.short_delta_target),
  );
  const [allowOffHours, setAllowOffHours] = useState(initialRequest.allow_off_hours);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [historySymbol, setHistorySymbol] = useState("");
  const [historyStatus, setHistoryStatus] = useState("all");
  const [historyLimit, setHistoryLimit] = useState("8");
  const deferredSymbol = useDeferredValue(symbol.trim().toUpperCase());
  const deferredHistorySymbol = useDeferredValue(historySymbol.trim().toUpperCase());
  const queryClient = useQueryClient();

  const symbolSuggestionsQuery = useQuery({
    queryKey: ["generator-symbols", deferredSymbol],
    queryFn: () => getGeneratorSymbols(deferredSymbol, 36),
    staleTime: 15 * 60_000,
  });

  const recentJobsQuery = useQuery({
    queryKey: ["generator-jobs", deferredHistorySymbol, historyStatus, historyLimit],
    queryFn: () =>
      getGeneratorJobs({
        symbol: deferredHistorySymbol || undefined,
        status: historyStatus === "all" ? undefined : historyStatus,
        limit: Number(historyLimit) || 8,
      }),
    staleTime: 15_000,
  });

  const activeJobQuery = useQuery({
    queryKey: ["generator-job", activeJobId],
    queryFn: () => getGeneratorJob(activeJobId as string),
    enabled: Boolean(activeJobId),
    staleTime: 0,
  });

  const mutation = useMutation({
    mutationFn: createGeneratorJob,
    onSuccess: (job) => {
      queryClient.setQueryData(["generator-job", job.generator_job_id], job);
      setActiveJobId(job.generator_job_id);
      queryClient.invalidateQueries({ queryKey: ["generator-jobs"] });
    },
  });

  const activeJob = activeJobQuery.data ?? null;
  const result = activeJob?.result;
  const isRunning =
    mutation.isPending || activeJob?.status === "queued" || activeJob?.status === "running";
  const errorMessage =
    mutation.error instanceof Error
      ? mutation.error.message
      : activeJob?.status === "failed"
        ? (activeJob.error_text ?? "Generator job failed.")
        : null;

  const candidateRows = useMemo(() => buildCandidateRows(result), [result]);
  const symbolMatches = useMemo(() => {
    const remoteMatches = symbolSuggestionsQuery.data?.symbols;
    if (remoteMatches?.length) {
      return remoteMatches;
    }
    return [];
  }, [symbolSuggestionsQuery.data?.symbols]);
  const resolvedSelectedId =
    selectedId && candidateRows.some((row) => row.id === selectedId)
      ? selectedId
      : (candidateRows[0]?.id ?? null);
  const selectedCandidate =
    candidateRows.find((row) => row.id === resolvedSelectedId)?.raw ??
    (candidateRows[0]?.raw ?? null);

  const applyRequestToForm = (request: GeneratorJobRequestPayload) => {
    startTransition(() => {
      setSymbol(request.symbol);
      setProfile(request.profile);
      setStrategy(request.strategy);
      setGreeksSource(request.greeks_source);
      setTop(String(request.top));
      setMinCredit(request.min_credit === undefined ? "" : String(request.min_credit));
      setShortDeltaMax(
        request.short_delta_max === undefined ? "" : String(request.short_delta_max),
      );
      setShortDeltaTarget(
        request.short_delta_target === undefined ? "" : String(request.short_delta_target),
      );
      setAllowOffHours(request.allow_off_hours);
      setSelectedId(null);
      setActiveJobId(null);
    });
  };

  const submitRequest = (request: GeneratorJobRequestPayload) => {
    setActiveJobId(null);
    setSelectedId(null);
    mutation.mutate(request);
  };

  const handleSubmit = () => {
    submitRequest({
      symbol: symbol.trim().toUpperCase(),
      profile,
      strategy,
      greeks_source: greeksSource,
      top: Number(top) || 5,
      min_credit: minCredit === "" ? undefined : Number(minCredit),
      short_delta_max: shortDeltaMax === "" ? undefined : Number(shortDeltaMax),
      short_delta_target: shortDeltaTarget === "" ? undefined : Number(shortDeltaTarget),
      allow_off_hours: allowOffHours,
    });
  };

  return (
    <main className="min-h-dvh">
      <div className="mx-auto flex max-w-[1680px] flex-col gap-4 px-4 py-4 lg:px-6">
        <header className="panel grid gap-4 px-4 py-4 lg:grid-cols-[minmax(0,1fr)_auto] lg:px-6">
          <div>
            <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.26em] text-muted-foreground">
              <Sparkles className="size-3.5" />
              Generator
            </div>
            <h1 className="mt-2 max-w-4xl text-[clamp(2rem,5vw,3.2rem)] leading-[0.96] font-semibold tracking-tight">
              Generate ideal single-symbol plays, or show exactly why not.
            </h1>
            <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
              This uses the live scanner directly, with the same profile and ranking logic as the rest of the backend.
            </p>
          </div>
        </header>

        <div className="grid gap-4 xl:grid-cols-[minmax(340px,0.8fr)_minmax(0,1.2fr)_360px]">
          <div className="flex min-w-0 flex-col gap-4">
            <aside className="panel">
              <div className="panel-header">
                <div className="min-w-0">
                  <div className="min-w-0 text-sm font-medium">Request</div>
                  <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                    Symbol, profile, and scan overrides
                  </div>
                </div>
              </div>
              <div className="panel-body space-y-4">
              <Field label="Symbol">
                <div className="space-y-3">
                  <Input
                    value={symbol}
                    onChange={(event) => setSymbol(event.target.value.toUpperCase())}
                    placeholder="Search or enter a symbol"
                    list="generator-symbol-suggestions"
                    className="rounded-2xl bg-background/70"
                  />
                  <datalist id="generator-symbol-suggestions">
                    {symbolMatches.map((item) => (
                      <option key={item.symbol} value={item.symbol}>
                        {formatSymbolOption(item.symbol, item.name, item.in_curated_universe)}
                      </option>
                    ))}
                  </datalist>
                  <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                    <Badge variant="outline" className="rounded-full px-2 py-0.5 text-[10px] uppercase tracking-[0.16em]">
                      {symbolSuggestionsQuery.data?.source_status === "fallback" ? "curated fallback" : "alpaca live"}
                    </Badge>
                    <span>Type a symbol and use the native suggestion list if it helps.</span>
                  </div>
                </div>
              </Field>

              <div className="grid gap-4 sm:grid-cols-2">
                <Field label="Profile">
                  <Select value={profile} onValueChange={(value) => setProfile(value ?? "weekly")}>
                    <SelectTrigger className="rounded-2xl bg-background/70">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="0dte">0DTE</SelectItem>
                      <SelectItem value="weekly">Weekly</SelectItem>
                      <SelectItem value="core">Core</SelectItem>
                      <SelectItem value="micro">Micro</SelectItem>
                      <SelectItem value="swing">Swing</SelectItem>
                    </SelectContent>
                  </Select>
                </Field>
                <Field label="Strategy">
                  <Select value={strategy} onValueChange={(value) => setStrategy(value ?? "combined")}>
                    <SelectTrigger className="rounded-2xl bg-background/70">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="combined">Combined</SelectItem>
                      <SelectItem value="put_credit">Put credit</SelectItem>
                      <SelectItem value="call_credit">Call credit</SelectItem>
                    </SelectContent>
                  </Select>
                </Field>
              </div>

              <div className="grid gap-4 sm:grid-cols-2">
                <Field label="Greeks source">
                  <Select value={greeksSource} onValueChange={(value) => setGreeksSource(value ?? "auto")}>
                    <SelectTrigger className="rounded-2xl bg-background/70">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="auto">Auto</SelectItem>
                      <SelectItem value="alpaca">Alpaca</SelectItem>
                      <SelectItem value="local">Local</SelectItem>
                    </SelectContent>
                  </Select>
                </Field>
                <Field label="Top plays">
                  <Input
                    value={top}
                    onChange={(event) => setTop(event.target.value)}
                    type="number"
                    min={1}
                    max={25}
                    className="rounded-2xl bg-background/70"
                  />
                </Field>
              </div>

              <Separator />

              <div className="space-y-3">
                <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                  Optional overrides
                </div>
                <div className="grid gap-4 sm:grid-cols-3">
                  <Field label="Min credit">
                    <Input
                      value={minCredit}
                      onChange={(event) => setMinCredit(event.target.value)}
                      type="number"
                      step="0.01"
                      placeholder="default"
                      className="rounded-2xl bg-background/70"
                    />
                  </Field>
                  <Field label="Delta max">
                    <Input
                      value={shortDeltaMax}
                      onChange={(event) => setShortDeltaMax(event.target.value)}
                      type="number"
                      step="0.01"
                      placeholder="default"
                      className="rounded-2xl bg-background/70"
                    />
                  </Field>
                  <Field label="Delta target">
                    <Input
                      value={shortDeltaTarget}
                      onChange={(event) => setShortDeltaTarget(event.target.value)}
                      type="number"
                      step="0.01"
                      placeholder="default"
                      className="rounded-2xl bg-background/70"
                    />
                  </Field>
                </div>
              </div>
              <Separator />
              <Button
                onClick={handleSubmit}
                size="lg"
                className={cn(
                  "w-full cursor-pointer rounded-2xl border border-primary/20 bg-primary px-4 text-primary-foreground",
                  "shadow-[0_18px_40px_-22px_color-mix(in_oklab,var(--primary)_75%,black)]",
                  "hover:-translate-y-0.5 hover:brightness-[1.06] hover:shadow-[0_24px_48px_-20px_color-mix(in_oklab,var(--primary)_78%,black)]",
                  "active:translate-y-0 active:brightness-100",
                )}
                disabled={mutation.isPending || symbol.trim().length === 0}
              >
                {mutation.isPending ? (
                  <LoaderCircle className="size-4 animate-spin" />
                ) : (
                  <Sparkles className="size-4" />
                )}
                Generate
              </Button>
              </div>
            </aside>

            <section className="panel">
              <div className="panel-header">
                <div className="min-w-0">
                  <div className="min-w-0 text-sm font-medium">Recent jobs</div>
                  <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                    Persisted generator runs and recent outcomes
                  </div>
                </div>
              </div>
              <div className="panel-body space-y-2">
                <div className="grid gap-3 sm:grid-cols-[minmax(0,1fr)_180px_120px]">
                  <Field label="Symbol filter">
                    <Input
                      value={historySymbol}
                      onChange={(event) => setHistorySymbol(event.target.value.toUpperCase())}
                      placeholder="All symbols"
                      className="rounded-2xl bg-background/70"
                    />
                  </Field>
                  <Field label="Status">
                    <Select value={historyStatus} onValueChange={(value) => setHistoryStatus(value ?? "all")}>
                      <SelectTrigger className="rounded-2xl bg-background/70">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">All statuses</SelectItem>
                        <SelectItem value="queued">Queued</SelectItem>
                        <SelectItem value="running">Running</SelectItem>
                        <SelectItem value="succeeded">Succeeded</SelectItem>
                        <SelectItem value="no_play">No play</SelectItem>
                        <SelectItem value="failed">Failed</SelectItem>
                      </SelectContent>
                    </Select>
                  </Field>
                  <Field label="Limit">
                    <Select value={historyLimit} onValueChange={(value) => setHistoryLimit(value ?? "8")}>
                      <SelectTrigger className="rounded-2xl bg-background/70">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="8">8 jobs</SelectItem>
                        <SelectItem value="16">16 jobs</SelectItem>
                        <SelectItem value="32">32 jobs</SelectItem>
                        <SelectItem value="64">64 jobs</SelectItem>
                      </SelectContent>
                    </Select>
                  </Field>
                </div>
                <Separator />
                {recentJobsQuery.isLoading ? (
                  <LoadingState body="Loading recent generator jobs..." />
                ) : !(recentJobsQuery.data?.jobs.length) ? (
                  <EmptyState
                    title="No matching generator jobs"
                    body="Adjust the filters or submit a generator request to populate this history."
                  />
                ) : (
                  recentJobsQuery.data.jobs.map((job) => (
                    <div
                      key={job.generator_job_id}
                      className={cn(
                        "rounded-2xl border border-border/70 bg-background/70 px-3 py-3 transition-colors hover:bg-accent/40",
                        activeJobId === job.generator_job_id && "border-stone-900/20 bg-accent/40",
                      )}
                    >
                      <div className="flex items-center justify-between gap-3">
                        <div className="min-w-0">
                          <div className="flex items-center gap-2">
                            <span className="font-medium tracking-[0.08em]">{job.symbol}</span>
                            <StatusBadge value={job.status} tone="job" />
                          </div>
                          <div className="mt-1 text-xs text-muted-foreground">
                            {String(job.request.profile ?? "—")} · {String(job.request.strategy ?? "—")} · {String(job.request.greeks_source ?? "—")}
                          </div>
                        </div>
                        <div className="text-right text-xs text-muted-foreground">
                          <div>{formatDateTime(job.created_at)}</div>
                          <div>{job.summary.candidate_count} candidates</div>
                        </div>
                      </div>
                      <div className="mt-2 flex flex-wrap gap-2 text-xs text-foreground/75">
                        {job.summary.preferred_strategy ? (
                          <span>
                            {job.summary.preferred_strategy} {job.summary.preferred_strikes}
                          </span>
                        ) : (
                          <span>{job.summary.rejection_count} blockers</span>
                        )}
                        {job.summary.top_score != null ? <span>score {job.summary.top_score.toFixed(1)}</span> : null}
                      </div>
                      <div className="mt-3 flex flex-wrap gap-2">
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          onClick={() => applyRequestToForm(normalizeGeneratorJobRequestRecord(job.request))}
                        >
                          Load into form
                        </Button>
                        <Button
                          type="button"
                          variant="secondary"
                          size="sm"
                          disabled={mutation.isPending}
                          onClick={() => {
                            const request = normalizeGeneratorJobRequestRecord(job.request);
                            applyRequestToForm(request);
                            submitRequest(request);
                          }}
                        >
                          {mutation.isPending ? <LoaderCircle className="size-3.5 animate-spin" /> : null}
                          Run again
                        </Button>
                        <Link
                          href={`/generator/jobs/${job.generator_job_id}`}
                          className={buttonVariants({ variant: "ghost", size: "sm" })}
                        >
                          Details
                        </Link>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </section>
          </div>

          <section className="flex min-w-0 flex-col gap-4">
            <div className="panel">
              <div className="panel-header">
                <div className="min-w-0">
                  <div className="min-w-0 text-sm font-medium">Generated plays</div>
                  <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                    Best-ranked candidates for the current request
                  </div>
                </div>
                {result ? (
                  <Badge
                    variant="outline"
                    className={cn(
                      "rounded-full px-2.5 py-1",
                      result.status === "ok"
                        ? "border-emerald-200 bg-emerald-100 text-emerald-900"
                        : "border-amber-200 bg-amber-100 text-amber-900",
                    )}
                  >
                    {result.status === "ok" ? "play available" : "no play"}
                  </Badge>
                ) : null}
                {!result && activeJob ? (
                  <Badge variant="outline" className="rounded-full px-2.5 py-1 uppercase tracking-[0.16em]">
                    {activeJob.status}
                  </Badge>
                ) : null}
              </div>
              <div className="panel-body">
                {!activeJobId && mutation.isIdle ? (
                  <EmptyState
                    title="Ready to generate"
                    body="Choose a symbol and profile, then run the generator to see ideal spreads or a structured no-play diagnosis."
                  />
                ) : isRunning || activeJobQuery.isLoading ? (
                  <LoadingState />
                ) : errorMessage || activeJobQuery.isError ? (
                  <EmptyState
                    title="Generator failed"
                    body={
                      errorMessage ??
                      (activeJobQuery.error instanceof Error ? activeJobQuery.error.message : "Unknown generator error.")
                    }
                    tone="error"
                  />
                ) : (
                  <DataTable
                    columns={CANDIDATE_COLUMNS}
                    data={candidateRows}
                    emptyMessage="No spreads qualified for this request."
                    getRowId={(row) => row.id}
                    selectedId={resolvedSelectedId}
                    onSelect={(row) => setSelectedId(row.id)}
                  />
                )}
              </div>
            </div>

            <div className="panel">
              <div className="panel-header">
                <div className="min-w-0">
                  <div className="min-w-0 text-sm font-medium">Strategy breakdown</div>
                  <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                    Coverage and fallback reasons by strategy
                  </div>
                </div>
              </div>
              <div className="panel-body space-y-3">
                {!result ? (
                  <EmptyState
                    title="No run yet"
                    body="Once the generator runs, this panel will show per-strategy coverage, Greeks availability, and top-candidate status."
                  />
                ) : (
                  result.strategy_runs.map((run) => (
                    <div key={run.strategy} className="rounded-2xl border border-border/70 bg-background/70 px-4 py-3">
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <div className="flex items-center gap-2">
                          <StrategyBadge strategy={run.strategy} />
                          <span className="mono text-muted-foreground">{run.run_id}</span>
                        </div>
                        <div className="flex flex-wrap gap-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                          <span>quoted {run.quoted_contract_count}</span>
                          <span>alpaca delta {run.alpaca_delta_contract_count}</span>
                          <span>usable delta {run.delta_contract_count}</span>
                          <span>local {run.local_delta_contract_count}</span>
                        </div>
                      </div>
                      <div className="mt-3 flex flex-wrap gap-2">
                        <StatusBadge
                          value={String(run.setup?.status ?? "unknown")}
                          tone="setup"
                        />
                        <Badge variant="outline" className="rounded-full border-stone-300 bg-stone-100/80 text-stone-700">
                          {run.candidate_count} candidates
                        </Badge>
                      </div>
                      {run.no_play_reasons.length ? (
                        <ul className="mt-3 space-y-2 text-sm text-foreground/80">
                          {run.no_play_reasons.map((reason, index) => (
                            <li key={`${run.strategy}-${reason.code}-${index}`}>
                              <span className="font-medium">{reason.code.replaceAll("_", " ")}:</span>{" "}
                              {reason.message}
                            </li>
                          ))}
                        </ul>
                      ) : (
                        <div className="mt-3 text-sm text-muted-foreground">
                          This side produced at least one viable candidate.
                        </div>
                      )}
                    </div>
                  ))
                )}
              </div>
            </div>
          </section>

          <aside className="panel">
            <div className="panel-header">
              <div className="min-w-0">
                <div className="min-w-0 text-sm font-medium">Inspector</div>
                <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                  Preferred play or no-play reasoning
                </div>
              </div>
            </div>
            <div className="panel-body space-y-4">
              {!result ? (
                <EmptyState
                  title="No result yet"
                  body="Run the generator to inspect the preferred play, filter payload, and any reasons a trade could not be recommended."
                />
              ) : result.status === "no_play" ? (
                <>
                  <div>
                    <div className="text-sm font-medium">No play surfaced</div>
                    <div className="mt-1 text-sm leading-6 text-muted-foreground">
                      The scanner did not find a spread that survived the requested profile and filter set.
                    </div>
                  </div>
                  <Separator />
                  <div className="space-y-2">
                    {result.rejection_summary.map((reason, index) => (
                      <div key={`${reason.code}-${index}`} className="rounded-2xl border border-border/70 bg-background/70 px-3 py-3">
                        <div className="flex items-center gap-2">
                          <StatusBadge value={String(reason.severity ?? "info")} tone="outcome" />
                          <span className="text-sm font-medium">{reason.code.replaceAll("_", " ")}</span>
                        </div>
                        <p className="mt-2 text-sm text-foreground/80">{reason.message}</p>
                      </div>
                    ))}
                  </div>
                </>
              ) : selectedCandidate ? (
                <>
                  <div className="space-y-2">
                    <div className="flex flex-wrap items-center gap-2">
                      <StrategyBadge strategy={selectedCandidate.strategy} />
                      <span className="mono">
                        {selectedCandidate.short_strike?.toFixed(2)} / {selectedCandidate.long_strike?.toFixed(2)}
                      </span>
                    </div>
                    <div className="grid gap-2 sm:grid-cols-2">
                      <MetricTile label="score" value={selectedCandidate.quality_score?.toFixed(1) ?? "—"} />
                      <MetricTile label="credit" value={`$${(selectedCandidate.midpoint_credit ?? 0).toFixed(2)}`} />
                      <MetricTile label="expected move" value={selectedCandidate.expected_move ? `$${selectedCandidate.expected_move.toFixed(2)}` : "—"} />
                      <MetricTile label="return / risk" value={selectedCandidate.return_on_risk ? `${(selectedCandidate.return_on_risk * 100).toFixed(1)}%` : "—"} />
                    </div>
                  </div>
                  <Separator />
                  <CandidateOperatorActions job={activeJob} selectedCandidate={selectedCandidate} />
                  <Separator />
                  <ReasonBlock title="Board notes" items={selectedCandidate.board_notes ?? []} />
                  <ReasonBlock title="Setup reasons" items={selectedCandidate.setup_reasons ?? []} />
                  <ReasonBlock title="Calendar reasons" items={selectedCandidate.calendar_reasons ?? []} />
                  <ReasonBlock title="Data quality notes" items={(selectedCandidate as { data_reasons?: string[] }).data_reasons ?? []} />
                </>
              ) : (
                <EmptyState
                  title="No play selected"
                  body="Select a generated candidate to inspect its reasoning."
                />
              )}
            </div>
          </aside>
        </div>
      </div>
    </main>
  );
}

export function CandidateOperatorActions({
  job,
  selectedCandidate,
}: {
  job: GeneratorJob | null | undefined;
  selectedCandidate: CandidateDetail | null;
}) {
  const queryClient = useQueryClient();
  const [selectedLiveLabel, setSelectedLiveLabel] = useState("");
  const [pendingAction, setPendingAction] = useState<string | null>(null);

  const jobsQuery = useQuery({
    queryKey: ["jobs"],
    queryFn: getJobs,
    staleTime: 60_000,
  });

  const liveLabels = useMemo(
    () =>
      uniq(
        (jobsQuery.data?.jobs ?? [])
          .filter((item) => item.job_type === "live_collector")
          .map((item) => item.singleton_scope ?? item.job_key)
          .filter((label): label is string => Boolean(label)),
      ),
    [jobsQuery.data?.jobs],
  );

  const resolvedLiveLabel =
    selectedLiveLabel && liveLabels.includes(selectedLiveLabel)
      ? selectedLiveLabel
      : (liveLabels[0] ?? "");

  const actionMutation = useMutation({
    mutationFn: (payload: {
      action: "create_alert" | "promote_live";
      live_label?: string;
      target_state?: "promotable" | "monitor";
      strategy: string;
      short_symbol: string;
      long_symbol: string;
    }) => createGeneratorCandidateAction(job?.generator_job_id ?? "", payload),
    onSuccess: (result) => {
      if (result.action === "create_alert") {
        queryClient.invalidateQueries({ queryKey: ["alerts-latest"] });
        return;
      }
      queryClient.invalidateQueries({ queryKey: ["live"] });
      queryClient.invalidateQueries({ queryKey: ["live-events"] });
    },
    onSettled: () => {
      setPendingAction(null);
    },
  });

  if (!job || !selectedCandidate) {
    return null;
  }

  const runAction = (
    action: "create_alert" | "promote_live",
    targetState?: "promotable" | "monitor",
  ) => {
    const actionKey = targetState ? `${action}:${targetState}` : action;
    setPendingAction(actionKey);
    actionMutation.mutate({
      action,
      target_state: targetState,
      live_label: resolvedLiveLabel || undefined,
      strategy: selectedCandidate.strategy,
      short_symbol: selectedCandidate.short_symbol,
      long_symbol: selectedCandidate.long_symbol,
    });
  };

  const noLiveWorkflow = !resolvedLiveLabel;

  return (
    <div className="space-y-3">
      <div>
        <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
          Operator actions
        </div>
        <p className="mt-2 text-sm leading-6 text-muted-foreground">
          Push this candidate into the current live workflow or emit a manual alert from the persisted job result.
        </p>
      </div>
      <Field label="Live workflow">
        <Select
          value={resolvedLiveLabel || undefined}
          onValueChange={(value) => setSelectedLiveLabel(value ?? "")}
          disabled={!liveLabels.length}
        >
          <SelectTrigger className="rounded-2xl bg-background/70">
            <SelectValue placeholder={jobsQuery.isLoading ? "Loading live workflows..." : "No live workflow available"} />
          </SelectTrigger>
          <SelectContent>
            {liveLabels.map((label) => (
              <SelectItem key={label} value={label}>
                {label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </Field>
      <div className="flex flex-wrap gap-2">
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={actionMutation.isPending || noLiveWorkflow}
          onClick={() => runAction("create_alert")}
        >
          {pendingAction === "create_alert" ? <LoaderCircle className="size-3.5 animate-spin" /> : null}
          Create alert
        </Button>
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={actionMutation.isPending || noLiveWorkflow}
          onClick={() => runAction("promote_live", "monitor")}
        >
          {pendingAction === "promote_live:monitor" ? <LoaderCircle className="size-3.5 animate-spin" /> : null}
          Mark monitor
        </Button>
        <Button
          type="button"
          variant="secondary"
          size="sm"
          disabled={actionMutation.isPending || noLiveWorkflow}
          onClick={() => runAction("promote_live", "promotable")}
        >
          {pendingAction === "promote_live:promotable" ? <LoaderCircle className="size-3.5 animate-spin" /> : null}
          Mark promotable
        </Button>
      </div>
      {noLiveWorkflow ? (
        <div className="text-sm text-muted-foreground">
          No live collector workflow is available yet. Run a live collector cycle first, then these actions will attach to its label.
        </div>
      ) : null}
      {jobsQuery.isError ? (
        <div className="text-sm text-rose-700">
          {jobsQuery.error instanceof Error ? jobsQuery.error.message : "Could not load live workflows."}
        </div>
      ) : null}
      {actionMutation.isError ? (
        <div className="text-sm text-rose-700">
          {actionMutation.error instanceof Error
            ? actionMutation.error.message
            : "Could not apply the selected operator action."}
        </div>
      ) : null}
      {actionMutation.data ? (
        <div
          className={cn(
            "rounded-2xl border px-3 py-3 text-sm",
            actionMutation.data.changed
              ? "border-emerald-200 bg-emerald-50 text-emerald-950"
              : "border-amber-200 bg-amber-50 text-amber-950",
          )}
        >
          {actionMutation.data.message}
        </div>
      ) : null}
    </div>
  );
}

export function buildCandidateRows(result: GeneratorResponse | null | undefined): CandidateRow[] {
  return (result?.top_candidates ?? []).map((candidate) => ({
    id: candidateRowId(candidate),
    strategy: candidate.strategy,
    strikes: `${candidate.short_strike.toFixed(2)} / ${candidate.long_strike.toFixed(2)}`,
    expirationDate: candidate.expiration_date ?? "—",
    score: candidate.quality_score,
    credit: candidate.midpoint_credit,
    setupStatus: candidate.setup_status ?? "unknown",
    calendarStatus: candidate.calendar_status ?? "unknown",
    raw: candidate,
  }));
}

export function candidateRowId(candidate: CandidateDetail) {
  return `${candidate.strategy}:${candidate.short_symbol}:${candidate.long_symbol}`;
}

function formatSymbolOption(symbol: string, name: string | null | undefined, inCuratedUniverse: boolean) {
  const suffix = inCuratedUniverse ? " · curated" : "";
  if (!name) {
    return `${symbol}${suffix}`;
  }
  return `${symbol} — ${name}${suffix}`;
}

function Field({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <label className="space-y-2">
      <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">{label}</div>
      {children}
    </label>
  );
}

export function MetricTile({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric-card min-w-0">
      <div className="text-[10px] uppercase tracking-[0.22em] text-muted-foreground">{label}</div>
      <div className="mt-2 truncate text-base font-medium">{value}</div>
    </div>
  );
}

export function EmptyState({
  title,
  body,
  tone = "default",
}: {
  title: string;
  body: string;
  tone?: "default" | "error";
}) {
  return (
    <div
      className={cn(
        "rounded-2xl border px-4 py-5",
        tone === "error"
          ? "border-red-200 bg-red-50 text-red-900"
          : "border-border/70 bg-background/70 text-foreground",
      )}
    >
      <div className="text-sm font-medium">{title}</div>
      <p className="mt-2 text-sm leading-6 text-muted-foreground">{body}</p>
    </div>
  );
}

export function LoadingState({
  body = "Scanning live option chains and ranking candidates...",
}: {
  body?: string;
}) {
  return (
    <div className="flex items-center gap-3 rounded-2xl border border-border/70 bg-background/70 px-4 py-5 text-sm text-muted-foreground">
      <LoaderCircle className="size-4 animate-spin" />
      {body}
    </div>
  );
}

export function ReasonBlock({ title, items }: { title: string; items: string[] }) {
  if (!items.length) {
    return null;
  }
  return (
    <div>
      <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">{title}</div>
      <ul className="mt-2 space-y-2 text-sm text-foreground/80">
        {items.map((item) => (
          <li key={`${title}-${item}`}>- {item}</li>
        ))}
      </ul>
    </div>
  );
}

export function StrategyBadge({ strategy }: { strategy: string }) {
  const isPut = strategy === "put_credit";
  return (
    <Badge
      variant="outline"
      className={cn(
        "rounded-full border-transparent",
        isPut ? "bg-emerald-100 text-emerald-900" : "bg-rose-100 text-rose-900",
      )}
    >
      {strategy}
    </Badge>
  );
}

export function StatusBadge({
  value,
  tone,
}: {
  value: string;
  tone: "setup" | "calendar" | "outcome" | "job";
}) {
  const normalized = value.toLowerCase();
  let className = "border-stone-300 bg-stone-100/80 text-stone-700";
  if (tone === "setup") {
    if (normalized.includes("favorable") || normalized.includes("supportive")) {
      className = "border-emerald-200 bg-emerald-100 text-emerald-900";
    } else if (normalized.includes("unfavorable") || normalized.includes("adverse")) {
      className = "border-rose-200 bg-rose-100 text-rose-900";
    } else {
      className = "border-amber-200 bg-amber-100 text-amber-900";
    }
  }
  if (tone === "calendar") {
    if (normalized.includes("clean")) {
      className = "border-emerald-200 bg-emerald-100 text-emerald-900";
    } else if (normalized.includes("penalized") || normalized.includes("blocked")) {
      className = "border-rose-200 bg-rose-100 text-rose-900";
    } else {
      className = "border-amber-200 bg-amber-100 text-amber-900";
    }
  }
  if (tone === "outcome") {
    if (normalized.includes("high") || normalized.includes("loss")) {
      className = "border-rose-200 bg-rose-100 text-rose-900";
    } else if (normalized.includes("medium") || normalized.includes("still")) {
      className = "border-amber-200 bg-amber-100 text-amber-900";
    } else {
      className = "border-stone-300 bg-stone-100/80 text-stone-700";
    }
  }
  if (tone === "job") {
    if (normalized.includes("succeeded")) {
      className = "border-emerald-200 bg-emerald-100 text-emerald-900";
    } else if (normalized.includes("running") || normalized.includes("queued")) {
      className = "border-amber-200 bg-amber-100 text-amber-900";
    } else if (normalized.includes("failed")) {
      className = "border-rose-200 bg-rose-100 text-rose-900";
    } else {
      className = "border-stone-300 bg-stone-100/80 text-stone-700";
    }
  }

  return (
    <Badge variant="outline" className={cn("rounded-full", className)}>
      {value}
    </Badge>
  );
}

function formatDateTime(value: string) {
  return formatLocalDateTime(value);
}
