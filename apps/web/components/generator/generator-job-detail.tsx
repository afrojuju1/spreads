"use client";

import Link from "next/link";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { LoaderCircle } from "lucide-react";
import { useMemo, useState } from "react";

import { DataTable } from "@/components/dashboard/data-table";
import {
  buildCandidateRows,
  CandidateOperatorActions,
  CANDIDATE_COLUMNS,
  EmptyState,
  LoadingState,
  MetricTile,
  ReasonBlock,
  StatusBadge,
  StrategyBadge,
} from "@/components/generator/generator-workbench";
import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { createGeneratorJob, getGeneratorJob } from "@/lib/api";
import {
  buildGeneratorFormHref,
  normalizeGeneratorJobRequestRecord,
} from "@/lib/generator-request";

export function GeneratorJobDetail({ generatorJobId }: { generatorJobId: string }) {
  const router = useRouter();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const jobQuery = useQuery({
    queryKey: ["generator-job", generatorJobId],
    queryFn: () => getGeneratorJob(generatorJobId),
    staleTime: 0,
  });
  const rerunMutation = useMutation({
    mutationFn: createGeneratorJob,
    onSuccess: (job) => {
      router.push(`/generator/jobs/${job.generator_job_id}`);
    },
  });

  const job = jobQuery.data;
  const result = job?.result;
  const normalizedRequest = job ? normalizeGeneratorJobRequestRecord(job.request) : null;
  const candidateRows = useMemo(() => buildCandidateRows(result), [result]);
  const resolvedSelectedId =
    selectedId && candidateRows.some((row) => row.id === selectedId)
      ? selectedId
      : (candidateRows[0]?.id ?? null);
  const selectedCandidate =
    candidateRows.find((row) => row.id === resolvedSelectedId)?.raw ?? candidateRows[0]?.raw ?? null;

  return (
    <main className="min-h-dvh">
      <div className="mx-auto flex max-w-[1680px] flex-col gap-4 px-4 py-4 lg:px-6">
        <header className="panel grid gap-4 px-4 py-4 lg:px-6">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div className="min-w-0">
              <div className="text-[11px] uppercase tracking-[0.26em] text-muted-foreground">
                Generator job detail
              </div>
              <h1 className="mt-2 text-[clamp(1.8rem,4vw,3rem)] leading-[0.96] font-semibold tracking-tight">
                {job?.symbol ?? "Generator"} history and persisted result
              </h1>
              <div className="mt-2 flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
                <Link href="/generator" className="text-foreground underline underline-offset-4">
                  Back to generator
                </Link>
                {job ? <span className="mono">{job.generator_job_id}</span> : null}
              </div>
              {normalizedRequest ? (
                <div className="mt-4 flex flex-wrap gap-2">
                  <Link
                    href={buildGeneratorFormHref(normalizedRequest)}
                    className={buttonVariants({ variant: "outline", size: "sm" })}
                  >
                    Load into form
                  </Link>
                  <Button
                    type="button"
                    variant="secondary"
                    size="sm"
                    disabled={rerunMutation.isPending}
                    onClick={() => rerunMutation.mutate(normalizedRequest)}
                  >
                    {rerunMutation.isPending ? <LoaderCircle className="size-3.5 animate-spin" /> : null}
                    Run again
                  </Button>
                </div>
              ) : null}
              {rerunMutation.isError ? (
                <div className="mt-3 text-sm text-rose-700">
                  {rerunMutation.error instanceof Error ? rerunMutation.error.message : "Could not rerun this generator job."}
                </div>
              ) : null}
            </div>
            {job ? <StatusBadge value={job.status} tone="job" /> : null}
          </div>
          {job ? (
            <div className="grid gap-3 md:grid-cols-3 xl:grid-cols-6">
              <MetricTile label="symbol" value={job.symbol} />
              <MetricTile label="created" value={formatDateTime(job.created_at)} />
              <MetricTile label="started" value={job.started_at ? formatDateTime(job.started_at) : "—"} />
              <MetricTile label="finished" value={job.finished_at ? formatDateTime(job.finished_at) : "—"} />
              <MetricTile label="profile" value={String(job.request.profile ?? "—")} />
              <MetricTile label="strategy" value={String(job.request.strategy ?? "—")} />
            </div>
          ) : null}
        </header>

        <div className="grid gap-4 xl:grid-cols-[minmax(340px,0.8fr)_minmax(0,1.2fr)_360px]">
          <aside className="panel">
            <div className="panel-header">
              <div className="min-w-0">
                <div className="min-w-0 text-sm font-medium">Request + diagnostics</div>
                <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                  Persisted inputs, verdict, and recommendations
                </div>
              </div>
            </div>
            <div className="panel-body space-y-4">
              {jobQuery.isLoading ? (
                <LoadingState body="Loading generator job detail..." />
              ) : jobQuery.isError ? (
                <EmptyState
                  title="Could not load job"
                  body={jobQuery.error instanceof Error ? jobQuery.error.message : "Unknown generator job error."}
                  tone="error"
                />
              ) : !job ? (
                <EmptyState
                  title="Job not found"
                  body="This generator job does not exist or is no longer available."
                  tone="error"
                />
              ) : (
                <>
                  <div className="grid gap-2 sm:grid-cols-2">
                    <MetricTile label="greeks" value={String(job.request.greeks_source ?? "—")} />
                    <MetricTile label="top plays" value={String(job.request.top ?? "—")} />
                  </div>
                  <Separator />
                  {result?.diagnostics ? (
                    <>
                      <div className="rounded-2xl border border-border/70 bg-background/70 px-3 py-3">
                        <div className="flex items-center gap-2">
                          <Badge variant="outline" className="rounded-full">
                            {result.diagnostics.overview.playability_verdict}
                          </Badge>
                          <span className="text-sm font-medium">Diagnostics overview</span>
                        </div>
                        <p className="mt-2 text-sm text-muted-foreground">
                          {result.diagnostics.overview.symbol} · {result.diagnostics.overview.profile} · {result.diagnostics.overview.strategy}
                        </p>
                      </div>
                      <div className="space-y-3">
                        {result.diagnostics.groups.map((group) => (
                          <div key={group.bucket} className="rounded-2xl border border-border/70 bg-background/70 px-3 py-3">
                            <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                              {group.bucket.replaceAll("_", " ")}
                            </div>
                            <ul className="mt-2 space-y-2 text-sm text-foreground/80">
                              {group.reasons.map((reason, index) => (
                                <li key={`${group.bucket}-${reason.code}-${index}`}>
                                  <span className="font-medium">{reason.code.replaceAll("_", " ")}:</span>{" "}
                                  {reason.message}
                                </li>
                              ))}
                            </ul>
                          </div>
                        ))}
                      </div>
                      <Separator />
                      <div className="space-y-2">
                        <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                          Recommendations
                        </div>
                        {result.recommendations.map((recommendation) => (
                          <div key={recommendation.code} className="rounded-2xl border border-border/70 bg-background/70 px-3 py-3">
                            <div className="text-sm font-medium">{recommendation.title}</div>
                            <div className="mt-1 text-sm text-foreground/80">{recommendation.action}</div>
                            <div className="mt-2 text-xs text-muted-foreground">{recommendation.reason}</div>
                          </div>
                        ))}
                      </div>
                    </>
                  ) : null}
                </>
              )}
            </div>
          </aside>

          <section className="flex min-w-0 flex-col gap-4">
            <div className="panel">
              <div className="panel-header">
                <div className="min-w-0">
                  <div className="min-w-0 text-sm font-medium">Ranked candidates</div>
                  <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                    Persisted generator output for this job
                  </div>
                </div>
              </div>
              <div className="panel-body">
                {jobQuery.isLoading ? (
                  <LoadingState body="Loading ranked candidates..." />
                ) : !result ? (
                  <EmptyState
                    title="Result not ready"
                    body="This job is still queued or running. The table will populate when the persisted result lands."
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
                  <div className="min-w-0 text-sm font-medium">Strategy comparison</div>
                  <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                    Coverage, blockers, and candidate counts by scanned side
                  </div>
                </div>
              </div>
              <div className="panel-body space-y-3">
                {!result ? (
                  <EmptyState
                    title="No completed result yet"
                    body="Strategy comparison appears after the generator finishes."
                  />
                ) : (
                  result.strategy_comparison.map((run) => (
                    <div key={run.strategy} className="rounded-2xl border border-border/70 bg-background/70 px-4 py-3">
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <div className="flex items-center gap-2">
                          <StrategyBadge strategy={run.strategy} />
                          <span className="mono text-muted-foreground">{run.run_id}</span>
                        </div>
                        <div className="flex flex-wrap gap-2 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                          <span>quoted {run.quoted_contract_count}</span>
                          <span>usable {run.delta_contract_count}</span>
                          <span>candidates {run.candidate_count}</span>
                        </div>
                      </div>
                      {run.blocker_summary.length ? (
                        <ul className="mt-3 space-y-2 text-sm text-foreground/80">
                          {run.blocker_summary.map((reason, index) => (
                            <li key={`${run.strategy}-${reason.code}-${index}`}>
                              <span className="font-medium">{reason.code.replaceAll("_", " ")}:</span>{" "}
                              {reason.message}
                            </li>
                          ))}
                        </ul>
                      ) : (
                        <div className="mt-3 text-sm text-muted-foreground">
                          This side produced actionable candidates without blocker reasons.
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
                  Preferred play explanation and selected candidate details
                </div>
              </div>
            </div>
            <div className="panel-body space-y-4">
              {!result ? (
                <EmptyState
                  title="Result pending"
                  body="The selected generator job has not finished yet."
                />
              ) : result.status === "no_play" ? (
                <>
                  <div>
                    <div className="text-sm font-medium">No play surfaced</div>
                    <div className="mt-1 text-sm leading-6 text-muted-foreground">
                      The scanner did not find a spread that survived the requested filters for this persisted job.
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
                  {result.preferred_play_explanation ? (
                    <div className="rounded-2xl border border-border/70 bg-background/70 px-3 py-3">
                      <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                        Preferred play
                      </div>
                      <p className="mt-2 text-sm text-foreground/80">
                        {result.preferred_play_explanation.summary}
                      </p>
                    </div>
                  ) : null}
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
                  <CandidateOperatorActions job={job} selectedCandidate={selectedCandidate} />
                  <Separator />
                  <ReasonBlock title="Board notes" items={selectedCandidate.board_notes ?? []} />
                  <ReasonBlock title="Setup reasons" items={selectedCandidate.setup_reasons ?? []} />
                  <ReasonBlock title="Calendar reasons" items={selectedCandidate.calendar_reasons ?? []} />
                  <ReasonBlock title="Data quality notes" items={(selectedCandidate as { data_reasons?: string[] }).data_reasons ?? []} />
                </>
              ) : (
                <EmptyState
                  title="No candidate selected"
                  body="Select a ranked candidate to inspect it here."
                />
              )}
            </div>
          </aside>
        </div>
      </div>
    </main>
  );
}

function formatDateTime(value: string) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}
