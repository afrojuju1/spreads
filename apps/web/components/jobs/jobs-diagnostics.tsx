"use client";

import { useQuery } from "@tanstack/react-query";
import { ColumnDef } from "@tanstack/react-table";
import { Activity, BriefcaseBusiness, LoaderCircle, RefreshCw, ServerCog, TimerReset } from "lucide-react";

import { DataTable } from "@/components/data-table";
import {
  type JobDefinition,
  type JobRun,
  getJobRuns,
  getJobs,
  getJobsHealth,
} from "@/lib/api";
import { formatLocalDateTime } from "@/lib/date";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";

type JobDefinitionRow = {
  id: string;
  jobKey: string;
  jobType: string;
  scheduleType: string;
  enabled: boolean;
  raw: JobDefinition;
};

type JobRunRow = {
  id: string;
  jobKey: string;
  jobType: string;
  status: string;
  sessionId: string;
  slotAt: string;
  captureStatus: string;
  workerName: string;
  raw: JobRun;
};

function formatTimestamp(value: string | null | undefined): string {
  return formatLocalDateTime(value);
}

function tone(value: string): string {
  switch (value) {
    case "running":
      return "border-sky-200 bg-sky-100 text-sky-900";
    case "healthy":
    case "succeeded":
    case "enabled":
      return "border-emerald-200 bg-emerald-100 text-emerald-900";
    case "degraded":
    case "queued":
    case "skipped":
      return "border-amber-200 bg-amber-100 text-amber-900";
    case "failed":
    case "disabled":
    case "empty":
      return "border-rose-200 bg-rose-100 text-rose-900";
    default:
      return "border-border/70 bg-card text-foreground";
  }
}

function quoteCapture(run: JobRun): Record<string, unknown> {
  const result = run.result;
  if (!result || typeof result !== "object") {
    return {};
  }
  const capture = (result as Record<string, unknown>).quote_capture;
  return capture && typeof capture === "object" ? (capture as Record<string, unknown>) : {};
}

function readString(value: unknown, fallback = "—"): string {
  return typeof value === "string" && value !== "" ? value : fallback;
}

function StatTile({
  label,
  value,
  note,
}: {
  label: string;
  value: string;
  note?: string;
}) {
  return (
    <div className="rounded-2xl border border-border/70 bg-background/70 px-4 py-3">
      <div className="text-[11px] uppercase tracking-[0.2em] text-muted-foreground">{label}</div>
      <div className="mt-2 text-xl font-semibold">{value}</div>
      {note ? <div className="mt-1 text-xs text-muted-foreground">{note}</div> : null}
    </div>
  );
}

const JOB_RUN_COLUMNS: ColumnDef<JobRunRow>[] = [
  {
    accessorKey: "slotAt",
    header: "Time",
    cell: ({ getValue }) => <span className="font-mono text-[12px]">{formatTimestamp(String(getValue()))}</span>,
  },
  {
    accessorKey: "jobKey",
    header: "Job",
    cell: ({ row }) => (
      <div>
        <div className="font-semibold tracking-[0.03em]">{row.original.jobKey}</div>
        <div className="text-xs text-muted-foreground">{row.original.jobType}</div>
      </div>
    ),
  },
  {
    accessorKey: "status",
    header: "Status",
    cell: ({ getValue }) => (
      <Badge variant="outline" className={`rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em] ${tone(String(getValue()))}`}>
        {String(getValue())}
      </Badge>
    ),
  },
  {
    accessorKey: "captureStatus",
    header: "Capture",
    cell: ({ getValue }) => (
      <Badge variant="outline" className={`rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em] ${tone(String(getValue()))}`}>
        {String(getValue()).replaceAll("_", " ")}
      </Badge>
    ),
  },
  {
    accessorKey: "workerName",
    header: "Worker",
    cell: ({ getValue }) => <span className="text-muted-foreground">{String(getValue())}</span>,
  },
  {
    accessorKey: "sessionId",
    header: "Session",
    cell: ({ getValue }) => <span className="font-mono text-[11px] text-muted-foreground">{String(getValue())}</span>,
  },
];

const JOB_DEFINITION_COLUMNS: ColumnDef<JobDefinitionRow>[] = [
  {
    accessorKey: "jobKey",
    header: "Job",
    cell: ({ row }) => (
      <div>
        <div className="font-semibold tracking-[0.03em]">{row.original.jobKey}</div>
        <div className="text-xs text-muted-foreground">{row.original.jobType}</div>
      </div>
    ),
  },
  {
    accessorKey: "scheduleType",
    header: "Schedule",
    cell: ({ getValue }) => <span className="text-foreground/80">{String(getValue()).replaceAll("_", " ")}</span>,
  },
  {
    accessorKey: "enabled",
    header: "State",
    cell: ({ getValue }) => (
      <Badge variant="outline" className={`rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em] ${tone(Boolean(getValue()) ? "enabled" : "disabled")}`}>
        {Boolean(getValue()) ? "enabled" : "disabled"}
      </Badge>
    ),
  },
];

export function JobsDiagnostics() {
  const jobsQuery = useQuery({
    queryKey: ["jobs"],
    queryFn: () => getJobs(),
  });
  const jobRunsQuery = useQuery({
    queryKey: ["job-runs"],
    queryFn: () => getJobRuns(80),
  });
  const healthQuery = useQuery({
    queryKey: ["jobs-health"],
    queryFn: () => getJobsHealth(),
  });

  const jobRows: JobDefinitionRow[] = (jobsQuery.data?.jobs ?? []).map((job) => ({
    id: job.job_key,
    jobKey: job.job_key,
    jobType: job.job_type,
    scheduleType: job.schedule_type,
    enabled: job.enabled,
    raw: job,
  }));
  const runRows: JobRunRow[] = (jobRunsQuery.data?.job_runs ?? []).map((run) => ({
    id: run.job_run_id,
    jobKey: run.job_key,
    jobType: run.job_type,
    status: run.status,
    sessionId: readString(run.session_id),
    slotAt: readString(run.slot_at ?? run.scheduled_for ?? run.started_at ?? run.finished_at, ""),
    captureStatus: readString(quoteCapture(run).capture_status, "—"),
    workerName: readString(run.worker_name),
    raw: run,
  }));

  const schedulerActive = Boolean(healthQuery.data?.scheduler);
  const workerCount = healthQuery.data?.workers?.length ?? 0;
  const runningCount = healthQuery.data?.running_jobs?.length ?? 0;
  const queuedCount = healthQuery.data?.queued_jobs?.length ?? 0;

  return (
    <main className="mx-auto max-w-[1680px] px-4 py-6 lg:px-6">
      <section className="rounded-[32px] border border-border/70 bg-card/80 shadow-[0_30px_90px_-54px_rgba(15,23,42,0.55)]">
        <div className="flex flex-col gap-4 border-b border-border/70 px-5 py-5 lg:flex-row lg:items-end lg:justify-between lg:px-6">
          <div>
            <Badge variant="outline" className="rounded-full border-border/70 bg-background/80 px-2.5 py-1 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
              <BriefcaseBusiness data-icon="inline-start" />
              Jobs diagnostics
            </Badge>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">Runtime health</div>
            <div className="mt-2 text-sm text-foreground/70">
              Scheduler, workers, recent slot runs, and enabled job definitions.
            </div>
          </div>
          <Button
            type="button"
            variant="outline"
            onClick={() => {
              void jobsQuery.refetch();
              void jobRunsQuery.refetch();
              void healthQuery.refetch();
            }}
          >
            <RefreshCw data-icon="inline-start" />
            Refresh
          </Button>
        </div>
        <div className="flex flex-col gap-4 px-4 py-4 md:px-5">
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <StatTile
              label="Scheduler"
              value={schedulerActive ? "Active" : "Offline"}
              note="Current runtime lease"
            />
            <StatTile
              label="Workers"
              value={String(workerCount)}
              note="Active worker leases"
            />
            <StatTile
              label="Running"
              value={String(runningCount)}
              note="Currently running jobs"
            />
            <StatTile
              label="Queued"
              value={String(queuedCount)}
              note="Queued jobs awaiting workers"
            />
          </div>

          {jobsQuery.isLoading || jobRunsQuery.isLoading || healthQuery.isLoading ? (
            <div className="flex items-center gap-2 px-2 py-8 text-sm text-muted-foreground">
              <LoaderCircle className="size-4 animate-spin" />
              Loading runtime state…
            </div>
          ) : null}

          {jobsQuery.isError || jobRunsQuery.isError || healthQuery.isError ? (
            <div className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-900">
              Jobs diagnostics could not be loaded.
            </div>
          ) : null}

          {!jobsQuery.isLoading && !jobRunsQuery.isLoading && !healthQuery.isLoading ? (
            <div className="grid gap-4 xl:grid-cols-2">
              <section className="rounded-[28px] border border-border/70 bg-background/70">
                <div className="border-b border-border/70 px-4 py-4">
                  <div className="flex items-center gap-2 text-sm font-medium">
                    <Activity className="size-4 text-muted-foreground" />
                    Recent runs
                  </div>
                </div>
                <div className="px-4 py-4">
                  <DataTable
                    columns={JOB_RUN_COLUMNS}
                    data={runRows}
                    getRowId={(row) => row.id}
                    emptyMessage="No recent job runs were found."
                  />
                </div>
              </section>

              <section className="rounded-[28px] border border-border/70 bg-background/70">
                <div className="border-b border-border/70 px-4 py-4">
                  <div className="flex items-center gap-2 text-sm font-medium">
                    <ServerCog className="size-4 text-muted-foreground" />
                    Job definitions
                  </div>
                </div>
                <div className="px-4 py-4">
                  <DataTable
                    columns={JOB_DEFINITION_COLUMNS}
                    data={jobRows}
                    getRowId={(row) => row.id}
                    emptyMessage="No job definitions were found."
                  />
                </div>
              </section>
            </div>
          ) : null}

          {!healthQuery.isLoading && !healthQuery.isError ? (
            <section className="rounded-[28px] border border-border/70 bg-background/70">
              <div className="border-b border-border/70 px-4 py-4">
                <div className="flex items-center gap-2 text-sm font-medium">
                  <TimerReset className="size-4 text-muted-foreground" />
                  Latest successful collectors
                </div>
              </div>
              <div className="grid gap-3 px-4 py-4 md:grid-cols-2 xl:grid-cols-3">
                {Object.entries(healthQuery.data?.latest_successful_collectors ?? {}).length ? (
                  Object.entries(healthQuery.data?.latest_successful_collectors ?? {}).map(([jobKey, run]) => {
                    const row = run && typeof run === "object" ? (run as Record<string, unknown>) : null;
                    return (
                      <div key={jobKey} className="rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
                        <div className="text-sm font-semibold tracking-[0.03em]">{jobKey}</div>
                        <div className="mt-1 text-xs text-muted-foreground">
                          {row ? formatTimestamp(readString(row.finished_at, readString(row.slot_at, "—"))) : "No successful run yet"}
                        </div>
                        <div className="mt-3">
                          <Badge variant="outline" className={`rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em] ${tone(row ? readString(row.status, "healthy") : "empty")}`}>
                            {row ? readString(row.status, "healthy") : "empty"}
                          </Badge>
                        </div>
                      </div>
                    );
                  })
                ) : (
                  <div className="px-1 py-2 text-sm text-muted-foreground">
                    No collector runs have succeeded yet.
                  </div>
                )}
              </div>
            </section>
          ) : null}
        </div>
      </section>
    </main>
  );
}
