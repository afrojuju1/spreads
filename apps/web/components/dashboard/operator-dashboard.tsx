"use client";

import { useQuery } from "@tanstack/react-query";
import { ColumnDef } from "@tanstack/react-table";
import {
  Activity,
  BellRing,
  BriefcaseBusiness,
  CandlestickChart,
  LoaderCircle,
  PanelRightOpen,
  Radar,
  RefreshCw,
  SlidersHorizontal,
} from "lucide-react";
import { useDeferredValue, useState } from "react";
import Link from "next/link";

import { DataTable } from "@/components/dashboard/data-table";
import {
  AlertRecord,
  CandidateDetail,
  JobRun,
  LiveCandidate,
  LiveEvent,
  SessionIdea,
  TuningBucket,
  getAlerts,
  getJobs,
  getJobRuns,
  getJobsHealth,
  getLive,
  getLiveEvents,
  getSessionSummary,
  getSessionTuning,
} from "@/lib/api";
import { cn } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  ScrollArea,
} from "@/components/ui/scroll-area";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Separator } from "@/components/ui/separator";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet";
import { Skeleton } from "@/components/ui/skeleton";

type DashboardSection = "live" | "sessions" | "alerts" | "jobs";

type CandidateTableRow = {
  id: string;
  bucket: "board" | "watchlist";
  symbol: string;
  strategy: string;
  strikes: string;
  expirationDate: string;
  score: number;
  credit: number;
  setupStatus: string;
  calendarStatus: string;
  generatedAt: string;
  raw: LiveCandidate;
};

type EventTableRow = {
  id: string;
  time: string;
  symbol: string;
  eventType: string;
  message: string;
  raw: LiveEvent;
};

type AlertTableRow = {
  id: string;
  time: string;
  symbol: string;
  alertType: string;
  status: string;
  label: string;
  description: string;
  raw: AlertRecord;
};

type JobRunTableRow = {
  id: string;
  jobKey: string;
  jobType: string;
  status: string;
  startedAt: string;
  finishedAt: string;
  workerName: string;
  raw: JobRun;
};

type SessionIdeaRow = {
  id: string;
  symbol: string;
  strategy: string;
  classification: string;
  scoreBucket: string;
  latestScore: number;
  outcomeBucket: string;
  replayVerdict: string;
  estimatedClosePnl: number | null;
  firstSeen: string;
  entrySeen: string;
  raw: SessionIdea;
};

type InspectorSelection =
  | { section: DashboardSection; kind: "candidate"; data: CandidateTableRow }
  | { section: DashboardSection; kind: "event"; data: EventTableRow }
  | { section: DashboardSection; kind: "alert"; data: AlertTableRow }
  | { section: DashboardSection; kind: "job"; data: JobRunTableRow }
  | { section: DashboardSection; kind: "idea"; data: SessionIdeaRow };

const SECTION_ITEMS: Array<{
  value: DashboardSection;
  label: string;
  icon: React.ComponentType<{ className?: string }>;
  caption: string;
}> = [
  { value: "live", label: "Live", icon: Radar, caption: "board + watchlist" },
  { value: "sessions", label: "Sessions", icon: CandlestickChart, caption: "outcomes + tuning" },
  { value: "alerts", label: "Alerts", icon: BellRing, caption: "discord feed" },
  { value: "jobs", label: "Jobs", icon: BriefcaseBusiness, caption: "runtime health" },
];

const CANDIDATE_COLUMNS: ColumnDef<CandidateTableRow>[] = [
  {
    accessorKey: "symbol",
    header: "Symbol",
    cell: ({ row }) => (
      <div className="flex items-center gap-2">
        <span className="font-semibold tracking-[0.06em]">{row.original.symbol}</span>
        <Badge
          variant="outline"
          className={cn(
            "border-transparent",
            row.original.bucket === "board"
              ? "bg-stone-900 text-stone-50"
              : "bg-amber-100 text-amber-900",
          )}
        >
          {row.original.bucket}
        </Badge>
      </div>
    ),
  },
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
    accessorKey: "score",
    header: "Score",
    cell: ({ getValue }) => <span className="mono">{formatScore(Number(getValue()))}</span>,
  },
  {
    accessorKey: "credit",
    header: "Credit",
    cell: ({ getValue }) => <span className="mono">{formatDollar(Number(getValue()))}</span>,
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
  {
    accessorKey: "generatedAt",
    header: "Updated",
    cell: ({ getValue }) => <span className="mono text-muted-foreground">{formatTime(String(getValue()))}</span>,
  },
];

const EVENT_COLUMNS: ColumnDef<EventTableRow>[] = [
  {
    accessorKey: "time",
    header: "Time",
    cell: ({ getValue }) => <span className="mono text-muted-foreground">{formatTime(String(getValue()))}</span>,
  },
  {
    accessorKey: "symbol",
    header: "Symbol",
    cell: ({ getValue }) => <span className="font-semibold">{String(getValue())}</span>,
  },
  {
    accessorKey: "eventType",
    header: "Event",
    cell: ({ getValue }) => <StatusBadge value={String(getValue())} tone="event" />,
  },
  {
    accessorKey: "message",
    header: "Message",
    cell: ({ getValue }) => <span className="text-foreground/80">{String(getValue())}</span>,
  },
];

const ALERT_COLUMNS: ColumnDef<AlertTableRow>[] = [
  {
    accessorKey: "time",
    header: "Time",
    cell: ({ getValue }) => <span className="mono text-muted-foreground">{formatTime(String(getValue()))}</span>,
  },
  {
    accessorKey: "symbol",
    header: "Symbol",
    cell: ({ getValue }) => <span className="font-semibold">{String(getValue())}</span>,
  },
  {
    accessorKey: "alertType",
    header: "Type",
    cell: ({ getValue }) => <StatusBadge value={String(getValue())} tone="event" />,
  },
  {
    accessorKey: "status",
    header: "Status",
    cell: ({ getValue }) => <StatusBadge value={String(getValue())} tone="job" />,
  },
  {
    accessorKey: "description",
    header: "Description",
    cell: ({ getValue }) => <span className="text-foreground/80">{String(getValue())}</span>,
  },
];

const JOB_RUN_COLUMNS: ColumnDef<JobRunTableRow>[] = [
  {
    accessorKey: "jobType",
    header: "Type",
    cell: ({ getValue }) => <span className="font-medium">{String(getValue())}</span>,
  },
  {
    accessorKey: "status",
    header: "Status",
    cell: ({ getValue }) => <StatusBadge value={String(getValue())} tone="job" />,
  },
  {
    accessorKey: "jobKey",
    header: "Job",
    cell: ({ getValue }) => <span className="mono text-[12px]">{String(getValue())}</span>,
  },
  {
    accessorKey: "startedAt",
    header: "Started",
    cell: ({ getValue }) => <span className="mono text-muted-foreground">{formatDateTime(String(getValue()))}</span>,
  },
  {
    accessorKey: "finishedAt",
    header: "Finished",
    cell: ({ getValue }) => <span className="mono text-muted-foreground">{formatDateTime(String(getValue()))}</span>,
  },
];

const SESSION_COLUMNS: ColumnDef<SessionIdeaRow>[] = [
  {
    accessorKey: "symbol",
    header: "Symbol",
    cell: ({ getValue }) => <span className="font-semibold">{String(getValue())}</span>,
  },
  {
    accessorKey: "strategy",
    header: "Side",
    cell: ({ getValue }) => <StrategyBadge strategy={String(getValue())} />,
  },
  {
    accessorKey: "classification",
    header: "Class",
    cell: ({ getValue }) => (
      <Badge variant="outline" className="border-stone-300 bg-stone-100/80 text-stone-700">
        {String(getValue())}
      </Badge>
    ),
  },
  {
    accessorKey: "latestScore",
    header: "Score",
    cell: ({ getValue }) => <span className="mono">{formatScore(Number(getValue()))}</span>,
  },
  {
    accessorKey: "scoreBucket",
    header: "Bucket",
    cell: ({ getValue }) => <span className="mono text-muted-foreground">{String(getValue())}</span>,
  },
  {
    accessorKey: "outcomeBucket",
    header: "Outcome",
    cell: ({ getValue }) => <StatusBadge value={String(getValue())} tone="outcome" />,
  },
  {
    accessorKey: "estimatedClosePnl",
    header: "Close PnL",
    cell: ({ getValue }) => (
      <span className={cn("mono", pnlTone(getValue() as number | null))}>
        {formatPnl(getValue() as number | null)}
      </span>
    ),
  },
  {
    accessorKey: "replayVerdict",
    header: "Verdict",
    cell: ({ getValue }) => <span className="text-foreground/80">{String(getValue())}</span>,
  },
];

export function OperatorDashboard({ section }: { section: DashboardSection }) {
  const [liveLabel, setLiveLabel] = useState("");
  const [sessionLabel, setSessionLabel] = useState("");
  const [sessionDate, setSessionDate] = useState("");
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<InspectorSelection | null>(null);
  const [controlsOpen, setControlsOpen] = useState(false);
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const deferredSearch = useDeferredValue(search.trim().toLowerCase());

  const jobsQuery = useQuery({
    queryKey: ["jobs"],
    queryFn: getJobs,
    refetchInterval: 120_000,
  });
  const jobRunsQuery = useQuery({
    queryKey: ["job-runs"],
    queryFn: () => getJobRuns(8),
    refetchInterval: 60_000,
  });
  const jobsHealthQuery = useQuery({
    queryKey: ["jobs-health"],
    queryFn: getJobsHealth,
    refetchInterval: 30_000,
  });
  const alertsQuery = useQuery({
    queryKey: ["alerts-latest"],
    queryFn: () => getAlerts(8),
    refetchInterval: 60_000,
  });

  const liveLabels = jobsQuery.data?.jobs
    .filter((job) => job.job_type === "live_collector")
    .map((job) => job.singleton_scope ?? job.job_key)
    .filter(Boolean) as string[] | undefined;

  const sessionLabels = jobsQuery.data?.jobs
    .filter((job) => job.job_type === "post_close_analysis")
    .map((job) => String(job.payload.label ?? ""))
    .filter(Boolean);

  const resolvedLiveLabel = liveLabel || liveLabels?.[0] || "";
  const resolvedSessionLabel = sessionLabel || sessionLabels?.[0] || "";

  const liveQuery = useQuery({
    queryKey: ["live", resolvedLiveLabel],
    queryFn: () => getLive(resolvedLiveLabel),
    enabled: Boolean(resolvedLiveLabel),
    refetchInterval: 30_000,
  });
  const liveEventsQuery = useQuery({
    queryKey: ["live-events", resolvedLiveLabel],
    queryFn: () => getLiveEvents(resolvedLiveLabel, 8),
    enabled: Boolean(resolvedLiveLabel),
    refetchInterval: 30_000,
  });

  const resolvedSessionDate =
    sessionDate || liveQuery.data?.session_date || new Date().toISOString().slice(0, 10);

  const summaryQuery = useQuery({
    queryKey: ["session-summary", resolvedSessionDate, resolvedSessionLabel],
    queryFn: () => getSessionSummary(resolvedSessionDate, resolvedSessionLabel),
    enabled: Boolean(resolvedSessionDate && resolvedSessionLabel),
  });
  const tuningQuery = useQuery({
    queryKey: ["session-tuning", resolvedSessionDate, resolvedSessionLabel],
    queryFn: () => getSessionTuning(resolvedSessionDate, resolvedSessionLabel),
    enabled: Boolean(resolvedSessionDate && resolvedSessionLabel),
  });

  const liveRows = filterRows(buildCandidateRows(liveQuery.data), deferredSearch, [
    "symbol",
    "strategy",
    "strikes",
    "setupStatus",
    "calendarStatus",
  ]);
  const eventRows = filterRows(buildEventRows(liveEventsQuery.data?.events ?? []), deferredSearch, [
    "symbol",
    "eventType",
    "message",
  ]);
  const alertRows = filterRows(buildAlertRows(alertsQuery.data?.alerts ?? []), deferredSearch, [
    "symbol",
    "alertType",
    "status",
    "label",
    "description",
  ]);
  const jobRunRows = filterRows(buildJobRunRows(jobRunsQuery.data?.job_runs ?? []), deferredSearch, [
    "jobKey",
    "jobType",
    "status",
  ]);
  const sessionIdeaRows = filterRows(
    buildSessionIdeaRows(summaryQuery.data?.outcomes?.ideas ?? []),
    deferredSearch,
    ["symbol", "strategy", "classification", "scoreBucket", "replayVerdict", "outcomeBucket"],
  );

  const activeSelection =
    selected && selected.section === section
      ? selected
      : defaultSelection({
          section,
          liveRows,
          eventRows,
          alertRows,
          jobRunRows,
          sessionIdeaRows,
        });

  const healthScheduler = jobsHealthQuery.data?.scheduler;
  const healthWorkers = jobsHealthQuery.data?.workers ?? [];
  const liveBoardCount = liveQuery.data?.board_candidates.length ?? 0;
  const liveWatchlistCount = liveQuery.data?.watchlist_candidates.length ?? 0;
  const outcomeCounts = (summaryQuery.data?.outcomes?.outcome_counts_by_bucket ??
    {}) as Record<string, Record<string, number>>;
  const ideasCount = summaryQuery.data?.outcomes?.idea_count ?? 0;
  const countsByBucket = (summaryQuery.data?.outcomes?.counts_by_bucket ??
    {}) as Record<string, number>;
  const tuningDimensions = (tuningQuery.data?.dimensions ?? {}) as Record<string, TuningBucket[]>;
  const runningJobCount = jobsHealthQuery.data?.running_jobs?.length ?? 0;
  const queuedJobCount = jobsHealthQuery.data?.queued_jobs?.length ?? 0;
  const recentSuccessCount = (jobRunsQuery.data?.job_runs ?? []).filter(
    (run) => run.status === "succeeded",
  ).length;

  return (
    <main className="min-h-dvh">
      <div className="mx-auto flex max-w-[1680px] flex-col gap-4 px-4 py-4 lg:px-6">
        <header className="panel grid gap-4 px-4 py-4 lg:grid-cols-[minmax(0,1fr)_auto] lg:px-6">
          <div className="space-y-2">
            <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.26em] text-muted-foreground">
              <Activity className="size-3.5" />
              Spreads operator
            </div>
            <div className="flex flex-wrap items-end gap-4">
              <div>
                <h1 className="max-w-4xl text-[clamp(2.1rem,6vw,3.4rem)] leading-[0.95] font-semibold tracking-tight text-foreground">
                  Live board, session review, and runtime control.
                </h1>
                <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
                  TanStack-driven operator workspace over the Postgres-backed live API.
                </p>
              </div>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2 lg:justify-end">
            <HealthPill
              label="api"
              value="healthy"
              tone="good"
              detail="served by FastAPI"
            />
            <HealthPill
              label="scheduler"
              value={healthScheduler ? "active" : "missing"}
              tone={healthScheduler ? "good" : "warn"}
              detail={healthScheduler ? formatTime(String(healthScheduler.heartbeat_at ?? "")) : "no heartbeat"}
            />
            <HealthPill
              label="worker"
              value={healthWorkers.length ? "active" : "idle"}
              tone={healthWorkers.length ? "good" : "warn"}
              detail={healthWorkers.length ? `${healthWorkers.length} online` : "none reported"}
            />
          </div>
        </header>

        <div className="lg:hidden">
          <div className="panel flex flex-col gap-3 px-3 py-3">
            <ScrollArea className="w-full whitespace-nowrap">
              <div className="flex w-max gap-2 pr-2">
                {SECTION_ITEMS.map((item) => (
                  <SectionButton
                    key={item.value}
                    item={item}
                    active={section === item.value}
                    href={`/${item.value}`}
                    compact
                  />
                ))}
              </div>
            </ScrollArea>
            <div className="flex items-center gap-2">
              <Sheet open={controlsOpen} onOpenChange={setControlsOpen}>
                <SheetTrigger
                  render={
                    <Button variant="outline" className="flex-1 justify-start rounded-2xl" />
                  }
                >
                  <SlidersHorizontal className="size-4" />
                  Controls
                </SheetTrigger>
                <SheetContent side="bottom" className="max-h-[85dvh] rounded-t-3xl border-border bg-card/98">
                  <SheetHeader className="border-b border-border/70 px-5 py-4">
                    <SheetTitle>Workspace controls</SheetTitle>
                    <SheetDescription>
                      Switch labels, filter rows, and review live counts.
                    </SheetDescription>
                  </SheetHeader>
                  <ScrollArea className="max-h-[calc(85dvh-5rem)] px-5 py-4">
                    <ControlsPanel
                      section={section}
                      setControlsOpen={setControlsOpen}
                      search={search}
                      setSearch={setSearch}
                      liveLabels={liveLabels ?? []}
                      resolvedLiveLabel={resolvedLiveLabel}
                      setLiveLabel={setLiveLabel}
                      sessionLabels={sessionLabels ?? []}
                      resolvedSessionLabel={resolvedSessionLabel}
                      setSessionLabel={setSessionLabel}
                      resolvedSessionDate={resolvedSessionDate}
                      setSessionDate={setSessionDate}
                      liveBoardCount={liveBoardCount}
                      liveWatchlistCount={liveWatchlistCount}
                      alertCount={alertRows.length}
                      ideasCount={ideasCount}
                    />
                  </ScrollArea>
                </SheetContent>
              </Sheet>
              <Sheet open={inspectorOpen} onOpenChange={setInspectorOpen}>
                <SheetTrigger
                  render={
                    <Button
                      variant="outline"
                      className="flex-1 justify-start rounded-2xl"
                      disabled={!activeSelection}
                    />
                  }
                >
                  <PanelRightOpen className="size-4" />
                  Details
                </SheetTrigger>
                <SheetContent side="bottom" className="h-[88dvh] rounded-t-3xl border-border bg-card/98">
                  <SheetHeader className="border-b border-border/70 px-5 py-4">
                    <SheetTitle>{activeSelection ? inspectorTitle(activeSelection) : "Inspector"}</SheetTitle>
                    <SheetDescription>
                      Drill into the selected row without leaving the current workspace.
                    </SheetDescription>
                  </SheetHeader>
                  <ScrollArea className="h-[calc(88dvh-5rem)] px-5 py-4">
                    {activeSelection ? (
                      <InspectorContent selection={activeSelection} />
                    ) : (
                      <EmptyHint message="Select a row to inspect it here." />
                    )}
                  </ScrollArea>
                </SheetContent>
              </Sheet>
            </div>
          </div>
        </div>

        <div className="grid gap-4 lg:grid-cols-[240px_minmax(0,1fr)_360px]">
          <aside className="panel order-2 hidden flex-col gap-4 px-3 py-3 lg:order-1 lg:flex">
            <ControlsPanel
              section={section}
              setControlsOpen={setControlsOpen}
              search={search}
              setSearch={setSearch}
              liveLabels={liveLabels ?? []}
              resolvedLiveLabel={resolvedLiveLabel}
              setLiveLabel={setLiveLabel}
              sessionLabels={sessionLabels ?? []}
              resolvedSessionLabel={resolvedSessionLabel}
              setSessionLabel={setSessionLabel}
              resolvedSessionDate={resolvedSessionDate}
              setSessionDate={setSessionDate}
              liveBoardCount={liveBoardCount}
              liveWatchlistCount={liveWatchlistCount}
              alertCount={alertRows.length}
              ideasCount={ideasCount}
            />
          </aside>

          <div className="order-1 flex min-w-0 flex-col gap-4 lg:order-2">
            {section === "live" ? (
              <SectionHeader
                title="Live board"
                subtitle="Accepted board ideas and watchlist candidates from the latest persisted cycle."
                stamp={liveQuery.data?.generated_at}
                loading={liveQuery.isFetching}
              />
            ) : null}
            {section === "live" ? (
              <div className="grid gap-4">
                <Panel
                  title="Cycle snapshot"
                  meta={`${liveQuery.data?.profile ?? "—"} · ${liveQuery.data?.strategy ?? "—"}`}
                >
                  <SnapshotMetricRow
                    metrics={[
                      { label: "board", value: String(liveBoardCount) },
                      { label: "watchlist", value: String(liveWatchlistCount) },
                      { label: "session", value: liveQuery.data?.session_date ?? "—" },
                      { label: "universe", value: liveQuery.data?.universe_label ?? "—" },
                    ]}
                  />
                  <div className="mt-3">
                    {liveQuery.isLoading ? (
                      <LoadingBlock />
                    ) : (
                      <DataTable
                        columns={CANDIDATE_COLUMNS}
                        data={liveRows}
                        emptyMessage="No live board or watchlist candidates for the selected label."
                        getRowId={(row) => row.id}
                        selectedId={
                          activeSelection?.section === "live" && activeSelection.kind === "candidate"
                            ? activeSelection.data.id
                            : null
                        }
                        onSelect={(row) => setSelected({ section: "live", kind: "candidate", data: row })}
                      />
                    )}
                  </div>
                </Panel>
                <Panel title="Event tape" meta={`${eventRows.length} recent events`}>
                  {liveEventsQuery.isLoading ? (
                    <LoadingBlock />
                  ) : (
                    <DataTable
                      columns={EVENT_COLUMNS}
                      data={eventRows}
                      emptyMessage="No board events recorded for the selected label."
                      getRowId={(row) => row.id}
                      selectedId={
                        activeSelection?.section === "live" && activeSelection.kind === "event"
                          ? activeSelection.data.id
                          : null
                      }
                      onSelect={(row) => setSelected({ section: "live", kind: "event", data: row })}
                    />
                  )}
                </Panel>
              </div>
            ) : null}

            {section === "sessions" ? (
              <>
              <SectionHeader
                title="Session review"
                subtitle="Outcome snapshots and signal buckets rendered from the DB-backed post-close analytics."
                stamp={summaryQuery.data?.session_date}
                loading={summaryQuery.isFetching}
              />
              <div className="grid gap-4 xl:grid-cols-[minmax(0,1.2fr)_minmax(360px,0.8fr)]">
                <Panel title="Ideas and outcomes" meta={`${sessionIdeaRows.length} ideas`}>
                  <MetricRow
                    metrics={[
                      { label: "board", value: String(countsByBucket.board ?? 0) },
                      { label: "watchlist", value: String(countsByBucket.watchlist ?? 0) },
                      { label: "wins", value: String(outcomeCounts.board?.win ?? 0) },
                      { label: "open", value: String((outcomeCounts.board?.still_open ?? 0) + (outcomeCounts.watchlist?.still_open ?? 0)) },
                    ]}
                  />
                  <div className="mt-3">
                    {summaryQuery.isLoading ? (
                      <LoadingBlock />
                    ) : (
                      <DataTable
                        columns={SESSION_COLUMNS}
                        data={sessionIdeaRows}
                        emptyMessage="No session outcomes available for the selected date and label."
                        getRowId={(row) => row.id}
                        selectedId={
                          activeSelection?.section === "sessions" && activeSelection.kind === "idea"
                            ? activeSelection.data.id
                            : null
                        }
                        onSelect={(row) => setSelected({ section: "sessions", kind: "idea", data: row })}
                      />
                    )}
                  </div>
                </Panel>
                <div className="grid gap-4">
                  <TuningPanel
                    title="By classification"
                    buckets={tuningDimensions.classification ?? []}
                  />
                  <TuningPanel
                    title="By strategy"
                    buckets={tuningDimensions.strategy ?? []}
                  />
                  <TuningPanel
                    title="By score bucket"
                    buckets={tuningDimensions.score_bucket ?? []}
                  />
                </div>
              </div>
              </>
            ) : null}

            {section === "alerts" ? (
              <>
              <SectionHeader
                title="Alerts"
                subtitle="Discord alert feed from persisted alert rows, with delivery status and descriptions."
                stamp={alertRows[0]?.time}
                loading={alertsQuery.isFetching}
              />
              <Panel title="Latest alerts" meta={`${alertRows.length} rows`}>
                <MetricRow
                  metrics={[
                    { label: "delivered", value: String(alertRows.filter((row) => row.status === "delivered").length) },
                    { label: "skipped", value: String(alertRows.filter((row) => row.status === "skipped").length) },
                    { label: "failed", value: String(alertRows.filter((row) => row.status === "failed").length) },
                    { label: "symbols", value: String(new Set(alertRows.map((row) => row.symbol)).size) },
                  ]}
                />
                <div className="mt-3">
                  {alertsQuery.isLoading ? (
                    <LoadingBlock />
                  ) : (
                    <DataTable
                      columns={ALERT_COLUMNS}
                      data={alertRows}
                      emptyMessage="No alerts have been emitted yet."
                      getRowId={(row) => row.id}
                      selectedId={
                        activeSelection?.section === "alerts" && activeSelection.kind === "alert"
                          ? activeSelection.data.id
                          : null
                      }
                      onSelect={(row) => setSelected({ section: "alerts", kind: "alert", data: row })}
                    />
                  )}
                </div>
              </Panel>
              </>
            ) : null}

            {section === "jobs" ? (
              <>
              <SectionHeader
                title="Jobs and runtime"
                subtitle="Seeded job definitions, recent job runs, and the current scheduler / worker heartbeat state."
                stamp={
                  typeof healthScheduler?.heartbeat_at === "string"
                    ? healthScheduler.heartbeat_at
                    : undefined
                }
                loading={jobsHealthQuery.isFetching}
              />
              <MetricRow
                metrics={[
                  { label: "definitions", value: String(jobsQuery.data?.jobs.length ?? 0) },
                  { label: "running", value: String(runningJobCount) },
                  { label: "queued", value: String(queuedJobCount) },
                  { label: "recent ok", value: String(recentSuccessCount) },
                ]}
              />
              <div className="grid gap-4 xl:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
                <Panel title="Definitions" meta={`${jobsQuery.data?.jobs.length ?? 0} jobs`}>
                  <div className="space-y-3">
                    {(jobsQuery.data?.jobs ?? []).map((job) => (
                      <button
                        key={job.job_key}
                        type="button"
                        className="metric-card w-full text-left transition hover:border-ring/40 hover:bg-accent/20"
                        onClick={() =>
                          setSelected({
                            section: "jobs",
                            kind: "job",
                            data: {
                              id: job.job_key,
                              jobKey: job.job_key,
                              jobType: job.job_type,
                              status: job.enabled ? "enabled" : "disabled",
                              startedAt: job.created_at,
                              finishedAt: job.updated_at,
                              workerName: "",
                              raw: {
                                job_run_id: job.job_key,
                                job_key: job.job_key,
                                job_type: job.job_type,
                                status: job.enabled ? "enabled" : "disabled",
                                scheduled_for: null,
                                started_at: job.created_at,
                                finished_at: job.updated_at,
                                heartbeat_at: null,
                                worker_name: null,
                                payload: job.payload,
                                result: job.schedule,
                                error_text: null,
                              },
                            },
                          })
                        }
                      >
                        <div className="flex items-center justify-between gap-3">
                          <div>
                            <div className="font-medium text-foreground">{job.job_type}</div>
                            <div className="mono mt-1 text-muted-foreground">{job.job_key}</div>
                          </div>
                          <StatusBadge value={job.enabled ? "enabled" : "disabled"} tone="job" />
                        </div>
                      </button>
                    ))}
                  </div>
                </Panel>
                <Panel title="Recent runs" meta={`${jobRunRows.length} rows`}>
                  {jobRunsQuery.isLoading ? (
                    <LoadingBlock />
                  ) : (
                    <DataTable
                      columns={JOB_RUN_COLUMNS}
                      data={jobRunRows}
                      emptyMessage="No job runs have been recorded yet."
                      getRowId={(row) => row.id}
                      selectedId={
                        activeSelection?.section === "jobs" && activeSelection.kind === "job"
                          ? activeSelection.data.id
                          : null
                      }
                      onSelect={(row) => setSelected({ section: "jobs", kind: "job", data: row })}
                    />
                  )}
                </Panel>
              </div>
              </>
            ) : null}
          </div>

          <div className="order-3 hidden lg:block">
            <InspectorPanel selection={activeSelection} />
          </div>
        </div>
      </div>
    </main>
  );
}

function ControlsPanel({
  section,
  setControlsOpen,
  search,
  setSearch,
  liveLabels,
  resolvedLiveLabel,
  setLiveLabel,
  sessionLabels,
  resolvedSessionLabel,
  setSessionLabel,
  resolvedSessionDate,
  setSessionDate,
  liveBoardCount,
  liveWatchlistCount,
  alertCount,
  ideasCount,
}: {
  section: DashboardSection;
  setControlsOpen: (open: boolean) => void;
  search: string;
  setSearch: (value: string) => void;
  liveLabels: string[];
  resolvedLiveLabel: string;
  setLiveLabel: (value: string) => void;
  sessionLabels: string[];
  resolvedSessionLabel: string;
  setSessionLabel: (value: string) => void;
  resolvedSessionDate: string;
  setSessionDate: (value: string) => void;
  liveBoardCount: number;
  liveWatchlistCount: number;
  alertCount: number;
  ideasCount: number;
}) {
  return (
    <>
      <div className="space-y-1 px-2">
        <div className="text-[11px] uppercase tracking-[0.2em] text-muted-foreground">
          Workspace
        </div>
        <div className="text-lg font-semibold text-foreground">Operator console</div>
      </div>
      <div className="grid gap-2">
        {SECTION_ITEMS.map((item) => (
          <SectionButton
            key={item.value}
            item={item}
            active={section === item.value}
            href={`/${item.value}`}
            onNavigate={() => setControlsOpen(false)}
          />
        ))}
      </div>
      <Separator />
      <div className="space-y-3 px-2">
        <div className="text-[11px] uppercase tracking-[0.2em] text-muted-foreground">
          Filters
        </div>
        <Input
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Search symbol, side, status…"
          className="h-9 rounded-2xl bg-background/80"
        />
        <div className="space-y-2">
          <label className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
            Live label
          </label>
          <Select value={resolvedLiveLabel} onValueChange={(value) => setLiveLabel(value ?? "")}>
            <SelectTrigger className="w-full rounded-2xl bg-background/80">
              <SelectValue placeholder="Select live label" />
            </SelectTrigger>
            <SelectContent>
              {liveLabels.map((label) => (
                <SelectItem key={label} value={label}>
                  {label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-2">
          <label className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
            Session label
          </label>
          <Select
            value={resolvedSessionLabel}
            onValueChange={(value) => setSessionLabel(value ?? "")}
          >
            <SelectTrigger className="w-full rounded-2xl bg-background/80">
              <SelectValue placeholder="Select session label" />
            </SelectTrigger>
            <SelectContent>
              {sessionLabels.map((label) => (
                <SelectItem key={label} value={label}>
                  {label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-2">
          <label className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
            Session date
          </label>
          <Input
            type="date"
            value={resolvedSessionDate}
            onChange={(event) => setSessionDate(event.target.value)}
            className="rounded-2xl bg-background/80"
          />
        </div>
      </div>
      <Separator />
      <div className="grid gap-2 px-2">
        <MiniStat label="board" value={String(liveBoardCount)} />
        <MiniStat label="watchlist" value={String(liveWatchlistCount)} />
        <MiniStat label="alerts" value={String(alertCount)} />
        <MiniStat label="ideas" value={String(ideasCount)} />
      </div>
    </>
  );
}

function SectionButton({
  item,
  active,
  href,
  onNavigate,
  compact = false,
}: {
  item: (typeof SECTION_ITEMS)[number];
  active: boolean;
  href: string;
  onNavigate?: () => void;
  compact?: boolean;
}) {
  const Icon = item.icon;

  return (
    <Link
      href={href}
      onClick={onNavigate}
      className={cn(
        "block rounded-2xl border text-left transition",
        active
          ? "border-border/80 bg-background/90 shadow-sm"
          : "border-transparent bg-transparent hover:border-border/60 hover:bg-background/60",
        compact ? "min-w-[150px] px-3 py-2.5" : "w-full px-3 py-3",
      )}
      >
        <span className="flex items-center gap-3">
          <Icon className="size-4" />
        <span className="text-left">
          <span className="block text-sm font-medium text-foreground">{item.label}</span>
          <span className="block text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
            {item.caption}
          </span>
        </span>
        </span>
    </Link>
  );
}

function Panel({
  title,
  meta,
  children,
}: {
  title: string;
  meta?: string;
  children: React.ReactNode;
}) {
  return (
    <section className="panel overflow-hidden">
      <div className="panel-header items-start">
        <div className="min-w-0 flex-1">
          <h2 className="break-words text-base font-semibold text-foreground">{title}</h2>
          {meta ? (
            <p className="mt-1 break-words text-xs uppercase tracking-[0.18em] text-muted-foreground">
              {meta}
            </p>
          ) : null}
        </div>
      </div>
      <div className="panel-body">{children}</div>
    </section>
  );
}

function SectionHeader({
  title,
  subtitle,
  stamp,
  loading,
}: {
  title: string;
  subtitle: string;
  stamp?: string;
  loading?: boolean;
}) {
  return (
    <div className="panel flex flex-col gap-3 px-4 py-3 sm:flex-row sm:items-start sm:justify-between lg:px-5">
      <div className="min-w-0 flex-1">
        <div className="text-[11px] uppercase tracking-[0.22em] text-muted-foreground">workspace</div>
        <h2 className="mt-1 break-words text-2xl font-semibold tracking-tight text-foreground">{title}</h2>
        <p className="mt-1 max-w-3xl break-words text-sm text-muted-foreground">{subtitle}</p>
      </div>
      <div className="flex shrink-0 items-center gap-2 text-sm text-muted-foreground">
        {loading ? <LoaderCircle className="size-4 animate-spin" /> : <RefreshCw className="size-4" />}
        <span className="mono">{stamp ? formatDateTime(stamp) : "updating"}</span>
      </div>
    </div>
  );
}

function MetricRow({
  metrics,
}: {
  metrics: Array<{ label: string; value: string }>;
}) {
  return (
    <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-4">
      {metrics.map((metric) => (
        <div
          key={metric.label}
          className="rounded-2xl border border-border/70 bg-background/80 px-3 py-2.5"
        >
          <div className="flex items-center justify-between gap-3">
            <span className="text-[9px] uppercase tracking-[0.22em] text-muted-foreground">
              {metric.label}
            </span>
            <span className="mono truncate text-right text-sm font-medium text-foreground sm:text-base">
              {metric.value}
            </span>
          </div>
        </div>
      ))}
    </div>
  );
}

function SnapshotMetricRow({
  metrics,
}: {
  metrics: Array<{ label: string; value: string }>;
}) {
  return (
    <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-4">
      {metrics.map((metric) => {
        const compactValue = metric.value.length > 6;

        return (
          <div
            key={metric.label}
            className="rounded-2xl border border-border/70 bg-background/80 px-3 py-3"
          >
            <div className="flex items-center justify-between gap-3">
              <span className="text-[9px] uppercase tracking-[0.22em] text-muted-foreground">
                {metric.label}
              </span>
              <span
                className={cn(
                  "truncate text-right font-medium text-foreground",
                  compactValue ? "mono text-[11px]" : "mono text-2xl leading-none",
                )}
                title={metric.value}
              >
                {metric.value}
              </span>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function MiniStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric-card flex items-center justify-between gap-4 px-3 py-2.5">
      <span className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">{label}</span>
      <span className="mono text-base font-medium text-foreground">{value}</span>
    </div>
  );
}

function HealthPill({
  label,
  value,
  tone,
  detail,
}: {
  label: string;
  value: string;
  tone: "good" | "warn" | "neutral";
  detail: string;
}) {
  const toneClasses =
    tone === "good"
      ? "border-emerald-300/70 bg-emerald-100/70 text-emerald-900"
      : tone === "warn"
        ? "border-amber-300/70 bg-amber-100/70 text-amber-900"
        : "border-stone-300/70 bg-stone-100/80 text-stone-800";

  return (
    <div className={cn("rounded-2xl border px-3 py-2", toneClasses)}>
      <div className="text-[11px] uppercase tracking-[0.18em] opacity-75">{label}</div>
      <div className="mt-1 flex items-center gap-2">
        <span className="font-medium">{value}</span>
        <span className="mono text-[12px] opacity-80">{detail}</span>
      </div>
    </div>
  );
}

function StrategyBadge({ strategy }: { strategy: string }) {
  const classes =
    strategy === "put_credit"
      ? "border-emerald-200 bg-emerald-100 text-emerald-900"
      : strategy === "call_credit"
        ? "border-rose-200 bg-rose-100 text-rose-900"
        : "border-stone-200 bg-stone-100 text-stone-800";

  return (
    <Badge variant="outline" className={cn("border", classes)}>
      {strategy}
    </Badge>
  );
}

function StatusBadge({
  value,
  tone,
}: {
  value: string;
  tone: "setup" | "calendar" | "event" | "job" | "outcome";
}) {
  const normalized = value.toLowerCase();
  let classes = "border-stone-200 bg-stone-100 text-stone-800";

  if (tone === "setup") {
    if (normalized.includes("favor")) {
      classes = "border-emerald-200 bg-emerald-100 text-emerald-900";
    } else if (normalized.includes("caution") || normalized.includes("neutral")) {
      classes = "border-amber-200 bg-amber-100 text-amber-900";
    }
  } else if (tone === "calendar") {
    if (normalized.includes("clean")) {
      classes = "border-emerald-200 bg-emerald-100 text-emerald-900";
    } else if (normalized.includes("penalized")) {
      classes = "border-amber-200 bg-amber-100 text-amber-900";
    } else {
      classes = "border-rose-200 bg-rose-100 text-rose-900";
    }
  } else if (tone === "event") {
    if (normalized.includes("promoted") || normalized.includes("new")) {
      classes = "border-emerald-200 bg-emerald-100 text-emerald-900";
    } else if (normalized.includes("drop")) {
      classes = "border-rose-200 bg-rose-100 text-rose-900";
    } else {
      classes = "border-amber-200 bg-amber-100 text-amber-900";
    }
  } else if (tone === "job") {
    if (normalized.includes("success") || normalized.includes("deliver") || normalized.includes("enable")) {
      classes = "border-emerald-200 bg-emerald-100 text-emerald-900";
    } else if (normalized.includes("fail")) {
      classes = "border-rose-200 bg-rose-100 text-rose-900";
    } else {
      classes = "border-amber-200 bg-amber-100 text-amber-900";
    }
  } else if (tone === "outcome") {
    if (normalized.includes("win")) {
      classes = "border-emerald-200 bg-emerald-100 text-emerald-900";
    } else if (normalized.includes("loss")) {
      classes = "border-rose-200 bg-rose-100 text-rose-900";
    } else {
      classes = "border-amber-200 bg-amber-100 text-amber-900";
    }
  }

  return (
    <Badge variant="outline" className={cn("border", classes)}>
      {value}
    </Badge>
  );
}

function TuningPanel({
  title,
  buckets,
}: {
  title: string;
  buckets: TuningBucket[];
}) {
  return (
    <Panel title={title} meta={`${buckets.length} buckets`}>
      {buckets.length ? (
        <div className="space-y-3">
          {buckets.map((bucket) => (
            <div key={bucket.bucket} className="metric-card">
              <div className="flex items-center justify-between gap-3">
                <span className="font-medium text-foreground">{bucket.bucket}</span>
                <span className="mono text-muted-foreground">{formatScore(bucket.average_latest_score)}</span>
              </div>
              <div className="mt-3 grid grid-cols-3 gap-2 text-sm">
                <StatCell label="count" value={String(bucket.count ?? 0)} />
                <StatCell label="win" value={bucket.win_rate == null ? "—" : `${Math.round(bucket.win_rate * 100)}%`} />
                <StatCell label="avg pnl" value={formatPnl(bucket.average_estimated_pnl ?? null)} />
              </div>
            </div>
          ))}
        </div>
      ) : (
        <EmptyHint message="No tuning buckets resolved for this dimension yet." />
      )}
    </Panel>
  );
}

function StatCell({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl bg-background/80 px-3 py-2">
      <div className="text-[10px] uppercase tracking-[0.18em] text-muted-foreground">{label}</div>
      <div className="mono mt-1 text-sm text-foreground">{value}</div>
    </div>
  );
}

function InspectorPanel({ selection }: { selection: InspectorSelection | null }) {
  return (
    <aside className="panel min-h-[720px] overflow-hidden">
      <div className="panel-header">
        <div>
          <div className="text-[11px] uppercase tracking-[0.22em] text-muted-foreground">inspector</div>
          <h2 className="mt-1 text-lg font-semibold text-foreground">
            {selection ? inspectorTitle(selection) : "Nothing selected"}
          </h2>
        </div>
      </div>
      <ScrollArea className="h-[calc(100vh-14rem)] min-h-[640px]">
        <div className="panel-body">
          {selection ? <InspectorContent selection={selection} /> : <EmptyHint message="Select a row from any table to inspect the current candidate, alert, job run, or session outcome." />}
        </div>
      </ScrollArea>
    </aside>
  );
}

function InspectorContent({ selection }: { selection: InspectorSelection }) {
  if (selection.kind === "candidate") {
    const candidate = selection.data.raw.candidate;
    return (
      <div className="space-y-5">
        <InspectorTopLine
          title={`${selection.data.symbol} ${selection.data.strategy}`}
          subtitle={`${selection.data.expirationDate} · ${selection.data.strikes}`}
          badges={[
            <StrategyBadge key="strategy" strategy={selection.data.strategy} />,
            <StatusBadge key="setup" value={selection.data.setupStatus} tone="setup" />,
            <StatusBadge key="calendar" value={selection.data.calendarStatus} tone="calendar" />,
          ]}
        />
        <MetricGrid
          items={[
            { label: "score", value: formatScore(selection.data.score) },
            { label: "credit", value: formatDollar(selection.data.credit) },
            { label: "dte", value: String(candidate.days_to_expiration ?? "—") },
            { label: "greeks", value: candidate.greeks_source ?? "—" },
            { label: "fill", value: candidate.fill_ratio == null ? "—" : `${Math.round(candidate.fill_ratio * 100)}%` },
            { label: "ror", value: candidate.return_on_risk == null ? "—" : `${(candidate.return_on_risk * 100).toFixed(1)}%` },
          ]}
        />
        <ReasonBlock title="Setup reasons" reasons={candidate.setup_reasons ?? []} />
        <ReasonBlock title="Calendar reasons" reasons={candidate.calendar_reasons ?? []} />
        <JsonBlock title="Raw candidate" value={candidate} />
      </div>
    );
  }

  if (selection.kind === "event") {
    return (
      <div className="space-y-5">
        <InspectorTopLine
          title={`${selection.data.raw.symbol} ${selection.data.raw.event_type}`}
          subtitle={selection.data.raw.message}
          badges={[<StatusBadge key="event" value={selection.data.raw.event_type} tone="event" />]}
        />
        {selection.data.raw.current_candidate ? (
          <ReasonBlock
            title="Current candidate setup"
            reasons={selection.data.raw.current_candidate.setup_reasons ?? []}
          />
        ) : null}
        <JsonBlock title="Event payload" value={selection.data.raw} />
      </div>
    );
  }

  if (selection.kind === "alert") {
    const payload = selection.data.raw.payload ?? {};
    const candidate = asCandidateDetail(payload);

    return (
      <div className="space-y-5">
        <InspectorTopLine
          title={`${selection.data.symbol} ${selection.data.alertType}`}
          subtitle={selection.data.description}
          badges={[
            <StatusBadge key="status" value={selection.data.status} tone="job" />,
            candidate?.strategy ? <StrategyBadge key="strategy" strategy={candidate.strategy} /> : <></>,
          ]}
        />
        {candidate ? (
          <MetricGrid
            items={[
              { label: "strikes", value: `${candidate.short_strike} / ${candidate.long_strike}` },
              { label: "score", value: formatScore(candidate.quality_score) },
              { label: "credit", value: formatDollar(candidate.midpoint_credit) },
              { label: "greeks", value: candidate.greeks_source ?? "—" },
            ]}
          />
        ) : null}
        <JsonBlock title="Alert payload" value={selection.data.raw.payload} />
        <JsonBlock title="Delivery response" value={selection.data.raw.response} />
      </div>
    );
  }

  if (selection.kind === "job") {
    return (
      <div className="space-y-5">
        <InspectorTopLine
          title={selection.data.jobType}
          subtitle={selection.data.jobKey}
          badges={[<StatusBadge key="status" value={selection.data.status} tone="job" />]}
        />
        <MetricGrid
          items={[
            { label: "started", value: formatDateTime(selection.data.startedAt) },
            { label: "finished", value: formatDateTime(selection.data.finishedAt) },
            { label: "worker", value: selection.data.workerName || "—" },
          ]}
        />
        <JsonBlock title="Payload" value={selection.data.raw.payload} />
        <JsonBlock title="Result" value={selection.data.raw.result} />
        {selection.data.raw.error_text ? <JsonBlock title="Error" value={selection.data.raw.error_text} /> : null}
      </div>
    );
  }

  const idea = selection.data.raw;
  return (
    <div className="space-y-5">
      <InspectorTopLine
        title={`${idea.underlying_symbol} ${idea.strategy}`}
        subtitle={`${idea.expiration_date} · ${idea.short_symbol} / ${idea.long_symbol}`}
        badges={[
          <StrategyBadge key="strategy" strategy={idea.strategy} />,
          <Badge key="classification" variant="outline" className="border-stone-300 bg-stone-100/80 text-stone-800">
            {idea.classification}
          </Badge>,
          <StatusBadge key="outcome" value={idea.outcome_bucket} tone="outcome" />,
        ]}
      />
      <MetricGrid
        items={[
          { label: "latest score", value: formatScore(idea.latest_score) },
          { label: "first seen", value: formatDateTime(idea.first_seen) },
          { label: "entry seen", value: formatDateTime(idea.entry_seen) },
          { label: "close pnl", value: formatPnl(idea.estimated_close_pnl) },
          { label: "verdict", value: idea.replay_verdict ?? "—" },
          { label: "greeks", value: idea.greeks_source ?? "—" },
        ]}
      />
      <ReasonBlock title="Entry setup reasons" reasons={idea.entry_candidate.setup_reasons ?? []} />
      <ReasonBlock title="Entry calendar reasons" reasons={idea.entry_candidate.calendar_reasons ?? []} />
      <JsonBlock title="Outcome payload" value={idea} />
    </div>
  );
}

function InspectorTopLine({
  title,
  subtitle,
  badges,
}: {
  title: string;
  subtitle: string;
  badges: React.ReactNode[];
}) {
  return (
    <div className="space-y-3">
      <div>
        <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">selected</div>
        <h3 className="mt-1 text-xl font-semibold tracking-tight text-foreground">{title}</h3>
        <p className="mt-2 text-sm text-muted-foreground">{subtitle}</p>
      </div>
      <div className="flex flex-wrap items-center gap-2">{badges}</div>
    </div>
  );
}

function MetricGrid({ items }: { items: Array<{ label: string; value: string }> }) {
  return (
    <div className="grid grid-cols-2 gap-3">
      {items.map((item) => (
        <div key={item.label} className="metric-card">
          <div className="text-[10px] uppercase tracking-[0.18em] text-muted-foreground">{item.label}</div>
          <div className="mt-2 text-sm font-medium text-foreground">{item.value}</div>
        </div>
      ))}
    </div>
  );
}

function ReasonBlock({ title, reasons }: { title: string; reasons: string[] }) {
  return (
    <div className="space-y-3">
      <div className="text-[11px] uppercase tracking-[0.2em] text-muted-foreground">{title}</div>
      {reasons.length ? (
        <div className="space-y-2">
          {reasons.map((reason) => (
            <div key={reason} className="rounded-2xl border border-border/70 bg-background/70 px-3 py-2 text-sm text-foreground/80">
              {reason}
            </div>
          ))}
        </div>
      ) : (
        <EmptyHint message="No reasons recorded." />
      )}
    </div>
  );
}

function JsonBlock({ title, value }: { title: string; value: unknown }) {
  return (
    <div className="space-y-3">
      <div className="text-[11px] uppercase tracking-[0.2em] text-muted-foreground">{title}</div>
      <pre className="max-h-96 overflow-auto rounded-2xl border border-border/70 bg-stone-950 px-4 py-3 font-mono text-[12px] leading-6 text-stone-100">
        {JSON.stringify(value, null, 2)}
      </pre>
    </div>
  );
}

function EmptyHint({ message }: { message: string }) {
  return (
    <div className="rounded-2xl border border-dashed border-border/80 bg-background/70 px-4 py-5 text-sm text-muted-foreground">
      {message}
    </div>
  );
}

function LoadingBlock() {
  return (
    <div className="space-y-2">
      <Skeleton className="h-10 rounded-2xl" />
      <Skeleton className="h-10 rounded-2xl" />
      <Skeleton className="h-10 rounded-2xl" />
      <Skeleton className="h-10 rounded-2xl" />
    </div>
  );
}

function buildCandidateRows(data: { board_candidates: LiveCandidate[]; watchlist_candidates: LiveCandidate[] } | undefined): CandidateTableRow[] {
  if (!data) {
    return [];
  }

  return [...data.board_candidates, ...data.watchlist_candidates].map((item) => ({
    id: `${item.bucket}:${item.candidate_id}`,
    bucket: item.bucket,
    symbol: item.underlying_symbol,
    strategy: item.strategy,
    strikes: `${item.candidate.short_strike.toFixed(0)} / ${item.candidate.long_strike.toFixed(0)}`,
    expirationDate: item.expiration_date,
    score: item.quality_score,
    credit: item.midpoint_credit,
    setupStatus: item.candidate.setup_status ?? "unknown",
    calendarStatus: item.candidate.calendar_status ?? "unknown",
    generatedAt: item.generated_at,
    raw: item,
  }));
}

function buildEventRows(events: LiveEvent[]): EventTableRow[] {
  return events.map((event) => ({
    id: `${event.generated_at}:${event.symbol}:${event.event_type}`,
    time: event.generated_at,
    symbol: event.symbol,
    eventType: event.event_type,
    message: event.message,
    raw: event,
  }));
}

function buildAlertRows(alerts: AlertRecord[]): AlertTableRow[] {
  return alerts.map((alert) => ({
    id: String(alert.alert_id),
    time: alert.created_at,
    symbol: alert.symbol,
    alertType: alert.alert_type,
    status: alert.status,
    label: alert.label,
    description: String((alert.payload as Record<string, unknown> | undefined)?.description ?? "No description"),
    raw: alert,
  }));
}

function buildJobRunRows(runs: JobRun[]): JobRunTableRow[] {
  return runs.map((run) => ({
    id: run.job_run_id,
    jobKey: run.job_key,
    jobType: run.job_type,
    status: run.status,
    startedAt: run.started_at ?? "",
    finishedAt: run.finished_at ?? "",
    workerName: run.worker_name ?? "",
    raw: run,
  }));
}

function buildSessionIdeaRows(ideas: SessionIdea[]): SessionIdeaRow[] {
  return ideas.map((idea) => ({
    id: `${idea.underlying_symbol}:${idea.short_symbol}:${idea.long_symbol}:${idea.classification}`,
    symbol: idea.underlying_symbol,
    strategy: idea.strategy,
    classification: idea.classification,
    scoreBucket: idea.score_bucket,
    latestScore: idea.latest_score,
    outcomeBucket: idea.outcome_bucket,
    replayVerdict: idea.replay_verdict ?? "pending",
    estimatedClosePnl: idea.estimated_close_pnl ?? null,
    firstSeen: idea.first_seen,
    entrySeen: idea.entry_seen,
    raw: idea,
  }));
}

function filterRows<T extends Record<string, unknown>>(rows: T[], query: string, keys: Array<keyof T>) {
  if (!query) {
    return rows;
  }

  return rows.filter((row) =>
    keys.some((key) => String(row[key] ?? "").toLowerCase().includes(query)),
  );
}

function defaultSelection({
  section,
  liveRows,
  eventRows,
  alertRows,
  jobRunRows,
  sessionIdeaRows,
}: {
  section: DashboardSection;
  liveRows: CandidateTableRow[];
  eventRows: EventTableRow[];
  alertRows: AlertTableRow[];
  jobRunRows: JobRunTableRow[];
  sessionIdeaRows: SessionIdeaRow[];
}): InspectorSelection | null {
  if (section === "live") {
    if (liveRows[0]) {
      return { section, kind: "candidate", data: liveRows[0] };
    }
    if (eventRows[0]) {
      return { section, kind: "event", data: eventRows[0] };
    }
  }

  if (section === "alerts" && alertRows[0]) {
    return { section, kind: "alert", data: alertRows[0] };
  }

  if (section === "jobs" && jobRunRows[0]) {
    return { section, kind: "job", data: jobRunRows[0] };
  }

  if (section === "sessions" && sessionIdeaRows[0]) {
    return { section, kind: "idea", data: sessionIdeaRows[0] };
  }

  return null;
}

function inspectorTitle(selection: InspectorSelection) {
  if (selection.kind === "candidate") {
    return `${selection.data.symbol} candidate`;
  }
  if (selection.kind === "event") {
    return `${selection.data.symbol} event`;
  }
  if (selection.kind === "alert") {
    return `${selection.data.symbol} alert`;
  }
  if (selection.kind === "job") {
    return selection.data.jobType;
  }
  return `${selection.data.symbol} outcome`;
}

function asCandidateDetail(payload: Record<string, unknown> | undefined) {
  const candidate = payload?.candidate;
  if (!candidate || typeof candidate !== "object") {
    return null;
  }
  return candidate as CandidateDetail;
}

function formatDateTime(value: string | undefined) {
  if (!value) {
    return "—";
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    hour12: false,
    timeZone: "America/New_York",
  }).format(parsed);
}

function formatTime(value: string | undefined) {
  if (!value) {
    return "—";
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat("en-US", {
    hour: "numeric",
    minute: "2-digit",
    hour12: false,
    timeZone: "America/New_York",
  }).format(parsed);
}

function formatDollar(value: number | undefined) {
  if (value == null || Number.isNaN(value)) {
    return "—";
  }

  return `$${value.toFixed(2)}`;
}

function formatPnl(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) {
    return "—";
  }

  const sign = value > 0 ? "+" : "";
  return `${sign}$${value.toFixed(2)}`;
}

function pnlTone(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) {
    return "text-muted-foreground";
  }
  if (value > 0) {
    return "text-emerald-700";
  }
  if (value < 0) {
    return "text-rose-700";
  }
  return "text-stone-700";
}

function formatScore(value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) {
    return "—";
  }
  return value.toFixed(1);
}
