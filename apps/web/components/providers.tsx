"use client";

import Link from "next/link";
import {
  QueryClient,
  QueryClientProvider,
  useQueryClient,
} from "@tanstack/react-query";
import { CheckCircle2, LoaderCircle, TriangleAlert, XCircle } from "lucide-react";
import { isString, startCase, take, trim } from "lodash-es";
import {
  createContext,
  useContext,
  useEffect,
  useEffectEvent,
  useRef,
  useState,
} from "react";

import { Button, buttonVariants } from "@/components/ui/button";
import {
  buildGlobalEventsWebSocketUrl,
  parseGlobalRealtimeEvent,
  type GlobalRealtimeEvent,
} from "@/lib/api";
import { cn } from "@/lib/utils";

type RealtimeConnectionState = "connecting" | "connected" | "reconnecting";
type RealtimeNoticeTone = "success" | "warning" | "error" | "info";

type RealtimeNotice = {
  id: string;
  title: string;
  body: string;
  href?: string;
  summary: string;
  timestamp: string;
  tone: RealtimeNoticeTone;
};

type RealtimeActivityContextValue = {
  connectionState: RealtimeConnectionState;
  latestSummary: string | null;
  notices: RealtimeNotice[];
  dismissNotice: (noticeId: string) => void;
};

const NOTICE_TTL_MS = 6_000;
const MAX_NOTICES = 4;

const RealtimeActivityContext = createContext<RealtimeActivityContextValue>({
  connectionState: "connecting",
  latestSummary: null,
  notices: [],
  dismissNotice: () => {},
});

function readText(value: unknown): string | undefined {
  return isString(value) && trim(value) !== "" ? trim(value) : undefined;
}

function humanizeToken(value: string): string {
  return startCase(value);
}

function buildRealtimeNotice(event: GlobalRealtimeEvent): RealtimeNotice | null {
  const payload = event.payload;

  switch (event.topic) {
    case "generator.job.updated": {
      const status = readText(payload.status);
      const symbol = readText(payload.symbol) ?? "generator";
      if (!status || !["succeeded", "no_play", "failed"].includes(status)) {
        return null;
      }
      return {
        id: `${event.topic}:${event.entity_id}:${status}`,
        title:
          status === "succeeded"
            ? `Generator ready for ${symbol}`
            : status === "no_play"
              ? `No play for ${symbol}`
              : `Generator failed for ${symbol}`,
        body:
          status === "failed"
            ? readText(payload.error_text) ?? "The generator job failed before it produced a result."
            : status === "no_play"
              ? "The job completed, but no spread survived the requested filters."
              : "A ranked generator result is ready for review.",
        href: `/generator/jobs/${event.entity_id}`,
        summary: `Generator ${symbol} ${humanizeToken(status)}`,
        timestamp: event.timestamp,
        tone: status === "succeeded" ? "success" : status === "no_play" ? "warning" : "error",
      };
    }
    case "alert.event.created": {
      const symbol = readText(payload.symbol) ?? "alert";
      const alertType = humanizeToken(readText(payload.alert_type) ?? "event");
      const status = readText(payload.status) ?? "created";
      return {
        id: `${event.topic}:${event.entity_id}`,
        title: `Alert ${humanizeToken(status)}`,
        body: `${symbol} ${alertType} was recorded in the alert feed.`,
        href: "/alerts",
        summary: `Alert ${symbol} ${humanizeToken(status)}`,
        timestamp: event.timestamp,
        tone: status === "failed" ? "error" : status === "skipped" ? "warning" : "info",
      };
    }
    case "live.cycle.updated": {
      const label = readText(payload.live_label) ?? readText(payload.label) ?? "live";
      const symbol = readText(payload.symbol) ?? "candidate";
      const bucket = readText(payload.bucket);
      const title =
        bucket === "board"
          ? `Live board updated for ${label}`
          : bucket === "watchlist"
            ? `Watchlist updated for ${label}`
            : `Live workflow updated for ${label}`;
      return {
        id: `${event.topic}:${event.entity_id}`,
        title,
        body: readText(payload.message) ?? `${symbol} was applied to the ${label} live workflow.`,
        href: "/live",
        summary: `Live ${label} updated`,
        timestamp: event.timestamp,
        tone: "info",
      };
    }
    case "job.run.updated": {
      const status = readText(payload.status);
      if (!status || !["failed", "skipped"].includes(status)) {
        return null;
      }
      const jobType = humanizeToken(readText(payload.job_type) ?? "job");
      const jobKey = readText(payload.job_key) ?? event.entity_id;
      return {
        id: `${event.topic}:${event.entity_id}:${status}`,
        title: `Job run ${humanizeToken(status)}`,
        body:
          readText(payload.error_text) ??
          `${jobType} ${status === "skipped" ? "did not run" : "reported a failure"} for ${jobKey}.`,
        href: "/jobs",
        summary: `Job ${jobType} ${humanizeToken(status)}`,
        timestamp: event.timestamp,
        tone: status === "failed" ? "error" : "warning",
      };
    }
    case "live.collector.degraded": {
      const label = readText(payload.label) ?? "live collector";
      const captureStatus = humanizeToken(readText(payload.capture_status) ?? "degraded");
      const reasons = Array.isArray(payload.reasons)
        ? payload.reasons.filter(isString).map((reason) => humanizeToken(reason))
        : [];
      const reasonText = reasons.length ? reasons.join(", ") : "Collector health degraded";
      return {
        id: `${event.topic}:${event.entity_id}:${reasonText}`,
        title: `Live collector degraded for ${label}`,
        body: `${captureStatus}. ${reasonText}.`,
        href: "/jobs",
        summary: `Live ${label} degraded`,
        timestamp: event.timestamp,
        tone: "warning",
      };
    }
    case "post_market.analysis.updated": {
      const status = readText(payload.status);
      if (!status || !["succeeded", "failed"].includes(status)) {
        return null;
      }
      const label = readText(payload.label) ?? "post-market";
      const sessionDate = readText(payload.session_date);
      return {
        id: `${event.topic}:${event.entity_id}:${status}`,
        title:
          status === "succeeded"
            ? `Post-market ready for ${label}`
            : `Post-market failed for ${label}`,
        body:
          status === "succeeded"
            ? `The ${label} analysis${sessionDate ? ` for ${sessionDate}` : ""} finished successfully.`
            : `The ${label} analysis${sessionDate ? ` for ${sessionDate}` : ""} failed.`,
        href: "/sessions",
        summary: `Post-market ${label} ${humanizeToken(status)}`,
        timestamp: event.timestamp,
        tone: status === "succeeded" ? "success" : "error",
      };
    }
    default:
      return null;
  }
}

function noticeToneClasses(tone: RealtimeNoticeTone): string {
  switch (tone) {
    case "success":
      return "border-emerald-200 bg-emerald-50 text-emerald-950";
    case "warning":
      return "border-amber-200 bg-amber-50 text-amber-950";
    case "error":
      return "border-rose-200 bg-rose-50 text-rose-950";
    default:
      return "border-stone-200 bg-stone-50 text-stone-950";
  }
}

function NoticeIcon({ tone }: { tone: RealtimeNoticeTone }) {
  if (tone === "success") {
    return <CheckCircle2 className="size-4" />;
  }
  if (tone === "warning") {
    return <TriangleAlert className="size-4" />;
  }
  if (tone === "error") {
    return <XCircle className="size-4" />;
  }
  return <LoaderCircle className="size-4" />;
}

function formatNoticeTime(timestamp: string): string {
  const parsed = new Date(timestamp);
  if (Number.isNaN(parsed.getTime())) {
    return timestamp;
  }
  return parsed.toLocaleTimeString(undefined, {
    hour: "numeric",
    minute: "2-digit",
  });
}

function GlobalRealtimeBridge({
  onConnectionStateChange,
  onNotice,
}: {
  onConnectionStateChange: (state: RealtimeConnectionState) => void;
  onNotice: (notice: RealtimeNotice) => void;
}) {
  const queryClient = useQueryClient();
  const reconnectTimerRef = useRef<number | null>(null);

  const handleRealtimeEvent = useEffectEvent((payload: string) => {
    const realtimeEvent = parseGlobalRealtimeEvent(payload);
    switch (realtimeEvent.topic) {
      case "generator.job.updated":
        queryClient.invalidateQueries({ queryKey: ["generator-jobs"] });
        queryClient.invalidateQueries({ queryKey: ["generator-job", realtimeEvent.entity_id] });
        break;
      case "alert.event.created":
        queryClient.invalidateQueries({ queryKey: ["alerts-latest"] });
        break;
      case "live.cycle.updated":
        queryClient.invalidateQueries({ queryKey: ["live"] });
        queryClient.invalidateQueries({ queryKey: ["live-events"] });
        break;
      case "job.run.updated":
        queryClient.invalidateQueries({ queryKey: ["jobs"] });
        queryClient.invalidateQueries({ queryKey: ["job-runs"] });
        queryClient.invalidateQueries({ queryKey: ["jobs-health"] });
        break;
      case "live.collector.degraded":
        queryClient.invalidateQueries({ queryKey: ["job-runs"] });
        queryClient.invalidateQueries({ queryKey: ["jobs-health"] });
        break;
      case "post_market.analysis.updated":
        queryClient.invalidateQueries({ queryKey: ["session-summary"] });
        queryClient.invalidateQueries({ queryKey: ["session-tuning"] });
        break;
      default:
        break;
    }

    const notice = buildRealtimeNotice(realtimeEvent);
    if (notice) {
      onNotice(notice);
    }
  });

  useEffect(() => {
    let closedByCleanup = false;
    let socket: WebSocket | null = null;

    onConnectionStateChange("connecting");

    const connect = () => {
      socket = new WebSocket(buildGlobalEventsWebSocketUrl());
      socket.onopen = () => {
        onConnectionStateChange("connected");
      };
      socket.onmessage = (event) => {
        if (typeof event.data === "string") {
          handleRealtimeEvent(event.data);
        }
      };
      socket.onclose = () => {
        if (closedByCleanup) {
          return;
        }
        onConnectionStateChange("reconnecting");
        reconnectTimerRef.current = window.setTimeout(connect, 1500);
      };
    };

    connect();

    return () => {
      closedByCleanup = true;
      if (reconnectTimerRef.current !== null) {
        window.clearTimeout(reconnectTimerRef.current);
      }
      socket?.close();
    };
  }, [onConnectionStateChange]);

  return null;
}

function ShellActivityToasts() {
  const { notices, dismissNotice } = useRealtimeActivity();

  if (!notices.length) {
    return null;
  }

  return (
    <div className="pointer-events-none fixed right-4 bottom-4 z-50 flex w-[min(380px,calc(100vw-2rem))] flex-col gap-2">
      {notices.map((notice) => (
        <div
          key={notice.id}
          className={cn(
            "pointer-events-auto rounded-2xl border px-4 py-3 shadow-[0_24px_60px_-28px_rgba(15,23,42,0.45)] backdrop-blur",
            noticeToneClasses(notice.tone),
          )}
        >
          <div className="flex items-start gap-3">
            <div className="mt-0.5 shrink-0">
              <NoticeIcon tone={notice.tone} />
            </div>
            <div className="min-w-0 flex-1">
              <div className="flex items-start justify-between gap-2">
                <div>
                  <div className="text-[11px] uppercase tracking-[0.18em] opacity-70">
                    {formatNoticeTime(notice.timestamp)}
                  </div>
                  <div className="mt-1 text-sm font-medium">{notice.title}</div>
                </div>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon-xs"
                  className="text-current"
                  onClick={() => dismissNotice(notice.id)}
                >
                  <XCircle className="size-3.5" />
                </Button>
              </div>
              <p className="mt-2 text-sm opacity-85">{notice.body}</p>
              {notice.href ? (
                <div className="mt-3">
                  <Link href={notice.href} className={buttonVariants({ variant: "outline", size: "xs" })}>
                    Open
                  </Link>
                </div>
              ) : null}
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}

export function useRealtimeActivity() {
  return useContext(RealtimeActivityContext);
}

export function Providers({ children }: { children: React.ReactNode }) {
  const [queryClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            retry: 1,
            staleTime: 30_000,
            refetchOnWindowFocus: false,
          },
        },
      }),
  );
  const [connectionState, setConnectionState] = useState<RealtimeConnectionState>("connecting");
  const [latestSummary, setLatestSummary] = useState<string | null>(null);
  const [notices, setNotices] = useState<RealtimeNotice[]>([]);
  const seenNoticeIdsRef = useRef<Set<string>>(new Set());
  const noticeTimersRef = useRef<Map<string, number>>(new Map());

  const dismissNotice = (noticeId: string) => {
    const timer = noticeTimersRef.current.get(noticeId);
    if (timer !== undefined) {
      window.clearTimeout(timer);
      noticeTimersRef.current.delete(noticeId);
    }
    setNotices((current) => current.filter((notice) => notice.id !== noticeId));
  };

  useEffect(() => {
    const timers = noticeTimersRef.current;
    return () => {
      for (const timer of timers.values()) {
        window.clearTimeout(timer);
      }
      timers.clear();
    };
  }, []);

  const pushNotice = (notice: RealtimeNotice) => {
    if (seenNoticeIdsRef.current.has(notice.id)) {
      return;
    }
    seenNoticeIdsRef.current.add(notice.id);
    setLatestSummary(notice.summary);
    setNotices((current) => take([notice, ...current], MAX_NOTICES));
    const timer = window.setTimeout(() => dismissNotice(notice.id), NOTICE_TTL_MS);
    noticeTimersRef.current.set(notice.id, timer);
  };

  return (
    <QueryClientProvider client={queryClient}>
      <RealtimeActivityContext.Provider
        value={{
          connectionState,
          latestSummary,
          notices,
          dismissNotice,
        }}
      >
        <GlobalRealtimeBridge
          onConnectionStateChange={setConnectionState}
          onNotice={pushNotice}
        />
        {children}
        <ShellActivityToasts />
      </RealtimeActivityContext.Provider>
    </QueryClientProvider>
  );
}
