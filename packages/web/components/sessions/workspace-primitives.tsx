import type { ReactNode } from "react";

import { trim } from "lodash-es";

import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import {
  formatCalendarDate,
  formatElapsedDuration,
  formatLocalDateTime,
  formatLocalTime,
} from "@/lib/date";
import { cn } from "@/lib/utils";

export function readNumber(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }
  return fallback;
}

export function readString(value: unknown, fallback = "—"): string {
  return typeof value === "string" && trim(value) !== "" ? value : fallback;
}

export function formatDate(value: string | null | undefined): string {
  return formatCalendarDate(value);
}

export function formatTime(value: string | null | undefined): string {
  return formatLocalTime(value);
}

export function formatTimestamp(value: string | null | undefined): string {
  return formatLocalDateTime(value);
}

export function formatCurrency(value: number): string {
  return new Intl.NumberFormat(undefined, {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

export function formatNullableCurrency(
  value: number | null | undefined,
): string {
  return value == null ? "—" : formatCurrency(value);
}

export function formatSignedCurrency(
  value: number | null | undefined,
): string {
  if (value == null) {
    return "—";
  }
  return new Intl.NumberFormat(undefined, {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
    signDisplay: "exceptZero",
  }).format(value);
}

export function formatScore(value: number): string {
  return value.toFixed(1);
}

export function formatQuantity(value: number | null | undefined): string {
  if (value == null) {
    return "—";
  }
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: 2,
  }).format(value);
}

export function formatSignedPercent(
  value: number | null | undefined,
): string {
  if (value == null) {
    return "—";
  }
  const percent = value * 100;
  return `${percent > 0 ? "+" : ""}${percent.toFixed(2)}%`;
}

export function formatDuration(raw: {
  started_at?: string | null;
  finished_at?: string | null;
}): string {
  return formatElapsedDuration(raw.started_at, raw.finished_at);
}

function statusTone(value: string): string {
  switch (value) {
    case "running":
      return "border-sky-200 bg-sky-100 text-sky-900 dark:border-sky-900/80 dark:bg-sky-950/55 dark:text-sky-100";
    case "healthy":
    case "succeeded":
      return "border-emerald-200 bg-emerald-100 text-emerald-900 dark:border-emerald-900/80 dark:bg-emerald-950/55 dark:text-emerald-100";
    case "degraded":
    case "skipped":
      return "border-amber-200 bg-amber-100 text-amber-900 dark:border-amber-900/80 dark:bg-amber-950/55 dark:text-amber-100";
    case "failed":
    case "empty":
      return "border-rose-200 bg-rose-100 text-rose-900 dark:border-rose-900/80 dark:bg-rose-950/55 dark:text-rose-100";
    default:
      return "border-border/70 bg-card text-foreground";
  }
}

function captureTone(value: string): string {
  switch (value) {
    case "healthy":
      return "border-emerald-200 bg-emerald-100 text-emerald-900 dark:border-emerald-900/80 dark:bg-emerald-950/55 dark:text-emerald-100";
    case "baseline_only":
      return "border-amber-200 bg-amber-100 text-amber-900 dark:border-amber-900/80 dark:bg-amber-950/55 dark:text-amber-100";
    case "recovery_only":
      return "border-orange-200 bg-orange-100 text-orange-900 dark:border-orange-900/80 dark:bg-orange-950/55 dark:text-orange-100";
    case "empty":
      return "border-rose-200 bg-rose-100 text-rose-900 dark:border-rose-900/80 dark:bg-rose-950/55 dark:text-rose-100";
    default:
      return "border-border/70 bg-card text-foreground";
  }
}

export function valueTone(value: number | null | undefined): string {
  if (value == null) {
    return "text-muted-foreground";
  }
  if (value > 0) {
    return "text-emerald-700 dark:text-emerald-300";
  }
  if (value < 0) {
    return "text-rose-700 dark:text-rose-300";
  }
  return "text-foreground/80";
}

export function SessionStatusBadge({
  value,
}: {
  value: string | null | undefined;
}) {
  const resolved = readString(value, "idle");
  return (
    <Badge
      variant="outline"
      className={cn(
        "rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em]",
        statusTone(resolved),
      )}
    >
      {resolved.replaceAll("_", " ")}
    </Badge>
  );
}

export function CaptureStatusBadge({
  value,
}: {
  value: string | null | undefined;
}) {
  const resolved = readString(value, "unknown");
  return (
    <Badge
      variant="outline"
      className={cn(
        "rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em]",
        captureTone(resolved),
      )}
    >
      {resolved.replaceAll("_", " ")}
    </Badge>
  );
}

function tradeabilityTone(value: string): string {
  switch (value) {
    case "live_ready":
      return "border-emerald-200 bg-emerald-100 text-emerald-900 dark:border-emerald-900/80 dark:bg-emerald-950/55 dark:text-emerald-100";
    case "degraded_quotes":
      return "border-amber-200 bg-amber-100 text-amber-900 dark:border-amber-900/80 dark:bg-amber-950/55 dark:text-amber-100";
    case "recovery_only":
      return "border-sky-200 bg-sky-100 text-sky-900 dark:border-sky-900/80 dark:bg-sky-950/55 dark:text-sky-100";
    case "research_only":
      return "border-stone-200 bg-stone-100 text-stone-900 dark:border-stone-700 dark:bg-stone-800 dark:text-stone-100";
    default:
      return "border-border/70 bg-card text-foreground";
  }
}

export function TradeabilityBadge({
  value,
}: {
  value: string | null | undefined;
}) {
  const resolved = readString(value, "unknown");
  return (
    <Badge
      variant="outline"
      className={cn(
        "rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em]",
        tradeabilityTone(resolved),
      )}
    >
      {resolved.replaceAll("_", " ")}
    </Badge>
  );
}

function autoExecutionTone(value: string): string {
  switch (value) {
    case "submitted":
      return "border-emerald-200 bg-emerald-100 text-emerald-900 dark:border-emerald-900/80 dark:bg-emerald-950/55 dark:text-emerald-100";
    case "skipped":
      return "border-amber-200 bg-amber-100 text-amber-900 dark:border-amber-900/80 dark:bg-amber-950/55 dark:text-amber-100";
    case "blocked":
      return "border-rose-200 bg-rose-100 text-rose-900 dark:border-rose-900/80 dark:bg-rose-950/55 dark:text-rose-100";
    default:
      return "border-border/70 bg-card text-foreground";
  }
}

export function AutoExecutionStatusBadge({
  value,
}: {
  value: string | null | undefined;
}) {
  const resolved = readString(value, "unknown");
  return (
    <Badge
      variant="outline"
      className={cn(
        "rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em]",
        autoExecutionTone(resolved),
      )}
    >
      {resolved.replaceAll("_", " ")}
    </Badge>
  );
}

function executionTone(value: string): string {
  switch (value) {
    case "filled":
      return "border-emerald-200 bg-emerald-100 text-emerald-900 dark:border-emerald-900/80 dark:bg-emerald-950/55 dark:text-emerald-100";
    case "partially_filled":
      return "border-sky-200 bg-sky-100 text-sky-900 dark:border-sky-900/80 dark:bg-sky-950/55 dark:text-sky-100";
    case "canceled":
    case "done_for_day":
    case "expired":
      return "border-amber-200 bg-amber-100 text-amber-900 dark:border-amber-900/80 dark:bg-amber-950/55 dark:text-amber-100";
    case "failed":
    case "rejected":
      return "border-rose-200 bg-rose-100 text-rose-900 dark:border-rose-900/80 dark:bg-rose-950/55 dark:text-rose-100";
    default:
      return "border-border/70 bg-card text-foreground";
  }
}

export function ExecutionStatusBadge({
  value,
}: {
  value: string | null | undefined;
}) {
  const resolved = readString(value, "unknown");
  return (
    <Badge
      variant="outline"
      className={cn(
        "rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em]",
        executionTone(resolved),
      )}
    >
      {resolved.replaceAll("_", " ")}
    </Badge>
  );
}

function portfolioTone(value: string): string {
  switch (value) {
    case "open":
      return "border-emerald-200 bg-emerald-100 text-emerald-900 dark:border-emerald-900/80 dark:bg-emerald-950/55 dark:text-emerald-100";
    case "partial_close":
    case "partial_open":
      return "border-sky-200 bg-sky-100 text-sky-900 dark:border-sky-900/80 dark:bg-sky-950/55 dark:text-sky-100";
    case "closed":
      return "border-stone-200 bg-stone-100 text-stone-900 dark:border-stone-700 dark:bg-stone-800 dark:text-stone-100";
    default:
      return "border-border/70 bg-card text-foreground";
  }
}

export function PortfolioStatusBadge({
  value,
}: {
  value: string | null | undefined;
}) {
  const resolved = readString(value, "unknown");
  return (
    <Badge
      variant="outline"
      className={cn(
        "rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em]",
        portfolioTone(resolved),
      )}
    >
      {resolved.replaceAll("_", " ")}
    </Badge>
  );
}

function riskTone(value: string): string {
  switch (value) {
    case "ok":
      return "border-emerald-200 bg-emerald-100 text-emerald-900 dark:border-emerald-900/80 dark:bg-emerald-950/55 dark:text-emerald-100";
    case "disabled":
      return "border-amber-200 bg-amber-100 text-amber-900 dark:border-amber-900/80 dark:bg-amber-950/55 dark:text-amber-100";
    case "breach":
    case "blocked":
      return "border-rose-200 bg-rose-100 text-rose-900 dark:border-rose-900/80 dark:bg-rose-950/55 dark:text-rose-100";
    default:
      return "border-border/70 bg-card text-foreground";
  }
}

export function RiskStatusBadge({
  value,
}: {
  value: string | null | undefined;
}) {
  const resolved = readString(value, "unknown");
  return (
    <Badge
      variant="outline"
      className={cn(
        "rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em]",
        riskTone(resolved),
      )}
    >
      {resolved.replaceAll("_", " ")}
    </Badge>
  );
}

function reconciliationTone(value: string): string {
  switch (value) {
    case "matched":
      return "border-emerald-200 bg-emerald-100 text-emerald-900 dark:border-emerald-900/80 dark:bg-emerald-950/55 dark:text-emerald-100";
    case "pending":
      return "border-amber-200 bg-amber-100 text-amber-900 dark:border-amber-900/80 dark:bg-amber-950/55 dark:text-amber-100";
    case "mismatch":
      return "border-rose-200 bg-rose-100 text-rose-900 dark:border-rose-900/80 dark:bg-rose-950/55 dark:text-rose-100";
    case "clear":
      return "border-stone-200 bg-stone-100 text-stone-900 dark:border-stone-700 dark:bg-stone-800 dark:text-stone-100";
    default:
      return "border-border/70 bg-card text-foreground";
  }
}

export function ReconciliationStatusBadge({
  value,
}: {
  value: string | null | undefined;
}) {
  const resolved = readString(value, "unknown");
  return (
    <Badge
      variant="outline"
      className={cn(
        "rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em]",
        reconciliationTone(resolved),
      )}
    >
      {resolved.replaceAll("_", " ")}
    </Badge>
  );
}

export function AccountEnvironmentBadge({
  value,
}: {
  value: string | null | undefined;
}) {
  const resolved = readString(value, "custom");
  const tone =
    resolved === "paper"
      ? "border-sky-200 bg-sky-100 text-sky-900 dark:border-sky-900/80 dark:bg-sky-950/55 dark:text-sky-100"
      : resolved === "live"
        ? "border-amber-200 bg-amber-100 text-amber-900 dark:border-amber-900/80 dark:bg-amber-950/55 dark:text-amber-100"
        : "border-border/70 bg-card text-foreground";
  return (
    <Badge
      variant="outline"
      className={cn(
        "rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em]",
        tone,
      )}
    >
      {resolved}
    </Badge>
  );
}

export function MetricTile({
  label,
  value,
  note,
}: {
  label: string;
  value: string;
  note?: string;
}) {
  return (
    <div className="min-w-0 rounded-2xl border border-border/70 bg-card/80 px-4 py-3">
      <div className="break-words text-[11px] uppercase tracking-[0.2em] text-muted-foreground">
        {label}
      </div>
      <div className="mt-2 min-w-0 break-words text-lg leading-tight font-semibold sm:text-xl">
        {value}
      </div>
      {note ? (
        <div className="mt-1 break-words text-xs leading-snug text-muted-foreground">
          {note}
        </div>
      ) : null}
    </div>
  );
}

export function SectionSurface({
  title,
  description,
  children,
}: {
  title: string;
  description?: string;
  children: ReactNode;
}) {
  return (
    <section className="rounded-[28px] border border-border/70 bg-card/80 shadow-[0_30px_90px_-54px_rgba(15,23,42,0.55)]">
      <div className="border-b border-border/70 px-5 py-4">
        <div className="text-[11px] uppercase tracking-[0.22em] text-muted-foreground">
          {title}
        </div>
        {description ? (
          <div className="mt-1 text-sm text-foreground/70">{description}</div>
        ) : null}
      </div>
      <div className="px-4 py-4 md:px-5">{children}</div>
    </section>
  );
}

export function LoadingState() {
  return (
    <div className="flex flex-col gap-4">
      <Skeleton className="h-48 w-full rounded-[28px]" />
      <Skeleton className="h-72 w-full rounded-[28px]" />
      <Skeleton className="h-72 w-full rounded-[28px]" />
    </div>
  );
}
