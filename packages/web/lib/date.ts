import { differenceInSeconds, format, isValid, parseISO } from "date-fns";

const FALLBACK_TEXT = "—";

export function parseDateValue(value: string | null | undefined): Date | null {
  if (!value) {
    return null;
  }

  const parsed = parseISO(value);
  return isValid(parsed) ? parsed : null;
}

export function formatCalendarDate(
  value: string | null | undefined,
  pattern = "MMM d, yyyy",
): string {
  if (!value) {
    return FALLBACK_TEXT;
  }

  const parsed = parseDateValue(value);
  return parsed ? format(parsed, pattern) : value;
}

export function formatLocalTime(value: string | null | undefined, pattern = "h:mm a"): string {
  if (!value) {
    return FALLBACK_TEXT;
  }

  const parsed = parseDateValue(value);
  return parsed ? format(parsed, pattern) : value;
}

export function formatLocalDateTime(
  value: string | null | undefined,
  pattern = "MMM d, h:mm a",
): string {
  if (!value) {
    return FALLBACK_TEXT;
  }

  const parsed = parseDateValue(value);
  return parsed ? format(parsed, pattern) : value;
}

export function formatElapsedDuration(
  startedAt: string | null | undefined,
  finishedAt: string | null | undefined,
): string {
  const started = parseDateValue(startedAt);
  const finished = parseDateValue(finishedAt);
  if (!started || !finished) {
    return FALLBACK_TEXT;
  }

  const seconds = Math.max(differenceInSeconds(finished, started), 0);
  if (seconds < 60) {
    return `${seconds}s`;
  }

  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  return remainder === 0 ? `${minutes}m` : `${minutes}m ${remainder}s`;
}
