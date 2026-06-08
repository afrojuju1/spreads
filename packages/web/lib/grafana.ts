"use client";

import { useMemo, useSyncExternalStore } from "react";

const DEFAULT_GRAFANA_PORT = "33000";
const TRADING_LOGS_DASHBOARD_PATH = "/d/trading-logs/trading-logs";

export function grafanaBaseUrl(): string {
  const configured = process.env.NEXT_PUBLIC_GRAFANA_URL;
  if (configured) {
    return configured.replace(/\/$/, "");
  }
  if (typeof window === "undefined") {
    return `http://127.0.0.1:${DEFAULT_GRAFANA_PORT}`;
  }
  return `${window.location.protocol}//${window.location.hostname}:${DEFAULT_GRAFANA_PORT}`;
}

function grafanaHydrationBaseUrl(): string {
  const configured = process.env.NEXT_PUBLIC_GRAFANA_URL;
  if (configured) {
    return configured.replace(/\/$/, "");
  }
  return `http://127.0.0.1:${DEFAULT_GRAFANA_PORT}`;
}

function subscribeGrafanaBaseUrl(): () => void {
  return () => {};
}

export function grafanaTradingLogsUrl(params?: {
  from?: string;
  to?: string;
}, baseUrl = grafanaBaseUrl()): string {
  const url = new URL(TRADING_LOGS_DASHBOARD_PATH, baseUrl);
  url.searchParams.set("orgId", "1");
  url.searchParams.set("from", params?.from ?? "now-2h");
  url.searchParams.set("to", params?.to ?? "now");
  return url.toString();
}

export function useGrafanaTradingLogsUrl(params?: {
  from?: string;
  to?: string;
}): string {
  const baseUrl = useSyncExternalStore(
    subscribeGrafanaBaseUrl,
    grafanaBaseUrl,
    grafanaHydrationBaseUrl,
  );
  const from = params?.from;
  const to = params?.to;

  return useMemo(
    () => grafanaTradingLogsUrl({ from, to }, baseUrl),
    [baseUrl, from, to],
  );
}
