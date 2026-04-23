"use client";

import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { startCase } from "lodash-es";
import { Bot, RefreshCw } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import {
  buildOpportunitiesHref,
  buildPositionsHref,
  buildPipelineHref,
  getRuntimeDetail,
} from "@/lib/api";
import {
  formatDate,
  formatNullableCurrency,
  formatQuantity,
  formatTimestamp,
  LoadingState,
  MetricTile,
  readString,
  SectionSurface,
} from "@/components/operator/operator-primitives";

type AutomationDetailPageContentProps = {
  botId: string;
  automationId: string;
  marketDate?: string;
};

type DetailRecord = Record<string, unknown>;

function readRecord(value: unknown): DetailRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as DetailRecord)
    : {};
}

function readOptionalNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function humanizeToken(value: string | null | undefined, fallback = "—"): string {
  const resolved = readString(value, fallback);
  return resolved === fallback ? fallback : startCase(resolved.replaceAll("_", " "));
}

function formatDetailValue(key: string, value: unknown): string {
  if (typeof value === "boolean") {
    return value ? "Yes" : "No";
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    if (key === "daily_loss_limit") {
      return formatNullableCurrency(value);
    }
    return formatQuantity(value);
  }
  if (typeof value === "string") {
    if (/^\d{2}:\d{2}$/.test(value)) {
      return `${value} ET`;
    }
    if (value.includes("_")) {
      return humanizeToken(value);
    }
    return value;
  }
  if (Array.isArray(value)) {
    const strings = value.filter(
      (item): item is string => typeof item === "string" && item.trim() !== "",
    );
    return strings.length ? strings.join(", ") : "—";
  }
  return "Configured";
}

function buildDetailItems(
  record: DetailRecord,
  preferredOrder: string[] = [],
): Array<{ label: string; value: string }> {
  const entries = Object.entries(record).filter(([, value]) => value != null);
  const orderedEntries = [
    ...preferredOrder.flatMap((key) =>
      key in record && record[key] != null ? [[key, record[key]] as const] : [],
    ),
    ...entries
      .filter(([key]) => !preferredOrder.includes(key))
      .sort(([left], [right]) => left.localeCompare(right)),
  ];

  return orderedEntries.map(([key, value]) => ({
    label: startCase(key.replaceAll("_", " ")),
    value: formatDetailValue(key, value),
  }));
}

function DetailField({
  label,
  value,
  note,
  mono = false,
}: {
  label: string;
  value: string;
  note?: string;
  mono?: boolean;
}) {
  return (
    <div className="rounded-2xl border border-border/70 bg-background/70 p-4">
      <div className="text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
        {label}
      </div>
      <div
        className={`mt-2 text-sm text-foreground/90 ${mono ? "font-mono text-[12px]" : ""}`}
      >
        {value}
      </div>
      {note ? (
        <div className="mt-2 text-xs leading-5 text-muted-foreground">{note}</div>
      ) : null}
    </div>
  );
}

export function AutomationDetailPageContent({
  botId,
  automationId,
  marketDate,
}: AutomationDetailPageContentProps) {
  const detailQuery = useQuery({
    queryKey: ["runtimes", botId, automationId, marketDate ?? ""],
    queryFn: () =>
      getRuntimeDetail(botId, automationId, { marketDate, limit: 200 }),
  });

  if (detailQuery.isLoading) {
    return <LoadingState />;
  }

  const detail = detailQuery.data;
  if (detailQuery.isError || !detail) {
    return (
      <div className="flex flex-col gap-4">
        <div className="app-tone-error rounded-2xl border px-4 py-3 text-sm">
          Runtime detail could not be loaded.
        </div>
        <Link href="/runtimes" className={buttonVariants({ variant: "outline" })}>
          Back to runtimes
        </Link>
      </div>
    );
  }

  const schedule = readRecord(detail.schedule);
  const triggerPolicy = readRecord(detail.trigger_policy);
  const config = readRecord(detail.config);
  const configHash = readString(detail.config_hash, readString(config.config_hash, ""));
  const discoveryHref = detail.latest_discovery?.pipeline_id
    ? buildPipelineHref(
        detail.latest_discovery.pipeline_id,
        detail.latest_discovery.session_date ?? detail.market_date ?? null,
      )
    : null;
  const runtimeControlItems = [
    {
      label: "Live",
      value: detail.live_enabled ? "Enabled" : "Disabled",
      note: detail.live_enabled
        ? "Runtime can participate in live decisions."
        : "Runtime is currently disabled for live decisions.",
    },
    {
      label: "Execution",
      value: humanizeToken(detail.execution_mode, "Unknown"),
      note: "Current routing mode for execution attempts.",
    },
    {
      label: "Approval",
      value: humanizeToken(detail.approval_mode, "Unknown"),
      note: "Approval gate applied before orders are submitted.",
    },
    {
      label: "Open Cap",
      value:
        readOptionalNumber(detail.max_open_positions) == null
          ? "—"
          : formatQuantity(detail.max_open_positions),
      note: "Maximum concurrent open positions.",
    },
    {
      label: "Daily Cap",
      value:
        readOptionalNumber(detail.max_daily_actions) == null
          ? "—"
          : formatQuantity(detail.max_daily_actions),
      note: "Maximum actions allowed per day.",
    },
    {
      label: "New Entries",
      value:
        readOptionalNumber(detail.max_new_entries_per_day) == null
          ? "—"
          : formatQuantity(detail.max_new_entries_per_day),
      note: "New entries allowed per day.",
    },
    {
      label: "Loss Limit",
      value: formatNullableCurrency(detail.daily_loss_limit),
      note: "Daily stop level for this runtime.",
    },
  ];
  const scheduleItems = buildDetailItems(schedule, [
    "cadence",
    "market_hours_only",
    "start_time_et",
    "end_time_et",
  ]);
  const triggerPolicyItems = buildDetailItems(triggerPolicy, [
    "min_opportunity_score",
  ]);

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
                <Bot data-icon="inline-start" />
                Runtime
              </Badge>
              <Badge variant="outline">{humanizeToken(detail.automation_type)}</Badge>
              <Badge variant="outline">
                {humanizeToken(detail.execution_mode, "unknown")}
              </Badge>
              <Badge variant="outline">
                {detail.live_enabled ? "live enabled" : "live disabled"}
              </Badge>
              <Badge variant="outline">
                {humanizeToken(detail.approval_mode, "unknown")} approval
              </Badge>
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">
              {detail.bot_name}
            </div>
            <div className="mt-2 text-sm text-foreground/70">
              {detail.automation_id} ·{" "}
              {humanizeToken(detail.strategy_family, "unscoped")}. Use this
              runtime for owner-scoped decisions, intents, positions, and linked
              diagnostics context.
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
            <Link href="/runtimes" className={buttonVariants({ variant: "outline" })}>
              All runtimes
            </Link>
          </div>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <MetricTile
          label="Live opportunities"
          value={String(detail.live_opportunity_count)}
          note={`Total ${detail.opportunity_count}`}
        />
        <MetricTile
          label="Decisions"
          value={String(detail.decision_count)}
          note={`Selected ${detail.decision_state_counts.selected ?? 0}`}
        />
        <MetricTile
          label="Open positions"
          value={String(detail.open_position_count)}
          note={formatNullableCurrency(detail.open_unrealized_pnl)}
        />
        <MetricTile
          label="Daily PnL"
          value={formatNullableCurrency(detail.daily_total_pnl)}
          note={formatNullableCurrency(detail.daily_realized_pnl)}
        />
        <MetricTile
          label="Diagnostics"
          value={detail.latest_discovery?.label ?? "—"}
          note={
            detail.latest_discovery?.session_date
              ? formatDate(detail.latest_discovery.session_date)
              : "No linked diagnostics"
          }
        />
      </div>

      <SectionSurface
        title="Runtime Controls"
        description="Operator-facing limits, cadence, planner gates, and canonical links for this runtime."
      >
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          {runtimeControlItems.map((item) => (
            <MetricTile
              key={item.label}
              label={item.label}
              value={item.value}
              note={item.note}
            />
          ))}
        </div>

        <div className="mt-4 grid gap-4 xl:grid-cols-2">
          <div className="rounded-[24px] border border-border/70 bg-background/45 p-4">
            <div className="text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
              Schedule
            </div>
            {scheduleItems.length ? (
              <div className="mt-3 grid gap-3 sm:grid-cols-2">
                {scheduleItems.map((item) => (
                  <DetailField
                    key={item.label}
                    label={item.label}
                    value={item.value}
                  />
                ))}
              </div>
            ) : (
              <div className="mt-3 text-sm text-muted-foreground">
                No schedule was configured.
              </div>
            )}
          </div>

          <div className="rounded-[24px] border border-border/70 bg-background/45 p-4">
            <div className="text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
              Trigger Policy
            </div>
            {triggerPolicyItems.length ? (
              <div className="mt-3 grid gap-3 sm:grid-cols-2">
                {triggerPolicyItems.map((item) => (
                  <DetailField
                    key={item.label}
                    label={item.label}
                    value={item.value}
                  />
                ))}
              </div>
            ) : (
              <div className="mt-3 text-sm text-muted-foreground">
                No trigger policy was configured.
              </div>
            )}
          </div>
        </div>

        <div className="mt-4 grid gap-3 lg:grid-cols-3">
          <DetailField
            label="Universe"
            value={
              detail.symbols.length
                ? `${detail.symbols.length} symbols`
                : "No symbols"
            }
            note={detail.symbols.length ? detail.symbols.join(", ") : undefined}
          />
          <DetailField
            label="Strategy Config"
            value={readString(detail.strategy_config_id)}
          />
          <DetailField
            label="Latest Runtime Run"
            value={
              detail.latest_automation_run?.started_at
                ? formatTimestamp(String(detail.latest_automation_run.started_at))
                : "No recent run"
            }
            note={humanizeToken(readString(detail.latest_automation_run?.status, "unknown"))}
          />
          <DetailField
            label="Latest Diagnostics"
            value={
              detail.latest_discovery?.label
                ? `${detail.latest_discovery.label}${detail.latest_discovery.session_date ? ` · ${detail.latest_discovery.session_date}` : ""}`
                : "No diagnostics linkage"
            }
          />
          <DetailField
            label="Config Hash"
            value={configHash ? configHash.slice(0, 12) : "—"}
            note={configHash || undefined}
            mono={Boolean(configHash)}
          />
        </div>
        {discoveryHref ? (
          <div className="mt-4 flex flex-wrap gap-2">
            <Link href={discoveryHref} className={buttonVariants({ variant: "outline" })}>
              Open linked diagnostics
            </Link>
            <Link
              href={buildOpportunitiesHref({
                marketDate: detail.market_date ?? null,
                botId: detail.bot_id,
                automationId: detail.automation_id,
                strategyConfigId: detail.strategy_config_id ?? null,
              })}
              className={buttonVariants({ variant: "outline" })}
            >
              Open filtered opportunities
            </Link>
            <Link
              href={buildPositionsHref({
                marketDate: detail.market_date ?? null,
                botId: detail.bot_id,
                automationId: detail.automation_id,
                strategyConfigId: detail.strategy_config_id ?? null,
              })}
              className={buttonVariants({ variant: "outline" })}
            >
              Open filtered positions
            </Link>
          </div>
        ) : (
          <div className="mt-4 flex flex-wrap gap-2">
            <Link
              href={buildOpportunitiesHref({
                marketDate: detail.market_date ?? null,
                botId: detail.bot_id,
                automationId: detail.automation_id,
                strategyConfigId: detail.strategy_config_id ?? null,
              })}
              className={buttonVariants({ variant: "outline" })}
            >
              Open filtered opportunities
            </Link>
            <Link
              href={buildPositionsHref({
                marketDate: detail.market_date ?? null,
                botId: detail.bot_id,
                automationId: detail.automation_id,
                strategyConfigId: detail.strategy_config_id ?? null,
              })}
              className={buttonVariants({ variant: "outline" })}
            >
              Open filtered positions
            </Link>
          </div>
        )}
      </SectionSurface>
    </div>
  );
}
