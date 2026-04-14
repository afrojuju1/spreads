"use client";

import Link from "next/link";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";
import { ChevronDown, ChevronRight, Radar, RefreshCw, TriangleAlert } from "lucide-react";
import { useState, type ReactNode } from "react";

import { DataTable } from "@/components/data-table";
import {
  formatDate,
  formatNullableCurrency,
  formatQuantity,
  formatScore,
  formatTimestamp,
  LoadingState,
  MetricTile,
  readNumber,
  readString,
  SectionSurface,
} from "@/components/sessions/workspace-primitives";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  buildPipelineHref,
  executeOpportunity,
  getOpportunities,
  type Opportunity,
} from "@/lib/api";
import { parseDateValue } from "@/lib/date";
import { cn } from "@/lib/utils";

function readRecord(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function readStringList(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.flatMap((item) =>
    typeof item === "string" && item.trim() !== "" ? [item] : [],
  );
}

function humanize(value: string | null | undefined, fallback = "—"): string {
  return readString(value, fallback).replaceAll("_", " ");
}

function opportunityRecord(opportunity: Opportunity): Record<string, unknown> {
  return opportunity as Record<string, unknown>;
}

function getOpportunityCandidate(opportunity: Opportunity): Record<string, unknown> {
  return readRecord(opportunityRecord(opportunity).candidate);
}

function getOpportunityRiskHints(opportunity: Opportunity): Record<string, unknown> {
  return readRecord(opportunityRecord(opportunity).risk_hints);
}

function getOpportunityExecutionShape(opportunity: Opportunity): Record<string, unknown> {
  return readRecord(opportunityRecord(opportunity).execution_shape);
}

function getOpportunityOrderPayload(opportunity: Opportunity): Record<string, unknown> {
  const executionShape = getOpportunityExecutionShape(opportunity);
  return readRecord(executionShape.order_payload ?? opportunity.order_payload);
}

function getOpportunityReasonCodes(opportunity: Opportunity): string[] {
  return readStringList(opportunityRecord(opportunity).reason_codes);
}

function getOpportunityBlockers(opportunity: Opportunity): string[] {
  return readStringList(opportunityRecord(opportunity).blockers);
}

function getOpportunitySetupReasons(opportunity: Opportunity): string[] {
  return readStringList(getOpportunityCandidate(opportunity).setup_reasons);
}

function getOpportunityExpirationDate(opportunity: Opportunity): string {
  const record = opportunityRecord(opportunity);
  const candidate = getOpportunityCandidate(opportunity);
  return readString(record.expiration_date ?? candidate.expiration_date);
}

function getOpportunityGeneratedAt(opportunity: Opportunity): string | null {
  const record = opportunityRecord(opportunity);
  const candidate = getOpportunityCandidate(opportunity);
  return readString(
    candidate.generated_at ?? record.updated_at ?? record.created_at,
    "",
  ) || null;
}

function getOpportunityPipelineLabel(opportunity: Opportunity): string {
  return readString(opportunity.label, readString(opportunity.pipeline_id));
}

function getOpportunityProfile(opportunity: Opportunity): string {
  const record = opportunityRecord(opportunity);
  return readString(
    record.profile ?? opportunity.style_profile ?? opportunity.horizon_intent,
    "unscoped",
  );
}

function getOpportunityBias(opportunity: Opportunity): string {
  const record = opportunityRecord(opportunity);
  return readString(record.side_bias ?? record.side, "neutral");
}

function getOpportunityShortStrike(opportunity: Opportunity): number | null {
  const candidate = getOpportunityCandidate(opportunity);
  const value = candidate.short_strike;
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function getOpportunityLongStrike(opportunity: Opportunity): number | null {
  const candidate = getOpportunityCandidate(opportunity);
  const value = candidate.long_strike;
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function getOpportunityStrikes(opportunity: Opportunity): string {
  const shortStrike = getOpportunityShortStrike(opportunity);
  const longStrike = getOpportunityLongStrike(opportunity);
  if (shortStrike != null && longStrike != null) {
    return `${formatQuantity(shortStrike)} / ${formatQuantity(longStrike)}`;
  }

  const candidate = getOpportunityCandidate(opportunity);
  const shortSymbol = readString(candidate.short_symbol, "");
  const longSymbol = readString(candidate.long_symbol, "");
  if (shortSymbol || longSymbol) {
    return [shortSymbol || "short", longSymbol || "long"].join(" / ");
  }
  return "—";
}

function getOpportunityShortSymbol(opportunity: Opportunity): string {
  const candidate = getOpportunityCandidate(opportunity);
  return readString(candidate.short_symbol);
}

function getOpportunityLongSymbol(opportunity: Opportunity): string {
  const candidate = getOpportunityCandidate(opportunity);
  return readString(candidate.long_symbol);
}

function getOpportunityShortDelta(opportunity: Opportunity): number | null {
  const candidate = getOpportunityCandidate(opportunity);
  const value = candidate.short_delta;
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function getOpportunityLongDelta(opportunity: Opportunity): number | null {
  const candidate = getOpportunityCandidate(opportunity);
  const value = candidate.long_delta;
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function getOpportunityExpectedMove(opportunity: Opportunity): number | null {
  const candidate = getOpportunityCandidate(opportunity);
  const value = candidate.expected_move;
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function getOpportunityDte(opportunity: Opportunity): number | null {
  const candidate = getOpportunityCandidate(opportunity);
  const value = candidate.days_to_expiration;
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function getOpportunityCycleId(opportunity: Opportunity): string {
  const record = opportunityRecord(opportunity);
  return readString(record.cycle_id ?? record.source_cycle_id);
}

function getOpportunityStateReason(opportunity: Opportunity): string {
  const record = opportunityRecord(opportunity);
  return readString(record.state_reason);
}

function getOpportunityMidpointCredit(opportunity: Opportunity): number | null {
  const riskHints = getOpportunityRiskHints(opportunity);
  const hinted = readNumber(riskHints.midpoint_credit, Number.NaN);
  if (Number.isFinite(hinted)) {
    return hinted;
  }

  const candidate = getOpportunityCandidate(opportunity);
  const candidateValue = readNumber(candidate.midpoint_credit, Number.NaN);
  return Number.isFinite(candidateValue) ? candidateValue : null;
}

function getOpportunityWidth(opportunity: Opportunity): number | null {
  const riskHints = getOpportunityRiskHints(opportunity);
  const value = readNumber(riskHints.width, Number.NaN);
  return Number.isFinite(value) ? value : null;
}

function getOpportunityMaxLoss(opportunity: Opportunity): number | null {
  const riskHints = getOpportunityRiskHints(opportunity);
  const hinted = readNumber(riskHints.max_loss, Number.NaN);
  if (Number.isFinite(hinted)) {
    return hinted;
  }

  const candidate = getOpportunityCandidate(opportunity);
  const candidateValue = readNumber(candidate.max_loss, Number.NaN);
  return Number.isFinite(candidateValue) ? candidateValue : null;
}

function getOpportunityReturnOnRisk(opportunity: Opportunity): number | null {
  const riskHints = getOpportunityRiskHints(opportunity);
  const hinted = readNumber(riskHints.return_on_risk, Number.NaN);
  if (Number.isFinite(hinted)) {
    return hinted;
  }

  const candidate = getOpportunityCandidate(opportunity);
  const candidateValue = readNumber(candidate.return_on_risk, Number.NaN);
  return Number.isFinite(candidateValue) ? candidateValue : null;
}

function getOpportunityFillRatio(opportunity: Opportunity): number | null {
  const riskHints = getOpportunityRiskHints(opportunity);
  const hinted = readNumber(riskHints.fill_ratio, Number.NaN);
  if (Number.isFinite(hinted)) {
    return hinted;
  }

  const candidate = getOpportunityCandidate(opportunity);
  const candidateValue = readNumber(candidate.fill_ratio, Number.NaN);
  return Number.isFinite(candidateValue) ? candidateValue : null;
}

function formatPercent(value: number | null | undefined): string {
  return value == null ? "—" : `${(value * 100).toFixed(1)}%`;
}

function formatConfidence(value: number | null | undefined): string {
  return value == null ? "—" : value.toFixed(2);
}

function formatDelta(value: number | null | undefined): string {
  return value == null ? "—" : value.toFixed(4);
}

function formatAge(value: string | null | undefined): string {
  const parsed = parseDateValue(value);
  if (!parsed) {
    return "—";
  }

  const elapsedSeconds = Math.max(
    Math.floor((Date.now() - parsed.getTime()) / 1000),
    0,
  );
  if (elapsedSeconds < 60) {
    return `${elapsedSeconds}s`;
  }
  if (elapsedSeconds < 3600) {
    return `${Math.floor(elapsedSeconds / 60)}m`;
  }
  const hours = Math.floor(elapsedSeconds / 3600);
  const minutes = Math.floor((elapsedSeconds % 3600) / 60);
  return minutes > 0 ? `${hours}h ${minutes}m` : `${hours}h`;
}

function isOpportunityConsumed(opportunity: Opportunity): boolean {
  return opportunity.lifecycle_state === "consumed";
}

function selectionTone(value: string): string {
  switch (value) {
    case "promotable":
      return "border-emerald-200 bg-emerald-100 text-emerald-900 dark:border-emerald-900/80 dark:bg-emerald-950/55 dark:text-emerald-100";
    case "monitor":
      return "border-sky-200 bg-sky-100 text-sky-900 dark:border-sky-900/80 dark:bg-sky-950/55 dark:text-sky-100";
    default:
      return "border-border/70 bg-card text-foreground";
  }
}

function lifecycleTone(value: string): string {
  switch (value) {
    case "ready":
      return "border-emerald-200 bg-emerald-100 text-emerald-900 dark:border-emerald-900/80 dark:bg-emerald-950/55 dark:text-emerald-100";
    case "candidate":
      return "border-amber-200 bg-amber-100 text-amber-900 dark:border-amber-900/80 dark:bg-amber-950/55 dark:text-amber-100";
    case "consumed":
      return "border-stone-200 bg-stone-100 text-stone-900 dark:border-stone-700 dark:bg-stone-800 dark:text-stone-100";
    default:
      return "border-border/70 bg-card text-foreground";
  }
}

function reasonTone(reason: string): string {
  const normalized = reason.toLowerCase();
  if (normalized.startsWith("caution")) {
    return "border-amber-300/80 bg-amber-100/70 text-amber-950 dark:border-amber-900/80 dark:bg-amber-950/35 dark:text-amber-100";
  }
  if (normalized.startsWith("supportive")) {
    return "border-emerald-300/80 bg-emerald-100/70 text-emerald-950 dark:border-emerald-900/80 dark:bg-emerald-950/35 dark:text-emerald-100";
  }
  return "border-border/70 bg-background/80 text-foreground";
}

function OpportunitySelectionBadge({
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
        selectionTone(resolved),
      )}
    >
      {resolved.replaceAll("_", " ")}
    </Badge>
  );
}

function OpportunityLifecycleBadge({
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
        lifecycleTone(resolved),
      )}
    >
      {resolved.replaceAll("_", " ")}
    </Badge>
  );
}

function BoardMetric({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone?: string;
}) {
  return (
    <div className="flex items-center justify-between gap-3 text-sm">
      <span className="text-muted-foreground">{label}</span>
      <span className={cn("font-mono text-right text-foreground", tone)}>
        {value}
      </span>
    </div>
  );
}

function ExpandedSection({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <div className="rounded-2xl border border-border/70 bg-background/80 px-4 py-3">
      <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
        {title}
      </div>
      <div className="mt-3">{children}</div>
    </div>
  );
}

function ExpandedOpportunityPanel({
  opportunity,
}: {
  opportunity: Opportunity;
}) {
  const setupReasons = getOpportunitySetupReasons(opportunity);
  const blockers = getOpportunityBlockers(opportunity);
  const reasonCodes = getOpportunityReasonCodes(opportunity);
  const orderPayload = getOpportunityOrderPayload(opportunity);
  const generatedAt = getOpportunityGeneratedAt(opportunity);
  const orderLimitPrice = readString(orderPayload.limit_price, "");
  const orderQty = readString(orderPayload.qty, "");

  return (
    <div className="grid gap-3 py-3 lg:grid-cols-[1.4fr_1fr_1fr]">
      <ExpandedSection title="Setup Reasons">
        <div className="grid gap-2">
          {setupReasons.length ? (
            setupReasons.map((reason) => (
              <div
                key={reason}
                className={cn(
                  "rounded-xl border px-3 py-2 text-sm leading-5",
                  reasonTone(reason),
                )}
              >
                {reason}
              </div>
            ))
          ) : (
            <div className="rounded-xl border border-dashed border-border/70 px-3 py-2 text-sm text-muted-foreground">
              No setup rationale was captured for this opportunity.
            </div>
          )}
          {blockers.length ? (
            <div className="rounded-xl border border-rose-300/70 bg-rose-100/70 px-3 py-2 text-sm text-rose-950 dark:border-rose-900/80 dark:bg-rose-950/35 dark:text-rose-100">
              <div className="text-[11px] uppercase tracking-[0.18em]">
                Blockers
              </div>
              <div className="mt-2 flex flex-wrap gap-2">
                {blockers.map((blocker) => (
                  <Badge
                    key={blocker}
                    variant="outline"
                    className="rounded-full border-current/30 bg-transparent px-2 py-1 text-[11px] uppercase tracking-[0.16em]"
                  >
                    {humanize(blocker)}
                  </Badge>
                ))}
              </div>
            </div>
          ) : null}
        </div>
      </ExpandedSection>

      <ExpandedSection title="Execution Shape">
        <div className="grid gap-2">
          <BoardMetric
            label="Short leg"
            value={`${getOpportunityShortSymbol(opportunity)} · δ ${formatDelta(
              getOpportunityShortDelta(opportunity),
            )}`}
          />
          <BoardMetric
            label="Long leg"
            value={`${getOpportunityLongSymbol(opportunity)} · δ ${formatDelta(
              getOpportunityLongDelta(opportunity),
            )}`}
          />
          <BoardMetric
            label="Order"
            value={`${orderQty || "—"} lot · ${orderLimitPrice || "—"} credit`}
          />
          <BoardMetric
            label="Fill ratio"
            value={formatPercent(getOpportunityFillRatio(opportunity))}
          />
          <BoardMetric
            label="Expected move"
            value={formatQuantity(getOpportunityExpectedMove(opportunity))}
          />
          <BoardMetric
            label="Generated"
            value={formatTimestamp(generatedAt)}
          />
        </div>
      </ExpandedSection>

      <ExpandedSection title="Lineage">
        <div className="grid gap-2">
          <BoardMetric
            label="Pipeline"
            value={getOpportunityPipelineLabel(opportunity)}
          />
          <BoardMetric
            label="State"
            value={`${humanize(opportunity.lifecycle_state)} · ${humanize(
              opportunity.selection_state,
            )}`}
          />
          <BoardMetric
            label="Reason"
            value={humanize(getOpportunityStateReason(opportunity))}
          />
          <BoardMetric
            label="Reason codes"
            value={reasonCodes.length ? reasonCodes.join(", ") : "—"}
          />
          <div className="rounded-xl border border-border/70 bg-muted/20 px-3 py-2 text-xs text-muted-foreground">
            <div className="uppercase tracking-[0.16em]">IDs</div>
            <div className="mt-2 break-all font-mono text-[11px] text-foreground/80">
              {opportunity.opportunity_id}
            </div>
            <div className="mt-2 break-all font-mono text-[11px] text-foreground/70">
              {getOpportunityCycleId(opportunity)}
            </div>
          </div>
        </div>
      </ExpandedSection>
    </div>
  );
}

function StateRankCell({
  opportunity,
}: {
  opportunity: Opportunity;
}) {
  return (
    <div className="min-w-[132px] space-y-2">
      <OpportunitySelectionBadge value={opportunity.selection_state} />
      <OpportunityLifecycleBadge value={opportunity.lifecycle_state} />
      <div className="font-mono text-xs text-muted-foreground">
        Rank {opportunity.selection_rank ?? "—"}
      </div>
    </div>
  );
}

function UnderlyingCell({
  opportunity,
}: {
  opportunity: Opportunity;
}) {
  return (
    <div className="min-w-[260px]">
      <div className="font-semibold">{opportunity.underlying_symbol}</div>
      <div className="text-xs text-muted-foreground">
        {humanize(opportunity.strategy_family)} · {humanize(getOpportunityBias(opportunity))} ·{" "}
        {humanize(getOpportunityProfile(opportunity))}
      </div>
      <div className="mt-1 font-mono text-xs text-foreground/85">
        {getOpportunityStrikes(opportunity)}
      </div>
      <Link
        href={buildPipelineHref(opportunity.pipeline_id ?? undefined, opportunity.market_date)}
        className="mt-1 inline-block text-xs text-muted-foreground underline-offset-4 hover:underline"
      >
        {getOpportunityPipelineLabel(opportunity)}
      </Link>
    </div>
  );
}

function CreditRiskCell({
  opportunity,
}: {
  opportunity: Opportunity;
}) {
  return (
    <div className="min-w-[180px] space-y-1">
      <BoardMetric
        label="Mid"
        value={formatNullableCurrency(getOpportunityMidpointCredit(opportunity))}
      />
      <BoardMetric
        label="Width"
        value={formatQuantity(getOpportunityWidth(opportunity))}
      />
      <BoardMetric
        label="Max loss"
        value={formatNullableCurrency(getOpportunityMaxLoss(opportunity))}
      />
      <BoardMetric
        label="RoR"
        value={formatPercent(getOpportunityReturnOnRisk(opportunity))}
      />
    </div>
  );
}

function ConvictionCell({
  opportunity,
}: {
  opportunity: Opportunity;
}) {
  const reasonCodes = getOpportunityReasonCodes(opportunity);

  return (
    <div className="min-w-[180px] space-y-1">
      <BoardMetric
        label="Score"
        value={
          opportunity.promotion_score == null
            ? "—"
            : formatScore(opportunity.promotion_score)
        }
      />
      <BoardMetric
        label="Exec"
        value={
          opportunity.execution_score == null
            ? "—"
            : formatScore(opportunity.execution_score)
        }
      />
      <BoardMetric
        label="Conf"
        value={formatConfidence(opportunity.confidence)}
      />
      <BoardMetric
        label="Reason"
        value={reasonCodes[0] ? humanize(reasonCodes[0]) : "—"}
      />
    </div>
  );
}

function TimingCell({
  opportunity,
}: {
  opportunity: Opportunity;
}) {
  const generatedAt = getOpportunityGeneratedAt(opportunity);

  return (
    <div className="min-w-[156px] space-y-1">
      <BoardMetric label="Date" value={formatDate(opportunity.market_date)} />
      <BoardMetric
        label="Expiry"
        value={formatDate(getOpportunityExpirationDate(opportunity))}
      />
      <BoardMetric
        label="DTE"
        value={
          getOpportunityDte(opportunity) == null
            ? "—"
            : String(getOpportunityDte(opportunity))
        }
      />
      <BoardMetric label="Age" value={formatAge(generatedAt)} />
    </div>
  );
}

export function OpportunitiesIndexPageContent() {
  const queryClient = useQueryClient();
  const [expandedOpportunityId, setExpandedOpportunityId] = useState<string | null>(
    null,
  );
  const opportunitiesQuery = useQuery({
    queryKey: ["opportunities"],
    queryFn: () => getOpportunities({ limit: 200 }),
  });
  const executeMutation = useMutation({
    mutationFn: (opportunityId: string) => executeOpportunity(opportunityId, {}),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["opportunities"] }),
        queryClient.invalidateQueries({ queryKey: ["pipelines"] }),
        queryClient.invalidateQueries({ queryKey: ["positions"] }),
      ]);
    },
  });

  if (opportunitiesQuery.isLoading) {
    return <LoadingState />;
  }

  const opportunities = opportunitiesQuery.data?.opportunities ?? [];
  const promotableCount = opportunities.filter(
    (row) => row.selection_state === "promotable",
  ).length;
  const monitorCount = opportunities.filter(
    (row) => row.selection_state === "monitor",
  ).length;
  const readyCount = opportunities.filter(
    (row) => row.lifecycle_state === "ready",
  ).length;
  const consumedCount = opportunities.filter(isOpportunityConsumed).length;
  const pipelineCount = new Set(
    opportunities.flatMap((row) => (row.pipeline_id ? [row.pipeline_id] : [])),
  ).size;
  const latestTimestamp = opportunities.reduce<string | null>((latest, row) => {
    const candidate = getOpportunityGeneratedAt(row);
    if (!candidate) {
      return latest;
    }
    const latestDate = parseDateValue(latest);
    const candidateDate = parseDateValue(candidate);
    if (!candidateDate) {
      return latest;
    }
    if (!latestDate || candidateDate > latestDate) {
      return candidate;
    }
    return latest;
  }, null);

  const columns: ColumnDef<Opportunity>[] = [
    {
      id: "state_rank",
      header: "State / Rank",
      cell: ({ row }) => <StateRankCell opportunity={row.original} />,
    },
    {
      id: "underlying_setup",
      header: "Underlying / Setup",
      cell: ({ row }) => <UnderlyingCell opportunity={row.original} />,
    },
    {
      id: "credit_risk",
      header: "Credit / Risk",
      cell: ({ row }) => <CreditRiskCell opportunity={row.original} />,
    },
    {
      id: "conviction",
      header: "Conviction",
      cell: ({ row }) => <ConvictionCell opportunity={row.original} />,
    },
    {
      id: "timing",
      header: "Timing",
      cell: ({ row }) => <TimingCell opportunity={row.original} />,
    },
    {
      id: "actions",
      header: "Act",
      cell: ({ row }) =>
        isOpportunityConsumed(row.original) ? (
          <span className="text-xs uppercase tracking-[0.16em] text-muted-foreground">
            Consumed
          </span>
        ) : (
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={executeMutation.isPending}
            onClick={() => executeMutation.mutate(row.original.opportunity_id)}
          >
            Execute
          </Button>
        ),
    },
    {
      id: "expand",
      header: "",
      cell: ({ row }) => {
        const expanded = expandedOpportunityId === row.original.opportunity_id;

        return (
          <Button
            type="button"
            size="icon-sm"
            variant="ghost"
            aria-expanded={expanded}
            aria-label={expanded ? "Collapse details" : "Expand details"}
            onClick={() =>
              setExpandedOpportunityId((current) =>
                current === row.original.opportunity_id
                  ? null
                  : row.original.opportunity_id,
              )
            }
          >
            {expanded ? <ChevronDown /> : <ChevronRight />}
          </Button>
        );
      },
    },
  ];

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
                Opportunities
              </Badge>
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">
              Opportunity board
            </div>
            <div className="mt-2 text-sm text-foreground/70">
              Scan the live pool across pipelines, execute directly, and expand
              any row for setup rationale, leg shape, and lineage.
            </div>
          </div>
          <Button
            type="button"
            variant="outline"
            onClick={() => void opportunitiesQuery.refetch()}
          >
            <RefreshCw data-icon="inline-start" />
            Refresh
          </Button>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-6">
        <MetricTile
          label="Opportunities"
          value={String(opportunities.length)}
          note="Current live rows"
        />
        <MetricTile
          label="Promotable"
          value={String(promotableCount)}
          note="Selection leaders"
        />
        <MetricTile
          label="Monitor"
          value={String(monitorCount)}
          note="Still tracking"
        />
        <MetricTile
          label="Ready"
          value={String(readyCount)}
          note="Lifecycle ready"
        />
        <MetricTile
          label="Consumed"
          value={String(consumedCount)}
          note="Already used"
        />
        <MetricTile
          label="Pipelines"
          value={String(pipelineCount)}
          note={latestTimestamp ? `Updated ${formatTimestamp(latestTimestamp)}` : "No recent update"}
        />
      </div>

      {executeMutation.isError ? (
        <div className="rounded-2xl border border-rose-300/70 bg-rose-100/80 px-4 py-3 text-sm text-rose-950 dark:border-rose-900/80 dark:bg-rose-950/40 dark:text-rose-100">
          <div className="flex items-start gap-2">
            <TriangleAlert className="mt-0.5 size-4 shrink-0" />
            <span>
              {executeMutation.error instanceof Error
                ? executeMutation.error.message
                : "Execution failed."}
            </span>
          </div>
        </div>
      ) : null}

      <SectionSurface
        title="Opportunity Board"
        description="Primary rows stay decision-oriented. Expand a row to inspect setup reasons, option legs, execution shape, and lineage without leaving this workspace."
      >
        <DataTable
          columns={columns}
          data={opportunities}
          emptyMessage="No opportunities were available."
          getRowId={(row) => row.opportunity_id}
          isExpanded={(row) => row.opportunity_id === expandedOpportunityId}
          pageSize={20}
          renderExpandedContent={(row) => (
            <ExpandedOpportunityPanel opportunity={row} />
          )}
        />
      </SectionSurface>
    </div>
  );
}
