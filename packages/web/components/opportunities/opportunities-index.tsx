"use client";

import Link from "next/link";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";
import { Radar, RefreshCw, TriangleAlert } from "lucide-react";
import { startTransition, useEffect, type ReactNode } from "react";

import { DataTable } from "@/components/data-table";
import { MarketDateFilter } from "@/components/market-date-filter";
import {
  formatDate,
  formatNullableCurrency,
  formatQuantity,
  formatScore,
  formatSignedCurrency,
  formatSignedPercent,
  formatTimestamp,
  LoadingState,
  MetricTile,
  readString,
  SectionSurface,
} from "@/components/operator/operator-primitives";
import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  buildOpportunitiesHref,
  buildRuntimeHref,
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

function getOpportunityOwner(opportunity: Opportunity): Record<string, unknown> {
  return readRecord(opportunityRecord(opportunity).owner);
}

function getOpportunityDiscovery(opportunity: Opportunity): Record<string, unknown> {
  return readRecord(opportunityRecord(opportunity).discovery);
}

function getOpportunityRuntimeLabel(opportunity: Opportunity): string {
  const owner = getOpportunityOwner(opportunity);
  const botId = readString(owner.bot_id, "");
  const automationId = readString(owner.automation_id, "");
  if (botId && automationId) {
    return `${botId} / ${automationId}`;
  }
  return "—";
}

function getOpportunityDiscoveryLabel(opportunity: Opportunity): string {
  const discovery = getOpportunityDiscovery(opportunity);
  return readString(
    discovery.label,
    readString(opportunity.label, readString(opportunity.pipeline_id)),
  );
}

function getOpportunityRuntimeHref(opportunity: Opportunity): string {
  const owner = getOpportunityOwner(opportunity);
  const botId = typeof owner.bot_id === "string" ? owner.bot_id : null;
  const automationId =
    typeof owner.automation_id === "string" ? owner.automation_id : null;
  return buildRuntimeHref(
    botId,
    automationId,
    opportunity.market_date,
  );
}

function hasOpportunityRuntimeOwner(opportunity: Opportunity): boolean {
  const owner = getOpportunityOwner(opportunity);
  return Boolean(readString(owner.bot_id, "") && readString(owner.automation_id, ""));
}

function getOpportunityDiscoveryHref(opportunity: Opportunity): string {
  const discovery = getOpportunityDiscovery(opportunity);
  const pipelineId =
    typeof discovery.pipeline_id === "string"
      ? discovery.pipeline_id
      : opportunity.pipeline_id;
  return buildPipelineHref(
    pipelineId,
    opportunity.market_date,
  );
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

type OpportunityLegRecord = Record<string, unknown>;

const CREDIT_STRATEGY_FAMILIES = new Set([
  "call_credit_spread",
  "put_credit_spread",
  "iron_condor",
  "short_call",
  "short_put",
]);

const DEBIT_STRATEGY_FAMILIES = new Set([
  "call_debit_spread",
  "put_debit_spread",
  "long_call",
  "long_put",
  "long_straddle",
  "long_strangle",
]);

const SHORT_SINGLE_LEG_STRATEGY_FAMILIES = new Set(["short_call", "short_put"]);

function readRecordList(value: unknown): Record<string, unknown>[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.flatMap((item) =>
    typeof item === "object" && item !== null && !Array.isArray(item)
      ? [item as Record<string, unknown>]
      : [],
  );
}

function readFiniteNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function formatSignedNumber(
  value: number | null | undefined,
  maximumFractionDigits = 2,
): string {
  if (value == null) {
    return "—";
  }
  return new Intl.NumberFormat(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits,
    signDisplay: "exceptZero",
  }).format(value);
}

function getOpportunityLegs(opportunity: Opportunity): OpportunityLegRecord[] {
  return readRecordList(opportunity.legs);
}

function getOpportunityEconomics(opportunity: Opportunity): Record<string, unknown> {
  return readRecord(opportunity.economics);
}

function getOpportunityStrategyMetrics(opportunity: Opportunity): Record<string, unknown> {
  return readRecord(opportunity.strategy_metrics);
}

function getOpportunityEvidence(opportunity: Opportunity): Record<string, unknown> {
  return readRecord(opportunity.evidence);
}

function getOpportunityStrategyFamily(opportunity: Opportunity): string {
  return String(opportunity.strategy_family || "").trim().toLowerCase();
}

function getOpportunityPremiumKind(opportunity: Opportunity): "credit" | "debit" | "unknown" {
  const family = getOpportunityStrategyFamily(opportunity);
  if (CREDIT_STRATEGY_FAMILIES.has(family)) {
    return "credit";
  }
  if (DEBIT_STRATEGY_FAMILIES.has(family)) {
    return "debit";
  }
  return "unknown";
}

function isOpportunityShortSingleLeg(opportunity: Opportunity): boolean {
  return (
    SHORT_SINGLE_LEG_STRATEGY_FAMILIES.has(getOpportunityStrategyFamily(opportunity)) &&
    getOpportunityLegs(opportunity).length === 1
  );
}

function getOpportunityExpirationDate(opportunity: Opportunity): string {
  const expirations = Array.from(
    new Set(
      getOpportunityLegs(opportunity)
        .map((leg) => readString(leg.expiration_date, ""))
        .filter((expiration) => expiration !== ""),
    ),
  );
  if (expirations.length === 1) {
    return expirations[0];
  }
  const record = opportunityRecord(opportunity);
  return readString(record.expiration_date, "");
}

function getOpportunityGeneratedAt(opportunity: Opportunity): string | null {
  const evidence = getOpportunityEvidence(opportunity);
  const record = opportunityRecord(opportunity);
  return readString(
    evidence.generated_at ?? record.updated_at ?? record.created_at,
    "",
  ) || null;
}

function getOpportunityLegRole(leg: OpportunityLegRecord): string | null {
  return readString(leg.role, "") || null;
}

function getOpportunityLegSide(leg: OpportunityLegRecord): string | null {
  return readString(leg.side, "") || null;
}

function getOpportunityLegPositionIntent(leg: OpportunityLegRecord): string | null {
  return readString(leg.position_intent, "") || null;
}

function getOpportunityLegRatio(leg: OpportunityLegRecord): number {
  const ratio = readFiniteNumber(leg.ratio_qty);
  return ratio != null && ratio > 0 ? ratio : 1;
}

function getOpportunityLegOptionType(leg: OpportunityLegRecord): string | null {
  const explicit = readString(leg.option_type, "") || null;
  if (explicit) {
    return explicit;
  }
  const symbol = readString(leg.symbol, "") || "";
  const match = symbol.match(/([cp])(?=\d+(?:\.\d+)?$)/i);
  if (!match) {
    return null;
  }
  return match[1].toLowerCase() === "c" ? "call" : "put";
}

function getOpportunityLegStrike(leg: OpportunityLegRecord): number | null {
  return readFiniteNumber(leg.strike);
}

function getOpportunityLegQuantityLabel(leg: OpportunityLegRecord): string {
  const ratio = formatQuantity(getOpportunityLegRatio(leg));
  const role = getOpportunityLegRole(leg);
  if (role === "short") {
    return `-${ratio}`;
  }
  if (role === "long") {
    return `+${ratio}`;
  }
  return ratio;
}

function getOpportunityLegStrikeCode(leg: OpportunityLegRecord): string {
  const strike = getOpportunityLegStrike(leg);
  const optionType = getOpportunityLegOptionType(leg);
  if (strike != null && optionType) {
    return `${formatQuantity(strike)}${optionType === "call" ? "C" : "P"}`;
  }
  return readString(leg.symbol);
}

function getOpportunityLegCountLabel(opportunity: Opportunity): string {
  const count = getOpportunityLegs(opportunity).length;
  return `${count} leg${count === 1 ? "" : "s"}`;
}

function getOpportunityPrimaryExposureLeg(
  opportunity: Opportunity,
): OpportunityLegRecord | null {
  const legs = getOpportunityLegs(opportunity);
  return (
    legs.find((leg) => getOpportunityLegRole(leg) === "short") ??
    legs.find((leg) => getOpportunityLegRole(leg) === "long") ??
    legs[0] ??
    null
  );
}

function getOpportunityStructurePath(opportunity: Opportunity): string {
  const rendered = getOpportunityLegs(opportunity).map(
    (leg) => `${getOpportunityLegQuantityLabel(leg)} ${getOpportunityLegStrikeCode(leg)}`,
  );
  return rendered.length ? rendered.join(" / ") : "—";
}

function getOpportunityDte(opportunity: Opportunity): number | null {
  const expirationDate = getOpportunityExpirationDate(opportunity);
  if (!isMarketDateValue(opportunity.market_date) || !isMarketDateValue(expirationDate)) {
    return null;
  }
  const marketStart = Date.parse(`${opportunity.market_date}T00:00:00Z`);
  const expiryStart = Date.parse(`${expirationDate}T00:00:00Z`);
  if (!Number.isFinite(marketStart) || !Number.isFinite(expiryStart)) {
    return null;
  }
  return Math.max(Math.round((expiryStart - marketStart) / 86_400_000), 0);
}

function getOpportunityStateReason(opportunity: Opportunity): string {
  const record = opportunityRecord(opportunity);
  return readString(record.state_reason);
}

function getOpportunityEligibilityState(opportunity: Opportunity): string {
  const record = opportunityRecord(opportunity);
  return readString(record.eligibility_state ?? record.eligibility, "");
}

function getOpportunityLifecycleState(opportunity: Opportunity): string {
  return readString(opportunity.lifecycle_state, "");
}

function hasLiveOpportunityEligibility(opportunity: Opportunity): boolean {
  const eligibilityState = getOpportunityEligibilityState(opportunity);
  return eligibilityState === "" || eligibilityState === "live";
}

function isOpportunityStale(opportunity: Opportunity): boolean {
  return (
    getOpportunityLifecycleState(opportunity) === "stale" ||
    getOpportunityEligibilityState(opportunity) === "stale"
  );
}

function isOpportunityExpired(opportunity: Opportunity): boolean {
  return (
    getOpportunityLifecycleState(opportunity) === "expired" ||
    getOpportunityEligibilityState(opportunity) === "expired"
  );
}

function isOpportunityLiveBoardRow(opportunity: Opportunity): boolean {
  return (
    ["candidate", "ready", "blocked"].includes(getOpportunityLifecycleState(opportunity)) &&
    hasLiveOpportunityEligibility(opportunity)
  );
}

function isOpportunityLiveExecutable(opportunity: Opportunity): boolean {
  if (isOpportunityConsumed(opportunity)) {
    return false;
  }
  return isOpportunityLiveBoardRow(opportunity);
}

function getOpportunityMidpointValue(opportunity: Opportunity): number | null {
  return readFiniteNumber(getOpportunityEconomics(opportunity).midpoint_credit);
}

function getOpportunityNaturalValue(opportunity: Opportunity): number | null {
  return readFiniteNumber(getOpportunityEconomics(opportunity).natural_credit);
}

function getOpportunityRiskAmount(opportunity: Opportunity): number | null {
  return readFiniteNumber(getOpportunityEconomics(opportunity).max_loss);
}

function getOpportunityRiskAmountLabel(opportunity: Opportunity): string {
  return isOpportunityShortSingleLeg(opportunity) ? "Stress" : "MaxL";
}

function getOpportunityRiskAmountDetailLabel(opportunity: Opportunity): string {
  return isOpportunityShortSingleLeg(opportunity) ? "Stress loss" : "Max loss";
}

function getOpportunityReturnOnRisk(opportunity: Opportunity): number | null {
  return readFiniteNumber(getOpportunityEconomics(opportunity).return_on_risk);
}

function getOpportunityFillRatio(opportunity: Opportunity): number | null {
  return readFiniteNumber(getOpportunityEconomics(opportunity).fill_ratio);
}

function getOpportunityExpectedValue(opportunity: Opportunity): number | null {
  return readFiniteNumber(getOpportunityEconomics(opportunity).expected_value_dollars);
}

function getOpportunityAdjustedExpectedValue(opportunity: Opportunity): number | null {
  return readFiniteNumber(
    getOpportunityEconomics(opportunity).slippage_adjusted_expected_value_dollars,
  );
}

function getOpportunityEntrySlippage(opportunity: Opportunity): number | null {
  return readFiniteNumber(getOpportunityEconomics(opportunity).entry_slippage_dollars);
}

function getOpportunityWidth(opportunity: Opportunity): number | null {
  const value = readFiniteNumber(getOpportunityStrategyMetrics(opportunity).width);
  return value != null && value > 0 ? value : null;
}

function getOpportunityUnderlyingPrice(opportunity: Opportunity): number | null {
  return readFiniteNumber(getOpportunityStrategyMetrics(opportunity).underlying_price);
}

function getOpportunityBreakeven(opportunity: Opportunity): number | null {
  return readFiniteNumber(getOpportunityStrategyMetrics(opportunity).breakeven);
}

function getOpportunityLowerBreakeven(opportunity: Opportunity): number | null {
  return readFiniteNumber(getOpportunityStrategyMetrics(opportunity).lower_breakeven);
}

function getOpportunityUpperBreakeven(opportunity: Opportunity): number | null {
  return readFiniteNumber(getOpportunityStrategyMetrics(opportunity).upper_breakeven);
}

function getOpportunityBreakevenDisplay(opportunity: Opportunity): string {
  const lower = getOpportunityLowerBreakeven(opportunity);
  const upper = getOpportunityUpperBreakeven(opportunity);
  if (lower != null && upper != null) {
    return `${formatQuantity(lower)} - ${formatQuantity(upper)}`;
  }
  const breakeven = getOpportunityBreakeven(opportunity);
  return breakeven == null ? "—" : formatQuantity(breakeven);
}

function getOpportunityBreakevenCushionPct(opportunity: Opportunity): number | null {
  return readFiniteNumber(getOpportunityStrategyMetrics(opportunity).breakeven_cushion_pct);
}

function getOpportunityExpectedMove(opportunity: Opportunity): number | null {
  return readFiniteNumber(getOpportunityStrategyMetrics(opportunity).expected_move);
}

function getOpportunityExpectedMovePct(opportunity: Opportunity): number | null {
  return readFiniteNumber(getOpportunityStrategyMetrics(opportunity).expected_move_pct);
}

function getOpportunityExpectedMoveDisplay(opportunity: Opportunity): string {
  const amount = getOpportunityExpectedMove(opportunity);
  const percent = getOpportunityExpectedMovePct(opportunity);
  if (amount == null && percent == null) {
    return "—";
  }
  if (amount != null && percent != null) {
    return `${formatQuantity(amount)} (${formatPercent(percent)})`;
  }
  if (amount != null) {
    return formatQuantity(amount);
  }
  return formatPercent(percent);
}

function getOpportunityStrikeDistance(opportunity: Opportunity): number | null {
  const spot = getOpportunityUnderlyingPrice(opportunity);
  const leg = getOpportunityPrimaryExposureLeg(opportunity);
  const strike = leg ? getOpportunityLegStrike(leg) : null;
  if (spot == null || strike == null) {
    return null;
  }
  const optionType = leg ? getOpportunityLegOptionType(leg) : null;
  if (optionType === "put") {
    return spot - strike;
  }
  if (optionType === "call") {
    return strike - spot;
  }
  return Math.abs(spot - strike);
}

function getOpportunityStrikeDistancePct(opportunity: Opportunity): number | null {
  const strikeDistance = getOpportunityStrikeDistance(opportunity);
  const spot = getOpportunityUnderlyingPrice(opportunity);
  if (strikeDistance == null || spot == null || spot === 0) {
    return null;
  }
  return strikeDistance / spot;
}

function getOpportunityStrikeDistanceDisplay(opportunity: Opportunity): string {
  const strikeDistance = getOpportunityStrikeDistance(opportunity);
  const strikeDistancePct = getOpportunityStrikeDistancePct(opportunity);
  if (strikeDistance == null && strikeDistancePct == null) {
    return "—";
  }
  if (strikeDistance != null && strikeDistancePct != null) {
    return `${formatSignedNumber(strikeDistance, 2)} (${formatSignedPercent(
      strikeDistancePct,
    )})`;
  }
  if (strikeDistance != null) {
    return formatSignedNumber(strikeDistance, 2);
  }
  return formatSignedPercent(strikeDistancePct);
}

function getOpportunityMoneynessLabel(opportunity: Opportunity): string {
  const strikeDistance = getOpportunityStrikeDistance(opportunity);
  if (strikeDistance == null) {
    return "—";
  }
  if (Math.abs(strikeDistance) < 0.01) {
    return "ATM";
  }
  return strikeDistance > 0 ? "OTM" : "ITM";
}

function getOpportunityProbabilityOfProfit(opportunity: Opportunity): number | null {
  return readFiniteNumber(getOpportunityStrategyMetrics(opportunity).probability_of_profit);
}

function getOpportunityModelImpliedVolatility(opportunity: Opportunity): number | null {
  return readFiniteNumber(
    getOpportunityStrategyMetrics(opportunity).model_implied_volatility,
  );
}

function getOpportunityNetDelta(opportunity: Opportunity): number | null {
  return readFiniteNumber(getOpportunityStrategyMetrics(opportunity).net_delta);
}

function getOpportunityNetTheta(opportunity: Opportunity): number | null {
  return readFiniteNumber(getOpportunityStrategyMetrics(opportunity).net_theta);
}

function getOpportunityNetVega(opportunity: Opportunity): number | null {
  return readFiniteNumber(getOpportunityStrategyMetrics(opportunity).net_vega);
}

function getOpportunityRankingPolicyStatus(opportunity: Opportunity): string {
  return readString(getOpportunityEvidence(opportunity).ranking_policy_status);
}

function getOpportunityRankingPolicyBlockers(opportunity: Opportunity): string[] {
  return readStringList(getOpportunityEvidence(opportunity).ranking_policy_blockers);
}

function formatPremiumValue(
  opportunity: Opportunity,
  value: number | null | undefined,
): string {
  if (value == null) {
    return "—";
  }
  const premiumKind = getOpportunityPremiumKind(opportunity);
  if (premiumKind === "credit") {
    return `${formatNullableCurrency(value)} cr`;
  }
  if (premiumKind === "debit") {
    return `${formatNullableCurrency(value)} db`;
  }
  return formatNullableCurrency(value);
}

function formatPercent(value: number | null | undefined): string {
  return value == null ? "—" : `${(value * 100).toFixed(1)}%`;
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

function isMarketDateValue(value: string | null | undefined): value is string {
  return /^\d{4}-\d{2}-\d{2}$/.test(value ?? "");
}

function isOpportunityConsumed(opportunity: Opportunity): boolean {
  return opportunity.lifecycle_state === "consumed";
}

function hasOpportunityExceptionalLifecycle(opportunity: Opportunity): boolean {
  const lifecycleState = readString(opportunity.lifecycle_state, "");
  return !["candidate", "ready", ""].includes(lifecycleState);
}

function getOpportunityRankChip(opportunity: Opportunity): string | null {
  const rank = opportunity.selection_rank;
  if (
    typeof rank === "number" &&
    Number.isFinite(rank) &&
    opportunity.selection_state === "promotable" &&
    rank <= 3
  ) {
    return `#${rank}`;
  }
  return null;
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
    case "blocked":
      return "border-rose-200 bg-rose-100 text-rose-900 dark:border-rose-900/80 dark:bg-rose-950/55 dark:text-rose-100";
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
  rankLabel,
}: {
  value: string | null | undefined;
  rankLabel?: string | null;
}) {
  const resolved = readString(value, "unknown");
  const showRankLabel = resolved === "promotable" && Boolean(rankLabel);
  return (
    <Badge
      variant="outline"
      className={cn(
        "rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.16em]",
        selectionTone(resolved),
      )}
    >
      <span>{resolved.replaceAll("_", " ")}</span>
      {showRankLabel ? (
        <span className="ml-1 font-mono tracking-[0.08em]">{rankLabel}</span>
      ) : null}
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
    <div className="flex items-start justify-between gap-3 text-sm">
      <span className="text-muted-foreground">{label}</span>
      <span
        className={cn(
          "max-w-[68%] min-w-0 break-all font-mono text-right text-foreground",
          tone,
        )}
      >
        {value}
      </span>
    </div>
  );
}

function InspectorSection({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <div className="px-5 py-4">
      <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
        {title}
      </div>
      <div className="mt-3">{children}</div>
    </div>
  );
}

function InspectorReasonLine({
  reason,
  marker,
  className,
}: {
  reason: string;
  marker: "+" | "!";
  className?: string;
}) {
  return (
    <div className="flex items-start gap-3">
      <span
        className={cn(
          "inline-flex size-5 shrink-0 items-center justify-center rounded-full border text-[11px] font-semibold",
          className,
        )}
      >
        {marker}
      </span>
      <span className="text-sm leading-5 text-foreground/90">{reason}</span>
    </div>
  );
}

function InspectorSummaryMetric({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <div className="min-w-0">
      <div className="text-[10px] uppercase tracking-[0.16em] text-muted-foreground">
        {label}
      </div>
      <div className="mt-1 truncate text-lg font-semibold tracking-[0.01em] text-foreground">
        {value}
      </div>
    </div>
  );
}

function OpportunityInspectorUnavailableState() {
  return (
    <div className="flex min-h-[20rem] flex-col items-center justify-center border-y border-dashed border-border/70 bg-background/30 px-6 py-10 text-center">
      <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
        Opportunity inspector
      </div>
      <div className="mt-3 text-xl font-semibold tracking-[0.01em] text-foreground">
        Selected row is no longer on this board
      </div>
      <div className="mt-2 max-w-[20rem] text-sm leading-6 text-muted-foreground">
        The board refreshed or the current scope changed. Close this drawer or select another row to keep reviewing without leaving the page.
      </div>
    </div>
  );
}

function OpportunityLegTable({
  opportunity,
}: {
  opportunity: Opportunity;
}) {
  const legs = getOpportunityLegs(opportunity);

  return (
    <div data-opportunity-leg-table className="grid gap-3">
      {legs.map((leg, index) => {
        const symbol = readString(leg.symbol);
        const expirationDate = readString(leg.expiration_date, "") || null;
        const optionType = getOpportunityLegOptionType(leg);
        const intent = humanize(
          getOpportunityLegPositionIntent(leg),
          humanize(getOpportunityLegSide(leg), "—"),
        );

        return (
          <div
            key={`${symbol}:${index}`}
            className="rounded-2xl border border-border/70 bg-background/35 px-4 py-4"
          >
            <div className="min-w-0">
              <div className="font-medium text-foreground">
                {getOpportunityLegStrikeCode(leg)}
              </div>
              <div className="mt-1 break-all text-xs text-muted-foreground">
                {symbol}
                {optionType ? ` · ${humanize(optionType)}` : ""}
              </div>
            </div>
            <div className="mt-4 grid grid-cols-2 gap-x-4 gap-y-3">
              <OpportunityLegField
                label="Qty"
                value={getOpportunityLegQuantityLabel(leg)}
                mono
              />
              <OpportunityLegField
                label="Role"
                value={humanize(getOpportunityLegRole(leg))}
              />
              <OpportunityLegField
                label="Expiry"
                value={expirationDate ? formatDate(expirationDate) : "—"}
              />
              <OpportunityLegField label="Intent" value={intent} />
            </div>
          </div>
        );
      })}
    </div>
  );
}

function OpportunityLegField({
  label,
  value,
  mono = false,
}: {
  label: string;
  value: string;
  mono?: boolean;
}) {
  return (
    <div className="min-w-0">
      <div className="text-[10px] uppercase tracking-[0.16em] text-muted-foreground">
        {label}
      </div>
      <div
        className={cn(
          "mt-1 break-words text-sm text-foreground",
          mono ? "font-mono" : "",
        )}
      >
        {value}
      </div>
    </div>
  );
}

function OpportunityInspectorPanel({
  opportunity,
  historicalDateSelected,
  executePending,
  onExecute,
  onClose,
}: {
  opportunity: Opportunity;
  historicalDateSelected: boolean;
  executePending: boolean;
  onExecute: (opportunityId: string) => void;
  onClose: () => void;
}) {
  const rankChip = getOpportunityRankChip(opportunity);
  const setupReasons = getOpportunitySetupReasons(opportunity);
  const blockers = getOpportunityBlockers(opportunity);
  const rankingPolicyBlockers = getOpportunityRankingPolicyBlockers(opportunity);
  const reasonCodes = getOpportunityReasonCodes(opportunity);
  const orderPayload = getOpportunityOrderPayload(opportunity);
  const generatedAt = getOpportunityGeneratedAt(opportunity);
  const expirationDate = getOpportunityExpirationDate(opportunity);
  const dte = getOpportunityDte(opportunity);
  const lifecycleState = getOpportunityLifecycleState(opportunity);
  const policyStatus = getOpportunityRankingPolicyStatus(opportunity);
  const orderLimitPrice = readFiniteNumber(orderPayload.limit_price);
  const orderQty = readFiniteNumber(orderPayload.qty);
  const orderType = readString(orderPayload.type);
  const orderTimeInForce = readString(orderPayload.time_in_force);
  const orderClass =
    readString(orderPayload.order_class, "") ||
    (getOpportunityLegs(opportunity).length > 1 ? "mleg" : "single");
  const orderIntent =
    readString(orderPayload.position_intent, "") ||
    readString(getOpportunityLegs(opportunity)[0]?.position_intent, "");
  const consumed = isOpportunityConsumed(opportunity);
  const liveExecutable = isOpportunityLiveExecutable(opportunity);
  const executionDisabledLabel = historicalDateSelected
    ? "Review only"
    : consumed
      ? "Consumed"
      : "Unavailable";
  const canExecute = !historicalDateSelected && liveExecutable;
  const executionFooterNote = historicalDateSelected
    ? `Historical board for ${formatDate(opportunity.market_date)}. Execution is disabled outside the current Chicago market date.`
    : consumed
      ? "This opportunity has already been consumed by an execution attempt."
      : canExecute
        ? "Execution is available for this opportunity."
        : "This opportunity is not currently executable."

  return (
    <div className="overflow-hidden bg-background/70">
      <div className="px-5 pt-5 pb-4">
        <div className="min-w-0">
          <div className="text-2xl font-semibold tracking-[0.01em] text-foreground">
            {opportunity.underlying_symbol}
          </div>
          <div className="mt-2 text-sm text-muted-foreground">
            {humanize(opportunity.strategy_family)} · {humanize(getOpportunityProfile(opportunity))} ·{" "}
            {formatDate(expirationDate)} · {dte == null ? "—" : `${dte} DTE`}
          </div>
          <div className="mt-3 flex flex-wrap items-center gap-2">
            <OpportunitySelectionBadge
              value={opportunity.selection_state}
              rankLabel={rankChip}
            />
            {hasOpportunityExceptionalLifecycle(opportunity) ? (
              <OpportunityLifecycleBadge value={lifecycleState} />
            ) : null}
            {!canExecute ? (
              <Badge variant="outline" className="rounded-full">
                {executionDisabledLabel}
              </Badge>
            ) : null}
          </div>
          <div className="mt-3 font-mono text-sm text-foreground/85">
            {getOpportunityStructurePath(opportunity)}
          </div>
          <div className="mt-2 text-sm text-muted-foreground">
            Age {formatAge(generatedAt)} · {humanize(getOpportunityBias(opportunity))} bias · Policy{" "}
            {humanize(policyStatus)}
          </div>
        </div>
      </div>

      <Separator />

      <InspectorSection title="Decision">
        <div className="grid grid-cols-2 gap-x-4 gap-y-4 sm:grid-cols-3">
          <InspectorSummaryMetric
            label="Mid"
            value={formatPremiumValue(opportunity, getOpportunityMidpointValue(opportunity))}
          />
          <InspectorSummaryMetric
            label={getOpportunityRiskAmountDetailLabel(opportunity)}
            value={formatNullableCurrency(getOpportunityRiskAmount(opportunity))}
          />
          <InspectorSummaryMetric
            label="RoR"
            value={formatPercent(getOpportunityReturnOnRisk(opportunity))}
          />
          <InspectorSummaryMetric
            label="POP"
            value={formatPercent(getOpportunityProbabilityOfProfit(opportunity))}
          />
          <InspectorSummaryMetric
            label="Adj EV"
            value={formatSignedCurrency(getOpportunityAdjustedExpectedValue(opportunity))}
          />
          <InspectorSummaryMetric
            label="Exec"
            value={
              opportunity.execution_score == null
                ? "—"
                : formatScore(opportunity.execution_score)
            }
          />
        </div>
        <div className="mt-5 border-t border-border/70 pt-4">
          <div className="text-[10px] uppercase tracking-[0.16em] text-muted-foreground">
            Why this idea
          </div>
          <div className="mt-3 grid gap-3">
            {setupReasons.length ? (
              setupReasons.map((reason) => (
                <InspectorReasonLine
                  key={reason}
                  reason={reason}
                  marker="+"
                  className={reasonTone(reason)}
                />
              ))
            ) : (
              <div className="text-sm text-muted-foreground">
                No setup rationale was captured for this opportunity.
              </div>
            )}
            {rankingPolicyBlockers.length ? (
              rankingPolicyBlockers.map((blocker) => (
                <InspectorReasonLine
                  key={blocker}
                  reason={`Policy: ${humanize(blocker)}`}
                  marker="!"
                  className="border-amber-300/80 bg-amber-100/80 text-amber-950 dark:border-amber-900/80 dark:bg-amber-950/35 dark:text-amber-100"
                />
              ))
            ) : null}
            {blockers.length ? (
              blockers.map((blocker) => (
                <InspectorReasonLine
                  key={blocker}
                  reason={humanize(blocker)}
                  marker="!"
                  className="border-rose-300/80 bg-rose-100/80 text-rose-950 dark:border-rose-900/80 dark:bg-rose-950/35 dark:text-rose-100"
                />
              ))
            ) : null}
          </div>
        </div>
      </InspectorSection>

      <Separator />

      <InspectorSection title="Structure & Exposure">
        <div className="grid gap-4">
          <OpportunityLegTable opportunity={opportunity} />
          <div className="grid grid-cols-2 gap-x-4 gap-y-3">
            <CellMetric
              label="Spot"
              value={formatQuantity(getOpportunityUnderlyingPrice(opportunity))}
            />
            <CellMetric
              label="Expected Move"
              value={getOpportunityExpectedMoveDisplay(opportunity)}
            />
            <CellMetric
              label="Breakeven"
              value={getOpportunityBreakevenDisplay(opportunity)}
            />
            <CellMetric
              label="Cushion"
              value={formatSignedPercent(getOpportunityBreakevenCushionPct(opportunity))}
            />
            {isOpportunityShortSingleLeg(opportunity) ? (
              <CellMetric
                label="Strike Dist"
                value={getOpportunityStrikeDistanceDisplay(opportunity)}
              />
            ) : null}
            {isOpportunityShortSingleLeg(opportunity) ? (
              <CellMetric
                label="Moneyness"
                value={getOpportunityMoneynessLabel(opportunity)}
              />
            ) : null}
            {getOpportunityWidth(opportunity) != null ? (
              <CellMetric
                label="Width"
                value={formatQuantity(getOpportunityWidth(opportunity))}
              />
            ) : null}
            <CellMetric
              label="Model IV"
              value={formatPercent(getOpportunityModelImpliedVolatility(opportunity))}
            />
            <CellMetric
              label="Delta"
              value={formatSignedNumber(getOpportunityNetDelta(opportunity), 2)}
            />
            <CellMetric
              label="Theta"
              value={formatSignedNumber(getOpportunityNetTheta(opportunity), 2)}
            />
            <CellMetric
              label="Vega"
              value={formatSignedNumber(getOpportunityNetVega(opportunity), 2)}
            />
          </div>
        </div>
      </InspectorSection>

      <Separator />

      <InspectorSection title="Execution Plan">
        <div className="grid grid-cols-2 gap-x-4 gap-y-3">
          <CellMetric
            label="Quantity"
            value={orderQty == null ? "—" : `${formatQuantity(orderQty)} lot`}
          />
          <CellMetric
            label="Limit"
            value={formatPremiumValue(opportunity, orderLimitPrice)}
          />
          <CellMetric
            label="Natural"
            value={formatPremiumValue(opportunity, getOpportunityNaturalValue(opportunity))}
          />
          <CellMetric
            label="Fill"
            value={formatPercent(getOpportunityFillRatio(opportunity))}
          />
          <CellMetric
            label="Slip"
            value={formatSignedCurrency(getOpportunityEntrySlippage(opportunity))}
          />
          <CellMetric
            label="Exp Value"
            value={formatSignedCurrency(getOpportunityExpectedValue(opportunity))}
          />
          <CellMetric
            label="Type / TIF"
            value={`${humanize(orderType)} / ${humanize(orderTimeInForce)}`}
          />
          <CellMetric
            label="Structure"
            value={humanize(orderClass)}
          />
          <CellMetric
            label="Intent"
            value={humanize(orderIntent)}
          />
        </div>
      </InspectorSection>

      <Separator />

      <InspectorSection title="Provenance">
        <div className="grid gap-2">
          <BoardMetric
            label="Runtime"
            value={getOpportunityRuntimeLabel(opportunity)}
          />
          <BoardMetric
            label="Diagnostics"
            value={getOpportunityDiscoveryLabel(opportunity)}
          />
          <BoardMetric
            label="Generated"
            value={formatTimestamp(generatedAt)}
          />
          <BoardMetric
            label="State"
            value={`${humanize(lifecycleState)} · ${humanize(opportunity.selection_state)}`}
          />
          <BoardMetric
            label="Reason"
            value={humanize(getOpportunityStateReason(opportunity))}
          />
          <BoardMetric
            label="Policy"
            value={humanize(policyStatus)}
          />
          <BoardMetric
            label="Reason codes"
            value={reasonCodes.length ? reasonCodes.join(", ") : "—"}
          />
          <div className="mt-3 flex flex-wrap gap-2">
            {hasOpportunityRuntimeOwner(opportunity) ? (
              <Link
                href={getOpportunityRuntimeHref(opportunity)}
                className={buttonVariants({ variant: "outline", size: "sm" })}
              >
                Open runtime
              </Link>
            ) : null}
            <Link
              href={getOpportunityDiscoveryHref(opportunity)}
              className={buttonVariants({ variant: "outline", size: "sm" })}
            >
              Open diagnostics
            </Link>
          </div>
        </div>
      </InspectorSection>

      <div className="sticky bottom-0 flex items-center justify-between gap-3 border-t border-border/70 bg-background/95 px-5 py-4 backdrop-blur">
        <div className="min-w-0 text-sm text-muted-foreground">
          {executionFooterNote}
        </div>
        <div className="flex shrink-0 items-center gap-2">
          <Button type="button" variant="outline" onClick={onClose}>
            Close
          </Button>
          <Button
            type="button"
            disabled={!canExecute || executePending}
            onClick={() => onExecute(opportunity.opportunity_id)}
          >
            {canExecute ? "Execute" : executionDisabledLabel}
          </Button>
        </div>
      </div>
    </div>
  );
}

function OpportunityCell({
  opportunity,
}: {
  opportunity: Opportunity;
}) {
  const generatedAt = getOpportunityGeneratedAt(opportunity);
  const rankChip = getOpportunityRankChip(opportunity);
  const policyStatus = getOpportunityRankingPolicyStatus(opportunity);
  const lifecycleState = getOpportunityLifecycleState(opportunity);
  const eligibilityState = getOpportunityEligibilityState(opportunity);
  const showEligibilityBadge =
    eligibilityState !== "" &&
    eligibilityState !== "live" &&
    eligibilityState !== lifecycleState;

  return (
    <div className="min-w-[300px]">
      <div className="flex flex-wrap items-start gap-2">
        <div className="font-semibold">{opportunity.underlying_symbol}</div>
        <OpportunitySelectionBadge
          value={opportunity.selection_state}
          rankLabel={rankChip}
        />
        {hasOpportunityExceptionalLifecycle(opportunity) ? (
          <OpportunityLifecycleBadge value={lifecycleState} />
        ) : null}
        {showEligibilityBadge ? (
          <Badge variant="outline" className="rounded-full">
            {humanize(eligibilityState)}
          </Badge>
        ) : null}
      </div>
      <div className="text-xs text-muted-foreground">
        {humanize(opportunity.strategy_family)} · {humanize(getOpportunityBias(opportunity))} ·{" "}
        {humanize(getOpportunityProfile(opportunity))}
      </div>
      <div className="mt-1 text-xs text-muted-foreground">
        Age {formatAge(generatedAt)} · Policy {humanize(policyStatus)}
      </div>
      <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-xs">
        {hasOpportunityRuntimeOwner(opportunity) ? (
          <Link
            href={getOpportunityRuntimeHref(opportunity)}
            className="text-foreground underline-offset-4 hover:underline"
          >
            Runtime · {getOpportunityRuntimeLabel(opportunity)}
          </Link>
        ) : null}
        <Link
          href={getOpportunityDiscoveryHref(opportunity)}
          className="text-muted-foreground underline-offset-4 hover:underline"
        >
          Diagnostics · {getOpportunityDiscoveryLabel(opportunity)}
        </Link>
      </div>
    </div>
  );
}

function StructureCell({
  opportunity,
}: {
  opportunity: Opportunity;
}) {
  const expirationDate = getOpportunityExpirationDate(opportunity);
  const dte = getOpportunityDte(opportunity);
  const width = getOpportunityWidth(opportunity);
  const expectedMove = getOpportunityExpectedMoveDisplay(opportunity);
  const cushion = getOpportunityBreakevenCushionPct(opportunity);

  return (
    <div className="min-w-[288px] space-y-2">
      <div className="flex flex-wrap gap-2 text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
        <span>{getOpportunityLegCountLabel(opportunity)}</span>
        <span>{formatDate(expirationDate)}</span>
        {dte != null ? <span>{dte} DTE</span> : null}
      </div>
      <StructureLegChips opportunity={opportunity} />
      <div className="space-y-1 text-xs">
        <div className="flex flex-wrap gap-x-3 gap-y-1 text-muted-foreground">
          <span>
            Spot{" "}
            <span className="font-mono text-foreground">
              {formatQuantity(getOpportunityUnderlyingPrice(opportunity))}
            </span>
          </span>
          <span>
            BE{" "}
            <span className="font-mono text-foreground">
              {getOpportunityBreakevenDisplay(opportunity)}
            </span>
          </span>
        </div>
        <div className="flex flex-wrap gap-x-3 gap-y-1 text-muted-foreground">
          <span>
            EM{" "}
            <span className="font-mono text-foreground">{expectedMove}</span>
          </span>
          <span>
            Cushion{" "}
            <span className="font-mono text-foreground">
              {formatSignedPercent(cushion)}
            </span>
          </span>
          {width != null ? (
            <span>
              Width{" "}
              <span className="font-mono text-foreground">
                {formatQuantity(width)}
              </span>
            </span>
          ) : null}
        </div>
      </div>
    </div>
  );
}

function EconomicsCell({
  opportunity,
}: {
  opportunity: Opportunity;
}) {
  return (
    <div className="min-w-[236px] grid grid-cols-2 gap-x-4 gap-y-2">
      <CellMetric
        label="Mid"
        value={formatPremiumValue(opportunity, getOpportunityMidpointValue(opportunity))}
      />
      <CellMetric
        label="Nat"
        value={formatPremiumValue(opportunity, getOpportunityNaturalValue(opportunity))}
      />
      <CellMetric
        label={getOpportunityRiskAmountLabel(opportunity)}
        value={formatNullableCurrency(getOpportunityRiskAmount(opportunity))}
      />
      <CellMetric
        label="RoR"
        value={formatPercent(getOpportunityReturnOnRisk(opportunity))}
      />
      <CellMetric
        label="Fill"
        value={formatPercent(getOpportunityFillRatio(opportunity))}
      />
      <CellMetric
        label="Slip"
        value={formatSignedCurrency(getOpportunityEntrySlippage(opportunity))}
      />
    </div>
  );
}

function EdgeRiskCell({
  opportunity,
}: {
  opportunity: Opportunity;
}) {
  return (
    <div className="min-w-[236px] grid grid-cols-2 gap-x-4 gap-y-2">
      <CellMetric
        label="Promo"
        value={
          opportunity.promotion_score == null
            ? "—"
            : formatScore(opportunity.promotion_score)
        }
      />
      <CellMetric
        label="Exec"
        value={
          opportunity.execution_score == null
            ? "—"
            : formatScore(opportunity.execution_score)
        }
      />
      <CellMetric
        label="POP"
        value={formatPercent(getOpportunityProbabilityOfProfit(opportunity))}
      />
      <CellMetric
        label="Adj EV"
        value={formatSignedCurrency(getOpportunityAdjustedExpectedValue(opportunity))}
      />
      <CellMetric
        label="Delta"
        value={formatSignedNumber(getOpportunityNetDelta(opportunity), 2)}
      />
      <CellMetric
        label="Theta"
        value={formatSignedNumber(getOpportunityNetTheta(opportunity), 2)}
      />
    </div>
  );
}

function CellMetric({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone?: string;
}) {
  return (
    <div className="min-w-0">
      <div className="text-[10px] uppercase tracking-[0.16em] text-muted-foreground">
        {label}
      </div>
      <div className={cn("mt-1 truncate font-mono text-xs text-foreground", tone)}>
        {value}
      </div>
    </div>
  );
}

function legChipTone(role: string | null): string {
  if (role === "short") {
    return "border-rose-300/70 bg-rose-100/80 text-rose-950 dark:border-rose-900/80 dark:bg-rose-950/35 dark:text-rose-100";
  }
  if (role === "long") {
    return "border-sky-300/70 bg-sky-100/80 text-sky-950 dark:border-sky-900/80 dark:bg-sky-950/35 dark:text-sky-100";
  }
  return "border-border/70 bg-background/80 text-foreground";
}

function StructureLegChips({
  opportunity,
}: {
  opportunity: Opportunity;
}) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {getOpportunityLegs(opportunity).map((leg, index) => (
        <span
          key={`${readString(leg.symbol)}:${index}`}
          className={cn(
            "inline-flex items-center rounded-full border px-2 py-1 font-mono text-[11px]",
            legChipTone(getOpportunityLegRole(leg)),
          )}
        >
          {getOpportunityLegQuantityLabel(leg)} {getOpportunityLegStrikeCode(leg)}
        </span>
      ))}
    </div>
  );
}

export function OpportunitiesIndexPageContent({
  marketDate,
  botId,
  automationId,
  strategyConfigId,
  label,
  defaultMarketDate,
}: {
  marketDate?: string;
  botId?: string;
  automationId?: string;
  strategyConfigId?: string;
  label?: string;
  defaultMarketDate: string;
}) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const queryClient = useQueryClient();
  const hasOwnerScope = Boolean(botId && automationId);
  const ownerScopeLabel = hasOwnerScope
    ? `Runtime · ${botId} / ${automationId}`
    : label
      ? `Diagnostics · ${label}`
      : "All runtimes";
  const selectedMarketDate: string = isMarketDateValue(marketDate)
    ? marketDate
    : defaultMarketDate;
  const selectedOpportunityId =
    readString(searchParams.get("opportunityId"), "") || null;
  const showNonLive = searchParams.get("showNonLive") === "1";
  const historicalDateSelected = selectedMarketDate !== defaultMarketDate;
  const dateScopeLabel = formatDate(selectedMarketDate);
  const dateScopeDescription = historicalDateSelected
    ? "Historical review-only board"
    : "Current market date";
  const visibilityScopeLabel = showNonLive
    ? "Live + stale / expired"
    : "Live only";
  const visibilityScopeDescription = showNonLive
    ? "Diagnostics include stale and expired rows, and those rows stay non-executable."
    : "Only live rows are shown on the operator board.";

  function replaceSearchParams(nextParams: URLSearchParams) {
    const nextQuery = nextParams.toString();
    startTransition(() => {
      router.replace(nextQuery ? `${pathname}?${nextQuery}` : pathname, {
        scroll: false,
      });
    });
  }

  useEffect(() => {
    if (isMarketDateValue(marketDate)) {
      return;
    }
    const nextParams = new URLSearchParams(searchParams.toString());
    nextParams.set("marketDate", defaultMarketDate);
    startTransition(() => {
      router.replace(`${pathname}?${nextParams.toString()}`, {
        scroll: false,
      });
    });
  }, [defaultMarketDate, marketDate, pathname, router, searchParams]);

  const opportunitiesQuery = useQuery({
    queryKey: [
      "opportunities",
      selectedMarketDate,
      botId ?? "",
      automationId ?? "",
      strategyConfigId ?? "",
      label ?? "",
      showNonLive ? "with-non-live" : "live-only",
    ],
    queryFn: () =>
      getOpportunities({
        marketDate: selectedMarketDate,
        botId,
        automationId,
        strategyConfigId,
        label,
        includeNonLive: showNonLive,
        limit: 200,
      }),
  });
  const executeMutation = useMutation({
    mutationFn: (opportunityId: string) => executeOpportunity(opportunityId, {}),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["runtimes"] }),
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
  const liveOpportunities = opportunities.filter(isOpportunityLiveBoardRow);
  const inspectedOpportunity =
    selectedOpportunityId == null
      ? null
      : opportunities.find(
          (row) => row.opportunity_id === selectedOpportunityId,
        ) ?? null;
  const promotableCount = liveOpportunities.filter(
    (row) => row.selection_state === "promotable",
  ).length;
  const monitorCount = liveOpportunities.filter(
    (row) => row.selection_state === "monitor",
  ).length;
  const readyCount = liveOpportunities.filter(
    (row) => row.lifecycle_state === "ready",
  ).length;
  const staleCount = opportunities.filter(
    (row) => isOpportunityStale(row),
  ).length;
  const expiredCount = opportunities.filter(
    (row) => isOpportunityExpired(row),
  ).length;
  const runtimeCount = new Set(
    opportunities.flatMap((row) => {
      const owner = getOpportunityOwner(row);
      const botId = readString(owner.bot_id, "");
      const automationId = readString(owner.automation_id, "");
      if (botId && automationId) {
        return [`${botId}:${automationId}`];
      }
      return [];
    }),
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

  function replaceMarketDate(nextMarketDate: string) {
    const nextParams = new URLSearchParams(searchParams.toString());
    nextParams.set("marketDate", nextMarketDate);
    nextParams.delete("opportunityId");
    replaceSearchParams(nextParams);
  }

  function inspectOpportunity(opportunity: Opportunity) {
    const nextParams = new URLSearchParams(searchParams.toString());
    nextParams.set("opportunityId", opportunity.opportunity_id);
    replaceSearchParams(nextParams);
  }

  function clearInspector() {
    const nextParams = new URLSearchParams(searchParams.toString());
    nextParams.delete("opportunityId");
    replaceSearchParams(nextParams);
  }

  function setShowNonLive(nextShowNonLive: boolean) {
    const nextParams = new URLSearchParams(searchParams.toString());
    if (nextShowNonLive) {
      nextParams.set("showNonLive", "1");
    } else {
      nextParams.delete("showNonLive");
    }
    nextParams.delete("opportunityId");
    replaceSearchParams(nextParams);
  }

  function clearOwnerScope() {
    startTransition(() => {
      router.replace(
        buildOpportunitiesHref({
          marketDate: selectedMarketDate,
          showNonLive,
        }),
        { scroll: false },
      );
    });
  }

  const columns: ColumnDef<Opportunity>[] = [
    {
      id: "opportunity",
      header: "Opportunity",
      cell: ({ row }) => <OpportunityCell opportunity={row.original} />,
    },
    {
      id: "structure",
      header: "Structure",
      cell: ({ row }) => <StructureCell opportunity={row.original} />,
    },
    {
      id: "economics",
      header: "Economics",
      cell: ({ row }) => <EconomicsCell opportunity={row.original} />,
    },
    {
      id: "edge_risk",
      header: "Edge / Risk",
      cell: ({ row }) => <EdgeRiskCell opportunity={row.original} />,
    },
    {
      id: "actions",
      header: "Act",
      cell: ({ row }) => {
        if (historicalDateSelected) {
          return (
            <span className="text-xs uppercase tracking-[0.16em] text-muted-foreground">
              Review only
            </span>
          );
        }
        if (!isOpportunityLiveExecutable(row.original)) {
          return (
            <span className="text-xs uppercase tracking-[0.16em] text-muted-foreground">
              {isOpportunityConsumed(row.original) ? "Consumed" : "Unavailable"}
            </span>
          );
        }
        return (
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={executeMutation.isPending}
            onClick={() => executeMutation.mutate(row.original.opportunity_id)}
          >
            Execute
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
              <Badge variant="outline">
                {historicalDateSelected ? "Historical review" : "Today"}
              </Badge>
              <Badge variant="outline">{visibilityScopeLabel}</Badge>
              {hasOwnerScope ? (
                <Badge variant="outline">{ownerScopeLabel}</Badge>
              ) : null}
              {!hasOwnerScope && label ? (
                <Badge variant="outline">{ownerScopeLabel}</Badge>
              ) : null}
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">
              Opportunity board
            </div>
            <div className="mt-2 text-sm text-foreground/70">
              Work one market date at a time across live opportunities, with an
              optional stale and expired diagnostic view. Runtime ownership and
              diagnostics lineage stay attached to every row, but this surface
              stays centered on entry decisions. Current date scope:{" "}
              {dateScopeLabel}. Current visibility scope:{" "}
              {visibilityScopeLabel}. Current workspace scope: {ownerScopeLabel}.
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            <div className="flex items-center rounded-xl border border-border/70 bg-background/80 p-1">
              <Button
                type="button"
                size="sm"
                variant={showNonLive ? "ghost" : "secondary"}
                onClick={() => setShowNonLive(false)}
              >
                Live only
              </Button>
              <Button
                type="button"
                size="sm"
                variant={showNonLive ? "secondary" : "ghost"}
                onClick={() => setShowNonLive(true)}
              >
                Stale / expired
              </Button>
            </div>
            <MarketDateFilter
              selectedMarketDate={selectedMarketDate}
              defaultMarketDate={defaultMarketDate}
              onSelectMarketDate={replaceMarketDate}
            />
            <Button
              type="button"
              variant="outline"
              onClick={() => void opportunitiesQuery.refetch()}
            >
              <RefreshCw data-icon="inline-start" />
              Refresh
            </Button>
            {hasOwnerScope || label ? (
              <Button type="button" variant="outline" onClick={clearOwnerScope}>
                Clear scope
              </Button>
            ) : null}
          </div>
        </div>
      </div>

      {historicalDateSelected ? (
        <div className="rounded-2xl border border-amber-300/70 bg-amber-100/80 px-4 py-3 text-sm text-amber-950 dark:border-amber-900/80 dark:bg-amber-950/40 dark:text-amber-100">
          <div className="flex items-start gap-2">
            <TriangleAlert className="mt-0.5 size-4 shrink-0" />
            <span>
              Historical opportunity boards are review-only. Direct execution is
              disabled outside the current Chicago market date of{" "}
              {formatDate(defaultMarketDate)}.
            </span>
          </div>
        </div>
      ) : null}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-8">
        <MetricTile
          label="Date Scope"
          value={dateScopeLabel}
          note={dateScopeDescription}
        />
        <MetricTile
          label="Opportunities"
          value={String(opportunities.length)}
          note={
            historicalDateSelected
              ? "Rows captured for the selected date"
              : showNonLive
                ? "Live rows plus stale and expired diagnostics"
                : "Current live rows"
          }
        />
        <MetricTile
          label="Promotable"
          value={String(promotableCount)}
          note="Live selection leaders"
        />
        <MetricTile
          label="Monitor"
          value={String(monitorCount)}
          note="Live rows still tracking"
        />
        <MetricTile
          label="Ready"
          value={String(readyCount)}
          note="Live rows lifecycle ready"
        />
        <MetricTile
          label="Stale"
          value={String(staleCount)}
          note={
            showNonLive ? "Missed recent cycles" : "Hidden until toggle on"
          }
        />
        <MetricTile
          label="Expired"
          value={String(expiredCount)}
          note={
            showNonLive ? "Past board retention window" : "Hidden until toggle on"
          }
        />
        <MetricTile
          label="Runtimes"
          value={String(runtimeCount)}
          note={
            latestTimestamp
              ? `Updated ${formatTimestamp(latestTimestamp)}`
              : "No recent update"
          }
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
        description={
          historicalDateSelected
            ? "Historical boards are review-only. Select a row to inspect leg-native structure, economics, and diagnostics lineage in a detail drawer without leaving the board."
            : "Select a row to inspect leg-native structure, economics, runtime ownership, and lineage in a detail drawer without leaving the board."
        }
      >
        <div className="mb-4 flex flex-wrap items-center justify-between gap-3 rounded-2xl border border-border/70 bg-background/40 px-4 py-3">
          <div>
            <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
              Visibility
            </div>
            <div className="mt-1 text-sm text-foreground/80">
              {showNonLive
                ? visibilityScopeDescription
                : "Turn on stale / expired to inspect recent absences without leaving the page."}
            </div>
          </div>
          <Badge variant="outline" className="rounded-full">
            {visibilityScopeLabel}
          </Badge>
        </div>
        <DataTable
          columns={columns}
          data={opportunities}
          emptyMessage={
            showNonLive
              ? "No live, stale, or expired opportunities were available."
              : "No live opportunities were available."
          }
          getRowId={(row) => row.opportunity_id}
          onSelect={inspectOpportunity}
          selectedId={selectedOpportunityId}
          pageSize={20}
        />
        <Sheet
          open={selectedOpportunityId != null}
          onOpenChange={(open) => {
            if (!open) {
              clearInspector();
            }
          }}
        >
          <SheetContent
            side="right"
            className="p-0 data-[side=right]:w-full data-[side=right]:max-w-none data-[side=right]:md:w-[28rem] data-[side=right]:md:max-w-[28rem] data-[side=right]:lg:w-[45vw] data-[side=right]:lg:max-w-[45vw]"
          >
            <SheetHeader className="sr-only">
              <SheetTitle>Opportunity inspector</SheetTitle>
              <SheetDescription>
                Selected opportunity structure, rationale, and lineage.
              </SheetDescription>
            </SheetHeader>
            <div className="min-h-0 overflow-y-auto">
              {inspectedOpportunity ? (
                <OpportunityInspectorPanel
                  opportunity={inspectedOpportunity}
                  historicalDateSelected={historicalDateSelected}
                  executePending={executeMutation.isPending}
                  onExecute={(opportunityId) => executeMutation.mutate(opportunityId)}
                  onClose={clearInspector}
                />
              ) : (
                <OpportunityInspectorUnavailableState />
              )}
            </div>
          </SheetContent>
        </Sheet>
      </SectionSurface>
    </div>
  );
}
