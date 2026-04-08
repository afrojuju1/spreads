import { z } from "zod";

export type GeneratorPageSearchParams = Record<string, string | string[] | undefined>;

export type GeneratorJobRequestPayload = {
  symbol: string;
  profile: string;
  strategy: string;
  greeks_source: string;
  top: number;
  min_credit?: number;
  short_delta_max?: number;
  short_delta_target?: number;
  allow_off_hours: boolean;
};

export const DEFAULT_GENERATOR_REQUEST: GeneratorJobRequestPayload = {
  symbol: "SPY",
  profile: "weekly",
  strategy: "combined",
  greeks_source: "auto",
  top: 5,
  allow_off_hours: false,
};

const generatorJobRequestSchema = z.object({
  symbol: z.string().min(1),
  profile: z.string().default(DEFAULT_GENERATOR_REQUEST.profile),
  strategy: z.string().default(DEFAULT_GENERATOR_REQUEST.strategy),
  greeks_source: z.string().default(DEFAULT_GENERATOR_REQUEST.greeks_source),
  top: z.number().int().min(1).max(25).default(DEFAULT_GENERATOR_REQUEST.top),
  min_credit: z.number().positive().optional(),
  short_delta_max: z.number().positive().optional(),
  short_delta_target: z.number().positive().optional(),
  allow_off_hours: z.boolean().default(DEFAULT_GENERATOR_REQUEST.allow_off_hours),
});

function firstValue(value: string | string[] | undefined): string | undefined {
  if (Array.isArray(value)) {
    return value[0];
  }
  return value;
}

function parseOptionalNumber(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value !== "string" || value.trim() === "") {
    return undefined;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function parseBoolean(value: unknown): boolean | undefined {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value !== "string") {
    return undefined;
  }
  if (value === "true") {
    return true;
  }
  if (value === "false") {
    return false;
  }
  return undefined;
}

export function normalizeGeneratorJobRequestRecord(
  request: Record<string, unknown>,
): GeneratorJobRequestPayload {
  return generatorJobRequestSchema.parse({
    symbol:
      typeof request.symbol === "string" && request.symbol.trim() !== ""
        ? request.symbol.trim().toUpperCase()
        : DEFAULT_GENERATOR_REQUEST.symbol,
    profile:
      typeof request.profile === "string" && request.profile.trim() !== ""
        ? request.profile
        : DEFAULT_GENERATOR_REQUEST.profile,
    strategy:
      typeof request.strategy === "string" && request.strategy.trim() !== ""
        ? request.strategy
        : DEFAULT_GENERATOR_REQUEST.strategy,
    greeks_source:
      typeof request.greeks_source === "string" && request.greeks_source.trim() !== ""
        ? request.greeks_source
        : DEFAULT_GENERATOR_REQUEST.greeks_source,
    top: parseOptionalNumber(request.top) ?? DEFAULT_GENERATOR_REQUEST.top,
    min_credit: parseOptionalNumber(request.min_credit),
    short_delta_max: parseOptionalNumber(request.short_delta_max),
    short_delta_target: parseOptionalNumber(request.short_delta_target),
    allow_off_hours: parseBoolean(request.allow_off_hours) ?? DEFAULT_GENERATOR_REQUEST.allow_off_hours,
  });
}

export function parseGeneratorPageRequest(
  searchParams: GeneratorPageSearchParams,
): GeneratorJobRequestPayload {
  return generatorJobRequestSchema.parse({
    symbol: firstValue(searchParams.symbol)?.trim().toUpperCase() || DEFAULT_GENERATOR_REQUEST.symbol,
    profile: firstValue(searchParams.profile) || DEFAULT_GENERATOR_REQUEST.profile,
    strategy: firstValue(searchParams.strategy) || DEFAULT_GENERATOR_REQUEST.strategy,
    greeks_source: firstValue(searchParams.greeks_source) || DEFAULT_GENERATOR_REQUEST.greeks_source,
    top: parseOptionalNumber(firstValue(searchParams.top)) ?? DEFAULT_GENERATOR_REQUEST.top,
    min_credit: parseOptionalNumber(firstValue(searchParams.min_credit)),
    short_delta_max: parseOptionalNumber(firstValue(searchParams.short_delta_max)),
    short_delta_target: parseOptionalNumber(firstValue(searchParams.short_delta_target)),
    allow_off_hours:
      parseBoolean(firstValue(searchParams.allow_off_hours)) ?? DEFAULT_GENERATOR_REQUEST.allow_off_hours,
  });
}

export function buildGeneratorFormHref(request: GeneratorJobRequestPayload): string {
  const params = new URLSearchParams();
  params.set("symbol", request.symbol);
  params.set("profile", request.profile);
  params.set("strategy", request.strategy);
  params.set("greeks_source", request.greeks_source);
  params.set("top", String(request.top));
  if (request.min_credit !== undefined) {
    params.set("min_credit", String(request.min_credit));
  }
  if (request.short_delta_max !== undefined) {
    params.set("short_delta_max", String(request.short_delta_max));
  }
  if (request.short_delta_target !== undefined) {
    params.set("short_delta_target", String(request.short_delta_target));
  }
  if (request.allow_off_hours) {
    params.set("allow_off_hours", "true");
  }
  return `/generator?${params.toString()}`;
}
