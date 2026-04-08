import {
  head,
  isArray,
  isBoolean,
  isFinite,
  isNumber,
  isString,
  pickBy,
  toNumber,
  toUpper,
  trim,
} from "lodash-es";
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
  if (isArray(value)) {
    return head(value);
  }
  return value;
}

function parseOptionalNumber(value: unknown): number | undefined {
  if (isNumber(value) && isFinite(value)) {
    return value;
  }
  if (!isString(value) || trim(value) === "") {
    return undefined;
  }
  const parsed = toNumber(value);
  return isFinite(parsed) ? parsed : undefined;
}

function parseBoolean(value: unknown): boolean | undefined {
  if (isBoolean(value)) {
    return value;
  }
  if (!isString(value)) {
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
      isString(request.symbol) && trim(request.symbol) !== ""
        ? toUpper(trim(request.symbol))
        : DEFAULT_GENERATOR_REQUEST.symbol,
    profile:
      isString(request.profile) && trim(request.profile) !== ""
        ? request.profile
        : DEFAULT_GENERATOR_REQUEST.profile,
    strategy:
      isString(request.strategy) && trim(request.strategy) !== ""
        ? request.strategy
        : DEFAULT_GENERATOR_REQUEST.strategy,
    greeks_source:
      isString(request.greeks_source) && trim(request.greeks_source) !== ""
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
    symbol: firstValue(searchParams.symbol)
      ? toUpper(trim(firstValue(searchParams.symbol) ?? ""))
      : DEFAULT_GENERATOR_REQUEST.symbol,
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
  const values = pickBy(
    {
      symbol: request.symbol,
      profile: request.profile,
      strategy: request.strategy,
      greeks_source: request.greeks_source,
      top: request.top,
      min_credit: request.min_credit,
      short_delta_max: request.short_delta_max,
      short_delta_target: request.short_delta_target,
      allow_off_hours: request.allow_off_hours ? "true" : undefined,
    },
    (value) => value !== undefined,
  );
  for (const [key, value] of Object.entries(values)) {
    params.set(key, String(value));
  }
  return `/generator?${params.toString()}`;
}
