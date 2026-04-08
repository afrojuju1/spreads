import { z } from "zod";

const candidateDetailSchema = z
  .object({
    run_id: z.string().optional(),
    profile: z.string().optional(),
    strategy: z.string(),
    short_symbol: z.string(),
    long_symbol: z.string(),
    short_strike: z.number(),
    long_strike: z.number(),
    quality_score: z.number(),
    midpoint_credit: z.number(),
    setup_status: z.string().nullable().optional(),
    calendar_status: z.string().nullable().optional(),
    setup_reasons: z.array(z.string()).optional(),
    calendar_reasons: z.array(z.string()).optional(),
    board_notes: z.array(z.string()).optional(),
    greeks_source: z.string().optional(),
    expiration_date: z.string().optional(),
    underlying_symbol: z.string().optional(),
    underlying_price: z.number().optional(),
    expected_move: z.number().optional(),
    return_on_risk: z.number().optional(),
    fill_ratio: z.number().optional(),
    days_to_expiration: z.number().optional(),
  })
  .passthrough();

const liveCandidateSchema = z
  .object({
    candidate_id: z.number(),
    cycle_id: z.string(),
    label: z.string(),
    session_date: z.string(),
    generated_at: z.string(),
    bucket: z.enum(["board", "watchlist"]),
    position: z.number(),
    run_id: z.string(),
    underlying_symbol: z.string(),
    strategy: z.string(),
    expiration_date: z.string(),
    short_symbol: z.string(),
    long_symbol: z.string(),
    quality_score: z.number(),
    midpoint_credit: z.number(),
    candidate: candidateDetailSchema,
  })
  .passthrough();

const liveResponseSchema = z.object({
  cycle_id: z.string(),
  label: z.string(),
  session_date: z.string(),
  generated_at: z.string(),
  universe_label: z.string(),
  strategy: z.string(),
  profile: z.string(),
  greeks_source: z.string(),
  symbols: z.array(z.string()),
  failures: z.array(z.string()),
  selection_state: z.record(z.string(), z.unknown()).default({}),
  board_candidates: z.array(liveCandidateSchema),
  watchlist_candidates: z.array(liveCandidateSchema),
});

const liveEventSchema = z
  .object({
    cycle_id: z.string(),
    label: z.string(),
    session_date: z.string(),
    generated_at: z.string(),
    symbol: z.string(),
    event_type: z.string(),
    message: z.string(),
    previous_candidate: candidateDetailSchema.nullable().optional(),
    current_candidate: candidateDetailSchema.nullable().optional(),
  })
  .passthrough();

const liveEventsResponseSchema = z.object({
  events: z.array(liveEventSchema),
});

const alertSchema = z
  .object({
    alert_id: z.number(),
    created_at: z.string(),
    session_date: z.string(),
    label: z.string(),
    cycle_id: z.string(),
    symbol: z.string(),
    alert_type: z.string(),
    dedupe_key: z.string(),
    status: z.string(),
    delivery_target: z.string(),
    payload: z.record(z.string(), z.unknown()).nullable().optional(),
    response: z.record(z.string(), z.unknown()).nullable().optional(),
    error_text: z.string().nullable().optional(),
  })
  .passthrough();

const alertsResponseSchema = z.object({
  alerts: z.array(alertSchema),
});

const jobDefinitionSchema = z
  .object({
    job_key: z.string(),
    job_type: z.string(),
    enabled: z.boolean(),
    schedule_type: z.string(),
    schedule: z.record(z.string(), z.unknown()),
    payload: z.record(z.string(), z.unknown()),
    market_calendar: z.string().nullable().optional(),
    singleton_scope: z.string().nullable().optional(),
    created_at: z.string(),
    updated_at: z.string(),
  })
  .passthrough();

const jobsResponseSchema = z.object({
  jobs: z.array(jobDefinitionSchema),
});

const universesResponseSchema = z.record(z.string(), z.array(z.string()));

const generatorSymbolSuggestionSchema = z.object({
  symbol: z.string(),
  name: z.string().nullable().optional(),
  in_curated_universe: z.boolean(),
});

const generatorSymbolsResponseSchema = z.object({
  query: z.string(),
  source_status: z.enum(["alpaca", "fallback"]),
  symbols: z.array(generatorSymbolSuggestionSchema),
});

const jobRunSchema = z
  .object({
    job_run_id: z.string(),
    job_key: z.string(),
    job_type: z.string(),
    status: z.string(),
    scheduled_for: z.string().nullable().optional(),
    started_at: z.string().nullable().optional(),
    finished_at: z.string().nullable().optional(),
    heartbeat_at: z.string().nullable().optional(),
    worker_name: z.string().nullable().optional(),
    payload: z.record(z.string(), z.unknown()).nullable().optional(),
    result: z.record(z.string(), z.unknown()).nullable().optional(),
    error_text: z.string().nullable().optional(),
  })
  .passthrough();

const jobRunsResponseSchema = z.object({
  job_runs: z.array(jobRunSchema),
});

const jobsHealthSchema = z
  .object({
    scheduler: z.record(z.string(), z.unknown()).nullable().optional(),
    workers: z.array(z.record(z.string(), z.unknown())).optional(),
    running_jobs: z.array(jobRunSchema).optional(),
    queued_jobs: z.array(jobRunSchema).optional(),
    latest_successful_collectors: z.record(z.string(), z.unknown()).optional(),
  })
  .passthrough();

const sessionIdeaSchema = z
  .object({
    underlying_symbol: z.string(),
    strategy: z.string(),
    expiration_date: z.string(),
    short_symbol: z.string(),
    long_symbol: z.string(),
    classification: z.string(),
    first_seen: z.string(),
    entry_seen: z.string(),
    latest_seen: z.string(),
    entry_run_id: z.string(),
    entry_cycle_id: z.string(),
    first_board_seen: z.string().nullable().optional(),
    first_watchlist_seen: z.string().nullable().optional(),
    latest_score: z.number(),
    score_bucket: z.string(),
    occurrence_count: z.number(),
    replay_status: z.string(),
    replay_verdict: z.string().nullable().optional(),
    outcome_bucket: z.string(),
    estimated_close_pnl: z.number().nullable().optional(),
    estimated_expiry_pnl: z.number().nullable().optional(),
    profit_target_hit: z.boolean().nullable().optional(),
    stop_hit: z.boolean().nullable().optional(),
    still_in_play: z.boolean().nullable().optional(),
    entry_candidate: candidateDetailSchema,
    latest_candidate: candidateDetailSchema,
    setup_status: z.string().nullable().optional(),
    calendar_status: z.string().nullable().optional(),
    greeks_source: z.string().nullable().optional(),
    session_phase: z.string().nullable().optional(),
    vwap_regime: z.string().nullable().optional(),
    trend_regime: z.string().nullable().optional(),
    opening_range_regime: z.string().nullable().optional(),
    session_extreme_regime: z.string().nullable().optional(),
  })
  .passthrough();

const sessionSummarySchema = z
  .object({
    label: z.string(),
    session_date: z.string(),
    cycle_count: z.number(),
    latest_cycle: z.record(z.string(), z.unknown()).nullable().optional(),
    run_overview: z.record(z.string(), z.unknown()).nullable().optional(),
    quote_overview: z.record(z.string(), z.unknown()).nullable().optional(),
    event_overview: z.record(z.string(), z.unknown()).nullable().optional(),
    symbol_breakdown: z.array(z.record(z.string(), z.unknown())).optional(),
    leg_summaries: z.array(z.record(z.string(), z.unknown())).optional(),
    outcomes: z
      .object({
        session_date: z.string(),
        label: z.string(),
        idea_count: z.number(),
        counts_by_bucket: z.record(z.string(), z.number()).optional(),
        outcome_counts_by_bucket: z
          .record(z.string(), z.record(z.string(), z.number()))
          .optional(),
        average_estimated_pnl_by_bucket: z
          .record(z.string(), z.number().nullable())
          .optional(),
        by_symbol: z.record(z.string(), z.unknown()).optional(),
        by_strategy: z.record(z.string(), z.unknown()).optional(),
        by_score_bucket: z.record(z.string(), z.unknown()).optional(),
        ideas: z.array(sessionIdeaSchema),
      })
      .passthrough(),
    tuning: z.record(z.string(), z.unknown()).nullable().optional(),
  })
  .passthrough();

const tuningBucketSchema = z
  .object({
    bucket: z.string(),
    count: z.number().optional(),
    board_count: z.number().optional(),
    watchlist_count: z.number().optional(),
    win_count: z.number().optional(),
    loss_count: z.number().optional(),
    still_open_count: z.number().optional(),
    unavailable_count: z.number().optional(),
    resolved_count: z.number().optional(),
    win_rate: z.number().nullable().optional(),
    average_estimated_pnl: z.number().nullable().optional(),
    average_latest_score: z.number().nullable().optional(),
  })
  .passthrough();

const sessionTuningSchema = z.object({
  sample_size: z.number().optional(),
  dimensions: z.record(z.string(), z.array(tuningBucketSchema)).optional(),
  strongest_signals: z.array(tuningBucketSchema),
  weakest_signals: z.array(tuningBucketSchema),
  provisional_strongest_signals: z.array(tuningBucketSchema).optional(),
  provisional_weakest_signals: z.array(tuningBucketSchema).optional(),
});

const generatorReasonSchema = z
  .object({
    code: z.string(),
    message: z.string(),
    strategy: z.string().optional(),
    severity: z.string().optional(),
    details: z.record(z.string(), z.unknown()).optional(),
  })
  .passthrough();

const generatorDiagnosticGroupSchema = z
  .object({
    bucket: z.string(),
    reason_count: z.number(),
    reasons: z.array(generatorReasonSchema),
  })
  .passthrough();

const generatorRecommendationSchema = z
  .object({
    code: z.string(),
    title: z.string(),
    action: z.string(),
    reason: z.string(),
    priority: z.string().optional(),
  })
  .passthrough();

const generatorPreferredPlayExplanationSchema = z
  .object({
    summary: z.string(),
    strategy: z.string().optional(),
    short_symbol: z.string().optional(),
    long_symbol: z.string().optional(),
    short_strike: z.number().optional(),
    long_strike: z.number().optional(),
    quality_score: z.number().optional(),
    score_margin_vs_runner_up: z.number().nullable().optional(),
    setup_status: z.string().nullable().optional(),
    calendar_status: z.string().nullable().optional(),
    midpoint_credit: z.number().optional(),
    return_on_risk: z.number().nullable().optional(),
  })
  .passthrough();

const generatorDiagnosticsSchema = z
  .object({
    overview: z
      .object({
        status: z.string(),
        symbol: z.string(),
        profile: z.string(),
        strategy: z.string(),
        playability_verdict: z.string(),
      })
      .passthrough(),
    groups: z.array(generatorDiagnosticGroupSchema),
  })
  .passthrough();

const generatorStrategyComparisonSchema = z
  .object({
    strategy: z.string(),
    run_id: z.string(),
    setup_status: z.string().nullable().optional(),
    candidate_count: z.number(),
    quoted_contract_count: z.number(),
    alpaca_delta_contract_count: z.number(),
    delta_contract_count: z.number(),
    local_delta_contract_count: z.number(),
    blocker_codes: z.array(z.string()),
    blocker_summary: z.array(generatorReasonSchema),
  })
  .passthrough();

const generatorStrategyRunSchema = z
  .object({
    strategy: z.string(),
    run_id: z.string(),
    setup: z.record(z.string(), z.unknown()).nullable().optional(),
    candidate_count: z.number(),
    quoted_contract_count: z.number(),
    alpaca_delta_contract_count: z.number(),
    delta_contract_count: z.number(),
    local_delta_contract_count: z.number(),
    top_candidate: candidateDetailSchema.nullable().optional(),
    no_play_reasons: z.array(generatorReasonSchema),
  })
  .passthrough();

const generatorResponseSchema = z
  .object({
    status: z.enum(["ok", "no_play"]),
    generated_at: z.string(),
    symbol: z.string(),
    profile: z.string(),
    strategy: z.string(),
    greeks_source: z.string(),
    filters: z.record(z.string(), z.unknown()).optional(),
    preferred_play: candidateDetailSchema.nullable().optional(),
    top_candidates: z.array(candidateDetailSchema),
    strategy_runs: z.array(generatorStrategyRunSchema),
    rejection_summary: z.array(generatorReasonSchema),
    diagnostics: generatorDiagnosticsSchema,
    strategy_comparison: z.array(generatorStrategyComparisonSchema),
    preferred_play_explanation: generatorPreferredPlayExplanationSchema.nullable().optional(),
    recommendations: z.array(generatorRecommendationSchema),
    failures: z.array(z.record(z.string(), z.string())).optional(),
    request: z.record(z.string(), z.unknown()).optional(),
  })
  .passthrough();

const generatorJobSummarySchema = z
  .object({
    preferred_strategy: z.string().nullable().optional(),
    preferred_strikes: z.string().nullable().optional(),
    top_score: z.number().nullable().optional(),
    candidate_count: z.number(),
    rejection_count: z.number(),
  })
  .passthrough();

const generatorJobSchema = z
  .object({
    generator_job_id: z.string(),
    arq_job_id: z.string().nullable().optional(),
    symbol: z.string(),
    status: z.enum(["queued", "running", "succeeded", "no_play", "failed"]),
    created_at: z.string(),
    started_at: z.string().nullable().optional(),
    finished_at: z.string().nullable().optional(),
    request: z.record(z.string(), z.unknown()),
    result: generatorResponseSchema.nullable().optional(),
    summary: generatorJobSummarySchema,
    error_text: z.string().nullable().optional(),
  })
  .passthrough();

const generatorJobsResponseSchema = z.object({
  jobs: z.array(generatorJobSchema),
});

const generatorJobEventSchema = z.object({
  type: z.enum(["snapshot", "running", "completed", "failed", "error"]),
  detail: z.string().optional(),
  job: generatorJobSchema.optional(),
});

const globalRealtimeEventSchema = z.object({
  type: z.string(),
  topic: z.string(),
  entity_type: z.string(),
  entity_id: z.string(),
  timestamp: z.string(),
  payload: z.record(z.string(), z.unknown()).default({}),
});

export type CandidateDetail = z.infer<typeof candidateDetailSchema>;
export type LiveCandidate = z.infer<typeof liveCandidateSchema>;
export type LiveEvent = z.infer<typeof liveEventSchema>;
export type AlertRecord = z.infer<typeof alertSchema>;
export type JobDefinition = z.infer<typeof jobDefinitionSchema>;
export type JobRun = z.infer<typeof jobRunSchema>;
export type LiveResponse = z.infer<typeof liveResponseSchema>;
export type SessionIdea = z.infer<typeof sessionIdeaSchema>;
export type SessionSummary = z.infer<typeof sessionSummarySchema>;
export type SessionTuning = z.infer<typeof sessionTuningSchema>;
export type TuningBucket = z.infer<typeof tuningBucketSchema>;
export type GeneratorResponse = z.infer<typeof generatorResponseSchema>;
export type GeneratorJob = z.infer<typeof generatorJobSchema>;
export type GeneratorJobEvent = z.infer<typeof generatorJobEventSchema>;
export type GeneratorDiagnostics = z.infer<typeof generatorDiagnosticsSchema>;
export type GeneratorStrategyComparison = z.infer<typeof generatorStrategyComparisonSchema>;
export type GeneratorRecommendation = z.infer<typeof generatorRecommendationSchema>;
export type GlobalRealtimeEvent = z.infer<typeof globalRealtimeEventSchema>;
export type UniversesResponse = z.infer<typeof universesResponseSchema>;
export type GeneratorSymbolSuggestion = z.infer<typeof generatorSymbolSuggestionSchema>;
export type GeneratorSymbolsResponse = z.infer<typeof generatorSymbolsResponseSchema>;

async function fetchApi<T>(
  path: string,
  schema: z.ZodType<T>,
  searchParams?: Record<string, string | number | undefined>,
) {
  const url = new URL(`/api/backend/${path}`, window.location.origin);

  for (const [key, value] of Object.entries(searchParams ?? {})) {
    if (value !== undefined && value !== "") {
      url.searchParams.set(key, String(value));
    }
  }

  const response = await fetch(url, {
    headers: {
      accept: "application/json",
    },
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`API request failed: ${response.status} ${response.statusText}`);
  }

  const payload = await response.json();
  return schema.parse(payload);
}

async function postApi<TRequest, TResponse>(
  path: string,
  schema: z.ZodType<TResponse>,
  body: TRequest,
  searchParams?: Record<string, string | number | undefined>,
) {
  const url = new URL(`/api/backend/${path}`, window.location.origin);

  for (const [key, value] of Object.entries(searchParams ?? {})) {
    if (value !== undefined && value !== "") {
      url.searchParams.set(key, String(value));
    }
  }

  const response = await fetch(url, {
    method: "POST",
    headers: {
      accept: "application/json",
      "content-type": "application/json",
    },
    body: JSON.stringify(body),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`API request failed: ${response.status} ${response.statusText}`);
  }

  const payload = await response.json();
  return schema.parse(payload);
}

export function getLive(label: string) {
  return fetchApi(`live/${label}`, liveResponseSchema);
}

export function getUniverses() {
  return fetchApi("universes", universesResponseSchema);
}

export function getGeneratorSymbols(query: string, limit = 40) {
  return fetchApi("generator/symbols", generatorSymbolsResponseSchema, { query, limit });
}

export function getLiveEvents(label: string, limit = 10) {
  return fetchApi(`live/${label}/events`, liveEventsResponseSchema, { limit });
}

export function getAlerts(limit = 12) {
  return fetchApi("alerts/latest", alertsResponseSchema, { limit });
}

export function getJobs() {
  return fetchApi("jobs", jobsResponseSchema);
}

export function getJobRuns(limit = 8) {
  return fetchApi("jobs/runs", jobRunsResponseSchema, { limit });
}

export function getJobsHealth() {
  return fetchApi("jobs/health", jobsHealthSchema);
}

export function getSessionSummary(sessionDate: string, label: string) {
  return fetchApi(`sessions/${sessionDate}/${label}/summary`, sessionSummarySchema);
}

export function getSessionTuning(sessionDate: string, label: string) {
  return fetchApi(`sessions/${sessionDate}/${label}/tuning`, sessionTuningSchema);
}

export function generateIdeas(payload: {
  symbol: string;
  profile: string;
  strategy: string;
  greeks_source: string;
  top: number;
  min_credit?: number;
  short_delta_max?: number;
  short_delta_target?: number;
  allow_off_hours?: boolean;
}) {
  return postApi("generator/ideas", generatorResponseSchema, payload);
}

export function createGeneratorJob(payload: {
  symbol: string;
  profile: string;
  strategy: string;
  greeks_source: string;
  top: number;
  min_credit?: number;
  short_delta_max?: number;
  short_delta_target?: number;
  allow_off_hours?: boolean;
}) {
  return postApi("generator/jobs", generatorJobSchema, payload);
}

export function getGeneratorJobs(filters?: {
  symbol?: string;
  status?: string;
  limit?: number;
}) {
  return fetchApi("generator/jobs", generatorJobsResponseSchema, filters);
}

export function getGeneratorJob(generatorJobId: string) {
  return fetchApi(`generator/jobs/${generatorJobId}`, generatorJobSchema);
}

export function parseGeneratorJobEvent(payload: string) {
  return generatorJobEventSchema.parse(JSON.parse(payload));
}

export function buildGeneratorJobWebSocketUrl(generatorJobId: string) {
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const host = window.location.hostname;
  const backendPort =
    process.env.NEXT_PUBLIC_SPREADS_API_WS_PORT?.trim() ||
    process.env.NEXT_PUBLIC_SPREADS_API_PORT?.trim() ||
    "58080";
  return `${protocol}://${host}:${backendPort}/ws/generator/${generatorJobId}`;
}

export function parseGlobalRealtimeEvent(payload: string) {
  return globalRealtimeEventSchema.parse(JSON.parse(payload));
}

export function buildGlobalEventsWebSocketUrl() {
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const host = window.location.hostname;
  const backendPort =
    process.env.NEXT_PUBLIC_SPREADS_API_WS_PORT?.trim() ||
    process.env.NEXT_PUBLIC_SPREADS_API_PORT?.trim() ||
    "58080";
  return `${protocol}://${host}:${backendPort}/ws/events`;
}
