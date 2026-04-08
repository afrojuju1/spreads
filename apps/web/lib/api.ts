import { z } from "zod";

import type { GeneratorJobRequestPayload } from "@/lib/generator-request";

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

const accountSnapshotSchema = z
  .object({
    account_number: z.string().nullable().optional(),
    status: z.string().nullable().optional(),
    currency: z.string().nullable().optional(),
    equity: z.number().nullable().optional(),
    last_equity: z.number().nullable().optional(),
    cash: z.number().nullable().optional(),
    buying_power: z.number().nullable().optional(),
    regt_buying_power: z.number().nullable().optional(),
    daytrading_buying_power: z.number().nullable().optional(),
    non_marginable_buying_power: z.number().nullable().optional(),
    options_buying_power: z.number().nullable().optional(),
    portfolio_value: z.number().nullable().optional(),
    long_market_value: z.number().nullable().optional(),
    short_market_value: z.number().nullable().optional(),
    initial_margin: z.number().nullable().optional(),
    maintenance_margin: z.number().nullable().optional(),
    daytrade_count: z.number().nullable().optional(),
    pattern_day_trader: z.boolean().nullable().optional(),
    trading_blocked: z.boolean().nullable().optional(),
    transfers_blocked: z.boolean().nullable().optional(),
    account_blocked: z.boolean().nullable().optional(),
    shorting_enabled: z.boolean().nullable().optional(),
  })
  .passthrough();

const accountPnlSchema = z
  .object({
    day_change: z.number().nullable().optional(),
    day_change_percent: z.number().nullable().optional(),
  })
  .passthrough();

const accountHistoryRangeSchema = z.enum(["1D", "1W", "1M"]);

const accountHistoryPointSchema = z
  .object({
    timestamp: z.string(),
    equity: z.number().nullable().optional(),
    profit_loss: z.number().nullable().optional(),
    profit_loss_pct: z.number().nullable().optional(),
  })
  .passthrough();

const accountHistorySchema = z
  .object({
    range: accountHistoryRangeSchema,
    period: z.string().nullable().optional(),
    timeframe: z.string().nullable().optional(),
    intraday_reporting: z.string().nullable().optional(),
    base_value: z.number().nullable().optional(),
    points: z.array(accountHistoryPointSchema),
  })
  .passthrough();

const accountPositionSchema = z
  .object({
    asset_id: z.string().nullable().optional(),
    symbol: z.string(),
    asset_class: z.string().nullable().optional(),
    exchange: z.string().nullable().optional(),
    side: z.string().nullable().optional(),
    qty: z.number().nullable().optional(),
    qty_available: z.number().nullable().optional(),
    market_value: z.number().nullable().optional(),
    cost_basis: z.number().nullable().optional(),
    avg_entry_price: z.number().nullable().optional(),
    current_price: z.number().nullable().optional(),
    change_today: z.number().nullable().optional(),
    unrealized_pl: z.number().nullable().optional(),
    unrealized_plpc: z.number().nullable().optional(),
    unrealized_intraday_pl: z.number().nullable().optional(),
    unrealized_intraday_plpc: z.number().nullable().optional(),
  })
  .passthrough();

const accountOverviewSchema = z
  .object({
    broker: z.literal("alpaca"),
    environment: z.enum(["paper", "live", "custom"]),
    retrieved_at: z.string(),
    account: accountSnapshotSchema,
    pnl: accountPnlSchema,
    history: accountHistorySchema,
    positions: z.array(accountPositionSchema),
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
    tuning: z.lazy(() => sessionTuningSchema).nullable().optional(),
  })
  .passthrough();

const tuningBucketSchema = z
  .object({
    dimension: z.string().optional(),
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

const sessionListItemSchema = z
  .object({
    session_id: z.string(),
    label: z.string(),
    session_date: z.string(),
    status: z.string(),
    latest_slot_at: z.string().nullable().optional(),
    latest_slot_status: z.string().nullable().optional(),
    latest_capture_status: z.string().nullable().optional(),
    websocket_quote_events_saved: z.number(),
    baseline_quote_events_saved: z.number(),
    recovery_quote_events_saved: z.number(),
    board_count: z.number(),
    watchlist_count: z.number(),
    alert_count: z.number(),
    updated_at: z.string().nullable().optional(),
  })
  .passthrough();

const sessionListResponseSchema = z.object({
  sessions: z.array(sessionListItemSchema),
});

const sessionAnalysisSchema = sessionSummarySchema
  .extend({
    analysis_run: z.record(z.string(), z.unknown()),
  })
  .passthrough();

const executionOrderSchema = z
  .object({
    execution_order_id: z.number(),
    execution_attempt_id: z.string(),
    broker: z.string(),
    broker_order_id: z.string(),
    parent_broker_order_id: z.string().nullable().optional(),
    client_order_id: z.string().nullable().optional(),
    order_status: z.string(),
    order_type: z.string().nullable().optional(),
    time_in_force: z.string().nullable().optional(),
    order_class: z.string().nullable().optional(),
    side: z.string().nullable().optional(),
    symbol: z.string().nullable().optional(),
    leg_symbol: z.string().nullable().optional(),
    leg_side: z.string().nullable().optional(),
    position_intent: z.string().nullable().optional(),
    quantity: z.number().nullable().optional(),
    limit_price: z.number().nullable().optional(),
    filled_qty: z.number().nullable().optional(),
    filled_avg_price: z.number().nullable().optional(),
    submitted_at: z.string().nullable().optional(),
    updated_at: z.string(),
    order: z.record(z.string(), z.unknown()),
  })
  .passthrough();

const executionFillSchema = z
  .object({
    execution_fill_id: z.number(),
    execution_attempt_id: z.string(),
    execution_order_id: z.number().nullable().optional(),
    broker: z.string(),
    broker_fill_id: z.string(),
    broker_order_id: z.string(),
    symbol: z.string(),
    side: z.string().nullable().optional(),
    fill_type: z.string().nullable().optional(),
    quantity: z.number(),
    cumulative_quantity: z.number().nullable().optional(),
    remaining_quantity: z.number().nullable().optional(),
    price: z.number().nullable().optional(),
    filled_at: z.string(),
    fill: z.record(z.string(), z.unknown()),
  })
  .passthrough();

const executionAttemptSchema = z
  .object({
    execution_attempt_id: z.string(),
    session_id: z.string(),
    session_date: z.string(),
    label: z.string(),
    cycle_id: z.string().nullable().optional(),
    candidate_id: z.number().nullable().optional(),
    bucket: z.string().nullable().optional(),
    candidate_generated_at: z.string().nullable().optional(),
    run_id: z.string().nullable().optional(),
    job_run_id: z.string().nullable().optional(),
    underlying_symbol: z.string(),
    strategy: z.string(),
    expiration_date: z.string(),
    short_symbol: z.string(),
    long_symbol: z.string(),
    trade_intent: z.string(),
    session_position_id: z.string().nullable().optional(),
    quantity: z.number(),
    limit_price: z.number(),
    requested_at: z.string(),
    submitted_at: z.string().nullable().optional(),
    completed_at: z.string().nullable().optional(),
    status: z.string(),
    broker: z.string(),
    broker_order_id: z.string().nullable().optional(),
    client_order_id: z.string().nullable().optional(),
    request: z.record(z.string(), z.unknown()),
    candidate: candidateDetailSchema,
    error_text: z.string().nullable().optional(),
    orders: z.array(executionOrderSchema),
    fills: z.array(executionFillSchema),
  })
  .passthrough();

const sessionExecutionActionResponseSchema = z
  .object({
    action: z.enum(["submit", "refresh"]),
    changed: z.boolean(),
    message: z.string(),
    attempt: executionAttemptSchema,
  })
  .passthrough();

const sessionPortfolioQuoteSchema = z
  .object({
    symbol: z.string(),
    bid: z.number(),
    ask: z.number(),
    midpoint: z.number(),
    timestamp: z.string().nullable().optional(),
    source: z.string(),
  })
  .passthrough();

const sessionPortfolioPositionSchema = z
  .object({
    position_id: z.string(),
    execution_attempt_id: z.string(),
    session_position_id: z.string().optional(),
    open_execution_attempt_id: z.string().optional(),
    candidate_id: z.number().nullable().optional(),
    underlying_symbol: z.string(),
    strategy: z.string(),
    short_symbol: z.string(),
    long_symbol: z.string(),
    expiration_date: z.string().nullable().optional(),
    position_status: z.string(),
    broker_status: z.string(),
    requested_quantity: z.number().nullable().optional(),
    opened_quantity: z.number().nullable().optional(),
    remaining_quantity: z.number().nullable().optional(),
    closed_quantity: z.number().nullable().optional(),
    filled_quantity: z.number().optional(),
    entry_credit: z.number().nullable().optional(),
    entry_notional: z.number().nullable().optional(),
    width: z.number().nullable().optional(),
    max_profit: z.number().nullable().optional(),
    max_loss: z.number().nullable().optional(),
    opened_at: z.string().nullable().optional(),
    completed_at: z.string().nullable().optional(),
    closed_at: z.string().nullable().optional(),
    realized_pnl: z.number().nullable().optional(),
    unrealized_pnl: z.number().nullable().optional(),
    net_pnl: z.number().nullable().optional(),
    spread_mark_midpoint: z.number().nullable().optional(),
    spread_mark_close: z.number().nullable().optional(),
    estimated_midpoint_pnl: z.number().nullable().optional(),
    estimated_close_pnl: z.number().nullable().optional(),
    mark_source: z.string().nullable().optional(),
    mark_timestamp: z.string().nullable().optional(),
    short_quote: sessionPortfolioQuoteSchema.nullable().optional(),
    long_quote: sessionPortfolioQuoteSchema.nullable().optional(),
  })
  .passthrough();

const sessionPortfolioSummarySchema = z
  .object({
    position_count: z.number(),
    open_position_count: z.number(),
    partial_close_position_count: z.number().optional(),
    closed_position_count: z.number().optional(),
    filled_contract_count: z.number().optional(),
    opened_contract_count: z.number().optional(),
    remaining_contract_count: z.number().optional(),
    entry_notional_total: z.number().nullable().optional(),
    max_profit_total: z.number().nullable().optional(),
    max_loss_total: z.number().nullable().optional(),
    realized_pnl_total: z.number().nullable().optional(),
    unrealized_pnl_total: z.number().nullable().optional(),
    net_pnl_total: z.number().nullable().optional(),
    estimated_midpoint_pnl_total: z.number().nullable().optional(),
    estimated_close_pnl_total: z.number().nullable().optional(),
    quoted_position_count: z.number(),
    unquoted_position_count: z.number(),
    mark_source: z.string().nullable().optional(),
    mark_error: z.string().nullable().optional(),
    retrieved_at: z.string(),
  })
  .passthrough();

const sessionPortfolioSchema = z
  .object({
    summary: sessionPortfolioSummarySchema,
    positions: z.array(sessionPortfolioPositionSchema),
  })
  .passthrough();

const sessionDetailSchema = z
  .object({
    session_id: z.string(),
    label: z.string(),
    session_date: z.string(),
    status: z.string(),
    updated_at: z.string().nullable().optional(),
    latest_slot: jobRunSchema.nullable().optional(),
    current_cycle: liveResponseSchema.nullable().optional(),
    board_candidates: z.array(liveCandidateSchema),
    watchlist_candidates: z.array(liveCandidateSchema),
    slot_runs: z.array(jobRunSchema),
    alerts: z.array(alertSchema),
    events: z.array(liveEventSchema),
    executions: z.array(executionAttemptSchema),
    portfolio: sessionPortfolioSchema,
    analysis: sessionAnalysisSchema.nullable().optional(),
  })
  .passthrough();

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

const generatorJobActionResponseSchema = z
  .object({
    action: z.enum(["create_alert", "promote_live"]),
    changed: z.boolean(),
    message: z.string(),
    live_label: z.string().nullable().optional(),
    bucket: z.enum(["board", "watchlist"]).nullable().optional(),
    cycle_id: z.string().nullable().optional(),
    event_type: z.string().nullable().optional(),
    symbol: z.string().nullable().optional(),
    generated_at: z.string().nullable().optional(),
    board_count: z.number().optional(),
    watchlist_count: z.number().optional(),
    alert: alertSchema.nullable().optional(),
  })
  .passthrough();

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
export type AccountSnapshot = z.infer<typeof accountSnapshotSchema>;
export type AccountPnl = z.infer<typeof accountPnlSchema>;
export type AccountHistoryPoint = z.infer<typeof accountHistoryPointSchema>;
export type AccountHistory = z.infer<typeof accountHistorySchema>;
export type AccountPosition = z.infer<typeof accountPositionSchema>;
export type AccountOverview = z.infer<typeof accountOverviewSchema>;
export type AccountHistoryRange = z.infer<typeof accountHistoryRangeSchema>;
export type SessionIdea = z.infer<typeof sessionIdeaSchema>;
export type SessionSummary = z.infer<typeof sessionSummarySchema>;
export type SessionTuning = z.infer<typeof sessionTuningSchema>;
export type SessionListItem = z.infer<typeof sessionListItemSchema>;
export type ExecutionOrder = z.infer<typeof executionOrderSchema>;
export type ExecutionFill = z.infer<typeof executionFillSchema>;
export type ExecutionAttempt = z.infer<typeof executionAttemptSchema>;
export type SessionPortfolioQuote = z.infer<typeof sessionPortfolioQuoteSchema>;
export type SessionPortfolioPosition = z.infer<typeof sessionPortfolioPositionSchema>;
export type SessionPortfolioSummary = z.infer<typeof sessionPortfolioSummarySchema>;
export type SessionPortfolio = z.infer<typeof sessionPortfolioSchema>;
export type SessionDetail = z.infer<typeof sessionDetailSchema>;
export type TuningBucket = z.infer<typeof tuningBucketSchema>;
export type GeneratorResponse = z.infer<typeof generatorResponseSchema>;
export type GeneratorJob = z.infer<typeof generatorJobSchema>;
export type GeneratorJobActionResponse = z.infer<typeof generatorJobActionResponseSchema>;
export type SessionExecutionActionResponse = z.infer<typeof sessionExecutionActionResponseSchema>;
export type GeneratorJobEvent = z.infer<typeof generatorJobEventSchema>;
export type GeneratorDiagnostics = z.infer<typeof generatorDiagnosticsSchema>;
export type GeneratorStrategyComparison = z.infer<typeof generatorStrategyComparisonSchema>;
export type GeneratorRecommendation = z.infer<typeof generatorRecommendationSchema>;
export type GlobalRealtimeEvent = z.infer<typeof globalRealtimeEventSchema>;
export type UniversesResponse = z.infer<typeof universesResponseSchema>;
export type GeneratorSymbolSuggestion = z.infer<typeof generatorSymbolSuggestionSchema>;
export type GeneratorSymbolsResponse = z.infer<typeof generatorSymbolsResponseSchema>;
export type GeneratorCandidateActionRequest = {
  action: "create_alert" | "promote_live";
  strategy: string;
  short_symbol: string;
  long_symbol: string;
  live_label?: string;
  bucket?: "board" | "watchlist";
};
export type SessionExecutionRequest = {
  candidate_id: number;
  quantity?: number;
  limit_price?: number;
};
export type SessionPositionCloseRequest = {
  quantity?: number;
  limit_price?: number;
};

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
    let detail = "";
    try {
      const payload = await response.json();
      detail = typeof payload?.detail === "string" ? payload.detail : "";
    } catch {}
    throw new Error(
      detail || `API request failed: ${response.status} ${response.statusText}`,
    );
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
    let detail = "";
    try {
      const payload = await response.json();
      detail = typeof payload?.detail === "string" ? payload.detail : "";
    } catch {}
    throw new Error(
      detail || `API request failed: ${response.status} ${response.statusText}`,
    );
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

export function getAccountOverview(historyRange: AccountHistoryRange = "1D") {
  return fetchApi("account/overview", accountOverviewSchema, {
    history_range: historyRange,
  });
}

export function getSessions(filters?: {
  sessionDate?: string;
  limit?: number;
}) {
  return fetchApi("sessions", sessionListResponseSchema, {
    session_date: filters?.sessionDate,
    limit: filters?.limit,
  });
}

export function getSessionDetail(sessionId: string) {
  return fetchApi(`sessions/${encodeURIComponent(sessionId)}`, sessionDetailSchema);
}

export function createSessionExecution(
  sessionId: string,
  payload: SessionExecutionRequest,
) {
  return postApi(
    `sessions/${encodeURIComponent(sessionId)}/executions`,
    sessionExecutionActionResponseSchema,
    payload,
  );
}

export function closeSessionPosition(
  sessionId: string,
  sessionPositionId: string,
  payload: SessionPositionCloseRequest = {},
) {
  return postApi(
    `sessions/${encodeURIComponent(sessionId)}/positions/${encodeURIComponent(sessionPositionId)}/close`,
    sessionExecutionActionResponseSchema,
    payload,
  );
}

export function refreshSessionExecution(
  sessionId: string,
  executionAttemptId: string,
) {
  return postApi(
    `sessions/${encodeURIComponent(sessionId)}/executions/${encodeURIComponent(executionAttemptId)}/refresh`,
    sessionExecutionActionResponseSchema,
    {},
  );
}

export function generateIdeas(payload: GeneratorJobRequestPayload) {
  return postApi("generator/ideas", generatorResponseSchema, payload);
}

export function createGeneratorJob(payload: GeneratorJobRequestPayload) {
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

export function createGeneratorCandidateAction(
  generatorJobId: string,
  payload: GeneratorCandidateActionRequest,
) {
  return postApi(
    `generator/jobs/${generatorJobId}/actions`,
    generatorJobActionResponseSchema,
    payload,
  );
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

export function buildSessionHref(sessionId?: string | null) {
  if (!sessionId) {
    return "/";
  }
  return `/?session_id=${encodeURIComponent(sessionId)}`;
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
