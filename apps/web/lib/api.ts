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
    market_date: z.string().optional(),
    pipeline_id: z.string().nullable().optional(),
    opportunity_id: z.string().nullable().optional(),
    generated_at: z.string(),
    selection_state: z.enum(["promotable", "monitor"]),
    selection_rank: z.number(),
    state_reason: z.string(),
    origin: z.string(),
    eligibility: z.string(),
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
  session_date: z.string().optional(),
  market_date: z.string().optional(),
  generated_at: z.string(),
  universe_label: z.string(),
  strategy: z.string().optional(),
  strategy_mode: z.string().optional(),
  profile: z.string().optional(),
  legacy_profile: z.string().optional(),
  greeks_source: z.string(),
  pipeline_id: z.string().nullable().optional(),
  legacy_session_id: z.string().nullable().optional(),
  job_run_id: z.string().nullable().optional(),
  symbols: z.array(z.string()),
  failures: z.array(z.string()),
  summary: z.record(z.string(), z.unknown()).optional(),
  selection_memory: z.record(z.string(), z.unknown()).default({}),
  selection_counts: z
    .object({
      promotable: z.number(),
      monitor: z.number(),
    })
    .default({ promotable: 0, monitor: 0 }),
  opportunities: z.array(liveCandidateSchema),
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

const alertSchema = z
  .object({
    alert_id: z.number(),
    record_kind: z.string().optional(),
    created_at: z.string(),
    updated_at: z.string().optional(),
    session_date: z.string(),
    label: z.string(),
    session_id: z.string().nullable().optional(),
    cycle_id: z.string(),
    symbol: z.string(),
    alert_type: z.string(),
    dedupe_key: z.string(),
    status: z.string(),
    delivery_target: z.string().nullable().optional(),
    attempt_count: z.number().optional(),
    claimed_at: z.string().nullable().optional(),
    last_attempt_at: z.string().nullable().optional(),
    next_attempt_at: z.string().nullable().optional(),
    delivered_at: z.string().nullable().optional(),
    planner_job_run_id: z.string().nullable().optional(),
    delivery_job_run_id: z.string().nullable().optional(),
    worker_name: z.string().nullable().optional(),
    payload: z.record(z.string(), z.unknown()).nullable().optional(),
    state: z.record(z.string(), z.unknown()).nullable().optional(),
    response: z.record(z.string(), z.unknown()).nullable().optional(),
    error_text: z.string().nullable().optional(),
  })
  .passthrough();

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
    sync: z
      .object({
        status: z.string(),
        updated_at: z.string(),
        summary: z.record(z.string(), z.unknown()).default({}),
        error_text: z.string().nullable().optional(),
      })
      .nullable()
      .optional(),
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

const replayGroupSchema = z
  .object({
    group_value: z.string(),
    count: z.number(),
    matched_count: z.number().optional(),
    coverage_rate: z.number().nullable().optional(),
    pooled_estimated_close_return_on_risk: z.number().nullable().optional(),
    pooled_estimated_final_return_on_risk: z.number().nullable().optional(),
    pooled_actual_net_return_on_risk: z.number().nullable().optional(),
    pooled_actual_minus_estimated_close_return_on_risk: z
      .number()
      .nullable()
      .optional(),
    average_estimated_close_return_on_risk: z.number().nullable().optional(),
    average_estimated_final_return_on_risk: z.number().nullable().optional(),
    average_actual_net_return_on_risk: z.number().nullable().optional(),
    average_actual_minus_estimated_close_return_on_risk: z
      .number()
      .nullable()
      .optional(),
  })
  .passthrough();

const replayDeploymentSliceSchema = z
  .object({
    count: z.number().optional(),
    matched_count: z.number().optional(),
    coverage_rate: z.number().nullable().optional(),
    pooled_estimated_close_return_on_risk: z.number().nullable().optional(),
    pooled_estimated_final_return_on_risk: z.number().nullable().optional(),
    pooled_actual_net_return_on_risk: z.number().nullable().optional(),
    pooled_actual_minus_estimated_close_return_on_risk: z
      .number()
      .nullable()
      .optional(),
    by_profile: z.array(replayGroupSchema).default([]),
    by_strategy_family: z.array(replayGroupSchema).default([]),
    by_entry_return_on_risk_bucket: z.array(replayGroupSchema).default([]),
    by_midpoint_credit_bucket: z.array(replayGroupSchema).default([]),
    by_width_bucket: z.array(replayGroupSchema).default([]),
    by_dte_bucket: z.array(replayGroupSchema).default([]),
  })
  .passthrough();

const replayDeploymentQualitySchema = z
  .object({
    allocator_selected: replayDeploymentSliceSchema.nullable().optional(),
    actual_deployed: replayDeploymentSliceSchema.nullable().optional(),
  })
  .passthrough();

const decisionReplaySchema = z
  .object({
    warnings: z.array(z.string()).default([]),
    session: z.record(z.string(), z.unknown()).optional(),
    scorecard: z
      .object({
        deployment_quality: replayDeploymentQualitySchema.nullable().optional(),
      })
      .passthrough()
      .nullable()
      .optional(),
  })
  .passthrough();

const decisionReplayBatchSchema = z
  .object({
    warnings: z.array(z.string()).default([]),
    aggregate: z
      .object({
        session_count: z.number().optional(),
        deployment_quality: replayDeploymentQualitySchema.nullable().optional(),
      })
      .passthrough()
      .nullable()
      .optional(),
  })
  .passthrough();

const pipelineReplaySchema = z
  .object({
    include_replay: z.string(),
    recent_limit: z.number().optional(),
    current: decisionReplaySchema.nullable().optional(),
    recent: decisionReplayBatchSchema.nullable().optional(),
    warnings: z.array(z.string()).default([]),
  })
  .passthrough();

const autoExecutionSummarySchema = z
  .object({
    status: z.string(),
    changed: z.boolean().optional(),
    reason: z.string().nullable().optional(),
    message: z.string().nullable().optional(),
    selected_opportunity_id: z.string().nullable().optional(),
    selected_candidate_id: z.number().nullable().optional(),
    selected_symbol: z.string().nullable().optional(),
    selected_strategy_family: z.string().nullable().optional(),
    allocation_score: z.number().nullable().optional(),
    decision_reason: z.string().nullable().optional(),
    execution_blockers: z.array(z.string()).default([]),
    candidate_count: z.number().optional(),
    allocation_count: z.number().optional(),
    execution_intent_count: z.number().optional(),
    top_opportunity_id: z.string().nullable().optional(),
  })
  .passthrough();

const sessionListItemSchema = z
  .object({
    session_id: z.string(),
    pipeline_id: z.string().nullable().optional(),
    label: z.string(),
    session_date: z.string(),
    status: z.string(),
    latest_slot_at: z.string().nullable().optional(),
    latest_slot_status: z.string().nullable().optional(),
    latest_capture_status: z.string().nullable().optional(),
    latest_auto_execution: autoExecutionSummarySchema.nullable().optional(),
    latest_auto_execution_status: z.string().nullable().optional(),
    tradeability_state: z.string().nullable().optional(),
    tradeability_reason: z.string().nullable().optional(),
    tradeability_message: z.string().nullable().optional(),
    stream_quote_events_saved: z.number().optional(),
    websocket_quote_events_saved: z.number().optional(),
    baseline_quote_events_saved: z.number(),
    recovery_quote_events_saved: z.number(),
    promotable_count: z.number(),
    monitor_count: z.number(),
    alert_count: z.number(),
    updated_at: z.string().nullable().optional(),
  })
  .passthrough();

const sessionListResponseSchema = z.object({
  sessions: z.array(sessionListItemSchema),
});

const pipelineListItemSchema = z
  .object({
    pipeline_id: z.string(),
    label: z.string(),
    name: z.string().nullable().optional(),
    status: z.string(),
    latest_market_date: z.string(),
    legacy_session_id: z.string(),
    latest_slot_at: z.string().nullable().optional(),
    latest_slot_status: z.string().nullable().optional(),
    latest_capture_status: z.string().nullable().optional(),
    latest_auto_execution: autoExecutionSummarySchema.nullable().optional(),
    latest_auto_execution_status: z.string().nullable().optional(),
    tradeability_state: z.string().nullable().optional(),
    tradeability_reason: z.string().nullable().optional(),
    tradeability_message: z.string().nullable().optional(),
    promotable_count: z.number(),
    monitor_count: z.number(),
    alert_count: z.number(),
    updated_at: z.string().nullable().optional(),
    style_profile: z.string().nullable().optional(),
    horizon_intent: z.string().nullable().optional(),
    product_scope: z.record(z.string(), z.unknown()).nullable().optional(),
    policy: z.record(z.string(), z.unknown()).nullable().optional(),
  })
  .passthrough();

const pipelineListResponseSchema = z.object({
  pipelines: z.array(pipelineListItemSchema),
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
    pipeline_id: z.string().nullable().optional(),
    market_date: z.string().nullable().optional(),
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
    position_id: z.string().nullable().optional(),
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
    risk_status: z.string().nullable().optional(),
    risk_note: z.string().nullable().optional(),
    reconciliation_status: z.string().nullable().optional(),
    reconciliation_note: z.string().nullable().optional(),
    last_reconciled_at: z.string().nullable().optional(),
    last_exit_evaluated_at: z.string().nullable().optional(),
    last_exit_reason: z.string().nullable().optional(),
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
    mismatch_position_count: z.number().optional(),
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
    pipeline_id: z.string().nullable().optional(),
    label: z.string(),
    session_date: z.string(),
    status: z.string(),
    updated_at: z.string().nullable().optional(),
    risk_status: z.string().nullable().optional(),
    risk_note: z.string().nullable().optional(),
    reconciliation_status: z.string().nullable().optional(),
    reconciliation_note: z.string().nullable().optional(),
    tradeability_state: z.string().nullable().optional(),
    tradeability_reason: z.string().nullable().optional(),
    tradeability_message: z.string().nullable().optional(),
    latest_auto_execution: autoExecutionSummarySchema.nullable().optional(),
    latest_slot: jobRunSchema.nullable().optional(),
    current_cycle: liveResponseSchema.nullable().optional(),
    opportunities: z.array(liveCandidateSchema),
    analysis_only_opportunities: z.array(liveCandidateSchema).optional(),
    selection_counts: z
      .object({
        promotable: z.number(),
        monitor: z.number(),
      })
      .default({ promotable: 0, monitor: 0 }),
    slot_runs: z.array(jobRunSchema),
    alerts: z.array(alertSchema),
    events: z.array(liveEventSchema),
    executions: z.array(executionAttemptSchema),
    portfolio: sessionPortfolioSchema,
    analysis: sessionAnalysisSchema.nullable().optional(),
  })
  .passthrough();

const pipelineDetailSchema = sessionDetailSchema
  .extend({
    pipeline_id: z.string(),
    market_date: z.string(),
    legacy_session_id: z.string(),
    pipeline: z.record(z.string(), z.unknown()).nullable().optional(),
    cycles: z.array(z.record(z.string(), z.unknown())).default([]),
    replay: pipelineReplaySchema.nullable().optional(),
  })
  .passthrough();

const opportunitySchema = z
  .object({
    opportunity_id: z.string(),
    pipeline_id: z.string().nullable().optional(),
    market_date: z.string(),
    label: z.string(),
    root_symbol: z.string().nullable().optional(),
    underlying_symbol: z.string(),
    strategy_family: z.string(),
    style_profile: z.string().nullable().optional(),
    horizon_intent: z.string().nullable().optional(),
    product_class: z.string().nullable().optional(),
    lifecycle_state: z.string(),
    selection_state: z.string(),
    selection_rank: z.number().nullable().optional(),
    confidence: z.number().nullable().optional(),
    promotion_score: z.number().nullable().optional(),
    execution_score: z.number().nullable().optional(),
    legacy_session_id: z.string().nullable().optional(),
    order_payload: z.record(z.string(), z.unknown()).nullable().optional(),
    legs: z.array(z.record(z.string(), z.unknown())).default([]),
    economics: z.record(z.string(), z.unknown()).default({}),
    strategy_metrics: z.record(z.string(), z.unknown()).default({}),
    evidence: z.record(z.string(), z.unknown()).default({}),
  })
  .passthrough();

const opportunityListResponseSchema = z.object({
  opportunities: z.array(opportunitySchema),
});

const positionSchema = z
  .object({
    position_id: z.string(),
    pipeline_id: z.string(),
    market_date: z.string().nullable().optional(),
    position_status: z.string(),
    root_symbol: z.string(),
    strategy_family: z.string(),
    style_profile: z.string().nullable().optional(),
    horizon_intent: z.string().nullable().optional(),
    product_class: z.string().nullable().optional(),
    requested_quantity: z.number().optional(),
    opened_quantity: z.number().optional(),
    remaining_quantity: z.number().optional(),
    closed_quantity: z.number().nullable().optional(),
    entry_value: z.number().nullable().optional(),
    realized_pnl: z.number().nullable().optional(),
    unrealized_pnl: z.number().nullable().optional(),
    net_pnl: z.number().nullable().optional(),
    open_execution_attempt_id: z.string(),
    legacy_session_position_id: z.string().nullable().optional(),
    legs_json: z.array(z.record(z.string(), z.unknown())).default([]),
    economics_json: z.record(z.string(), z.unknown()).default({}),
    strategy_metrics_json: z.record(z.string(), z.unknown()).default({}),
    open_execution_attempt: executionAttemptSchema.nullable().optional(),
    closes: z.array(z.record(z.string(), z.unknown())).default([]),
  })
  .passthrough();

const positionListResponseSchema = z.object({
  summary: z.record(z.string(), z.unknown()),
  positions: z.array(positionSchema),
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
export type JobRun = z.infer<typeof jobRunSchema>;
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
export type PipelineListItem = z.infer<typeof pipelineListItemSchema>;
export type ExecutionOrder = z.infer<typeof executionOrderSchema>;
export type ExecutionFill = z.infer<typeof executionFillSchema>;
export type ExecutionAttempt = z.infer<typeof executionAttemptSchema>;
export type SessionPortfolioQuote = z.infer<typeof sessionPortfolioQuoteSchema>;
export type SessionPortfolioPosition = z.infer<typeof sessionPortfolioPositionSchema>;
export type SessionPortfolioSummary = z.infer<typeof sessionPortfolioSummarySchema>;
export type SessionPortfolio = z.infer<typeof sessionPortfolioSchema>;
export type SessionDetail = z.infer<typeof sessionDetailSchema>;
export type PipelineDetail = z.infer<typeof pipelineDetailSchema>;
export type Opportunity = z.infer<typeof opportunitySchema>;
export type Position = z.infer<typeof positionSchema>;
export type TuningBucket = z.infer<typeof tuningBucketSchema>;
export type ReplayGroup = z.infer<typeof replayGroupSchema>;
export type ReplayDeploymentSlice = z.infer<typeof replayDeploymentSliceSchema>;
export type ReplayDeploymentQuality = z.infer<typeof replayDeploymentQualitySchema>;
export type PipelineReplay = z.infer<typeof pipelineReplaySchema>;
export type AutoExecutionSummary = z.infer<typeof autoExecutionSummarySchema>;
export type SessionExecutionActionResponse = z.infer<typeof sessionExecutionActionResponseSchema>;
export type GlobalRealtimeEvent = z.infer<typeof globalRealtimeEventSchema>;
export type OpportunityExecutionRequest = {
  quantity?: number;
  limit_price?: number;
};
export type PositionCloseRequest = {
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

export function getAccountOverview(historyRange: AccountHistoryRange = "1D") {
  return fetchApi("account/overview", accountOverviewSchema, {
    history_range: historyRange,
  });
}

export function getPipelines(filters?: {
  marketDate?: string;
  limit?: number;
}) {
  return fetchApi("pipelines", pipelineListResponseSchema, {
    market_date: filters?.marketDate,
    limit: filters?.limit,
  });
}

export function getPipelineDetail(
  pipelineId: string,
  filters?: {
    marketDate?: string;
    includeReplay?: "none" | "current" | "recent" | "both";
  },
) {
  return fetchApi(
    `pipelines/${encodeURIComponent(pipelineId)}`,
    pipelineDetailSchema,
    {
      market_date: filters?.marketDate,
      include_replay: filters?.includeReplay,
    },
  );
}

export function getOpportunities(filters?: {
  pipelineId?: string;
  marketDate?: string;
  lifecycleState?: string;
  includeAnalysisOnly?: boolean;
  limit?: number;
}) {
  return fetchApi("opportunities", opportunityListResponseSchema, {
    pipeline_id: filters?.pipelineId,
    market_date: filters?.marketDate,
    lifecycle_state: filters?.lifecycleState,
    include_analysis_only: filters?.includeAnalysisOnly,
    limit: filters?.limit,
  });
}

export function getOpportunityDetail(opportunityId: string) {
  return fetchApi(
    `opportunities/${encodeURIComponent(opportunityId)}`,
    opportunitySchema,
  );
}

export function executeOpportunity(
  opportunityId: string,
  payload: OpportunityExecutionRequest,
) {
  return postApi(
    `opportunities/${encodeURIComponent(opportunityId)}/execute`,
    sessionExecutionActionResponseSchema,
    payload,
  );
}

export function getPositions(filters?: {
  pipelineId?: string;
  marketDate?: string;
  limit?: number;
}) {
  return fetchApi("positions", positionListResponseSchema, {
    pipeline_id: filters?.pipelineId,
    market_date: filters?.marketDate,
    limit: filters?.limit,
  });
}

export function getPositionDetail(positionId: string) {
  return fetchApi(`positions/${encodeURIComponent(positionId)}`, positionSchema);
}

export function closePosition(
  positionId: string,
  payload: PositionCloseRequest = {},
) {
  return postApi(
    `positions/${encodeURIComponent(positionId)}/close`,
    sessionExecutionActionResponseSchema,
    payload,
  );
}

export function refreshExecution(executionAttemptId: string) {
  return postApi(
    `executions/${encodeURIComponent(executionAttemptId)}/refresh`,
    sessionExecutionActionResponseSchema,
    {},
  );
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

export function parseGlobalRealtimeEvent(payload: string) {
  return globalRealtimeEventSchema.parse(JSON.parse(payload));
}

export function buildSessionHref(sessionId?: string | null) {
  if (!sessionId) {
    return "/sessions";
  }
  return `/sessions/${encodeURIComponent(sessionId)}`;
}

export function buildPipelineHref(pipelineId?: string | null, marketDate?: string | null) {
  if (!pipelineId) {
    return "/pipelines";
  }
  if (!marketDate) {
    return `/pipelines/${encodeURIComponent(pipelineId)}`;
  }
  return `/pipelines/${encodeURIComponent(pipelineId)}?marketDate=${encodeURIComponent(marketDate)}`;
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
