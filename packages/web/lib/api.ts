import { z } from "zod";

const candidateDetailSchema = z
  .object({
    run_id: z.string().optional(),
    profile: z.string().optional(),
    strategy: z.string(),
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
    market_date: z.string().nullable().optional(),
    session_date: z.string(),
    label: z.string(),
    cycle_id: z.string().nullable().optional(),
    bucket: z.string().nullable().optional(),
    candidate_generated_at: z.string().nullable().optional(),
    run_id: z.string().nullable().optional(),
    job_run_id: z.string().nullable().optional(),
    underlying_symbol: z.string(),
    strategy: z.string(),
    expiration_date: z.string().nullable().optional(),
    trade_intent: z.string(),
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

const operatorActionResponseSchema = z
  .object({
    action: z.enum(["submit", "refresh", "cancel"]),
    changed: z.boolean(),
    message: z.string(),
    attempt: executionAttemptSchema,
  })
  .passthrough();

const ownerRefSchema = z
  .object({
    owner_kind: z.string().nullable().optional(),
    trading_strategy_id: z.string().nullable().optional(),
    config_hash: z.string().nullable().optional(),
    strategy_run_id: z.string().nullable().optional(),
  })
  .passthrough();

const sourceRefSchema = z
  .object({
    source_object_type: z.string().nullable().optional(),
    source_object_id: z.string().nullable().optional(),
  })
  .passthrough();

const positionSchema = z
  .object({
    position_id: z.string(),
    trading_strategy_id: z.string().nullable().optional(),
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
    legs_json: z.array(z.record(z.string(), z.unknown())).default([]),
    economics_json: z.record(z.string(), z.unknown()).default({}),
    strategy_metrics_json: z.record(z.string(), z.unknown()).default({}),
    open_execution_attempt: executionAttemptSchema.nullable().optional(),
    closes: z.array(z.record(z.string(), z.unknown())).default([]),
    owner: ownerRefSchema.optional(),
    source: sourceRefSchema.optional(),
  })
  .passthrough();

const positionListResponseSchema = z.object({
  summary: z.record(z.string(), z.unknown()),
  positions: z.array(positionSchema),
});

const executionRuntimeCapabilityItemSchema = z
  .object({
    name: z.string(),
    status: z.string(),
  })
  .passthrough();

const executionRuntimeItemSchema = z
  .object({
    runtime: z.string(),
    status: z.string(),
    ready: z.boolean(),
    reason: z.string().nullable().optional(),
    entry_strategy_count: z.number().default(0),
    strategy_families: z.record(z.string(), z.number()).default({}),
    capabilities: z.array(executionRuntimeCapabilityItemSchema).default([]),
  })
  .passthrough();

const executionRuntimeCapabilitiesSchema = z
  .object({
    schema_version: z.string(),
    default_runtime: z.string(),
    runtimes: z.array(executionRuntimeItemSchema),
  })
  .passthrough();

const operatorStateSchema = z
  .object({
    status: z.string().default("unknown"),
    generated_at: z.string().nullable().optional(),
    summary: z.record(z.string(), z.unknown()).default({}),
    attention: z.array(z.record(z.string(), z.unknown())).default([]),
    details: z.record(z.string(), z.unknown()).default({}),
  })
  .passthrough();

const globalRealtimeEventSchema = z.object({
  type: z.string(),
  topic: z.string(),
  entity_type: z.string(),
  entity_id: z.string(),
  timestamp: z.string(),
  payload: z.record(z.string(), z.unknown()).default({}),
});

export type CandidateDetail = z.infer<typeof candidateDetailSchema>;
export type AccountSnapshot = z.infer<typeof accountSnapshotSchema>;
export type AccountPnl = z.infer<typeof accountPnlSchema>;
export type AccountHistoryPoint = z.infer<typeof accountHistoryPointSchema>;
export type AccountHistory = z.infer<typeof accountHistorySchema>;
export type AccountPosition = z.infer<typeof accountPositionSchema>;
export type AccountOverview = z.infer<typeof accountOverviewSchema>;
export type AccountHistoryRange = z.infer<typeof accountHistoryRangeSchema>;
export type ExecutionOrder = z.infer<typeof executionOrderSchema>;
export type ExecutionFill = z.infer<typeof executionFillSchema>;
export type ExecutionAttempt = z.infer<typeof executionAttemptSchema>;
export type OwnerRef = z.infer<typeof ownerRefSchema>;
export type SourceRef = z.infer<typeof sourceRefSchema>;
export type Position = z.infer<typeof positionSchema>;
export type ExecutionRuntimeCapabilities = z.infer<typeof executionRuntimeCapabilitiesSchema>;
export type TradingOpsState = z.infer<typeof operatorStateSchema>;
export type StorageOpsState = z.infer<typeof operatorStateSchema>;
export type GlobalRealtimeEvent = z.infer<typeof globalRealtimeEventSchema>;
export type PositionCloseRequest = {
  quantity?: number;
  limit_price?: number;
  execution_runtime?: string;
};
export type EquityOrderRequest = {
  symbol: string;
  side: "buy" | "sell";
  quantity: number;
  limit_price: number;
  time_in_force?: "day" | "gtc";
  label?: string;
  market_date?: string;
  execution_runtime?: string;
};

async function fetchApi<T>(
  path: string,
  schema: z.ZodType<T>,
  searchParams?: Record<string, string | number | boolean | undefined>,
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
  searchParams?: Record<string, string | number | boolean | undefined>,
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

export function getExecutionRuntimes() {
  return fetchApi("executions/runtimes", executionRuntimeCapabilitiesSchema);
}

export function getTradingOpsState(filters?: {
  marketDate?: string;
}) {
  return fetchApi("internal/trading-ops/state", operatorStateSchema, {
    market_date: filters?.marketDate,
  });
}

export function getStorageOpsState() {
  return fetchApi("internal/storage-ops/state", operatorStateSchema);
}

export function submitEquityOrder(payload: EquityOrderRequest) {
  return postApi(
    "executions/equity-orders",
    operatorActionResponseSchema,
    payload,
  );
}

export function getPositions(filters?: {
  marketDate?: string;
  tradingStrategyId?: string;
  limit?: number;
}) {
  return fetchApi("positions", positionListResponseSchema, {
    market_date: filters?.marketDate,
    trading_strategy_id: filters?.tradingStrategyId,
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
    operatorActionResponseSchema,
    payload,
  );
}

export function refreshExecution(executionAttemptId: string) {
  return postApi(
    `executions/${encodeURIComponent(executionAttemptId)}/refresh`,
    operatorActionResponseSchema,
    {},
  );
}

export function cancelExecution(executionAttemptId: string) {
  return postApi(
    `executions/${encodeURIComponent(executionAttemptId)}/cancel`,
    operatorActionResponseSchema,
    {},
  );
}

export function parseGlobalRealtimeEvent(payload: string) {
  return globalRealtimeEventSchema.parse(JSON.parse(payload));
}

export function buildPositionsHref(filters?: {
  marketDate?: string | null;
  tradingStrategyId?: string | null;
  [key: string]: unknown;
}) {
  const params = new URLSearchParams();
  if (filters?.marketDate) {
    params.set("marketDate", filters.marketDate);
  }
  if (filters?.tradingStrategyId) {
    params.set("tradingStrategyId", filters.tradingStrategyId);
  }
  const query = params.toString();
  return query ? `/positions?${query}` : "/positions";
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
