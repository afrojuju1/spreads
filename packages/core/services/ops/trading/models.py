from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.services.execution_intents.shared import OPEN_POSITION_STATES

OPEN_POSITION_STATUSES = sorted(OPEN_POSITION_STATES)
MARK_STALE_AFTER_SECONDS = 15 * 60
TOP_POSITION_LIMIT = 5
RECENT_ALERT_LIMIT = 200
SOURCE_SYMBOL_LIMIT = 25
ENTRY_QUALITY_STAGE_ORDER = (
    "source_preflight",
    "underlying_setup",
    "chain_viability",
    "contract_fit",
    "premium_quality",
    "selection",
)
LIFECYCLE_PROVENANCES = ("natural_strategy", "synthetic_validation", "operator_direct")
LATEST_LIFECYCLE_PROVENANCES = ("natural_strategy", "synthetic_validation")
BROKER_OPTION_ASSET_CLASSES = {"option", "us_option"}
NO_ENTRY_REASON_GROUPS = (
    (
        "market_context",
        "market context fit",
        ("market_context_", "market_regime_"),
        (),
    ),
    (
        "target_dte_chain",
        "target DTE chain viability",
        ("target_dte_",),
        (),
    ),
    (
        "expected_move_coverage",
        "partial expected-move coverage",
        (),
        ("partial_expected_move_coverage_gap",),
    ),
    (
        "option_liquidity",
        "option liquidity",
        ("open_interest_", "relative_spread_", "bid_ask_", "quote_size_"),
        (),
    ),
    (
        "market_data_completeness",
        "market data completeness",
        ("no_",),
        ("no_delta", "no_snapshot", "no_expected_move"),
    ),
    (
        "contract_fit",
        "contract fit",
        ("delta_", "dte_", "itm_call_"),
        ("delta_outside_range", "itm_call_skipped"),
    ),
)
NO_ENTRY_GROUP_CATEGORIES = {
    "contract_fit": "contract_fit",
    "expected_move_coverage": "data_quality",
    "market_context": "market_context",
    "market_data_completeness": "data_quality",
    "option_liquidity": "liquidity",
    "target_dte_chain": "data_quality",
}


@dataclass(frozen=True)
class _MarketControlProjection:
    market_date: str
    market_session: dict[str, Any]
    market_open: bool
    control: dict[str, Any]
    kill_switch_reason: str | None
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


@dataclass(frozen=True)
class _JobsProjection:
    payload: dict[str, Any]
    summary: dict[str, Any]
    details: dict[str, Any]
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


@dataclass(frozen=True)
class _AccountProjection:
    broker_sync_status: str
    broker_sync: dict[str, Any]
    account_snapshot: dict[str, Any]
    account: dict[str, Any]
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


@dataclass(frozen=True)
class _EngineProjection:
    payload: dict[str, Any]
    summary: dict[str, Any]
    status: str
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


@dataclass(frozen=True)
class _MarketContextProjection:
    payload: dict[str, Any]
    summary: dict[str, Any]
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


@dataclass(frozen=True)
class _ExecutionProjection:
    open_execution_attempts: list[dict[str, Any]]
    summarized_open_execution_attempts: list[dict[str, Any]]
    stale_open_execution_count: int
    submit_unknown_execution_count: int
    approved_admission_intent_gap_count: int
    approved_admission_intent_gap_ids: list[str]
    capacity_blocked_underlyings: list[str]
    execution_health_status: str
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


@dataclass(frozen=True)
class _PositionProjection:
    open_positions: list[dict[str, Any]]
    top_positions: list[dict[str, Any]]
    risk_breach_count: int
    reconciliation_mismatch_count: int
    missing_mark_count: int
    stale_mark_count: int
    mark_freshness_required: bool
    broker_unquoted_positions: int
    mark_error: str | None
    mark_health_status: str
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


@dataclass(frozen=True)
class _AlertProjection:
    alert_delivery: dict[str, Any]
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


@dataclass(frozen=True)
class _FlowProjection:
    trading_flows: list[dict[str, Any]]
    degraded_flows: list[dict[str, Any]]
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]


@dataclass(frozen=True)
class _StrategyBreadthProjection:
    payload: dict[str, Any]
    summary: dict[str, Any]


@dataclass(frozen=True)
class _ExecutionContractProjection:
    payload: dict[str, Any]
    summary: dict[str, Any]
    statuses: tuple[str, ...]
    attention: list[dict[str, str]]
