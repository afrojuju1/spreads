from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path
from typing import Any

from core.domain.models import (
    DailyBar,
    ExpectedMoveEstimate,
    IntradayBar,
    OptionContract,
    OptionSnapshot,
    SymbolMarketSlice,
    UnderlyingSetupContext,
)
from core.integrations.calendar_events.models import (
    CalendarEventReason,
    CalendarPolicyDecision,
)
from core.services.scanners.config import parse_args

REPO_ROOT = Path(__file__).resolve().parents[4]
REPLAY_ARTIFACT_ROOT = REPO_ROOT / "outputs" / "scanner_replays" / "artifacts"

_SYMBOL_ARG_KEYS = (
    "strategy",
    "profile",
    "session_label",
    "greeks_source",
    "evaluation_date",
    "evaluation_timestamp",
    "session_bucket_override",
    "min_dte",
    "max_dte",
    "short_delta_min",
    "short_delta_max",
    "short_delta_target",
    "min_width",
    "max_width",
    "min_credit",
    "min_open_interest",
    "max_relative_spread",
    "min_return_on_risk",
    "feed",
    "stock_feed",
    "calendar_policy",
    "refresh_calendar_events",
    "setup_filter",
    "expand_duplicates",
    "data_policy",
    "calendar_confidence_policy",
    "min_fill_ratio",
    "min_short_vs_expected_move_ratio",
    "min_breakeven_vs_expected_move_ratio",
    "ranking_min_probability_of_profit",
    "ranking_min_expected_value_dollars",
    "ranking_min_slippage_adjusted_expected_value_dollars",
    "ranking_max_entry_slippage_dollars",
    "ranking_min_model_implied_volatility",
    "ranking_max_model_implied_volatility",
    "ranking_weight_probability_of_profit",
    "ranking_weight_expected_value_dollars",
    "ranking_weight_slippage_adjusted_expected_value_dollars",
    "ranking_weight_entry_slippage_dollars",
    "ranking_weight_model_implied_volatility",
)


def _relative_repo_path(path: Path) -> str:
    resolved = path.expanduser().resolve()
    try:
        return str(resolved.relative_to(REPO_ROOT))
    except ValueError:
        return str(resolved)


def _resolve_repo_path(path: str) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return (REPO_ROOT / candidate).resolve()


def replay_artifact_output_path(run_id: str) -> Path:
    return REPLAY_ARTIFACT_ROOT / f"{run_id}.json"


def serialize_symbol_args(args: argparse.Namespace) -> dict[str, Any]:
    return {key: getattr(args, key, None) for key in _SYMBOL_ARG_KEYS}


def deserialize_symbol_args(payload: dict[str, Any] | None) -> argparse.Namespace:
    args = parse_args([])
    for key, value in dict(payload or {}).items():
        setattr(args, key, value)
    return args


def serialize_market_slice(market_slice: SymbolMarketSlice) -> dict[str, Any]:
    return dict(asdict(market_slice))


def _deserialize_contracts_by_expiration(
    payload: dict[str, list[dict[str, Any]]] | None,
) -> dict[str, list[OptionContract]]:
    return {str(expiration_date): [OptionContract(**dict(row)) for row in list(rows or [])] for expiration_date, rows in dict(payload or {}).items()}


def _deserialize_snapshots_by_expiration(
    payload: dict[str, dict[str, dict[str, Any]]] | None,
) -> dict[str, dict[str, OptionSnapshot]]:
    return {
        str(expiration_date): {str(symbol): OptionSnapshot(**dict(snapshot_payload)) for symbol, snapshot_payload in dict(snapshot_map or {}).items()}
        for expiration_date, snapshot_map in dict(payload or {}).items()
    }


def _deserialize_expected_moves(
    payload: dict[str, dict[str, Any]] | None,
) -> dict[str, ExpectedMoveEstimate]:
    return {str(expiration_date): ExpectedMoveEstimate(**dict(row)) for expiration_date, row in dict(payload or {}).items()}


def deserialize_market_slice(payload: dict[str, Any]) -> SymbolMarketSlice:
    row = dict(payload)
    return SymbolMarketSlice(
        symbol=str(row["symbol"]),
        underlying_type=str(row["underlying_type"]),
        spot_price=float(row["spot_price"]),
        daily_bars=tuple(DailyBar(**dict(bar)) for bar in list(row.get("daily_bars") or [])),
        intraday_bars=tuple(IntradayBar(**dict(bar)) for bar in list(row.get("intraday_bars") or [])),
        call_contracts_by_expiration=_deserialize_contracts_by_expiration(row.get("call_contracts_by_expiration")),
        put_contracts_by_expiration=_deserialize_contracts_by_expiration(row.get("put_contracts_by_expiration")),
        call_snapshots_by_expiration=_deserialize_snapshots_by_expiration(row.get("call_snapshots_by_expiration")),
        put_snapshots_by_expiration=_deserialize_snapshots_by_expiration(row.get("put_snapshots_by_expiration")),
        expected_moves_by_expiration=_deserialize_expected_moves(row.get("expected_moves_by_expiration")),
    )


def deserialize_setup_context(
    payload: dict[str, Any] | None,
) -> UnderlyingSetupContext | None:
    if payload is None:
        return None
    row = dict(payload)
    return UnderlyingSetupContext(
        strategy=str(row["strategy"]),
        status=str(row["status"]),
        score=float(row["score"]),
        reasons=tuple(str(value) for value in list(row.get("reasons") or [])),
        daily_score=row.get("daily_score"),
        intraday_score=row.get("intraday_score"),
        spot_vs_sma20_pct=row.get("spot_vs_sma20_pct"),
        sma20_vs_sma50_pct=row.get("sma20_vs_sma50_pct"),
        return_5d_pct=row.get("return_5d_pct"),
        distance_to_20d_extreme_pct=row.get("distance_to_20d_extreme_pct"),
        latest_close=row.get("latest_close"),
        sma20=row.get("sma20"),
        sma50=row.get("sma50"),
        source_window_days=int(row.get("source_window_days") or 0),
        spot_vs_vwap_pct=row.get("spot_vs_vwap_pct"),
        intraday_return_pct=row.get("intraday_return_pct"),
        distance_to_session_extreme_pct=row.get("distance_to_session_extreme_pct"),
        opening_range_break_pct=row.get("opening_range_break_pct"),
        vwap=row.get("vwap"),
        opening_range_high=row.get("opening_range_high"),
        opening_range_low=row.get("opening_range_low"),
        source_window_minutes=row.get("source_window_minutes"),
    )


def serialize_calendar_decisions_by_expiration(
    decisions_by_expiration: dict[str, CalendarPolicyDecision] | None,
) -> dict[str, dict[str, Any]]:
    return {str(expiration_date): dict(asdict(decision)) for expiration_date, decision in dict(decisions_by_expiration or {}).items()}


def _deserialize_calendar_reason(payload: dict[str, Any]) -> CalendarEventReason:
    return CalendarEventReason(
        code=str(payload["code"]),
        event_type=str(payload["event_type"]),
        severity=str(payload["severity"]),
        message=str(payload["message"]),
        scheduled_at=payload.get("scheduled_at"),
        source=payload.get("source"),
    )


def deserialize_calendar_decisions_by_expiration(
    payload: dict[str, dict[str, Any]] | None,
) -> dict[str, CalendarPolicyDecision]:
    decisions: dict[str, CalendarPolicyDecision] = {}
    for expiration_date, row in dict(payload or {}).items():
        decision_payload = dict(row)
        decisions[str(expiration_date)] = CalendarPolicyDecision(
            status=str(decision_payload["status"]),
            reasons=tuple(_deserialize_calendar_reason(dict(reason)) for reason in list(decision_payload.get("reasons") or [])),
            days_to_nearest_event=decision_payload.get("days_to_nearest_event"),
            events_before_expiry=int(decision_payload.get("events_before_expiry") or 0),
            assignment_risk=bool(decision_payload.get("assignment_risk")),
            macro_regime=decision_payload.get("macro_regime"),
            source_confidence=str(decision_payload.get("source_confidence") or "unknown"),
            sources=tuple(str(value) for value in list(decision_payload.get("sources") or [])),
            last_updated=decision_payload.get("last_updated"),
            earnings_phase=str(decision_payload.get("earnings_phase") or "clean"),
            earnings_event_date=decision_payload.get("earnings_event_date"),
            earnings_session_timing=str(decision_payload.get("earnings_session_timing") or "unknown"),
            earnings_cohort_key=decision_payload.get("earnings_cohort_key"),
            earnings_days_to_event=decision_payload.get("earnings_days_to_event"),
            earnings_days_since_event=decision_payload.get("earnings_days_since_event"),
            earnings_timing_confidence=str(decision_payload.get("earnings_timing_confidence") or "unknown"),
            earnings_horizon_crosses_report=bool(decision_payload.get("earnings_horizon_crosses_report")),
            earnings_primary_source=decision_payload.get("earnings_primary_source"),
            earnings_supporting_sources=tuple(str(value) for value in list(decision_payload.get("earnings_supporting_sources") or [])),
            earnings_consensus_status=str(decision_payload.get("earnings_consensus_status") or "missing"),
            earnings_enrichment=dict(decision_payload.get("earnings_enrichment") or {}),
        )
    return decisions


def write_scan_replay_artifact(
    *,
    run_id: str,
    generated_at: str,
    symbol_args: argparse.Namespace,
    market_slice: SymbolMarketSlice,
    setup_context: UnderlyingSetupContext | None,
    candidate_filter: dict[str, Any] | None,
    calendar_decisions_by_expiration: dict[str, CalendarPolicyDecision] | None,
) -> str:
    output_path = replay_artifact_output_path(run_id)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "kind": "scan_replay_artifact",
        "version": "v1",
        "run_id": run_id,
        "generated_at": generated_at,
        "symbol_args": serialize_symbol_args(symbol_args),
        "market_slice": serialize_market_slice(market_slice),
        "setup_context": None if setup_context is None else dict(asdict(setup_context)),
        "candidate_filter": dict(candidate_filter or {}),
        "calendar_decisions_by_expiration": serialize_calendar_decisions_by_expiration(calendar_decisions_by_expiration),
    }
    output_path.write_text(json.dumps(payload, indent=2, default=str) + "\n")
    return _relative_repo_path(output_path)


def load_scan_replay_artifact(path: str) -> dict[str, Any]:
    resolved_path = _resolve_repo_path(path)
    return dict(json.loads(resolved_path.read_text()))


__all__ = [
    "deserialize_calendar_decisions_by_expiration",
    "deserialize_market_slice",
    "deserialize_setup_context",
    "deserialize_symbol_args",
    "load_scan_replay_artifact",
    "replay_artifact_output_path",
    "serialize_calendar_decisions_by_expiration",
    "serialize_market_slice",
    "serialize_symbol_args",
    "write_scan_replay_artifact",
]
