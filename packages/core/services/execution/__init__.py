from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.db.decorators import with_storage
from core.domain.opportunity_models import Opportunity, OpportunityLeg
from core.services.alpaca import create_alpaca_client_from_env
from core.services.candidate_policy import (
    candidate_has_intraday_setup_context,
    resolve_candidate_profile,
    resolve_deployment_quality_thresholds,
)
from core.services.control_plane import (
    OPEN_ACTIVITY_AUTO,
    OPEN_ACTIVITY_MANUAL,
    assess_open_activity_gate,
    get_active_policy_rollout_map,
    publish_control_gate_event,
)
from core.services.deployment_policy import (
    DEPLOYMENT_MODE_PAPER_AUTO,
    deployment_mode_auto_executes,
)
from core.services.exit_manager import (
    resolve_exit_policy_snapshot,
)
from core.services.execution_lifecycle import (
    PENDING_SUBMISSION_STATUS,
    SUBMIT_UNKNOWN_STATUS,
)
from core.services.execution_portfolio import build_structure_quote_snapshot
from core.services.option_structures import (
    build_multileg_order_payload,
    candidate_legs,
    closing_legs,
    legs_identity_key,
    net_premium_kind,
    normalize_legs,
    primary_short_long_symbols,
    structure_quote_snapshot,
)
from core.services.opportunity_execution_plan import build_execution_plan
from core.services.positions import enrich_position_row
from core.services.runtime_identity import (
    build_live_run_scope_id,
    build_pipeline_id,
    resolve_pipeline_policy_fields,
)
from core.services.risk_manager import (
    evaluate_open_execution,
    normalize_risk_policy,
    validate_close_execution,
)
from core.services.signal_state import publish_opportunity_event
from core.services.session_positions import (
    CLOSE_TRADE_INTENT,
    OPEN_TRADE_INTENT,
    resolve_trade_intent,
)
from core.services.value_coercion import (
    as_text as _as_text,
    coerce_float as _coerce_float,
    coerce_int as _coerce_int,
    utc_now_iso as _utc_now,
)
from core.storage.serializers import parse_datetime

from .attempts import (
    _get_attempt_payload,
    _publish_execution_attempt_event,
    _publish_risk_decision_event,
    _queue_execution_attempt,
    _reconcile_submit_unknown_attempt,
    _require_execution_schema,
    _require_position_schema,
    _submission_message,
    _sync_attempt_state,
    _sync_linked_execution_intent,
    list_session_execution_attempts as list_session_execution_attempts,
)
from .guard import run_open_execution_guard as run_open_execution_guard
from .policy import (
    _build_policy_refs,
    _requested_policy_payload,
    _resolve_source_policies,
    _validate_open_timing_window,
    normalize_execution_policy,
)
from .shared import (
    BROKER_NAME,
    DEFAULT_ENTRY_PRICING_MODE,
    DEFAULT_MAX_CREDIT_CONCESSION,
    DEFAULT_MIN_CREDIT_RETENTION_PCT,
    EXECUTION_SCHEMA_MESSAGE as EXECUTION_SCHEMA_MESSAGE,
    OPEN_STATUSES,
    _candidate_with_payload,
    _clamp_fraction,
    _execution_attempt_id,
    _execution_attempt_identity,
    _execution_client_order_id,
    _is_terminal_status,
    _normalize_attempt_context,
    _normalize_limit_value,
    _resolve_completed_at,
    _risk_decision_id,
    _strategy_family_from_payload,
)


def _opportunity_legs_from_row(opportunity: Mapping[str, Any]) -> list[OpportunityLeg]:
    legs_payload = opportunity.get("legs")
    if not isinstance(legs_payload, list):
        execution_shape = opportunity.get("execution_shape")
        if isinstance(execution_shape, Mapping):
            order_payload = execution_shape.get("order_payload")
            if isinstance(order_payload, Mapping):
                legs_payload = order_payload.get("legs")
    resolved_legs = normalize_legs(legs_payload)
    if not resolved_legs:
        return []
    built: list[OpportunityLeg] = []
    for index, leg in enumerate(resolved_legs, start=1):
        built.append(
            OpportunityLeg(
                leg_index=index,
                symbol=str(leg["symbol"]),
                side=str(leg["side"] or ""),
                position_intent=_as_text(leg.get("position_intent")),
                ratio_qty=_as_text(leg.get("ratio_qty")),
            )
        )
    return built


def _opportunity_execution_blockers_from_row(
    opportunity: Mapping[str, Any],
) -> list[str]:
    candidate = opportunity.get("candidate")
    if isinstance(candidate, Mapping):
        blockers = candidate.get("execution_blockers")
        if isinstance(blockers, list):
            rendered = [str(value) for value in blockers if str(value or "").strip()]
            if rendered:
                return rendered
        score_evidence = candidate.get("score_evidence")
        if isinstance(score_evidence, Mapping):
            blockers = score_evidence.get("execution_blockers")
            if isinstance(blockers, list):
                rendered = [
                    str(value) for value in blockers if str(value or "").strip()
                ]
                if rendered:
                    return rendered
    evidence = opportunity.get("evidence")
    if isinstance(evidence, Mapping):
        blockers = evidence.get("execution_blockers")
        if isinstance(blockers, list):
            return [str(value) for value in blockers if str(value or "").strip()]
    return []


def _plan_opportunity_from_signal_row(
    opportunity: Mapping[str, Any],
) -> Opportunity | None:
    opportunity_id = _as_text(opportunity.get("opportunity_id"))
    cycle_id = _as_text(opportunity.get("cycle_id")) or _as_text(
        opportunity.get("source_cycle_id")
    )
    label = _as_text(opportunity.get("label"))
    market_date = _as_text(opportunity.get("market_date")) or _as_text(
        opportunity.get("session_date")
    )
    candidate_id = _coerce_int(opportunity.get("source_candidate_id"))
    if (
        opportunity_id is None
        or cycle_id is None
        or label is None
        or market_date is None
        or candidate_id is None
    ):
        return None

    candidate = (
        dict(opportunity.get("candidate") or {})
        if isinstance(opportunity.get("candidate"), Mapping)
        else {}
    )
    policy_fields = resolve_pipeline_policy_fields(
        profile=candidate.get("profile") or opportunity.get("profile"),
        root_symbol=str(
            candidate.get("underlying_symbol")
            or opportunity.get("underlying_symbol")
            or ""
        ),
    )
    execution_blockers = _opportunity_execution_blockers_from_row(opportunity)
    scoring_state = _as_text(candidate.get("scoring_state"))
    if execution_blockers:
        state = "blocked"
        state_reason = "Execution blockers are present on the live opportunity."
    elif scoring_state in {"promotable", "monitor", "blocked"}:
        state = str(scoring_state)
        state_reason = (
            _as_text(candidate.get("scoring_state_reason"))
            or _as_text(opportunity.get("state_reason"))
            or "selected"
        )
    elif _as_text(opportunity.get("selection_state")) == "promotable":
        state = "promotable"
        state_reason = (
            _as_text(opportunity.get("state_reason")) or "selected_promotable"
        )
    else:
        state = "monitor"
        state_reason = _as_text(opportunity.get("state_reason")) or "selected_monitor"

    evidence = (
        dict(opportunity.get("evidence") or {})
        if isinstance(opportunity.get("evidence"), Mapping)
        else {}
    )
    score_evidence = candidate.get("score_evidence")
    if isinstance(score_evidence, Mapping):
        profile_score_evidence = score_evidence.get("profile_score_evidence")
        if isinstance(profile_score_evidence, Mapping):
            evidence["profile_score_evidence"] = dict(profile_score_evidence)
    execution_score = _coerce_float(opportunity.get("execution_score"))
    if execution_score is None:
        execution_score = _coerce_float(candidate.get("execution_score"))
    if execution_score is not None:
        evidence["execution_score"] = execution_score
    evidence["execution_blockers"] = execution_blockers
    evidence["selection_state"] = opportunity.get("selection_state")
    evidence["candidate_id"] = candidate_id

    risk_hints = (
        dict(opportunity.get("risk_hints") or {})
        if isinstance(opportunity.get("risk_hints"), Mapping)
        else {}
    )
    style_profile = (
        _as_text(candidate.get("score_style_profile"))
        or _as_text(opportunity.get("style_profile"))
        or str(policy_fields["style_profile"])
    )
    if style_profile not in {"reactive", "tactical", "carry"}:
        style_profile = str(policy_fields["style_profile"])
    if style_profile not in {"reactive", "tactical", "carry"}:
        style_profile = "tactical"
    return Opportunity(
        opportunity_id=opportunity_id,
        cycle_id=cycle_id,
        session_id=build_live_run_scope_id(label, market_date),
        candidate_id=candidate_id,
        symbol=str(
            opportunity.get("underlying_symbol")
            or candidate.get("underlying_symbol")
            or ""
        ),
        legacy_strategy=str(
            candidate.get("strategy") or opportunity.get("strategy_family") or "unknown"
        ),
        expiration_date=str(
            opportunity.get("expiration_date") or candidate.get("expiration_date") or ""
        ),
        short_symbol=str(candidate.get("short_symbol") or ""),
        long_symbol=str(candidate.get("long_symbol") or ""),
        style_profile=style_profile,
        strategy_family=str(
            opportunity.get("strategy_family") or candidate.get("strategy") or "unknown"
        ),
        regime_snapshot_id=f"live_regime:{opportunity_id}",
        strategy_intent_id=f"live_strategy_intent:{opportunity_id}",
        horizon_intent_id=f"live_horizon_intent:{opportunity_id}",
        discovery_score=_coerce_float(opportunity.get("discovery_score"))
        or _coerce_float(candidate.get("discovery_score"))
        or _coerce_float(candidate.get("quality_score"))
        or 0.0,
        promotion_score=_coerce_float(opportunity.get("promotion_score"))
        or _coerce_float(candidate.get("promotion_score"))
        or _coerce_float(candidate.get("quality_score"))
        or 0.0,
        rank=_coerce_int(opportunity.get("selection_rank")) or 0,
        state=state,
        state_reason=state_reason,
        expected_edge_value=_coerce_float(risk_hints.get("return_on_risk"))
        or _coerce_float(candidate.get("return_on_risk")),
        max_loss=_coerce_float(risk_hints.get("max_loss"))
        or _coerce_float(candidate.get("max_loss")),
        capital_usage=_coerce_float(risk_hints.get("max_loss"))
        or _coerce_float(candidate.get("max_loss")),
        execution_complexity=None,
        product_class=_as_text(opportunity.get("product_class"))
        or str(policy_fields["product_class"]),
        legacy_selection_state=_as_text(opportunity.get("selection_state")),
        evidence=evidence,
        legs=_opportunity_legs_from_row(opportunity),
    )


def _resolve_auto_execution_plan(
    *,
    signal_store: Any,
    cycle_id: str,
) -> dict[str, Any]:
    if signal_store is None or not signal_store.schema_ready():
        return {
            "available": False,
            "opportunities": [],
            "allocation_decisions": [],
            "execution_intents": [],
            "opportunity_rows_by_id": {},
        }
    opportunity_rows = signal_store.list_active_cycle_opportunities(
        cycle_id,
        eligibility_state="live",
        exclude_consumed=True,
        limit=200,
    )
    opportunities: list[Opportunity] = []
    rows_by_id: dict[str, dict[str, Any]] = {}
    skipped: list[dict[str, Any]] = []
    for row in opportunity_rows:
        payload = dict(row)
        plan_item = _plan_opportunity_from_signal_row(payload)
        if plan_item is None:
            skipped.append(payload)
            continue
        opportunities.append(plan_item)
        rows_by_id[plan_item.opportunity_id] = payload
    plan = build_execution_plan(opportunities)
    return {
        "available": True,
        **plan,
        "opportunity_rows_by_id": rows_by_id,
        "skipped_rows": skipped,
    }


def _validate_auto_execution_candidate(
    candidate: dict[str, Any],
) -> tuple[str | None, str | None]:
    profile = _as_text(candidate.get("profile"))
    if profile != "0dte":
        return None, None
    setup_status = _as_text(candidate.get("setup_status")) or "unknown"
    if setup_status != "favorable":
        return (
            "setup_not_favorable",
            "Automatic 0DTE execution is limited to favorable technical setups.",
        )
    if not candidate_has_intraday_setup_context(candidate):
        return (
            "awaiting_intraday_setup",
            "Automatic 0DTE execution requires persisted intraday setup context on the selected candidate.",
        )
    return None, None


def _resolve_candidate_entry_prices(
    candidate_payload: dict[str, Any],
) -> tuple[float | None, float | None]:
    midpoint_value = _normalize_limit_value(
        candidate_payload.get(
            "midpoint_value", candidate_payload.get("midpoint_credit")
        )
    )
    natural_value = _normalize_limit_value(
        candidate_payload.get("natural_value", candidate_payload.get("natural_credit"))
    )
    return midpoint_value, natural_value


def _entry_fill_ratio(
    *,
    midpoint_value: float,
    natural_value: float,
    premium_kind: str | None,
) -> float:
    if midpoint_value <= 0 or natural_value <= 0:
        return 0.0
    if premium_kind == "debit":
        return round(_clamp_fraction(midpoint_value / natural_value, maximum=1.0), 4)
    return round(_clamp_fraction(natural_value / midpoint_value, maximum=1.0), 4)


def _execution_retention_bound(
    *,
    midpoint_value: float,
    premium_kind: str | None,
    min_retention_pct: float,
) -> float:
    if premium_kind == "debit":
        return round(max(midpoint_value / min_retention_pct, midpoint_value), 4)
    return round(midpoint_value * min_retention_pct, 4)


def _quote_record_sort_key(record: dict[str, Any]) -> tuple[str, str]:
    return (
        _as_text(record.get("quote_timestamp")) or "",
        _as_text(record.get("captured_at")) or "",
    )


def _latest_quote_records_by_symbol(
    quote_records: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    latest: dict[str, tuple[tuple[str, str], dict[str, Any]]] = {}
    for record in quote_records:
        symbol = _as_text(record.get("option_symbol"))
        if symbol is None:
            continue
        sort_key = _quote_record_sort_key(record)
        current = latest.get(symbol)
        if current is None or sort_key >= current[0]:
            latest[symbol] = (sort_key, dict(record))
    return {symbol: row for symbol, (_, row) in latest.items()}


def _resolve_reactive_quote_snapshot(
    candidate: dict[str, Any],
    quote_records: list[dict[str, Any]],
) -> dict[str, Any] | None:
    latest_quotes = _latest_quote_records_by_symbol(quote_records)
    candidate_payload = _candidate_with_payload(candidate)
    strategy_family = _strategy_family_from_payload(candidate_payload)
    legs = candidate_legs(candidate_payload)
    sources = {
        str(record.get("option_symbol")): str(record.get("source"))
        for record in quote_records
        if str(record.get("option_symbol") or "").strip()
        and str(record.get("source") or "").strip()
    }
    return structure_quote_snapshot(
        legs=legs,
        strategy_family=strategy_family,
        quotes_by_symbol=latest_quotes,
        sources_by_symbol=sources,
    )


def _resolve_reactive_auto_execution(
    *,
    candidate: dict[str, Any],
    execution_policy: dict[str, Any],
    quote_records: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    candidate_payload = _candidate_with_payload(candidate)
    if not quote_records:
        return {
            "ok": False,
            "reason": "awaiting_reactive_quotes",
            "message": "Automatic 0DTE execution skipped because reactive quote capture did not return any quotes.",
        }

    live_snapshot = _resolve_reactive_quote_snapshot(candidate, quote_records)
    if live_snapshot is None:
        return {
            "ok": False,
            "reason": "awaiting_reactive_quotes",
            "message": "Automatic 0DTE execution skipped because a current spread quote snapshot was not available.",
        }

    strategy_family = _strategy_family_from_payload(candidate_payload)
    premium_kind = net_premium_kind(strategy_family)
    live_midpoint_value = _normalize_limit_value(live_snapshot.get("midpoint_value"))
    live_natural_value = _normalize_limit_value(live_snapshot.get("natural_value"))
    if (
        live_midpoint_value is None
        or live_natural_value is None
        or live_midpoint_value <= 0
        or live_natural_value <= 0
    ):
        return {
            "ok": False,
            "reason": "live_quotes_not_executable",
            "message": "Automatic 0DTE execution skipped because live spread quotes were not executable.",
            "reactive_quote": live_snapshot,
        }

    scanned_midpoint_value = _normalize_limit_value(
        candidate_payload.get("midpoint_credit")
    )
    if scanned_midpoint_value is not None:
        retention_bound = _execution_retention_bound(
            midpoint_value=scanned_midpoint_value,
            premium_kind=premium_kind,
            min_retention_pct=float(execution_policy["min_credit_retention_pct"]),
        )
        if premium_kind == "debit" and live_midpoint_value > retention_bound:
            return {
                "ok": False,
                "reason": "live_debit_above_ceiling",
                "message": (
                    "Automatic 0DTE execution skipped because the live spread debit rose above the concession ceiling."
                ),
                "reactive_quote": {
                    **live_snapshot,
                    "debit_ceiling": retention_bound,
                },
            }
        if premium_kind != "debit" and live_midpoint_value < retention_bound:
            return {
                "ok": False,
                "reason": "live_credit_below_floor",
                "message": (
                    "Automatic 0DTE execution skipped because the live spread credit fell below the retention floor."
                ),
                "reactive_quote": {
                    **live_snapshot,
                    "credit_floor": retention_bound,
                },
            }

    pricing_candidate = {
        **candidate_payload,
        "midpoint_credit": live_midpoint_value,
        "natural_credit": live_natural_value,
        "fill_ratio": _entry_fill_ratio(
            midpoint_value=live_midpoint_value,
            natural_value=live_natural_value,
            premium_kind=premium_kind,
        ),
    }
    limit_price = _resolve_open_limit_price(
        candidate_payload=pricing_candidate,
        explicit_limit_price=None,
        execution_policy=execution_policy,
    )
    return {
        "ok": True,
        "limit_price": limit_price,
        "reactive_quote": {
            **live_snapshot,
            "fill_ratio": pricing_candidate["fill_ratio"],
            "limit_price": limit_price,
        },
    }


def _capped_structure_return_on_risk(
    *,
    midpoint_value: float | None,
    span_value: float | None,
    premium_kind: str | None,
) -> float | None:
    if (
        midpoint_value is None
        or span_value is None
        or midpoint_value <= 0
        or span_value <= 0
        or midpoint_value >= span_value
    ):
        return None
    if premium_kind == "debit":
        return round((span_value - midpoint_value) / midpoint_value, 4)
    return round(midpoint_value / (span_value - midpoint_value), 4)


def _candidate_capped_structure_span(
    candidate_payload: Mapping[str, Any],
) -> float | None:
    width = _coerce_float(candidate_payload.get("width"))
    if width is not None and width > 0:
        return width
    max_profit = _coerce_float(candidate_payload.get("max_profit"))
    max_loss = _coerce_float(candidate_payload.get("max_loss"))
    if max_profit is None or max_loss is None:
        return None
    span_dollars = max_profit + max_loss
    if span_dollars <= 0:
        return None
    return round(span_dollars / 100.0, 4)


def _validate_live_deployment_quality(
    *,
    candidate_payload: Mapping[str, Any],
    deployment_mode: str | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    profile = resolve_candidate_profile(candidate_payload)
    thresholds = resolve_deployment_quality_thresholds(profile)
    minimum_return_on_risk = _coerce_float(
        thresholds.get("min_execution_return_on_risk")
    )
    enforce_return_floor = (
        minimum_return_on_risk is not None
        and str(deployment_mode or "").strip().lower() != DEPLOYMENT_MODE_PAPER_AUTO
    )
    if minimum_return_on_risk is None and not enforce_return_floor:
        return {
            "ok": True,
            "profile": profile,
        }

    strategy_family = _strategy_family_from_payload(candidate_payload)
    premium_kind = net_premium_kind(strategy_family)
    if strategy_family in {"long_straddle", "long_strangle"}:
        return {
            "ok": False,
            "reason": "long_vol_live_execution_disabled",
            "message": (
                "Open execution is blocked because long-vol earnings structures "
                "are still shadow-only in the live path."
            ),
            "profile": profile,
        }
    legs = candidate_legs(candidate_payload)
    span_value = _candidate_capped_structure_span(candidate_payload)
    if not legs or span_value is None or span_value <= 0:
        return {
            "ok": False,
            "reason": "live_deployment_quality_unavailable",
            "message": (
                "Open execution is blocked because the candidate is missing capped-risk "
                "structure geometry for live deployment validation."
            ),
            "profile": profile,
        }

    live_snapshot, error_text = build_structure_quote_snapshot(
        legs=legs,
        strategy_family=strategy_family,
        client=client,
    )
    if live_snapshot is None:
        return {
            "ok": False,
            "reason": "live_quotes_unavailable",
            "message": (
                "Open execution is blocked because a current live multi-leg structure "
                "snapshot is unavailable."
            ),
            "profile": profile,
            "quote_error": error_text,
        }

    live_midpoint_value = _normalize_limit_value(live_snapshot.get("midpoint_value"))
    live_return_on_risk = _capped_structure_return_on_risk(
        midpoint_value=live_midpoint_value,
        span_value=span_value,
        premium_kind=premium_kind,
    )
    if live_midpoint_value is None or live_return_on_risk is None:
        return {
            "ok": False,
            "reason": "live_quotes_not_executable",
            "message": (
                "Open execution is blocked because the live structure quotes were not executable."
            ),
            "profile": profile,
            "live_quote": live_snapshot,
        }

    if enforce_return_floor and live_return_on_risk < minimum_return_on_risk:
        return {
            "ok": False,
            "reason": "live_return_on_risk_below_floor",
            "message": (
                "Open execution is blocked because live return on risk "
                f"{live_return_on_risk:.4f} is below the deployment floor "
                f"{minimum_return_on_risk:.4f}."
            ),
            "profile": profile,
            "live_quote": {
                **live_snapshot,
                "span_value": span_value,
                "live_return_on_risk": live_return_on_risk,
                "minimum_return_on_risk": minimum_return_on_risk,
            },
        }

    return {
        "ok": True,
        "profile": profile,
        "live_quote": {
            **live_snapshot,
            "span_value": span_value,
            "live_return_on_risk": live_return_on_risk,
            "minimum_return_on_risk": minimum_return_on_risk,
        },
    }


def _resolve_open_limit_price(
    *,
    candidate_payload: dict[str, Any],
    explicit_limit_price: float | None,
    execution_policy: dict[str, Any],
) -> float:
    premium_kind = net_premium_kind(_strategy_family_from_payload(candidate_payload))
    explicit_value = _normalize_limit_value(explicit_limit_price)
    if explicit_value is not None:
        return round(max(explicit_value, 0.01), 2)

    midpoint_value, natural_value = _resolve_candidate_entry_prices(candidate_payload)
    if midpoint_value is None:
        order_payload = dict(candidate_payload.get("order_payload") or {})
        midpoint_value = _normalize_limit_value(order_payload.get("limit_price"))
    if midpoint_value is None or midpoint_value <= 0:
        raise ValueError("Execution limit price must be positive")

    pricing_mode = str(
        execution_policy.get("pricing_mode") or DEFAULT_ENTRY_PRICING_MODE
    )
    if pricing_mode == "midpoint" or natural_value is None or natural_value <= 0:
        return round(max(midpoint_value, 0.01), 2)

    fill_ratio = _clamp_fraction(
        _coerce_float(candidate_payload.get("fill_ratio")) or 0.0, maximum=1.0
    )
    min_credit_retention_pct = _clamp_fraction(
        _coerce_float(execution_policy.get("min_credit_retention_pct"))
        or DEFAULT_MIN_CREDIT_RETENTION_PCT,
        minimum=0.5,
        maximum=1.0,
    )
    max_credit_concession = max(
        _coerce_float(execution_policy.get("max_credit_concession"))
        or DEFAULT_MAX_CREDIT_CONCESSION,
        0.0,
    )
    if premium_kind == "debit":
        debit_ceiling = _execution_retention_bound(
            midpoint_value=midpoint_value,
            premium_kind=premium_kind,
            min_retention_pct=min_credit_retention_pct,
        )
        max_concession_to_ceiling = max(debit_ceiling - midpoint_value, 0.0)
        fill_ratio_concession = max(natural_value - midpoint_value, 0.0) * max(
            1.0 - fill_ratio, 0.0
        )
        concession = min(
            fill_ratio_concession, max_credit_concession, max_concession_to_ceiling
        )
        return round(
            min(
                max(midpoint_value + concession, 0.01),
                max(natural_value, 0.01),
                debit_ceiling,
            ),
            2,
        )

    credit_floor = max(natural_value, midpoint_value * min_credit_retention_pct, 0.01)
    max_concession_to_floor = max(midpoint_value - credit_floor, 0.0)
    fill_ratio_concession = max(midpoint_value - natural_value, 0.0) * max(
        1.0 - fill_ratio, 0.0
    )
    concession = min(
        fill_ratio_concession, max_credit_concession, max_concession_to_floor
    )
    return round(max(midpoint_value - concession, credit_floor, 0.01), 2)


def _classify_auto_execution_block(exc: Exception) -> dict[str, Any] | None:
    if not isinstance(exc, ValueError):
        return None
    message = str(exc).strip()
    if not message:
        return None
    if message.startswith("Open execution exceeds ") and message.endswith("."):
        constraint = message.removeprefix("Open execution exceeds ").removesuffix(".")
        return {
            "reason": "risk_policy_blocked",
            "message": message,
            "block_category": "risk_policy",
            "constraint": constraint,
        }
    if message == "Open execution is blocked because the quote snapshot is stale.":
        return {
            "reason": "stale_quote",
            "message": message,
            "block_category": "quote_freshness",
        }
    if (
        message
        == "Open execution is blocked because the exit force-close window has already started."
    ):
        return {
            "reason": "force_close_window_started",
            "message": message,
            "block_category": "timing_window",
        }
    if message.startswith("Open execution is blocked because only "):
        return {
            "reason": "insufficient_time_to_force_close",
            "message": message,
            "block_category": "timing_window",
        }
    if message == "Execution is blocked by SPREADS_EXECUTION_KILL_SWITCH.":
        return {
            "reason": "kill_switch_blocked",
            "message": message,
            "block_category": "kill_switch",
        }
    if message == "Open execution is blocked because control mode is halted.":
        return {
            "reason": "control_mode_halted",
            "message": message,
            "block_category": "control_mode",
        }
    if message.startswith("Open execution is blocked on a live Alpaca account."):
        return {
            "reason": "environment_blocked",
            "message": message,
            "block_category": "environment",
        }
    if message in {
        "Open execution is blocked because a current live spread snapshot is unavailable.",
        "Open execution is blocked because a current live multi-leg structure snapshot is unavailable.",
    }:
        return {
            "reason": "live_quotes_unavailable",
            "message": message,
            "block_category": "deployment_quality",
        }
    if message in {
        "Open execution is blocked because the live spread quotes were not executable.",
        "Open execution is blocked because the live structure quotes were not executable.",
    }:
        return {
            "reason": "live_quotes_not_executable",
            "message": message,
            "block_category": "deployment_quality",
        }
    if message.startswith("Open execution is blocked because live return on risk "):
        return {
            "reason": "live_return_on_risk_below_floor",
            "message": message,
            "block_category": "deployment_quality",
        }
    return None


def _resolve_session_candidate(
    *,
    collector_store: Any,
    session_id: str,
    candidate_id: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    candidate = collector_store.get_candidate(candidate_id)
    if candidate is None:
        raise ValueError(f"Unknown candidate_id: {candidate_id}")
    cycle = collector_store.get_cycle(str(candidate["cycle_id"]))
    if cycle is None:
        raise ValueError(f"Missing cycle for candidate_id: {candidate_id}")
    candidate_session_id = cycle.get("session_id") or build_live_run_scope_id(
        cycle["label"], cycle["session_date"]
    )
    if str(candidate_session_id) != session_id:
        raise ValueError(
            f"Candidate {candidate_id} does not belong to session {session_id}"
        )
    return dict(candidate), dict(cycle)


def _build_order_request(
    *,
    candidate: dict[str, Any],
    quantity: int | None,
    limit_price: float | None,
    execution_policy: dict[str, Any],
    client_order_id: str,
) -> tuple[dict[str, Any], int, float]:
    candidate_payload = _candidate_with_payload(candidate)
    strategy_family = _strategy_family_from_payload(candidate_payload)
    order_payload = dict(candidate_payload.get("order_payload") or {})
    resolved_legs = normalize_legs(order_payload.get("legs")) or candidate_legs(
        candidate_payload
    )
    if not resolved_legs:
        raise ValueError(
            "Selected live candidate does not include an executable order payload"
        )
    resolved_quantity = (
        quantity if quantity is not None else _coerce_int(order_payload.get("qty")) or 1
    )
    if resolved_quantity <= 0:
        raise ValueError("Execution quantity must be positive")
    resolved_limit_price = _resolve_open_limit_price(
        candidate_payload=candidate_payload,
        explicit_limit_price=limit_price,
        execution_policy=execution_policy,
    )
    request = build_multileg_order_payload(
        legs=resolved_legs,
        limit_price=resolved_limit_price,
        strategy_family=strategy_family,
        trade_intent=OPEN_TRADE_INTENT,
        quantity=resolved_quantity,
    )
    request["client_order_id"] = client_order_id
    return request, int(resolved_quantity), round(float(resolved_limit_price), 2)


def _build_close_order_request(
    *,
    position: dict[str, Any],
    quantity: int | None,
    limit_price: float | None,
    client_order_id: str,
) -> tuple[dict[str, Any], int, float]:
    remaining_quantity = _coerce_float(position.get("remaining_quantity"))
    if remaining_quantity is None or remaining_quantity <= 0:
        raise ValueError("Session position does not have remaining quantity to close")
    resolved_quantity = (
        quantity if quantity is not None else int(round(remaining_quantity))
    )
    if resolved_quantity <= 0:
        raise ValueError("Close quantity must be positive")
    if resolved_quantity > remaining_quantity:
        raise ValueError(
            "Close quantity exceeds the remaining session position quantity"
        )

    resolved_limit_price = (
        limit_price
        if limit_price is not None
        else _coerce_float(position.get("close_mark"))
    )
    resolved_limit_price = _normalize_limit_value(resolved_limit_price)
    if resolved_limit_price is None or resolved_limit_price <= 0:
        raise ValueError(
            "Close execution requires a positive limit price or a quoted close mark"
        )

    strategy_family = _strategy_family_from_payload(position)
    resolved_legs = normalize_legs(position.get("legs"))
    if not resolved_legs:
        resolved_legs = candidate_legs(position)
    if not resolved_legs:
        raise ValueError("Close execution requires canonical position legs")
    request = build_multileg_order_payload(
        legs=closing_legs(resolved_legs),
        limit_price=float(resolved_limit_price),
        strategy_family=strategy_family,
        trade_intent=CLOSE_TRADE_INTENT,
        quantity=resolved_quantity,
    )
    request["client_order_id"] = client_order_id
    return request, int(resolved_quantity), round(float(resolved_limit_price), 2)


@with_storage()
def submit_live_session_execution(
    *,
    db_target: str,
    session_id: str,
    candidate_id: int,
    quantity: int | None = None,
    limit_price: float | None = None,
    request_metadata: dict[str, Any] | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    collector_store = storage.collector
    execution_store = storage.execution
    job_store = storage.jobs
    signal_store = getattr(storage, "signals", None)
    risk_store = getattr(storage, "risk", None)
    requested_at = _utc_now()
    client_order_id = _execution_client_order_id()
    attempt_id: str | None = None
    risk_decision: dict[str, Any] | None = None
    try:
        _require_execution_schema(execution_store)
        _require_position_schema(execution_store)
        candidate, cycle = _resolve_session_candidate(
            collector_store=collector_store,
            session_id=session_id,
            candidate_id=candidate_id,
        )
        candidate_payload = _candidate_with_payload(candidate)
        candidate_strategy_family = _strategy_family_from_payload(candidate_payload)
        candidate_identity = legs_identity_key(
            strategy=candidate_strategy_family,
            legs=candidate_legs(candidate_payload),
        )
        source_policies = _resolve_source_policies(
            cycle=cycle,
            job_store=job_store,
        )
        active_policy_rollouts = get_active_policy_rollout_map(storage=storage)
        opportunity = None
        if (
            signal_store is not None
            and hasattr(signal_store, "schema_ready")
            and signal_store.schema_ready()
        ):
            requested_opportunity_id = None
            if isinstance(request_metadata, Mapping):
                requested_opportunity_id = _as_text(
                    request_metadata.get("opportunity_id")
                )
            if requested_opportunity_id is not None:
                requested_opportunity = signal_store.get_opportunity(
                    requested_opportunity_id
                )
                if requested_opportunity is not None and str(
                    requested_opportunity.get("lifecycle_state") or ""
                ) in {"candidate", "ready", "blocked"}:
                    opportunity = requested_opportunity
            if opportunity is None:
                opportunity = signal_store.find_active_opportunity_by_candidate_id(
                    candidate_id
                )
        opportunity_ref = (
            None
            if opportunity is None
            else {
                "opportunity_id": str(opportunity["opportunity_id"]),
                "signal_state_ref": opportunity.get("signal_state_ref"),
                "lifecycle_state": opportunity.get("lifecycle_state"),
                "selection_state": opportunity.get("selection_state"),
            }
        )

        list_open_attempts = getattr(
            execution_store,
            "list_session_attempts_by_status",
            None,
        )
        if callable(list_open_attempts):
            existing_attempts = [
                dict(attempt)
                for attempt in list_open_attempts(
                    session_id=session_id,
                    statuses=sorted(OPEN_STATUSES),
                    trade_intent=OPEN_TRADE_INTENT,
                    limit=200,
                )
                if _execution_attempt_identity(dict(attempt)) == candidate_identity
            ]
        else:
            existing_attempts = execution_store.list_open_attempts_for_identity(
                session_id=session_id,
                strategy=str(candidate["strategy"]),
                short_symbol=str(candidate["short_symbol"]),
                long_symbol=str(candidate["long_symbol"]),
                statuses=sorted(OPEN_STATUSES),
            )
        if existing_attempts:
            payload = _get_attempt_payload(
                execution_store,
                str(existing_attempts[0]["execution_attempt_id"]),
            )
            return {
                "action": "submit",
                "changed": False,
                "message": (
                    f"An active execution already exists for "
                    f"{payload['short_symbol']} / {payload['long_symbol']} in this session."
                ),
                "attempt": payload,
            }

        gate = assess_open_activity_gate(
            activity_kind=OPEN_ACTIVITY_MANUAL,
            storage=storage,
        )
        if not gate["allowed"]:
            publish_control_gate_event(
                db_target=db_target,
                decision=gate,
                activity_kind=OPEN_ACTIVITY_MANUAL,
                session_id=session_id,
                session_date=str(cycle["session_date"]),
                label=str(cycle["label"]),
                candidate_id=_coerce_int(candidate.get("candidate_id")),
                cycle_id=_as_text(cycle.get("cycle_id")),
            )
            raise ValueError(str(gate["message"]))

        requested_execution_policy = _requested_policy_payload(
            request_metadata=request_metadata,
            policy_name="execution_policy",
            source_policies=source_policies,
            active_policy_rollouts=active_policy_rollouts,
        )
        requested_risk_policy = _requested_policy_payload(
            request_metadata=request_metadata,
            policy_name="risk_policy",
            source_policies=source_policies,
            active_policy_rollouts=active_policy_rollouts,
        )
        resolved_execution_policy = normalize_execution_policy(
            {
                "execution_policy": requested_execution_policy,
                "risk_policy": requested_risk_policy,
            }
        )
        order_request, resolved_quantity, resolved_limit_price = _build_order_request(
            candidate=candidate,
            quantity=quantity,
            limit_price=limit_price,
            execution_policy=resolved_execution_policy,
            client_order_id=client_order_id,
        )
        live_deployment_quality = _validate_live_deployment_quality(
            candidate_payload=candidate_payload,
            deployment_mode=str(resolved_execution_policy.get("deployment_mode") or ""),
        )
        if not live_deployment_quality["ok"]:
            raise ValueError(str(live_deployment_quality["message"]))
        requested_exit_policy = _requested_policy_payload(
            request_metadata=request_metadata,
            policy_name="exit_policy",
            source_policies=source_policies,
            active_policy_rollouts=active_policy_rollouts,
        )
        resolved_exit_policy = resolve_exit_policy_snapshot(
            session_date=str(cycle["session_date"]),
            payload=requested_exit_policy,
        )
        timing_gate = _validate_open_timing_window(
            exit_policy=resolved_exit_policy,
            current_time=parse_datetime(requested_at) or datetime.now(UTC),
            profile=resolve_candidate_profile(candidate_payload),
            deployment_mode=str(resolved_execution_policy.get("deployment_mode") or ""),
        )
        if not timing_gate["allowed"]:
            raise ValueError(str(timing_gate["message"]))
        risk_evaluation = evaluate_open_execution(
            execution_store=execution_store,
            session_id=session_id,
            candidate=candidate,
            cycle=cycle,
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            risk_policy=requested_risk_policy,
            execution_policy=resolved_execution_policy,
        )
        resolved_risk_policy = dict(risk_evaluation["policy"])
        policy_refs = _build_policy_refs(
            request_metadata=request_metadata,
            source_policies=source_policies,
            active_policy_rollouts=active_policy_rollouts,
            resolved_risk_policy=resolved_risk_policy,
            resolved_execution_policy=resolved_execution_policy,
            resolved_exit_policy=resolved_exit_policy,
        )
        if (
            risk_store is not None
            and hasattr(risk_store, "schema_ready")
            and risk_store.schema_ready()
        ):
            risk_decision = risk_store.create_risk_decision(
                risk_decision_id=_risk_decision_id(),
                decision_kind="open_execution",
                status=str(risk_evaluation["status"]),
                note=str(risk_evaluation["note"]),
                session_id=session_id,
                session_date=str(cycle["session_date"]),
                label=str(cycle["label"]),
                cycle_id=_as_text(cycle.get("cycle_id")),
                candidate_id=_coerce_int(candidate.get("candidate_id")),
                opportunity_id=None
                if opportunity_ref is None
                else str(opportunity_ref["opportunity_id"]),
                execution_attempt_id=None,
                trade_intent=OPEN_TRADE_INTENT,
                entity_type="signal_subject",
                entity_key=(
                    str(opportunity.get("entity_key"))
                    if isinstance(opportunity, dict) and opportunity.get("entity_key")
                    else f"signal_subject:{cycle['label']}:{candidate['underlying_symbol']}"
                ),
                underlying_symbol=str(candidate["underlying_symbol"]),
                strategy=str(candidate["strategy"]),
                quantity=resolved_quantity,
                limit_price=resolved_limit_price,
                reason_codes=[
                    str(value) for value in risk_evaluation.get("reason_codes") or []
                ],
                blockers=[
                    str(value) for value in risk_evaluation.get("blockers") or []
                ],
                metrics=dict(risk_evaluation.get("metrics") or {}),
                evidence={
                    "candidate_generated_at": _as_text(candidate.get("generated_at")),
                    "opportunity": opportunity_ref,
                    "source_job": {
                        "job_type": source_policies["source_job_type"],
                        "job_key": source_policies["source_job_key"],
                        "job_run_id": source_policies["source_job_run_id"],
                    },
                    "requested_limit_price": resolved_limit_price,
                    "requested_quantity": resolved_quantity,
                },
                policy_refs=policy_refs,
                resolved_risk_policy=resolved_risk_policy,
                decided_at=requested_at,
            )
        if str(risk_evaluation["status"]) in {"blocked", "unknown"}:
            if risk_decision is not None:
                _publish_risk_decision_event(risk_decision)
            raise ValueError(str(risk_evaluation["note"]))

        pipeline_policy_fields = resolve_pipeline_policy_fields(
            profile=candidate_payload.get("profile"),
            root_symbol=str(candidate["underlying_symbol"]),
        )
        attempt_legs = normalize_legs(order_request.get("legs")) or candidate_legs(
            candidate_payload
        )
        compatibility_short_symbol, compatibility_long_symbol = (
            primary_short_long_symbols(attempt_legs)
        )
        attempt_id = _execution_attempt_id()
        attempt = execution_store.create_attempt(
            execution_attempt_id=attempt_id,
            session_id=session_id,
            session_date=str(cycle["session_date"]),
            label=str(cycle["label"]),
            pipeline_id=build_pipeline_id(str(cycle["label"])),
            market_date=str(cycle["session_date"]),
            cycle_id=_as_text(cycle.get("cycle_id")),
            opportunity_id=None
            if opportunity_ref is None
            else str(opportunity_ref["opportunity_id"]),
            risk_decision_id=None
            if risk_decision is None
            else str(risk_decision["risk_decision_id"]),
            candidate_id=_coerce_int(candidate.get("candidate_id")),
            attempt_context=_normalize_attempt_context(
                candidate.get("selection_state")
            ),
            candidate_generated_at=_as_text(candidate.get("generated_at")),
            run_id=_as_text(candidate.get("run_id")),
            job_run_id=_as_text(cycle.get("job_run_id")),
            underlying_symbol=str(candidate["underlying_symbol"]),
            strategy=str(candidate["strategy"]),
            expiration_date=str(candidate["expiration_date"]),
            short_symbol=str(
                compatibility_short_symbol or candidate.get("short_symbol") or ""
            ),
            long_symbol=str(
                compatibility_long_symbol or candidate.get("long_symbol") or ""
            ),
            trade_intent=OPEN_TRADE_INTENT,
            position_id=None,
            root_symbol=str(candidate["underlying_symbol"]),
            strategy_family=candidate_strategy_family,
            style_profile=str(pipeline_policy_fields["style_profile"]),
            horizon_intent=str(pipeline_policy_fields["horizon_intent"]),
            product_class=str(pipeline_policy_fields["product_class"]),
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            requested_at=requested_at,
            status=PENDING_SUBMISSION_STATUS,
            broker=BROKER_NAME,
            client_order_id=client_order_id,
            request={
                **({} if request_metadata is None else request_metadata),
                **({} if opportunity_ref is None else {"opportunity": opportunity_ref}),
                **(
                    {}
                    if risk_decision is None
                    else {
                        "risk_decision": {
                            "risk_decision_id": str(risk_decision["risk_decision_id"]),
                            "status": str(risk_decision["status"]),
                            "policy_refs": dict(risk_decision.get("policy_refs") or {}),
                        }
                    }
                ),
                "trade_intent": OPEN_TRADE_INTENT,
                "execution_policy": resolved_execution_policy,
                "risk_policy": resolved_risk_policy,
                "exit_policy": resolved_exit_policy,
                "source_job": {
                    "job_type": source_policies["source_job_type"],
                    "job_key": source_policies["source_job_key"],
                    "job_run_id": source_policies["source_job_run_id"],
                },
                "order": order_request,
            },
            candidate=candidate_payload,
        )
        payload = _queue_execution_attempt(
            job_store=job_store,
            execution_store=execution_store,
            attempt=attempt,
        )
        if risk_decision is not None and risk_store is not None:
            try:
                risk_decision = risk_store.attach_execution_attempt(
                    risk_decision_id=str(risk_decision["risk_decision_id"]),
                    execution_attempt_id=attempt_id,
                )
                _publish_risk_decision_event(risk_decision)
            except Exception:
                pass
        if opportunity_ref is not None and signal_store is not None:
            try:
                consumed_opportunity, consumed_changed = (
                    signal_store.mark_opportunity_consumed(
                        opportunity_id=str(opportunity_ref["opportunity_id"]),
                        execution_attempt_id=attempt_id,
                        consumed_at=requested_at,
                    )
                )
                if consumed_opportunity is not None and consumed_changed:
                    publish_opportunity_event(
                        topic="opportunity.lifecycle.updated",
                        opportunity=consumed_opportunity,
                        session_date=str(cycle["session_date"]),
                        correlation_id=str(cycle["cycle_id"]),
                        causation_id=attempt_id,
                        timestamp=requested_at,
                        source="execution",
                    )
            except Exception:
                pass
        message = _submission_message(payload, queued=True)
        return {
            "action": "submit",
            "changed": True,
            "message": message,
            **({} if risk_decision is None else {"risk_decision": risk_decision}),
            "attempt": payload,
        }
    except Exception as exc:
        if attempt_id is not None:
            current_attempt = execution_store.get_attempt(attempt_id)
            if (
                current_attempt is not None
                and str(current_attempt.get("status") or "")
                == PENDING_SUBMISSION_STATUS
            ):
                execution_store.update_attempt(
                    execution_attempt_id=attempt_id,
                    status="failed",
                    client_order_id=client_order_id,
                    completed_at=requested_at,
                    error_text=str(exc),
                )
                payload = _get_attempt_payload(execution_store, attempt_id)
                _publish_execution_attempt_event(
                    payload,
                    message=f"Execution failed before submission: {exc}",
                )
        raise


@with_storage()
def refresh_live_session_execution(
    *,
    db_target: str,
    session_id: str,
    execution_attempt_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    attempt = execution_store.get_attempt(execution_attempt_id)
    if attempt is None:
        raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")
    if str(attempt["session_id"]) != session_id:
        raise ValueError(
            f"Execution {execution_attempt_id} does not belong to session {session_id}"
        )
    if (
        _as_text(attempt.get("broker_order_id")) is None
        and str(attempt.get("status") or "") == PENDING_SUBMISSION_STATUS
    ):
        payload = _get_attempt_payload(execution_store, execution_attempt_id)
        return {
            "action": "refresh",
            "changed": False,
            "message": "Execution is still queued for broker submission.",
            "attempt": payload,
        }
    if (
        _as_text(attempt.get("broker_order_id")) is None
        and str(attempt.get("status") or "") == SUBMIT_UNKNOWN_STATUS
    ):
        client_order_id = _as_text(attempt.get("client_order_id"))
        if client_order_id is None:
            payload = _get_attempt_payload(execution_store, execution_attempt_id)
            return {
                "action": "refresh",
                "changed": False,
                "message": (
                    "Execution submit outcome is uncertain and cannot be reconciled "
                    "because the client order id is missing."
                ),
                "attempt": payload,
            }
        client = create_alpaca_client_from_env()
        reconciled_attempt = _reconcile_submit_unknown_attempt(
            execution_store=execution_store,
            attempt=attempt,
            client=client,
        )
        if reconciled_attempt is None:
            payload = _get_attempt_payload(execution_store, execution_attempt_id)
            return {
                "action": "refresh",
                "changed": False,
                "message": (
                    "Execution submit outcome is uncertain and no broker order has been "
                    f"found yet for client_order_id {client_order_id}."
                ),
                "attempt": payload,
            }
        message = (
            f"Reconciled execution {execution_attempt_id} via client_order_id "
            f"{client_order_id}: {reconciled_attempt['status']}."
        )
        _publish_execution_attempt_event(reconciled_attempt, message=message)
        _sync_linked_execution_intent(
            execution_store=execution_store,
            attempt=reconciled_attempt,
            event_type="reconciled",
            message=message,
        )
        return {
            "action": "refresh",
            "changed": True,
            "message": message,
            "attempt": reconciled_attempt,
        }
    broker_order_id = _as_text(attempt.get("broker_order_id"))
    if broker_order_id is None:
        raise ValueError("Execution does not have a broker order id to refresh")

    client = create_alpaca_client_from_env()
    order_snapshot = client.get_order(broker_order_id, nested=True)
    payload = _sync_attempt_state(
        execution_store=execution_store,
        attempt=dict(attempt),
        client=client,
        order_snapshot=order_snapshot,
    )
    message = f"Refreshed execution {execution_attempt_id}: {payload['status']}."
    _publish_execution_attempt_event(payload, message=message)
    _sync_linked_execution_intent(
        execution_store=execution_store,
        attempt=payload,
        event_type="refreshed",
        message=message,
    )
    return {
        "action": "refresh",
        "changed": True,
        "message": message,
        "attempt": payload,
    }


@with_storage()
def submit_opportunity_execution(
    *,
    db_target: str,
    opportunity_id: str,
    quantity: int | None = None,
    limit_price: float | None = None,
    request_metadata: dict[str, Any] | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    signal_store = storage.signals
    opportunity = signal_store.get_opportunity(opportunity_id)
    if opportunity is None:
        raise ValueError(f"Unknown opportunity_id: {opportunity_id}")
    lifecycle_state = _as_text(opportunity.get("lifecycle_state")) or ""
    if lifecycle_state not in {"candidate", "ready", "blocked"}:
        raise ValueError("Opportunity is no longer active for execution")
    eligibility_state = _as_text(opportunity.get("eligibility_state")) or _as_text(
        opportunity.get("eligibility")
    )
    if eligibility_state not in {None, "live"}:
        raise ValueError("Opportunity is not live-executable")
    candidate_id = _coerce_int(opportunity.get("source_candidate_id"))
    if candidate_id is None:
        raise ValueError("Opportunity is missing a source candidate id")
    label = _as_text(opportunity.get("label"))
    market_date = _as_text(opportunity.get("market_date")) or _as_text(
        opportunity.get("session_date")
    )
    if label is None or market_date is None:
        raise ValueError("Opportunity is missing label or market_date")
    return submit_live_session_execution(
        db_target=db_target,
        session_id=build_live_run_scope_id(label, market_date),
        candidate_id=candidate_id,
        quantity=quantity,
        limit_price=limit_price,
        request_metadata={
            **({} if request_metadata is None else request_metadata),
            "opportunity_id": opportunity_id,
            "pipeline_id": opportunity.get("pipeline_id"),
            "market_date": market_date,
        },
        storage=storage,
    )


@with_storage()
def submit_position_close_by_id(
    *,
    db_target: str,
    position_id: str,
    quantity: int | None = None,
    limit_price: float | None = None,
    request_metadata: dict[str, Any] | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    job_store = storage.jobs
    if not execution_store.portfolio_schema_ready():
        raise ValueError(f"Unknown position_id: {position_id}")
    stored_position = execution_store.get_position(position_id)
    if stored_position is None:
        raise ValueError(f"Unknown position_id: {position_id}")
    position = enrich_position_row(dict(stored_position))
    if (
        str(position.get("position_status") or position.get("status") or "open")
        == "closed"
    ):
        raise ValueError("Position is already closed")

    existing_attempts = execution_store.list_open_attempts_for_position(
        position_id=position_id,
        statuses=sorted(OPEN_STATUSES),
    )
    if existing_attempts:
        payload = _get_attempt_payload(
            execution_store,
            str(existing_attempts[0]["execution_attempt_id"]),
        )
        return {
            "action": "submit",
            "changed": False,
            "message": "An active close execution already exists for this position.",
            "attempt": payload,
        }

    requested_at = _utc_now()
    client_order_id = _execution_client_order_id()
    trade_intent = resolve_trade_intent(CLOSE_TRADE_INTENT)
    attempt_id: str | None = None
    try:
        order_request, resolved_quantity, resolved_limit_price = (
            _build_close_order_request(
                position=position,
                quantity=quantity,
                limit_price=limit_price,
                client_order_id=client_order_id,
            )
        )
        validate_close_execution(
            position=position,
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
        )
        pipeline_id = _as_text(position.get("pipeline_id"))
        label = _as_text(position.get("label"))
        market_date = _as_text(position.get("market_date"))
        if pipeline_id is None or label is None or market_date is None:
            raise ValueError("Position is missing pipeline or market_date")
        policy_fields = resolve_pipeline_policy_fields(
            profile=(position.get("risk_policy") or {}).get("profile"),
            root_symbol=str(position["underlying_symbol"]),
        )
        attempt_legs = normalize_legs(order_request.get("legs")) or normalize_legs(
            position.get("legs")
        )
        compatibility_short_symbol, compatibility_long_symbol = (
            primary_short_long_symbols(attempt_legs)
        )
        attempt_id = _execution_attempt_id()
        attempt = execution_store.create_attempt(
            execution_attempt_id=attempt_id,
            session_id=build_live_run_scope_id(label, market_date),
            session_date=market_date,
            label=label,
            pipeline_id=pipeline_id,
            market_date=market_date,
            cycle_id=None,
            opportunity_id=_as_text(position.get("source_opportunity_id")),
            risk_decision_id=None,
            candidate_id=_coerce_int(position.get("candidate_id")),
            attempt_context="position_close",
            candidate_generated_at=None,
            run_id=None,
            job_run_id=None,
            underlying_symbol=str(position["underlying_symbol"]),
            strategy=str(position["strategy"]),
            expiration_date=str(position["expiration_date"]),
            short_symbol=str(
                compatibility_short_symbol or position.get("short_symbol") or ""
            ),
            long_symbol=str(
                compatibility_long_symbol or position.get("long_symbol") or ""
            ),
            trade_intent=trade_intent,
            position_id=position_id,
            root_symbol=str(position["underlying_symbol"]),
            strategy_family=_strategy_family_from_payload(position),
            style_profile=str(
                position.get("style_profile") or policy_fields["style_profile"]
            ),
            horizon_intent=str(
                position.get("horizon_intent") or policy_fields["horizon_intent"]
            ),
            product_class=str(
                position.get("product_class") or policy_fields["product_class"]
            ),
            quantity=resolved_quantity,
            limit_price=resolved_limit_price,
            requested_at=requested_at,
            status=PENDING_SUBMISSION_STATUS,
            broker=BROKER_NAME,
            client_order_id=client_order_id,
            request={
                **({} if request_metadata is None else request_metadata),
                "trade_intent": trade_intent,
                "position_id": position_id,
                "order": order_request,
            },
            candidate={},
        )
        payload = _queue_execution_attempt(
            job_store=job_store,
            execution_store=execution_store,
            attempt=attempt,
        )
        message = _submission_message(payload, queued=True)
        return {
            "action": "submit",
            "changed": True,
            "message": message,
            "attempt": payload,
        }
    except Exception as exc:
        if attempt_id is not None:
            current_attempt = execution_store.get_attempt(attempt_id)
            if (
                current_attempt is not None
                and str(current_attempt.get("status") or "")
                == PENDING_SUBMISSION_STATUS
            ):
                execution_store.update_attempt(
                    execution_attempt_id=attempt_id,
                    status="failed",
                    client_order_id=client_order_id,
                    completed_at=requested_at,
                    error_text=str(exc),
                    position_id=position_id,
                )
                payload = _get_attempt_payload(execution_store, attempt_id)
                _publish_execution_attempt_event(
                    payload,
                    message=f"Close execution failed before submission: {exc}",
                )
        raise


@with_storage()
def refresh_execution_attempt(
    *,
    db_target: str,
    execution_attempt_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    attempt = execution_store.get_attempt(execution_attempt_id)
    if attempt is None:
        raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")
    session_id = _as_text(attempt.get("session_id"))
    if session_id is None:
        label = _as_text(attempt.get("label"))
        market_date = _as_text(attempt.get("market_date")) or _as_text(
            attempt.get("session_date")
        )
        if label is None or market_date is None:
            raise ValueError(
                "Execution attempt is missing session compatibility fields"
            )
        session_id = build_live_run_scope_id(label, market_date)
    return refresh_live_session_execution(
        db_target=db_target,
        session_id=session_id,
        execution_attempt_id=execution_attempt_id,
        storage=storage,
    )


@with_storage()
def run_execution_submit(
    *,
    db_target: str,
    execution_attempt_id: str,
    heartbeat: Any | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    _require_execution_schema(execution_store)
    attempt = execution_store.get_attempt(execution_attempt_id)
    if attempt is None:
        raise ValueError(f"Unknown execution_attempt_id: {execution_attempt_id}")

    payload = _get_attempt_payload(execution_store, execution_attempt_id)
    broker_order_id = _as_text(payload.get("broker_order_id"))
    status = str(payload.get("status") or "")
    if broker_order_id is not None or status != PENDING_SUBMISSION_STATUS:
        return {
            "status": "skipped",
            "reason": "attempt_already_submitted",
            "execution_attempt_id": execution_attempt_id,
            "attempt_status": status,
            "broker_order_id": broker_order_id,
        }

    request = dict(payload.get("request") or {})
    order_request = request.get("order")
    if not isinstance(order_request, dict) or not order_request:
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status="failed",
            completed_at=_utc_now(),
            error_text="Execution attempt is missing its broker order payload.",
            position_id=_as_text(payload.get("position_id")),
        )
        failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
        _publish_execution_attempt_event(
            failed_attempt,
            message="Execution failed before submission: missing broker order payload.",
        )
        _sync_linked_execution_intent(
            execution_store=execution_store,
            attempt=failed_attempt,
            state="failed",
            event_type="failed",
            message="Execution failed before submission: missing broker order payload.",
        )
        raise ValueError("Execution attempt is missing its broker order payload.")

    if str(payload.get("trade_intent") or OPEN_TRADE_INTENT) == OPEN_TRADE_INTENT:
        request_execution_policy = (
            request.get("execution_policy")
            if isinstance(request.get("execution_policy"), Mapping)
            else {}
        )
        timing_gate = _validate_open_timing_window(
            exit_policy=request.get("exit_policy"),
            current_time=datetime.now(UTC),
            profile=resolve_candidate_profile(dict(payload.get("candidate") or {})),
            deployment_mode=str(request_execution_policy.get("deployment_mode") or ""),
        )
        if not timing_gate["allowed"]:
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status="failed",
                completed_at=_utc_now(),
                error_text=str(timing_gate["message"]),
                position_id=_as_text(payload.get("position_id")),
            )
            failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=f"Execution failed before submission: {timing_gate['message']}",
            )
            _sync_linked_execution_intent(
                execution_store=execution_store,
                attempt=failed_attempt,
                state="failed",
                event_type="failed",
                message=f"Execution failed before submission: {timing_gate['message']}",
            )
            return {
                "status": "blocked",
                "reason": str(timing_gate["reason"]),
                "execution_attempt_id": execution_attempt_id,
                "message": str(timing_gate["message"]),
                "attempt": failed_attempt,
            }

    if callable(heartbeat):
        heartbeat()
    client = create_alpaca_client_from_env()
    if str(payload.get("trade_intent") or OPEN_TRADE_INTENT) == OPEN_TRADE_INTENT:
        request_payload = (
            payload.get("request")
            if isinstance(payload.get("request"), Mapping)
            else {}
        )
        request_execution_policy = (
            request_payload.get("execution_policy")
            if isinstance(request_payload.get("execution_policy"), Mapping)
            else {}
        )
        live_deployment_quality = _validate_live_deployment_quality(
            candidate_payload=dict(payload.get("candidate") or {}),
            deployment_mode=str(request_execution_policy.get("deployment_mode") or ""),
            client=client,
        )
        if not live_deployment_quality["ok"]:
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status="failed",
                completed_at=_utc_now(),
                error_text=str(live_deployment_quality["message"]),
                position_id=_as_text(payload.get("position_id")),
            )
            failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=(
                    "Execution failed before submission: "
                    f"{live_deployment_quality['message']}"
                ),
            )
            _sync_linked_execution_intent(
                execution_store=execution_store,
                attempt=failed_attempt,
                state="failed",
                event_type="failed",
                message=(
                    "Execution failed before submission: "
                    f"{live_deployment_quality['message']}"
                ),
            )
            return {
                "status": "blocked",
                "reason": str(live_deployment_quality["reason"]),
                "execution_attempt_id": execution_attempt_id,
                "message": str(live_deployment_quality["message"]),
                "attempt": failed_attempt,
                **(
                    {}
                    if live_deployment_quality.get("live_quote") is None
                    else {"live_quote": dict(live_deployment_quality["live_quote"])}
                ),
            }
    requested_at = _as_text(payload.get("requested_at")) or _utc_now()
    client_order_id = _as_text(payload.get("client_order_id"))

    submitted_order: dict[str, Any] | None = None
    try:
        submitted_order = client.submit_order(order_request)
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status=str(submitted_order.get("status") or "submitted").lower(),
            broker_order_id=_as_text(submitted_order.get("id")),
            client_order_id=_as_text(submitted_order.get("client_order_id"))
            or client_order_id,
            submitted_at=_as_text(submitted_order.get("submitted_at")) or requested_at,
            position_id=_as_text(payload.get("position_id")),
        )
        if callable(heartbeat):
            heartbeat()
        try:
            order_snapshot = client.get_order(str(submitted_order["id"]), nested=True)
        except Exception:
            order_snapshot = submitted_order
        synced_attempt = _sync_attempt_state(
            execution_store=execution_store,
            attempt=payload,
            client=client,
            order_snapshot=order_snapshot,
        )
        message = _submission_message(synced_attempt, queued=False)
        _publish_execution_attempt_event(synced_attempt, message=message)
        _sync_linked_execution_intent(
            execution_store=execution_store,
            attempt=synced_attempt,
            event_type="submitted",
            message=message,
        )
        return {
            "status": "submitted",
            "execution_attempt_id": execution_attempt_id,
            "message": message,
            "attempt": synced_attempt,
        }
    except Exception as exc:
        if submitted_order is None:
            execution_store.update_attempt(
                execution_attempt_id=execution_attempt_id,
                status="failed",
                client_order_id=client_order_id,
                completed_at=requested_at,
                error_text=str(exc),
                position_id=_as_text(payload.get("position_id")),
            )
            failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
            _publish_execution_attempt_event(
                failed_attempt,
                message=f"Execution failed before submission: {exc}",
            )
            _sync_linked_execution_intent(
                execution_store=execution_store,
                attempt=failed_attempt,
                state="failed",
                event_type="failed",
                message=f"Execution failed before submission: {exc}",
            )
            raise
        broker_order_id = _as_text(submitted_order.get("id"))
        submitted_status = str(submitted_order.get("status") or "submitted").lower()
        execution_store.update_attempt(
            execution_attempt_id=execution_attempt_id,
            status=submitted_status,
            broker_order_id=broker_order_id,
            client_order_id=_as_text(submitted_order.get("client_order_id"))
            or client_order_id,
            submitted_at=_as_text(submitted_order.get("submitted_at")) or requested_at,
            completed_at=_resolve_completed_at(submitted_order)
            if _is_terminal_status(submitted_status)
            else None,
            error_text=str(exc),
            position_id=_as_text(payload.get("position_id")),
        )
        failed_attempt = _get_attempt_payload(execution_store, execution_attempt_id)
        _publish_execution_attempt_event(
            failed_attempt,
            message=(
                f"Order {broker_order_id or execution_attempt_id} was submitted, "
                f"but local execution sync failed: {exc}"
            ),
        )
        _sync_linked_execution_intent(
            execution_store=execution_store,
            attempt=failed_attempt,
            event_type="submit_unknown",
            message=(
                f"Order {broker_order_id or execution_attempt_id} was submitted, "
                f"but local execution sync failed: {exc}"
            ),
        )
        raise


@with_storage()
def submit_auto_session_execution(
    *,
    db_target: str,
    session_id: str,
    cycle_id: str,
    policy: dict[str, Any] | None,
    job_run_id: str | None = None,
    reactive_quote_records: list[dict[str, Any]] | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    active_policy_rollouts = get_active_policy_rollout_map(storage=storage)
    execution_rollout = active_policy_rollouts.get("execution_policy")
    collector_store = storage.collector
    cycle = collector_store.get_cycle(cycle_id)
    source_policies = (
        {
            "execution_policy": normalize_execution_policy(None),
            "risk_policy": normalize_risk_policy(None),
        }
        if cycle is None
        else _resolve_source_policies(cycle=dict(cycle), job_store=storage.jobs)
    )
    requested_policy = (
        dict(execution_rollout["policy"])
        if execution_rollout is not None
        and isinstance(execution_rollout.get("policy"), dict)
        else policy
    )
    requested_risk_policy = _requested_policy_payload(
        request_metadata=None,
        policy_name="risk_policy",
        source_policies=source_policies,
        active_policy_rollouts=active_policy_rollouts,
    )
    normalized_policy = normalize_execution_policy(
        {
            "execution_policy": requested_policy,
            "risk_policy": requested_risk_policy,
        }
    )
    if not deployment_mode_auto_executes(str(normalized_policy["deployment_mode"])):
        return {
            "action": "auto_submit",
            "changed": False,
            "reason": "execution_disabled",
            "message": "Automatic execution is disabled for this live collector.",
            "policy": normalized_policy,
        }

    gate = assess_open_activity_gate(
        activity_kind=OPEN_ACTIVITY_AUTO,
        storage=storage,
    )
    if not gate["allowed"]:
        publish_control_gate_event(
            db_target=db_target,
            decision=gate,
            activity_kind=OPEN_ACTIVITY_AUTO,
            session_id=session_id,
            session_date=None,
            label=None,
            candidate_id=None,
            cycle_id=cycle_id,
        )
        return {
            "action": "auto_submit",
            "changed": False,
            "reason": str(gate["reason"]),
            "message": str(gate["message"]),
            "policy": normalized_policy,
            "control": dict(gate["control"]),
        }

    execution_store = storage.execution
    signal_store = getattr(storage, "signals", None)
    _require_execution_schema(execution_store)
    _require_position_schema(execution_store)
    execution_plan = _resolve_auto_execution_plan(
        signal_store=signal_store,
        cycle_id=cycle_id,
    )
    if not bool(execution_plan.get("available", False)):
        return {
            "action": "auto_submit",
            "changed": False,
            "reason": "live_opportunity_store_unavailable",
            "message": "Automatic execution skipped because the live opportunity store is unavailable.",
            "policy": normalized_policy,
        }
    ranked_opportunities = list(execution_plan.get("opportunities") or [])
    allocation_decisions = list(execution_plan.get("allocation_decisions") or [])
    execution_intents = list(execution_plan.get("execution_intents") or [])
    opportunity_rows_by_id = dict(execution_plan.get("opportunity_rows_by_id") or {})
    plan_summary = {
        "candidate_count": len(ranked_opportunities),
        "allocation_count": len(allocation_decisions),
        "execution_intent_count": len(execution_intents),
        "top_opportunity_id": (
            None if not ranked_opportunities else ranked_opportunities[0].opportunity_id
        ),
        "top_symbol": None
        if not ranked_opportunities
        else ranked_opportunities[0].symbol,
        "top_strategy_family": None
        if not ranked_opportunities
        else ranked_opportunities[0].strategy_family,
    }
    if not ranked_opportunities:
        return {
            "action": "auto_submit",
            "changed": False,
            "reason": "no_live_opportunity",
            "message": "Automatic execution skipped because the cycle does not have an active live opportunity.",
            "policy": normalized_policy,
            "execution_plan": plan_summary,
        }

    selected_intent = execution_intents[0] if execution_intents else None
    selected_decision = (
        None
        if selected_intent is None
        else next(
            (
                decision
                for decision in allocation_decisions
                if decision.opportunity_id == selected_intent.opportunity_id
            ),
            None,
        )
    )
    if selected_intent is None:
        top_decision = allocation_decisions[0] if allocation_decisions else None
        top_opportunity = ranked_opportunities[0] if ranked_opportunities else None
        top_row = (
            None
            if top_opportunity is None
            else opportunity_rows_by_id.get(top_opportunity.opportunity_id)
        )
        selected_candidate_id = None
        if isinstance(top_row, Mapping):
            selected_candidate_id = _coerce_int(top_row.get("source_candidate_id"))
        reason_code = (
            None
            if top_decision is None or not top_decision.rejection_codes
            else str(top_decision.rejection_codes[0])
        )
        return {
            "action": "auto_submit",
            "changed": False,
            "reason": reason_code or "no_allocated_opportunity",
            "message": (
                "Automatic execution skipped because no live opportunity cleared the "
                "execution planner."
                if top_decision is None
                else top_decision.allocation_reason
            ),
            "policy": normalized_policy,
            "selected_candidate_id": selected_candidate_id,
            "selected_opportunity_id": None
            if top_opportunity is None
            else top_opportunity.opportunity_id,
            "execution_plan": {
                **plan_summary,
                "top_allocation_decision": (
                    None if top_decision is None else top_decision.to_payload()
                ),
            },
        }

    selected_opportunity = opportunity_rows_by_id.get(selected_intent.opportunity_id)
    if not isinstance(selected_opportunity, Mapping):
        return {
            "action": "auto_submit",
            "changed": False,
            "reason": "selected_opportunity_missing",
            "message": "Automatic execution skipped because the selected live opportunity could not be reloaded.",
            "policy": normalized_policy,
            "selected_opportunity_id": selected_intent.opportunity_id,
            "execution_plan": plan_summary,
        }
    selected_candidate_id = _coerce_int(selected_opportunity.get("source_candidate_id"))
    if selected_candidate_id is None:
        return {
            "action": "auto_submit",
            "changed": False,
            "reason": "selected_opportunity_missing_candidate",
            "message": "Automatic execution skipped because the selected live opportunity is missing its source candidate id.",
            "policy": normalized_policy,
            "selected_opportunity_id": selected_intent.opportunity_id,
            "execution_plan": plan_summary,
        }
    selected_candidate = _candidate_with_payload(dict(selected_opportunity))
    blocked_reason, blocked_message = _validate_auto_execution_candidate(
        selected_candidate
    )
    if blocked_reason is not None:
        return {
            "action": "auto_submit",
            "changed": False,
            "reason": blocked_reason,
            "message": blocked_message,
            "policy": normalized_policy,
            "selected_candidate_id": selected_candidate_id,
            "selected_opportunity_id": selected_intent.opportunity_id,
            "execution_plan": {
                **plan_summary,
                "selected_execution_intent": selected_intent.to_payload(),
                "selected_allocation_decision": (
                    None
                    if selected_decision is None
                    else selected_decision.to_payload()
                ),
            },
        }

    reactive_quote: dict[str, Any] | None = None
    limit_price: float | None = None
    if _as_text(selected_candidate.get("profile")) == "0dte":
        reactive_resolution = _resolve_reactive_auto_execution(
            candidate=selected_candidate,
            execution_policy=normalized_policy,
            quote_records=reactive_quote_records,
        )
        if not reactive_resolution.get("ok"):
            return {
                "action": "auto_submit",
                "changed": False,
                "reason": str(reactive_resolution["reason"]),
                "message": str(reactive_resolution["message"]),
                "policy": normalized_policy,
                "selected_candidate_id": selected_candidate_id,
                "selected_opportunity_id": selected_intent.opportunity_id,
                "reactive_quote": reactive_resolution.get("reactive_quote"),
                "execution_plan": {
                    **plan_summary,
                    "selected_execution_intent": selected_intent.to_payload(),
                    "selected_allocation_decision": (
                        None
                        if selected_decision is None
                        else selected_decision.to_payload()
                    ),
                },
            }
        limit_price = _coerce_float(reactive_resolution.get("limit_price"))
        reactive_quote = (
            dict(reactive_resolution["reactive_quote"])
            if isinstance(reactive_resolution.get("reactive_quote"), dict)
            else None
        )

    try:
        result = submit_opportunity_execution(
            db_target=db_target,
            opportunity_id=selected_intent.opportunity_id,
            quantity=int(normalized_policy["quantity"]),
            limit_price=limit_price,
            request_metadata={
                "source": {
                    "kind": "auto_opportunity_execution",
                    "mode": normalized_policy["mode"],
                    "cycle_id": cycle_id,
                    "job_run_id": job_run_id,
                    "candidate_id": selected_candidate_id,
                    "opportunity_id": selected_intent.opportunity_id,
                    "reason": "allocator_selected",
                },
                "opportunity_id": selected_intent.opportunity_id,
                "allocation_decision": (
                    None
                    if selected_decision is None
                    else selected_decision.to_payload()
                ),
                "execution_intent": selected_intent.to_payload(),
                "auto_execution_plan": {
                    **plan_summary,
                    "selected_execution_intent": selected_intent.to_payload(),
                },
                "execution_policy": normalized_policy,
                **(
                    {} if reactive_quote is None else {"reactive_quote": reactive_quote}
                ),
            },
        )
    except Exception as exc:
        blocked = _classify_auto_execution_block(exc)
        if blocked is None:
            raise
        return {
            "action": "auto_submit",
            "changed": False,
            "policy": normalized_policy,
            "selected_candidate_id": selected_candidate_id,
            "selected_opportunity_id": selected_intent.opportunity_id,
            **blocked,
            **({} if reactive_quote is None else {"reactive_quote": reactive_quote}),
            "execution_plan": {
                **plan_summary,
                "selected_execution_intent": selected_intent.to_payload(),
                "selected_allocation_decision": (
                    None
                    if selected_decision is None
                    else selected_decision.to_payload()
                ),
            },
        }
    return {
        **result,
        "action": "auto_submit",
        "reason": None,
        "policy": normalized_policy,
        "selected_candidate_id": selected_candidate_id,
        "selected_opportunity_id": selected_intent.opportunity_id,
        **({} if reactive_quote is None else {"reactive_quote": reactive_quote}),
        "execution_plan": {
            **plan_summary,
            "selected_execution_intent": selected_intent.to_payload(),
            "selected_allocation_decision": (
                None if selected_decision is None else selected_decision.to_payload()
            ),
        },
    }
