from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.services.candidate_policy import resolve_candidate_profile, resolve_deployment_quality_thresholds
from core.services.execution_portfolio import build_structure_quote_snapshot
from core.services.option_structures import (
    build_order_payload,
    candidate_legs,
    closing_legs,
    net_premium_kind,
    normalize_legs,
    order_payload_legs,
)
from core.services.session_positions import (
    CLOSE_TRADE_INTENT,
    OPEN_TRADE_INTENT,
)
from core.services.value_coercion import (
    as_text,
    coerce_float,
    coerce_int,
)
from .shared import (
    DEFAULT_ENTRY_PRICING_MODE,
    DEFAULT_MAX_CREDIT_CONCESSION,
    DEFAULT_MIN_CREDIT_RETENTION_PCT,
    _clamp_fraction,
    _normalize_limit_value,
    _strategy_family_from_payload,
)

LONG_VOL_DEBIT_FAMILIES = {"long_straddle", "long_strangle"}


def _resolve_candidate_entry_prices(
    candidate_payload: dict[str, Any],
) -> tuple[float | None, float | None]:
    midpoint_value = _normalize_limit_value(candidate_payload.get("midpoint_value", candidate_payload.get("midpoint_credit")))
    natural_value = _normalize_limit_value(candidate_payload.get("natural_value", candidate_payload.get("natural_credit")))
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


def _capped_structure_return_on_risk(
    *,
    midpoint_value: float | None,
    span_value: float | None,
    premium_kind: str | None,
) -> float | None:
    if midpoint_value is None or span_value is None or midpoint_value <= 0 or span_value <= 0 or midpoint_value >= span_value:
        return None
    if premium_kind == "debit":
        return round((span_value - midpoint_value) / midpoint_value, 4)
    return round(midpoint_value / (span_value - midpoint_value), 4)


def _candidate_capped_structure_span(
    candidate_payload: Mapping[str, Any],
) -> float | None:
    width = coerce_float(candidate_payload.get("width"))
    if width is not None and width > 0:
        return width
    max_profit = coerce_float(candidate_payload.get("max_profit"))
    max_loss = coerce_float(candidate_payload.get("max_loss"))
    if max_profit is None or max_loss is None:
        return None
    span_dollars = max_profit + max_loss
    if span_dollars <= 0:
        return None
    return round(span_dollars / 100.0, 4)


def _validate_uncapped_debit_live_quality(
    *,
    candidate_payload: Mapping[str, Any],
    execution_policy: Mapping[str, Any],
    profile: str,
    live_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    live_midpoint_value = _normalize_limit_value(live_snapshot.get("midpoint_value"))
    live_natural_value = _normalize_limit_value(live_snapshot.get("natural_value"))
    if live_midpoint_value is None or live_natural_value is None or live_midpoint_value <= 0 or live_natural_value <= 0:
        return {
            "ok": False,
            "reason": "live_quotes_not_executable",
            "message": "Open execution is blocked because the live long-vol debit quotes were not executable.",
            "profile": profile,
            "live_quote": dict(live_snapshot),
        }

    scanned_midpoint_value, _ = _resolve_candidate_entry_prices(dict(candidate_payload))
    min_retention_pct = _clamp_fraction(
        coerce_float(execution_policy.get("min_credit_retention_pct")) or DEFAULT_MIN_CREDIT_RETENTION_PCT,
        minimum=0.5,
        maximum=1.0,
    )
    live_quote = {
        **dict(live_snapshot),
        "live_debit": live_midpoint_value,
        "live_natural_debit": live_natural_value,
        "live_fill_ratio": _entry_fill_ratio(
            midpoint_value=live_midpoint_value,
            natural_value=live_natural_value,
            premium_kind="debit",
        ),
    }
    if scanned_midpoint_value is not None and scanned_midpoint_value > 0:
        debit_ceiling = _execution_retention_bound(
            midpoint_value=scanned_midpoint_value,
            premium_kind="debit",
            min_retention_pct=min_retention_pct,
        )
        live_quote.update(
            {
                "scanned_debit": scanned_midpoint_value,
                "debit_ceiling": debit_ceiling,
                "min_retention_pct": min_retention_pct,
            }
        )
        if live_midpoint_value > debit_ceiling:
            return {
                "ok": False,
                "reason": "live_debit_above_ceiling",
                "message": (
                    "Open execution is blocked because the live long-vol debit "
                    f"{live_midpoint_value:.2f} rose above the execution ceiling "
                    f"{debit_ceiling:.2f}."
                ),
                "profile": profile,
                "live_quote": live_quote,
            }

    return {
        "ok": True,
        "profile": profile,
        "live_quote": live_quote,
    }


def _validate_live_deployment_quality(
    *,
    candidate_payload: Mapping[str, Any],
    deployment_mode: str | None = None,
    execution_policy: Mapping[str, Any] | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    profile = resolve_candidate_profile(candidate_payload)
    thresholds = resolve_deployment_quality_thresholds(profile)
    strategy_family = _strategy_family_from_payload(candidate_payload)
    premium_kind = net_premium_kind(strategy_family)
    legs = candidate_legs(candidate_payload)
    if strategy_family in LONG_VOL_DEBIT_FAMILIES:
        if len(legs) < 2 or any(str(leg.get("role") or "").strip().lower() != "long" for leg in legs):
            return {
                "ok": False,
                "reason": "long_vol_legs_unavailable",
                "message": "Open execution is blocked because the long-vol candidate is missing two long option legs.",
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
                "message": "Open execution is blocked because a current live long-vol quote snapshot is unavailable.",
                "profile": profile,
                "quote_error": error_text,
            }
        return _validate_uncapped_debit_live_quality(
            candidate_payload=candidate_payload,
            execution_policy={} if execution_policy is None else execution_policy,
            profile=profile,
            live_snapshot=live_snapshot,
        )

    minimum_return_on_risk = coerce_float(thresholds.get("min_execution_return_on_risk"))
    if minimum_return_on_risk is None:
        return {
            "ok": True,
            "profile": profile,
        }

    span_value = _candidate_capped_structure_span(candidate_payload)
    if not legs or span_value is None or span_value <= 0:
        return {
            "ok": False,
            "reason": "live_deployment_quality_unavailable",
            "message": ("Open execution is blocked because the candidate is missing capped-risk structure geometry for live deployment validation."),
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
            "message": ("Open execution is blocked because a current live multi-leg structure snapshot is unavailable."),
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
            "message": ("Open execution is blocked because the live structure quotes were not executable."),
            "profile": profile,
            "live_quote": live_snapshot,
        }

    if live_return_on_risk < minimum_return_on_risk:
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

    pricing_mode = str(execution_policy.get("pricing_mode") or DEFAULT_ENTRY_PRICING_MODE)
    if pricing_mode == "midpoint" or natural_value is None or natural_value <= 0:
        return round(max(midpoint_value, 0.01), 2)

    fill_ratio = _clamp_fraction(coerce_float(candidate_payload.get("fill_ratio")) or 0.0, maximum=1.0)
    min_credit_retention_pct = _clamp_fraction(
        coerce_float(execution_policy.get("min_credit_retention_pct")) or DEFAULT_MIN_CREDIT_RETENTION_PCT,
        minimum=0.5,
        maximum=1.0,
    )
    max_credit_concession = max(
        coerce_float(execution_policy.get("max_credit_concession")) or DEFAULT_MAX_CREDIT_CONCESSION,
        0.0,
    )
    if premium_kind == "debit":
        debit_ceiling = _execution_retention_bound(
            midpoint_value=midpoint_value,
            premium_kind=premium_kind,
            min_retention_pct=min_credit_retention_pct,
        )
        max_concession_to_ceiling = max(debit_ceiling - midpoint_value, 0.0)
        fill_ratio_concession = max(natural_value - midpoint_value, 0.0) * max(1.0 - fill_ratio, 0.0)
        concession = min(fill_ratio_concession, max_credit_concession, max_concession_to_ceiling)
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
    fill_ratio_concession = max(midpoint_value - natural_value, 0.0) * max(1.0 - fill_ratio, 0.0)
    concession = min(fill_ratio_concession, max_credit_concession, max_concession_to_floor)
    return round(max(midpoint_value - concession, credit_floor, 0.01), 2)


def _build_close_order_request(
    *,
    position: dict[str, Any],
    quantity: int | None,
    limit_price: float | None,
    client_order_id: str,
) -> tuple[dict[str, Any], int, float]:
    remaining_quantity = coerce_float(position.get("remaining_quantity"))
    if remaining_quantity is None or remaining_quantity <= 0:
        raise ValueError("Session position does not have remaining quantity to close")
    resolved_quantity = quantity if quantity is not None else int(round(remaining_quantity))
    if resolved_quantity <= 0:
        raise ValueError("Close quantity must be positive")
    if resolved_quantity > remaining_quantity:
        raise ValueError("Close quantity exceeds the remaining session position quantity")

    resolved_limit_price = limit_price if limit_price is not None else coerce_float(position.get("close_mark"))
    resolved_limit_price = _normalize_limit_value(resolved_limit_price)
    if resolved_limit_price is None or resolved_limit_price <= 0:
        raise ValueError("Close execution requires a positive limit price or a quoted close mark")

    strategy_family = _strategy_family_from_payload(position)
    resolved_legs = normalize_legs(position.get("legs"))
    if not resolved_legs:
        resolved_legs = candidate_legs(position)
    if not resolved_legs:
        raise ValueError("Close execution requires canonical position legs")
    request = build_order_payload(
        legs=closing_legs(resolved_legs),
        limit_price=float(resolved_limit_price),
        strategy_family=strategy_family,
        trade_intent=CLOSE_TRADE_INTENT,
        quantity=resolved_quantity,
    )
    request["client_order_id"] = client_order_id
    return request, int(resolved_quantity), round(float(resolved_limit_price), 2)


def _normalize_submit_order_request(
    *,
    payload: Mapping[str, Any],
    order_request: Mapping[str, Any],
) -> dict[str, Any]:
    request = dict(order_request)
    request_legs = order_payload_legs(
        request,
        expiration_date=as_text(payload.get("expiration_date")),
    ) or normalize_legs(payload.get("legs"))
    if not request_legs or len(request_legs) != 1:
        return request
    requires_single_leg_rebuild = str(request.get("order_class") or "").strip().lower() == "mleg" or "symbol" not in request or "side" not in request
    if not requires_single_leg_rebuild:
        return request
    limit_price = coerce_float(request.get("limit_price")) or coerce_float(payload.get("limit_price"))
    if limit_price is None:
        return request
    quantity = coerce_int(request.get("qty")) or coerce_int(payload.get("quantity")) or 1
    normalized_request = build_order_payload(
        legs=request_legs,
        limit_price=limit_price,
        strategy_family=_strategy_family_from_payload(payload),
        trade_intent=str(payload.get("trade_intent") or OPEN_TRADE_INTENT),
        quantity=quantity,
    )
    client_order_id = as_text(request.get("client_order_id"))
    if client_order_id is not None:
        normalized_request["client_order_id"] = client_order_id
    return normalized_request
