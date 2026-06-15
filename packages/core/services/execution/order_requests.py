from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from core.common import clamp
from core.money import option_limit_price
from core.services.account_capacity import estimate_buying_power_requirement
from core.services.candidate_policy import resolve_candidate_profile, resolve_deployment_quality_thresholds
from core.services.execution_portfolio import build_structure_quote_snapshot
from core.services.option_structures import (
    build_order_payload,
    candidate_legs,
    closing_legs,
    common_expiration_date,
    net_premium_kind,
    normalize_legs,
    order_payload_legs,
    signed_net_limit_price,
)
from core.services.session_positions import (
    CLOSE_TRADE_INTENT,
    OPEN_TRADE_INTENT,
)
from core.storage.serializers import parse_datetime
from core.value_coercion import (
    as_text,
    coerce_float,
    coerce_int,
)
from .shared import (
    DEFAULT_ENTRY_PRICING_MODE,
    DEFAULT_MAX_CREDIT_CONCESSION,
    DEFAULT_MIN_CREDIT_RETENTION_PCT,
    _normalize_limit_value,
    _strategy_family_from_payload,
)

LONG_VOL_DEBIT_FAMILIES = {"long_straddle", "long_strangle"}
SUPPORTED_ALPACA_MLEG_FAMILIES = {
    "call_credit_spread",
    "put_credit_spread",
    "call_debit_spread",
    "put_debit_spread",
    "iron_condor",
    "long_straddle",
    "long_strangle",
}
SUPPORTED_SINGLE_OPTION_FAMILIES = {
    "long_call",
    "long_put",
    "short_put",
}
OPTION_STRUCTURE_FAMILIES = SUPPORTED_ALPACA_MLEG_FAMILIES.union(SUPPORTED_SINGLE_OPTION_FAMILIES)
FAMILY_LEG_COUNTS = {
    "call_credit_spread": 2,
    "put_credit_spread": 2,
    "call_debit_spread": 2,
    "put_debit_spread": 2,
    "iron_condor": 4,
    "long_straddle": 2,
    "long_strangle": 2,
    "long_call": 1,
    "long_put": 1,
    "short_put": 1,
}
FAMILY_OPTION_TYPES = {
    "call_credit_spread": {"call"},
    "call_debit_spread": {"call"},
    "long_call": {"call"},
    "put_credit_spread": {"put"},
    "put_debit_spread": {"put"},
    "short_put": {"put"},
    "long_put": {"put"},
}
DEFAULT_STRUCTURE_QUOTE_MAX_AGE_SECONDS = 180.0
VALID_POSITION_INTENTS = {
    "buy_to_open",
    "sell_to_open",
    "buy_to_close",
    "sell_to_close",
}
VALID_ROLE_POSITION_INTENTS = {
    "long": {"buy_to_open", "sell_to_close"},
    "short": {"sell_to_open", "buy_to_close"},
}


def _structure_guard_block(
    reason: str,
    message: str,
    **evidence: Any,
) -> dict[str, Any]:
    return {
        "ok": False,
        "reason": reason,
        "message": message,
        "reason_codes": [reason],
        "blockers": [reason],
        "evidence": {key: value for key, value in evidence.items() if value is not None},
    }


def _structure_guard_ok(**evidence: Any) -> dict[str, Any]:
    return {
        "ok": True,
        "reason": "approved",
        "reason_codes": ["approved"],
        "blockers": [],
        "evidence": {key: value for key, value in evidence.items() if value is not None},
    }


def _normalized_attempt_legs(
    *,
    payload: Mapping[str, Any],
    order_request: Mapping[str, Any],
) -> list[dict[str, Any]]:
    expiration_date = as_text(payload.get("expiration_date"))
    candidate_payload = payload.get("candidate") if isinstance(payload.get("candidate"), Mapping) else {}
    return (
        normalize_legs(payload.get("legs"), expiration_date=expiration_date)
        or candidate_legs(candidate_payload)
        or order_payload_legs(order_request, expiration_date=expiration_date)
    )


def _role_counts(legs: list[Mapping[str, Any]]) -> dict[str, int]:
    counts = {"long": 0, "short": 0}
    for leg in legs:
        role = as_text(leg.get("role"))
        if role in counts:
            counts[role] += 1
    return counts


def _type_counts(legs: list[Mapping[str, Any]]) -> dict[str, int]:
    counts = {"call": 0, "put": 0}
    for leg in legs:
        option_type = as_text(leg.get("option_type"))
        if option_type in counts:
            counts[option_type] += 1
    return counts


def _strike_values(legs: list[Mapping[str, Any]]) -> list[float]:
    values: list[float] = []
    for leg in legs:
        strike = coerce_float(leg.get("strike"))
        if strike is not None:
            values.append(strike)
    return values


def _quote_timestamp_from_mapping(value: Any) -> str | None:
    if not isinstance(value, Mapping):
        return None
    for key in ("timestamp", "captured_at", "observed_at", "generated_at", "source_timestamp"):
        timestamp = as_text(value.get(key))
        if timestamp is not None:
            return timestamp
    quote_metrics = value.get("quote_metrics")
    if isinstance(quote_metrics, Mapping):
        return _quote_timestamp_from_mapping(quote_metrics)
    quote_freshness = value.get("quote_freshness")
    if isinstance(quote_freshness, Mapping):
        return _quote_timestamp_from_mapping(quote_freshness)
    return None


def _option_structure_quote_timestamp(
    *,
    payload: Mapping[str, Any],
    request_payload: Mapping[str, Any],
    candidate_payload: Mapping[str, Any],
) -> str | None:
    for value in (
        request_payload.get("option_selection"),
        candidate_payload.get("option_selection"),
        candidate_payload.get("quote_metrics"),
        candidate_payload.get("quote_freshness"),
        candidate_payload,
        payload,
    ):
        timestamp = _quote_timestamp_from_mapping(value)
        if timestamp is not None:
            return timestamp
    return as_text(payload.get("candidate_generated_at")) or as_text(payload.get("requested_at"))


def _option_structure_max_quote_age_seconds(
    *,
    request_payload: Mapping[str, Any],
    candidate_payload: Mapping[str, Any],
) -> float:
    execution_policy = request_payload.get("execution_policy") if isinstance(request_payload.get("execution_policy"), Mapping) else {}
    risk_policy = request_payload.get("risk_policy") if isinstance(request_payload.get("risk_policy"), Mapping) else {}
    quote_freshness = candidate_payload.get("quote_freshness") if isinstance(candidate_payload.get("quote_freshness"), Mapping) else {}
    for value in (
        execution_policy.get("max_quote_age_seconds"),
        execution_policy.get("stale_quote_after_seconds"),
        risk_policy.get("stale_quote_after_seconds"),
        risk_policy.get("max_candidate_age_seconds"),
        candidate_payload.get("max_quote_age_seconds"),
        quote_freshness.get("max_quote_age_seconds"),
    ):
        parsed = coerce_float(value)
        if parsed is not None and parsed > 0:
            return parsed
    return DEFAULT_STRUCTURE_QUOTE_MAX_AGE_SECONDS


def _validate_leg_contract(
    *,
    legs: list[dict[str, Any]],
    family: str,
) -> dict[str, Any] | None:
    expected_count = FAMILY_LEG_COUNTS.get(family)
    if expected_count is None:
        return _structure_guard_block(
            "unsupported_option_family",
            f"Option submission is blocked because {family} is not an explicitly supported execution family.",
            strategy_family=family,
        )
    if len(legs) != expected_count:
        return _structure_guard_block(
            "invalid_leg_count",
            f"Option submission is blocked because {family} requires {expected_count} leg(s), not {len(legs)}.",
            strategy_family=family,
            expected_leg_count=expected_count,
            leg_count=len(legs),
        )
    if common_expiration_date(legs) is None:
        return _structure_guard_block(
            "mixed_expiration_legs",
            "Option submission is blocked because all structure legs must share one expiration date.",
            strategy_family=family,
            leg_count=len(legs),
        )
    for leg in legs:
        symbol = as_text(leg.get("symbol"))
        role = as_text(leg.get("role"))
        position_intent = as_text(leg.get("position_intent"))
        side = as_text(leg.get("side"))
        ratio_qty = coerce_float(leg.get("ratio_qty"))
        if symbol is None:
            return _structure_guard_block("missing_leg_symbol", "Option submission is blocked because a leg is missing its symbol.")
        if role not in {"long", "short"}:
            return _structure_guard_block(
                "invalid_leg_role",
                "Option submission is blocked because each leg must resolve to a long or short role.",
                leg_symbol=symbol,
                role=role,
            )
        if side not in {"buy", "sell"}:
            return _structure_guard_block(
                "invalid_leg_side",
                "Option submission is blocked because each leg must resolve to buy or sell.",
                leg_symbol=symbol,
                side=side,
            )
        if position_intent not in VALID_POSITION_INTENTS:
            return _structure_guard_block(
                "invalid_position_intent",
                "Option submission is blocked because each leg must include a valid position intent.",
                leg_symbol=symbol,
                position_intent=position_intent,
            )
        if position_intent not in VALID_ROLE_POSITION_INTENTS.get(role, set()):
            return _structure_guard_block(
                "position_intent_role_mismatch",
                "Option submission is blocked because a leg role does not match its position intent.",
                leg_symbol=symbol,
                role=role,
                position_intent=position_intent,
            )
        if ratio_qty is None or ratio_qty <= 0:
            return _structure_guard_block(
                "invalid_leg_ratio",
                "Option submission is blocked because each leg must have a positive ratio quantity.",
                leg_symbol=symbol,
                ratio_qty=leg.get("ratio_qty"),
            )

    role_counts = _role_counts(legs)
    type_counts = _type_counts(legs)
    expected_types = FAMILY_OPTION_TYPES.get(family)
    if expected_types is not None and any(as_text(leg.get("option_type")) not in expected_types for leg in legs):
        return _structure_guard_block(
            "invalid_leg_mix",
            f"Option submission is blocked because {family} requires {', '.join(sorted(expected_types))} option legs.",
            strategy_family=family,
            option_type_counts=type_counts,
        )

    if family in {"call_credit_spread", "put_credit_spread", "call_debit_spread", "put_debit_spread"} and role_counts != {
        "long": 1,
        "short": 1,
    }:
        return _structure_guard_block(
            "invalid_role_symmetry",
            "Option submission is blocked because vertical spreads require one long and one short leg.",
            strategy_family=family,
            role_counts=role_counts,
        )
    if family == "iron_condor":
        valid_condor = role_counts == {"long": 2, "short": 2} and type_counts == {"call": 2, "put": 2}
        for option_type in ("call", "put"):
            matching = [leg for leg in legs if as_text(leg.get("option_type")) == option_type]
            valid_condor = valid_condor and _role_counts(matching) == {"long": 1, "short": 1}
        if not valid_condor:
            return _structure_guard_block(
                "invalid_role_symmetry",
                "Option submission is blocked because iron condors require one long and one short call plus one long and one short put.",
                strategy_family=family,
                role_counts=role_counts,
                option_type_counts=type_counts,
            )
    if family in LONG_VOL_DEBIT_FAMILIES:
        if role_counts != {"long": 2, "short": 0} or type_counts != {"call": 1, "put": 1}:
            return _structure_guard_block(
                "invalid_leg_mix",
                "Option submission is blocked because long-vol structures require one long call and one long put.",
                strategy_family=family,
                role_counts=role_counts,
                option_type_counts=type_counts,
            )
        strikes = _strike_values(legs)
        if family == "long_straddle" and len(set(strikes)) > 1:
            return _structure_guard_block(
                "invalid_leg_mix",
                "Option submission is blocked because long straddles require matching strikes.",
                strategy_family=family,
                strikes=strikes,
            )
        if family == "long_strangle" and len(set(strikes)) == 1 and len(strikes) == len(legs):
            return _structure_guard_block(
                "invalid_leg_mix",
                "Option submission is blocked because long strangles require different strikes.",
                strategy_family=family,
                strikes=strikes,
            )
    if family in {"long_call", "long_put"} and role_counts != {"long": 1, "short": 0}:
        return _structure_guard_block(
            "invalid_role_symmetry",
            "Option submission is blocked because long single-leg options require one long leg.",
            strategy_family=family,
            role_counts=role_counts,
        )
    if family == "short_put" and role_counts != {"long": 0, "short": 1}:
        return _structure_guard_block(
            "invalid_role_symmetry",
            "Option submission is blocked because short puts require one short put leg.",
            strategy_family=family,
            role_counts=role_counts,
        )
    return None


def _validate_option_structure_submission(
    *,
    payload: Mapping[str, Any],
    order_request: Mapping[str, Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    legs = _normalized_attempt_legs(payload=payload, order_request=order_request)
    if not legs:
        return {"ok": True, "reason": "not_option_structure", "reason_codes": [], "blockers": [], "evidence": {}}
    family = _strategy_family_from_payload(payload)
    trade_intent = str(payload.get("trade_intent") or OPEN_TRADE_INTENT).strip().lower()
    if family not in OPTION_STRUCTURE_FAMILIES:
        return _structure_guard_block(
            "unsupported_option_family",
            f"Option submission is blocked because {family} is not explicitly supported for broker submission.",
            strategy_family=family,
        )

    order_class = str(order_request.get("order_class") or "").strip().lower()
    if len(legs) > 1:
        if family not in SUPPORTED_ALPACA_MLEG_FAMILIES:
            return _structure_guard_block(
                "unsupported_mleg_family",
                f"Option submission is blocked because {family} is not enabled for Alpaca mleg submission.",
                strategy_family=family,
                leg_count=len(legs),
            )
        if order_class != "mleg":
            return _structure_guard_block(
                "mleg_order_class_required",
                "Option submission is blocked because multi-leg options must use Alpaca order_class=mleg.",
                strategy_family=family,
                order_class=order_class or None,
                leg_count=len(legs),
            )
        if len(legs) < 2 or len(legs) > 4:
            return _structure_guard_block(
                "invalid_leg_count",
                "Option submission is blocked because Alpaca mleg option orders must contain 2 to 4 legs.",
                strategy_family=family,
                leg_count=len(legs),
            )
    elif order_class == "mleg":
        return _structure_guard_block(
            "invalid_leg_count",
            "Option submission is blocked because Alpaca mleg orders require at least two legs.",
            strategy_family=family,
            leg_count=len(legs),
        )

    normalized_legs = normalize_legs(legs, expiration_date=as_text(payload.get("expiration_date")))
    leg_error = _validate_leg_contract(
        legs=normalized_legs,
        family=family,
    )
    if leg_error is not None:
        return leg_error

    quantity = coerce_int(order_request.get("qty")) or coerce_int(payload.get("quantity"))
    if quantity is None or quantity <= 0:
        return _structure_guard_block(
            "invalid_quantity",
            "Option submission is blocked because the requested quantity must be positive.",
            strategy_family=family,
            quantity=order_request.get("qty") or payload.get("quantity"),
        )
    limit_price = coerce_float(order_request.get("limit_price"))
    if limit_price is None or limit_price == 0:
        return _structure_guard_block(
            "invalid_limit_price",
            "Option submission is blocked because the broker order payload requires a non-zero limit price.",
            strategy_family=family,
            limit_price=order_request.get("limit_price"),
        )
    premium_kind = net_premium_kind(family)
    if len(normalized_legs) > 1 and premium_kind is None:
        return _structure_guard_block(
            "unsupported_mleg_family",
            f"Option submission is blocked because {family} does not resolve a credit/debit premium kind.",
            strategy_family=family,
        )
    if len(normalized_legs) > 1:
        expected_signed_limit = signed_net_limit_price(
            limit_price=abs(limit_price),
            strategy_family=family,
            trade_intent=trade_intent,
        )
        if expected_signed_limit < 0 and limit_price >= 0:
            return _structure_guard_block(
                "invalid_net_credit_sign",
                "Option submission is blocked because opening credit structures require a negative mleg limit price.",
                strategy_family=family,
                trade_intent=trade_intent,
                limit_price=limit_price,
                expected_limit_sign="negative",
            )
        if expected_signed_limit > 0 and limit_price <= 0:
            return _structure_guard_block(
                "invalid_net_debit_sign",
                "Option submission is blocked because this structure requires a positive mleg limit price.",
                strategy_family=family,
                trade_intent=trade_intent,
                limit_price=limit_price,
                expected_limit_sign="positive",
            )

    candidate_payload = dict(payload.get("candidate") or {}) if isinstance(payload.get("candidate"), Mapping) else {}
    candidate_payload.update(
        {
            "strategy": family,
            "strategy_family": family,
            "legs": normalized_legs,
        }
    )
    economics_payload = dict(payload.get("economics") or {}) if isinstance(payload.get("economics"), Mapping) else {}
    for key, value in economics_payload.items():
        candidate_payload.setdefault(key, value)
    if trade_intent == OPEN_TRADE_INTENT:
        buying_power_requirement = estimate_buying_power_requirement(
            candidate_payload,
            quantity,
            limit_price=abs(limit_price),
        )
        required_buying_power = coerce_float(buying_power_requirement.get("required_buying_power"))
        if required_buying_power is None or required_buying_power <= 0:
            return _structure_guard_block(
                "max_risk_unavailable",
                "Option submission is blocked because max risk or buying-power requirement could not be resolved.",
                strategy_family=family,
                quantity=quantity,
                buying_power_basis=as_text(buying_power_requirement.get("basis")),
            )

    request_payload = payload.get("request") if isinstance(payload.get("request"), Mapping) else {}
    quote_timestamp = _option_structure_quote_timestamp(
        payload=payload,
        request_payload=request_payload,
        candidate_payload=candidate_payload,
    )
    parsed_quote_timestamp = parse_datetime(quote_timestamp)
    if parsed_quote_timestamp is None:
        return _structure_guard_block(
            "quote_timestamp_missing",
            "Option submission is blocked because the option structure quote timestamp is unavailable.",
            strategy_family=family,
        )
    if parsed_quote_timestamp.tzinfo is None:
        parsed_quote_timestamp = parsed_quote_timestamp.replace(tzinfo=UTC)
    resolved_now = now or datetime.now(UTC)
    quote_age_seconds = max((resolved_now - parsed_quote_timestamp.astimezone(UTC)).total_seconds(), 0.0)
    max_quote_age_seconds = _option_structure_max_quote_age_seconds(
        request_payload=request_payload,
        candidate_payload=candidate_payload,
    )
    if quote_age_seconds > max_quote_age_seconds:
        return _structure_guard_block(
            "stale_quote_snapshot",
            "Option submission is blocked because the option structure quote snapshot is stale.",
            strategy_family=family,
            quote_timestamp=quote_timestamp,
            quote_age_seconds=round(quote_age_seconds, 3),
            max_quote_age_seconds=max_quote_age_seconds,
        )

    return _structure_guard_ok(
        strategy_family=family,
        trade_intent=trade_intent,
        leg_count=len(normalized_legs),
        order_class=order_class or "single",
        quantity=quantity,
        limit_price=limit_price,
        quote_timestamp=quote_timestamp,
        quote_age_seconds=round(quote_age_seconds, 3),
        max_quote_age_seconds=max_quote_age_seconds,
    )


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
        return round(clamp(midpoint_value / natural_value, high=1.0), 4)
    return round(clamp(natural_value / midpoint_value, high=1.0), 4)


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

    candidate_midpoint_value, _ = _resolve_candidate_entry_prices(dict(candidate_payload))
    min_retention_pct = clamp(
        coerce_float(execution_policy.get("min_credit_retention_pct")) or DEFAULT_MIN_CREDIT_RETENTION_PCT,
        low=0.5,
        high=1.0,
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
    if candidate_midpoint_value is not None and candidate_midpoint_value > 0:
        debit_ceiling = _execution_retention_bound(
            midpoint_value=candidate_midpoint_value,
            premium_kind="debit",
            min_retention_pct=min_retention_pct,
        )
        live_quote.update(
            {
                "candidate_debit": candidate_midpoint_value,
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
        return option_limit_price(explicit_value) or 0.01

    midpoint_value, natural_value = _resolve_candidate_entry_prices(candidate_payload)
    if midpoint_value is None:
        order_payload = dict(candidate_payload.get("order_payload") or {})
        midpoint_value = _normalize_limit_value(order_payload.get("limit_price"))
    if midpoint_value is None or midpoint_value <= 0:
        raise ValueError("Execution limit price must be positive")

    pricing_mode = str(execution_policy.get("pricing_mode") or DEFAULT_ENTRY_PRICING_MODE)
    if pricing_mode == "midpoint" or natural_value is None or natural_value <= 0:
        return option_limit_price(midpoint_value) or 0.01

    fill_ratio = clamp(coerce_float(candidate_payload.get("fill_ratio")) or 0.0, high=1.0)
    min_credit_retention_pct = clamp(
        coerce_float(execution_policy.get("min_credit_retention_pct")) or DEFAULT_MIN_CREDIT_RETENTION_PCT,
        low=0.5,
        high=1.0,
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
        return option_limit_price(
            min(
                max(midpoint_value + concession, 0.01),
                max(natural_value, 0.01),
                debit_ceiling,
            )
        ) or 0.01

    credit_floor = max(natural_value, midpoint_value * min_credit_retention_pct, 0.01)
    max_concession_to_floor = max(midpoint_value - credit_floor, 0.0)
    fill_ratio_concession = max(midpoint_value - natural_value, 0.0) * max(1.0 - fill_ratio, 0.0)
    concession = min(fill_ratio_concession, max_credit_concession, max_concession_to_floor)
    return option_limit_price(max(midpoint_value - concession, credit_floor, 0.01)) or 0.01


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
    order_limit_price = option_limit_price(resolved_limit_price)
    if order_limit_price is None:
        raise ValueError("Close execution requires a positive limit price or a quoted close mark")

    strategy_family = _strategy_family_from_payload(position)
    resolved_legs = normalize_legs(position.get("legs"))
    if not resolved_legs:
        resolved_legs = candidate_legs(position)
    if not resolved_legs:
        raise ValueError("Close execution requires canonical position legs")
    request = build_order_payload(
        legs=closing_legs(resolved_legs),
        limit_price=order_limit_price,
        strategy_family=strategy_family,
        trade_intent=CLOSE_TRADE_INTENT,
        quantity=resolved_quantity,
    )
    request["client_order_id"] = client_order_id
    return request, int(resolved_quantity), order_limit_price


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
