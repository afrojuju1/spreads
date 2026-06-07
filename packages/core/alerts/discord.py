from __future__ import annotations

from typing import Any

from core.integrations.http_client import VendorHttpClient, VendorHttpError
from core.services.option_structures import (
    candidate_legs,
    net_premium_kind,
    normalize_strategy_family,
    payload_display_fields,
)

SUCCESS_GREEN = 0x2ECC71
BEARISH_RED = 0xE74C3C
NEUTRAL_YELLOW = 0xF1C40F
INFO_BLUE = 0x3498DB
DISCORD_HTTP = VendorHttpClient(timeout_seconds=10, user_agent="spreads-alerts/1.0")
_RUNTIME_READY_ALERTS = frozenset({"runtime_entry_selected"})
_NON_SIGNAL_STATUS_VALUES = frozenset({"", "clean", "none", "n/a", "unknown"})
_ALERT_STATUS_TITLES = {
    "runtime_entry_selected": "ENTRY READY",
    "new_promotable_idea": "NEW IDEA",
    "monitor_promoted": "PROMOTED",
    "side_flip": "SIDE FLIP",
    "promotable_replaced": "REPLACED",
    "score_breakout": "SCORE BREAKOUT",
}


def strategy_color(strategy: str, *, alert_type: str) -> int:
    if alert_type == "side_flip":
        return NEUTRAL_YELLOW
    if alert_type == "score_breakout":
        return INFO_BLUE
    normalized = normalize_strategy_family(strategy)
    if normalized in {
        "put_credit_spread",
        "call_debit_spread",
        "long_call",
        "short_put",
    }:
        return SUCCESS_GREEN
    if normalized in {
        "call_credit_spread",
        "put_debit_spread",
        "long_put",
        "short_call",
    }:
        return BEARISH_RED
    return NEUTRAL_YELLOW


def compact_value(value: Any, *, fallback: str = "n/a") -> str:
    if value is None:
        return fallback
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def compact_money(value: Any, *, fallback: str = "n/a") -> str:
    if value is None:
        return fallback
    rendered = float(value)
    if abs(rendered) >= 100:
        return f"${rendered:,.0f}"
    return f"${rendered:,.2f}"


def compact_pct(value: Any, *, fallback: str = "n/a") -> str:
    if value is None:
        return fallback
    return f"{float(value) * 100:.1f}%"


def compact_strike(value: Any, *, fallback: str = "n/a") -> str:
    if value is None:
        return fallback
    rendered = float(value)
    if rendered.is_integer():
        return f"{rendered:.0f}"
    return f"{rendered:.2f}"


def compact_dte(value: Any, *, fallback: str = "n/a") -> str:
    if value is None:
        return fallback
    return f"{int(value)}DTE"


def compact_count(value: Any, *, fallback: str = "n/a") -> str:
    if value is None:
        return fallback
    rendered = float(value)
    absolute = abs(rendered)
    if absolute >= 1_000_000:
        return f"{rendered / 1_000_000:.1f}m"
    if absolute >= 10_000:
        return f"{rendered / 1_000:.0f}k"
    if absolute >= 1_000:
        return f"{rendered / 1_000:.1f}k"
    return f"{int(rendered)}" if rendered.is_integer() else f"{rendered:.1f}"


def compact_ratio(value: Any, *, fallback: str = "n/a") -> str:
    if value is None:
        return fallback
    return f"{float(value):.2f}x"


def compact_number(
    value: Any,
    *,
    places: int = 1,
    fallback: str = "n/a",
) -> str:
    if value is None:
        return fallback
    rendered = float(value)
    if rendered.is_integer() and places <= 0:
        return f"{int(rendered)}"
    return f"{rendered:.{places}f}"


def compact_integer(value: Any, *, fallback: str = "n/a") -> str:
    if value is None:
        return fallback
    return f"{int(round(float(value))):,}"


def compact_signed_money(value: Any, *, fallback: str = "n/a") -> str:
    if value is None:
        return fallback
    rendered = float(value)
    return f"{rendered:+.2f}"


def _normalized_reason_text(value: Any) -> str | None:
    rendered = str(value or "").strip().lower()
    if not rendered:
        return None
    return rendered.replace("_", "-")


def _expiration_text(candidate: dict[str, Any]) -> str:
    expiration_date = str(candidate.get("expiration_date") or "").strip()
    dte_text = compact_dte(candidate.get("days_to_expiration"))
    if expiration_date and dte_text != "n/a":
        return f"{expiration_date} ({dte_text})"
    return expiration_date or dte_text


def _strategy_title(strategy: Any) -> str:
    normalized = normalize_strategy_family(strategy)
    return normalized.replace("_", " ").upper()


def _alert_status_title(alert_type: Any) -> str:
    normalized = str(alert_type or "").strip().lower()
    if normalized in _ALERT_STATUS_TITLES:
        return _ALERT_STATUS_TITLES[normalized]
    return normalized.replace("_", " ").upper() or "ALERT"


def _metric_part(label: str, value: str | None) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    if not rendered or rendered == "n/a":
        return None
    return f"{label} {rendered}"


def _join_metric_parts(parts: list[str | None], *, fallback: str = "n/a") -> str:
    resolved = [part for part in parts if part]
    return " | ".join(resolved) if resolved else fallback


def _alert_footer_text(alert: dict[str, Any]) -> str:
    deploy_env = str(alert.get("deploy_env") or "").strip()
    parts = []
    if deploy_env:
        parts.append(f"env {deploy_env}")
    parts.extend(
        [
            str(alert.get("label") or "n/a"),
            str(alert.get("profile") or "n/a"),
            str(alert.get("strategy_mode") or "n/a"),
        ]
    )
    return " | ".join(parts)


def _leg_token(leg: dict[str, Any]) -> str:
    strike_text = compact_strike(leg.get("strike"))
    option_type = str(leg.get("option_type") or "").strip().lower()
    suffix = {"call": "C", "put": "P"}.get(option_type, "")
    if strike_text != "n/a":
        return f"{strike_text}{suffix}"
    return str(leg.get("symbol") or "n/a")


def _leg_qty_text(leg: dict[str, Any]) -> str:
    raw_value = leg.get("ratio_qty")
    if raw_value in (None, ""):
        return "1x"
    rendered = float(raw_value)
    return f"{int(rendered)}x" if rendered.is_integer() else f"{rendered:g}x"


def _leg_intent_abbrev(leg: dict[str, Any]) -> str:
    intent = str(leg.get("position_intent") or "").strip().lower()
    if intent == "sell_to_open":
        return "STO"
    if intent == "buy_to_open":
        return "BTO"
    if intent == "buy_to_close":
        return "BTC"
    if intent == "sell_to_close":
        return "STC"
    role = str(leg.get("role") or "").strip().lower()
    if role == "short":
        return "STO"
    if role == "long":
        return "BTO"
    return "LEG"


def _structure_summary(candidate: dict[str, Any]) -> str:
    strategy = normalize_strategy_family(candidate.get("strategy"))
    legs = candidate_legs(candidate)
    if not legs:
        return str(payload_display_fields(candidate).get("strike_path") or "n/a")
    if strategy == "iron_condor":
        put_long = put_short = call_short = call_long = None
        for leg in legs:
            option_type = str(leg.get("option_type") or "").strip().lower()
            role = str(leg.get("role") or "").strip().lower()
            if option_type == "put" and role == "long" and put_long is None:
                put_long = leg
            elif option_type == "put" and role == "short" and put_short is None:
                put_short = leg
            elif option_type == "call" and role == "short" and call_short is None:
                call_short = leg
            elif option_type == "call" and role == "long" and call_long is None:
                call_long = leg
        if None not in (put_long, put_short, call_short, call_long):
            return f"{_leg_token(put_long)} / {_leg_token(put_short)} + " f"{_leg_token(call_short)} / {_leg_token(call_long)}"
    return " / ".join(f"{str(leg.get('role') or 'leg').lower()} {_leg_qty_text(leg)} {_leg_token(leg)}" for leg in legs[:4])


def _oi_floor(candidate: dict[str, Any]) -> str | None:
    short_oi = candidate.get("short_open_interest")
    long_oi = candidate.get("long_open_interest")
    values = [int(round(float(value))) for value in (short_oi, long_oi) if value not in (None, "")]
    if not values:
        return None
    return compact_integer(min(values))


def _selection_score(candidate: dict[str, Any]) -> str | None:
    if candidate.get("quality_score") is None:
        return None
    return compact_number(candidate.get("quality_score"), places=1)


def _delta_text(candidate: dict[str, Any]) -> str | None:
    delta = candidate.get("short_delta")
    if delta is None:
        return None
    return compact_number(abs(float(delta)), places=2)


def _underlying_spot_text(candidate: dict[str, Any]) -> str | None:
    if candidate.get("underlying_price") is None:
        return None
    return f"${float(candidate['underlying_price']):,.2f}"


def _order_price_line(candidate: dict[str, Any]) -> str | None:
    premium_kind = net_premium_kind(candidate.get("strategy"))
    midpoint = candidate.get("midpoint_credit")
    if midpoint is None:
        return None
    if premium_kind == "credit":
        return f"LIMIT CREDIT {compact_money(midpoint)}"
    if premium_kind == "debit":
        return f"LIMIT DEBIT {compact_money(midpoint)}"
    return f"LIMIT {compact_money(midpoint)}"


def _ticket_lines(candidate: dict[str, Any]) -> list[str]:
    underlying = str(candidate.get("underlying_symbol") or "UNKNOWN")
    expiration = str(candidate.get("expiration_date") or "").strip()
    lines: list[str] = []
    for leg in candidate_legs(candidate):
        line = f"{_leg_intent_abbrev(leg)} {_leg_qty_text(leg)} {underlying} {_leg_token(leg)}"
        if expiration:
            line += f" exp {expiration}"
        lines.append(line)
    price_line = _order_price_line(candidate)
    if price_line:
        lines.append(price_line)
    return lines


def _contract_lines(candidate: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for leg in candidate_legs(candidate):
        lines.append(f"{_leg_intent_abbrev(leg)} {_leg_qty_text(leg)} {str(leg.get('symbol') or 'n/a')}")
    return lines


def _thesis_parts(candidate: dict[str, Any]) -> list[str]:
    parts: list[str] = []
    for note in list(candidate.get("selection_notes") or candidate.get("board_notes") or []):
        rendered = str(note).strip().replace("_", "-")
        if rendered and rendered not in parts:
            parts.append(rendered)
    for status_key in ("setup_status", "calendar_status", "data_status"):
        rendered = str(candidate.get(status_key) or "").strip().lower()
        if rendered in _NON_SIGNAL_STATUS_VALUES:
            continue
        normalized = rendered.replace("_", "-")
        if normalized not in parts:
            parts.append(normalized)
    return parts


def _runtime_description(alert: dict[str, Any]) -> str:
    details = alert.get("details") if isinstance(alert.get("details"), dict) else {}
    parts = [_expiration_text(alert["candidate"]), "selected for entry"]
    execution_mode = str(details.get("execution_mode") or "").strip()
    approval_mode = str(details.get("approval_mode") or "").strip()
    if execution_mode:
        parts.append(execution_mode)
    if approval_mode:
        parts.append(approval_mode)
    execution_admission = _execution_admission(alert)
    if execution_admission is not None:
        admission_status = str(execution_admission.get("status") or "").strip().lower()
        admissible_quantity = execution_admission.get("admissible_quantity")
        if admission_status == "admissible" and admissible_quantity not in (None, ""):
            parts.append(f"acct qty {compact_integer(admissible_quantity)}")
        elif admission_status:
            parts.append(f"acct {admission_status}")
    return " | ".join(parts)


def _spread_description(alert: dict[str, Any]) -> str:
    alert_type = str(alert.get("alert_type") or "").strip().lower()
    if alert_type in _RUNTIME_READY_ALERTS:
        return _runtime_description(alert)
    return str(alert.get("description") or "").strip()


def _execution_admission(alert: dict[str, Any]) -> dict[str, Any] | None:
    raw = alert.get("execution_admission")
    if isinstance(raw, dict):
        return dict(raw)
    details = alert.get("details") if isinstance(alert.get("details"), dict) else {}
    status = str(details.get("execution_admission_status") or "").strip()
    reason = str(details.get("execution_admission_reason") or "").strip()
    if not status and not reason:
        return None
    return {
        "status": status or None,
        "reason": reason or None,
    }


def _execution_field(alert: dict[str, Any]) -> dict[str, Any] | None:
    execution_admission = _execution_admission(alert)
    if execution_admission is None:
        return None
    status = str(execution_admission.get("status") or "").strip().lower()
    parts = [
        _metric_part("status", status or None),
        _metric_part(
            "qty",
            compact_integer(execution_admission.get("admissible_quantity")),
        ),
        _metric_part(
            "req",
            compact_money(execution_admission.get("required_buying_power")),
        ),
        _metric_part(
            "avail",
            compact_money(execution_admission.get("available_buying_power")),
        ),
    ]
    reserved_buying_power = execution_admission.get("reserved_buying_power")
    if reserved_buying_power not in (None, "", 0, 0.0):
        parts.append(_metric_part("reserved", compact_money(reserved_buying_power)))
    reason_text = _normalized_reason_text(execution_admission.get("reason"))
    if reason_text is not None and status != "admissible":
        parts.append(_metric_part("why", reason_text))
    return {
        "name": "Execution",
        "value": _join_metric_parts(parts),
        "inline": False,
    }


def _single_leg_sections(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    legs = candidate_legs(candidate)
    risk_parts = [
        _metric_part("strike", _leg_token(legs[0])) if legs else _metric_part("strike", compact_strike(candidate.get("short_strike"))),
        _metric_part("BE", compact_strike(candidate.get("breakeven"))),
        _metric_part("EM", compact_money(candidate.get("expected_move"))),
        _metric_part("BE/EM", compact_signed_money(candidate.get("breakeven_vs_expected_move"))),
    ]
    liquidity_parts = [
        _metric_part("OI", _oi_floor(candidate)),
        _metric_part("size", compact_integer(candidate.get("min_quote_size"))),
        _metric_part("delta", _delta_text(candidate)),
        _metric_part("sel", _selection_score(candidate)),
    ]
    fields = [
        {"name": "Ticket", "value": "\n".join(_ticket_lines(candidate)) or "n/a", "inline": False},
        {
            "name": "Contracts",
            "value": "\n".join(_contract_lines(candidate)) or "n/a",
            "inline": False,
        },
        {
            "name": "Edge",
            "value": _join_metric_parts(
                [
                    _metric_part("POP", compact_pct(candidate.get("probability_of_profit"))),
                    _metric_part("credit", compact_money(candidate.get("midpoint_credit"))),
                    _metric_part("fill", compact_pct(candidate.get("fill_ratio"))),
                    _metric_part("RoR", compact_pct(candidate.get("return_on_risk"))),
                ]
            ),
            "inline": False,
        },
        {"name": "Risk", "value": _join_metric_parts(risk_parts), "inline": False},
        {
            "name": "Liquidity",
            "value": _join_metric_parts([_metric_part("spot", _underlying_spot_text(candidate)), *liquidity_parts]),
            "inline": False,
        },
    ]
    thesis_parts = _thesis_parts(candidate)
    if thesis_parts:
        fields.append({"name": "Thesis", "value": " | ".join(thesis_parts[:4]), "inline": False})
    return fields


def _vertical_sections(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    premium_label = "credit" if net_premium_kind(candidate.get("strategy")) == "credit" else "debit"
    risk_parts = [
        _metric_part("width", compact_strike(candidate.get("width"))),
        _metric_part("max loss", compact_money(candidate.get("max_loss"))),
        _metric_part("BE", compact_strike(candidate.get("breakeven"))),
        _metric_part("EM", compact_money(candidate.get("expected_move"))),
    ]
    liquidity_parts = [
        _metric_part("OI", _oi_floor(candidate)),
        _metric_part("size", compact_integer(candidate.get("min_quote_size"))),
        _metric_part("delta", _delta_text(candidate)),
        _metric_part("sel", _selection_score(candidate)),
    ]
    fields = [
        {"name": "Ticket", "value": "\n".join(_ticket_lines(candidate)) or "n/a", "inline": False},
        {
            "name": "Contracts",
            "value": "\n".join(_contract_lines(candidate)) or "n/a",
            "inline": False,
        },
        {
            "name": "Edge",
            "value": _join_metric_parts(
                [
                    _metric_part("POP", compact_pct(candidate.get("probability_of_profit"))),
                    _metric_part(premium_label, compact_money(candidate.get("midpoint_credit"))),
                    _metric_part("fill", compact_pct(candidate.get("fill_ratio"))),
                    _metric_part("RoR", compact_pct(candidate.get("return_on_risk"))),
                ]
            ),
            "inline": False,
        },
        {"name": "Risk", "value": _join_metric_parts(risk_parts), "inline": False},
        {
            "name": "Liquidity",
            "value": _join_metric_parts([_metric_part("spot", _underlying_spot_text(candidate)), *liquidity_parts]),
            "inline": False,
        },
    ]
    thesis_parts = _thesis_parts(candidate)
    if thesis_parts:
        fields.append({"name": "Thesis", "value": " | ".join(thesis_parts[:4]), "inline": False})
    return fields


def _iron_condor_sections(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    risk_parts = [
        _metric_part("width", compact_strike(candidate.get("width"))),
        _metric_part("max loss", compact_money(candidate.get("max_loss"))),
        _metric_part(
            "BE",
            (
                (f"{compact_strike(candidate.get('lower_breakeven'))} - " f"{compact_strike(candidate.get('upper_breakeven'))}")
                if candidate.get("lower_breakeven") is not None and candidate.get("upper_breakeven") is not None
                else None
            ),
        ),
        _metric_part("EM", compact_money(candidate.get("expected_move"))),
    ]
    positioning_parts = [
        _metric_part("short/EM", compact_signed_money(candidate.get("short_vs_expected_move"))),
        _metric_part("BE/EM", compact_signed_money(candidate.get("breakeven_vs_expected_move"))),
        _metric_part("balance", compact_pct(candidate.get("side_balance_score"))),
        _metric_part("sym", compact_ratio(candidate.get("wing_symmetry_ratio"))),
    ]
    liquidity_parts = [
        _metric_part("OI", _oi_floor(candidate)),
        _metric_part("size", compact_integer(candidate.get("min_quote_size"))),
        _metric_part("legs", compact_integer(len(candidate_legs(candidate)))),
        _metric_part("sel", _selection_score(candidate)),
    ]
    fields = [
        {"name": "Ticket", "value": "\n".join(_ticket_lines(candidate)) or "n/a", "inline": False},
        {
            "name": "Contracts",
            "value": "\n".join(_contract_lines(candidate)) or "n/a",
            "inline": False,
        },
        {
            "name": "Edge",
            "value": _join_metric_parts(
                [
                    _metric_part("POP", compact_pct(candidate.get("probability_of_profit"))),
                    _metric_part("credit", compact_money(candidate.get("midpoint_credit"))),
                    _metric_part("fill", compact_pct(candidate.get("fill_ratio"))),
                    _metric_part("RoR", compact_pct(candidate.get("return_on_risk"))),
                ]
            ),
            "inline": False,
        },
        {"name": "Risk", "value": _join_metric_parts(risk_parts), "inline": False},
        {
            "name": "Positioning",
            "value": _join_metric_parts(positioning_parts),
            "inline": False,
        },
        {
            "name": "Liquidity",
            "value": _join_metric_parts([_metric_part("spot", _underlying_spot_text(candidate)), *liquidity_parts]),
            "inline": False,
        },
    ]
    thesis_parts = _thesis_parts(candidate)
    if thesis_parts:
        fields.append({"name": "Thesis", "value": " | ".join(thesis_parts[:4]), "inline": False})
    return fields


def _generic_spread_sections(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    premium_kind = net_premium_kind(candidate.get("strategy"))
    premium_label = "credit" if premium_kind == "credit" else "debit" if premium_kind == "debit" else "entry"
    fields = [
        {"name": "Ticket", "value": "\n".join(_ticket_lines(candidate)) or "n/a", "inline": False},
        {
            "name": "Contracts",
            "value": "\n".join(_contract_lines(candidate)) or "n/a",
            "inline": False,
        },
        {
            "name": "Edge",
            "value": _join_metric_parts(
                [
                    _metric_part("POP", compact_pct(candidate.get("probability_of_profit"))),
                    _metric_part(premium_label, compact_money(candidate.get("midpoint_credit"))),
                    _metric_part("fill", compact_pct(candidate.get("fill_ratio"))),
                    _metric_part("RoR", compact_pct(candidate.get("return_on_risk"))),
                ]
            ),
            "inline": False,
        },
        {
            "name": "Risk",
            "value": _join_metric_parts(
                [
                    _metric_part("BE", compact_strike(candidate.get("breakeven"))),
                    _metric_part("max loss", compact_money(candidate.get("max_loss"))),
                    _metric_part("EM", compact_money(candidate.get("expected_move"))),
                    _metric_part("sel", _selection_score(candidate)),
                ]
            ),
            "inline": False,
        },
        {
            "name": "Structure",
            "value": _join_metric_parts(
                [
                    _metric_part("exp", _expiration_text(candidate)),
                    _metric_part("spot", _underlying_spot_text(candidate)),
                    _metric_part("shape", _structure_summary(candidate)),
                ]
            ),
            "inline": False,
        },
    ]
    thesis_parts = _thesis_parts(candidate)
    if thesis_parts:
        fields.append({"name": "Thesis", "value": " | ".join(thesis_parts[:4]), "inline": False})
    return fields


def _spread_fields(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    strategy = normalize_strategy_family(candidate.get("strategy"))
    if strategy in {"short_put", "short_call", "long_call", "long_put"}:
        return _single_leg_sections(candidate)
    if strategy in {
        "call_credit_spread",
        "put_credit_spread",
        "call_debit_spread",
        "put_debit_spread",
    }:
        return _vertical_sections(candidate)
    if strategy == "iron_condor":
        return _iron_condor_sections(candidate)
    return _generic_spread_sections(candidate)


def _spread_expected_move_line(candidate: dict[str, Any]) -> str | None:
    expected_move = candidate.get("expected_move")
    if expected_move is None:
        return None
    parts = [
        f"EM {compact_money(expected_move)}",
    ]
    if candidate.get("expected_move_pct") is not None:
        parts.append(compact_pct(candidate.get("expected_move_pct")))
    if candidate.get("short_vs_expected_move") is not None:
        parts.append(f"strike {compact_signed_money(candidate.get('short_vs_expected_move'))}")
    if candidate.get("breakeven_vs_expected_move") is not None:
        parts.append(f"BE {compact_signed_money(candidate.get('breakeven_vs_expected_move'))}")
    return " | ".join(parts)


def _build_spread_discord_payload(alert: dict[str, Any]) -> dict[str, Any]:
    candidate = alert["candidate"]
    strategy = str(candidate["strategy"])
    title = f"{alert['symbol']} {_expiration_text(candidate)} " f"{_strategy_title(strategy)} | {_alert_status_title(alert.get('alert_type'))}"
    description = _spread_description(alert)
    fields = _spread_fields(candidate)
    if str(alert.get("alert_type") or "").strip().lower() in _RUNTIME_READY_ALERTS:
        execution_field = _execution_field(alert)
        if execution_field is not None:
            insert_index = min(2, len(fields))
            fields.insert(insert_index, execution_field)

    embed = {
        "title": title,
        "description": description,
        "color": strategy_color(strategy, alert_type=alert["alert_type"]),
        "fields": fields,
        "footer": {"text": _alert_footer_text(alert)},
        "timestamp": alert["created_at"],
    }
    return {"embeds": [embed]}


def _build_ops_discord_payload(alert: dict[str, Any]) -> dict[str, Any]:
    details = alert.get("details") if isinstance(alert.get("details"), dict) else {}
    ops_scope = str(
        details.get("trading_strategy_id")
        or details.get("strategy_id")
        or details.get("job_key")
        or details.get("scope")
        or alert.get("symbol")
        or "ops"
    )
    fields = [
        {"name": "Scope", "value": ops_scope, "inline": False},
        {
            "name": "Market Date",
            "value": str(details.get("market_date") or alert.get("session_date") or "n/a"),
            "inline": True,
        },
        {
            "name": "Selected",
            "value": compact_count(details.get("selected_count")),
            "inline": True,
        },
        {
            "name": "Intents",
            "value": compact_count(details.get("intent_count")),
            "inline": True,
        },
        {
            "name": "Submitted",
            "value": compact_count(details.get("submitted_count")),
            "inline": True,
        },
        {
            "name": "Aged Out",
            "value": compact_count(details.get("dispatch_window_elapsed_count")),
            "inline": True,
        },
        {
            "name": "Pending Gap",
            "value": compact_count(details.get("pending_submission_gap_count")),
            "inline": True,
        },
    ]
    embed = {
        "title": f"Ops Alert | {ops_scope}",
        "description": alert["description"],
        "color": NEUTRAL_YELLOW,
        "fields": fields,
        "footer": {"text": _alert_footer_text(alert)},
        "timestamp": alert["created_at"],
    }
    return {"embeds": [embed]}


def _discord_value(value: Any, *, fallback: str = "n/a", limit: int = 1000) -> str:
    rendered = str(value if value not in (None, "") else fallback).strip() or fallback
    if len(rendered) <= limit:
        return rendered
    return rendered[: limit - 3] + "..."


def _compact_percent_points(value: Any, *, fallback: str = "n/a") -> str:
    if value is None:
        return fallback
    return f"{float(value):+.2f}%"


def _research_color(alert: dict[str, Any], details: dict[str, Any]) -> int:
    if str(alert.get("alert_type") or "") == "research_tradingagents_batch_summary":
        incomplete_count = int(details.get("failed_count") or 0) + int(details.get("timed_out_count") or 0)
        return NEUTRAL_YELLOW if incomplete_count else INFO_BLUE
    tradingagents = details.get("tradingagents") if isinstance(details.get("tradingagents"), dict) else {}
    signal = str(tradingagents.get("validated_signal") or "").strip().lower()
    quality_status = str(tradingagents.get("quality_status") or "").strip().lower()
    if quality_status == "fail":
        return NEUTRAL_YELLOW
    if signal in {"buy", "overweight"}:
        return SUCCESS_GREEN
    if signal in {"sell", "underweight"}:
        return BEARISH_RED
    return INFO_BLUE


def _research_finviz_field(source_entry: dict[str, Any]) -> str:
    parts = [
        _metric_part("score", compact_value(source_entry.get("score"))),
        _metric_part("price", compact_money(source_entry.get("price"))),
        _metric_part("vol", compact_count(source_entry.get("daily_volume"))),
        _metric_part(
            "move",
            _compact_percent_points(source_entry.get("move_percent")),
        ),
        _metric_part("relvol", compact_ratio(source_entry.get("relative_volume"))),
    ]
    return _join_metric_parts(parts)


def _build_research_actionable_payload(
    alert: dict[str, Any],
    details: dict[str, Any],
) -> dict[str, Any]:
    tradingagents = details.get("tradingagents") if isinstance(details.get("tradingagents"), dict) else {}
    source_entry = details.get("source_entry") if isinstance(details.get("source_entry"), dict) else {}
    symbol = str(alert.get("symbol") or tradingagents.get("ticker") or "n/a")
    signal = str(tradingagents.get("validated_signal") or "n/a")
    quality_status = str(tradingagents.get("quality_status") or "n/a")
    fields = [
        {
            "name": "Signal",
            "value": _discord_value(
                _join_metric_parts(
                    [
                        _metric_part("validated", signal),
                        _metric_part(
                            "raw",
                            compact_value(tradingagents.get("raw_signal")),
                        ),
                        _metric_part("quality", quality_status),
                    ]
                )
            ),
            "inline": False,
        },
        {
            "name": "Finviz",
            "value": _discord_value(_research_finviz_field(source_entry)),
            "inline": False,
        },
        {
            "name": "Runtime",
            "value": _discord_value(
                _join_metric_parts(
                    [
                        _metric_part(
                            "profile",
                            compact_value(tradingagents.get("run_profile")),
                        ),
                        _metric_part(
                            "wall",
                            compact_value(tradingagents.get("wall_seconds")),
                        ),
                        _metric_part(
                            "elapsed",
                            compact_value(tradingagents.get("elapsed_seconds")),
                        ),
                    ]
                )
            ),
            "inline": False,
        },
    ]
    blocked_reason = str(tradingagents.get("blocked_reason") or "").strip()
    if blocked_reason:
        fields.append(
            {
                "name": "Quality Note",
                "value": _discord_value(blocked_reason),
                "inline": False,
            }
        )
    report_path = str(tradingagents.get("report_path") or "").strip()
    if report_path:
        fields.append(
            {
                "name": "Report",
                "value": _discord_value(report_path),
                "inline": False,
            }
        )
    embed = {
        "title": f"{symbol} | TradingAgents {signal} | quality {quality_status}",
        "description": str(alert.get("description") or ""),
        "color": _research_color(alert, details),
        "fields": fields,
        "footer": {"text": _alert_footer_text(alert)},
        "timestamp": alert["created_at"],
    }
    return {"embeds": [embed]}


def _build_research_summary_payload(
    alert: dict[str, Any],
    details: dict[str, Any],
) -> dict[str, Any]:
    ticker_results = [item for item in list(details.get("ticker_results") or []) if isinstance(item, dict)]
    result_lines = []
    for item in ticker_results[:8]:
        result_lines.append(
            " | ".join(
                [
                    str(item.get("ticker") or "n/a"),
                    str(item.get("validated_signal") or "no-signal"),
                    str(item.get("quality_status") or item.get("status") or "n/a"),
                    "actionable" if item.get("actionable") else "watch",
                ]
            )
        )
    fields = [
        {
            "name": "Candidates",
            "value": compact_count(details.get("candidate_count")),
            "inline": True,
        },
        {
            "name": "Completed",
            "value": compact_count(details.get("completed_count")),
            "inline": True,
        },
        {
            "name": "Actionable",
            "value": compact_count(details.get("actionable_count")),
            "inline": True,
        },
        {
            "name": "Tickers",
            "value": _discord_value(", ".join(list(details.get("selected_tickers") or []))),
            "inline": False,
        },
    ]
    if result_lines:
        fields.append(
            {
                "name": "Results",
                "value": _discord_value("\n".join(result_lines)),
                "inline": False,
            }
        )
    embed = {
        "title": "Finviz TradingAgents Batch",
        "description": str(alert.get("description") or ""),
        "color": _research_color(alert, details),
        "fields": fields,
        "footer": {"text": _alert_footer_text(alert)},
        "timestamp": alert["created_at"],
    }
    return {"embeds": [embed]}


def _build_research_discord_payload(alert: dict[str, Any]) -> dict[str, Any]:
    details = alert.get("details") if isinstance(alert.get("details"), dict) else {}
    if str(alert.get("alert_type") or "") == "research_tradingagents_batch_summary":
        return _build_research_summary_payload(alert, details)
    return _build_research_actionable_payload(alert, details)


def build_discord_payload(alert: dict[str, Any]) -> dict[str, Any]:
    if str(alert.get("alert_type") or "").startswith("ops_"):
        return _build_ops_discord_payload(alert)
    if str(alert.get("alert_type") or "").startswith("research_"):
        return _build_research_discord_payload(alert)
    return _build_spread_discord_payload(alert)


def send_discord_webhook(webhook_url: str, payload: dict[str, Any]) -> dict[str, Any]:
    try:
        response = DISCORD_HTTP.request(
            "POST",
            webhook_url,
            "",
            body=payload,
            headers={
                "Accept": "application/json",
            },
        )
        return {
            "status_code": response.status_code,
            "body": response.text[:1000],
        }
    except VendorHttpError as exc:
        raise RuntimeError(f"Discord webhook error {exc.status_code or 'transport'}: {(exc.response_body or str(exc))[:500]}") from exc
