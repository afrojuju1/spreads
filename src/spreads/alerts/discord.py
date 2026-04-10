from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any


SUCCESS_GREEN = 0x2ECC71
BEARISH_RED = 0xE74C3C
NEUTRAL_YELLOW = 0xF1C40F
INFO_BLUE = 0x3498DB


def strategy_color(strategy: str, *, alert_type: str) -> int:
    if alert_type == "side_flip":
        return NEUTRAL_YELLOW
    if alert_type == "score_breakout":
        return INFO_BLUE
    return SUCCESS_GREEN if strategy == "put_credit" else BEARISH_RED


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


def compact_signed_money(value: Any, *, fallback: str = "n/a") -> str:
    if value is None:
        return fallback
    rendered = float(value)
    return f"{rendered:+.2f}"


def _spread_leg_line(candidate: dict[str, Any], *, leg: str) -> str:
    prefix = "short" if leg == "short" else "long"
    strike = compact_strike(candidate.get(f"{prefix}_strike"))
    option_type = "C" if str(candidate.get("strategy") or "") == "call_credit" else "P"
    delta = candidate.get(f"{prefix}_delta")
    delta_text = "n/a" if delta is None else f"{abs(float(delta)):.2f}Δ"
    open_interest = int(candidate.get(f"{prefix}_open_interest") or 0)
    relative_spread = compact_pct(candidate.get(f"{prefix}_relative_spread"))
    midpoint = compact_money(candidate.get(f"{prefix}_midpoint"))
    return f"{strike}{option_type} {delta_text} oi {open_interest} mid {midpoint} spr {relative_spread}"


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
        parts.append(f"short {compact_signed_money(candidate.get('short_vs_expected_move'))}")
    if candidate.get("breakeven_vs_expected_move") is not None:
        parts.append(f"BE {compact_signed_money(candidate.get('breakeven_vs_expected_move'))}")
    return " | ".join(parts)


def _build_spread_discord_payload(alert: dict[str, Any]) -> dict[str, Any]:
    candidate = alert["candidate"]
    strategy = str(candidate["strategy"])
    setup_status = str(candidate.get("setup_status") or "unknown")
    calendar_status = str(candidate.get("calendar_status") or "unknown")
    data_status = str(candidate.get("data_status") or "unknown")
    title = (
        f"{alert['symbol']} {compact_dte(candidate.get('days_to_expiration'))} "
        f"{strategy.replace('_', ' ').title()}"
    )
    description = alert["description"]
    fields = [
        {"name": "Strikes", "value": f"{candidate['short_strike']:.2f} / {candidate['long_strike']:.2f}", "inline": True},
        {"name": "DTE", "value": compact_dte(candidate.get("days_to_expiration")), "inline": True},
        {"name": "Score", "value": f"{candidate['quality_score']:.1f}", "inline": True},
        {"name": "Credit", "value": compact_money(candidate.get("midpoint_credit")), "inline": True},
        {"name": "RoR", "value": compact_pct(candidate.get("return_on_risk")), "inline": True},
        {"name": "Breakeven", "value": compact_strike(candidate.get("breakeven")), "inline": True},
        {"name": "OI Floor", "value": str(min(int(candidate.get("short_open_interest") or 0), int(candidate.get("long_open_interest") or 0))), "inline": True},
        {"name": "Fill", "value": compact_pct(candidate.get("fill_ratio")), "inline": True},
        {"name": "Min Size", "value": str(int(candidate.get("min_quote_size") or 0)), "inline": True},
        {"name": "Statuses", "value": f"{setup_status} | {calendar_status} | {data_status}", "inline": False},
        {"name": "Short Leg", "value": _spread_leg_line(candidate, leg='short'), "inline": False},
        {"name": "Long Leg", "value": _spread_leg_line(candidate, leg='long'), "inline": False},
    ]
    expected_move_line = _spread_expected_move_line(candidate)
    if expected_move_line is not None:
        fields.append({"name": "Expected Move", "value": expected_move_line, "inline": False})
    board_notes = candidate.get("board_notes") or []
    if board_notes:
        fields.append({"name": "Why Now", "value": ", ".join(str(note) for note in board_notes[:3]), "inline": False})
    fields.append({"name": "Event", "value": alert["alert_type"].replace("_", " "), "inline": True})

    embed = {
        "title": title,
        "description": description,
        "color": strategy_color(strategy, alert_type=alert["alert_type"]),
        "fields": fields,
        "footer": {"text": f"{alert['label']} | {alert['profile']} | {alert['strategy_mode']}"},
        "timestamp": alert["created_at"],
    }
    return {"embeds": [embed]}


def _uoa_color(*, dominant_flow: str, decision_state: str) -> int:
    if dominant_flow == "call":
        return SUCCESS_GREEN
    if dominant_flow == "put":
        return BEARISH_RED
    if decision_state == "high":
        return NEUTRAL_YELLOW
    return INFO_BLUE


def _uoa_dte_preview(contracts: list[dict[str, Any]]) -> str:
    values = sorted(
        {
            int(contract["dte"])
            for contract in contracts
            if contract.get("dte") is not None
        }
    )
    if not values:
        return "n/a"
    return ", ".join(f"{value}DTE" for value in values[:3])


def _uoa_contract_line(contract: dict[str, Any]) -> str:
    option_type = str(contract.get("option_type") or "?").upper()[:1]
    strike = contract.get("strike_price")
    strike_text = "?"
    if strike is not None:
        strike_text = f"{float(strike):.0f}" if float(strike).is_integer() else f"{float(strike):.2f}"
    dte = contract.get("dte")
    dte_text = "n/a" if dte is None else f"{int(dte)}DTE"
    flow_size = int(contract.get("scoreable_size") or 0)
    premium = compact_money(contract.get("scoreable_premium"))
    midpoint = compact_value(contract.get("midpoint"))
    spread_pct = compact_pct(contract.get("spread_pct"))
    session_volume = compact_count(contract.get("volume"))
    open_interest = compact_count(contract.get("open_interest"))
    volume_oi_ratio = compact_ratio(contract.get("volume_oi_ratio"))
    return (
        f"{strike_text}{option_type} {dte_text} flow {compact_count(flow_size)} prem {premium} "
        f"vol {session_volume} oi {open_interest} v/oi {volume_oi_ratio} mid {midpoint} spr {spread_pct}"
    )


def _build_uoa_discord_payload(alert: dict[str, Any]) -> dict[str, Any]:
    candidate = alert["candidate"]
    current = candidate.get("current") if isinstance(candidate.get("current"), dict) else {}
    quote_context = candidate.get("quote_context") if isinstance(candidate.get("quote_context"), dict) else {}
    deltas = candidate.get("deltas") if isinstance(candidate.get("deltas"), dict) else {}
    contracts = [dict(item) for item in (candidate.get("top_contracts") or []) if isinstance(item, dict)]
    decision_state = str(candidate.get("decision_state") or "none")
    dominant_flow = str(current.get("dominant_flow") or "mixed")
    title = f"{alert['symbol']} {dominant_flow.upper()} UOA {decision_state.upper()}"
    baseline_parts: list[str] = []
    max_premium_ratio = deltas.get("max_premium_rate_ratio")
    max_trade_ratio = deltas.get("max_trade_rate_ratio")
    if max_premium_ratio is not None:
        baseline_parts.append(f"prem {float(max_premium_ratio):.1f}x")
    if max_trade_ratio is not None:
        baseline_parts.append(f"trades {float(max_trade_ratio):.1f}x")
    quote_parts = [
        str(quote_context.get("quality_state") or "unknown"),
        f"{int(quote_context.get('fresh_contract_count') or 0)} fresh",
        f"{int(quote_context.get('liquid_contract_count') or 0)} liquid",
    ]
    fields = [
        {"name": "State", "value": decision_state, "inline": True},
        {"name": "Score", "value": f"{float(candidate.get('decision_score') or 0.0):.1f}", "inline": True},
        {"name": "DTE", "value": _uoa_dte_preview(contracts), "inline": True},
        {"name": "Premium", "value": compact_money(current.get("scoreable_premium")), "inline": True},
        {"name": "Flow Size", "value": compact_count(current.get("scoreable_size")), "inline": True},
        {"name": "Trades", "value": str(int(current.get("scoreable_trade_count") or 0)), "inline": True},
        {"name": "Contracts", "value": str(int(current.get("scoreable_contract_count") or 0)), "inline": True},
        {"name": "Session Vol", "value": compact_count(current.get("supporting_volume")), "inline": True},
        {"name": "Session OI", "value": compact_count(current.get("supporting_open_interest")), "inline": True},
        {"name": "Vol/OI", "value": compact_ratio(current.get("supporting_volume_oi_ratio")), "inline": True},
        {"name": "Quotes", "value": " | ".join(quote_parts), "inline": True},
    ]
    if baseline_parts:
        fields.append({"name": "Baselines", "value": " | ".join(baseline_parts), "inline": False})
    if contracts:
        fields.append(
            {
                "name": "Top Contracts",
                "value": "\n".join(_uoa_contract_line(contract) for contract in contracts[:3]),
                "inline": False,
            }
        )
    embed = {
        "title": title,
        "description": alert["description"],
        "color": _uoa_color(dominant_flow=dominant_flow, decision_state=decision_state),
        "fields": fields,
        "footer": {"text": f"{alert['label']} | {alert['profile']} | {alert['strategy_mode']}"},
        "timestamp": alert["created_at"],
    }
    return {"embeds": [embed]}


def build_discord_payload(alert: dict[str, Any]) -> dict[str, Any]:
    if str(alert.get("alert_type") or "").startswith("uoa_"):
        return _build_uoa_discord_payload(alert)
    return _build_spread_discord_payload(alert)


def send_discord_webhook(webhook_url: str, payload: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        webhook_url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "spreads-alerts/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            response_body = response.read().decode("utf-8", errors="replace")
            return {
                "status_code": response.status,
                "body": response_body[:1000],
            }
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Discord webhook error {exc.code}: {error_body[:500]}") from exc
