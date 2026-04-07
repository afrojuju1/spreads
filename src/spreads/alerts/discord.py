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


def build_discord_payload(alert: dict[str, Any]) -> dict[str, Any]:
    candidate = alert["candidate"]
    strategy = str(candidate["strategy"])
    setup_status = str(candidate.get("setup_status") or "unknown")
    calendar_status = str(candidate.get("calendar_status") or "unknown")
    title = f"{alert['symbol']} {alert['profile'].upper()} {strategy.replace('_', ' ').title()}"
    description = alert["description"]
    fields = [
        {"name": "Strikes", "value": f"{candidate['short_strike']:.2f} / {candidate['long_strike']:.2f}", "inline": True},
        {"name": "Score", "value": f"{candidate['quality_score']:.1f}", "inline": True},
        {"name": "Credit", "value": compact_value(candidate.get("midpoint_credit")), "inline": True},
        {"name": "Setup", "value": setup_status, "inline": True},
        {"name": "Calendar", "value": calendar_status, "inline": True},
        {"name": "Event", "value": alert["alert_type"].replace("_", " "), "inline": True},
    ]
    board_notes = candidate.get("board_notes") or []
    if board_notes:
        fields.append({"name": "Why Now", "value": ", ".join(str(note) for note in board_notes[:3]), "inline": False})

    embed = {
        "title": title,
        "description": description,
        "color": strategy_color(strategy, alert_type=alert["alert_type"]),
        "fields": fields,
        "footer": {"text": f"{alert['label']} | {alert['profile']} | {alert['strategy_mode']}"},
        "timestamp": alert["created_at"],
    }
    return {"embeds": [embed]}


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
