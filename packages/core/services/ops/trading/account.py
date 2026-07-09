from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import datetime
from typing import Any


from core.money import money_float
from core.services.broker_sync import BROKER_SYNC_KEY
from core.services.option_structures import position_legs, unique_leg_symbols
from core.value_coercion import (
    as_list,
    as_mapping,
    as_text,
    coerce_float,
    coerce_int,
)

from core.services.ops.shared import (
    _attention,
    _seconds_since,
)

from .broker import broker_sync_payload as _broker_sync_payload

from core.services.ops.trading.models import (
    BROKER_OPTION_ASSET_CLASSES,
    TOP_POSITION_LIMIT,
    _AccountProjection,
)

def _age_seconds(value: Any, *, now: datetime) -> float | None:
    age = _seconds_since(value, now=now)
    return None if age is None else round(age, 1)


def _alert_delivery_payload(
    rows: list[dict[str, Any]],
    *,
    now: datetime,
) -> dict[str, Any]:
    recent_rows = [
        row
        for row in rows
        if _seconds_since(row.get("updated_at") or row.get("created_at"), now=now) is not None
        and (_seconds_since(row.get("updated_at") or row.get("created_at"), now=now) or 0) <= 24 * 60 * 60
    ]
    counts = Counter(str(row.get("status") or "unknown") for row in recent_rows)
    status = "healthy"
    if counts.get("dead_letter", 0) or counts.get("retry_wait", 0):
        status = "degraded"
    return {
        "status": status,
        "recent_event_count": len(recent_rows),
        "status_counts": dict(counts),
        "dead_letter_count": counts.get("dead_letter", 0),
        "retry_wait_count": counts.get("retry_wait", 0),
        "dispatching_count": counts.get("dispatching", 0),
        "pending_count": counts.get("pending", 0),
    }


def _account_snapshot_payload(snapshot: Mapping[str, Any] | None) -> dict[str, Any]:
    if snapshot is None:
        return {
            "status": "missing",
            "source": None,
            "environment": None,
            "captured_at": None,
            "account": {},
            "pnl": {},
            "positions": [],
        }
    return {
        "status": "ready",
        "snapshot_id": snapshot.get("snapshot_id"),
        "broker": snapshot.get("broker"),
        "environment": snapshot.get("environment"),
        "source": "snapshot",
        "captured_at": snapshot.get("captured_at"),
        "account": dict(snapshot.get("account") or {}),
        "pnl": dict(snapshot.get("pnl") or {}),
        "positions": list(snapshot.get("positions") or []),
    }


def _top_positions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for row in rows:
        exposure = coerce_float(row.get("max_loss"))
        if exposure is None:
            exposure = coerce_float(row.get("entry_notional"))
        net_pnl = coerce_float(row.get("net_pnl"))
        ranked.append(
            {
                "position_id": row.get("position_id"),
                "underlying_symbol": row.get("underlying_symbol") or row.get("root_symbol"),
                "status": row.get("status") or row.get("position_status"),
                "exposure": 0.0 if exposure is None else money_float(abs(exposure)),
                "net_pnl": None if net_pnl is None else money_float(net_pnl),
                "risk_status": row.get("risk_status"),
            }
        )
    ranked.sort(key=lambda row: float(row.get("exposure") or 0.0), reverse=True)
    return ranked[:TOP_POSITION_LIMIT]


def _is_option_broker_position(position: Mapping[str, Any]) -> bool:
    return str(position.get("asset_class") or "").strip().lower() in BROKER_OPTION_ASSET_CLASSES


def _managed_leg_index(open_positions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for position in open_positions:
        owner_kind = "spreads_managed"
        if str(position.get("source_object_type") or "") == "synthetic_validation":
            owner_kind = "spreads_synthetic_validation"
        for symbol in unique_leg_symbols(position_legs(position)):
            index.setdefault(
                symbol,
                {
                    "owner_kind": owner_kind,
                    "position_id": position.get("position_id"),
                    "trading_strategy_id": position.get("trading_strategy_id"),
                    "source_object_type": position.get("source_object_type"),
                    "root_symbol": position.get("root_symbol") or position.get("underlying_symbol"),
                    "strategy_family": position.get("strategy_family") or position.get("strategy"),
                },
            )
    return index


def _broker_exposure_state(
    *,
    account_snapshot: Mapping[str, Any],
    open_positions: list[dict[str, Any]],
    broker_sync: Mapping[str, Any],
) -> dict[str, Any]:
    broker_positions = [dict(row) for row in as_list(account_snapshot.get("positions")) if isinstance(row, Mapping)]
    managed_by_symbol = _managed_leg_index(open_positions)
    classified: list[dict[str, Any]] = []
    owner_counts: Counter[str] = Counter()
    option_owner_counts: Counter[str] = Counter()
    total_market_value = 0.0
    option_market_value = 0.0

    for position in broker_positions:
        symbol = as_text(position.get("symbol"))
        managed = managed_by_symbol.get(symbol or "")
        owner_kind = "external_manual" if managed is None else str(managed.get("owner_kind") or "spreads_managed")
        is_option = _is_option_broker_position(position)
        owner_counts[owner_kind] += 1
        if is_option:
            option_owner_counts[owner_kind] += 1
        market_value = coerce_float(position.get("market_value")) or 0.0
        total_market_value += market_value
        if is_option:
            option_market_value += market_value
        classified.append(
            {
                "symbol": symbol,
                "asset_class": position.get("asset_class"),
                "side": position.get("side"),
                "qty": position.get("qty"),
                "market_value": position.get("market_value"),
                "cost_basis": position.get("cost_basis"),
                "unrealized_pl": position.get("unrealized_pl"),
                "unrealized_intraday_pl": position.get("unrealized_intraday_pl"),
                "ownership": owner_kind,
                "spreads_position_id": None if managed is None else managed.get("position_id"),
                "trading_strategy_id": None if managed is None else managed.get("trading_strategy_id"),
                "source_object_type": None if managed is None else managed.get("source_object_type"),
                "root_symbol": None if managed is None else managed.get("root_symbol"),
                "strategy_family": None if managed is None else managed.get("strategy_family"),
            }
        )

    external_option_count = option_owner_counts.get("external_manual", 0)
    managed_option_count = sum(count for owner, count in option_owner_counts.items() if owner != "external_manual")
    status = "clear"
    if external_option_count and managed_option_count:
        status = "mixed"
    elif external_option_count:
        status = "external_present"
    elif managed_option_count:
        status = "managed"

    broker_sync_summary = as_mapping(broker_sync.get("summary"))
    return {
        "status": status,
        "broker_position_count": len(broker_positions),
        "broker_option_position_count": sum(1 for row in broker_positions if _is_option_broker_position(row)),
        "spreads_managed_option_position_count": managed_option_count,
        "external_manual_option_position_count": external_option_count,
        "owner_counts": dict(sorted(owner_counts.items())),
        "option_owner_counts": dict(sorted(option_owner_counts.items())),
        "total_market_value": money_float(total_market_value),
        "option_market_value": money_float(option_market_value),
        "broker_sync_orphan_position_count": coerce_int(broker_sync_summary.get("orphan_broker_position_count")) or 0,
        "positions": classified[:25],
    }

def _project_account(
    *,
    storage: Any,
    now: datetime,
    market_session: Mapping[str, Any],
) -> _AccountProjection:
    statuses: list[str] = []
    attention: list[dict[str, str]] = []
    broker_store = storage.broker
    if broker_store.schema_ready():
        broker_sync_status, broker_sync = _broker_sync_payload(
            broker_store.get_sync_state(BROKER_SYNC_KEY),
            now=now,
            market_session=market_session,
        )
        account_snapshot = _account_snapshot_payload(broker_store.get_latest_account_snapshot())
    else:
        broker_sync_status = "blocked"
        broker_sync = {
            "status": "missing",
            "raw_status": None,
            "updated_at": None,
            "summary": {},
            "error_text": None,
            "age_seconds": None,
        }
        account_snapshot = _account_snapshot_payload(None)
        attention.append(
            _attention(
                severity="high",
                code="broker_schema_unavailable",
                message="Broker sync and account snapshot storage are not available yet.",
            )
        )
    statuses.append(broker_sync_status)
    if broker_sync_status not in {"healthy", "idle"}:
        attention.append(
            _attention(
                severity="high" if broker_sync_status == "blocked" else "medium",
                code="broker_sync_unhealthy",
                message="Broker sync is missing, stale, or degraded.",
            )
        )

    account = as_mapping(account_snapshot.get("account"))
    if account_snapshot.get("status") != "ready":
        statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="account_snapshot_missing",
                message="No stored broker account snapshot is available.",
            )
        )
    elif account.get("trading_blocked") or account.get("account_blocked"):
        statuses.append("blocked")
        attention.append(
            _attention(
                severity="high",
                code="broker_account_blocked",
                message="The stored broker account snapshot indicates trading is blocked.",
            )
        )

    return _AccountProjection(
        broker_sync_status=broker_sync_status,
        broker_sync=broker_sync,
        account_snapshot=account_snapshot,
        account=account,
        statuses=tuple(statuses),
        attention=attention,
    )
