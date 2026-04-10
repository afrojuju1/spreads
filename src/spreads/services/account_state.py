from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

from spreads.db.core import first_model_row
from spreads.db.decorators import with_session
from spreads.services.alpaca import create_alpaca_client_from_env, resolve_trading_environment
from spreads.storage.broker_models import AccountSnapshotModel, BrokerSyncStateModel
from spreads.storage.capabilities import StorageCapabilities

AccountHistoryRange = Literal["1D", "1W", "1M"]

HISTORY_RANGE_REQUESTS: dict[AccountHistoryRange, dict[str, str | None]] = {
    "1D": {
        "period": "1D",
        "timeframe": "5Min",
        "intraday_reporting": "market_hours",
    },
    "1W": {
        "period": "1W",
        "timeframe": "1H",
        "intraday_reporting": "market_hours",
    },
    "1M": {
        "period": "1M",
        "timeframe": "1D",
        "intraday_reporting": None,
    },
}
ACCOUNT_OVERVIEW_LIVE_TIMEOUT_SECONDS = 5.0


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    return None


def _parse_history_timestamp(value: Any) -> str | None:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    text = _as_text(value)
    if text is None:
        return None
    if text.isdigit():
        return datetime.fromtimestamp(float(text), UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    normalized = text.replace("Z", "+00:00") if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return text
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def normalize_history_range(value: str | None) -> AccountHistoryRange:
    normalized = (_as_text(value) or "1D").upper()
    if normalized not in HISTORY_RANGE_REQUESTS:
        raise ValueError(f"Unsupported history range: {normalized}")
    return normalized  # type: ignore[return-value]


def _normalize_account(payload: dict[str, Any]) -> dict[str, Any]:
    equity = _coerce_float(payload.get("equity"))
    last_equity = _coerce_float(payload.get("last_equity"))
    return {
        "account_number": _as_text(payload.get("account_number")),
        "status": _as_text(payload.get("status")),
        "currency": _as_text(payload.get("currency")) or "USD",
        "equity": equity,
        "last_equity": last_equity,
        "cash": _coerce_float(payload.get("cash")),
        "buying_power": _coerce_float(payload.get("buying_power")),
        "regt_buying_power": _coerce_float(payload.get("regt_buying_power")),
        "daytrading_buying_power": _coerce_float(payload.get("daytrading_buying_power")),
        "non_marginable_buying_power": _coerce_float(payload.get("non_marginable_buying_power")),
        "options_buying_power": _coerce_float(payload.get("options_buying_power")),
        "portfolio_value": equity,
        "long_market_value": _coerce_float(payload.get("long_market_value")),
        "short_market_value": _coerce_float(payload.get("short_market_value")),
        "initial_margin": _coerce_float(payload.get("initial_margin")),
        "maintenance_margin": _coerce_float(payload.get("maintenance_margin")),
        "daytrade_count": _coerce_int(payload.get("daytrade_count")),
        "pattern_day_trader": _coerce_bool(payload.get("pattern_day_trader")),
        "trading_blocked": _coerce_bool(payload.get("trading_blocked")),
        "transfers_blocked": _coerce_bool(payload.get("transfers_blocked")),
        "account_blocked": _coerce_bool(payload.get("account_blocked")),
        "shorting_enabled": _coerce_bool(payload.get("shorting_enabled")),
    }


def _build_pnl_snapshot(account: dict[str, Any]) -> dict[str, Any]:
    equity = _coerce_float(account.get("equity"))
    last_equity = _coerce_float(account.get("last_equity"))
    if equity is None or last_equity is None:
        return {
            "day_change": None,
            "day_change_percent": None,
        }
    day_change = equity - last_equity
    day_change_percent = None if last_equity == 0 else day_change / last_equity
    return {
        "day_change": round(day_change, 2),
        "day_change_percent": day_change_percent,
    }


def _normalize_positions(payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in payload:
        market_value = _coerce_float(item.get("market_value"))
        rows.append(
            {
                "asset_id": _as_text(item.get("asset_id")),
                "symbol": _as_text(item.get("symbol")) or "—",
                "asset_class": _as_text(item.get("asset_class")),
                "exchange": _as_text(item.get("exchange")),
                "side": _as_text(item.get("side")),
                "qty": _coerce_float(item.get("qty")),
                "qty_available": _coerce_float(item.get("qty_available")),
                "market_value": market_value,
                "cost_basis": _coerce_float(item.get("cost_basis")),
                "avg_entry_price": _coerce_float(item.get("avg_entry_price")),
                "current_price": _coerce_float(item.get("current_price")),
                "change_today": _coerce_float(item.get("change_today")),
                "unrealized_pl": _coerce_float(item.get("unrealized_pl")),
                "unrealized_plpc": _coerce_float(item.get("unrealized_plpc")),
                "unrealized_intraday_pl": _coerce_float(item.get("unrealized_intraday_pl")),
                "unrealized_intraday_plpc": _coerce_float(item.get("unrealized_intraday_plpc")),
            }
        )
    rows.sort(
        key=lambda item: abs(float(item["market_value"])) if item.get("market_value") is not None else 0.0,
        reverse=True,
    )
    return rows


def _normalize_history(
    *,
    history_range: AccountHistoryRange,
    payload: dict[str, Any],
    request: dict[str, str | None],
) -> dict[str, Any]:
    timestamps = payload.get("timestamp")
    equities = payload.get("equity")
    pnl_values = payload.get("profit_loss")
    pnl_percentages = payload.get("profit_loss_pct")
    points: list[dict[str, Any]] = []

    if isinstance(timestamps, list):
        for index, raw_timestamp in enumerate(timestamps):
            timestamp = _parse_history_timestamp(raw_timestamp)
            if timestamp is None:
                continue
            equity = None
            if isinstance(equities, list) and index < len(equities):
                equity = _coerce_float(equities[index])
            profit_loss = None
            if isinstance(pnl_values, list) and index < len(pnl_values):
                profit_loss = _coerce_float(pnl_values[index])
            profit_loss_pct = None
            if isinstance(pnl_percentages, list) and index < len(pnl_percentages):
                profit_loss_pct = _coerce_float(pnl_percentages[index])
            points.append(
                {
                    "timestamp": timestamp,
                    "equity": equity,
                    "profit_loss": profit_loss,
                    "profit_loss_pct": profit_loss_pct,
                }
            )

    return {
        "range": history_range,
        "period": request["period"],
        "timeframe": request["timeframe"],
        "intraday_reporting": request["intraday_reporting"],
        "base_value": _coerce_float(payload.get("base_value")),
        "points": points,
    }


def _schema_ready(session: Session) -> bool:
    return StorageCapabilities(session.get_bind()).has_tables("account_snapshots", "broker_sync_state")


def _sync_payload(session: Session) -> dict[str, Any] | None:
    if not _schema_ready(session):
        return None
    state = first_model_row(
        session,
        select(BrokerSyncStateModel).where(BrokerSyncStateModel.sync_key == "broker_sync:alpaca"),
    )
    if state is None:
        return None
    return {
        "status": state["status"],
        "updated_at": state["updated_at"],
        "summary": dict(state["summary"]),
        "error_text": state["error_text"],
    }


def fetch_account_overview_live(
    *,
    history_range: str | None = None,
    request_timeout_seconds: float | None = None,
) -> dict[str, Any]:
    resolved_history_range = normalize_history_range(history_range)
    history_request = HISTORY_RANGE_REQUESTS[resolved_history_range]
    client = create_alpaca_client_from_env(request_timeout_seconds=request_timeout_seconds)
    with ThreadPoolExecutor(max_workers=3) as executor:
        account_future = executor.submit(client.get_account)
        positions_future = executor.submit(client.list_positions)
        history_future = executor.submit(
            client.get_account_portfolio_history,
            period=history_request["period"],
            timeframe=history_request["timeframe"],
            intraday_reporting=history_request["intraday_reporting"],
        )
        account_payload = account_future.result()
        positions_payload = positions_future.result()
        history_payload = history_future.result()

    account = _normalize_account(account_payload)
    return {
        "broker": "alpaca",
        "environment": resolve_trading_environment(client.trading_base_url),
        "source": "live",
        "retrieved_at": _utc_now(),
        "account": account,
        "pnl": _build_pnl_snapshot(account),
        "history": _normalize_history(
            history_range=resolved_history_range,
            payload=history_payload,
            request=history_request,
        ),
        "positions": _normalize_positions(positions_payload),
    }


def _overview_from_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    history = dict(snapshot.get("history") or {})
    history.setdefault("points", list(history.get("points") or []))
    return {
        "broker": str(snapshot["broker"]),
        "environment": str(snapshot["environment"]),
        "source": "snapshot",
        "retrieved_at": str(snapshot["captured_at"]),
        "account": dict(snapshot.get("account") or {}),
        "pnl": dict(snapshot.get("pnl") or {}),
        "history": history,
        "positions": list(snapshot.get("positions") or []),
    }


@with_session()
def get_account_overview(
    *,
    history_range: str | None = None,
    db_target: str | None = None,
    session: Session | None = None,
) -> dict[str, Any]:
    sync = _sync_payload(session)
    try:
        overview = fetch_account_overview_live(
            history_range=history_range,
            request_timeout_seconds=ACCOUNT_OVERVIEW_LIVE_TIMEOUT_SECONDS,
        )
    except Exception:
        snapshot = None
        if _schema_ready(session):
            snapshot = first_model_row(
                session,
                select(AccountSnapshotModel)
                .order_by(AccountSnapshotModel.captured_at.desc(), AccountSnapshotModel.snapshot_id.desc())
                .limit(1),
            )
        if snapshot is None:
            raise
        overview = _overview_from_snapshot(snapshot)
    return {
        **overview,
        "sync": sync,
    }
