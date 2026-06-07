from __future__ import annotations

import argparse
import asyncio
import logging
import os
import socket
from collections import defaultdict
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from core.common import load_local_env
from core.integrations.alpaca.client import DEFAULT_DATA_BASE_URL
from core.jobs.orchestration import market_recorder_runtime_lease_key
from core.observability.logging import configure_logging, log_event
from core.runtime.config import default_database_url
from core.services.market_dates import NEW_YORK, market_session_window
from core.services.option_quote_records import build_quote_records
from core.services.option_stream_broker import (
    AlpacaOptionStreamBroker,
    render_option_capture_timestamp,
)
from core.services.option_trade_records import build_trade_records
from core.services.trading_engine.capture_targets import refresh_engine_capture_targets
from core.storage.factory import build_storage_context

DEFAULT_POLL_SECONDS = 25.0
DEFAULT_OFF_HOURS_POLL_SECONDS = 30.0
DEFAULT_IDLE_LOG_SECONDS = 300.0
DEFAULT_QUOTE_DURATION_SECONDS = 20.0
DEFAULT_TRADE_DURATION_SECONDS = 20.0
DEFAULT_TARGET_LIMIT = 1000
MARKET_RECORDER_SOURCE = "market_recorder"
MARKET_RECORDER_LEASE_SCOPE = "alpaca_options"

logger = logging.getLogger(__name__)


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _env_float(name: str, default: float) -> float:
    raw = _as_text(os.environ.get(name))
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _current_deploy_env() -> str | None:
    return _as_text(os.environ.get("SPREADS_DEPLOY_ENV"))


def _configured_recorder_owner_env() -> str | None:
    return _as_text(os.environ.get("SPREADS_MARKET_RECORDER_OWNER_ENV"))


def _market_recorder_owner_id() -> str:
    deploy_env = _current_deploy_env() or "unknown"
    return f"{deploy_env}:{socket.gethostname()}:{os.getpid()}"


def _market_recorder_lease_seconds(
    *,
    poll_seconds: float,
    quote_duration_seconds: float,
    trade_duration_seconds: float,
) -> int:
    base_window = max(
        float(poll_seconds or 0.0),
        float(quote_duration_seconds or 0.0),
        float(trade_duration_seconds or 0.0),
        1.0,
    )
    return max(int(base_window * 2) + 15, 60)


def _owner_mismatch_payload() -> dict[str, Any] | None:
    configured_owner_env = _configured_recorder_owner_env()
    current_env = _current_deploy_env()
    if configured_owner_env is None or current_env == configured_owner_env:
        return None
    return {
        "status": "skipped",
        "reason": "owner_env_mismatch",
        "deploy_env": current_env,
        "configured_owner_env": configured_owner_env,
        "message": (f"Market recorder ownership is assigned to {configured_owner_env}; " f"{current_env or 'unknown'} is read-only."),
    }


def _market_session_payload(*, now: datetime, calendar_name: str = "NYSE") -> dict[str, Any]:
    local_now = now.astimezone(NEW_YORK)
    market_window = market_session_window(calendar_name, local_now.date())
    if market_window is None:
        return {
            "calendar": calendar_name,
            "status": "closed",
            "is_open": False,
            "market_open_at": None,
            "market_close_at": None,
        }
    market_open, market_close = market_window
    is_open = market_open <= local_now < market_close
    return {
        "calendar": calendar_name,
        "status": "open" if is_open else "closed",
        "is_open": is_open,
        "market_open_at": market_open.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "market_close_at": market_close.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }


def _closed_market_idle_summary(*, calendar_name: str = "NYSE") -> dict[str, Any] | None:
    market_session = _market_session_payload(now=datetime.now(UTC), calendar_name=calendar_name)
    if bool(market_session.get("is_open")):
        return None
    return {
        "status": "idle",
        "reason": "market_closed",
        "market_session": market_session,
    }


def _build_route(row: Mapping[str, Any]) -> dict[str, Any] | None:
    option_symbol = _as_text(row.get("option_symbol"))
    if option_symbol is None:
        return None
    label = _as_text(row.get("label")) or _as_text(row.get("session_id")) or _as_text(row.get("owner_key")) or "market_recorder"
    return {
        "option_symbol": option_symbol,
        "label": label,
        "profile": _as_text(row.get("profile")),
        "underlying_symbol": _as_text(row.get("underlying_symbol")),
        "strategy": _as_text(row.get("strategy")),
        "leg_role": _as_text(row.get("leg_role")) or "contract",
        "quote_enabled": bool(row.get("quote_enabled", True)),
        "trade_enabled": bool(row.get("trade_enabled", False)),
    }


def _build_capture_groups(
    target_rows: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in target_rows:
        route = _build_route(row)
        if route is None:
            continue
        feed = _as_text(row.get("feed")) or "opra"
        data_base_url = (_as_text(row.get("data_base_url")) or DEFAULT_DATA_BASE_URL).rstrip("/")
        group = grouped.setdefault(
            (feed, data_base_url),
            {
                "feed": feed,
                "data_base_url": data_base_url,
                "candidates_by_symbol": {},
                "routes_by_symbol": defaultdict(dict),
            },
        )
        option_symbol = route["option_symbol"]
        candidate = group["candidates_by_symbol"].setdefault(
            option_symbol,
            {
                "option_symbol": option_symbol,
                "underlying_symbol": route["underlying_symbol"],
                "strategy": route["strategy"],
                "leg_role": route["leg_role"],
            },
        )
        if candidate.get("underlying_symbol") is None:
            candidate["underlying_symbol"] = route["underlying_symbol"]
        if candidate.get("strategy") is None:
            candidate["strategy"] = route["strategy"]
        if candidate.get("leg_role") in {None, "", "contract"}:
            candidate["leg_role"] = route["leg_role"]

        route_key = (
            route["label"],
            route["profile"],
            route["underlying_symbol"],
            route["strategy"],
            route["leg_role"],
        )
        existing_route = group["routes_by_symbol"][option_symbol].setdefault(
            route_key,
            {
                **route,
                "quote_enabled": False,
                "trade_enabled": False,
            },
        )
        existing_route["quote_enabled"] = bool(existing_route["quote_enabled"] or route["quote_enabled"])
        existing_route["trade_enabled"] = bool(existing_route["trade_enabled"] or route["trade_enabled"])

    groups: list[dict[str, Any]] = []
    for group in grouped.values():
        candidates_by_symbol = dict(group.pop("candidates_by_symbol"))
        routes_by_symbol = {
            option_symbol: [dict(route) for route in route_map.values()] for option_symbol, route_map in dict(group.pop("routes_by_symbol")).items()
        }
        quote_symbols = sorted(
            option_symbol for option_symbol, routes in routes_by_symbol.items() if any(bool(route.get("quote_enabled")) for route in routes)
        )
        trade_symbols = sorted(
            option_symbol for option_symbol, routes in routes_by_symbol.items() if any(bool(route.get("trade_enabled")) for route in routes)
        )
        groups.append(
            {
                **group,
                "candidates": list(candidates_by_symbol.values()),
                "routes_by_symbol": routes_by_symbol,
                "quote_symbols": quote_symbols,
                "trade_symbols": trade_symbols,
            }
        )
    return groups


def _target_counts(target_rows: list[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in target_rows:
        reason = _as_text(row.get("reason")) or "unknown"
        counts[reason] = counts.get(reason, 0) + 1
    return counts


def _capture_group_summary(capture_groups: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "feed": group.get("feed"),
            "data_base_url": group.get("data_base_url"),
            "quote_symbol_count": len(list(group.get("quote_symbols") or [])),
            "trade_symbol_count": len(list(group.get("trade_symbols") or [])),
        }
        for group in capture_groups
    ]


def _save_capture_summary(
    *,
    capture_store: Any,
    summary: dict[str, Any],
    target_rows: list[Mapping[str, Any]],
    target_limit: int,
    target_refresh: Mapping[str, Any],
    capture_groups: list[Mapping[str, Any]],
) -> dict[str, Any]:
    if not capture_store.schema_ready():
        return summary
    capture_summary = capture_store.save_capture_summary(
        source=MARKET_RECORDER_SOURCE,
        status=str(summary.get("status") or "unknown"),
        active_target_count=int(target_refresh.get("active_target_count") or len(target_rows)),
        selected_target_count=len(target_rows),
        capture_group_count=int(summary.get("capture_group_count") or len(capture_groups)),
        quote_rows_saved=int(summary.get("quote_rows_saved") or 0),
        trade_rows_saved=int(summary.get("trade_rows_saved") or 0),
        target_limit=target_limit,
        target_counts=dict(target_refresh.get("active_target_counts") or _target_counts(target_rows)),
        group_summary=_capture_group_summary(capture_groups),
        errors={
            "quote_errors": list(summary.get("quote_errors") or []),
            "trade_errors": list(summary.get("trade_errors") or []),
        },
        metadata={
            "target_refresh": dict(target_refresh),
            "active_target_limit_reached": int(target_refresh.get("active_target_count") or len(target_rows)) > len(target_rows),
        },
    )
    return {**summary, "capture_summary_id": capture_summary.get("capture_summary_id")}


def _fan_out_quote_rows(
    *,
    cycle_id: str,
    quote_records: list[dict[str, Any]],
    routes_by_symbol: Mapping[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in quote_records:
        option_symbol = _as_text(record.get("option_symbol"))
        if option_symbol is None:
            continue
        for route in list(routes_by_symbol.get(option_symbol) or []):
            if not bool(route.get("quote_enabled")):
                continue
            rows.append(
                {
                    **dict(record),
                    "cycle_id": cycle_id,
                    "label": route["label"],
                    "profile": route.get("profile"),
                    "underlying_symbol": route.get("underlying_symbol") or record.get("underlying_symbol"),
                    "strategy": route.get("strategy") or record.get("strategy"),
                    "leg_role": route.get("leg_role") or record.get("leg_role"),
                    "source": MARKET_RECORDER_SOURCE,
                }
            )
    return rows


def _fan_out_trade_rows(
    *,
    cycle_id: str,
    trade_records: list[dict[str, Any]],
    routes_by_symbol: Mapping[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in trade_records:
        option_symbol = _as_text(record.get("option_symbol"))
        if option_symbol is None:
            continue
        for route in list(routes_by_symbol.get(option_symbol) or []):
            if not bool(route.get("trade_enabled")):
                continue
            rows.append(
                {
                    **dict(record),
                    "cycle_id": cycle_id,
                    "label": route["label"],
                    "profile": route.get("profile"),
                    "underlying_symbol": route.get("underlying_symbol") or record.get("underlying_symbol"),
                    "strategy": route.get("strategy") or record.get("strategy"),
                    "leg_role": route.get("leg_role") or record.get("leg_role"),
                    "source": MARKET_RECORDER_SOURCE,
                }
            )
    return rows


async def _capture_group(
    *,
    broker: AlpacaOptionStreamBroker,
    group: Mapping[str, Any],
    quote_duration_seconds: float,
    trade_duration_seconds: float,
) -> dict[str, Any]:
    quote_symbols = list(group.get("quote_symbols") or [])
    trade_symbols = list(group.get("trade_symbols") or [])
    routes_by_symbol = group.get("routes_by_symbol") if isinstance(group.get("routes_by_symbol"), Mapping) else {}
    data_base_url = str(group.get("data_base_url") or DEFAULT_DATA_BASE_URL)
    feed = str(group.get("feed") or "opra")

    quote_task = None
    trade_task = None
    if quote_symbols and quote_duration_seconds > 0:
        quote_task = asyncio.create_task(
            broker.capture(
                symbols=quote_symbols,
                feed=feed,
                duration_seconds=quote_duration_seconds,
                want_quotes=True,
                want_trades=False,
                data_base_url=data_base_url,
            )
        )
    if trade_symbols and trade_duration_seconds > 0:
        trade_task = asyncio.create_task(
            broker.capture(
                symbols=trade_symbols,
                feed=feed,
                duration_seconds=trade_duration_seconds,
                want_quotes=False,
                want_trades=True,
                data_base_url=data_base_url,
            )
        )

    captured_at = render_option_capture_timestamp()
    candidates = [dict(row) for row in list(group.get("candidates") or []) if isinstance(row, Mapping)]
    quote_candidates = [row for row in candidates if _as_text(row.get("option_symbol")) in set(quote_symbols)]
    trade_candidates = [row for row in candidates if _as_text(row.get("option_symbol")) in set(trade_symbols)]

    quote_records: list[dict[str, Any]] = []
    trade_records: list[dict[str, Any]] = []
    quote_error = None
    trade_error = None
    if quote_task is not None:
        try:
            quote_result = await quote_task
            quote_records = build_quote_records(
                captured_at=captured_at,
                symbol_metadata={str(row["option_symbol"]): dict(row) for row in quote_candidates if _as_text(row.get("option_symbol")) is not None},
                quotes=quote_result.quotes,
                source=MARKET_RECORDER_SOURCE,
            )
        except Exception as exc:
            quote_error = str(exc)
    if trade_task is not None:
        try:
            trade_result = await trade_task
            trade_records = build_trade_records(
                captured_at=captured_at,
                symbol_metadata={str(row["option_symbol"]): dict(row) for row in trade_candidates if _as_text(row.get("option_symbol")) is not None},
                trades=trade_result.trades,
                source=MARKET_RECORDER_SOURCE,
            )
        except Exception as exc:
            trade_error = str(exc)

    cycle_id = f"market_recorder:{captured_at}:{uuid4().hex[:8]}"
    return {
        "feed": feed,
        "data_base_url": data_base_url,
        "quote_symbols": quote_symbols,
        "trade_symbols": trade_symbols,
        "quote_rows": _fan_out_quote_rows(
            cycle_id=cycle_id,
            quote_records=quote_records,
            routes_by_symbol=routes_by_symbol,
        ),
        "trade_rows": _fan_out_trade_rows(
            cycle_id=cycle_id,
            trade_records=trade_records,
            routes_by_symbol=routes_by_symbol,
        ),
        "quote_error": quote_error,
        "trade_error": trade_error,
    }


async def run_market_recorder_iteration(
    *,
    db_target: str,
    broker: AlpacaOptionStreamBroker,
    quote_duration_seconds: float,
    trade_duration_seconds: float,
    poll_seconds: float,
    target_limit: int,
    lease_key: str,
    lease_owner: str,
) -> dict[str, Any]:
    owner_mismatch = _owner_mismatch_payload()
    if owner_mismatch is not None:
        return owner_mismatch

    with build_storage_context(db_target) as storage:
        jobs_store = storage.jobs
        capture_store = storage.capture
        history_store = storage.history
        if jobs_store.schema_ready():
            lease_seconds = _market_recorder_lease_seconds(
                poll_seconds=poll_seconds,
                quote_duration_seconds=quote_duration_seconds,
                trade_duration_seconds=trade_duration_seconds,
            )
            lease_state = {
                "deploy_env": _current_deploy_env(),
                "configured_owner_env": _configured_recorder_owner_env(),
                "hostname": socket.gethostname(),
                "pid": os.getpid(),
            }
            acquired = jobs_store.acquire_lease(
                lease_key=lease_key,
                owner=lease_owner,
                expires_in_seconds=lease_seconds,
                state=lease_state,
            )
            if not acquired:
                existing_lease = jobs_store.get_lease(lease_key)
                return {
                    "status": "skipped",
                    "reason": "lease_unavailable",
                    "lease_key": lease_key,
                    "lease_owner": existing_lease.get("owner") if isinstance(existing_lease, Mapping) else None,
                    "message": "Another market recorder already owns the live options stream lease.",
                }
        if not capture_store.target_schema_ready():
            return {
                "status": "skipped",
                "reason": "capture_schema_unavailable",
            }
        target_refresh = refresh_engine_capture_targets(storage=storage)
        target_rows = [dict(row) for row in capture_store.list_active_capture_targets(limit=target_limit)]
        capture_groups = _build_capture_groups(target_rows)
        if not capture_groups:
            summary = {
                "status": "idle",
                "active_target_count": int(target_refresh.get("active_target_count") or 0),
                "selected_target_count": len(target_rows),
                "capture_group_count": 0,
                "target_refresh": target_refresh,
                "target_counts": dict(target_refresh.get("active_target_counts") or {}),
                "quote_rows_saved": 0,
                "trade_rows_saved": 0,
                "quote_errors": [],
                "trade_errors": [],
            }
            return _save_capture_summary(
                capture_store=capture_store,
                summary=summary,
                target_rows=target_rows,
                target_limit=target_limit,
                target_refresh=target_refresh,
                capture_groups=capture_groups,
            )

        group_results = await asyncio.gather(
            *[
                _capture_group(
                    broker=broker,
                    group=group,
                    quote_duration_seconds=quote_duration_seconds,
                    trade_duration_seconds=trade_duration_seconds,
                )
                for group in capture_groups
            ]
        )
        quote_rows = [row for result in group_results for row in list(result.get("quote_rows") or []) if isinstance(row, Mapping)]
        trade_rows = [row for result in group_results for row in list(result.get("trade_rows") or []) if isinstance(row, Mapping)]
        quote_rows_saved = history_store.save_option_quote_tick_rows(rows=[dict(row) for row in quote_rows])
        trade_rows_saved = history_store.save_option_trade_tick_rows(rows=[dict(row) for row in trade_rows])
        summary = {
            "status": "ok",
            "active_target_count": int(target_refresh.get("active_target_count") or len(target_rows)),
            "selected_target_count": len(target_rows),
            "capture_group_count": len(capture_groups),
            "target_refresh": target_refresh,
            "target_counts": dict(target_refresh.get("active_target_counts") or _target_counts(target_rows)),
            "quote_rows_saved": quote_rows_saved,
            "trade_rows_saved": trade_rows_saved,
            "quote_errors": [
                {
                    "feed": result["feed"],
                    "data_base_url": result["data_base_url"],
                    "error": result["quote_error"],
                }
                for result in group_results
                if _as_text(result.get("quote_error")) is not None
            ],
            "trade_errors": [
                {
                    "feed": result["feed"],
                    "data_base_url": result["data_base_url"],
                    "error": result["trade_error"],
                }
                for result in group_results
                if _as_text(result.get("trade_error")) is not None
            ],
        }
        return _save_capture_summary(
            capture_store=capture_store,
            summary=summary,
            target_rows=target_rows,
            target_limit=target_limit,
            target_refresh=target_refresh,
            capture_groups=capture_groups,
        )


async def run_market_recorder_loop(args: argparse.Namespace) -> int:
    broker = AlpacaOptionStreamBroker()
    lease_key = market_recorder_runtime_lease_key(MARKET_RECORDER_LEASE_SCOPE)
    lease_owner = _market_recorder_owner_id()
    last_idle_log_at = 0.0
    try:
        while True:
            if args.market_hours_only:
                idle_summary = _closed_market_idle_summary(calendar_name=args.market_calendar)
                if idle_summary is not None:
                    loop_now = asyncio.get_running_loop().time()
                    should_log = args.once or last_idle_log_at <= 0.0 or loop_now - last_idle_log_at >= float(args.idle_log_seconds)
                    if should_log:
                        log_event(
                            logger,
                            logging.INFO,
                            "market_recorder_idle",
                            **idle_summary,
                            next_check_seconds=float(args.off_hours_poll_seconds),
                        )
                        last_idle_log_at = loop_now
                    if args.once:
                        return 0
                    await asyncio.sleep(max(float(args.off_hours_poll_seconds), 1.0))
                    continue

            iteration_started_at = asyncio.get_running_loop().time()
            summary = await run_market_recorder_iteration(
                db_target=args.db,
                broker=broker,
                quote_duration_seconds=args.quote_duration_seconds,
                trade_duration_seconds=args.trade_duration_seconds,
                poll_seconds=args.poll_seconds,
                target_limit=args.target_limit,
                lease_key=lease_key,
                lease_owner=lease_owner,
            )
            log_event(
                logger,
                logging.INFO,
                "market_recorder_iteration",
                **summary,
            )
            if args.once:
                return 0
            elapsed = asyncio.get_running_loop().time() - iteration_started_at
            await asyncio.sleep(max(float(args.poll_seconds) - elapsed, 0.0))
    finally:
        try:
            with build_storage_context(args.db) as storage:
                jobs_store = storage.jobs
                if jobs_store.schema_ready():
                    jobs_store.release_lease(lease_key, owner=lease_owner)
        except Exception as exc:
            log_event(
                logger,
                logging.WARNING,
                "market_recorder_lease_release_failed",
                exc_info=True,
                lease_key=lease_key,
                lease_owner=lease_owner,
                error=str(exc),
            )
        await broker.aclose()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuously record option quote and trade ticks for active capture targets.")
    parser.add_argument(
        "--db",
        default=default_database_url(),
        help="Postgres database URL.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=DEFAULT_POLL_SECONDS,
        help="Seconds between recorder iterations. Default: 25",
    )
    parser.add_argument(
        "--market-hours-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Only open Alpaca option-stream capture during regular market hours. Default: true",
    )
    parser.add_argument(
        "--market-calendar",
        default="NYSE",
        help="Market calendar used for recorder market-hours gating. Default: NYSE",
    )
    parser.add_argument(
        "--off-hours-poll-seconds",
        type=float,
        default=_env_float("SPREADS_MARKET_RECORDER_OFF_HOURS_POLL_SECONDS", DEFAULT_OFF_HOURS_POLL_SECONDS),
        help="Seconds between market-hours checks while the market is closed. Default: 30",
    )
    parser.add_argument(
        "--idle-log-seconds",
        type=float,
        default=_env_float("SPREADS_MARKET_RECORDER_IDLE_LOG_SECONDS", DEFAULT_IDLE_LOG_SECONDS),
        help="Minimum seconds between closed-market idle log events. Default: 300",
    )
    parser.add_argument(
        "--quote-duration-seconds",
        type=float,
        default=DEFAULT_QUOTE_DURATION_SECONDS,
        help="Quote capture window per iteration. Default: 20",
    )
    parser.add_argument(
        "--trade-duration-seconds",
        type=float,
        default=DEFAULT_TRADE_DURATION_SECONDS,
        help="Trade capture window per iteration. Default: 20",
    )
    parser.add_argument(
        "--target-limit",
        type=int,
        default=DEFAULT_TARGET_LIMIT,
        help="Maximum active capture targets to load per iteration. Default: 1000",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single recorder iteration and exit.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_local_env()
    configure_logging(service="market-recorder", force=True)
    args = parse_args(argv)
    try:
        return asyncio.run(run_market_recorder_loop(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
