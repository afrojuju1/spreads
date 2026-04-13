from __future__ import annotations

import argparse
import asyncio
import json
from collections import defaultdict
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from spreads.common import load_local_env
from spreads.runtime.config import default_database_url
from spreads.services.live_recovery import refresh_execution_capture_targets
from spreads.services.option_quote_records import build_quote_records
from spreads.services.option_stream_broker import (
    AlpacaOptionStreamBroker,
    render_option_capture_timestamp,
)
from spreads.services.option_trade_records import build_trade_records
from spreads.services.scanner import DEFAULT_DATA_BASE_URL
from spreads.storage.factory import build_storage_context

DEFAULT_POLL_SECONDS = 25.0
DEFAULT_QUOTE_DURATION_SECONDS = 20.0
DEFAULT_TRADE_DURATION_SECONDS = 20.0
DEFAULT_TARGET_LIMIT = 1000
MARKET_RECORDER_SOURCE = "market_recorder"


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _build_route(row: Mapping[str, Any]) -> dict[str, Any] | None:
    option_symbol = _as_text(row.get("option_symbol"))
    if option_symbol is None:
        return None
    label = (
        _as_text(row.get("label"))
        or _as_text(row.get("session_id"))
        or _as_text(row.get("owner_key"))
        or "market_recorder"
    )
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
        existing_route["quote_enabled"] = bool(
            existing_route["quote_enabled"] or route["quote_enabled"]
        )
        existing_route["trade_enabled"] = bool(
            existing_route["trade_enabled"] or route["trade_enabled"]
        )

    groups: list[dict[str, Any]] = []
    for group in grouped.values():
        candidates_by_symbol = dict(group.pop("candidates_by_symbol"))
        routes_by_symbol = {
            option_symbol: [dict(route) for route in route_map.values()]
            for option_symbol, route_map in dict(group.pop("routes_by_symbol")).items()
        }
        quote_symbols = sorted(
            option_symbol
            for option_symbol, routes in routes_by_symbol.items()
            if any(bool(route.get("quote_enabled")) for route in routes)
        )
        trade_symbols = sorted(
            option_symbol
            for option_symbol, routes in routes_by_symbol.items()
            if any(bool(route.get("trade_enabled")) for route in routes)
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
                    "underlying_symbol": route.get("underlying_symbol")
                    or record.get("underlying_symbol"),
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
                    "underlying_symbol": route.get("underlying_symbol")
                    or record.get("underlying_symbol"),
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
    routes_by_symbol = (
        group.get("routes_by_symbol")
        if isinstance(group.get("routes_by_symbol"), Mapping)
        else {}
    )
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
    candidates = [
        dict(row)
        for row in list(group.get("candidates") or [])
        if isinstance(row, Mapping)
    ]
    quote_candidates = [
        row for row in candidates if _as_text(row.get("option_symbol")) in set(quote_symbols)
    ]
    trade_candidates = [
        row for row in candidates if _as_text(row.get("option_symbol")) in set(trade_symbols)
    ]

    quote_records: list[dict[str, Any]] = []
    trade_records: list[dict[str, Any]] = []
    quote_error = None
    trade_error = None
    if quote_task is not None:
        try:
            quote_result = await quote_task
            quote_records = build_quote_records(
                captured_at=captured_at,
                symbol_metadata={
                    str(row["option_symbol"]): dict(row)
                    for row in quote_candidates
                    if _as_text(row.get("option_symbol")) is not None
                },
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
                symbol_metadata={
                    str(row["option_symbol"]): dict(row)
                    for row in trade_candidates
                    if _as_text(row.get("option_symbol")) is not None
                },
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
    target_limit: int,
) -> dict[str, Any]:
    with build_storage_context(db_target) as storage:
        recovery_store = storage.recovery
        history_store = storage.history
        if not recovery_store.schema_ready():
            return {
                "status": "skipped",
                "reason": "recovery_schema_unavailable",
            }
        execution_targets = refresh_execution_capture_targets(storage=storage)
        target_rows = [
            dict(row)
            for row in recovery_store.list_active_capture_targets(limit=target_limit)
        ]
        capture_groups = _build_capture_groups(target_rows)
        if not capture_groups:
            return {
                "status": "idle",
                "active_target_count": 0,
                "capture_group_count": 0,
                "execution_targets": execution_targets,
                "quote_rows_saved": 0,
                "trade_rows_saved": 0,
                "quote_errors": [],
                "trade_errors": [],
            }

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
        quote_rows = [
            row
            for result in group_results
            for row in list(result.get("quote_rows") or [])
            if isinstance(row, Mapping)
        ]
        trade_rows = [
            row
            for result in group_results
            for row in list(result.get("trade_rows") or [])
            if isinstance(row, Mapping)
        ]
        quote_rows_saved = history_store.save_option_quote_event_rows(
            rows=[dict(row) for row in quote_rows]
        )
        trade_rows_saved = history_store.save_option_trade_event_rows(
            rows=[dict(row) for row in trade_rows]
        )
        return {
            "status": "ok",
            "active_target_count": len(target_rows),
            "capture_group_count": len(capture_groups),
            "execution_targets": execution_targets,
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


async def run_market_recorder_loop(args: argparse.Namespace) -> int:
    broker = AlpacaOptionStreamBroker()
    try:
        while True:
            iteration_started_at = asyncio.get_running_loop().time()
            summary = await run_market_recorder_iteration(
                db_target=args.db,
                broker=broker,
                quote_duration_seconds=args.quote_duration_seconds,
                trade_duration_seconds=args.trade_duration_seconds,
                target_limit=args.target_limit,
            )
            print(
                json.dumps(
                    {
                        "event": "market_recorder_iteration",
                        "timestamp": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
                        **summary,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                flush=True,
            )
            if args.once:
                return 0
            elapsed = asyncio.get_running_loop().time() - iteration_started_at
            await asyncio.sleep(max(float(args.poll_seconds) - elapsed, 0.0))
    finally:
        await broker.aclose()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Continuously record raw option quote and trade events for active recovery capture targets."
    )
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
    args = parse_args(argv)
    try:
        return asyncio.run(run_market_recorder_loop(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
