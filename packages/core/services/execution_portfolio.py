from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from core.db.decorators import with_storage
from core.services.alpaca import create_alpaca_client_from_env
from core.services.option_structures import (
    net_premium_kind,
    normalize_strategy_family,
    structure_quote_snapshot,
    position_legs,
    primary_short_long_symbols,
    unique_leg_symbols,
)
from core.services.positions import enrich_position_row
from core.services.risk_manager import assess_position_risk
from core.services.runtime_identity import parse_live_run_scope_id
from core.services.scanner import LiveOptionQuote

QUOTE_FEEDS = ("opra", "indicative")
OPEN_POSITION_STATUSES = {"open", "partial_close"}


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


def _round_money(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 2)


def _max_timestamp(*values: str | None) -> str | None:
    best_value: str | None = None
    best_timestamp: datetime | None = None
    for value in values:
        text = _as_text(value)
        if text is None:
            continue
        normalized = text.replace("Z", "+00:00") if text.endswith("Z") else text
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        parsed = parsed.astimezone(UTC)
        if best_timestamp is None or parsed > best_timestamp:
            best_timestamp = parsed
            best_value = parsed.isoformat(timespec="seconds").replace("+00:00", "Z")
    return best_value


def _quote_payload(quote: LiveOptionQuote, *, source: str) -> dict[str, Any]:
    return {
        "symbol": quote.symbol,
        "bid": quote.bid,
        "ask": quote.ask,
        "midpoint": quote.midpoint,
        "timestamp": quote.timestamp,
        "source": source,
    }


def fetch_latest_option_quotes(
    symbols: list[str],
    *,
    client: Any | None = None,
    feeds: tuple[str, ...] = QUOTE_FEEDS,
) -> tuple[dict[str, LiveOptionQuote], dict[str, str], str | None]:
    if not symbols:
        return {}, {}, None
    resolved_client = client
    if resolved_client is None:
        try:
            resolved_client = create_alpaca_client_from_env()
        except Exception as exc:
            return {}, {}, str(exc)
    quotes: dict[str, LiveOptionQuote] = {}
    sources: dict[str, str] = {}
    errors: list[str] = []

    for feed in feeds:
        pending = [symbol for symbol in symbols if symbol not in quotes]
        if not pending:
            break
        try:
            response = resolved_client.get_latest_option_quotes(pending, feed=feed)
        except Exception as exc:
            errors.append(f"{feed}: {exc}")
            continue
        for symbol, quote in response.items():
            quotes[symbol] = quote
            sources[symbol] = feed

    error_text = None
    if errors and len(quotes) < len(symbols):
        error_text = "; ".join(errors)
    return quotes, sources, error_text


def resolve_quote_source_for_symbols(
    sources: dict[str, str],
    *symbols: str,
) -> str | None:
    values = {
        sources[symbol] for symbol in symbols if symbol in sources and sources[symbol]
    }
    if not values:
        return None
    if len(values) == 1:
        return next(iter(values))
    return "mixed"


def build_vertical_spread_quote_snapshot(
    *,
    short_symbol: str,
    long_symbol: str,
    strategy_family: str,
    client: Any | None = None,
    feeds: tuple[str, ...] = QUOTE_FEEDS,
) -> tuple[dict[str, Any] | None, str | None]:
    return build_structure_quote_snapshot(
        legs=[
            {"symbol": short_symbol, "role": "short"},
            {"symbol": long_symbol, "role": "long"},
        ],
        strategy_family=strategy_family,
        client=client,
        feeds=feeds,
    )


def build_structure_quote_snapshot(
    *,
    legs: list[dict[str, Any]],
    strategy_family: str,
    client: Any | None = None,
    feeds: tuple[str, ...] = QUOTE_FEEDS,
) -> tuple[dict[str, Any] | None, str | None]:
    quote_symbols = unique_leg_symbols(legs)
    quotes, sources, error_text = fetch_latest_option_quotes(
        quote_symbols,
        client=client,
        feeds=feeds,
    )
    snapshot = structure_quote_snapshot(
        legs=legs,
        strategy_family=strategy_family,
        quotes_by_symbol=quotes,
        sources_by_symbol=sources,
    )
    if snapshot is None:
        return None, error_text
    return snapshot, error_text


def build_credit_spread_quote_snapshot(
    *,
    short_symbol: str,
    long_symbol: str,
    client: Any | None = None,
    feeds: tuple[str, ...] = QUOTE_FEEDS,
) -> tuple[dict[str, Any] | None, str | None]:
    return build_vertical_spread_quote_snapshot(
        short_symbol=short_symbol,
        long_symbol=long_symbol,
        strategy_family="call_credit_spread",
        client=client,
        feeds=feeds,
    )


def _sum_or_none(values: list[float | None]) -> float | None:
    resolved = [value for value in values if value is not None]
    if not resolved:
        return None
    return round(sum(resolved), 2)


def _position_matches_session_id(position: dict[str, Any], session_id: str) -> bool:
    resolved = parse_live_run_scope_id(session_id)
    if resolved is None:
        return False
    return (
        str(position.get("pipeline_id")) == f"pipeline:{resolved['label']}"
        and str(position.get("market_date_opened") or position.get("market_date"))
        == resolved["market_date"]
    )


def _empty_portfolio() -> dict[str, Any]:
    return {
        "summary": {
            "position_count": 0,
            "open_position_count": 0,
            "partial_close_position_count": 0,
            "closed_position_count": 0,
            "filled_contract_count": 0.0,
            "opened_contract_count": 0.0,
            "remaining_contract_count": 0.0,
            "entry_notional_total": None,
            "max_profit_total": None,
            "max_loss_total": None,
            "realized_pnl_total": None,
            "unrealized_pnl_total": None,
            "net_pnl_total": None,
            "estimated_midpoint_pnl_total": None,
            "estimated_close_pnl_total": None,
            "quoted_position_count": 0,
            "unquoted_position_count": 0,
            "mismatch_position_count": 0,
            "mark_source": None,
            "mark_error": None,
            "retrieved_at": _utc_now(),
        },
        "positions": [],
    }


@with_storage()
def refresh_session_position_marks(
    *,
    db_target: str,
    session_ids: list[str] | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    execution_store = storage.execution
    if not execution_store.portfolio_schema_ready():
        return {
            "status": "skipped",
            "reason": "positions_schema_unavailable",
        }

    open_positions = [
        enrich_position_row(dict(position))
        for position in execution_store.list_positions(
            statuses=sorted(OPEN_POSITION_STATUSES),
            limit=500,
        )
    ]
    if session_ids is not None:
        open_positions = [
            position
            for position in open_positions
            if any(
                _position_matches_session_id(position, str(session_id))
                for session_id in session_ids
            )
        ]
    if not open_positions:
        return {
            "status": "ok",
            "position_count": 0,
            "updated_position_count": 0,
            "quoted_position_count": 0,
            "unquoted_position_count": 0,
            "mark_error": None,
            "retrieved_at": _utc_now(),
        }

    quote_symbols = sorted(
        {
            symbol
            for position in open_positions
            for symbol in unique_leg_symbols(position_legs(position))
        }
    )
    quotes, sources, mark_error = fetch_latest_option_quotes(quote_symbols)
    retrieved_at = _utc_now()
    updated_position_count = 0
    quoted_position_count = 0

    for position in open_positions:
        remaining_quantity = _coerce_float(position.get("remaining_quantity")) or 0.0
        entry_value = _coerce_float(position.get("entry_value")) or _coerce_float(
            position.get("entry_credit")
        )
        strategy_family = str(position.get("strategy") or position.get("strategy_family") or "")
        legs = position_legs(position)
        live_snapshot = structure_quote_snapshot(
            legs=legs,
            strategy_family=strategy_family,
            quotes_by_symbol=quotes,
            sources_by_symbol=sources,
        )
        if remaining_quantity <= 0 or entry_value is None or live_snapshot is None:
            continue
        premium_kind = net_premium_kind(strategy_family)
        spread_mark_close = _coerce_float(live_snapshot.get("close_mark"))
        if spread_mark_close is None:
            continue
        if premium_kind == "debit":
            unrealized_pnl = (spread_mark_close - entry_value) * 100.0 * remaining_quantity
        else:
            unrealized_pnl = (entry_value - spread_mark_close) * 100.0 * remaining_quantity
        execution_store.update_position(
            position_id=str(position["position_id"]),
            close_mark=_round_money(spread_mark_close),
            close_mark_source=_as_text(live_snapshot.get("quote_source")),
            close_marked_at=_as_text(live_snapshot.get("captured_at")),
            unrealized_pnl=_round_money(unrealized_pnl),
            updated_at=retrieved_at,
        )
        updated_position_count += 1
        quoted_position_count += 1

    status = "ok"
    if mark_error and quoted_position_count < len(open_positions):
        status = "degraded"
    return {
        "status": status,
        "position_count": len(open_positions),
        "updated_position_count": updated_position_count,
        "quoted_position_count": quoted_position_count,
        "unquoted_position_count": len(open_positions) - quoted_position_count,
        "mark_error": mark_error,
        "retrieved_at": retrieved_at,
    }


@with_storage()
def build_session_execution_portfolio(
    *,
    db_target: str,
    session_id: str,
    executions: list[dict[str, Any]] | None = None,
    execution_store: Any | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    def _build_portfolio(resolved_execution_store: Any) -> dict[str, Any]:
        if not resolved_execution_store.portfolio_schema_ready():
            return _empty_portfolio()
        resolved_scope = parse_live_run_scope_id(session_id)
        if resolved_scope is None:
            return _empty_portfolio()

        persisted_positions = [
            enrich_position_row(dict(position))
            for position in resolved_execution_store.list_positions(
                pipeline_id=f"pipeline:{resolved_scope['label']}",
                market_date=resolved_scope["market_date"],
            )
        ]
        if not persisted_positions:
            return _empty_portfolio()
        retrieved_at = _utc_now()
        positions: list[dict[str, Any]] = []

        for persisted in persisted_positions:
            status = str(persisted.get("status") or "open")
            remaining_quantity = (
                _coerce_float(persisted.get("remaining_quantity")) or 0.0
            )
            entry_credit = _coerce_float(persisted.get("entry_credit"))
            strategy_family = str(
                persisted.get("strategy") or persisted.get("strategy_family") or ""
            )
            short_symbol = str(persisted["short_symbol"])
            long_symbol = str(persisted["long_symbol"])
            spread_mark_midpoint = None
            spread_mark_close = _coerce_float(persisted.get("close_mark"))
            estimated_midpoint_pnl = None
            unrealized_pnl = _coerce_float(persisted.get("unrealized_pnl"))
            mark_timestamp = _as_text(persisted.get("close_marked_at"))
            mark_source = _as_text(persisted.get("close_mark_source"))

            if (
                status in OPEN_POSITION_STATUSES
                and entry_credit is not None
                and spread_mark_close is not None
            ):
                if net_premium_kind(strategy_family) == "debit":
                    unrealized_pnl = (
                        (spread_mark_close - entry_credit)
                        * 100.0
                        * remaining_quantity
                    )
                else:
                    unrealized_pnl = (
                        (entry_credit - spread_mark_close)
                        * 100.0
                        * remaining_quantity
                    )
            elif status == "closed":
                unrealized_pnl = 0.0

            realized_pnl = _coerce_float(persisted.get("realized_pnl")) or 0.0
            position_risk = assess_position_risk(position=persisted)
            positions.append(
                {
                    "position_id": str(persisted["position_id"]),
                    "execution_attempt_id": str(persisted["open_execution_attempt_id"]),
                    "open_execution_attempt_id": str(
                        persisted["open_execution_attempt_id"]
                    ),
                    "candidate_id": persisted.get("candidate_id"),
                    "underlying_symbol": str(persisted["underlying_symbol"]),
                    "strategy": str(persisted["strategy"]),
                    "short_symbol": short_symbol,
                    "long_symbol": long_symbol,
                    "expiration_date": _as_text(persisted.get("expiration_date")),
                    "position_status": status,
                    "broker_status": _as_text(persisted.get("last_broker_status"))
                    or "unknown",
                    "requested_quantity": persisted.get("requested_quantity"),
                    "opened_quantity": persisted.get("opened_quantity"),
                    "filled_quantity": persisted.get("opened_quantity"),
                    "remaining_quantity": persisted.get("remaining_quantity"),
                    "closed_quantity": _round_money(
                        (_coerce_float(persisted.get("opened_quantity")) or 0.0)
                        - (_coerce_float(persisted.get("remaining_quantity")) or 0.0)
                    ),
                    "entry_credit": _coerce_float(persisted.get("entry_credit")),
                    "entry_notional": _coerce_float(persisted.get("entry_notional")),
                    "width": _coerce_float(persisted.get("width")),
                    "max_profit": _coerce_float(persisted.get("max_profit")),
                    "max_loss": _coerce_float(persisted.get("max_loss")),
                    "opened_at": _as_text(persisted.get("opened_at")),
                    "completed_at": _as_text(persisted.get("closed_at")),
                    "closed_at": _as_text(persisted.get("closed_at")),
                    "realized_pnl": _round_money(realized_pnl),
                    "unrealized_pnl": _round_money(unrealized_pnl),
                    "net_pnl": _round_money(realized_pnl + (unrealized_pnl or 0.0)),
                    "spread_mark_midpoint": _round_money(spread_mark_midpoint),
                    "spread_mark_close": _round_money(spread_mark_close),
                    "estimated_midpoint_pnl": _round_money(estimated_midpoint_pnl),
                    "estimated_close_pnl": _round_money(unrealized_pnl),
                    "mark_source": mark_source
                    or _as_text(persisted.get("close_mark_source")),
                    "mark_timestamp": mark_timestamp,
                    "risk_status": str(position_risk["status"]),
                    "risk_note": _as_text(position_risk.get("note")),
                    "reconciliation_status": _as_text(
                        persisted.get("reconciliation_status")
                    ),
                    "reconciliation_note": _as_text(
                        persisted.get("reconciliation_note")
                    ),
                    "last_reconciled_at": _as_text(persisted.get("last_reconciled_at")),
                    "last_exit_evaluated_at": _as_text(
                        persisted.get("last_exit_evaluated_at")
                    ),
                    "last_exit_reason": _as_text(persisted.get("last_exit_reason")),
                    "short_quote": None,
                    "long_quote": None,
                }
            )

        positions.sort(
            key=lambda item: (
                0 if item["position_status"] in OPEN_POSITION_STATUSES else 1,
                _as_text(item.get("closed_at"))
                or _as_text(item.get("opened_at"))
                or "",
            ),
            reverse=False,
        )

        quoted_position_count = sum(
            1
            for item in positions
            if item["position_status"] in OPEN_POSITION_STATUSES
            and item.get("spread_mark_close") is not None
        )
        open_positions = [
            item for item in positions if item["position_status"] == "open"
        ]
        partial_positions = [
            item for item in positions if item["position_status"] == "partial_close"
        ]
        closed_positions = [
            item for item in positions if item["position_status"] == "closed"
        ]
        mismatch_positions = [
            item
            for item in positions
            if _as_text(item.get("reconciliation_status")) == "mismatch"
        ]
        mark_sources = {
            str(item["mark_source"]) for item in positions if item.get("mark_source")
        }
        realized_total = _sum_or_none(
            [_coerce_float(item.get("realized_pnl")) for item in positions]
        )
        unrealized_total = _sum_or_none(
            [_coerce_float(item.get("unrealized_pnl")) for item in positions]
        )
        net_total = None
        if realized_total is not None or unrealized_total is not None:
            net_total = round((realized_total or 0.0) + (unrealized_total or 0.0), 2)

        return {
            "summary": {
                "position_count": len(positions),
                "open_position_count": len(open_positions),
                "partial_close_position_count": len(partial_positions),
                "closed_position_count": len(closed_positions),
                "filled_contract_count": round(
                    sum(
                        _coerce_float(item.get("opened_quantity")) or 0.0
                        for item in positions
                    ),
                    2,
                ),
                "opened_contract_count": round(
                    sum(
                        _coerce_float(item.get("opened_quantity")) or 0.0
                        for item in positions
                    ),
                    2,
                ),
                "remaining_contract_count": round(
                    sum(
                        _coerce_float(item.get("remaining_quantity")) or 0.0
                        for item in positions
                    ),
                    2,
                ),
                "entry_notional_total": _sum_or_none(
                    [_coerce_float(item.get("entry_notional")) for item in positions]
                ),
                "max_profit_total": _sum_or_none(
                    [_coerce_float(item.get("max_profit")) for item in positions]
                ),
                "max_loss_total": _sum_or_none(
                    [_coerce_float(item.get("max_loss")) for item in positions]
                ),
                "realized_pnl_total": realized_total,
                "unrealized_pnl_total": unrealized_total,
                "net_pnl_total": net_total,
                "estimated_midpoint_pnl_total": _sum_or_none(
                    [
                        _coerce_float(item.get("estimated_midpoint_pnl"))
                        for item in positions
                    ]
                ),
                "estimated_close_pnl_total": unrealized_total,
                "quoted_position_count": quoted_position_count,
                "unquoted_position_count": len(open_positions)
                + len(partial_positions)
                - quoted_position_count,
                "mismatch_position_count": len(mismatch_positions),
                "mark_source": None
                if not mark_sources
                else (next(iter(mark_sources)) if len(mark_sources) == 1 else "mixed"),
                "mark_error": None,
                "retrieved_at": retrieved_at,
            },
            "positions": positions,
        }

    resolved_execution_store = (
        execution_store if execution_store is not None else storage.execution
    )
    return _build_portfolio(resolved_execution_store)
