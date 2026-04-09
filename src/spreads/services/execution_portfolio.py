from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from spreads.services.risk_manager import assess_position_risk
from spreads.services.alpaca import create_alpaca_client_from_env
from spreads.services.scanner import LiveOptionQuote
from spreads.services.session_positions import sync_session_position_from_attempt
from spreads.storage.factory import build_execution_repository

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


def _fetch_latest_quotes(symbols: list[str]) -> tuple[dict[str, LiveOptionQuote], dict[str, str], str | None]:
    if not symbols:
        return {}, {}, None
    try:
        client = create_alpaca_client_from_env()
    except Exception as exc:
        return {}, {}, str(exc)
    quotes: dict[str, LiveOptionQuote] = {}
    sources: dict[str, str] = {}
    errors: list[str] = []

    for feed in QUOTE_FEEDS:
        pending = [symbol for symbol in symbols if symbol not in quotes]
        if not pending:
            break
        try:
            response = client.get_latest_option_quotes(pending, feed=feed)
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


def _mark_source_for_symbols(sources: dict[str, str], short_symbol: str, long_symbol: str) -> str | None:
    values = {
        sources[symbol]
        for symbol in (short_symbol, long_symbol)
        if symbol in sources and sources[symbol]
    }
    if not values:
        return None
    if len(values) == 1:
        return next(iter(values))
    return "mixed"


def _sum_or_none(values: list[float | None]) -> float | None:
    resolved = [value for value in values if value is not None]
    if not resolved:
        return None
    return round(sum(resolved), 2)


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


def build_session_execution_portfolio(
    *,
    db_target: str,
    session_id: str,
    executions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    execution_store = build_execution_repository(db_target)
    try:
        if not execution_store.positions_schema_ready():
            return _empty_portfolio()

        for attempt in executions or []:
            sync_session_position_from_attempt(
                execution_store=execution_store,
                attempt=attempt,
            )

        persisted_positions = [
            position.to_dict()
            for position in execution_store.list_session_positions(session_id=session_id)
        ]
        if not persisted_positions:
            return _empty_portfolio()

        quote_symbols: set[str] = set()
        for position in persisted_positions:
            if str(position.get("status") or "") in OPEN_POSITION_STATUSES:
                quote_symbols.update(
                    {
                        str(position["short_symbol"]),
                        str(position["long_symbol"]),
                    }
                )

        quotes, sources, mark_error = _fetch_latest_quotes(sorted(quote_symbols))
        retrieved_at = _utc_now()
        positions: list[dict[str, Any]] = []

        for persisted in persisted_positions:
            status = str(persisted.get("status") or "open")
            remaining_quantity = _coerce_float(persisted.get("remaining_quantity")) or 0.0
            entry_credit = _coerce_float(persisted.get("entry_credit"))
            short_symbol = str(persisted["short_symbol"])
            long_symbol = str(persisted["long_symbol"])
            short_quote = quotes.get(short_symbol)
            long_quote = quotes.get(long_symbol)
            mark_source = _mark_source_for_symbols(sources, short_symbol, long_symbol)
            spread_mark_midpoint = None
            spread_mark_close = _coerce_float(persisted.get("close_mark"))
            estimated_midpoint_pnl = None
            unrealized_pnl = _coerce_float(persisted.get("unrealized_pnl"))
            mark_timestamp = _as_text(persisted.get("close_marked_at"))

            if status in OPEN_POSITION_STATUSES and short_quote is not None and long_quote is not None and entry_credit is not None:
                spread_mark_midpoint = max(short_quote.midpoint - long_quote.midpoint, 0.0)
                spread_mark_close = max(short_quote.ask - long_quote.bid, 0.0)
                estimated_midpoint_pnl = (entry_credit - spread_mark_midpoint) * 100.0 * remaining_quantity
                unrealized_pnl = (entry_credit - spread_mark_close) * 100.0 * remaining_quantity
                mark_timestamp = _max_timestamp(short_quote.timestamp, long_quote.timestamp)
                execution_store.update_session_position(
                    session_position_id=str(persisted["session_position_id"]),
                    close_mark=_round_money(spread_mark_close),
                    close_mark_source=mark_source,
                    close_marked_at=mark_timestamp,
                    unrealized_pnl=_round_money(unrealized_pnl),
                    updated_at=retrieved_at,
                )
            elif status == "closed":
                unrealized_pnl = 0.0

            realized_pnl = _coerce_float(persisted.get("realized_pnl")) or 0.0
            position_risk = assess_position_risk(position=persisted)
            positions.append(
                {
                    "position_id": str(persisted["session_position_id"]),
                    "session_position_id": str(persisted["session_position_id"]),
                    "execution_attempt_id": str(persisted["open_execution_attempt_id"]),
                    "open_execution_attempt_id": str(persisted["open_execution_attempt_id"]),
                    "candidate_id": persisted.get("candidate_id"),
                    "underlying_symbol": str(persisted["underlying_symbol"]),
                    "strategy": str(persisted["strategy"]),
                    "short_symbol": short_symbol,
                    "long_symbol": long_symbol,
                    "expiration_date": _as_text(persisted.get("expiration_date")),
                    "position_status": status,
                    "broker_status": _as_text(persisted.get("last_broker_status")) or "unknown",
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
                    "mark_source": mark_source or _as_text(persisted.get("close_mark_source")),
                    "mark_timestamp": mark_timestamp,
                    "risk_status": str(position_risk["status"]),
                    "risk_note": _as_text(position_risk.get("note")),
                    "reconciliation_status": _as_text(persisted.get("reconciliation_status")),
                    "reconciliation_note": _as_text(persisted.get("reconciliation_note")),
                    "last_reconciled_at": _as_text(persisted.get("last_reconciled_at")),
                    "last_exit_evaluated_at": _as_text(persisted.get("last_exit_evaluated_at")),
                    "last_exit_reason": _as_text(persisted.get("last_exit_reason")),
                    "short_quote": None if short_quote is None else _quote_payload(short_quote, source=sources[short_symbol]),
                    "long_quote": None if long_quote is None else _quote_payload(long_quote, source=sources[long_symbol]),
                }
            )

        positions.sort(
            key=lambda item: (
                0 if item["position_status"] in OPEN_POSITION_STATUSES else 1,
                _as_text(item.get("closed_at")) or _as_text(item.get("opened_at")) or "",
            ),
            reverse=False,
        )

        quoted_position_count = sum(
            1
            for item in positions
            if item["position_status"] in OPEN_POSITION_STATUSES and item.get("spread_mark_close") is not None
        )
        open_positions = [item for item in positions if item["position_status"] == "open"]
        partial_positions = [item for item in positions if item["position_status"] == "partial_close"]
        closed_positions = [item for item in positions if item["position_status"] == "closed"]
        mismatch_positions = [
            item for item in positions if _as_text(item.get("reconciliation_status")) == "mismatch"
        ]
        mark_sources = {str(item["mark_source"]) for item in positions if item.get("mark_source")}
        realized_total = _sum_or_none([_coerce_float(item.get("realized_pnl")) for item in positions])
        unrealized_total = _sum_or_none([_coerce_float(item.get("unrealized_pnl")) for item in positions])
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
                    sum(_coerce_float(item.get("opened_quantity")) or 0.0 for item in positions),
                    2,
                ),
                "opened_contract_count": round(
                    sum(_coerce_float(item.get("opened_quantity")) or 0.0 for item in positions),
                    2,
                ),
                "remaining_contract_count": round(
                    sum(_coerce_float(item.get("remaining_quantity")) or 0.0 for item in positions),
                    2,
                ),
                "entry_notional_total": _sum_or_none([_coerce_float(item.get("entry_notional")) for item in positions]),
                "max_profit_total": _sum_or_none([_coerce_float(item.get("max_profit")) for item in positions]),
                "max_loss_total": _sum_or_none([_coerce_float(item.get("max_loss")) for item in positions]),
                "realized_pnl_total": realized_total,
                "unrealized_pnl_total": unrealized_total,
                "net_pnl_total": net_total,
                "estimated_midpoint_pnl_total": _sum_or_none(
                    [_coerce_float(item.get("estimated_midpoint_pnl")) for item in positions]
                ),
                "estimated_close_pnl_total": unrealized_total,
                "quoted_position_count": quoted_position_count,
                "unquoted_position_count": len(open_positions) + len(partial_positions) - quoted_position_count,
                "mismatch_position_count": len(mismatch_positions),
                "mark_source": None if not mark_sources else (next(iter(mark_sources)) if len(mark_sources) == 1 else "mixed"),
                "mark_error": mark_error,
                "retrieved_at": retrieved_at,
            },
            "positions": positions,
        }
    finally:
        execution_store.close()
