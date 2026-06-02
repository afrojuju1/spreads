from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, not_, or_, select

from core.storage.event_models import EventLogModel
from core.storage.factory import build_storage_context
from core.storage.models import OptionQuoteEventModel, OptionTradeEventModel
from core.storage.serializers import render_value

RAW_MARKET_EVENT_TOPICS = frozenset(
    {
        "market.quote.captured",
        "market.trade.captured",
    }
)

DEFAULT_OPTION_QUOTE_RETENTION_DAYS = 7
DEFAULT_OPTION_TRADE_RETENTION_DAYS = 30
DEFAULT_EVENT_LOG_MARKET_RETENTION_DAYS = 14
DEFAULT_EVENT_LOG_CONTROL_RETENTION_DAYS = 180
DEFAULT_RETENTION_BATCH_SIZE = 50_000
DEFAULT_RETENTION_MAX_BATCHES = 20


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def retention_defaults() -> dict[str, int]:
    return {
        "option_quote_days": _env_int(
            "SPREADS_OPTION_QUOTE_RETENTION_DAYS",
            DEFAULT_OPTION_QUOTE_RETENTION_DAYS,
        ),
        "option_trade_days": _env_int(
            "SPREADS_OPTION_TRADE_RETENTION_DAYS",
            DEFAULT_OPTION_TRADE_RETENTION_DAYS,
        ),
        "event_log_market_days": _env_int(
            "SPREADS_EVENT_LOG_MARKET_RETENTION_DAYS",
            DEFAULT_EVENT_LOG_MARKET_RETENTION_DAYS,
        ),
        "event_log_control_days": _env_int(
            "SPREADS_EVENT_LOG_CONTROL_RETENTION_DAYS",
            DEFAULT_EVENT_LOG_CONTROL_RETENTION_DAYS,
        ),
        "batch_size": _env_int(
            "SPREADS_RETENTION_BATCH_SIZE",
            DEFAULT_RETENTION_BATCH_SIZE,
        ),
        "max_batches": _env_int(
            "SPREADS_RETENTION_MAX_BATCHES",
            DEFAULT_RETENTION_MAX_BATCHES,
        ),
    }


def _validated_days(value: int, *, field_name: str) -> int:
    if value < 1:
        raise ValueError(f"{field_name} must be at least 1 day.")
    return value


def _cutoff(days: int, *, now: datetime) -> datetime:
    return now - timedelta(days=_validated_days(days, field_name="retention days"))


def _count(session: Any, model: Any, condition: Any) -> int:
    return int(
        session.scalar(select(func.count()).select_from(model).where(condition)) or 0
    )


def _delete_batches(
    session: Any,
    *,
    model: Any,
    id_column: Any,
    condition: Any,
    batch_size: int,
    max_batches: int,
) -> int:
    deleted = 0
    for _ in range(max_batches):
        subquery = select(id_column).where(condition).limit(batch_size)
        result = session.execute(delete(model).where(id_column.in_(subquery)))
        rowcount = int(result.rowcount or 0)
        deleted += rowcount
        if rowcount < batch_size:
            break
    return deleted


def _market_event_condition() -> Any:
    return or_(
        EventLogModel.event_class == "market_event",
        EventLogModel.topic.in_(tuple(RAW_MARKET_EVENT_TOPICS)),
    )


def prune_retained_data(
    *,
    db_target: str | None = None,
    dry_run: bool = True,
    option_quote_days: int | None = None,
    option_trade_days: int | None = None,
    event_log_market_days: int | None = None,
    event_log_control_days: int | None = None,
    batch_size: int | None = None,
    max_batches: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    defaults = retention_defaults()
    resolved_quote_days = _validated_days(
        option_quote_days or defaults["option_quote_days"],
        field_name="option_quote_days",
    )
    resolved_trade_days = _validated_days(
        option_trade_days or defaults["option_trade_days"],
        field_name="option_trade_days",
    )
    resolved_market_days = _validated_days(
        event_log_market_days or defaults["event_log_market_days"],
        field_name="event_log_market_days",
    )
    resolved_control_days = _validated_days(
        event_log_control_days or defaults["event_log_control_days"],
        field_name="event_log_control_days",
    )
    resolved_batch_size = max(int(batch_size or defaults["batch_size"]), 1)
    resolved_max_batches = max(int(max_batches or defaults["max_batches"]), 1)
    resolved_now = now or datetime.now(UTC)

    quote_cutoff = _cutoff(resolved_quote_days, now=resolved_now)
    trade_cutoff = _cutoff(resolved_trade_days, now=resolved_now)
    market_event_cutoff = _cutoff(resolved_market_days, now=resolved_now)
    control_event_cutoff = _cutoff(resolved_control_days, now=resolved_now)

    with build_storage_context(db_target) as storage:
        if not storage.history.schema_has_tables("option_quote_events"):
            raise RuntimeError("option_quote_events table is missing.")
        if not storage.history.schema_has_tables("option_trade_events"):
            raise RuntimeError("option_trade_events table is missing.")
        if not storage.events.schema_ready():
            raise RuntimeError("event_log table is missing.")

        market_condition = _market_event_condition()
        rows = [
            {
                "name": "option_quote_events",
                "retention_days": resolved_quote_days,
                "cutoff": quote_cutoff,
                "model": OptionQuoteEventModel,
                "id_column": OptionQuoteEventModel.quote_id,
                "condition": OptionQuoteEventModel.captured_at < quote_cutoff,
            },
            {
                "name": "option_trade_events",
                "retention_days": resolved_trade_days,
                "cutoff": trade_cutoff,
                "model": OptionTradeEventModel,
                "id_column": OptionTradeEventModel.trade_id,
                "condition": OptionTradeEventModel.captured_at < trade_cutoff,
            },
            {
                "name": "event_log_market_events",
                "retention_days": resolved_market_days,
                "cutoff": market_event_cutoff,
                "model": EventLogModel,
                "id_column": EventLogModel.event_id,
                "condition": market_condition
                & (EventLogModel.occurred_at < market_event_cutoff),
            },
            {
                "name": "event_log_control_events",
                "retention_days": resolved_control_days,
                "cutoff": control_event_cutoff,
                "model": EventLogModel,
                "id_column": EventLogModel.event_id,
                "condition": not_(market_condition)
                & (EventLogModel.occurred_at < control_event_cutoff),
            },
        ]

        table_results: list[dict[str, Any]] = []
        with storage.history.session_scope() as session:
            for row in rows:
                matching = _count(session, row["model"], row["condition"])
                deleted = (
                    0
                    if dry_run
                    else _delete_batches(
                        session,
                        model=row["model"],
                        id_column=row["id_column"],
                        condition=row["condition"],
                        batch_size=resolved_batch_size,
                        max_batches=resolved_max_batches,
                    )
                )
                table_results.append(
                    {
                        "name": row["name"],
                        "retention_days": row["retention_days"],
                        "cutoff": render_value(row["cutoff"]),
                        "matching_count": matching,
                        "deleted_count": deleted,
                    }
                )

    return {
        "status": "dry_run" if dry_run else "pruned",
        "dry_run": dry_run,
        "generated_at": render_value(resolved_now),
        "batch_size": resolved_batch_size,
        "max_batches": resolved_max_batches,
        "tables": table_results,
        "total_matching_count": sum(int(row["matching_count"]) for row in table_results),
        "total_deleted_count": sum(int(row["deleted_count"]) for row in table_results),
    }
