from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import delete, func, select

from core.storage.base import RepositoryBase
from core.storage.market_tick_models import OptionQuoteTickModel, OptionTradeTickModel
from core.storage.records import (
    OptionQuoteTickRecord,
    OptionTradeTickRecord,
)
from core.storage.serializers import parse_datetime, render_value


class MarketTickRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables("option_quote_ticks")

    def table_counts(self) -> dict[str, int]:
        with self.session_factory() as session:
            counts = {
                "option_quote_ticks": int(session.scalar(select(func.count()).select_from(OptionQuoteTickModel)) or 0),
            }
            if self.schema_has_tables("option_trade_ticks"):
                counts["option_trade_ticks"] = int(session.scalar(select(func.count()).select_from(OptionTradeTickModel)) or 0)
            return counts

    def truncate_all(self) -> None:
        with self.session_scope() as session:
            if self.schema_has_tables("option_trade_ticks"):
                session.execute(delete(OptionTradeTickModel))
            session.execute(delete(OptionQuoteTickModel))

    def list_session_quote_ticks(
        self,
        *,
        session_date: str,
        label: str,
    ) -> list[OptionQuoteTickRecord]:
        session_start, session_end = session_bounds(session_date)

        statement = (
            select(OptionQuoteTickModel)
            .where(OptionQuoteTickModel.captured_at >= session_start)
            .where(OptionQuoteTickModel.captured_at < session_end)
            .where(OptionQuoteTickModel.label == label)
            .order_by(OptionQuoteTickModel.quote_tick_id.asc())
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def session_quote_overview(
        self,
        *,
        session_date: str,
        label: str,
    ) -> dict[str, Any]:
        session_start, session_end = session_bounds(session_date)
        statement = (
            select(
                func.count(OptionQuoteTickModel.quote_tick_id),
                func.min(OptionQuoteTickModel.captured_at),
                func.max(OptionQuoteTickModel.captured_at),
                func.count(func.distinct(OptionQuoteTickModel.option_symbol)),
            )
            .where(OptionQuoteTickModel.captured_at >= session_start)
            .where(OptionQuoteTickModel.captured_at < session_end)
            .where(OptionQuoteTickModel.label == label)
        )
        with self.session_factory() as session:
            row = session.execute(statement).one()
        return {
            "quote_tick_count": int(row[0] or 0),
            "first_quote_at": render_value(row[1]),
            "last_quote_at": render_value(row[2]),
            "tracked_leg_count": int(row[3] or 0),
        }

    def list_session_quote_coverage(
        self,
        *,
        session_date: str,
        label: str,
    ) -> list[dict[str, Any]]:
        session_start, session_end = session_bounds(session_date)
        statement = (
            select(
                OptionQuoteTickModel.underlying_symbol.label("underlying_symbol"),
                OptionQuoteTickModel.strategy.label("strategy"),
                func.count(OptionQuoteTickModel.quote_tick_id).label("quote_ticks"),
                func.count(func.distinct(OptionQuoteTickModel.option_symbol)).label("unique_legs"),
                func.min(OptionQuoteTickModel.captured_at).label("first_quote_at"),
                func.max(OptionQuoteTickModel.captured_at).label("last_quote_at"),
            )
            .where(OptionQuoteTickModel.captured_at >= session_start)
            .where(OptionQuoteTickModel.captured_at < session_end)
            .where(OptionQuoteTickModel.label == label)
            .group_by(
                OptionQuoteTickModel.underlying_symbol,
                OptionQuoteTickModel.strategy,
            )
            .order_by(
                OptionQuoteTickModel.underlying_symbol.asc(),
                OptionQuoteTickModel.strategy.asc(),
            )
        )
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        return [
            {
                "underlying_symbol": str(render_value(row.underlying_symbol) or "UNKNOWN"),
                "strategy": str(render_value(row.strategy) or "unknown"),
                "quote_ticks": int(row.quote_ticks or 0),
                "unique_legs": int(row.unique_legs or 0),
                "first_quote_at": render_value(row.first_quote_at),
                "last_quote_at": render_value(row.last_quote_at),
            }
            for row in rows
        ]

    def list_session_quote_leg_summaries(
        self,
        *,
        session_date: str,
        label: str,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        session_start, session_end = session_bounds(session_date)
        statement = (
            select(
                OptionQuoteTickModel.option_symbol.label("option_symbol"),
                OptionQuoteTickModel.underlying_symbol.label("underlying_symbol"),
                OptionQuoteTickModel.strategy.label("strategy"),
                OptionQuoteTickModel.leg_role.label("leg_role"),
                func.count(OptionQuoteTickModel.quote_tick_id).label("tick_count"),
                func.min(OptionQuoteTickModel.captured_at).label("first_quote_at"),
                func.max(OptionQuoteTickModel.captured_at).label("last_quote_at"),
                func.min(OptionQuoteTickModel.midpoint).label("midpoint_min"),
                func.max(OptionQuoteTickModel.midpoint).label("midpoint_max"),
            )
            .where(OptionQuoteTickModel.captured_at >= session_start)
            .where(OptionQuoteTickModel.captured_at < session_end)
            .where(OptionQuoteTickModel.label == label)
            .group_by(
                OptionQuoteTickModel.option_symbol,
                OptionQuoteTickModel.underlying_symbol,
                OptionQuoteTickModel.strategy,
                OptionQuoteTickModel.leg_role,
            )
            .order_by(
                func.count(OptionQuoteTickModel.quote_tick_id).desc(),
                OptionQuoteTickModel.option_symbol.asc(),
            )
            .limit(max(int(limit), 1))
        )
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        return [
            {
                "option_symbol": str(render_value(row.option_symbol) or ""),
                "underlying_symbol": str(render_value(row.underlying_symbol) or "UNKNOWN"),
                "strategy": str(render_value(row.strategy) or "unknown"),
                "leg_role": str(render_value(row.leg_role) or "unknown"),
                "tick_count": int(row.tick_count or 0),
                "first_quote_at": render_value(row.first_quote_at),
                "last_quote_at": render_value(row.last_quote_at),
                "midpoint_min": float(row.midpoint_min or 0.0),
                "midpoint_max": float(row.midpoint_max or 0.0),
            }
            for row in rows
        ]

    def list_option_quote_ticks_window(
        self,
        *,
        option_symbols: list[str],
        captured_from: str | datetime,
        captured_to: str | datetime | None = None,
        label: str | None = None,
        profile: str | None = None,
        sources: str | list[str] | None = None,
    ) -> list[OptionQuoteTickRecord]:
        normalized_symbols = sorted({str(symbol or "").strip() for symbol in option_symbols if str(symbol or "").strip()})
        if not normalized_symbols:
            return []
        captured_from_dt = parse_datetime(captured_from)
        captured_to_dt = parse_datetime(captured_to)
        if captured_from_dt is None:
            return []
        if captured_to_dt is not None and captured_from_dt >= captured_to_dt:
            return []
        normalized_sources = []
        if isinstance(sources, str):
            normalized_sources = [sources.strip()] if sources.strip() else []
        elif isinstance(sources, list):
            normalized_sources = [str(source or "").strip() for source in sources if str(source or "").strip()]

        statement = (
            select(OptionQuoteTickModel)
            .where(OptionQuoteTickModel.option_symbol.in_(normalized_symbols))
            .where(OptionQuoteTickModel.captured_at >= captured_from_dt)
            .order_by(
                OptionQuoteTickModel.captured_at.asc(),
                OptionQuoteTickModel.quote_tick_id.asc(),
            )
        )
        if captured_to_dt is not None:
            statement = statement.where(OptionQuoteTickModel.captured_at < captured_to_dt)
        if label is not None:
            statement = statement.where(OptionQuoteTickModel.label == label)
        if profile is not None:
            statement = statement.where(OptionQuoteTickModel.profile == profile)
        if normalized_sources:
            statement = statement.where(OptionQuoteTickModel.source.in_(normalized_sources))
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def save_option_quote_ticks(
        self,
        *,
        cycle_id: str,
        label: str,
        profile: str,
        quotes: list[dict[str, Any]],
    ) -> int:
        if not quotes:
            return 0

        with self.session_scope() as session:
            session.add_all(
                [
                    OptionQuoteTickModel(
                        cycle_id=cycle_id,
                        captured_at=parse_datetime(quote["captured_at"]),
                        label=label,
                        underlying_symbol=quote.get("underlying_symbol"),
                        strategy=quote.get("strategy"),
                        profile=profile,
                        option_symbol=quote["option_symbol"],
                        leg_role=quote["leg_role"],
                        bid=quote["bid"],
                        ask=quote["ask"],
                        midpoint=quote["midpoint"],
                        bid_size=quote["bid_size"],
                        ask_size=quote["ask_size"],
                        source_timestamp=parse_datetime(quote.get("source_timestamp")),
                        source=quote.get("source", "alpaca_websocket"),
                    )
                    for quote in quotes
                ]
            )
        return len(quotes)

    def save_option_quote_tick_rows(
        self,
        *,
        rows: list[dict[str, Any]],
    ) -> int:
        if not rows:
            return 0

        with self.session_scope() as session:
            session.add_all(
                [
                    OptionQuoteTickModel(
                        cycle_id=str(row["cycle_id"]),
                        captured_at=parse_datetime(row["captured_at"]),
                        label=str(row["label"]),
                        underlying_symbol=row.get("underlying_symbol"),
                        strategy=row.get("strategy"),
                        profile=row.get("profile"),
                        option_symbol=str(row["option_symbol"]),
                        leg_role=str(row.get("leg_role") or "unknown"),
                        bid=float(row["bid"]),
                        ask=float(row["ask"]),
                        midpoint=float(row["midpoint"]),
                        bid_size=int(row["bid_size"]),
                        ask_size=int(row["ask_size"]),
                        source_timestamp=parse_datetime(row.get("source_timestamp")),
                        source=str(row.get("source") or "alpaca_websocket"),
                    )
                    for row in rows
                ]
            )
        return len(rows)

    def summarize_option_quote_window(
        self,
        *,
        option_symbols: list[str],
        captured_from: str | datetime,
        captured_to: str | datetime,
    ) -> dict[str, dict[str, Any]]:
        if not option_symbols:
            return {}
        captured_from_dt = parse_datetime(captured_from)
        captured_to_dt = parse_datetime(captured_to)
        if captured_from_dt is None or captured_to_dt is None or captured_from_dt >= captured_to_dt:
            return {}

        statement = (
            select(
                OptionQuoteTickModel.option_symbol,
                func.count(OptionQuoteTickModel.quote_tick_id),
                func.max(OptionQuoteTickModel.captured_at),
            )
            .where(OptionQuoteTickModel.option_symbol.in_(option_symbols))
            .where(OptionQuoteTickModel.captured_at >= captured_from_dt)
            .where(OptionQuoteTickModel.captured_at < captured_to_dt)
            .group_by(OptionQuoteTickModel.option_symbol)
        )
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        return {
            str(option_symbol): {
                "tick_count": int(tick_count or 0),
                "last_captured_at": render_value(last_captured_at),
            }
            for option_symbol, tick_count, last_captured_at in rows
        }

    def list_session_trade_ticks(
        self,
        *,
        session_date: str,
        label: str,
    ) -> list[OptionTradeTickRecord]:
        session_start, session_end = session_bounds(session_date)

        statement = (
            select(OptionTradeTickModel)
            .where(OptionTradeTickModel.captured_at >= session_start)
            .where(OptionTradeTickModel.captured_at < session_end)
            .where(OptionTradeTickModel.label == label)
            .order_by(OptionTradeTickModel.trade_tick_id.asc())
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_option_trade_ticks_window(
        self,
        *,
        option_symbols: list[str],
        captured_from: str | datetime,
        captured_to: str | datetime | None = None,
        label: str | None = None,
        profile: str | None = None,
        sources: str | list[str] | None = None,
    ) -> list[OptionTradeTickRecord]:
        normalized_symbols = sorted({str(symbol or "").strip() for symbol in option_symbols if str(symbol or "").strip()})
        if not normalized_symbols:
            return []
        captured_from_dt = parse_datetime(captured_from)
        captured_to_dt = parse_datetime(captured_to)
        if captured_from_dt is None:
            return []
        if captured_to_dt is not None and captured_from_dt >= captured_to_dt:
            return []
        normalized_sources = []
        if isinstance(sources, str):
            normalized_sources = [sources.strip()] if sources.strip() else []
        elif isinstance(sources, list):
            normalized_sources = [str(source or "").strip() for source in sources if str(source or "").strip()]

        statement = (
            select(OptionTradeTickModel)
            .where(OptionTradeTickModel.option_symbol.in_(normalized_symbols))
            .where(OptionTradeTickModel.captured_at >= captured_from_dt)
            .order_by(
                OptionTradeTickModel.captured_at.asc(),
                OptionTradeTickModel.trade_tick_id.asc(),
            )
        )
        if captured_to_dt is not None:
            statement = statement.where(OptionTradeTickModel.captured_at < captured_to_dt)
        if label is not None:
            statement = statement.where(OptionTradeTickModel.label == label)
        if profile is not None:
            statement = statement.where(OptionTradeTickModel.profile == profile)
        if normalized_sources:
            statement = statement.where(OptionTradeTickModel.source.in_(normalized_sources))
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def summarize_scoreable_trade_flow(
        self,
        *,
        label: str,
        underlyings: list[str],
        captured_from: str | datetime,
        captured_to: str | datetime,
    ) -> dict[str, dict[str, Any]]:
        if not underlyings:
            return {}
        captured_from_dt = parse_datetime(captured_from)
        captured_to_dt = parse_datetime(captured_to)
        if captured_from_dt is None or captured_to_dt is None or captured_from_dt >= captured_to_dt:
            return {}

        statement = (
            select(
                OptionTradeTickModel.underlying_symbol,
                func.count(OptionTradeTickModel.trade_tick_id),
                func.count(func.distinct(OptionTradeTickModel.option_symbol)),
                func.coalesce(func.sum(OptionTradeTickModel.premium), 0.0),
            )
            .where(OptionTradeTickModel.label == label)
            .where(OptionTradeTickModel.included_in_score.is_(True))
            .where(OptionTradeTickModel.underlying_symbol.in_(underlyings))
            .where(OptionTradeTickModel.captured_at >= captured_from_dt)
            .where(OptionTradeTickModel.captured_at < captured_to_dt)
            .group_by(OptionTradeTickModel.underlying_symbol)
        )
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        duration_minutes = max((captured_to_dt - captured_from_dt).total_seconds() / 60.0, 1.0 / 60.0)
        payload: dict[str, dict[str, Any]] = {}
        for underlying_symbol, trade_count, contract_count, premium in rows:
            symbol = str(underlying_symbol or "").strip()
            if not symbol:
                continue
            premium_value = float(premium or 0.0)
            trade_count_value = int(trade_count or 0)
            contract_count_value = int(contract_count or 0)
            payload[symbol] = {
                "duration_minutes": round(duration_minutes, 4),
                "scoreable_trade_count": trade_count_value,
                "scoreable_contract_count": contract_count_value,
                "scoreable_premium": round(premium_value, 4),
                "trade_rate_per_minute": round(trade_count_value / duration_minutes, 4),
                "contract_rate_per_minute": round(contract_count_value / duration_minutes, 4),
                "premium_rate_per_minute": round(premium_value / duration_minutes, 4),
            }
        return payload

    def latest_trade_session_date_before(
        self,
        *,
        label: str,
        before_session_date: str,
    ) -> str | None:
        current_session_start, _ = session_bounds(before_session_date)
        statement = (
            select(func.max(OptionTradeTickModel.captured_at))
            .where(OptionTradeTickModel.label == label)
            .where(OptionTradeTickModel.captured_at < current_session_start)
        )
        with self.session_factory() as session:
            latest = session.scalar(statement)
        if latest is None:
            return None
        latest_dt = latest if latest.tzinfo else latest.replace(tzinfo=timezone.utc)
        return latest_dt.astimezone(NEW_YORK).date().isoformat()

    def save_option_trade_ticks(
        self,
        *,
        cycle_id: str,
        label: str,
        profile: str,
        trades: list[dict[str, Any]],
    ) -> int:
        if not trades:
            return 0

        with self.session_scope() as session:
            session.add_all(
                [
                    OptionTradeTickModel(
                        cycle_id=cycle_id,
                        captured_at=parse_datetime(trade["captured_at"]),
                        label=label,
                        underlying_symbol=trade.get("underlying_symbol"),
                        strategy=trade.get("strategy"),
                        profile=profile,
                        option_symbol=trade["option_symbol"],
                        leg_role=trade.get("leg_role", "contract"),
                        price=trade["price"],
                        size=trade["size"],
                        premium=trade["premium"],
                        exchange_code=trade.get("exchange_code"),
                        conditions_json=list(trade.get("conditions") or []),
                        source_timestamp=parse_datetime(trade.get("source_timestamp")),
                        included_in_score=bool(trade.get("included_in_score")),
                        exclusion_reason=trade.get("exclusion_reason"),
                        raw_payload_json=dict(trade.get("raw_payload") or {}),
                        source=trade.get("source", "alpaca_websocket"),
                    )
                    for trade in trades
                ]
            )
        return len(trades)

    def save_option_trade_tick_rows(
        self,
        *,
        rows: list[dict[str, Any]],
    ) -> int:
        if not rows:
            return 0

        with self.session_scope() as session:
            session.add_all(
                [
                    OptionTradeTickModel(
                        cycle_id=str(row["cycle_id"]),
                        captured_at=parse_datetime(row["captured_at"]),
                        label=str(row["label"]),
                        underlying_symbol=row.get("underlying_symbol"),
                        strategy=row.get("strategy"),
                        profile=row.get("profile"),
                        option_symbol=str(row["option_symbol"]),
                        leg_role=str(row.get("leg_role") or "contract"),
                        price=float(row["price"]),
                        size=int(row["size"]),
                        premium=float(row["premium"]),
                        exchange_code=row.get("exchange_code"),
                        conditions_json=list(row.get("conditions") or []),
                        source_timestamp=parse_datetime(row.get("source_timestamp")),
                        included_in_score=bool(row.get("included_in_score")),
                        exclusion_reason=row.get("exclusion_reason"),
                        raw_payload_json=dict(row.get("raw_payload") or {}),
                        source=str(row.get("source") or "alpaca_websocket"),
                    )
                    for row in rows
                ]
            )
        return len(rows)

    def summarize_option_trade_window(
        self,
        *,
        option_symbols: list[str],
        captured_from: str | datetime,
        captured_to: str | datetime,
    ) -> dict[str, dict[str, Any]]:
        if not option_symbols:
            return {}
        captured_from_dt = parse_datetime(captured_from)
        captured_to_dt = parse_datetime(captured_to)
        if captured_from_dt is None or captured_to_dt is None or captured_from_dt >= captured_to_dt:
            return {}

        statement = (
            select(
                OptionTradeTickModel.option_symbol,
                func.count(OptionTradeTickModel.trade_tick_id),
                func.max(OptionTradeTickModel.captured_at),
            )
            .where(OptionTradeTickModel.option_symbol.in_(option_symbols))
            .where(OptionTradeTickModel.captured_at >= captured_from_dt)
            .where(OptionTradeTickModel.captured_at < captured_to_dt)
            .group_by(OptionTradeTickModel.option_symbol)
        )
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        return {
            str(option_symbol): {
                "tick_count": int(tick_count or 0),
                "last_captured_at": render_value(last_captured_at),
            }
            for option_symbol, tick_count, last_captured_at in rows
        }


NEW_YORK = ZoneInfo("America/New_York")


def session_bounds(session_date: str) -> tuple[datetime, datetime]:
    ny_date = date.fromisoformat(session_date)
    session_start_local = datetime.combine(ny_date, time.min, tzinfo=NEW_YORK)
    session_end_local = session_start_local + timedelta(days=1)
    return session_start_local.astimezone(timezone.utc), session_end_local.astimezone(timezone.utc)
