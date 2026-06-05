from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import and_, delete, func, select

from core.services.option_structures import (
    fallback_vertical_legs,
    legs_identity_key,
    normalize_legs,
    primary_short_long_symbols,
    structure_strike_path,
    structure_symbol_path,
)
from core.storage.base import RepositoryBase
from core.storage.market_tick_models import OptionQuoteTickModel, OptionTradeTickModel
from core.storage.models import ScanCandidateModel, ScanRunModel
from core.storage.records import (
    OptionQuoteTickRecord,
    OptionTradeTickRecord,
    ScanCandidateRecord,
    ScanRunRecord,
    SessionTopRunRecord,
)
from core.storage.serializers import parse_date, parse_datetime, render_value


class RunHistoryRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables("scan_runs", "scan_candidates", "option_quote_ticks")

    def table_counts(self) -> dict[str, int]:
        with self.session_factory() as session:
            counts = {
                "scan_runs": int(session.scalar(select(func.count()).select_from(ScanRunModel)) or 0),
                "scan_candidates": int(session.scalar(select(func.count()).select_from(ScanCandidateModel)) or 0),
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
            session.execute(delete(ScanCandidateModel))
            session.execute(delete(ScanRunModel))

    def _candidate_structure_payload(
        self,
        candidate: Any,
    ) -> tuple[list[dict[str, Any]], str | None]:
        expiration_date = getattr(candidate, "expiration_date", None)
        legs = normalize_legs(getattr(candidate, "legs", None), expiration_date=expiration_date)
        if not legs:
            order_payload = getattr(candidate, "order_payload", None)
            if isinstance(order_payload, dict):
                legs = normalize_legs(
                    order_payload.get("legs"),
                    expiration_date=expiration_date,
                )
        if not legs:
            legs = fallback_vertical_legs(
                short_symbol=getattr(candidate, "short_symbol", None),
                long_symbol=getattr(candidate, "long_symbol", None),
                expiration_date=expiration_date,
            )
        structure_identity = getattr(candidate, "structure_identity", None)
        if structure_identity is None and legs:
            structure_identity = legs_identity_key(
                strategy=getattr(candidate, "strategy", None),
                legs=legs,
            )
        return legs, structure_identity

    def _scan_candidate_extra(
        self,
        candidate: ScanCandidateModel | None,
    ) -> dict[str, Any]:
        if candidate is None:
            return {
                "short_symbol": None,
                "long_symbol": None,
                "symbol_path": None,
                "strike_path": None,
            }
        legs = list(candidate.legs_json or [])
        short_symbol, long_symbol = primary_short_long_symbols(legs)
        return {
            "short_symbol": short_symbol,
            "long_symbol": long_symbol,
            "symbol_path": structure_symbol_path(legs),
            "strike_path": structure_strike_path(legs, strategy=candidate.strategy),
        }

    def _session_top_run_row(
        self,
        run: ScanRunModel,
        candidate: ScanCandidateModel | None,
    ) -> SessionTopRunRecord:
        return self.row(
            run,
            aliases={"setup_json": "setup_json"},
            extra={
                **self._scan_candidate_extra(candidate),
                "midpoint_credit": None if candidate is None else candidate.midpoint_credit,
                "quality_score": None if candidate is None else candidate.quality_score,
                "calendar_status": None if candidate is None else candidate.calendar_status,
                "expected_move": None if candidate is None else candidate.expected_move,
                "short_vs_expected_move": None if candidate is None else candidate.short_vs_expected_move,
            },
        )

    def save_run(
        self,
        *,
        run_id: str,
        generated_at: str,
        symbol: str,
        strategy: str,
        session_label: str | None,
        profile: str,
        spot_price: float,
        output_path: str,
        filters: dict[str, Any],
        setup_status: str | None,
        setup_score: float | None,
        setup_payload: dict[str, Any] | None,
        candidates: list[Any],
    ) -> None:
        with self.session_scope() as session:
            run = session.get(ScanRunModel, run_id)
            if run is None:
                run = ScanRunModel(run_id=run_id)
                session.add(run)

            run.generated_at = parse_datetime(generated_at)
            run.symbol = symbol
            run.strategy = strategy
            run.session_label = session_label
            run.profile = profile
            run.spot_price = spot_price
            run.candidate_count = len(candidates)
            run.output_path = output_path
            run.filters_json = filters
            run.setup_status = setup_status
            run.setup_score = setup_score
            run.setup_json = setup_payload
            run.candidates = [
                self._build_scan_candidate_model(run_id=run_id, rank=rank, candidate=candidate) for rank, candidate in enumerate(candidates, start=1)
            ]

    def _build_scan_candidate_model(
        self,
        *,
        run_id: str,
        rank: int,
        candidate: Any,
    ) -> ScanCandidateModel:
        legs, structure_identity = self._candidate_structure_payload(candidate)
        if structure_identity is None:
            raise ValueError("Scan candidate is missing canonical structure identity")
        return ScanCandidateModel(
            run_id=run_id,
            rank=rank,
            strategy=candidate.strategy,
            expiration_date=parse_date(candidate.expiration_date),
            structure_identity=structure_identity,
            legs_json=list(legs),
            width=candidate.width,
            midpoint_credit=candidate.midpoint_credit,
            natural_credit=candidate.natural_credit,
            breakeven=candidate.breakeven,
            max_profit=candidate.max_profit,
            max_loss=candidate.max_loss,
            quality_score=candidate.quality_score,
            return_on_risk=candidate.return_on_risk,
            short_otm_pct=candidate.short_otm_pct,
            calendar_status=candidate.calendar_status,
            setup_status=getattr(candidate, "setup_status", None),
            expected_move=candidate.expected_move,
            short_vs_expected_move=candidate.short_vs_expected_move,
        )

    def get_run(self, run_id: str) -> ScanRunRecord | None:
        with self.session_factory() as session:
            run = session.get(ScanRunModel, run_id)
        if run is None:
            return None
        return self.row(run)

    def get_latest_run(self, symbol: str, strategy: str | None = None) -> ScanRunRecord | None:
        statement = select(ScanRunModel).where(ScanRunModel.symbol == symbol.upper())
        if strategy is not None:
            statement = statement.where(ScanRunModel.strategy == strategy)
        statement = statement.order_by(ScanRunModel.generated_at.desc()).limit(1)
        with self.session_factory() as session:
            run = session.scalar(statement)
        if run is None:
            return None
        return self.row(run)

    def list_candidates(self, run_id: str) -> list[ScanCandidateRecord]:
        statement = (
            select(ScanCandidateModel, ScanRunModel.symbol)
            .join(ScanRunModel, ScanRunModel.run_id == ScanCandidateModel.run_id)
            .where(ScanCandidateModel.run_id == run_id)
            .order_by(ScanCandidateModel.rank.asc())
        )
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        return [
            self.row(
                candidate,
                extra={
                    **self._scan_candidate_extra(candidate),
                    "underlying_symbol": underlying_symbol,
                },
            )
            for candidate, underlying_symbol in rows
        ]

    def list_runs(
        self,
        *,
        limit: int,
        symbol: str | None = None,
        strategy: str | None = None,
    ) -> list[ScanRunRecord]:
        statement = select(ScanRunModel)
        if symbol:
            statement = statement.where(ScanRunModel.symbol == symbol.upper())
        if strategy:
            statement = statement.where(ScanRunModel.strategy == strategy)
        statement = statement.order_by(ScanRunModel.generated_at.desc()).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_session_top_runs(
        self,
        *,
        session_date: str,
        session_label: str | None = None,
    ) -> list[SessionTopRunRecord]:
        session_start, session_end = session_bounds(session_date)

        statement = (
            select(ScanRunModel, ScanCandidateModel)
            .outerjoin(
                ScanCandidateModel,
                and_(
                    ScanCandidateModel.run_id == ScanRunModel.run_id,
                    ScanCandidateModel.rank == 1,
                ),
            )
            .where(ScanRunModel.generated_at >= session_start)
            .where(ScanRunModel.generated_at < session_end)
            .order_by(ScanRunModel.generated_at.asc())
        )
        if session_label:
            statement = statement.where(ScanRunModel.session_label == session_label)

        with self.session_factory() as session:
            rows = session.execute(statement).all()
        return [self._session_top_run_row(run, candidate) for run, candidate in rows]

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
