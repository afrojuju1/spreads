from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import and_, delete, func, select

from spreads.storage.base import RepositoryBase
from spreads.storage.models import OptionQuoteEventModel, OptionTradeEventModel, ScanCandidateModel, ScanRunModel
from spreads.storage.records import (
    OptionQuoteEventRecord,
    OptionTradeEventRecord,
    ScanCandidateRecord,
    ScanRunRecord,
    SessionTopRunRecord,
)
from spreads.storage.serializers import parse_date, parse_datetime


class RunHistoryRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables("scan_runs", "scan_candidates", "option_quote_events")

    def table_counts(self) -> dict[str, int]:
        with self.session_factory() as session:
            counts = {
                "scan_runs": int(session.scalar(select(func.count()).select_from(ScanRunModel)) or 0),
                "scan_candidates": int(
                    session.scalar(select(func.count()).select_from(ScanCandidateModel)) or 0
                ),
                "option_quote_events": int(
                    session.scalar(select(func.count()).select_from(OptionQuoteEventModel)) or 0
                ),
            }
            if self.schema_has_tables("option_trade_events"):
                counts["option_trade_events"] = int(
                    session.scalar(select(func.count()).select_from(OptionTradeEventModel)) or 0
                )
            return counts

    def truncate_all(self) -> None:
        with self.session_scope() as session:
            if self.schema_has_tables("option_trade_events"):
                session.execute(delete(OptionTradeEventModel))
            session.execute(delete(OptionQuoteEventModel))
            session.execute(delete(ScanCandidateModel))
            session.execute(delete(ScanRunModel))

    def _session_top_run_row(
        self,
        run: ScanRunModel,
        candidate: ScanCandidateModel | None,
    ) -> SessionTopRunRecord:
        return self.row(
            run,
            aliases={"setup_json": "setup_json"},
            extra={
                "short_symbol": None if candidate is None else candidate.short_symbol,
                "long_symbol": None if candidate is None else candidate.long_symbol,
                "short_strike": None if candidate is None else candidate.short_strike,
                "long_strike": None if candidate is None else candidate.long_strike,
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
                ScanCandidateModel(
                    run_id=run_id,
                    rank=rank,
                    strategy=candidate.strategy,
                    expiration_date=parse_date(candidate.expiration_date),
                    short_symbol=candidate.short_symbol,
                    long_symbol=candidate.long_symbol,
                    short_strike=candidate.short_strike,
                    long_strike=candidate.long_strike,
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
                for rank, candidate in enumerate(candidates, start=1)
            ]

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
            select(ScanCandidateModel)
            .where(ScanCandidateModel.run_id == run_id)
            .order_by(ScanCandidateModel.rank.asc())
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

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

    def list_session_quote_events(
        self,
        *,
        session_date: str,
        label: str,
    ) -> list[OptionQuoteEventRecord]:
        session_start, session_end = session_bounds(session_date)

        statement = (
            select(OptionQuoteEventModel)
            .where(OptionQuoteEventModel.captured_at >= session_start)
            .where(OptionQuoteEventModel.captured_at < session_end)
            .where(OptionQuoteEventModel.label == label)
            .order_by(OptionQuoteEventModel.quote_id.asc())
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def save_option_quote_events(
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
                    OptionQuoteEventModel(
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
                        quote_timestamp=parse_datetime(quote.get("quote_timestamp")),
                        source=quote.get("source", "alpaca_websocket"),
                    )
                    for quote in quotes
                ]
            )
        return len(quotes)

    def list_session_trade_events(
        self,
        *,
        session_date: str,
        label: str,
    ) -> list[OptionTradeEventRecord]:
        session_start, session_end = session_bounds(session_date)

        statement = (
            select(OptionTradeEventModel)
            .where(OptionTradeEventModel.captured_at >= session_start)
            .where(OptionTradeEventModel.captured_at < session_end)
            .where(OptionTradeEventModel.label == label)
            .order_by(OptionTradeEventModel.trade_id.asc())
        )
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
                OptionTradeEventModel.underlying_symbol,
                func.count(OptionTradeEventModel.trade_id),
                func.count(func.distinct(OptionTradeEventModel.option_symbol)),
                func.coalesce(func.sum(OptionTradeEventModel.premium), 0.0),
            )
            .where(OptionTradeEventModel.label == label)
            .where(OptionTradeEventModel.included_in_score.is_(True))
            .where(OptionTradeEventModel.underlying_symbol.in_(underlyings))
            .where(OptionTradeEventModel.captured_at >= captured_from_dt)
            .where(OptionTradeEventModel.captured_at < captured_to_dt)
            .group_by(OptionTradeEventModel.underlying_symbol)
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
            select(func.max(OptionTradeEventModel.captured_at))
            .where(OptionTradeEventModel.label == label)
            .where(OptionTradeEventModel.captured_at < current_session_start)
        )
        with self.session_factory() as session:
            latest = session.scalar(statement)
        if latest is None:
            return None
        latest_dt = latest if latest.tzinfo else latest.replace(tzinfo=timezone.utc)
        return latest_dt.astimezone(NEW_YORK).date().isoformat()

    def save_option_trade_events(
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
                    OptionTradeEventModel(
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
                        trade_timestamp=parse_datetime(trade.get("trade_timestamp")),
                        included_in_score=bool(trade.get("included_in_score")),
                        exclusion_reason=trade.get("exclusion_reason"),
                        raw_payload_json=dict(trade.get("raw_payload") or {}),
                        source=trade.get("source", "alpaca_websocket"),
                    )
                    for trade in trades
                ]
            )
        return len(trades)



NEW_YORK = ZoneInfo("America/New_York")


def session_bounds(session_date: str) -> tuple[datetime, datetime]:
    ny_date = date.fromisoformat(session_date)
    session_start_local = datetime.combine(ny_date, time.min, tzinfo=NEW_YORK)
    session_end_local = session_start_local + timedelta(days=1)
    return session_start_local.astimezone(timezone.utc), session_end_local.astimezone(timezone.utc)
