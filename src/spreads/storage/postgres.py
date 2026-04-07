from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterator

from sqlalchemy import and_, create_engine, delete, func, inspect, select
from sqlalchemy.orm import Session, sessionmaker

from spreads.storage.models import OptionQuoteEventModel, ScanCandidateModel, ScanRunModel


def normalize_database_url(url: str) -> str:
    if url.startswith("postgresql+psycopg://"):
        return url
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+psycopg://", 1)
    return url


def _parse_datetime(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    normalized = value.replace("Z", "+00:00") if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _parse_date(value: str | date) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return date.fromisoformat(str(value))


def _render_value(value: Any) -> Any:
    if isinstance(value, datetime):
        rendered = value.isoformat()
        return rendered.replace("+00:00", "Z") if rendered.endswith("+00:00") else rendered
    if isinstance(value, date):
        return value.isoformat()
    return value


def _row_to_dict(model: Any) -> dict[str, Any]:
    return {
        column.name: _render_value(getattr(model, column.name))
        for column in model.__table__.columns
    }


class PostgresRunHistoryStore:
    def __init__(self, database_url: str) -> None:
        self.path = database_url
        self.engine = create_engine(
            normalize_database_url(database_url),
            future=True,
            pool_pre_ping=True,
        )
        self.session_factory = sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
            future=True,
        )
        with self.session_factory() as session:
            session.execute(select(1))

    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def schema_ready(self) -> bool:
        tables = set(inspect(self.engine).get_table_names(schema="public"))
        required = {"scan_runs", "scan_candidates", "option_quote_events"}
        return required.issubset(tables)

    def table_counts(self) -> dict[str, int]:
        with self.session_factory() as session:
            return {
                "scan_runs": int(session.scalar(select(func.count()).select_from(ScanRunModel)) or 0),
                "scan_candidates": int(
                    session.scalar(select(func.count()).select_from(ScanCandidateModel)) or 0
                ),
                "option_quote_events": int(
                    session.scalar(select(func.count()).select_from(OptionQuoteEventModel)) or 0
                ),
            }

    def truncate_all(self) -> None:
        with self.session_scope() as session:
            session.execute(delete(OptionQuoteEventModel))
            session.execute(delete(ScanCandidateModel))
            session.execute(delete(ScanRunModel))

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

            run.generated_at = _parse_datetime(generated_at)
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
                    expiration_date=_parse_date(candidate.expiration_date),
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

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self.session_factory() as session:
            run = session.get(ScanRunModel, run_id)
        if run is None:
            return None
        payload = _row_to_dict(run)
        payload["filters"] = payload.pop("filters_json")
        payload["setup"] = payload.get("setup_json")
        return payload

    def get_latest_run(self, symbol: str, strategy: str | None = None) -> dict[str, Any] | None:
        statement = select(ScanRunModel).where(ScanRunModel.symbol == symbol.upper())
        if strategy is not None:
            statement = statement.where(ScanRunModel.strategy == strategy)
        statement = statement.order_by(ScanRunModel.generated_at.desc()).limit(1)
        with self.session_factory() as session:
            run = session.scalar(statement)
        if run is None:
            return None
        payload = _row_to_dict(run)
        payload["filters"] = payload.pop("filters_json")
        payload["setup"] = payload.get("setup_json")
        return payload

    def list_candidates(self, run_id: str) -> list[dict[str, Any]]:
        statement = (
            select(ScanCandidateModel)
            .where(ScanCandidateModel.run_id == run_id)
            .order_by(ScanCandidateModel.rank.asc())
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [_row_to_dict(row) for row in rows]

    def list_runs(
        self,
        *,
        limit: int,
        symbol: str | None = None,
        strategy: str | None = None,
    ) -> list[dict[str, Any]]:
        statement = select(ScanRunModel)
        if symbol:
            statement = statement.where(ScanRunModel.symbol == symbol.upper())
        if strategy:
            statement = statement.where(ScanRunModel.strategy == strategy)
        statement = statement.order_by(ScanRunModel.generated_at.desc()).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [_row_to_dict(row) for row in rows]

    def list_session_top_runs(
        self,
        *,
        session_date: str,
        session_label: str | None = None,
    ) -> list[dict[str, Any]]:
        session_start_date = date.fromisoformat(session_date)
        session_start = datetime.combine(session_start_date, time.min, tzinfo=timezone.utc)
        session_end = session_start + timedelta(days=1)

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

        payloads: list[dict[str, Any]] = []
        for run, candidate in rows:
            payload = {
                "run_id": _render_value(run.run_id),
                "generated_at": _render_value(run.generated_at),
                "symbol": _render_value(run.symbol),
                "strategy": _render_value(run.strategy),
                "profile": _render_value(run.profile),
                "spot_price": _render_value(run.spot_price),
                "candidate_count": _render_value(run.candidate_count),
                "setup_status": _render_value(run.setup_status),
                "setup_score": _render_value(run.setup_score),
                "setup_json": run.setup_json,
                "short_symbol": None,
                "long_symbol": None,
                "short_strike": None,
                "long_strike": None,
                "midpoint_credit": None,
                "quality_score": None,
                "calendar_status": None,
                "expected_move": None,
                "short_vs_expected_move": None,
            }
            if candidate is not None:
                payload.update(
                    {
                        "short_symbol": _render_value(candidate.short_symbol),
                        "long_symbol": _render_value(candidate.long_symbol),
                        "short_strike": _render_value(candidate.short_strike),
                        "long_strike": _render_value(candidate.long_strike),
                        "midpoint_credit": _render_value(candidate.midpoint_credit),
                        "quality_score": _render_value(candidate.quality_score),
                        "calendar_status": _render_value(candidate.calendar_status),
                        "expected_move": _render_value(candidate.expected_move),
                        "short_vs_expected_move": _render_value(
                            candidate.short_vs_expected_move
                        ),
                    }
                )
            payloads.append(payload)
        return payloads

    def list_session_quote_events(
        self,
        *,
        session_date: str,
        label: str,
    ) -> list[dict[str, Any]]:
        session_start_date = date.fromisoformat(session_date)
        session_start = datetime.combine(session_start_date, time.min, tzinfo=timezone.utc)
        session_end = session_start + timedelta(days=1)

        statement = (
            select(OptionQuoteEventModel)
            .where(OptionQuoteEventModel.captured_at >= session_start)
            .where(OptionQuoteEventModel.captured_at < session_end)
            .where(OptionQuoteEventModel.label == label)
            .order_by(OptionQuoteEventModel.quote_id.asc())
        )
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [_row_to_dict(row) for row in rows]

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
                        captured_at=_parse_datetime(quote["captured_at"]),
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
                        quote_timestamp=_parse_datetime(quote.get("quote_timestamp")),
                        source=quote.get("source", "alpaca_websocket"),
                    )
                    for quote in quotes
                ]
            )
        return len(quotes)

    def close(self) -> None:
        self.engine.dispose()
