from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from core.storage.engine_models import (
    TickerSourceObservationModel,
    TickerSourceRunModel,
    TickerSourceStateModel,
)
from core.storage.records import StorageRow
from core.storage.serializers import parse_datetime, render_value
from core.value_coercion import as_text, coerce_float, coerce_int, normalize_symbol, unique_text_list

if TYPE_CHECKING:
    pass


from core.storage.engine_facts.contracts import (
    TickerSourceObservationPayload,
)

class EngineFactSourceMixin:
    def upsert_ticker_source_run(
        self,
        *,
        ticker_source_run_id: str,
        ticker_source_type: str,
        ticker_source_id: str,
        job_run_id: str | None,
        status: str,
        config_hash: str | None,
        generated_at: str,
        completed_at: str | None,
        symbols: list[str],
        entries: list[dict[str, Any]],
        observations: list[dict[str, Any]] | None = None,
        summary: dict[str, Any],
        evidence: dict[str, Any],
        updated_at: str,
    ) -> StorageRow:
        generated_at_dt = parse_datetime(generated_at)
        completed_at_dt = parse_datetime(completed_at)
        updated_at_dt = parse_datetime(updated_at)
        if generated_at_dt is None or updated_at_dt is None:
            raise ValueError("generated_at and updated_at are required")
        selected_entries = [dict(entry) for entry in list(entries or []) if isinstance(entry, Mapping)]
        entries_by_symbol = {symbol: dict(entry) for entry in selected_entries if (symbol := normalize_symbol(entry.get("symbol"))) is not None}
        normalized_symbols = list(dict.fromkeys(symbol for value in symbols if (symbol := normalize_symbol(value)) is not None))
        selected_symbols = set(normalized_symbols)
        observation_rows = self._normalize_observations(
            observations=observations,
            selected_entries=selected_entries,
            selected_symbols=selected_symbols,
        )
        excluded_count = sum(1 for row in observation_rows if str(row.get("observation_state") or "") in {"excluded", "filtered_out"})
        with self.session_scope() as session:
            row = session.get(TickerSourceRunModel, ticker_source_run_id)
            if row is None:
                row = TickerSourceRunModel(
                    ticker_source_run_id=ticker_source_run_id,
                    ticker_source_type=ticker_source_type,
                    ticker_source_id=ticker_source_id,
                    job_run_id=job_run_id,
                    status=status,
                    config_hash=config_hash,
                    generated_at=generated_at_dt,
                    completed_at=completed_at_dt,
                    observed_count=len(observation_rows),
                    selected_count=len(normalized_symbols),
                    excluded_count=excluded_count,
                    summary_json=render_value(summary),
                    evidence_json=render_value(evidence),
                    created_at=updated_at_dt,
                    updated_at=updated_at_dt,
                )
                session.add(row)
            else:
                row.ticker_source_type = ticker_source_type
                row.ticker_source_id = ticker_source_id
                row.job_run_id = job_run_id
                row.status = status
                row.config_hash = config_hash
                row.generated_at = generated_at_dt
                row.completed_at = completed_at_dt
                row.observed_count = len(observation_rows)
                row.selected_count = len(normalized_symbols)
                row.excluded_count = excluded_count
                row.summary_json = render_value(summary)
                row.evidence_json = render_value(evidence)
                row.updated_at = updated_at_dt

            observations_by_symbol: dict[str, TickerSourceObservationModel] = {}
            for rank, observation in enumerate(observation_rows, start=1):
                symbol = str(observation["symbol"])
                ticker = session.scalar(
                    select(TickerSourceObservationModel).where(
                        TickerSourceObservationModel.ticker_source_run_id == ticker_source_run_id,
                        TickerSourceObservationModel.symbol == symbol,
                    )
                )
                entry = entries_by_symbol.get(symbol, {})
                observation_rank = coerce_int(observation.get("rank"))
                if observation_rank is None and str(observation.get("observation_state") or "") == "selected":
                    observation_rank = rank
                if ticker is None:
                    ticker = TickerSourceObservationModel(
                        ticker_source_run_id=ticker_source_run_id,
                        ticker_source_id=ticker_source_id,
                        symbol=symbol,
                        observation_state=str(observation.get("observation_state") or "observed"),
                        rank=observation_rank,
                        score=coerce_float(observation.get("score")),
                        company=as_text(observation.get("company")),
                        sector=as_text(observation.get("sector")),
                        industry=as_text(observation.get("industry")),
                        country=as_text(observation.get("country")),
                        price=coerce_float(observation.get("price")),
                        market_cap=coerce_int(observation.get("market_cap")),
                        daily_volume=coerce_int(observation.get("daily_volume")),
                        move_percent=coerce_float(observation.get("move_percent")),
                        relative_volume=coerce_float(observation.get("relative_volume")),
                        reason_codes_json=unique_text_list(observation.get("reason_codes") or entry.get("reason_codes")),
                        evidence_json=render_value(observation),
                        created_at=updated_at_dt,
                    )
                    session.add(ticker)
                else:
                    ticker.ticker_source_id = ticker_source_id
                    ticker.observation_state = str(observation.get("observation_state") or "observed")
                    ticker.rank = observation_rank
                    ticker.score = coerce_float(observation.get("score"))
                    ticker.company = as_text(observation.get("company"))
                    ticker.sector = as_text(observation.get("sector"))
                    ticker.industry = as_text(observation.get("industry"))
                    ticker.country = as_text(observation.get("country"))
                    ticker.price = coerce_float(observation.get("price"))
                    ticker.market_cap = coerce_int(observation.get("market_cap"))
                    ticker.daily_volume = coerce_int(observation.get("daily_volume"))
                    ticker.move_percent = coerce_float(observation.get("move_percent"))
                    ticker.relative_volume = coerce_float(observation.get("relative_volume"))
                    ticker.reason_codes_json = unique_text_list(observation.get("reason_codes") or entry.get("reason_codes"))
                    ticker.evidence_json = render_value(observation)
                observations_by_symbol[symbol] = ticker

            session.flush()
            self._update_ticker_source_state(
                session,
                ticker_source_id=ticker_source_id,
                ticker_source_run_id=ticker_source_run_id,
                generated_at=generated_at_dt,
                updated_at=updated_at_dt,
                status=status,
                observations_by_symbol=observations_by_symbol,
                selected_symbols=selected_symbols,
            )

            session.flush()
            session.refresh(row)
            return self.row(row)

    def get_latest_ticker_source_snapshot(
        self,
        *,
        ticker_source_id: str,
        max_age_seconds: int | None = None,
    ) -> StorageRow:
        with self.session_factory() as session:
            run = session.scalar(
                select(TickerSourceRunModel)
                .where(TickerSourceRunModel.ticker_source_id == ticker_source_id)
                .order_by(TickerSourceRunModel.generated_at.desc(), TickerSourceRunModel.ticker_source_run_id.asc())
                .limit(1)
            )
            if run is None:
                return {
                    "status": "missing",
                    "ticker_source_id": ticker_source_id,
                    "symbols": [],
                    "entries": [],
                    "summary": {},
                    "degradation": {
                        "status": "missing",
                        "reason": "no_ticker_source_run",
                    },
                    "ticker_source_run_id": None,
                    "job_run_id": None,
                    "generated_at": None,
                    "age_seconds": None,
                }
            observations = session.scalars(
                select(TickerSourceObservationModel)
                .where(TickerSourceObservationModel.ticker_source_run_id == run.ticker_source_run_id)
                .where(TickerSourceObservationModel.observation_state == "selected")
                .order_by(TickerSourceObservationModel.rank.asc().nullslast(), TickerSourceObservationModel.symbol.asc())
            ).all()
            run_row = self.row(run)
            observation_rows = self.rows(observations)
        generated_dt = run.generated_at.astimezone(UTC)
        age_seconds = max((datetime.now(UTC) - generated_dt).total_seconds(), 0.0)
        run_status = str(run.status or "").strip().lower()
        snapshot_status = (
            "ready" if observation_rows and run_status == "completed" else "empty" if run_status == "completed" else run_status or "missing"
        )
        if max_age_seconds is not None and age_seconds > max(int(max_age_seconds), 0):
            snapshot_status = "stale"
        symbols = [str(row.get("symbol") or "").upper() for row in observation_rows if str(row.get("symbol") or "").strip()]
        evidence = dict(run_row.get("evidence") or {})
        persisted_degradation = dict(evidence.get("degradation") or {})
        degradation_reason = as_text(persisted_degradation.get("reason"))
        if snapshot_status == "stale":
            degradation = {"status": "stale", "reason": "snapshot_stale"}
        elif persisted_degradation:
            degradation = persisted_degradation
        else:
            degradation = {
                "status": snapshot_status,
                "reason": None if snapshot_status in {"ready", "empty"} else snapshot_status,
            }
        if degradation_reason is not None and degradation.get("reason") is None:
            degradation["reason"] = degradation_reason
        return {
            "status": snapshot_status,
            "ticker_source_id": ticker_source_id,
            "ticker_source_run_id": run_row.get("ticker_source_run_id"),
            "ticker_source_type": run_row.get("ticker_source_type"),
            "job_run_id": run_row.get("job_run_id"),
            "generated_at": run_row.get("generated_at"),
            "age_seconds": age_seconds,
            "symbols": symbols if snapshot_status != "stale" else [],
            "entries": observation_rows if snapshot_status != "stale" else [],
            "summary": dict(run_row.get("summary") or {}),
            "degradation": degradation,
        }

    def list_ticker_source_state(
        self,
        *,
        ticker_source_id: str | None = None,
        active: bool | None = None,
        limit: int = 50,
    ) -> list[StorageRow]:
        statement = select(TickerSourceStateModel)
        if ticker_source_id is not None:
            statement = statement.where(TickerSourceStateModel.ticker_source_id == ticker_source_id)
        if active is not None:
            statement = statement.where(TickerSourceStateModel.active.is_(active))
        statement = statement.order_by(
            TickerSourceStateModel.active.desc(),
            TickerSourceStateModel.last_rank.asc().nullslast(),
            TickerSourceStateModel.last_seen_at.desc(),
            TickerSourceStateModel.symbol.asc(),
        ).limit(max(int(limit), 1))
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def _normalize_observations(
        cls,
        *,
        observations: list[dict[str, Any]] | None,
        selected_entries: list[dict[str, Any]],
        selected_symbols: set[str],
    ) -> list[dict[str, Any]]:
        rows: dict[str, dict[str, Any]] = {}
        for raw in list(observations or []):
            if not isinstance(raw, Mapping):
                continue
            try:
                observation = TickerSourceObservationPayload.model_validate(raw)
            except ValueError:
                continue
            rows[observation.symbol] = observation.as_storage_payload(
                default_state="selected" if observation.symbol in selected_symbols else "observed",
            )

        for rank, raw in enumerate(selected_entries, start=1):
            try:
                observation = TickerSourceObservationPayload.model_validate(raw)
            except ValueError:
                continue
            row = rows.get(
                observation.symbol,
                observation.as_storage_payload(default_state="selected"),
            )
            row["symbol"] = observation.symbol
            row["observation_state"] = "selected"
            row["rank"] = rank
            row.setdefault("score", observation.score)
            row.setdefault("reason_codes", observation.reason_codes)
            rows[observation.symbol] = row

        return list(rows.values())

    def _update_ticker_source_state(
        self,
        session: Any,
        *,
        ticker_source_id: str,
        ticker_source_run_id: str,
        generated_at: datetime,
        updated_at: datetime,
        status: str,
        observations_by_symbol: dict[str, TickerSourceObservationModel],
        selected_symbols: set[str],
    ) -> None:
        seen_symbols = set(observations_by_symbol)
        for symbol, observation in observations_by_symbol.items():
            state = session.scalar(
                select(TickerSourceStateModel).where(
                    TickerSourceStateModel.ticker_source_id == ticker_source_id,
                    TickerSourceStateModel.symbol == symbol,
                )
            )
            is_selected = symbol in selected_symbols and observation.observation_state == "selected"
            is_new_ticker_source_run = state is None or state.last_ticker_source_run_id != ticker_source_run_id
            if state is None:
                state = TickerSourceStateModel(
                    ticker_source_id=ticker_source_id,
                    symbol=symbol,
                    active=True,
                    first_seen_at=generated_at,
                    last_seen_at=generated_at,
                    first_selected_at=generated_at if is_selected else None,
                    last_selected_at=generated_at if is_selected else None,
                    seen_count=0,
                    selected_count=0,
                    consecutive_seen_count=0,
                    consecutive_missing_count=0,
                    last_rank=observation.rank,
                    best_rank=observation.rank,
                    last_score=observation.score,
                    best_score=observation.score,
                    last_state=observation.observation_state,
                    last_ticker_source_run_id=ticker_source_run_id,
                    last_observation_id=observation.ticker_source_observation_id,
                    last_metrics_json={},
                    created_at=updated_at,
                    updated_at=updated_at,
                )
                session.add(state)

            state.active = True
            state.last_seen_at = generated_at
            state.last_rank = observation.rank
            state.last_score = observation.score
            state.last_state = observation.observation_state
            state.last_ticker_source_run_id = ticker_source_run_id
            state.last_observation_id = observation.ticker_source_observation_id
            state.last_metrics_json = render_value(self._observation_metrics(observation))
            state.updated_at = updated_at
            if is_new_ticker_source_run:
                state.seen_count += 1
                state.consecutive_seen_count += 1
                state.consecutive_missing_count = 0
            if observation.rank is not None and (state.best_rank is None or observation.rank < state.best_rank):
                state.best_rank = observation.rank
            if observation.score is not None and (state.best_score is None or observation.score > state.best_score):
                state.best_score = observation.score
            if is_selected:
                if state.first_selected_at is None:
                    state.first_selected_at = generated_at
                state.last_selected_at = generated_at
                if is_new_ticker_source_run:
                    state.selected_count += 1

        if str(status or "").strip().lower() != "completed":
            return

        missing_statement = select(TickerSourceStateModel).where(
            TickerSourceStateModel.ticker_source_id == ticker_source_id,
            TickerSourceStateModel.active.is_(True),
        )
        if seen_symbols:
            missing_statement = missing_statement.where(~TickerSourceStateModel.symbol.in_(seen_symbols))
        for state in session.scalars(missing_statement).all():
            if state.last_ticker_source_run_id == ticker_source_run_id:
                continue
            state.active = False
            state.consecutive_seen_count = 0
            state.consecutive_missing_count += 1
            state.updated_at = updated_at

    @staticmethod
    def _observation_metrics(observation: TickerSourceObservationModel) -> dict[str, Any]:
        return {
            key: value
            for key, value in {
                "company": observation.company,
                "sector": observation.sector,
                "industry": observation.industry,
                "country": observation.country,
                "price": observation.price,
                "market_cap": observation.market_cap,
                "daily_volume": observation.daily_volume,
                "move_percent": observation.move_percent,
                "relative_volume": observation.relative_volume,
            }.items()
            if value is not None
        }


__all__ = ["EngineFactSourceMixin"]
