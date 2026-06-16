from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import delete, or_, select

from core.storage.base import RepositoryBase
from core.storage.engine_models import (
    CandidateRunModel,
    CandidateSymbolDiagnosticModel,
    TickerSourceObservationModel,
    TickerSourceRunModel,
    TickerSourceStateModel,
    TradeCandidateModel,
)
from core.storage.lifecycle_models import TradeAdmissionModel, TradeDecisionModel, TradeExecutionIntentModel, TradeSignalModel
from core.storage.read_models import TradeDecisionSignalRead
from core.storage.records import StorageRow
from core.storage.serializers import parse_date, parse_datetime, render_value


def _optional_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    return parse_date(value)


class EngineFactRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables(
            "ticker_source_runs",
            "ticker_source_observations",
            "ticker_source_state",
            "candidate_runs",
            "candidate_symbol_diagnostics",
            "trade_candidates",
            "trade_signals",
            "trade_decisions",
            "trade_execution_intents",
            "trade_admissions",
        )

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
        entries_by_symbol = {self._normalize_symbol(entry.get("symbol")): dict(entry) for entry in selected_entries}
        entries_by_symbol = {symbol: entry for symbol, entry in entries_by_symbol.items() if symbol is not None}
        normalized_symbols = list(dict.fromkeys(str(symbol).upper() for symbol in symbols if str(symbol or "").strip()))
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
                observation_rank = self._optional_int(observation.get("rank"))
                if observation_rank is None and str(observation.get("observation_state") or "") == "selected":
                    observation_rank = rank
                if ticker is None:
                    ticker = TickerSourceObservationModel(
                        ticker_source_run_id=ticker_source_run_id,
                        ticker_source_id=ticker_source_id,
                        symbol=symbol,
                        observation_state=str(observation.get("observation_state") or "observed"),
                        rank=observation_rank,
                        score=self._optional_float(observation.get("score")),
                        company=self._optional_text(observation.get("company")),
                        sector=self._optional_text(observation.get("sector")),
                        industry=self._optional_text(observation.get("industry")),
                        country=self._optional_text(observation.get("country")),
                        price=self._optional_float(observation.get("price")),
                        market_cap=self._optional_int(observation.get("market_cap")),
                        daily_volume=self._optional_int(observation.get("daily_volume")),
                        move_percent=self._optional_float(observation.get("move_percent")),
                        relative_volume=self._optional_float(observation.get("relative_volume")),
                        reason_codes_json=self._text_list(observation.get("reason_codes") or entry.get("reason_codes")),
                        evidence_json=render_value(observation),
                        created_at=updated_at_dt,
                    )
                    session.add(ticker)
                else:
                    ticker.ticker_source_id = ticker_source_id
                    ticker.observation_state = str(observation.get("observation_state") or "observed")
                    ticker.rank = observation_rank
                    ticker.score = self._optional_float(observation.get("score"))
                    ticker.company = self._optional_text(observation.get("company"))
                    ticker.sector = self._optional_text(observation.get("sector"))
                    ticker.industry = self._optional_text(observation.get("industry"))
                    ticker.country = self._optional_text(observation.get("country"))
                    ticker.price = self._optional_float(observation.get("price"))
                    ticker.market_cap = self._optional_int(observation.get("market_cap"))
                    ticker.daily_volume = self._optional_int(observation.get("daily_volume"))
                    ticker.move_percent = self._optional_float(observation.get("move_percent"))
                    ticker.relative_volume = self._optional_float(observation.get("relative_volume"))
                    ticker.reason_codes_json = self._text_list(observation.get("reason_codes") or entry.get("reason_codes"))
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
            "degradation": {
                "status": snapshot_status,
                "reason": None if snapshot_status in {"ready", "empty"} else "snapshot_stale" if snapshot_status == "stale" else snapshot_status,
            },
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

    def upsert_candidate_run(
        self,
        *,
        candidate_run_id: str,
        run_key: str,
        trading_strategy_id: str,
        trade_structure: str,
        routine: str,
        ticker_source_run_id: str | None,
        ticker_source_kind: str,
        ticker_source_id: str,
        status: str,
        config_hash: str,
        generated_at: str,
        completed_at: str | None,
        symbol_count: int,
        candidate_count: int,
        summary: dict[str, Any],
        evidence: dict[str, Any],
        updated_at: str,
    ) -> StorageRow:
        generated_at_dt = parse_datetime(generated_at)
        completed_at_dt = parse_datetime(completed_at)
        updated_at_dt = parse_datetime(updated_at)
        if generated_at_dt is None or updated_at_dt is None:
            raise ValueError("generated_at and updated_at are required")
        with self.session_scope() as session:
            row = session.get(CandidateRunModel, candidate_run_id)
            if row is None:
                row = CandidateRunModel(
                    candidate_run_id=candidate_run_id,
                    run_key=run_key,
                    trading_strategy_id=trading_strategy_id,
                    trade_structure=trade_structure,
                    routine=routine,
                    ticker_source_run_id=ticker_source_run_id,
                    ticker_source_kind=ticker_source_kind,
                    ticker_source_id=ticker_source_id,
                    status=status,
                    config_hash=config_hash,
                    generated_at=generated_at_dt,
                    completed_at=completed_at_dt,
                    symbol_count=int(symbol_count),
                    candidate_count=int(candidate_count),
                    summary_json=render_value(summary),
                    evidence_json=render_value(evidence),
                    created_at=updated_at_dt,
                    updated_at=updated_at_dt,
                )
                session.add(row)
            else:
                row.run_key = run_key
                row.trading_strategy_id = trading_strategy_id
                row.trade_structure = trade_structure
                row.routine = routine
                row.ticker_source_run_id = ticker_source_run_id
                row.ticker_source_kind = ticker_source_kind
                row.ticker_source_id = ticker_source_id
                row.status = status
                row.config_hash = config_hash
                row.generated_at = generated_at_dt
                row.completed_at = completed_at_dt
                row.symbol_count = int(symbol_count)
                row.candidate_count = int(candidate_count)
                row.summary_json = render_value(summary)
                row.evidence_json = render_value(evidence)
                row.updated_at = updated_at_dt
            session.flush()
            session.refresh(row)
            return self.row(row)

    def replace_candidate_symbol_diagnostics(
        self,
        *,
        candidate_run_id: str,
        trading_strategy_id: str,
        trade_structure: str,
        routine: str,
        ticker_source_run_id: str | None,
        ticker_source_kind: str,
        ticker_source_id: str,
        diagnostics: list[dict[str, Any]],
        updated_at: str,
    ) -> list[StorageRow]:
        updated_at_dt = parse_datetime(updated_at)
        if updated_at_dt is None:
            raise ValueError("updated_at is required")

        rows: list[StorageRow] = []
        normalized_rows = [
            dict(row)
            for row in list(diagnostics or [])
            if isinstance(row, Mapping) and self._normalize_symbol(row.get("underlying_symbol") or row.get("symbol")) is not None
        ]
        with self.session_scope() as session:
            session.execute(delete(CandidateSymbolDiagnosticModel).where(CandidateSymbolDiagnosticModel.candidate_run_id == candidate_run_id))
            for raw in normalized_rows:
                symbol = self._normalize_symbol(raw.get("underlying_symbol") or raw.get("symbol"))
                if symbol is None:
                    continue
                observed_at_dt = parse_datetime(raw.get("observed_at")) or updated_at_dt
                row = CandidateSymbolDiagnosticModel(
                    candidate_run_id=candidate_run_id,
                    underlying_symbol=symbol,
                    trading_strategy_id=trading_strategy_id,
                    trade_structure=trade_structure,
                    routine=routine,
                    ticker_source_run_id=ticker_source_run_id,
                    ticker_source_kind=ticker_source_kind,
                    ticker_source_id=ticker_source_id,
                    diagnostic_status=str(raw.get("diagnostic_status") or raw.get("status") or "unknown"),
                    observed_at=observed_at_dt,
                    spot_price=self._optional_float(raw.get("spot_price")),
                    expiration_count=self._optional_int(raw.get("expiration_count")) or 0,
                    contract_count=self._optional_int(raw.get("contract_count")) or 0,
                    snapshot_count=self._optional_int(raw.get("snapshot_count")) or 0,
                    raw_candidate_count=self._optional_int(raw.get("raw_candidate_count")) or 0,
                    postprocess_candidate_count=self._optional_int(raw.get("postprocess_candidate_count")) or 0,
                    runtime_candidate_count=self._optional_int(raw.get("runtime_candidate_count")) or 0,
                    returned_candidate_count=self._optional_int(raw.get("returned_candidate_count")) or 0,
                    setup_json=render_value(raw.get("setup") or {}),
                    market_data_json=render_value(raw.get("market_data") or {}),
                    rejection_counts_json=render_value(raw.get("rejection_counts") or {}),
                    ranking_gate_json=render_value(raw.get("ranking_gate") or {}),
                    examples_json=render_value(raw.get("examples") or {}),
                    evidence_json=render_value(raw.get("evidence") or {}),
                    created_at=updated_at_dt,
                    updated_at=updated_at_dt,
                )
                session.add(row)
                session.flush()
                rows.append(self.row(row))
        return rows

    def list_candidate_symbol_diagnostics(
        self,
        *,
        candidate_run_id: str,
        limit: int = 100,
    ) -> list[StorageRow]:
        with self.session_factory() as session:
            rows = session.scalars(
                select(CandidateSymbolDiagnosticModel)
                .where(CandidateSymbolDiagnosticModel.candidate_run_id == candidate_run_id)
                .order_by(
                    CandidateSymbolDiagnosticModel.returned_candidate_count.desc(),
                    CandidateSymbolDiagnosticModel.postprocess_candidate_count.desc(),
                    CandidateSymbolDiagnosticModel.raw_candidate_count.desc(),
                    CandidateSymbolDiagnosticModel.underlying_symbol.asc(),
                )
                .limit(max(int(limit), 1))
            ).all()
        return self.rows(rows)

    def upsert_trade_candidate(
        self,
        *,
        trade_candidate_id: str,
        candidate_run_id: str,
        trading_strategy_id: str,
        trade_structure: str,
        routine: str,
        config_hash: str,
        underlying_symbol: str,
        root_symbol: str | None,
        candidate_identity: str,
        rank: int | None,
        score: float | None,
        confidence: float | None,
        expiration_date: str | date | None,
        selection_state: str | None,
        candidate_state: str,
        observed_at: str,
        expires_at: str | None,
        legs: list[dict[str, Any]],
        execution_shape: dict[str, Any],
        economics: dict[str, Any],
        risk_hints: dict[str, Any],
        reason_codes: list[str],
        blockers: list[str],
        candidate: dict[str, Any],
        evidence: dict[str, Any],
        updated_at: str,
    ) -> StorageRow:
        observed_at_dt = parse_datetime(observed_at)
        expires_at_dt = parse_datetime(expires_at)
        updated_at_dt = parse_datetime(updated_at)
        if observed_at_dt is None or updated_at_dt is None:
            raise ValueError("observed_at and updated_at are required")
        with self.session_scope() as session:
            row = session.get(TradeCandidateModel, trade_candidate_id)
            if row is None:
                row = TradeCandidateModel(
                    trade_candidate_id=trade_candidate_id,
                    candidate_run_id=candidate_run_id,
                    trading_strategy_id=trading_strategy_id,
                    trade_structure=trade_structure,
                    routine=routine,
                    config_hash=config_hash,
                    underlying_symbol=underlying_symbol.upper(),
                    root_symbol=root_symbol,
                    candidate_identity=candidate_identity,
                    rank=rank,
                    score=score,
                    confidence=confidence,
                    expiration_date=_optional_date(expiration_date),
                    selection_state=selection_state,
                    candidate_state=candidate_state,
                    observed_at=observed_at_dt,
                    expires_at=expires_at_dt,
                    legs_json=render_value(legs),
                    execution_shape_json=render_value(execution_shape),
                    economics_json=render_value(economics),
                    risk_hints_json=render_value(risk_hints),
                    reason_codes_json=list(reason_codes),
                    blockers_json=list(blockers),
                    candidate_json=render_value(candidate),
                    evidence_json=render_value(evidence),
                    created_at=updated_at_dt,
                    updated_at=updated_at_dt,
                )
                session.add(row)
            else:
                row.candidate_run_id = candidate_run_id
                row.trading_strategy_id = trading_strategy_id
                row.trade_structure = trade_structure
                row.routine = routine
                row.config_hash = config_hash
                row.underlying_symbol = underlying_symbol.upper()
                row.root_symbol = root_symbol
                row.candidate_identity = candidate_identity
                row.rank = rank
                row.score = score
                row.confidence = confidence
                row.expiration_date = _optional_date(expiration_date)
                row.selection_state = selection_state
                row.candidate_state = candidate_state
                row.observed_at = observed_at_dt
                row.expires_at = expires_at_dt
                row.legs_json = render_value(legs)
                row.execution_shape_json = render_value(execution_shape)
                row.economics_json = render_value(economics)
                row.risk_hints_json = render_value(risk_hints)
                row.reason_codes_json = list(reason_codes)
                row.blockers_json = list(blockers)
                row.candidate_json = render_value(candidate)
                row.evidence_json = render_value(evidence)
                row.updated_at = updated_at_dt
            session.flush()
            session.refresh(row)
            return self.row(row)

    def upsert_trade_signal(
        self,
        *,
        trade_signal_id: str,
        idempotency_key: str,
        trade_candidate_id: str | None,
        source_kind: str,
        source_id: str,
        trading_strategy_id: str,
        trade_structure: str,
        routine: str,
        config_hash: str,
        session_date: str | date,
        market_session: str,
        observed_at: str,
        expires_at: str | None,
        underlying_symbol: str,
        root_symbol: str | None,
        asset_class: str | None,
        product_class: str | None,
        horizon: str | None,
        style_profile: str | None,
        signal_state: str,
        rank: int | None,
        score: float | None,
        confidence: float | None,
        legs: list[dict[str, Any]],
        execution_shape: dict[str, Any],
        economics: dict[str, Any],
        reason_codes: list[str],
        blockers: list[str],
        evidence: dict[str, Any],
        metrics: dict[str, Any],
        updated_at: str,
    ) -> StorageRow:
        observed_at_dt = parse_datetime(observed_at)
        expires_at_dt = parse_datetime(expires_at)
        updated_at_dt = parse_datetime(updated_at)
        if observed_at_dt is None or updated_at_dt is None:
            raise ValueError("observed_at and updated_at are required")
        with self.session_scope() as session:
            row = session.get(TradeSignalModel, trade_signal_id)
            if row is None:
                row = TradeSignalModel(
                    trade_signal_id=trade_signal_id,
                    idempotency_key=idempotency_key,
                    trade_candidate_id=trade_candidate_id,
                    source_kind=source_kind,
                    source_id=source_id,
                    trading_strategy_id=trading_strategy_id,
                    routine=routine,
                    config_hash=config_hash,
                    account_id=None,
                    session_date=parse_date(session_date),
                    market_session=market_session,
                    observed_at=observed_at_dt,
                    expires_at=expires_at_dt,
                    underlying_symbol=underlying_symbol.upper(),
                    root_symbol=root_symbol,
                    asset_class=asset_class,
                    trade_structure=trade_structure,
                    product_class=product_class,
                    horizon=horizon,
                    style_profile=style_profile,
                    signal_state=signal_state,
                    rank=rank,
                    score=score,
                    confidence=confidence,
                    legs_json=render_value(legs),
                    execution_shape_json=render_value(execution_shape),
                    economics_json=render_value(economics),
                    reason_codes_json=list(reason_codes),
                    blockers_json=list(blockers),
                    evidence_json=render_value(evidence),
                    metrics_json=render_value(metrics),
                    created_at=updated_at_dt,
                    updated_at=updated_at_dt,
                )
                session.add(row)
            else:
                row.idempotency_key = idempotency_key
                row.trade_candidate_id = trade_candidate_id
                row.source_kind = source_kind
                row.source_id = source_id
                row.trading_strategy_id = trading_strategy_id
                row.routine = routine
                row.config_hash = config_hash
                row.session_date = parse_date(session_date)
                row.market_session = market_session
                row.observed_at = observed_at_dt
                row.expires_at = expires_at_dt
                row.underlying_symbol = underlying_symbol.upper()
                row.root_symbol = root_symbol
                row.asset_class = asset_class
                row.trade_structure = trade_structure
                row.product_class = product_class
                row.horizon = horizon
                row.style_profile = style_profile
                row.signal_state = signal_state
                row.rank = rank
                row.score = score
                row.confidence = confidence
                row.legs_json = render_value(legs)
                row.execution_shape_json = render_value(execution_shape)
                row.economics_json = render_value(economics)
                row.reason_codes_json = list(reason_codes)
                row.blockers_json = list(blockers)
                row.evidence_json = render_value(evidence)
                row.metrics_json = render_value(metrics)
                row.updated_at = updated_at_dt
            session.flush()
            session.refresh(row)
            return self.row(row)

    def upsert_trade_decision(
        self,
        *,
        trade_decision_id: str,
        trade_signal_id: str,
        trading_strategy_id: str,
        trade_structure: str,
        routine: str,
        config_hash: str,
        run_key: str,
        scope_key: str,
        decision_state: str,
        rank: int | None,
        score: float | None,
        selected_quantity: int | None,
        selected_execution_shape: dict[str, Any],
        reason_codes: list[str],
        blockers: list[str],
        evidence: dict[str, Any],
        metrics: dict[str, Any],
        supersedes_decision_id: str | None,
        superseded_by_decision_id: str | None,
        decided_at: str,
    ) -> StorageRow:
        decided_at_dt = parse_datetime(decided_at)
        if decided_at_dt is None:
            raise ValueError("decided_at is required")
        with self.session_scope() as session:
            row = session.get(TradeDecisionModel, trade_decision_id)
            if row is None:
                row = TradeDecisionModel(
                    trade_decision_id=trade_decision_id,
                    trade_signal_id=trade_signal_id,
                    trading_strategy_id=trading_strategy_id,
                    trade_structure=trade_structure,
                    routine=routine,
                    config_hash=config_hash,
                    run_key=run_key,
                    scope_key=scope_key,
                    decision_state=decision_state,
                    rank=rank,
                    score=score,
                    selected_quantity=selected_quantity,
                    selected_execution_shape_json=render_value(selected_execution_shape),
                    reason_codes_json=list(reason_codes),
                    blockers_json=list(blockers),
                    evidence_json=render_value(evidence),
                    metrics_json=render_value(metrics),
                    supersedes_decision_id=supersedes_decision_id,
                    superseded_by_decision_id=superseded_by_decision_id,
                    decided_at=decided_at_dt,
                )
                session.add(row)
            else:
                row.trade_signal_id = trade_signal_id
                row.trading_strategy_id = trading_strategy_id
                row.trade_structure = trade_structure
                row.routine = routine
                row.config_hash = config_hash
                row.run_key = run_key
                row.scope_key = scope_key
                row.decision_state = decision_state
                row.rank = rank
                row.score = score
                row.selected_quantity = selected_quantity
                row.selected_execution_shape_json = render_value(selected_execution_shape)
                row.reason_codes_json = list(reason_codes)
                row.blockers_json = list(blockers)
                row.evidence_json = render_value(evidence)
                row.metrics_json = render_value(metrics)
                row.supersedes_decision_id = supersedes_decision_id
                row.superseded_by_decision_id = superseded_by_decision_id
                row.decided_at = decided_at_dt
            session.flush()
            session.refresh(row)
            return self.row(row)

    def get_trade_signal(self, trade_signal_id: str) -> StorageRow | None:
        with self.session_factory() as session:
            row = session.get(TradeSignalModel, trade_signal_id)
        if row is None:
            return None
        return self.row(row)

    def get_trade_decision(self, trade_decision_id: str) -> StorageRow | None:
        with self.session_factory() as session:
            row = session.get(TradeDecisionModel, trade_decision_id)
        if row is None:
            return None
        return self.row(row)

    def get_trade_decision_with_signal(self, trade_decision_id: str) -> TradeDecisionSignalRead | None:
        statement = (
            select(TradeDecisionModel, TradeSignalModel)
            .join(TradeSignalModel, TradeDecisionModel.trade_signal_id == TradeSignalModel.trade_signal_id)
            .where(TradeDecisionModel.trade_decision_id == trade_decision_id)
            .limit(1)
        )
        with self.session_factory() as session:
            row = session.execute(statement).first()
        if row is None:
            return None
        decision, signal = row
        return TradeDecisionSignalRead.from_rows(
            decision=self.row(decision),
            signal=self.row(signal),
        )

    def list_trade_signals(
        self,
        *,
        signal_states: list[str] | None = None,
        routine: str | None = None,
        as_of: str | None = None,
        limit: int = 100,
    ) -> list[StorageRow]:
        as_of_dt = parse_datetime(as_of)
        statement = select(TradeSignalModel)
        if signal_states:
            statement = statement.where(TradeSignalModel.signal_state.in_(signal_states))
        if routine is not None:
            statement = statement.where(TradeSignalModel.routine == routine)
        if as_of_dt is not None:
            statement = statement.where(or_(TradeSignalModel.expires_at.is_(None), TradeSignalModel.expires_at > as_of_dt))
        statement = statement.order_by(
            TradeSignalModel.score.desc().nullslast(),
            TradeSignalModel.rank.asc().nullslast(),
            TradeSignalModel.updated_at.desc(),
            TradeSignalModel.trade_signal_id.asc(),
        ).limit(max(int(limit), 1))
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def list_trade_decisions_with_signals(
        self,
        *,
        decision_states: list[str] | None = None,
        trading_strategy_ids: list[str] | None = None,
        routine: str | None = None,
        session_date: str | date | None = None,
        as_of: str | None = None,
        limit: int = 100,
    ) -> list[StorageRow]:
        as_of_dt = parse_datetime(as_of)
        statement = select(TradeDecisionModel, TradeSignalModel).join(
            TradeSignalModel, TradeDecisionModel.trade_signal_id == TradeSignalModel.trade_signal_id
        )
        if decision_states:
            statement = statement.where(TradeDecisionModel.decision_state.in_(decision_states))
        if trading_strategy_ids:
            statement = statement.where(TradeDecisionModel.trading_strategy_id.in_(trading_strategy_ids))
        if routine is not None:
            statement = statement.where(TradeDecisionModel.routine == routine)
        if session_date is not None:
            statement = statement.where(TradeSignalModel.session_date == parse_date(session_date))
        if as_of_dt is not None:
            statement = statement.where(or_(TradeSignalModel.expires_at.is_(None), TradeSignalModel.expires_at > as_of_dt))
        statement = statement.order_by(
            TradeDecisionModel.score.desc().nullslast(),
            TradeDecisionModel.rank.asc().nullslast(),
            TradeDecisionModel.decided_at.desc(),
            TradeDecisionModel.trade_decision_id.asc(),
        ).limit(max(int(limit), 1))
        with self.session_factory() as session:
            rows = session.execute(statement).all()
        return [
            {
                "trade_decision": self.row(decision),
                "trade_signal": self.row(signal),
            }
            for decision, signal in rows
        ]

    def list_trade_decisions(
        self,
        *,
        trading_strategy_id: str | None = None,
        decision_states: list[str] | None = None,
        routine: str | None = None,
        limit: int = 200,
    ) -> list[StorageRow]:
        statement = select(TradeDecisionModel)
        if trading_strategy_id is not None:
            statement = statement.where(TradeDecisionModel.trading_strategy_id == trading_strategy_id)
        if decision_states:
            statement = statement.where(TradeDecisionModel.decision_state.in_(decision_states))
        if routine is not None:
            statement = statement.where(TradeDecisionModel.routine == routine)
        statement = statement.order_by(
            TradeDecisionModel.decided_at.desc(),
            TradeDecisionModel.trade_decision_id.asc(),
        ).limit(max(int(limit), 1))
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return self.rows(rows)

    def upsert_trade_execution_intent(
        self,
        *,
        execution_intent_id: str,
        intent_kind: str,
        source_object_type: str,
        source_object_id: str,
        trade_signal_id: str | None,
        trade_decision_id: str | None,
        position_id: str | None,
        trading_strategy_id: str | None,
        trade_structure: str | None,
        routine: str | None,
        account_id: str | None,
        slot_key: str,
        idempotency_key: str,
        intent_state: str,
        claim_token: str | None,
        claimed_at: str | None,
        expires_at: str | None,
        supersedes_intent_id: str | None,
        superseded_by_intent_id: str | None,
        payload: dict[str, Any],
        policy_snapshot: dict[str, Any],
        config_hash: str | None,
        created_at: str,
        updated_at: str,
    ) -> StorageRow:
        created_at_dt = parse_datetime(created_at)
        updated_at_dt = parse_datetime(updated_at)
        claimed_at_dt = parse_datetime(claimed_at)
        expires_at_dt = parse_datetime(expires_at)
        if created_at_dt is None or updated_at_dt is None:
            raise ValueError("created_at and updated_at are required")
        with self.session_scope() as session:
            row = session.get(TradeExecutionIntentModel, execution_intent_id)
            if row is None:
                row = TradeExecutionIntentModel(
                    execution_intent_id=execution_intent_id,
                    intent_kind=intent_kind,
                    source_object_type=source_object_type,
                    source_object_id=source_object_id,
                    trade_signal_id=trade_signal_id,
                    trade_decision_id=trade_decision_id,
                    position_id=position_id,
                    trading_strategy_id=trading_strategy_id,
                    trade_structure=trade_structure,
                    routine=routine,
                    account_id=account_id,
                    slot_key=slot_key,
                    idempotency_key=idempotency_key,
                    intent_state=intent_state,
                    claim_token=claim_token,
                    claimed_at=claimed_at_dt,
                    expires_at=expires_at_dt,
                    supersedes_intent_id=supersedes_intent_id,
                    superseded_by_intent_id=superseded_by_intent_id,
                    payload_json=render_value(payload),
                    policy_snapshot_json=render_value(policy_snapshot),
                    config_hash=config_hash,
                    created_at=created_at_dt,
                    updated_at=updated_at_dt,
                )
                session.add(row)
            else:
                row.intent_kind = intent_kind
                row.source_object_type = source_object_type
                row.source_object_id = source_object_id
                row.trade_signal_id = trade_signal_id
                row.trade_decision_id = trade_decision_id
                row.position_id = position_id
                row.trading_strategy_id = trading_strategy_id
                row.trade_structure = trade_structure
                row.routine = routine
                row.account_id = account_id
                row.slot_key = slot_key
                row.idempotency_key = idempotency_key
                row.intent_state = intent_state
                row.claim_token = claim_token
                row.claimed_at = claimed_at_dt
                row.expires_at = expires_at_dt
                row.supersedes_intent_id = supersedes_intent_id
                row.superseded_by_intent_id = superseded_by_intent_id
                row.payload_json = render_value(payload)
                row.policy_snapshot_json = render_value(policy_snapshot)
                row.config_hash = config_hash
                row.updated_at = updated_at_dt
            session.flush()
            session.refresh(row)
            return self.row(row)

    def upsert_trade_admission(
        self,
        *,
        admission_decision_id: str,
        execution_intent_id: str,
        trade_signal_id: str | None,
        trade_decision_id: str | None,
        position_id: str | None,
        admission_kind: str,
        admission_state: str,
        account_id: str | None,
        session_date: str | date,
        requested_quantity: int | None,
        requested_notional: float | None,
        max_loss: float | None,
        policy_snapshot: dict[str, Any],
        capability_snapshot: dict[str, Any],
        metrics: dict[str, Any],
        reason_codes: list[str],
        blockers: list[str],
        evidence: dict[str, Any],
        note: str | None,
        execution_attempt_id: str | None,
        decided_at: str,
    ) -> StorageRow:
        decided_at_dt = parse_datetime(decided_at)
        if decided_at_dt is None:
            raise ValueError("decided_at is required")
        with self.session_scope() as session:
            row = session.get(TradeAdmissionModel, admission_decision_id)
            if row is None:
                row = TradeAdmissionModel(
                    admission_decision_id=admission_decision_id,
                    execution_intent_id=execution_intent_id,
                    trade_signal_id=trade_signal_id,
                    trade_decision_id=trade_decision_id,
                    position_id=position_id,
                    admission_kind=admission_kind,
                    admission_state=admission_state,
                    account_id=account_id,
                    session_date=parse_date(session_date),
                    requested_quantity=requested_quantity,
                    requested_notional=requested_notional,
                    max_loss=max_loss,
                    policy_snapshot_json=render_value(policy_snapshot),
                    capability_snapshot_json=render_value(capability_snapshot),
                    metrics_json=render_value(metrics),
                    reason_codes_json=list(reason_codes),
                    blockers_json=list(blockers),
                    evidence_json=render_value(evidence),
                    note=note,
                    execution_attempt_id=execution_attempt_id,
                    decided_at=decided_at_dt,
                )
                session.add(row)
            else:
                row.execution_intent_id = execution_intent_id
                row.trade_signal_id = trade_signal_id
                row.trade_decision_id = trade_decision_id
                row.position_id = position_id
                row.admission_kind = admission_kind
                row.admission_state = admission_state
                row.account_id = account_id
                row.session_date = parse_date(session_date)
                row.requested_quantity = requested_quantity
                row.requested_notional = requested_notional
                row.max_loss = max_loss
                row.policy_snapshot_json = render_value(policy_snapshot)
                row.capability_snapshot_json = render_value(capability_snapshot)
                row.metrics_json = render_value(metrics)
                row.reason_codes_json = list(reason_codes)
                row.blockers_json = list(blockers)
                row.evidence_json = render_value(evidence)
                row.note = note
                row.execution_attempt_id = execution_attempt_id
                row.decided_at = decided_at_dt
            session.flush()
            session.refresh(row)
            return self.row(row)

    def attach_trade_admission_attempt(
        self,
        *,
        admission_decision_id: str,
        execution_attempt_id: str,
    ) -> StorageRow | None:
        with self.session_scope() as session:
            row = session.get(TradeAdmissionModel, admission_decision_id)
            if row is None:
                return None
            row.execution_attempt_id = execution_attempt_id
            session.flush()
            session.refresh(row)
            return self.row(row)

    @classmethod
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
            symbol = cls._normalize_symbol(raw.get("symbol"))
            if symbol is None:
                continue
            row = dict(raw)
            row["symbol"] = symbol
            state = cls._optional_text(row.get("observation_state") or row.get("state"))
            row["observation_state"] = (state or ("selected" if symbol in selected_symbols else "observed")).strip().lower()
            rows[symbol] = row

        for rank, raw in enumerate(selected_entries, start=1):
            symbol = cls._normalize_symbol(raw.get("symbol"))
            if symbol is None:
                continue
            row = rows.get(symbol, dict(raw))
            row["symbol"] = symbol
            row["observation_state"] = "selected"
            row["rank"] = rank
            row.setdefault("score", raw.get("score"))
            row.setdefault("reason_codes", raw.get("reason_codes"))
            rows[symbol] = row

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

    @staticmethod
    def _normalize_symbol(value: Any) -> str | None:
        rendered = str(value or "").upper().strip()
        return rendered or None

    @staticmethod
    def _optional_text(value: Any) -> str | None:
        rendered = str(value or "").strip()
        return rendered or None

    @staticmethod
    def _optional_int(value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _optional_float(value: Any) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _text_list(value: Any) -> list[str]:
        if not isinstance(value, (list, tuple)):
            return []
        normalized: list[str] = []
        for item in value:
            rendered = str(item or "").strip()
            if rendered and rendered not in normalized:
                normalized.append(rendered)
        return normalized


__all__ = ["EngineFactRepository"]
