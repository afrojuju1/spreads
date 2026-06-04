from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import select

from core.storage.base import RepositoryBase
from core.storage.engine_models import CandidateRunModel, SourceRunModel, SourceTickerModel, TradeCandidateModel
from core.storage.lifecycle_models import TradeSignalModel
from core.storage.records import StorageRow
from core.storage.serializers import parse_date, parse_datetime, render_value


def _optional_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    return parse_date(value)


class EngineFactRepository(RepositoryBase):
    def schema_ready(self) -> bool:
        return self.schema_has_tables(
            "source_runs",
            "source_tickers",
            "candidate_runs",
            "trade_candidates",
            "trade_signals",
        )

    def upsert_source_run(
        self,
        *,
        source_run_id: str,
        source_type: str,
        source_ref: str,
        source_job_run_id: str | None,
        status: str,
        config_hash: str | None,
        generated_at: str,
        completed_at: str | None,
        symbols: list[str],
        entries: list[dict[str, Any]],
        summary: dict[str, Any],
        evidence: dict[str, Any],
        updated_at: str,
    ) -> StorageRow:
        generated_at_dt = parse_datetime(generated_at)
        completed_at_dt = parse_datetime(completed_at)
        updated_at_dt = parse_datetime(updated_at)
        if generated_at_dt is None or updated_at_dt is None:
            raise ValueError("generated_at and updated_at are required")
        entries_by_symbol = {
            str(entry.get("symbol") or "").upper(): dict(entry)
            for entry in list(entries or [])
            if str(entry.get("symbol") or "").strip()
        }
        normalized_symbols = list(dict.fromkeys(str(symbol).upper() for symbol in symbols if str(symbol or "").strip()))
        with self.session_scope() as session:
            row = session.get(SourceRunModel, source_run_id)
            if row is None:
                row = SourceRunModel(
                    source_run_id=source_run_id,
                    source_type=source_type,
                    source_ref=source_ref,
                    source_job_run_id=source_job_run_id,
                    status=status,
                    config_hash=config_hash,
                    generated_at=generated_at_dt,
                    completed_at=completed_at_dt,
                    symbol_count=len(normalized_symbols),
                    summary_json=render_value(summary),
                    evidence_json=render_value(evidence),
                    created_at=updated_at_dt,
                    updated_at=updated_at_dt,
                )
                session.add(row)
            else:
                row.source_type = source_type
                row.source_ref = source_ref
                row.source_job_run_id = source_job_run_id
                row.status = status
                row.config_hash = config_hash
                row.generated_at = generated_at_dt
                row.completed_at = completed_at_dt
                row.symbol_count = len(normalized_symbols)
                row.summary_json = render_value(summary)
                row.evidence_json = render_value(evidence)
                row.updated_at = updated_at_dt

            for rank, symbol in enumerate(normalized_symbols, start=1):
                ticker = session.scalar(
                    select(SourceTickerModel).where(
                        SourceTickerModel.source_run_id == source_run_id,
                        SourceTickerModel.symbol == symbol,
                    )
                )
                entry = entries_by_symbol.get(symbol, {})
                if ticker is None:
                    ticker = SourceTickerModel(
                        source_run_id=source_run_id,
                        source_ref=source_ref,
                        symbol=symbol,
                        rank=rank,
                        score=self._optional_float(entry.get("score")),
                        reason_codes_json=self._text_list(entry.get("reason_codes")),
                        evidence_json=render_value(entry),
                        created_at=updated_at_dt,
                    )
                    session.add(ticker)
                else:
                    ticker.source_ref = source_ref
                    ticker.rank = rank
                    ticker.score = self._optional_float(entry.get("score"))
                    ticker.reason_codes_json = self._text_list(entry.get("reason_codes"))
                    ticker.evidence_json = render_value(entry)

            session.flush()
            session.refresh(row)
            return self.row(row)

    def upsert_candidate_run(
        self,
        *,
        candidate_run_id: str,
        run_key: str,
        trading_strategy_id: str,
        trade_structure: str,
        routine: str,
        source_run_id: str | None,
        source_type: str,
        source_ref: str,
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
                    source_run_id=source_run_id,
                    source_type=source_type,
                    source_ref=source_ref,
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
                row.source_run_id = source_run_id
                row.source_type = source_type
                row.source_ref = source_ref
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

    @staticmethod
    def _optional_float(value: Any) -> float | None:
        if value in (None, ""):
            return None
        return float(value)

    @staticmethod
    def _text_list(value: Any) -> list[str]:
        if not isinstance(value, (list, tuple)):
            return []
        return [str(item) for item in value if str(item or "").strip()]


__all__ = ["EngineFactRepository"]
