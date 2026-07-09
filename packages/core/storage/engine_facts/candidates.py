from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, select

from core.storage.engine_models import (
    CandidateRunModel,
    CandidateSymbolDiagnosticModel,
    TradeCandidateModel,
)
from core.storage.records import StorageRow
from core.storage.serializers import parse_date, parse_datetime, render_value
from core.value_coercion import as_text

if TYPE_CHECKING:
    pass


from core.storage.engine_facts.contracts import (
    CandidateSymbolDiagnosticPayload,
)

class EngineFactCandidateMixin:
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
        normalized_rows: list[CandidateSymbolDiagnosticPayload] = []
        for raw_diagnostic in list(diagnostics or []):
            if not isinstance(raw_diagnostic, Mapping):
                continue
            try:
                normalized_rows.append(CandidateSymbolDiagnosticPayload.model_validate(raw_diagnostic))
            except ValueError:
                continue
        with self.session_scope() as session:
            session.execute(delete(CandidateSymbolDiagnosticModel).where(CandidateSymbolDiagnosticModel.candidate_run_id == candidate_run_id))
            for diagnostic in normalized_rows:
                raw = diagnostic.model_dump()
                observed_at_dt = diagnostic.observed_at or updated_at_dt
                row = CandidateSymbolDiagnosticModel(
                    candidate_run_id=candidate_run_id,
                    underlying_symbol=diagnostic.underlying_symbol,
                    trading_strategy_id=trading_strategy_id,
                    trade_structure=trade_structure,
                    routine=routine,
                    ticker_source_run_id=ticker_source_run_id,
                    ticker_source_kind=ticker_source_kind,
                    ticker_source_id=ticker_source_id,
                    diagnostic_status=diagnostic.diagnostic_status,
                    observed_at=observed_at_dt,
                    spot_price=diagnostic.spot_price,
                    expiration_count=diagnostic.expiration_count,
                    contract_count=diagnostic.contract_count,
                    snapshot_count=diagnostic.snapshot_count,
                    raw_candidate_count=diagnostic.raw_candidate_count,
                    postprocess_candidate_count=diagnostic.postprocess_candidate_count,
                    runtime_candidate_count=diagnostic.runtime_candidate_count,
                    returned_candidate_count=diagnostic.returned_candidate_count,
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
                    expiration_date=None if as_text(expiration_date) is None else parse_date(expiration_date),
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
                row.expiration_date = None if as_text(expiration_date) is None else parse_date(expiration_date)
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


__all__ = ["EngineFactCandidateMixin"]
