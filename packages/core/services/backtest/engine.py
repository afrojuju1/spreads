from __future__ import annotations

from typing import Any
from uuid import uuid4

from core.db.decorators import with_storage
from core.services.backtest.experiments import resolve_backtest_artifact_root, write_json_artifact
from core.services.backtest.execution_simulation import build_execution_simulation_backtest
from core.services.backtest.models import BacktestArtifactKind, BacktestMode, BacktestRequest, BacktestRunState
from core.services.backtest.strategy_rerun import build_strategy_rerun_backtest
from core.services.backtest.stored_facts import build_stored_facts_backtest
from core.services.backtest.strategy_scope import load_backtest_strategy_scope, strategy_scope_snapshot
from core.storage.serializers import parse_date
from core.value_coercion import as_mapping, coerce_float, coerce_int, utc_now_iso


def _result_fidelity(result: dict[str, Any]) -> dict[str, Any]:
    return dict(as_mapping(result.get("fidelity_labels")))


def _variant_rank_by_net_pnl(result: dict[str, Any]) -> dict[str, int]:
    rankings = as_mapping(as_mapping(result.get("comparison")).get("rankings"))
    rows = rankings.get("net_pnl") or []
    if not isinstance(rows, list):
        return {}
    ranks: dict[str, int] = {}
    for offset, row in enumerate(rows, start=1):
        variant_id = str(as_mapping(row).get("variant_id") or "").strip()
        if variant_id:
            ranks[variant_id] = offset
    return ranks


def _strategy_result_metrics(strategy_result: dict[str, Any]) -> dict[str, Any]:
    candidate_productivity = as_mapping(strategy_result.get("candidate_productivity"))
    selection_quality = as_mapping(strategy_result.get("selection_quality"))
    admissions = as_mapping(strategy_result.get("admissions"))
    execution = as_mapping(strategy_result.get("execution"))
    exits = as_mapping(strategy_result.get("exits"))
    pnl = as_mapping(strategy_result.get("pnl"))
    return {
        "candidate_run_count": coerce_int(candidate_productivity.get("candidate_run_count")) or 0,
        "trade_candidate_count": coerce_int(candidate_productivity.get("trade_candidate_count")) or 0,
        "signal_count": coerce_int(selection_quality.get("signal_count")) or 0,
        "decision_count": coerce_int(selection_quality.get("decision_count")) or 0,
        "selected_count": coerce_int(selection_quality.get("selected_count")) or 0,
        "admission_count": coerce_int(admissions.get("admission_count")) or 0,
        "approved_count": coerce_int(admissions.get("approved_count")) or 0,
        "attempt_count": coerce_int(execution.get("attempt_count")) or 0,
        "fill_count": coerce_int(execution.get("fill_count")) or 0,
        "close_decision_count": coerce_int(exits.get("close_decision_count")) or 0,
        "close_count": coerce_int(exits.get("close_count")) or 0,
        "net_pnl": coerce_float(pnl.get("net_pnl")) or 0.0,
    }


class BacktestEngine:
    @with_storage()
    def run(
        self,
        request: BacktestRequest,
        *,
        db_target: str | None = None,
        storage: Any | None = None,
    ) -> dict[str, Any]:
        backtest_run_id = f"bt_{uuid4().hex}"
        started_at = utc_now_iso()
        start_date = parse_date(request.start_date).isoformat()
        end_date = parse_date(request.end_date or request.start_date).isoformat()
        resolved_db_target = str(db_target or getattr(storage, "database_url", "") or "")
        strategies = load_backtest_strategy_scope(request.strategy_ids)
        strategy_ids = tuple(strategies)
        config_snapshot = strategy_scope_snapshot(strategies)
        resolved_artifact_root = str(resolve_backtest_artifact_root(request.artifact_root))

        backtests = storage.backtests
        persistence_ready = backtests.schema_ready()
        if persistence_ready:
            backtests.create_run(
                backtest_run_id=backtest_run_id,
                mode=request.mode.value,
                state=BacktestRunState.RUNNING.value,
                requested_by=request.requested_by,
                strategy_ids=list(strategy_ids),
                start_date=start_date,
                end_date=end_date,
                config_snapshot=config_snapshot,
                request=request.to_payload(),
                artifact_root=resolved_artifact_root,
                created_at=started_at,
                started_at=started_at,
            )

        try:
            if request.mode == BacktestMode.STORED_FACTS:
                result = build_stored_facts_backtest(
                    start_date=start_date,
                    end_date=end_date,
                    strategy_ids=strategy_ids,
                    max_days=request.max_days,
                    market_data_symbol_limit=request.market_data_symbol_limit,
                    storage=storage,
                )
                comparison_mode = "stored_facts_current_catalog"
            elif request.mode == BacktestMode.STRATEGY_RERUN:
                result = build_strategy_rerun_backtest(
                    start_date=start_date,
                    end_date=end_date,
                    strategy_ids=strategy_ids,
                    symbols=request.symbols,
                    max_days=request.max_days,
                    market_data_symbol_limit=request.market_data_symbol_limit,
                    candidate_limit=request.candidate_limit,
                    per_symbol_top=request.per_symbol_top,
                    storage=storage,
                    db_target=resolved_db_target,
                )
                comparison_mode = "strategy_rerun_current_config"
            elif request.mode == BacktestMode.EXECUTION_SIMULATION:
                result = build_execution_simulation_backtest(
                    start_date=start_date,
                    end_date=end_date,
                    strategy_ids=strategy_ids,
                    symbols=request.symbols,
                    max_days=request.max_days,
                    market_data_symbol_limit=request.market_data_symbol_limit,
                    candidate_limit=request.candidate_limit,
                    per_symbol_top=request.per_symbol_top,
                    storage=storage,
                    db_target=resolved_db_target,
                )
                comparison_mode = "execution_simulation_current_config"
            else:
                raise ValueError(f"Unsupported backtest mode: {request.mode}")
            result = dict(result)
            result["backtest_run_id"] = backtest_run_id
            result["request"] = request.to_payload()
            result["config_snapshot"] = config_snapshot
            artifact_records: list[dict[str, Any]] = []
            variant_records: list[dict[str, Any]] = []
            completed_at = utc_now_iso()
            if persistence_ready:
                artifact = write_json_artifact(
                    artifact_root=request.artifact_root,
                    backtest_run_id=backtest_run_id,
                    artifact_kind=BacktestArtifactKind.RESULT,
                    payload=result,
                    metadata={
                        "mode": request.mode.value,
                        "start_date": start_date,
                        "end_date": end_date,
                    },
                )
                artifact_records.append(
                    backtests.record_artifact(
                        backtest_artifact_id=artifact.backtest_artifact_id,
                        backtest_run_id=backtest_run_id,
                        artifact_kind=artifact.artifact_kind,
                        storage_kind=artifact.storage_kind,
                        uri=artifact.uri,
                        content_type=artifact.content_type,
                        row_count=artifact.row_count,
                        byte_count=artifact.byte_count,
                        schema=artifact.payload_schema,
                        metadata=artifact.metadata,
                        created_at=completed_at,
                    )
                )
                variant_rank = _variant_rank_by_net_pnl(result)
                for strategy_result in result.get("strategies") or []:
                    if not isinstance(strategy_result, dict):
                        continue
                    variant_id = str(strategy_result.get("variant_id") or "").strip()
                    trading_strategy_id = str(strategy_result.get("trading_strategy_id") or "").strip()
                    config_hash = str(strategy_result.get("config_hash") or "").strip()
                    if not variant_id or not trading_strategy_id:
                        continue
                    variant_records.append(
                        backtests.record_variant_result(
                            backtest_variant_id=f"btv_{uuid4().hex}",
                            backtest_run_id=backtest_run_id,
                            trading_strategy_id=trading_strategy_id,
                            config_hash=config_hash,
                            variant_hash=variant_id,
                            parameters={
                                "variant_id": variant_id,
                                "comparison_mode": comparison_mode,
                            },
                            summary={
                                "outcome_label": strategy_result.get("outcome_label"),
                                "market_dates": strategy_result.get("market_dates"),
                            },
                            metrics=_strategy_result_metrics(strategy_result),
                            fidelity=dict(as_mapping(strategy_result.get("fidelity_labels"))),
                            rank=variant_rank.get(variant_id),
                            created_at=completed_at,
                        )
                    )
                run_record = backtests.complete_run(
                    backtest_run_id=backtest_run_id,
                    summary=dict(as_mapping(result.get("summary"))),
                    fidelity=_result_fidelity(result),
                    completed_at=completed_at,
                )
                result["run"] = run_record
            result["artifacts"] = artifact_records
            result["variant_results"] = variant_records
            result["persistence"] = {
                "state": BacktestRunState.COMPLETED.value if persistence_ready else "schema_unavailable",
                "artifact_count": len(artifact_records),
                "variant_result_count": len(variant_records),
            }
            return result
        except Exception as exc:
            if persistence_ready:
                backtests.fail_run(
                    backtest_run_id=backtest_run_id,
                    error_text=str(exc)[:4000],
                    completed_at=utc_now_iso(),
                )
            raise


__all__ = ["BacktestEngine"]
