from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import date
import hashlib
from importlib import util
import itertools
import json
from pathlib import Path
from typing import Any

from core.services.backtest.execution_simulation import build_execution_simulation_backtest
from core.services.backtest.models import BacktestArtifactKind, BacktestMode, BacktestSweepConfig
from core.services.backtest.portfolio_simulation import build_portfolio_simulation_backtest
from core.services.backtest.strategy_rerun import build_strategy_rerun_backtest
from core.services.backtest.strategy_scope import load_backtest_strategy_scope, strategy_scope_snapshot
from core.services.trading_strategies import load_universe_symbols
from core.services.trading_strategy_build_models import StrategyLiquidityRules
from core.services.trading_strategy_execution_models import StrategyExecutionPolicy
from core.services.trading_strategy_risk_models import (
    StrategyProtectionPolicy,
    StrategyRiskDefaults,
    StrategyRiskLimits,
    StrategyRuntimeControls,
)
from core.services.trading_strategy_runtime_models import StrategyRoutine, StrategySource, TradingStrategyConfig
from core.value_coercion import as_mapping, coerce_float, utc_now_iso

LOWER_IS_BETTER_METRICS = {
    "gross_loss",
    "loss_count",
    "max_drawdown",
    "missing_mark_count",
}


def _config_root(strategy: TradingStrategyConfig) -> Path:
    return strategy.config_path.parents[1]


def _source_symbols(source: StrategySource, *, config_root: Path) -> tuple[str, ...]:
    if source.is_static:
        return load_universe_symbols(source.ref, config_root=config_root)
    if source.fallback_universe_ref:
        return load_universe_symbols(source.fallback_universe_ref, config_root=config_root)
    return ()


def _strategy_payload(strategy: TradingStrategyConfig) -> dict[str, Any]:
    return {
        "source": strategy.source.to_payload(),
        "build": strategy.build.to_payload(),
        "entry": None if strategy.entry is None else strategy.entry.to_payload(),
        "management": None if strategy.management is None else strategy.management.to_payload(),
        "liquidity": strategy.liquidity.to_payload(),
        "position_sizing": strategy.position_sizing.to_payload(),
        "risk_limits": strategy.risk_limits.to_payload(),
        "protection": strategy.protection.to_payload(),
        "runtime": strategy.runtime.to_payload(),
        "execution": strategy.execution.to_payload(),
    }


def _canonical_path(path: str, payload: Mapping[str, Any]) -> str:
    normalized = path.strip()
    if normalized == "quality_profile":
        return "entry.quality.profile_id"
    if normalized.startswith("quality."):
        return f"entry.{normalized}"
    if normalized.startswith("ranking."):
        return f"build.{normalized}"
    if normalized.startswith("exit."):
        return f"management.policy.{normalized.removeprefix('exit.')}"
    if normalized.startswith("build.short_delta.") and isinstance(payload.get("build"), Mapping) and "entry_delta" in payload["build"]:
        return normalized.replace("build.short_delta.", "build.entry_delta.", 1)
    return normalized


def _set_path(payload: dict[str, Any], path: str, value: Any) -> None:
    parts = [part for part in path.split(".") if part]
    if not parts:
        raise ValueError("variant parameter path must not be empty")
    current: Any = payload
    for part in parts[:-1]:
        if not isinstance(current, dict):
            raise ValueError(f"variant parameter path {path!r} crosses a non-mapping value")
        if part not in current or current[part] is None:
            current[part] = {}
        current = current[part]
    if not isinstance(current, dict):
        raise ValueError(f"variant parameter path {path!r} targets a non-mapping value")
    current[parts[-1]] = value


def _variant_config_hash(*, strategy: TradingStrategyConfig, payload: Mapping[str, Any], parameters: Mapping[str, Any]) -> str:
    rendered = json.dumps(
        {
            "base_config_hash": strategy.config_hash,
            "strategy_id": strategy.trading_strategy_id,
            "parameters": parameters,
            "payload": payload,
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha1(rendered.encode("utf-8")).hexdigest()


def _apply_variant_parameters(
    strategy: TradingStrategyConfig,
    parameters: Mapping[str, Any],
) -> TradingStrategyConfig:
    payload = _strategy_payload(strategy)
    applied_parameters: dict[str, Any] = {}
    for path, value in parameters.items():
        canonical = _canonical_path(str(path), payload)
        _set_path(payload, canonical, value)
        applied_parameters[canonical] = value

    config_root = _config_root(strategy)
    source = StrategySource.model_validate(payload["source"])
    build = strategy.trade_structure_spec.validate_build(payload["build"])
    entry = None if payload["entry"] is None else StrategyRoutine.model_validate(payload["entry"])
    management = None if payload["management"] is None else StrategyRoutine.model_validate(payload["management"])
    liquidity = StrategyLiquidityRules.model_validate(payload["liquidity"])
    position_sizing = StrategyRiskDefaults.model_validate(payload["position_sizing"])
    risk_limits = StrategyRiskLimits.model_validate(payload["risk_limits"])
    protection = StrategyProtectionPolicy.model_validate(payload["protection"])
    runtime = StrategyRuntimeControls.model_validate(payload["runtime"])
    execution = StrategyExecutionPolicy.model_validate(payload["execution"])
    config_hash = _variant_config_hash(strategy=strategy, payload=payload, parameters=applied_parameters)
    return replace(
        strategy,
        source=source,
        build=build,
        entry=entry,
        management=management,
        liquidity=liquidity,
        position_sizing=position_sizing,
        risk_limits=risk_limits,
        protection=protection,
        runtime=runtime,
        execution=execution,
        config_hash=config_hash,
        symbols=_source_symbols(source, config_root=config_root),
    )


def _parameter_sets(sweep: BacktestSweepConfig) -> list[dict[str, Any]]:
    if not sweep.dimensions:
        raise ValueError("parameter_sweep requires at least one sweep dimension")
    paths = list(sweep.dimensions)
    combinations = list(itertools.product(*(sweep.dimensions[path] for path in paths)))
    if len(combinations) > sweep.max_variants:
        raise ValueError(f"parameter_sweep expands to {len(combinations)} variants; max_variants is {sweep.max_variants}")
    return [dict(zip(paths, values, strict=True)) for values in combinations]


def _run_base_mode(
    *,
    mode: BacktestMode,
    start_date: str | date,
    end_date: str | date | None,
    strategy_ids: tuple[str, ...] | None,
    symbols: tuple[str, ...] | None,
    max_days: int,
    market_data_symbol_limit: int,
    candidate_limit: int,
    per_symbol_top: int,
    storage: Any,
    db_target: str,
    config_root: str | None,
    strategy_scope: dict[str, TradingStrategyConfig],
) -> dict[str, Any]:
    kwargs = {
        "start_date": start_date,
        "end_date": end_date,
        "strategy_ids": strategy_ids,
        "symbols": symbols,
        "max_days": max_days,
        "market_data_symbol_limit": market_data_symbol_limit,
        "candidate_limit": candidate_limit,
        "per_symbol_top": per_symbol_top,
        "storage": storage,
        "db_target": db_target,
        "config_root": config_root,
        "strategy_scope": strategy_scope,
    }
    if mode == BacktestMode.STRATEGY_RERUN:
        return build_strategy_rerun_backtest(**kwargs)
    if mode == BacktestMode.EXECUTION_SIMULATION:
        return build_execution_simulation_backtest(**kwargs)
    if mode == BacktestMode.PORTFOLIO_SIMULATION:
        return build_portfolio_simulation_backtest(**kwargs)
    raise ValueError(f"Unsupported parameter_sweep base mode: {mode}")


def _metric_value(row: Mapping[str, Any], metric: str) -> float | None:
    if "." in metric:
        current: Any = row
        for part in metric.split("."):
            current = as_mapping(current).get(part)
        return coerce_float(current)
    for section_name in (
        "performance_metrics",
        "pnl",
        "execution",
        "selection_quality",
        "admissions",
        "exits",
        "positions",
        "candidate_productivity",
    ):
        section = as_mapping(row.get(section_name))
        if metric in section:
            return coerce_float(section.get(metric))
    return coerce_float(row.get(metric))


def _rankings(rows: list[dict[str, Any]], rank_metric: str) -> dict[str, list[dict[str, Any]]]:
    reverse = rank_metric not in LOWER_IS_BETTER_METRICS
    ranked = sorted(
        (
            {
                "rank": 0,
                "variant_id": row.get("variant_id"),
                "sweep_variant_id": row.get("sweep_variant_id"),
                "trading_strategy_id": row.get("trading_strategy_id"),
                "value": _metric_value(row, rank_metric),
            }
            for row in rows
        ),
        key=lambda row: (
            row["value"] is None,
            -(row["value"] or 0.0) if reverse else (row["value"] or 0.0),
            str(row["variant_id"] or ""),
        ),
    )
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
    rankings = {rank_metric: ranked}
    if rank_metric != "net_pnl":
        rankings["net_pnl"] = _rankings(rows, "net_pnl")["net_pnl"]
    return rankings


def _acceleration_fidelity() -> str:
    available = [name for name in ("polars", "vectorbt") if util.find_spec(name) is not None]
    return "available_" + "_".join(available) if available else "not_installed_polars_vectorbt"


def build_parameter_sweep_backtest(
    *,
    start_date: str | date,
    end_date: str | date | None = None,
    strategy_ids: tuple[str, ...] | None = None,
    symbols: tuple[str, ...] | None = None,
    max_days: int = 31,
    market_data_symbol_limit: int = 250,
    candidate_limit: int = 10,
    per_symbol_top: int = 1,
    storage: Any,
    db_target: str,
    sweep: BacktestSweepConfig,
    config_root: str | None = None,
) -> dict[str, Any]:
    base_strategies = load_backtest_strategy_scope(strategy_ids)
    parameter_sets = _parameter_sets(sweep)
    strategy_rows: list[dict[str, Any]] = []
    variant_summaries: list[dict[str, Any]] = []
    variant_artifacts: list[dict[str, Any]] = []
    generated_at = utc_now_iso()

    for index, parameters in enumerate(parameter_sets, start=1):
        variant_scope = {
            strategy_id: _apply_variant_parameters(strategy, parameters)
            for strategy_id, strategy in base_strategies.items()
        }
        sweep_variant_id = "sweep_variant:" + hashlib.sha1(
            json.dumps({"index": index, "parameters": parameters}, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()[:16]
        variant_result = _run_base_mode(
            mode=sweep.base_mode,
            start_date=start_date,
            end_date=end_date,
            strategy_ids=tuple(variant_scope),
            symbols=symbols,
            max_days=max_days,
            market_data_symbol_limit=market_data_symbol_limit,
            candidate_limit=candidate_limit,
            per_symbol_top=per_symbol_top,
            storage=storage,
            db_target=db_target,
            config_root=config_root,
            strategy_scope=variant_scope,
        )
        for strategy_result in variant_result.get("strategies") or []:
            if not isinstance(strategy_result, dict):
                continue
            row = {
                **strategy_result,
                "sweep_variant_id": sweep_variant_id,
                "variant_parameters": dict(parameters),
                "base_mode": sweep.base_mode.value,
            }
            row["fidelity_labels"] = {
                **dict(as_mapping(strategy_result.get("fidelity_labels"))),
                "sweep": "parameter_grid_current_config_overlay",
                "comparison": f"{sweep.base_mode.value}_variant",
                "vectorized_acceleration": _acceleration_fidelity(),
            }
            strategy_rows.append(row)
        variant_summaries.append(
            {
                "sweep_variant_id": sweep_variant_id,
                "ordinal": index,
                "parameters": dict(parameters),
                "strategy_count": len(variant_scope),
                "summary": dict(as_mapping(variant_result.get("summary"))),
                "fidelity_labels": dict(as_mapping(variant_result.get("fidelity_labels"))),
            }
        )
        variant_artifacts.append(
            {
                "artifact_kind": BacktestArtifactKind.VARIANT_RESULT.value,
                "payload": variant_result,
                "metadata": {
                    "mode": BacktestMode.PARAMETER_SWEEP.value,
                    "base_mode": sweep.base_mode.value,
                    "sweep_variant_id": sweep_variant_id,
                    "parameters": dict(parameters),
                },
                "row_count": len(variant_result.get("strategies") or []),
            }
        )

    rankings = _rankings(strategy_rows, sweep.rank_metric)
    top_rank = rankings.get(sweep.rank_metric, [{}])[0] if rankings.get(sweep.rank_metric) else {}
    return {
        "status": "ready",
        "evaluation_mode": "parameter_sweep_current_model",
        "generated_at": generated_at,
        "summary": {
            "strategy_count": len(base_strategies),
            "variant_count": len(parameter_sets),
            "strategy_variant_result_count": len(strategy_rows),
            "base_mode": sweep.base_mode.value,
            "rank_metric": sweep.rank_metric,
            "top_variant_id": top_rank.get("variant_id"),
            "top_sweep_variant_id": top_rank.get("sweep_variant_id"),
            "top_rank_value": top_rank.get("value"),
            "net_pnl": sum(coerce_float(as_mapping(row.get("pnl")).get("net_pnl")) or 0.0 for row in strategy_rows),
        },
        "sweep": {
            "base_mode": sweep.base_mode.value,
            "max_variants": sweep.max_variants,
            "rank_metric": sweep.rank_metric,
            "dimensions": {path: list(values) for path, values in sweep.dimensions.items()},
            "variant_summaries": variant_summaries,
            "base_config_snapshot": strategy_scope_snapshot(base_strategies),
        },
        "strategies": strategy_rows,
        "comparison": {
            "mode": "parameter_sweep",
            "base_mode": sweep.base_mode.value,
            "primary_rank_metric": sweep.rank_metric,
            "rankings": rankings,
        },
        "variant_artifacts": variant_artifacts,
        "fidelity_labels": {
            "mode": "parameter_sweep_current_model",
            "comparison": f"{sweep.base_mode.value}_parameter_variants",
            "sweep": "parameter_grid_current_config_overlay",
            "vectorized_acceleration": _acceleration_fidelity(),
            "live_writes": "none",
        },
    }


__all__ = ["build_parameter_sweep_backtest"]
