from __future__ import annotations

from collections.abc import Iterable, Mapping
from types import SimpleNamespace
from typing import Any

from core.services.runtime_identity import (
    build_live_run_scope_id,
    build_pipeline_id,
    parse_live_run_scope_id,
    parse_pipeline_id,
    resolve_horizon_intent,
    resolve_pipeline_policy_fields,
    resolve_product_class,
    resolve_style_profile,
)
from core.services.scanners.config import resolve_symbols


def build_live_snapshot_label(
    *,
    universe_label: str,
    strategy: str,
    profile: str,
    greeks_source: str,
) -> str:
    return f"{universe_label}_{strategy}_{profile}_{greeks_source}".lower()


def _payload_namespace(payload: Mapping[str, Any]) -> SimpleNamespace:
    return SimpleNamespace(
        symbol=None,
        symbols=payload.get("symbols"),
        symbols_file=payload.get("symbols_file"),
        universe=str(payload.get("universe") or "0dte_core"),
        strategy=str(payload.get("strategy") or "combined"),
        profile=str(payload.get("profile") or "0dte"),
        greeks_source=str(payload.get("greeks_source") or "auto"),
    )


def resolve_live_collector_label(payload: Mapping[str, Any]) -> str:
    args = _payload_namespace(payload)
    _, universe_label = resolve_symbols(args)
    return build_live_snapshot_label(
        universe_label=universe_label,
        strategy=args.strategy,
        profile=args.profile,
        greeks_source=args.greeks_source,
    )


def pipeline_uses_runtime_owned_opportunities(
    pipeline: Mapping[str, Any] | None,
    *runs: Mapping[str, Any] | None,
) -> bool:
    if isinstance(pipeline, Mapping) and bool(
        pipeline.get("options_automation_enabled", False)
    ):
        return True
    for run in runs:
        if not isinstance(run, Mapping):
            continue
        payload = run.get("payload")
        if isinstance(payload, Mapping) and bool(
            payload.get("options_automation_enabled", False)
        ):
            return True
    return False


def list_enabled_live_collector_pipelines(
    job_definitions: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    pipelines_by_label: dict[str, dict[str, Any]] = {}

    for definition in job_definitions:
        if not bool(definition.get("enabled", False)):
            continue
        if str(definition.get("job_type")) != "live_collector":
            continue
        payload = dict(definition.get("payload") or {})
        label = resolve_live_collector_label(payload)
        existing = pipelines_by_label.get(label)
        if existing is None:
            pipelines_by_label[label] = {
                "label": label,
                "pipeline_id": build_pipeline_id(label),
                "job_key": str(definition["job_key"]),
                "job_keys": [str(definition["job_key"])],
                "payload": payload,
                "singleton_scope": definition.get("singleton_scope"),
            }
            continue
        existing["job_keys"].append(str(definition["job_key"]))

    return [pipelines_by_label[label] for label in sorted(pipelines_by_label)]


def build_live_session_catalog(
    job_definitions: Iterable[Mapping[str, Any]],
    *,
    realized_labels: Iterable[str] | None = None,
) -> dict[str, Any]:
    pipelines = list_enabled_live_collector_pipelines(job_definitions)
    expected_labels = [str(pipeline["label"]) for pipeline in pipelines]
    expected_label_set = set(expected_labels)
    realized = sorted({str(label) for label in (realized_labels or []) if str(label)})
    realized_set = set(realized)

    return {
        "pipelines": [
            {
                **pipeline,
                "has_session": str(pipeline["label"]) in realized_set,
            }
            for pipeline in pipelines
        ],
        "expected_labels": expected_labels,
        "realized_labels": realized,
        "unexpected_realized_labels": [
            label for label in realized if label not in expected_label_set
        ],
        "labels": sorted(expected_label_set | realized_set),
    }


__all__ = [
    "build_live_run_scope_id",
    "build_live_session_catalog",
    "build_live_snapshot_label",
    "build_pipeline_id",
    "list_enabled_live_collector_pipelines",
    "parse_live_run_scope_id",
    "parse_pipeline_id",
    "pipeline_uses_runtime_owned_opportunities",
    "resolve_horizon_intent",
    "resolve_live_collector_label",
    "resolve_pipeline_policy_fields",
    "resolve_product_class",
    "resolve_style_profile",
]
