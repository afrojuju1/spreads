from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from spreads.jobs.live_collector import (
    build_collection_args,
    build_scanner_args,
    snapshot_label,
)
from spreads.services.scanner import resolve_symbols


def resolve_live_collector_label(payload: Mapping[str, Any]) -> str:
    args = build_collection_args(dict(payload))
    scanner_args = build_scanner_args(args)
    _, universe_label = resolve_symbols(scanner_args)
    return snapshot_label(universe_label, args)


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
