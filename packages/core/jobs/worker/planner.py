from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from core.services.live_pipelines import build_live_session_catalog, build_live_run_scope_id
from core.services.market_dates import resolve_market_date
from core.services.post_market_analysis import (
    parse_args as parse_post_market_args,
    run_post_market_analysis,
)
from core.services.post_close.cli import build_analysis_args
from core.services.post_close.service import run_post_close_analysis
from core.storage.factory import build_post_market_repository

from .managed import ManagedJobFailure


def build_live_session_catalog_for_date(
    *,
    job_store: Any,
    session_date: str,
) -> dict[str, Any]:
    definitions = job_store.list_job_definitions(
        enabled_only=True, job_type="live_collector"
    )
    base_catalog = build_live_session_catalog(definitions, realized_labels=[])
    realized_labels = [
        str(pipeline["label"])
        for pipeline in base_catalog["pipelines"]
        if job_store.list_job_runs(
            job_key=str(pipeline["job_key"]),
            job_type="live_collector",
            status="succeeded",
            session_id=build_live_run_scope_id(str(pipeline["label"]), session_date),
            limit=1,
        )
    ]
    return build_live_session_catalog(definitions, realized_labels=realized_labels)


def _planner_analysis_payload(
    base_payload: dict[str, Any],
    *,
    db_target: str,
    label: str,
) -> dict[str, Any]:
    return {
        "db": db_target,
        "date": str(base_payload.get("date", "today")),
        "label": label,
        "replay_profit_target": base_payload.get("replay_profit_target", 0.5),
        "replay_stop_multiple": base_payload.get("replay_stop_multiple", 2.0),
    }


def run_post_close_analysis_targets(
    *,
    db_target: str,
    job_store: Any,
    payload: dict[str, Any],
    heartbeat: Any,
) -> dict[str, Any]:
    session_date = resolve_market_date(str(payload.get("date", "today")))
    catalog = build_live_session_catalog_for_date(
        job_store=job_store,
        session_date=session_date,
    )
    runs: list[dict[str, Any]] = []
    skipped_labels: list[dict[str, Any]] = []
    failed_labels: list[dict[str, Any]] = []

    for pipeline in catalog["pipelines"]:
        heartbeat()
        label = str(pipeline["label"])
        if not pipeline["has_session"]:
            skipped_labels.append({"label": label, "reason": "missing_session"})
            continue
        try:
            args = build_analysis_args(
                _planner_analysis_payload(payload, db_target=db_target, label=label)
            )
            runs.append(run_post_close_analysis(args, emit_output=False))
        except Exception as exc:
            failed_labels.append({"label": label, "error": str(exc)})

    result = {
        "mode": "planner",
        "session_date": session_date,
        "expected_labels": list(catalog["expected_labels"]),
        "realized_labels": list(catalog["realized_labels"]),
        "runs": runs,
        "skipped_labels": skipped_labels,
        "failed_labels": failed_labels,
    }
    if failed_labels:
        labels = ", ".join(item["label"] for item in failed_labels)
        raise ManagedJobFailure(
            f"Post-close analysis failed for labels: {labels}",
            result=result,
        )
    return result


def run_post_market_analysis_targets(
    *,
    db_target: str,
    job_store: Any,
    parent_job_run_id: str,
    payload: dict[str, Any],
    heartbeat: Any,
) -> dict[str, Any]:
    session_date = resolve_market_date(str(payload.get("date", "today")))
    catalog = build_live_session_catalog_for_date(
        job_store=job_store,
        session_date=session_date,
    )
    repository = build_post_market_repository(db_target)
    runs: list[dict[str, Any]] = []
    skipped_labels: list[dict[str, Any]] = []
    failed_labels: list[dict[str, Any]] = []
    try:
        for pipeline in catalog["pipelines"]:
            heartbeat()
            label = str(pipeline["label"])
            analysis_run_id = f"{parent_job_run_id}:{label}:{uuid4().hex[:8]}"
            if not pipeline["has_session"]:
                created_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
                repository.begin_run(
                    analysis_run_id=analysis_run_id,
                    job_run_id=None,
                    session_date=session_date,
                    label=label,
                    created_at=created_at,
                )
                repository.skip_run(
                    analysis_run_id=analysis_run_id,
                    completed_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                    error_text="No persisted collector cycles were available for this session label.",
                )
                skipped_labels.append(
                    {
                        "analysis_run_id": analysis_run_id,
                        "label": label,
                        "session_date": session_date,
                        "status": "skipped",
                        "reason": "missing_session",
                    }
                )
                continue
            try:
                args = parse_post_market_args(
                    [
                        "--db",
                        db_target,
                        "--date",
                        session_date,
                        "--label",
                        label,
                        "--replay-profit-target",
                        str(payload.get("replay_profit_target", 0.5)),
                        "--replay-stop-multiple",
                        str(payload.get("replay_stop_multiple", 2.0)),
                    ]
                )
                runs.append(
                    run_post_market_analysis(
                        args,
                        emit_output=False,
                        analysis_run_id=analysis_run_id,
                        job_run_id=None,
                    )
                )
            except Exception as exc:
                failed_labels.append({"label": label, "error": str(exc)})
    finally:
        repository.close()

    result = {
        "mode": "planner",
        "session_date": session_date,
        "expected_labels": list(catalog["expected_labels"]),
        "realized_labels": list(catalog["realized_labels"]),
        "runs": runs,
        "skipped_labels": skipped_labels,
        "failed_labels": failed_labels,
    }
    if failed_labels:
        labels = ", ".join(item["label"] for item in failed_labels)
        raise ManagedJobFailure(
            f"Post-market analysis failed for labels: {labels}",
            result=result,
        )
    return result
