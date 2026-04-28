from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from core.db.decorators import with_storage
from core.jobs.specs import list_declared_job_rows
from core.services.live_pipelines import list_enabled_discovery_run_pipelines
from core.services.live_runtime import get_live_session_for_cycle, list_latest_live_sessions


def _cycle_id_from_run_payload(run_payload: Mapping[str, Any]) -> str | None:
    result = run_payload.get("result")
    if isinstance(result, Mapping):
        cycle_id = str(result.get("cycle_id") or "").strip()
        if cycle_id:
            return cycle_id
        cycle_ids = result.get("cycle_ids")
        if isinstance(cycle_ids, Sequence) and not isinstance(cycle_ids, (str, bytes)):
            for item in cycle_ids:
                rendered = str(item or "").strip()
                if rendered:
                    return rendered
    rendered = str(run_payload.get("cycle_id") or "").strip()
    return rendered or None


def _build_uoa_state_payload(state: Mapping[str, Any]) -> dict[str, Any]:
    job_run = state.get("job_run")
    if not isinstance(job_run, Mapping):
        raise ValueError("No completed discovery-run execution was found")
    return {
        "job_run": {
            "job_run_id": job_run.get("job_run_id"),
            "job_key": job_run.get("job_key"),
            "job_type": job_run.get("job_type"),
            "status": job_run.get("status"),
            "scheduled_for": job_run.get("scheduled_for"),
            "started_at": job_run.get("started_at"),
            "finished_at": job_run.get("finished_at"),
            "session_id": job_run.get("session_id"),
            "slot_at": job_run.get("slot_at"),
            "worker_name": job_run.get("worker_name"),
        },
        "cycle": (
            dict(state.get("cycle") or {})
            if isinstance(state.get("cycle"), Mapping)
            else None
        ),
        "quote_capture": dict(state.get("quote_capture") or {}),
        "trade_capture": dict(state.get("trade_capture") or {}),
        "uoa_summary": dict(state.get("uoa_summary") or {}),
        "uoa_quote_summary": dict(state.get("uoa_quote_summary") or {}),
        "uoa_decisions": dict(state.get("uoa_decisions") or {}),
        "selection_summary": dict(state.get("selection_summary") or {}),
        "opportunities": [
            dict(item) for item in list(state.get("opportunities") or [])
        ],
        "candidate_counts": dict(state.get("candidate_counts") or {}),
        "selection_counts": dict(state.get("selection_counts") or {}),
        "cycle_events": [
            dict(item) for item in list(state.get("cycle_events") or [])
        ],
    }


def _enabled_discovery_run_labels() -> set[str]:
    return {
        str(pipeline.get("label") or "")
        for pipeline in list_enabled_discovery_run_pipelines(
            list_declared_job_rows(enabled_only=True, job_type="discovery_run")
        )
        if str(pipeline.get("label") or "").strip()
    }


def _enabled_uoa_labels() -> set[str]:
    labels: set[str] = set()
    for pipeline in list_enabled_discovery_run_pipelines(
        list_declared_job_rows(enabled_only=True, job_type="discovery_run")
    ):
        payload = (
            pipeline.get("payload") if isinstance(pipeline.get("payload"), Mapping) else {}
        )
        if not bool(payload.get("uoa_only", False)):
            continue
        label = str(pipeline.get("label") or "").strip()
        if label:
            labels.add(label)
    return labels


def _coerce_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _uoa_session_activity_rank(state: Mapping[str, Any]) -> int:
    uoa_summary = (
        state.get("uoa_summary") if isinstance(state.get("uoa_summary"), Mapping) else {}
    )
    uoa_overview = (
        uoa_summary.get("overview")
        if isinstance(uoa_summary.get("overview"), Mapping)
        else {}
    )
    uoa_decisions = (
        state.get("uoa_decisions")
        if isinstance(state.get("uoa_decisions"), Mapping)
        else {}
    )
    decision_overview = (
        uoa_decisions.get("overview")
        if isinstance(uoa_decisions.get("overview"), Mapping)
        else {}
    )
    summary_status = str(uoa_overview.get("summary_status") or "").strip().lower()
    decision_status = str(decision_overview.get("decision_status") or "").strip().lower()
    if summary_status == "active" or decision_status == "active":
        return 2
    if (
        _coerce_int(uoa_overview.get("scoreable_trade_count")) > 0
        or _coerce_int(uoa_overview.get("observed_contract_count")) > 0
        or _coerce_int(decision_overview.get("root_count")) > 0
    ):
        return 1
    return 0


def _uoa_session_sort_key(state: Mapping[str, Any]) -> tuple[int, str, str, str]:
    job_run = state.get("job_run") if isinstance(state.get("job_run"), Mapping) else {}
    cycle = state.get("cycle") if isinstance(state.get("cycle"), Mapping) else {}
    return (
        _uoa_session_activity_rank(state),
        str(
            job_run.get("slot_at")
            or job_run.get("scheduled_for")
            or cycle.get("generated_at")
            or ""
        ),
        str(job_run.get("finished_at") or job_run.get("started_at") or ""),
        str(cycle.get("cycle_id") or ""),
    )


def _latest_uoa_cycle_from_live_sessions(
    *,
    storage: Any,
    labels: set[str],
) -> tuple[str, str] | None:
    if not labels:
        return None
    sessions = [
        dict(session)
        for session in list_latest_live_sessions(
            storage=storage,
            limit=max(len(labels), 1),
        )
        if str(session.get("label") or "").strip() in labels
        and isinstance(session.get("cycle"), Mapping)
        and str(dict(session.get("cycle") or {}).get("cycle_id") or "").strip()
    ]
    if not sessions:
        return None
    selected = max(sessions, key=_uoa_session_sort_key)
    cycle = dict(selected.get("cycle") or {})
    return str(cycle["cycle_id"]), str(selected.get("label") or cycle.get("label") or "")


def _latest_uoa_run_for_labels(
    *,
    storage: Any,
    labels: set[str],
) -> Mapping[str, Any] | None:
    if not labels:
        return None
    limit = max(len(labels) * 20, 100)
    for run_record in storage.jobs.list_job_runs(
        job_type="discovery_run",
        status="succeeded",
        limit=limit,
    ):
        payload = run_record.get("payload") if isinstance(run_record.get("payload"), Mapping) else {}
        if str(payload.get("label") or "").strip() in labels:
            return run_record
    return None


@with_storage()
def get_latest_uoa_state(
    *,
    db_target: str | None = None,
    label: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    enabled_labels = _enabled_discovery_run_labels()
    preferred_labels = _enabled_uoa_labels()
    if label is None:
        label_sets: list[set[str]] = []
        if preferred_labels:
            label_sets.append(preferred_labels)
        if enabled_labels and enabled_labels not in label_sets:
            label_sets.append(enabled_labels)
        for candidate_labels in label_sets:
            selected = _latest_uoa_cycle_from_live_sessions(
                storage=storage,
                labels=candidate_labels,
            )
            if selected is not None:
                cycle_id, resolved_label = selected
                state = get_live_session_for_cycle(
                    storage=storage,
                    cycle_id=cycle_id,
                    label=resolved_label,
                )
                return _build_uoa_state_payload(state)
        run_record = None
        for candidate_labels in label_sets:
            run_record = _latest_uoa_run_for_labels(
                storage=storage,
                labels=candidate_labels,
            )
            if run_record is not None:
                break
        if run_record is None:
            raise ValueError("No completed discovery-run execution was found")
        cycle_id = _cycle_id_from_run_payload(run_record)
        if cycle_id is None:
            raise ValueError("Discovery-run execution is missing cycle_id")
        resolved_label = str(
            dict(run_record.get("payload") or {}).get("label") or ""
        ).strip()
        state = get_live_session_for_cycle(
            storage=storage,
            cycle_id=cycle_id,
            label=resolved_label or None,
        )
        return _build_uoa_state_payload(state)
    run_record = storage.jobs.get_latest_discovery_run(label=label, status="succeeded")
    if run_record is None:
        raise ValueError("No completed discovery-run execution was found")
    cycle_id = _cycle_id_from_run_payload(run_record)
    if cycle_id is None:
        raise ValueError("Discovery-run execution is missing cycle_id")
    state = get_live_session_for_cycle(
        storage=storage,
        cycle_id=cycle_id,
        label=label,
    )
    return _build_uoa_state_payload(state)


@with_storage()
def get_uoa_state_for_cycle(
    *,
    db_target: str | None = None,
    cycle_id: str,
    label: str | None = None,
    storage: Any | None = None,
) -> dict[str, Any]:
    state = get_live_session_for_cycle(
        storage=storage,
        cycle_id=cycle_id,
        label=label,
    )
    if state.get("job_run") is None:
        raise ValueError(
            f"No completed discovery-run execution was found for cycle_id={cycle_id}"
        )
    return _build_uoa_state_payload(state)
