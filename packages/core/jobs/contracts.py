from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from core.storage.factory import StorageContext

RoutineJobStatus = Literal["succeeded", "skipped"]


def build_ad_hoc_job_run_id(job_key: str, workflow_run_id: str) -> str:
    normalized_job_key = str(job_key or "").strip()
    normalized_run_id = str(workflow_run_id or "").strip()
    if not normalized_job_key or not normalized_run_id:
        raise ValueError("job_key and workflow_run_id are required")
    return f"{normalized_job_key}:workflow:{normalized_run_id}"


@dataclass(frozen=True)
class ResolvedRoutineRequest:
    job_run_id: str
    job_key: str
    job_type: str
    workflow_lane: str
    orchestration_id: str
    scheduled_for: datetime
    provider_attempt: int
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class RoutineExecutionContext:
    job_run_id: str
    job_key: str
    job_type: str
    workflow_lane: str
    scheduled_for: datetime
    provider_attempt: int
    worker_name: str
    database_url: str
    storage: StorageContext
    payload: Mapping[str, Any]
    heartbeat: Callable[[], None]


@dataclass(frozen=True)
class RoutineOutcome:
    job_status: RoutineJobStatus
    persisted_result: dict[str, Any]

    @classmethod
    def succeeded(cls, result: Mapping[str, Any]) -> RoutineOutcome:
        return cls(job_status="succeeded", persisted_result=dict(result))

    @classmethod
    def skipped(cls, result: Mapping[str, Any]) -> RoutineOutcome:
        return cls(job_status="skipped", persisted_result=dict(result))


RoutineHandler = Callable[[RoutineExecutionContext], RoutineOutcome]

__all__ = [
    "ResolvedRoutineRequest",
    "RoutineExecutionContext",
    "RoutineHandler",
    "RoutineJobStatus",
    "RoutineOutcome",
    "build_ad_hoc_job_run_id",
]
