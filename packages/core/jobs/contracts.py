from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from core.storage.factory import StorageContext

RoutineRequestSource = Literal["scheduled", "adhoc"]
RoutineJobStatus = Literal["succeeded", "skipped"]


@dataclass(frozen=True)
class ResolvedRoutineRequest:
    source: RoutineRequestSource
    job_run_id: str
    job_key: str
    job_type: str
    workflow_lane: str
    orchestration_id: str
    scheduled_for: datetime
    singleton_scope: str | None
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class RoutineExecutionContext:
    job_run_id: str
    job_key: str
    job_type: str
    workflow_lane: str
    scheduled_for: datetime
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
    "RoutineRequestSource",
]
