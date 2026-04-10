from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeAlias


StorageRow: TypeAlias = dict[str, Any]
RecordMapping: TypeAlias = Mapping[str, Any]


def make_storage_row(values: Mapping[str, Any] | None = None, /, **kwargs: Any) -> StorageRow:
    payload = {} if values is None else dict(values)
    if kwargs:
        payload.update(kwargs)
    return payload

ScanRunRecord = StorageRow
ScanCandidateRecord = StorageRow
SessionTopRunRecord = StorageRow
OptionQuoteEventRecord = StorageRow
CollectorCycleRecord = StorageRow
CollectorCycleCandidateRecord = StorageRow
CollectorCycleEventRecord = StorageRow
AlertEventRecord = StorageRow
AlertStateRecord = StorageRow
JobDefinitionRecord = StorageRow
JobRunRecord = StorageRow
AccountSnapshotRecord = StorageRow
BrokerSyncStateRecord = StorageRow
EventLogRecord = StorageRow
SignalStateRecord = StorageRow
SignalStateTransitionRecord = StorageRow
OpportunityRecord = StorageRow
JobLeaseRecord = StorageRow
PostMarketAnalysisRunRecord = StorageRow
ExecutionAttemptRecord = StorageRow
ExecutionOrderRecord = StorageRow
ExecutionFillRecord = StorageRow
SessionPositionRecord = StorageRow
SessionPositionCloseRecord = StorageRow


__all__ = [
    "StorageRow",
    "RecordMapping",
    "make_storage_row",
    "ScanRunRecord",
    "ScanCandidateRecord",
    "SessionTopRunRecord",
    "OptionQuoteEventRecord",
    "CollectorCycleRecord",
    "CollectorCycleCandidateRecord",
    "CollectorCycleEventRecord",
    "AlertEventRecord",
    "AlertStateRecord",
    "JobDefinitionRecord",
    "JobRunRecord",
    "AccountSnapshotRecord",
    "BrokerSyncStateRecord",
    "EventLogRecord",
    "SignalStateRecord",
    "SignalStateTransitionRecord",
    "OpportunityRecord",
    "JobLeaseRecord",
    "PostMarketAnalysisRunRecord",
    "ExecutionAttemptRecord",
    "ExecutionOrderRecord",
    "ExecutionFillRecord",
    "SessionPositionRecord",
    "SessionPositionCloseRecord",
]
