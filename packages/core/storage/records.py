from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeAlias


StorageRow: TypeAlias = dict[str, Any]
RecordMapping: TypeAlias = Mapping[str, Any]


def make_storage_row(
    values: Mapping[str, Any] | None = None, /, **kwargs: Any
) -> StorageRow:
    payload = {} if values is None else dict(values)
    if kwargs:
        payload.update(kwargs)
    return payload


ScanRunRecord = StorageRow
ScanCandidateRecord = StorageRow
SessionTopRunRecord = StorageRow
OptionQuoteEventRecord = StorageRow
OptionTradeEventRecord = StorageRow
CollectorCycleRecord = StorageRow
CollectorCycleCandidateRecord = StorageRow
CollectorCycleEventRecord = StorageRow
PipelineRecord = StorageRow
PipelineCycleRecord = StorageRow
AlertEventRecord = StorageRow
AlertStateRecord = StorageRow
JobDefinitionRecord = StorageRow
JobRunRecord = StorageRow
AccountSnapshotRecord = StorageRow
BrokerSyncStateRecord = StorageRow
EventLogRecord = StorageRow
ControlStateRecord = StorageRow
OperatorActionRecord = StorageRow
PolicyRolloutRecord = StorageRow
SignalStateRecord = StorageRow
SignalStateTransitionRecord = StorageRow
OpportunityRecord = StorageRow
OpportunityDecisionRecord = StorageRow
RiskDecisionRecord = StorageRow
JobLeaseRecord = StorageRow
PostMarketAnalysisRunRecord = StorageRow
ExecutionIntentRecord = StorageRow
ExecutionIntentEventRecord = StorageRow
ExecutionAttemptRecord = StorageRow
ExecutionOrderRecord = StorageRow
ExecutionFillRecord = StorageRow
PortfolioPositionRecord = StorageRow
PositionCloseRecord = StorageRow
LiveSessionSlotRecord = StorageRow
MarketRecorderTargetRecord = StorageRow


__all__ = [
    "StorageRow",
    "RecordMapping",
    "make_storage_row",
    "ScanRunRecord",
    "ScanCandidateRecord",
    "SessionTopRunRecord",
    "OptionQuoteEventRecord",
    "OptionTradeEventRecord",
    "CollectorCycleRecord",
    "CollectorCycleCandidateRecord",
    "CollectorCycleEventRecord",
    "PipelineRecord",
    "PipelineCycleRecord",
    "AlertEventRecord",
    "AlertStateRecord",
    "JobDefinitionRecord",
    "JobRunRecord",
    "AccountSnapshotRecord",
    "BrokerSyncStateRecord",
    "EventLogRecord",
    "ControlStateRecord",
    "OperatorActionRecord",
    "PolicyRolloutRecord",
    "SignalStateRecord",
    "SignalStateTransitionRecord",
    "OpportunityRecord",
    "OpportunityDecisionRecord",
    "RiskDecisionRecord",
    "JobLeaseRecord",
    "PostMarketAnalysisRunRecord",
    "ExecutionIntentRecord",
    "ExecutionIntentEventRecord",
    "ExecutionAttemptRecord",
    "ExecutionOrderRecord",
    "ExecutionFillRecord",
    "PortfolioPositionRecord",
    "PositionCloseRecord",
    "LiveSessionSlotRecord",
    "MarketRecorderTargetRecord",
]
