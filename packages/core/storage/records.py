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
OptionQuoteTickRecord = StorageRow
OptionTradeTickRecord = StorageRow
AlertEventRecord = StorageRow
AlertStateRecord = StorageRow
JobRunRecord = StorageRow
AccountSnapshotRecord = StorageRow
BrokerSyncStateRecord = StorageRow
ControlStateRecord = StorageRow
OperatorActionRecord = StorageRow
PolicyRolloutRecord = StorageRow
SignalStateRecord = StorageRow
SignalStateTransitionRecord = StorageRow
StrategyRunRecord = StorageRow
JobLeaseRecord = StorageRow
ExecutionIntentRecord = StorageRow
ExecutionIntentEventRecord = StorageRow
ExecutionAttemptRecord = StorageRow
ExecutionOrderRecord = StorageRow
ExecutionFillRecord = StorageRow
PortfolioPositionRecord = StorageRow
PositionCloseRecord = StorageRow
CaptureTargetRecord = StorageRow
CaptureSummaryRecord = StorageRow
TickerSourceRunRecord = StorageRow
TickerSourceObservationRecord = StorageRow
TickerSourceStateRecord = StorageRow
CandidateRunRecord = StorageRow
CandidateSymbolDiagnosticRecord = StorageRow
TradeCandidateRecord = StorageRow
TradeSignalRecord = StorageRow


__all__ = [
    "StorageRow",
    "RecordMapping",
    "make_storage_row",
    "ScanRunRecord",
    "ScanCandidateRecord",
    "SessionTopRunRecord",
    "OptionQuoteTickRecord",
    "OptionTradeTickRecord",
    "AlertEventRecord",
    "AlertStateRecord",
    "JobRunRecord",
    "AccountSnapshotRecord",
    "BrokerSyncStateRecord",
    "ControlStateRecord",
    "OperatorActionRecord",
    "PolicyRolloutRecord",
    "SignalStateRecord",
    "SignalStateTransitionRecord",
    "StrategyRunRecord",
    "JobLeaseRecord",
    "ExecutionIntentRecord",
    "ExecutionIntentEventRecord",
    "ExecutionAttemptRecord",
    "ExecutionOrderRecord",
    "ExecutionFillRecord",
    "PortfolioPositionRecord",
    "PositionCloseRecord",
    "CaptureTargetRecord",
    "CaptureSummaryRecord",
    "TickerSourceRunRecord",
    "TickerSourceObservationRecord",
    "TickerSourceStateRecord",
    "CandidateRunRecord",
    "CandidateSymbolDiagnosticRecord",
    "TradeCandidateRecord",
    "TradeSignalRecord",
]
