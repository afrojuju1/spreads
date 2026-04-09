from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any


class StorageRow(Mapping[str, Any]):
    __slots__ = ("_values",)

    def __init__(self, values: Mapping[str, Any] | None = None, /, **kwargs: Any) -> None:
        payload = {} if values is None else dict(values)
        if kwargs:
            payload.update(kwargs)
        object.__setattr__(self, "_values", payload)

    def to_dict(self) -> dict[str, Any]:
        return dict(self._values)

    def copy(self, /, **updates: Any) -> StorageRow:
        payload = self.to_dict()
        payload.update(updates)
        return type(self)(payload)

    def __getitem__(self, key: str) -> Any:
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __getattr__(self, name: str) -> Any:
        try:
            return self._values[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("StorageRow is immutable")

    def get(self, key: str, default: Any = None) -> Any:
        return self._values.get(key, default)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._values!r})"


RecordMapping = StorageRow

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
JobLeaseRecord = StorageRow
PostMarketAnalysisRunRecord = StorageRow
GeneratorJobRecord = StorageRow
ExecutionAttemptRecord = StorageRow
ExecutionOrderRecord = StorageRow
ExecutionFillRecord = StorageRow
SessionPositionRecord = StorageRow
SessionPositionCloseRecord = StorageRow


__all__ = [
    "StorageRow",
    "RecordMapping",
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
    "JobLeaseRecord",
    "PostMarketAnalysisRunRecord",
    "GeneratorJobRecord",
    "ExecutionAttemptRecord",
    "ExecutionOrderRecord",
    "ExecutionFillRecord",
    "SessionPositionRecord",
    "SessionPositionCloseRecord",
]
