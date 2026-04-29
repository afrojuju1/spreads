from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime


@dataclass(frozen=True)
class Sec13FIngestionRequest:
    report_period: date | None = None
    manager_cik: str | None = None


@dataclass(frozen=True)
class Sec13FIngestionResult:
    status: str
    source: str
    started_at: datetime
    completed_at: datetime
    filings_seen: int = 0
    positions_persisted: int = 0
    notes: tuple[str, ...] = field(default_factory=tuple)

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


def ingest_sec_13f(request: Sec13FIngestionRequest) -> Sec13FIngestionResult:
    now = datetime.now(UTC)
    return Sec13FIngestionResult(
        status="scaffold_only",
        source="sec_13f",
        started_at=now,
        completed_at=now,
        notes=(
            "V1 ingestion scaffold only; 13F fetch and persistence are not implemented yet.",
        ),
    )
