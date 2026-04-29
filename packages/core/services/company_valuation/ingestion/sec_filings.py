from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime


@dataclass(frozen=True)
class SecFilingsIngestionRequest:
    cik: str | None = None
    since: datetime | None = None
    until: datetime | None = None
    forms: tuple[str, ...] = ("10-K", "10-Q", "8-K")


@dataclass(frozen=True)
class SecFilingsIngestionResult:
    status: str
    source: str
    started_at: datetime
    completed_at: datetime
    filings_seen: int = 0
    filings_persisted: int = 0
    notes: tuple[str, ...] = field(default_factory=tuple)

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


def ingest_sec_filings(request: SecFilingsIngestionRequest) -> SecFilingsIngestionResult:
    now = datetime.now(UTC)
    return SecFilingsIngestionResult(
        status="scaffold_only",
        source="sec_filings",
        started_at=now,
        completed_at=now,
        notes=(
            "V1 ingestion scaffold only; SEC filings fetch and persistence are not implemented yet.",
        ),
    )
