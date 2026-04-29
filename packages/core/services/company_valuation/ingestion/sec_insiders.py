from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime


@dataclass(frozen=True)
class SecInsidersIngestionRequest:
    cik: str | None = None
    since: datetime | None = None
    until: datetime | None = None
    forms: tuple[str, ...] = ("3", "4", "5")


@dataclass(frozen=True)
class SecInsidersIngestionResult:
    status: str
    source: str
    started_at: datetime
    completed_at: datetime
    filings_seen: int = 0
    transactions_persisted: int = 0
    notes: tuple[str, ...] = field(default_factory=tuple)

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


def ingest_sec_insiders(
    request: SecInsidersIngestionRequest,
) -> SecInsidersIngestionResult:
    now = datetime.now(UTC)
    return SecInsidersIngestionResult(
        status="scaffold_only",
        source="sec_insiders",
        started_at=now,
        completed_at=now,
        notes=(
            "V1 ingestion scaffold only; insider filing fetch and persistence are not implemented yet.",
        ),
    )
