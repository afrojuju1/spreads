from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime


@dataclass(frozen=True)
class SecBeneficialOwnershipIngestionRequest:
    cik: str | None = None
    since: datetime | None = None
    until: datetime | None = None
    schedules: tuple[str, ...] = ("SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A")


@dataclass(frozen=True)
class SecBeneficialOwnershipIngestionResult:
    status: str
    source: str
    started_at: datetime
    completed_at: datetime
    filings_seen: int = 0
    positions_persisted: int = 0
    groups_persisted: int = 0
    notes: tuple[str, ...] = field(default_factory=tuple)

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


def ingest_sec_beneficial_ownership(
    request: SecBeneficialOwnershipIngestionRequest,
) -> SecBeneficialOwnershipIngestionResult:
    now = datetime.now(UTC)
    return SecBeneficialOwnershipIngestionResult(
        status="scaffold_only",
        source="sec_beneficial_ownership",
        started_at=now,
        completed_at=now,
        notes=(
            "V1 ingestion scaffold only; 13D/13G fetch, XML parsing, and persistence are not implemented yet.",
        ),
    )
