from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime


@dataclass(frozen=True)
class TreasuryCurveIngestionRequest:
    curve_date: date | None = None


@dataclass(frozen=True)
class TreasuryCurveIngestionResult:
    status: str
    source: str
    started_at: datetime
    completed_at: datetime
    curve_points_persisted: int = 0
    notes: tuple[str, ...] = field(default_factory=tuple)

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


def ingest_treasury_curve(
    request: TreasuryCurveIngestionRequest,
) -> TreasuryCurveIngestionResult:
    now = datetime.now(UTC)
    return TreasuryCurveIngestionResult(
        status="scaffold_only",
        source="treasury_curve",
        started_at=now,
        completed_at=now,
        notes=(
            "V1 ingestion scaffold only; Treasury curve fetch and persistence are not implemented yet.",
        ),
    )
