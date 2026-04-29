from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime


@dataclass(frozen=True)
class MarketInputsIngestionRequest:
    ticker: str | None = None
    issuer_id: str | None = None


@dataclass(frozen=True)
class MarketInputsIngestionResult:
    status: str
    source: str
    started_at: datetime
    completed_at: datetime
    snapshots_persisted: int = 0
    notes: tuple[str, ...] = field(default_factory=tuple)

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


def ingest_market_inputs(
    request: MarketInputsIngestionRequest,
) -> MarketInputsIngestionResult:
    now = datetime.now(UTC)
    return MarketInputsIngestionResult(
        status="scaffold_only",
        source="market_inputs",
        started_at=now,
        completed_at=now,
        notes=(
            "V1 ingestion scaffold only; price and market-cap input fetch and persistence are not implemented yet.",
        ),
    )
