from __future__ import annotations

import csv
import io
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, time

from core.services.company_valuation.ids import build_treasury_curve_snapshot_id
from core.storage.company_valuation_repository import CompanyValuationRepository

TREASURY_DAILY_RATES_BASE_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates"
)
CURVE_COLUMN_MAP = {
    "1 Mo": "1m",
    "1.5 Mo": "1_5m",
    "2 Mo": "2m",
    "3 Mo": "3m",
    "4 Mo": "4m",
    "6 Mo": "6m",
    "1 Yr": "1y",
    "2 Yr": "2y",
    "3 Yr": "3y",
    "5 Yr": "5y",
    "7 Yr": "7y",
    "10 Yr": "10y",
    "20 Yr": "20y",
    "30 Yr": "30y",
}


def _month_key(value: date) -> str:
    return value.strftime("%Y%m")


def _csv_url(curve_date: date) -> str:
    month_key = _month_key(curve_date)
    return (
        f"{TREASURY_DAILY_RATES_BASE_URL}/daily-treasury-rates.csv/all/{month_key}"
        f"?type=daily_treasury_yield_curve&field_tdr_date_value_month={month_key}&page&_format=csv"
    )


def _parse_curve_row(row: dict[str, str]) -> tuple[date, dict[str, float]]:
    curve_date = datetime.strptime(str(row["Date"]).strip(), "%m/%d/%Y").date()
    curve_points: dict[str, float] = {}
    for source_column, target_key in CURVE_COLUMN_MAP.items():
        raw = str(row.get(source_column) or "").strip()
        if not raw or raw.upper() == "N/A":
            continue
        curve_points[target_key] = float(raw) / 100.0
    return curve_date, curve_points


@dataclass(frozen=True)
class TreasuryCurveIngestionRequest:
    curve_date: date | None = None


@dataclass(frozen=True)
class TreasuryCurveIngestionResult:
    status: str
    source: str
    started_at: datetime
    completed_at: datetime
    curve_date: date | None = None
    curve_points_persisted: int = 0
    notes: tuple[str, ...] = field(default_factory=tuple)

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


def ingest_treasury_curve(
    request: TreasuryCurveIngestionRequest,
    *,
    repository: CompanyValuationRepository | None = None,
) -> TreasuryCurveIngestionResult:
    started_at = datetime.now(UTC)
    repo = repository or CompanyValuationRepository()
    target_date = request.curve_date or datetime.now(UTC).date()
    source_url = _csv_url(target_date)
    request_headers = {
        "Accept": "text/csv",
        "User-Agent": "Spreads Company Valuation Engine/1.0",
    }
    request_obj = urllib.request.Request(source_url, headers=request_headers)
    with urllib.request.urlopen(request_obj, timeout=30) as response:
        csv_text = response.read().decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(csv_text))
    selected_date: date | None = None
    selected_curve_points: dict[str, float] | None = None
    for row in reader:
        if not row.get("Date"):
            continue
        row_date, curve_points = _parse_curve_row(row)
        if row_date <= target_date and (
            selected_date is None or row_date > selected_date
        ):
            selected_date = row_date
            selected_curve_points = curve_points
    completed_at = datetime.now(UTC)
    if selected_date is None or selected_curve_points is None:
        return TreasuryCurveIngestionResult(
            status="not_found",
            source="us_treasury_par_yield_curve",
            started_at=started_at,
            completed_at=completed_at,
            curve_date=target_date,
            notes=(f"No Treasury curve row was available for month {target_date.strftime('%Y-%m')}.",),
        )

    payload = {
        "curve_snapshot_id": build_treasury_curve_snapshot_id(selected_date),
        "curve_date": selected_date,
        # Treasury publishes the daily curve after the trading session, so do not
        # make the row point-in-time visible at midnight.
        "available_at": datetime.combine(selected_date, time(hour=21), tzinfo=UTC),
        "curve_points_json": selected_curve_points,
        "source_url": source_url,
    }
    repo.upsert_treasury_curve_snapshot(payload)
    notes: list[str] = []
    if selected_date != target_date:
        notes.append(f"Requested {target_date.isoformat()} but used latest available row {selected_date.isoformat()}.")
    return TreasuryCurveIngestionResult(
        status="ok",
        source="us_treasury_par_yield_curve",
        started_at=started_at,
        completed_at=completed_at,
        curve_date=selected_date,
        curve_points_persisted=len(selected_curve_points),
        notes=tuple(notes),
    )
