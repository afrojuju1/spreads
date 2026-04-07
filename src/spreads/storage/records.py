from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any


class RecordMapping(Mapping[str, Any]):
    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)

    def __getitem__(self, key: str) -> Any:
        return self.__dict__[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.__dict__)

    def __len__(self) -> int:
        return len(self.__dict__)

    def get(self, key: str, default: Any = None) -> Any:
        return self.__dict__.get(key, default)


@dataclass(frozen=True)
class ScanRunRecord(RecordMapping):
    run_id: str
    generated_at: str
    symbol: str
    strategy: str
    session_label: str | None
    profile: str
    spot_price: float
    candidate_count: int
    output_path: str | None
    filters: dict[str, Any]
    setup_status: str | None
    setup_score: float | None
    setup: dict[str, Any] | None


@dataclass(frozen=True)
class ScanCandidateRecord(RecordMapping):
    run_id: str
    rank: int
    strategy: str
    expiration_date: str
    short_symbol: str
    long_symbol: str
    short_strike: float
    long_strike: float
    width: float
    midpoint_credit: float
    natural_credit: float
    breakeven: float
    max_profit: float
    max_loss: float
    quality_score: float
    return_on_risk: float
    short_otm_pct: float
    calendar_status: str | None
    setup_status: str | None
    expected_move: float | None
    short_vs_expected_move: float | None


@dataclass(frozen=True)
class SessionTopRunRecord(RecordMapping):
    run_id: str
    generated_at: str
    symbol: str
    strategy: str
    profile: str
    spot_price: float
    candidate_count: int
    setup_status: str | None
    setup_score: float | None
    setup_json: dict[str, Any] | None
    short_symbol: str | None
    long_symbol: str | None
    short_strike: float | None
    long_strike: float | None
    midpoint_credit: float | None
    quality_score: float | None
    calendar_status: str | None
    expected_move: float | None
    short_vs_expected_move: float | None


@dataclass(frozen=True)
class OptionQuoteEventRecord(RecordMapping):
    quote_id: int
    cycle_id: str
    captured_at: str
    label: str
    underlying_symbol: str | None
    strategy: str | None
    profile: str | None
    option_symbol: str
    leg_role: str
    bid: float
    ask: float
    midpoint: float
    bid_size: int
    ask_size: int
    quote_timestamp: str | None
    source: str


@dataclass(frozen=True)
class CollectorCycleRecord(RecordMapping):
    cycle_id: str
    label: str
    session_date: str
    generated_at: str
    universe_label: str
    strategy: str
    profile: str
    greeks_source: str
    symbols: list[str]
    failures: list[dict[str, Any]]
    selection_state: dict[str, Any]


@dataclass(frozen=True)
class CollectorCycleCandidateRecord(RecordMapping):
    candidate_id: int
    cycle_id: str
    label: str
    session_date: str
    generated_at: str
    bucket: str
    position: int
    run_id: str
    underlying_symbol: str
    strategy: str
    expiration_date: str
    short_symbol: str
    long_symbol: str
    quality_score: float
    midpoint_credit: float
    candidate: dict[str, Any]


@dataclass(frozen=True)
class CollectorCycleEventRecord(RecordMapping):
    event_id: int
    cycle_id: str
    label: str
    session_date: str
    generated_at: str
    symbol: str
    event_type: str
    message: str
    previous_candidate: dict[str, Any] | None
    current_candidate: dict[str, Any] | None


@dataclass(frozen=True)
class AlertEventRecord(RecordMapping):
    alert_id: int
    created_at: str
    session_date: str
    label: str
    cycle_id: str
    symbol: str
    alert_type: str
    dedupe_key: str
    status: str
    delivery_target: str
    payload: dict[str, Any]
    response: dict[str, Any] | None
    error_text: str | None


@dataclass(frozen=True)
class AlertStateRecord(RecordMapping):
    dedupe_key: str
    last_alert_at: str
    last_cycle_id: str
    last_alert_type: str
    state: dict[str, Any]


@dataclass(frozen=True)
class JobDefinitionRecord(RecordMapping):
    job_key: str
    job_type: str
    enabled: bool
    schedule_type: str
    schedule: dict[str, Any]
    payload: dict[str, Any]
    market_calendar: str
    singleton_scope: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class JobRunRecord(RecordMapping):
    job_run_id: str
    job_key: str
    arq_job_id: str | None
    job_type: str
    status: str
    scheduled_for: str
    started_at: str | None
    finished_at: str | None
    heartbeat_at: str | None
    worker_name: str | None
    payload: dict[str, Any]
    result: dict[str, Any] | None
    error_text: str | None


@dataclass(frozen=True)
class JobLeaseRecord(RecordMapping):
    lease_key: str
    job_run_id: str | None
    owner: str
    acquired_at: str
    expires_at: str
    lease_state: dict[str, Any]
