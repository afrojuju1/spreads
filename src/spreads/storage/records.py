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
