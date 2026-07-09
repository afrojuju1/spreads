from __future__ import annotations

from datetime import date, datetime
from hashlib import sha1
from typing import TYPE_CHECKING, Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from core.model_contracts import DomainModel
from core.storage.serializers import parse_date, parse_datetime
from core.value_coercion import as_text, coerce_float, coerce_int, normalize_symbol, unique_text_list

if TYPE_CHECKING:
    from core.services.trading_engine.market_context import MarketContextSnapshot


class TickerSourceObservationPayload(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    symbol: str
    observation_state: str | None = None
    state: str | None = None
    rank: int | None = None
    score: float | None = None
    company: str | None = None
    sector: str | None = None
    industry: str | None = None
    country: str | None = None
    price: float | None = None
    market_cap: int | None = None
    daily_volume: int | None = None
    move_percent: float | None = None
    relative_volume: float | None = None
    reason_codes: list[str] = Field(default_factory=list)

    @field_validator("symbol", mode="before")
    @classmethod
    def _normalize_symbol_field(cls, value: Any) -> str:
        symbol = normalize_symbol(value)
        if symbol is None:
            raise ValueError("symbol is required")
        return symbol

    @field_validator("observation_state", "state", "company", "sector", "industry", "country", mode="before")
    @classmethod
    def _normalize_optional_text_fields(cls, value: Any) -> str | None:
        return as_text(value)

    @field_validator("rank", "market_cap", "daily_volume", mode="before")
    @classmethod
    def _normalize_optional_int_fields(cls, value: Any) -> int | None:
        return coerce_int(value)

    @field_validator("score", "price", "move_percent", "relative_volume", mode="before")
    @classmethod
    def _normalize_optional_float_fields(cls, value: Any) -> float | None:
        return coerce_float(value)

    @field_validator("reason_codes", mode="before")
    @classmethod
    def _normalize_reason_codes(cls, value: Any) -> list[str]:
        return unique_text_list(value, accept_scalar=True)

    def as_storage_payload(
        self,
        *,
        default_state: str,
        rank: int | None = None,
        fallback_reason_codes: Any = None,
    ) -> dict[str, Any]:
        payload = self.model_dump(exclude_none=True)
        payload.pop("state", None)
        payload["observation_state"] = str(self.observation_state or self.state or default_state).strip().lower()
        if rank is not None:
            payload["rank"] = rank
        if not payload.get("reason_codes"):
            payload["reason_codes"] = unique_text_list(fallback_reason_codes, accept_scalar=True)
        return payload


class CandidateSymbolDiagnosticPayload(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    underlying_symbol: str = Field(validation_alias=AliasChoices("underlying_symbol", "symbol"))
    diagnostic_status: str = Field(default="unknown", validation_alias=AliasChoices("diagnostic_status", "status"))
    observed_at: datetime | None = None
    spot_price: float | None = None
    expiration_count: int = 0
    contract_count: int = 0
    snapshot_count: int = 0
    raw_candidate_count: int = 0
    postprocess_candidate_count: int = 0
    runtime_candidate_count: int = 0
    returned_candidate_count: int = 0

    @field_validator("underlying_symbol", mode="before")
    @classmethod
    def _normalize_underlying_symbol(cls, value: Any) -> str:
        symbol = normalize_symbol(value)
        if symbol is None:
            raise ValueError("underlying_symbol is required")
        return symbol

    @field_validator("diagnostic_status", mode="before")
    @classmethod
    def _normalize_status(cls, value: Any) -> str:
        return as_text(value) or "unknown"

    @field_validator("observed_at", mode="before")
    @classmethod
    def _normalize_observed_at(cls, value: Any) -> datetime | None:
        return parse_datetime(value)

    @field_validator("spot_price", mode="before")
    @classmethod
    def _normalize_spot_price(cls, value: Any) -> float | None:
        return coerce_float(value)

    @field_validator(
        "expiration_count",
        "contract_count",
        "snapshot_count",
        "raw_candidate_count",
        "postprocess_candidate_count",
        "runtime_candidate_count",
        "returned_candidate_count",
        mode="before",
    )
    @classmethod
    def _normalize_count(cls, value: Any) -> int:
        return coerce_int(value) or 0


class TradeDecisionSignalQuery(DomainModel):
    decision_states: list[str] = Field(default_factory=list)
    trading_strategy_ids: list[str] = Field(default_factory=list)
    routine: str | None = None
    session_date: date | None = None
    as_of: datetime | None = None
    limit: int = 100

    @field_validator("decision_states", "trading_strategy_ids", mode="before")
    @classmethod
    def _normalize_text_list(cls, value: Any) -> list[str]:
        return unique_text_list(value, accept_scalar=False)

    @field_validator("routine", mode="before")
    @classmethod
    def _normalize_routine(cls, value: Any) -> str | None:
        return as_text(value)

    @field_validator("session_date", mode="before")
    @classmethod
    def _normalize_session_date(cls, value: Any) -> date | None:
        return None if as_text(value) is None else parse_date(value)

    @field_validator("as_of", mode="before")
    @classmethod
    def _normalize_as_of(cls, value: Any) -> datetime | None:
        return parse_datetime(value)

    @field_validator("limit", mode="before")
    @classmethod
    def _normalize_limit(cls, value: Any, info: ValidationInfo) -> int:
        default = cls.model_fields[str(info.field_name)].default
        return max(coerce_int(value) or int(default), 1)


def _market_context_snapshot_id(snapshot: MarketContextSnapshot) -> str:
    if snapshot.snapshot_id:
        return snapshot.snapshot_id
    basis = "|".join(
        (
            snapshot.scope,
            snapshot.observed_at.isoformat(),
            snapshot.expires_at.isoformat(),
            str(snapshot.context_version),
            snapshot.config_hash or "",
        )
    )
    return f"market_context:{sha1(basis.encode('utf-8')).hexdigest()[:24]}"

__all__ = [
    "CandidateSymbolDiagnosticPayload",
    "TickerSourceObservationPayload",
    "TradeDecisionSignalQuery",
    "_market_context_snapshot_id",
]
