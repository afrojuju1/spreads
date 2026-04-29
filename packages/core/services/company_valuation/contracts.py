from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any, Literal, TypeAlias


OwnershipSourceType: TypeAlias = Literal["form3", "form4", "form5", "13d", "13g", "13f"]
TemplateStatus: TypeAlias = Literal["active", "inactive"]
CompanyValuationDocumentPayload: TypeAlias = dict[str, Any]


@dataclass(frozen=True)
class CompanyValuationTemplate:
    template_id: str
    template_version: str
    status: TemplateStatus
    assignment_rules: dict[str, Any]
    required_features: tuple[str, ...]
    optional_features: tuple[str, ...]
    quality_weight_map: dict[str, int]
    ownership_weight_map: dict[str, int]
    valuation_model_mix: dict[str, Any]
    confidence_rules: dict[str, Any]
    risk_rules: dict[str, Any]
    unsupported_conditions: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompanyValuationTemplateOverride:
    issuer_cik: str
    template_id: str
    reason: str
    active: bool = True

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompanyValuationIdentity:
    issuer_id: str
    cik: str
    ticker: str
    company_name: str
    template_id: str
    template_version: str

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FilingRef:
    filing_id: str
    accession_no: str
    form_type: str
    accepted_at: datetime
    available_at: datetime
    period_end: date | None = None

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OwnershipEvidence:
    source_type: OwnershipSourceType
    holder_id: str | None
    group_id: str | None
    event_date: date | None
    available_at: datetime
    headline: str
    reason_code: str
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OwnershipSignal:
    score: float
    confidence: float
    freshness_days: int | None
    reason_codes: tuple[str, ...] = ()
    evidence: tuple[OwnershipEvidence, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reason_codes"] = list(self.reason_codes)
        payload["evidence"] = [row.to_payload() for row in self.evidence]
        return payload


@dataclass(frozen=True)
class QualityBreakdown:
    total_score: float
    sub_scores: dict[str, float] = field(default_factory=dict)
    factor_contributions: dict[str, float] = field(default_factory=dict)
    reason_codes: tuple[str, ...] = ()
    confidence: float = 0.0

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reason_codes"] = list(self.reason_codes)
        return payload


@dataclass(frozen=True)
class ValuationSummary:
    intrinsic_value_bear: float | None = None
    intrinsic_value_base: float | None = None
    intrinsic_value_bull: float | None = None
    intrinsic_value_mid: float | None = None
    current_price: float | None = None
    valuation_gap: float | None = None
    confidence: float = 0.0
    reason_codes: tuple[str, ...] = ()
    assumption_summary: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reason_codes"] = list(self.reason_codes)
        return payload


@dataclass(frozen=True)
class PointInTimeSnapshot:
    issuer_id: str
    ticker: str | None
    as_of: datetime
    template_id: str | None = None
    latest_filing: dict[str, Any] | None = None
    latest_market_snapshot: dict[str, Any] | None = None
    latest_treasury_curve_snapshot: dict[str, Any] | None = None
    latest_ownership_available_at: datetime | None = None
    latest_company_valuation_snapshot: dict[str, Any] | None = None

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompanyValuationDocument:
    payload_version: str
    issuer: CompanyValuationIdentity
    as_of: datetime
    freshness: dict[str, Any] = field(default_factory=dict)
    source_summary: dict[str, Any] = field(default_factory=dict)
    quality: QualityBreakdown = field(default_factory=lambda: QualityBreakdown(total_score=0.0))
    valuation: ValuationSummary = field(default_factory=ValuationSummary)
    ownership: dict[str, Any] = field(default_factory=dict)
    risks: dict[str, Any] = field(default_factory=dict)
    delta_summary: dict[str, Any] = field(default_factory=dict)
    provenance: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> CompanyValuationDocumentPayload:
        payload = asdict(self)
        payload["issuer"] = self.issuer.to_payload()
        payload["quality"] = self.quality.to_payload()
        payload["valuation"] = self.valuation.to_payload()
        return payload


@dataclass(frozen=True)
class CompanyValuationScreenRow:
    screening_row_id: str
    issuer_id: str
    ticker: str
    template_id: str
    as_of: date
    quality_score: float | None = None
    intrinsic_value_mid: float | None = None
    current_price: float | None = None
    valuation_gap: float | None = None
    quality_confidence: float | None = None
    valuation_confidence: float | None = None
    ownership_score: float | None = None
    ownership_special_situation_flag: bool = False
    limited_coverage_flag: bool = False
    top_reason_codes: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["top_reason_codes"] = list(self.top_reason_codes)
        return payload
