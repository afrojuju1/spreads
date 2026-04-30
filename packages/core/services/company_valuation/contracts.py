from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any, Literal, TypeAlias


OwnershipSourceType: TypeAlias = Literal["form3", "form4", "form5", "13d", "13g", "13f"]
TemplateStatus: TypeAlias = Literal["active", "inactive"]
TaxonomyLevel: TypeAlias = Literal["sector", "industry_group", "industry", "subindustry"]
TaxonomySourceStandard: TypeAlias = Literal["sic", "naics", "issuer_override"]
TaxonomyMatchMode: TypeAlias = Literal["exact", "prefix"]
SupportStatus: TypeAlias = Literal["supported", "unsupported", "out_of_scope"]
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
    stressed_operator_flag: bool = False

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompanyValuationRawClassification:
    sic_code: str | None = None
    sic_title: str | None = None
    naics_code: str | None = None
    naics_title: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompanyValuationTaxonomyNode:
    taxonomy_node_id: str
    taxonomy_version: str
    taxonomy_level: TaxonomyLevel
    taxonomy_code: str
    taxonomy_name: str
    parent_taxonomy_node_id: str | None = None
    active: bool = True

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompanyValuationTaxonomyMapping:
    mapping_id: str
    mapping_version: str
    source_standard: TaxonomySourceStandard
    source_code: str
    source_title: str | None = None
    match_mode: TaxonomyMatchMode = "exact"
    canonical_sector_id: str | None = None
    canonical_industry_group_id: str | None = None
    canonical_industry_id: str | None = None
    canonical_subindustry_id: str | None = None
    priority: int = 100
    active: bool = True
    notes: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompanyValuationTemplateMapping:
    mapping_id: str
    mapping_version: str
    taxonomy_node_id: str
    taxonomy_level: TaxonomyLevel
    template_id: str
    active: bool = True
    notes: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompanyValuationTaxonomyOverride:
    issuer_cik: str
    canonical_sector_id: str
    canonical_industry_group_id: str | None = None
    canonical_industry_id: str | None = None
    canonical_subindustry_id: str | None = None
    reason: str = ""
    active: bool = True

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompanyValuationOverlayRule:
    rule_id: str
    rule_version: str
    flag_key: str
    reason: str
    company_name_keywords: tuple[str, ...] = ()
    sic_title_keywords: tuple[str, ...] = ()
    sic_prefixes: tuple[str, ...] = ()
    naics_prefixes: tuple[str, ...] = ()
    issuer_ciks: tuple[str, ...] = ()
    active: bool = True

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["company_name_keywords"] = list(self.company_name_keywords)
        payload["sic_title_keywords"] = list(self.sic_title_keywords)
        payload["sic_prefixes"] = list(self.sic_prefixes)
        payload["naics_prefixes"] = list(self.naics_prefixes)
        payload["issuer_ciks"] = list(self.issuer_ciks)
        return payload


@dataclass(frozen=True)
class CompanyValuationCanonicalTaxonomy:
    taxonomy_version: str
    canonical_sector_id: str | None = None
    canonical_industry_group_id: str | None = None
    canonical_industry_id: str | None = None
    canonical_subindustry_id: str | None = None
    classification_source: str = "unclassified"
    classification_confidence: float = 0.0
    source_standard: TaxonomySourceStandard | None = None
    mapping_id: str | None = None
    mapping_version: str | None = None
    reason: str = ""

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompanyValuationDefaultTemplateResolution:
    template_id: str
    template_version: str
    source: str
    reason: str
    mapping_id: str | None = None
    mapping_version: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompanyValuationSupportedIssuer:
    ticker: str
    expected_template_id: str | None = None
    reason: str = ""
    active: bool = True

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompanyValuationSupportPolicy:
    policy_version: str
    allowlist_required: bool = True
    supported_template_ids: tuple[str, ...] = ()
    supported_issuers: tuple[CompanyValuationSupportedIssuer, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["supported_template_ids"] = list(self.supported_template_ids)
        payload["supported_issuers"] = [
            row.to_payload() for row in self.supported_issuers
        ]
        return payload


@dataclass(frozen=True)
class CompanyValuationSupportResolution:
    status: SupportStatus
    reason: str
    in_curated_universe: bool = False
    expected_template_id: str | None = None
    expected_template_match: bool | None = None

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompanyValuationOverlayResolution:
    flags: dict[str, bool] = field(default_factory=dict)
    reasons: dict[str, str] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return {
            "flags": dict(self.flags),
            "reasons": dict(self.reasons),
        }


@dataclass(frozen=True)
class CompanyValuationTaxonomyResolution:
    raw_classification: CompanyValuationRawClassification
    canonical_taxonomy: CompanyValuationCanonicalTaxonomy
    default_template: CompanyValuationDefaultTemplateResolution
    support: CompanyValuationSupportResolution = field(
        default_factory=lambda: CompanyValuationSupportResolution(
            status="out_of_scope",
            reason="support status not resolved",
        )
    )
    overlays: CompanyValuationOverlayResolution = field(
        default_factory=CompanyValuationOverlayResolution
    )

    def to_payload(self) -> dict[str, Any]:
        return {
            "raw_classification": self.raw_classification.to_payload(),
            "canonical_taxonomy": self.canonical_taxonomy.to_payload(),
            "default_template": self.default_template.to_payload(),
            "support": self.support.to_payload(),
            "overlays": self.overlays.to_payload(),
        }


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
    latest_statement_snapshot: dict[str, Any] | None = None
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
    stressed_operator_flag: bool = False
    top_reason_codes: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["top_reason_codes"] = list(self.top_reason_codes)
        return payload
