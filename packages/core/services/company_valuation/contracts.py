from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, field_validator


OwnershipSourceType: TypeAlias = Literal["form3", "form4", "form5", "13d", "13g", "13f"]
TemplateStatus: TypeAlias = Literal["active", "inactive"]
TaxonomyLevel: TypeAlias = Literal["sector", "industry_group", "industry", "subindustry"]
TaxonomySourceStandard: TypeAlias = Literal["sic", "naics", "issuer_override"]
TaxonomyMatchMode: TypeAlias = Literal["exact", "prefix"]
SupportStatus: TypeAlias = Literal["supported", "unsupported", "out_of_scope"]
SupportTier: TypeAlias = Literal["core", "expanded"]
CompanyValuationDocumentPayload: TypeAlias = dict[str, Any]


class CompanyValuationContractModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    @field_validator("*", mode="before")
    @classmethod
    def _normalize_blank_strings(cls, value: Any) -> Any:
        if isinstance(value, str):
            rendered = value.strip()
            return rendered or None
        return value


class CompanyValuationTemplate(CompanyValuationContractModel):
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


class CompanyValuationTemplateOverride(CompanyValuationContractModel):
    issuer_cik: str
    template_id: str
    reason: str
    active: bool = True
    stressed_operator_flag: bool = False


class CompanyValuationBenchmarkPriorEntry(CompanyValuationContractModel):
    ticker: str
    analyst_count: int | None = None
    consensus_rating: str | None = None
    average_target: float | None = None
    median_target: float | None = None
    low_target: float | None = None
    high_target: float | None = None
    source_url: str | None = None
    active: bool = True


class CompanyValuationBenchmarkPriorSet(CompanyValuationContractModel):
    prior_set_id: str
    basket_id: str
    template_id: str
    as_of: str
    source_name: str
    target_field: str = "average_target"
    supported_only_default: bool = True
    minimum_coverage: int = 5
    trigger_mean_abs_gap_delta: float = 0.2
    trigger_sign_mismatch_count: int = 1
    source_notes: str = ""
    entries: tuple[CompanyValuationBenchmarkPriorEntry, ...] = ()


class CompanyValuationRawClassification(CompanyValuationContractModel):
    sic_code: str | None = None
    sic_title: str | None = None
    naics_code: str | None = None
    naics_title: str | None = None


class CompanyValuationTaxonomyNode(CompanyValuationContractModel):
    taxonomy_node_id: str
    taxonomy_version: str
    taxonomy_level: TaxonomyLevel
    taxonomy_code: str
    taxonomy_name: str
    parent_taxonomy_node_id: str | None = None
    active: bool = True


class CompanyValuationTaxonomyMapping(CompanyValuationContractModel):
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


class CompanyValuationTemplateMapping(CompanyValuationContractModel):
    mapping_id: str
    mapping_version: str
    taxonomy_node_id: str
    taxonomy_level: TaxonomyLevel
    template_id: str
    active: bool = True
    notes: str | None = None


class CompanyValuationTaxonomyOverride(CompanyValuationContractModel):
    issuer_cik: str
    canonical_sector_id: str
    canonical_industry_group_id: str | None = None
    canonical_industry_id: str | None = None
    canonical_subindustry_id: str | None = None
    reason: str = ""
    active: bool = True


class CompanyValuationOverlayRule(CompanyValuationContractModel):
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


class CompanyValuationCanonicalTaxonomy(CompanyValuationContractModel):
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


class CompanyValuationDefaultTemplateResolution(CompanyValuationContractModel):
    template_id: str
    template_version: str
    source: str
    reason: str
    mapping_id: str | None = None
    mapping_version: str | None = None


class CompanyValuationSupportedIssuer(CompanyValuationContractModel):
    ticker: str
    expected_template_id: str | None = None
    support_tier: SupportTier = "core"
    reason: str = ""
    active: bool = True


class CompanyValuationSupportPolicy(CompanyValuationContractModel):
    policy_version: str
    allowlist_required: bool = True
    supported_template_ids: tuple[str, ...] = ()
    supported_issuers: tuple[CompanyValuationSupportedIssuer, ...] = ()


class CompanyValuationSupportResolution(CompanyValuationContractModel):
    status: SupportStatus
    reason: str
    in_curated_universe: bool = False
    support_tier: SupportTier | None = None
    expected_template_id: str | None = None
    expected_template_match: bool | None = None


class CompanyValuationOverlayResolution(CompanyValuationContractModel):
    flags: dict[str, bool] = Field(default_factory=dict)
    reasons: dict[str, str] = Field(default_factory=dict)


class CompanyValuationTaxonomyResolution(CompanyValuationContractModel):
    raw_classification: CompanyValuationRawClassification
    canonical_taxonomy: CompanyValuationCanonicalTaxonomy
    default_template: CompanyValuationDefaultTemplateResolution
    support: CompanyValuationSupportResolution = Field(
        default_factory=lambda: CompanyValuationSupportResolution(
            status="out_of_scope",
            reason="support status not resolved",
        )
    )
    overlays: CompanyValuationOverlayResolution = Field(default_factory=CompanyValuationOverlayResolution)


class CompanyValuationIdentity(CompanyValuationContractModel):
    issuer_id: str
    cik: str
    ticker: str
    company_name: str
    template_id: str
    template_version: str


class FilingRef(CompanyValuationContractModel):
    filing_id: str
    accession_no: str
    form_type: str
    accepted_at: datetime
    available_at: datetime
    period_end: date | None = None


class OwnershipEvidence(CompanyValuationContractModel):
    source_type: OwnershipSourceType
    holder_id: str | None
    group_id: str | None
    event_date: date | None
    available_at: datetime
    headline: str
    reason_code: str
    metrics: dict[str, Any] = Field(default_factory=dict)


class OwnershipSignal(CompanyValuationContractModel):
    score: float
    confidence: float
    freshness_days: int | None
    reason_codes: tuple[str, ...] = ()
    evidence: tuple[OwnershipEvidence, ...] = ()


class QualityBreakdown(CompanyValuationContractModel):
    total_score: float
    sub_scores: dict[str, float] = Field(default_factory=dict)
    factor_contributions: dict[str, float] = Field(default_factory=dict)
    reason_codes: tuple[str, ...] = ()
    confidence: float = 0.0


class ValuationSummary(CompanyValuationContractModel):
    intrinsic_value_bear: float | None = None
    intrinsic_value_base: float | None = None
    intrinsic_value_bull: float | None = None
    intrinsic_value_mid: float | None = None
    current_price: float | None = None
    valuation_gap: float | None = None
    confidence: float = 0.0
    reason_codes: tuple[str, ...] = ()
    assumption_summary: dict[str, Any] = Field(default_factory=dict)


class PointInTimeSnapshot(CompanyValuationContractModel):
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


class CompanyValuationDocument(CompanyValuationContractModel):
    payload_version: str
    issuer: CompanyValuationIdentity
    as_of: datetime
    freshness: dict[str, Any] = Field(default_factory=dict)
    source_summary: dict[str, Any] = Field(default_factory=dict)
    quality: QualityBreakdown = Field(default_factory=lambda: QualityBreakdown(total_score=0.0))
    valuation: ValuationSummary = Field(default_factory=ValuationSummary)
    ownership: dict[str, Any] = Field(default_factory=dict)
    risks: dict[str, Any] = Field(default_factory=dict)
    delta_summary: dict[str, Any] = Field(default_factory=dict)
    provenance: dict[str, Any] = Field(default_factory=dict)


class CompanyValuationScreenRow(CompanyValuationContractModel):
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
