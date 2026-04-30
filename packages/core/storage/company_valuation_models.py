from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import BigInteger, Boolean, Date, DateTime, Float, ForeignKey, Index, Integer, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from core.storage.db import Base


class IssuerModel(Base):
    __tablename__ = "issuers"
    __table_args__ = (
        Index("idx_issuers_template_id", "template_id"),
        Index("idx_issuers_limited_coverage", "limited_coverage_flag"),
    )

    issuer_id: Mapped[str] = mapped_column(Text, primary_key=True)
    cik: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    company_name: Mapped[str] = mapped_column(Text, nullable=False)
    sic: Mapped[str | None] = mapped_column(Text, nullable=True)
    sic_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    naics: Mapped[str | None] = mapped_column(Text, nullable=True)
    template_id: Mapped[str] = mapped_column(Text, nullable=False)
    template_version: Mapped[str] = mapped_column(Text, nullable=False)
    template_assignment_source: Mapped[str] = mapped_column(Text, nullable=False)
    template_assignment_reason: Mapped[str] = mapped_column(Text, nullable=False)
    limited_coverage_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TaxonomyNodeModel(Base):
    __tablename__ = "taxonomy_nodes"
    __table_args__ = (
        UniqueConstraint(
            "taxonomy_version",
            "taxonomy_level",
            "taxonomy_code",
            name="ux_taxonomy_nodes_version_level_code",
        ),
        Index("idx_taxonomy_nodes_level", "taxonomy_level"),
        Index("idx_taxonomy_nodes_parent", "parent_taxonomy_node_id"),
    )

    taxonomy_node_id: Mapped[str] = mapped_column(Text, primary_key=True)
    taxonomy_version: Mapped[str] = mapped_column(Text, nullable=False)
    taxonomy_level: Mapped[str] = mapped_column(Text, nullable=False)
    taxonomy_code: Mapped[str] = mapped_column(Text, nullable=False)
    taxonomy_name: Mapped[str] = mapped_column(Text, nullable=False)
    parent_taxonomy_node_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("taxonomy_nodes.taxonomy_node_id", ondelete="SET NULL"),
        nullable=True,
    )
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class TaxonomyMappingModel(Base):
    __tablename__ = "taxonomy_mappings"
    __table_args__ = (
        Index(
            "idx_taxonomy_mappings_standard_code",
            "source_standard",
            "source_code",
        ),
        Index("idx_taxonomy_mappings_subindustry", "canonical_subindustry_id"),
    )

    mapping_id: Mapped[str] = mapped_column(Text, primary_key=True)
    mapping_version: Mapped[str] = mapped_column(Text, nullable=False)
    source_standard: Mapped[str] = mapped_column(Text, nullable=False)
    source_code: Mapped[str] = mapped_column(Text, nullable=False)
    source_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    match_mode: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_sector_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("taxonomy_nodes.taxonomy_node_id", ondelete="SET NULL"),
        nullable=True,
    )
    canonical_industry_group_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("taxonomy_nodes.taxonomy_node_id", ondelete="SET NULL"),
        nullable=True,
    )
    canonical_industry_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("taxonomy_nodes.taxonomy_node_id", ondelete="SET NULL"),
        nullable=True,
    )
    canonical_subindustry_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("taxonomy_nodes.taxonomy_node_id", ondelete="SET NULL"),
        nullable=True,
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)


class ValuationTemplateMappingModel(Base):
    __tablename__ = "valuation_template_mappings"
    __table_args__ = (
        UniqueConstraint(
            "mapping_version",
            "taxonomy_node_id",
            name="ux_valuation_template_mappings_version_node",
        ),
        Index("idx_valuation_template_mappings_template", "template_id"),
    )

    mapping_id: Mapped[str] = mapped_column(Text, primary_key=True)
    mapping_version: Mapped[str] = mapped_column(Text, nullable=False)
    taxonomy_node_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("taxonomy_nodes.taxonomy_node_id", ondelete="CASCADE"),
        nullable=False,
    )
    taxonomy_level: Mapped[str] = mapped_column(Text, nullable=False)
    template_id: Mapped[str] = mapped_column(Text, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)


class IssuerClassificationModel(Base):
    __tablename__ = "issuer_classifications"
    __table_args__ = (
        Index("idx_issuer_classifications_source", "classification_source"),
        Index(
            "idx_issuer_classifications_subindustry",
            "canonical_subindustry_id",
        ),
    )

    issuer_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("issuers.issuer_id", ondelete="CASCADE"),
        primary_key=True,
    )
    taxonomy_version: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_sector_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("taxonomy_nodes.taxonomy_node_id", ondelete="SET NULL"),
        nullable=True,
    )
    canonical_industry_group_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("taxonomy_nodes.taxonomy_node_id", ondelete="SET NULL"),
        nullable=True,
    )
    canonical_industry_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("taxonomy_nodes.taxonomy_node_id", ondelete="SET NULL"),
        nullable=True,
    )
    canonical_subindustry_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("taxonomy_nodes.taxonomy_node_id", ondelete="SET NULL"),
        nullable=True,
    )
    classification_source: Mapped[str] = mapped_column(Text, nullable=False)
    classification_confidence: Mapped[float] = mapped_column(Float, nullable=False)
    taxonomy_mapping_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("taxonomy_mappings.mapping_id", ondelete="SET NULL"),
        nullable=True,
    )
    valuation_template_mapping_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("valuation_template_mappings.mapping_id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class IssuerOverlayFlagModel(Base):
    __tablename__ = "issuer_overlay_flags"
    __table_args__ = (
        UniqueConstraint("issuer_id", "flag_key", name="ux_issuer_overlay_flags_issuer_flag"),
        Index("idx_issuer_overlay_flags_flag_key", "flag_key"),
    )

    issuer_overlay_flag_id: Mapped[str] = mapped_column(Text, primary_key=True)
    issuer_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("issuers.issuer_id", ondelete="CASCADE"),
        nullable=False,
    )
    flag_key: Mapped[str] = mapped_column(Text, nullable=False)
    flag_value: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class SecurityModel(Base):
    __tablename__ = "securities"
    __table_args__ = (
        UniqueConstraint("issuer_id", "ticker", name="ux_securities_issuer_ticker"),
        Index("idx_securities_issuer_primary", "issuer_id", "is_primary"),
        Index("idx_securities_cusip", "cusip"),
    )

    security_id: Mapped[str] = mapped_column(Text, primary_key=True)
    issuer_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("issuers.issuer_id", ondelete="CASCADE"),
        nullable=False,
    )
    ticker: Mapped[str] = mapped_column(Text, nullable=False)
    share_class: Mapped[str | None] = mapped_column(Text, nullable=True)
    exchange: Mapped[str | None] = mapped_column(Text, nullable=True)
    cusip: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    active_from: Mapped[date | None] = mapped_column(Date, nullable=True)
    active_to: Mapped[date | None] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class SecurityIdentifierHistoryModel(Base):
    __tablename__ = "security_identifier_history"
    __table_args__ = (
        UniqueConstraint(
            "identifier_type",
            "identifier_value",
            "effective_from",
            "security_id",
            name="ux_security_identifier_history",
        ),
        Index(
            "idx_security_identifier_history_lookup",
            "identifier_type",
            "identifier_value",
            "effective_to",
        ),
        Index(
            "idx_security_identifier_history_security",
            "security_id",
            "effective_from",
        ),
    )

    security_identifier_id: Mapped[str] = mapped_column(Text, primary_key=True)
    security_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("securities.security_id", ondelete="CASCADE"),
        nullable=False,
    )
    issuer_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("issuers.issuer_id", ondelete="CASCADE"),
        nullable=False,
    )
    identifier_type: Mapped[str] = mapped_column(Text, nullable=False)
    identifier_value: Mapped[str] = mapped_column(Text, nullable=False)
    issuer_name_reported: Mapped[str | None] = mapped_column(Text, nullable=True)
    title_of_class: Mapped[str | None] = mapped_column(Text, nullable=True)
    effective_from: Mapped[date | None] = mapped_column(Date, nullable=True)
    effective_to: Mapped[date | None] = mapped_column(Date, nullable=True)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    source_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    match_confidence: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class FilingModel(Base):
    __tablename__ = "filings"
    __table_args__ = (
        Index("idx_filings_issuer_form_available", "issuer_id", "form_type", "available_at"),
        Index("idx_filings_accepted_at", "accepted_at"),
        Index("idx_filings_period_end", "period_end"),
    )

    filing_id: Mapped[str] = mapped_column(Text, primary_key=True)
    issuer_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("issuers.issuer_id", ondelete="CASCADE"),
        nullable=False,
    )
    accession_no: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    form_type: Mapped[str] = mapped_column(Text, nullable=False)
    period_end: Mapped[date | None] = mapped_column(Date, nullable=True)
    filed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    accepted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    amendment_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    amendment_of_accession_no: Mapped[str | None] = mapped_column(Text, nullable=True)
    primary_document_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    primary_xml_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_storage_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_sha256: Mapped[str | None] = mapped_column(Text, nullable=True)
    parse_status: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class XbrlFactModel(Base):
    __tablename__ = "xbrl_facts"
    __table_args__ = (
        UniqueConstraint("fact_hash", name="ux_xbrl_facts_fact_hash"),
        Index("idx_xbrl_facts_issuer_concept_period", "issuer_id", "concept_name", "period_end"),
    )

    fact_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    filing_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("filings.filing_id", ondelete="CASCADE"),
        nullable=False,
    )
    issuer_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("issuers.issuer_id", ondelete="CASCADE"),
        nullable=False,
    )
    taxonomy: Mapped[str] = mapped_column(Text, nullable=False)
    concept_name: Mapped[str] = mapped_column(Text, nullable=False)
    unit: Mapped[str] = mapped_column(Text, nullable=False)
    period_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    instant_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    dimensions_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    value_numeric: Mapped[float | None] = mapped_column(Float, nullable=True)
    value_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    decimals: Mapped[str | None] = mapped_column(Text, nullable=True)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    fact_hash: Mapped[str] = mapped_column(Text, nullable=False)


class StatementPeriodSnapshotModel(Base):
    __tablename__ = "statement_period_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "issuer_id",
            "filing_id",
            "period_type",
            "period_end",
            "normalization_version",
            name="ux_statement_period_snapshots",
        ),
        Index("idx_statement_period_snapshots_issuer_period_end", "issuer_id", "period_end"),
    )

    snapshot_id: Mapped[str] = mapped_column(Text, primary_key=True)
    issuer_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("issuers.issuer_id", ondelete="CASCADE"),
        nullable=False,
    )
    filing_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("filings.filing_id", ondelete="CASCADE"),
        nullable=False,
    )
    period_type: Mapped[str] = mapped_column(Text, nullable=False)
    fiscal_year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    fiscal_period: Mapped[str | None] = mapped_column(Text, nullable=True)
    period_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    normalization_version: Mapped[str] = mapped_column(Text, nullable=False)
    metrics_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    source_fact_refs_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class TreasuryCurveSnapshotModel(Base):
    __tablename__ = "treasury_curve_snapshots"

    curve_snapshot_id: Mapped[str] = mapped_column(Text, primary_key=True)
    curve_date: Mapped[date] = mapped_column(Date, nullable=False, unique=True)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    curve_points_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)


class MarketSnapshotModel(Base):
    __tablename__ = "market_snapshots"
    __table_args__ = (
        Index("idx_market_snapshots_issuer_available", "issuer_id", "available_at"),
        Index("idx_market_snapshots_security_available", "security_id", "available_at"),
    )

    market_snapshot_id: Mapped[str] = mapped_column(Text, primary_key=True)
    security_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("securities.security_id", ondelete="CASCADE"),
        nullable=False,
    )
    issuer_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("issuers.issuer_id", ondelete="CASCADE"),
        nullable=False,
    )
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    shares_outstanding_market: Mapped[float | None] = mapped_column(Float, nullable=True)
    market_cap: Mapped[float | None] = mapped_column(Float, nullable=True)
    enterprise_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    source: Mapped[str] = mapped_column(Text, nullable=False)


class BeneficialOwnershipFilingModel(Base):
    __tablename__ = "beneficial_ownership_filings"
    __table_args__ = (
        Index("idx_beneficial_ownership_filings_issuer_event_date", "issuer_id", "event_date"),
        Index("idx_beneficial_ownership_filings_issuer_schedule", "issuer_id", "schedule_type", "filing_id"),
    )

    filing_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("filings.filing_id", ondelete="CASCADE"),
        primary_key=True,
    )
    issuer_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("issuers.issuer_id", ondelete="CASCADE"),
        nullable=False,
    )
    schedule_type: Mapped[str] = mapped_column(Text, nullable=False)
    event_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    passive_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    control_intent_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    group_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    amendment_no: Mapped[int | None] = mapped_column(Integer, nullable=True)
    prior_schedule_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    item4_purpose_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    item5_interest_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    item6_derivative_or_arrangement_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    ownership_xml_version: Mapped[str | None] = mapped_column(Text, nullable=True)


class BeneficialOwnerModel(Base):
    __tablename__ = "beneficial_owners"
    __table_args__ = (
        UniqueConstraint("normalized_name", "holder_cik", name="ux_beneficial_owners_name_cik"),
        Index("idx_beneficial_owners_parent", "parent_holder_id"),
    )

    holder_id: Mapped[str] = mapped_column(Text, primary_key=True)
    canonical_name: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_name: Mapped[str] = mapped_column(Text, nullable=False)
    holder_cik: Mapped[str | None] = mapped_column(Text, nullable=True)
    holder_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    parent_holder_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("beneficial_owners.holder_id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class BeneficialOwnerGroupModel(Base):
    __tablename__ = "beneficial_owner_groups"
    __table_args__ = (
        Index("idx_beneficial_owner_groups_issuer_effective_from", "issuer_id", "effective_from"),
    )

    group_id: Mapped[str] = mapped_column(Text, primary_key=True)
    issuer_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("issuers.issuer_id", ondelete="CASCADE"),
        nullable=False,
    )
    group_name: Mapped[str] = mapped_column(Text, nullable=False)
    group_kind: Mapped[str] = mapped_column(Text, nullable=False)
    root_filing_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("filings.filing_id", ondelete="CASCADE"),
        nullable=False,
    )
    effective_from: Mapped[date | None] = mapped_column(Date, nullable=True)
    effective_to: Mapped[date | None] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class BeneficialOwnerGroupMembershipModel(Base):
    __tablename__ = "beneficial_owner_group_memberships"
    __table_args__ = (
        Index("idx_beneficial_owner_group_memberships_holder", "holder_id"),
    )

    group_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("beneficial_owner_groups.group_id", ondelete="CASCADE"),
        primary_key=True,
    )
    holder_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("beneficial_owners.holder_id", ondelete="CASCADE"),
        primary_key=True,
    )
    filing_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("filings.filing_id", ondelete="CASCADE"),
        primary_key=True,
    )
    member_role: Mapped[str | None] = mapped_column(Text, nullable=True)
    effective_from: Mapped[date | None] = mapped_column(Date, nullable=True)
    effective_to: Mapped[date | None] = mapped_column(Date, nullable=True)


class BeneficialOwnerPositionModel(Base):
    __tablename__ = "beneficial_owner_positions"
    __table_args__ = (
        UniqueConstraint("source_row_hash", name="ux_beneficial_owner_positions_source_row_hash"),
        Index("idx_beneficial_owner_positions_issuer_available", "issuer_id", "available_at"),
        Index("idx_beneficial_owner_positions_holder_available", "holder_id", "available_at"),
        Index("idx_beneficial_owner_positions_issuer_ownership_pct", "issuer_id", "ownership_pct"),
    )

    position_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    issuer_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("issuers.issuer_id", ondelete="CASCADE"),
        nullable=False,
    )
    holder_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("beneficial_owners.holder_id", ondelete="CASCADE"),
        nullable=False,
    )
    group_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("beneficial_owner_groups.group_id", ondelete="SET NULL"),
        nullable=True,
    )
    filing_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("filings.filing_id", ondelete="CASCADE"),
        nullable=False,
    )
    schedule_type: Mapped[str] = mapped_column(Text, nullable=False)
    event_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    share_count_reported: Mapped[float | None] = mapped_column(Float, nullable=True)
    ownership_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    sole_voting_power: Mapped[float | None] = mapped_column(Float, nullable=True)
    shared_voting_power: Mapped[float | None] = mapped_column(Float, nullable=True)
    sole_dispositive_power: Mapped[float | None] = mapped_column(Float, nullable=True)
    shared_dispositive_power: Mapped[float | None] = mapped_column(Float, nullable=True)
    passive_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    control_intent_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    derivative_exposure_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    source_row_hash: Mapped[str] = mapped_column(Text, nullable=False)


class InsiderTransactionModel(Base):
    __tablename__ = "insider_transactions"
    __table_args__ = (
        Index("idx_insider_transactions_issuer_available", "issuer_id", "available_at"),
        Index("idx_insider_transactions_holder_available", "holder_id", "available_at"),
        Index("idx_insider_transactions_issuer_transaction_date", "issuer_id", "transaction_date"),
    )

    insider_transaction_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )
    issuer_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("issuers.issuer_id", ondelete="CASCADE"),
        nullable=False,
    )
    holder_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("beneficial_owners.holder_id", ondelete="SET NULL"),
        nullable=True,
    )
    filing_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("filings.filing_id", ondelete="CASCADE"),
        nullable=False,
    )
    transaction_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    transaction_code: Mapped[str] = mapped_column(Text, nullable=False)
    security_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    shares_delta: Mapped[float | None] = mapped_column(Float, nullable=True)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    shares_owned_after: Mapped[float | None] = mapped_column(Float, nullable=True)
    ownership_nature: Mapped[str | None] = mapped_column(Text, nullable=True)
    footnotes_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class InstitutionalHolderModel(Base):
    __tablename__ = "institutional_holders"
    __table_args__ = (
        UniqueConstraint("normalized_name", "manager_cik", name="ux_institutional_holders_name_cik"),
    )

    institutional_holder_id: Mapped[str] = mapped_column(Text, primary_key=True)
    manager_cik: Mapped[str | None] = mapped_column(Text, nullable=True)
    manager_name: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_name: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class InstitutionalFilingModel(Base):
    __tablename__ = "institutional_filings"
    __table_args__ = (
        Index(
            "idx_institutional_filings_holder_period",
            "institutional_holder_id",
            "report_period",
        ),
        Index(
            "idx_institutional_filings_manager_period",
            "manager_cik",
            "report_period",
        ),
    )

    filing_id: Mapped[str] = mapped_column(Text, primary_key=True)
    institutional_holder_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("institutional_holders.institutional_holder_id", ondelete="CASCADE"),
        nullable=False,
    )
    accession_no: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    manager_cik: Mapped[str | None] = mapped_column(Text, nullable=True)
    manager_name: Mapped[str] = mapped_column(Text, nullable=False)
    submission_type: Mapped[str] = mapped_column(Text, nullable=False)
    report_period: Mapped[date] = mapped_column(Date, nullable=False)
    filed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    accepted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    amendment_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    amendment_no: Mapped[int | None] = mapped_column(Integer, nullable=True)
    report_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    form13f_file_number: Mapped[str | None] = mapped_column(Text, nullable=True)
    crd_number: Mapped[str | None] = mapped_column(Text, nullable=True)
    sec_file_number: Mapped[str | None] = mapped_column(Text, nullable=True)
    other_included_managers_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    table_entry_total: Mapped[int | None] = mapped_column(Integer, nullable=True)
    table_value_total: Mapped[float | None] = mapped_column(Float, nullable=True)
    is_confidential_omitted: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    additional_information: Mapped[str | None] = mapped_column(Text, nullable=True)
    primary_document_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    information_table_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_dataset_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    parse_status: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class InstitutionalPositionModel(Base):
    __tablename__ = "institutional_positions"
    __table_args__ = (
        UniqueConstraint("source_row_hash", name="ux_institutional_positions_source_row_hash"),
        Index("idx_institutional_positions_issuer_report_period", "issuer_id", "report_period"),
        Index(
            "idx_institutional_positions_holder_report_period",
            "institutional_holder_id",
            "report_period",
        ),
    )

    institutional_position_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )
    institutional_holder_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("institutional_holders.institutional_holder_id", ondelete="CASCADE"),
        nullable=False,
    )
    issuer_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("issuers.issuer_id", ondelete="CASCADE"),
        nullable=False,
    )
    filing_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("institutional_filings.filing_id", ondelete="CASCADE"),
        nullable=False,
    )
    report_period: Mapped[date] = mapped_column(Date, nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    issuer_name_reported: Mapped[str | None] = mapped_column(Text, nullable=True)
    title_of_class: Mapped[str | None] = mapped_column(Text, nullable=True)
    cusip: Mapped[str | None] = mapped_column(Text, nullable=True)
    figi: Mapped[str | None] = mapped_column(Text, nullable=True)
    share_count: Mapped[float | None] = mapped_column(Float, nullable=True)
    market_value_reported: Mapped[float | None] = mapped_column(Float, nullable=True)
    put_call: Mapped[str | None] = mapped_column(Text, nullable=True)
    discretion_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    other_manager_refs_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    voting_authority_sole: Mapped[float | None] = mapped_column(Float, nullable=True)
    voting_authority_shared: Mapped[float | None] = mapped_column(Float, nullable=True)
    voting_authority_none: Mapped[float | None] = mapped_column(Float, nullable=True)
    resolution_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolution_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    source_row_hash: Mapped[str] = mapped_column(Text, nullable=False)


class UnresolvedInstitutionalPositionModel(Base):
    __tablename__ = "unresolved_institutional_positions"
    __table_args__ = (
        Index(
            "idx_unresolved_institutional_positions_status_retry",
            "resolution_status",
            "next_retry_at",
        ),
        Index(
            "idx_unresolved_institutional_positions_cusip_status",
            "cusip",
            "resolution_status",
        ),
        Index(
            "idx_unresolved_institutional_positions_report_period",
            "report_period",
            "resolution_status",
        ),
    )

    source_row_hash: Mapped[str] = mapped_column(Text, primary_key=True)
    institutional_holder_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("institutional_holders.institutional_holder_id", ondelete="CASCADE"),
        nullable=False,
    )
    filing_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("institutional_filings.filing_id", ondelete="CASCADE"),
        nullable=False,
    )
    report_period: Mapped[date] = mapped_column(Date, nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    issuer_name_reported: Mapped[str | None] = mapped_column(Text, nullable=True)
    title_of_class: Mapped[str | None] = mapped_column(Text, nullable=True)
    cusip: Mapped[str | None] = mapped_column(Text, nullable=True)
    figi: Mapped[str | None] = mapped_column(Text, nullable=True)
    share_count: Mapped[float | None] = mapped_column(Float, nullable=True)
    market_value_reported: Mapped[float | None] = mapped_column(Float, nullable=True)
    put_call: Mapped[str | None] = mapped_column(Text, nullable=True)
    discretion_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    other_manager_refs_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    voting_authority_sole: Mapped[float | None] = mapped_column(Float, nullable=True)
    voting_authority_shared: Mapped[float | None] = mapped_column(Float, nullable=True)
    voting_authority_none: Mapped[float | None] = mapped_column(Float, nullable=True)
    official_list_issuer_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    official_list_title_of_class: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolution_status: Mapped[str] = mapped_column(Text, nullable=False)
    resolution_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolution_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    resolution_attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class FeatureSnapshotModel(Base):
    __tablename__ = "feature_snapshots"
    __table_args__ = (
        UniqueConstraint("issuer_id", "as_of", "feature_version", name="ux_feature_snapshots"),
        Index("idx_feature_snapshots_template_as_of", "template_id", "as_of"),
    )

    feature_snapshot_id: Mapped[str] = mapped_column(Text, primary_key=True)
    issuer_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("issuers.issuer_id", ondelete="CASCADE"),
        nullable=False,
    )
    template_id: Mapped[str] = mapped_column(Text, nullable=False)
    template_version: Mapped[str] = mapped_column(Text, nullable=False)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    feature_version: Mapped[str] = mapped_column(Text, nullable=False)
    financial_features_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    ownership_features_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    dependency_refs_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class CompanyValuationSnapshotModel(Base):
    __tablename__ = "company_valuation_snapshots"
    __table_args__ = (
        UniqueConstraint("issuer_id", "as_of", "evaluation_version", name="ux_company_valuation_snapshots"),
        Index("idx_company_valuation_snapshots_issuer_as_of", "issuer_id", "as_of"),
        Index("idx_company_valuation_snapshots_template_as_of", "template_id", "as_of"),
    )

    company_valuation_snapshot_id: Mapped[str] = mapped_column(Text, primary_key=True)
    issuer_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("issuers.issuer_id", ondelete="CASCADE"),
        nullable=False,
    )
    security_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("securities.security_id", ondelete="SET NULL"),
        nullable=True,
    )
    template_id: Mapped[str] = mapped_column(Text, nullable=False)
    template_version: Mapped[str] = mapped_column(Text, nullable=False)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    evaluation_version: Mapped[str] = mapped_column(Text, nullable=False)
    quality_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    ownership_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    intrinsic_value_bear: Mapped[float | None] = mapped_column(Float, nullable=True)
    intrinsic_value_base: Mapped[float | None] = mapped_column(Float, nullable=True)
    intrinsic_value_bull: Mapped[float | None] = mapped_column(Float, nullable=True)
    intrinsic_value_mid: Mapped[float | None] = mapped_column(Float, nullable=True)
    current_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    valuation_gap: Mapped[float | None] = mapped_column(Float, nullable=True)
    quality_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    valuation_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    limited_coverage_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    top_reason_codes_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    valuation_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ScreeningRowModel(Base):
    __tablename__ = "screening_rows"
    __table_args__ = (
        UniqueConstraint("issuer_id", "as_of", name="ux_screening_rows_issuer_as_of"),
        Index("idx_screening_rows_template_as_of_quality", "template_id", "as_of", "quality_score"),
        Index("idx_screening_rows_as_of_valuation_gap", "as_of", "valuation_gap"),
        Index("idx_screening_rows_as_of_special_situation", "as_of", "ownership_special_situation_flag"),
    )

    screening_row_id: Mapped[str] = mapped_column(Text, primary_key=True)
    issuer_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("issuers.issuer_id", ondelete="CASCADE"),
        nullable=False,
    )
    security_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("securities.security_id", ondelete="SET NULL"),
        nullable=True,
    )
    ticker: Mapped[str] = mapped_column(Text, nullable=False)
    template_id: Mapped[str] = mapped_column(Text, nullable=False)
    as_of: Mapped[date] = mapped_column(Date, nullable=False)
    quality_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    intrinsic_value_mid: Mapped[float | None] = mapped_column(Float, nullable=True)
    current_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    valuation_gap: Mapped[float | None] = mapped_column(Float, nullable=True)
    quality_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    valuation_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    ownership_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    ownership_special_situation_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    limited_coverage_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    screen_rank_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    template_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    overall_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    top_reason_codes_json: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
