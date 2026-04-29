"""company valuation schema

Revision ID: 20260429_0036
Revises: 20260422_0035
Create Date: 2026-04-29 18:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20260429_0036"
down_revision = "20260422_0035"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "issuers",
        sa.Column("issuer_id", sa.Text(), nullable=False),
        sa.Column("cik", sa.Text(), nullable=False),
        sa.Column("company_name", sa.Text(), nullable=False),
        sa.Column("sic", sa.Text(), nullable=True),
        sa.Column("sic_description", sa.Text(), nullable=True),
        sa.Column("naics", sa.Text(), nullable=True),
        sa.Column("template_id", sa.Text(), nullable=False),
        sa.Column("template_version", sa.Text(), nullable=False),
        sa.Column("template_assignment_source", sa.Text(), nullable=False),
        sa.Column("template_assignment_reason", sa.Text(), nullable=False),
        sa.Column("limited_coverage_flag", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("issuer_id"),
        sa.UniqueConstraint("cik"),
    )
    op.create_index("idx_issuers_template_id", "issuers", ["template_id"], unique=False)
    op.create_index(
        "idx_issuers_limited_coverage",
        "issuers",
        ["limited_coverage_flag"],
        unique=False,
    )

    op.create_table(
        "securities",
        sa.Column("security_id", sa.Text(), nullable=False),
        sa.Column("issuer_id", sa.Text(), nullable=False),
        sa.Column("ticker", sa.Text(), nullable=False),
        sa.Column("share_class", sa.Text(), nullable=True),
        sa.Column("exchange", sa.Text(), nullable=True),
        sa.Column("cusip", sa.Text(), nullable=True),
        sa.Column("is_primary", sa.Boolean(), nullable=False),
        sa.Column("active_from", sa.Date(), nullable=True),
        sa.Column("active_to", sa.Date(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["issuer_id"], ["issuers.issuer_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("security_id"),
        sa.UniqueConstraint("issuer_id", "ticker", name="ux_securities_issuer_ticker"),
    )
    op.create_index("idx_securities_issuer_primary", "securities", ["issuer_id", "is_primary"], unique=False)
    op.create_index("idx_securities_cusip", "securities", ["cusip"], unique=False)

    op.create_table(
        "filings",
        sa.Column("filing_id", sa.Text(), nullable=False),
        sa.Column("issuer_id", sa.Text(), nullable=False),
        sa.Column("accession_no", sa.Text(), nullable=False),
        sa.Column("form_type", sa.Text(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=True),
        sa.Column("filed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("amendment_flag", sa.Boolean(), nullable=False),
        sa.Column("amendment_of_accession_no", sa.Text(), nullable=True),
        sa.Column("primary_document_url", sa.Text(), nullable=True),
        sa.Column("primary_xml_url", sa.Text(), nullable=True),
        sa.Column("raw_storage_uri", sa.Text(), nullable=True),
        sa.Column("raw_sha256", sa.Text(), nullable=True),
        sa.Column("parse_status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["issuer_id"], ["issuers.issuer_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("filing_id"),
        sa.UniqueConstraint("accession_no"),
    )
    op.create_index(
        "idx_filings_issuer_form_available",
        "filings",
        ["issuer_id", "form_type", "available_at"],
        unique=False,
    )
    op.create_index("idx_filings_accepted_at", "filings", ["accepted_at"], unique=False)
    op.create_index("idx_filings_period_end", "filings", ["period_end"], unique=False)

    op.create_table(
        "xbrl_facts",
        sa.Column("fact_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("filing_id", sa.Text(), nullable=False),
        sa.Column("issuer_id", sa.Text(), nullable=False),
        sa.Column("taxonomy", sa.Text(), nullable=False),
        sa.Column("concept_name", sa.Text(), nullable=False),
        sa.Column("unit", sa.Text(), nullable=False),
        sa.Column("period_start", sa.Date(), nullable=True),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("instant_flag", sa.Boolean(), nullable=False),
        sa.Column("dimensions_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("value_numeric", sa.Float(), nullable=True),
        sa.Column("value_text", sa.Text(), nullable=True),
        sa.Column("decimals", sa.Text(), nullable=True),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("fact_hash", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["filing_id"], ["filings.filing_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["issuer_id"], ["issuers.issuer_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("fact_id"),
        sa.UniqueConstraint("fact_hash", name="ux_xbrl_facts_fact_hash"),
    )
    op.create_index(
        "idx_xbrl_facts_issuer_concept_period",
        "xbrl_facts",
        ["issuer_id", "concept_name", "period_end"],
        unique=False,
    )

    op.create_table(
        "statement_period_snapshots",
        sa.Column("snapshot_id", sa.Text(), nullable=False),
        sa.Column("issuer_id", sa.Text(), nullable=False),
        sa.Column("filing_id", sa.Text(), nullable=False),
        sa.Column("period_type", sa.Text(), nullable=False),
        sa.Column("fiscal_year", sa.Integer(), nullable=True),
        sa.Column("fiscal_period", sa.Text(), nullable=True),
        sa.Column("period_start", sa.Date(), nullable=True),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("normalization_version", sa.Text(), nullable=False),
        sa.Column("metrics_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("source_fact_refs_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(["filing_id"], ["filings.filing_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["issuer_id"], ["issuers.issuer_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("snapshot_id"),
        sa.UniqueConstraint(
            "issuer_id",
            "filing_id",
            "period_type",
            "period_end",
            "normalization_version",
            name="ux_statement_period_snapshots",
        ),
    )
    op.create_index(
        "idx_statement_period_snapshots_issuer_period_end",
        "statement_period_snapshots",
        ["issuer_id", "period_end"],
        unique=False,
    )

    op.create_table(
        "treasury_curve_snapshots",
        sa.Column("curve_snapshot_id", sa.Text(), nullable=False),
        sa.Column("curve_date", sa.Date(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("curve_points_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("curve_snapshot_id"),
        sa.UniqueConstraint("curve_date"),
    )

    op.create_table(
        "market_snapshots",
        sa.Column("market_snapshot_id", sa.Text(), nullable=False),
        sa.Column("security_id", sa.Text(), nullable=False),
        sa.Column("issuer_id", sa.Text(), nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("shares_outstanding_market", sa.Float(), nullable=True),
        sa.Column("market_cap", sa.Float(), nullable=True),
        sa.Column("enterprise_value", sa.Float(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["issuer_id"], ["issuers.issuer_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["security_id"], ["securities.security_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("market_snapshot_id"),
    )
    op.create_index(
        "idx_market_snapshots_issuer_available",
        "market_snapshots",
        ["issuer_id", "available_at"],
        unique=False,
    )
    op.create_index(
        "idx_market_snapshots_security_available",
        "market_snapshots",
        ["security_id", "available_at"],
        unique=False,
    )

    op.create_table(
        "beneficial_owners",
        sa.Column("holder_id", sa.Text(), nullable=False),
        sa.Column("canonical_name", sa.Text(), nullable=False),
        sa.Column("normalized_name", sa.Text(), nullable=False),
        sa.Column("holder_cik", sa.Text(), nullable=True),
        sa.Column("holder_type", sa.Text(), nullable=True),
        sa.Column("parent_holder_id", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["parent_holder_id"],
            ["beneficial_owners.holder_id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("holder_id"),
        sa.UniqueConstraint("normalized_name", "holder_cik", name="ux_beneficial_owners_name_cik"),
    )
    op.create_index(
        "idx_beneficial_owners_parent",
        "beneficial_owners",
        ["parent_holder_id"],
        unique=False,
    )

    op.create_table(
        "beneficial_ownership_filings",
        sa.Column("filing_id", sa.Text(), nullable=False),
        sa.Column("issuer_id", sa.Text(), nullable=False),
        sa.Column("schedule_type", sa.Text(), nullable=False),
        sa.Column("event_date", sa.Date(), nullable=True),
        sa.Column("passive_flag", sa.Boolean(), nullable=False),
        sa.Column("control_intent_flag", sa.Boolean(), nullable=False),
        sa.Column("group_flag", sa.Boolean(), nullable=False),
        sa.Column("amendment_no", sa.Integer(), nullable=True),
        sa.Column("prior_schedule_type", sa.Text(), nullable=True),
        sa.Column("item4_purpose_text", sa.Text(), nullable=True),
        sa.Column("item5_interest_text", sa.Text(), nullable=True),
        sa.Column("item6_derivative_or_arrangement_text", sa.Text(), nullable=True),
        sa.Column("ownership_xml_version", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["filing_id"], ["filings.filing_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["issuer_id"], ["issuers.issuer_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("filing_id"),
    )
    op.create_index(
        "idx_beneficial_ownership_filings_issuer_event_date",
        "beneficial_ownership_filings",
        ["issuer_id", "event_date"],
        unique=False,
    )
    op.create_index(
        "idx_beneficial_ownership_filings_issuer_schedule",
        "beneficial_ownership_filings",
        ["issuer_id", "schedule_type", "filing_id"],
        unique=False,
    )

    op.create_table(
        "beneficial_owner_groups",
        sa.Column("group_id", sa.Text(), nullable=False),
        sa.Column("issuer_id", sa.Text(), nullable=False),
        sa.Column("group_name", sa.Text(), nullable=False),
        sa.Column("group_kind", sa.Text(), nullable=False),
        sa.Column("root_filing_id", sa.Text(), nullable=False),
        sa.Column("effective_from", sa.Date(), nullable=True),
        sa.Column("effective_to", sa.Date(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["issuer_id"], ["issuers.issuer_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["root_filing_id"], ["filings.filing_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("group_id"),
    )
    op.create_index(
        "idx_beneficial_owner_groups_issuer_effective_from",
        "beneficial_owner_groups",
        ["issuer_id", "effective_from"],
        unique=False,
    )

    op.create_table(
        "beneficial_owner_group_memberships",
        sa.Column("group_id", sa.Text(), nullable=False),
        sa.Column("holder_id", sa.Text(), nullable=False),
        sa.Column("filing_id", sa.Text(), nullable=False),
        sa.Column("member_role", sa.Text(), nullable=True),
        sa.Column("effective_from", sa.Date(), nullable=True),
        sa.Column("effective_to", sa.Date(), nullable=True),
        sa.ForeignKeyConstraint(["filing_id"], ["filings.filing_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["group_id"], ["beneficial_owner_groups.group_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["holder_id"], ["beneficial_owners.holder_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("group_id", "holder_id", "filing_id"),
    )
    op.create_index(
        "idx_beneficial_owner_group_memberships_holder",
        "beneficial_owner_group_memberships",
        ["holder_id"],
        unique=False,
    )

    op.create_table(
        "beneficial_owner_positions",
        sa.Column("position_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("issuer_id", sa.Text(), nullable=False),
        sa.Column("holder_id", sa.Text(), nullable=False),
        sa.Column("group_id", sa.Text(), nullable=True),
        sa.Column("filing_id", sa.Text(), nullable=False),
        sa.Column("schedule_type", sa.Text(), nullable=False),
        sa.Column("event_date", sa.Date(), nullable=True),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("share_count_reported", sa.Float(), nullable=True),
        sa.Column("ownership_pct", sa.Float(), nullable=True),
        sa.Column("sole_voting_power", sa.Float(), nullable=True),
        sa.Column("shared_voting_power", sa.Float(), nullable=True),
        sa.Column("sole_dispositive_power", sa.Float(), nullable=True),
        sa.Column("shared_dispositive_power", sa.Float(), nullable=True),
        sa.Column("passive_flag", sa.Boolean(), nullable=False),
        sa.Column("control_intent_flag", sa.Boolean(), nullable=False),
        sa.Column("derivative_exposure_flag", sa.Boolean(), nullable=False),
        sa.Column("source_row_hash", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["filing_id"], ["filings.filing_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["group_id"], ["beneficial_owner_groups.group_id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["holder_id"], ["beneficial_owners.holder_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["issuer_id"], ["issuers.issuer_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("position_id"),
        sa.UniqueConstraint("source_row_hash", name="ux_beneficial_owner_positions_source_row_hash"),
    )
    op.create_index(
        "idx_beneficial_owner_positions_issuer_available",
        "beneficial_owner_positions",
        ["issuer_id", "available_at"],
        unique=False,
    )
    op.create_index(
        "idx_beneficial_owner_positions_holder_available",
        "beneficial_owner_positions",
        ["holder_id", "available_at"],
        unique=False,
    )
    op.create_index(
        "idx_beneficial_owner_positions_issuer_ownership_pct",
        "beneficial_owner_positions",
        ["issuer_id", "ownership_pct"],
        unique=False,
    )

    op.create_table(
        "insider_transactions",
        sa.Column("insider_transaction_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("issuer_id", sa.Text(), nullable=False),
        sa.Column("holder_id", sa.Text(), nullable=True),
        sa.Column("filing_id", sa.Text(), nullable=False),
        sa.Column("transaction_date", sa.Date(), nullable=True),
        sa.Column("transaction_code", sa.Text(), nullable=False),
        sa.Column("security_type", sa.Text(), nullable=True),
        sa.Column("shares_delta", sa.Float(), nullable=True),
        sa.Column("price", sa.Float(), nullable=True),
        sa.Column("shares_owned_after", sa.Float(), nullable=True),
        sa.Column("ownership_nature", sa.Text(), nullable=True),
        sa.Column("footnotes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["filing_id"], ["filings.filing_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["holder_id"], ["beneficial_owners.holder_id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["issuer_id"], ["issuers.issuer_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("insider_transaction_id"),
    )
    op.create_index(
        "idx_insider_transactions_issuer_available",
        "insider_transactions",
        ["issuer_id", "available_at"],
        unique=False,
    )
    op.create_index(
        "idx_insider_transactions_holder_available",
        "insider_transactions",
        ["holder_id", "available_at"],
        unique=False,
    )
    op.create_index(
        "idx_insider_transactions_issuer_transaction_date",
        "insider_transactions",
        ["issuer_id", "transaction_date"],
        unique=False,
    )

    op.create_table(
        "institutional_holders",
        sa.Column("institutional_holder_id", sa.Text(), nullable=False),
        sa.Column("manager_cik", sa.Text(), nullable=True),
        sa.Column("manager_name", sa.Text(), nullable=False),
        sa.Column("normalized_name", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("institutional_holder_id"),
        sa.UniqueConstraint("normalized_name", "manager_cik", name="ux_institutional_holders_name_cik"),
    )

    op.create_table(
        "institutional_positions",
        sa.Column("institutional_position_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("institutional_holder_id", sa.Text(), nullable=False),
        sa.Column("issuer_id", sa.Text(), nullable=False),
        sa.Column("filing_id", sa.Text(), nullable=False),
        sa.Column("report_period", sa.Date(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("cusip", sa.Text(), nullable=True),
        sa.Column("share_count", sa.Float(), nullable=True),
        sa.Column("market_value_reported", sa.Float(), nullable=True),
        sa.Column("discretion_type", sa.Text(), nullable=True),
        sa.Column("other_manager_refs_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("voting_authority_sole", sa.Float(), nullable=True),
        sa.Column("voting_authority_shared", sa.Float(), nullable=True),
        sa.Column("voting_authority_none", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(["filing_id"], ["filings.filing_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["institutional_holder_id"],
            ["institutional_holders.institutional_holder_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["issuer_id"], ["issuers.issuer_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("institutional_position_id"),
    )
    op.create_index(
        "idx_institutional_positions_issuer_report_period",
        "institutional_positions",
        ["issuer_id", "report_period"],
        unique=False,
    )
    op.create_index(
        "idx_institutional_positions_holder_report_period",
        "institutional_positions",
        ["institutional_holder_id", "report_period"],
        unique=False,
    )

    op.create_table(
        "feature_snapshots",
        sa.Column("feature_snapshot_id", sa.Text(), nullable=False),
        sa.Column("issuer_id", sa.Text(), nullable=False),
        sa.Column("template_id", sa.Text(), nullable=False),
        sa.Column("template_version", sa.Text(), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("feature_version", sa.Text(), nullable=False),
        sa.Column("financial_features_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("ownership_features_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("dependency_refs_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["issuer_id"], ["issuers.issuer_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("feature_snapshot_id"),
        sa.UniqueConstraint("issuer_id", "as_of", "feature_version", name="ux_feature_snapshots"),
    )
    op.create_index(
        "idx_feature_snapshots_template_as_of",
        "feature_snapshots",
        ["template_id", "as_of"],
        unique=False,
    )

    op.create_table(
        "company_valuation_snapshots",
        sa.Column("company_valuation_snapshot_id", sa.Text(), nullable=False),
        sa.Column("issuer_id", sa.Text(), nullable=False),
        sa.Column("security_id", sa.Text(), nullable=True),
        sa.Column("template_id", sa.Text(), nullable=False),
        sa.Column("template_version", sa.Text(), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("evaluation_version", sa.Text(), nullable=False),
        sa.Column("quality_score", sa.Float(), nullable=True),
        sa.Column("ownership_score", sa.Float(), nullable=True),
        sa.Column("intrinsic_value_bear", sa.Float(), nullable=True),
        sa.Column("intrinsic_value_base", sa.Float(), nullable=True),
        sa.Column("intrinsic_value_bull", sa.Float(), nullable=True),
        sa.Column("intrinsic_value_mid", sa.Float(), nullable=True),
        sa.Column("current_price", sa.Float(), nullable=True),
        sa.Column("valuation_gap", sa.Float(), nullable=True),
        sa.Column("quality_confidence", sa.Float(), nullable=True),
        sa.Column("valuation_confidence", sa.Float(), nullable=True),
        sa.Column("limited_coverage_flag", sa.Boolean(), nullable=False),
        sa.Column("top_reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("valuation_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["issuer_id"], ["issuers.issuer_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["security_id"], ["securities.security_id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("company_valuation_snapshot_id"),
        sa.UniqueConstraint("issuer_id", "as_of", "evaluation_version", name="ux_company_valuation_snapshots"),
    )
    op.create_index(
        "idx_company_valuation_snapshots_issuer_as_of",
        "company_valuation_snapshots",
        ["issuer_id", "as_of"],
        unique=False,
    )
    op.create_index(
        "idx_company_valuation_snapshots_template_as_of",
        "company_valuation_snapshots",
        ["template_id", "as_of"],
        unique=False,
    )

    op.create_table(
        "screening_rows",
        sa.Column("screening_row_id", sa.Text(), nullable=False),
        sa.Column("issuer_id", sa.Text(), nullable=False),
        sa.Column("security_id", sa.Text(), nullable=True),
        sa.Column("ticker", sa.Text(), nullable=False),
        sa.Column("template_id", sa.Text(), nullable=False),
        sa.Column("as_of", sa.Date(), nullable=False),
        sa.Column("quality_score", sa.Float(), nullable=True),
        sa.Column("intrinsic_value_mid", sa.Float(), nullable=True),
        sa.Column("current_price", sa.Float(), nullable=True),
        sa.Column("valuation_gap", sa.Float(), nullable=True),
        sa.Column("quality_confidence", sa.Float(), nullable=True),
        sa.Column("valuation_confidence", sa.Float(), nullable=True),
        sa.Column("ownership_score", sa.Float(), nullable=True),
        sa.Column("ownership_special_situation_flag", sa.Boolean(), nullable=False),
        sa.Column("limited_coverage_flag", sa.Boolean(), nullable=False),
        sa.Column("top_reason_codes_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["issuer_id"], ["issuers.issuer_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["security_id"], ["securities.security_id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("screening_row_id"),
        sa.UniqueConstraint("issuer_id", "as_of", name="ux_screening_rows_issuer_as_of"),
    )
    op.create_index(
        "idx_screening_rows_template_as_of_quality",
        "screening_rows",
        ["template_id", "as_of", "quality_score"],
        unique=False,
    )
    op.create_index(
        "idx_screening_rows_as_of_valuation_gap",
        "screening_rows",
        ["as_of", "valuation_gap"],
        unique=False,
    )
    op.create_index(
        "idx_screening_rows_as_of_special_situation",
        "screening_rows",
        ["as_of", "ownership_special_situation_flag"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_screening_rows_as_of_special_situation", table_name="screening_rows")
    op.drop_index("idx_screening_rows_as_of_valuation_gap", table_name="screening_rows")
    op.drop_index("idx_screening_rows_template_as_of_quality", table_name="screening_rows")
    op.drop_table("screening_rows")

    op.drop_index("idx_company_valuation_snapshots_template_as_of", table_name="company_valuation_snapshots")
    op.drop_index("idx_company_valuation_snapshots_issuer_as_of", table_name="company_valuation_snapshots")
    op.drop_table("company_valuation_snapshots")

    op.drop_index("idx_feature_snapshots_template_as_of", table_name="feature_snapshots")
    op.drop_table("feature_snapshots")

    op.drop_index("idx_institutional_positions_holder_report_period", table_name="institutional_positions")
    op.drop_index("idx_institutional_positions_issuer_report_period", table_name="institutional_positions")
    op.drop_table("institutional_positions")

    op.drop_table("institutional_holders")

    op.drop_index("idx_insider_transactions_issuer_transaction_date", table_name="insider_transactions")
    op.drop_index("idx_insider_transactions_holder_available", table_name="insider_transactions")
    op.drop_index("idx_insider_transactions_issuer_available", table_name="insider_transactions")
    op.drop_table("insider_transactions")

    op.drop_index("idx_beneficial_owner_positions_issuer_ownership_pct", table_name="beneficial_owner_positions")
    op.drop_index("idx_beneficial_owner_positions_holder_available", table_name="beneficial_owner_positions")
    op.drop_index("idx_beneficial_owner_positions_issuer_available", table_name="beneficial_owner_positions")
    op.drop_table("beneficial_owner_positions")

    op.drop_index("idx_beneficial_owner_group_memberships_holder", table_name="beneficial_owner_group_memberships")
    op.drop_table("beneficial_owner_group_memberships")

    op.drop_index("idx_beneficial_owner_groups_issuer_effective_from", table_name="beneficial_owner_groups")
    op.drop_table("beneficial_owner_groups")

    op.drop_index("idx_beneficial_ownership_filings_issuer_schedule", table_name="beneficial_ownership_filings")
    op.drop_index("idx_beneficial_ownership_filings_issuer_event_date", table_name="beneficial_ownership_filings")
    op.drop_table("beneficial_ownership_filings")

    op.drop_index("idx_beneficial_owners_parent", table_name="beneficial_owners")
    op.drop_table("beneficial_owners")

    op.drop_index("idx_market_snapshots_security_available", table_name="market_snapshots")
    op.drop_index("idx_market_snapshots_issuer_available", table_name="market_snapshots")
    op.drop_table("market_snapshots")

    op.drop_table("treasury_curve_snapshots")

    op.drop_index("idx_statement_period_snapshots_issuer_period_end", table_name="statement_period_snapshots")
    op.drop_table("statement_period_snapshots")

    op.drop_index("idx_xbrl_facts_issuer_concept_period", table_name="xbrl_facts")
    op.drop_table("xbrl_facts")

    op.drop_index("idx_filings_period_end", table_name="filings")
    op.drop_index("idx_filings_accepted_at", table_name="filings")
    op.drop_index("idx_filings_issuer_form_available", table_name="filings")
    op.drop_table("filings")

    op.drop_index("idx_securities_cusip", table_name="securities")
    op.drop_index("idx_securities_issuer_primary", table_name="securities")
    op.drop_table("securities")

    op.drop_index("idx_issuers_limited_coverage", table_name="issuers")
    op.drop_index("idx_issuers_template_id", table_name="issuers")
    op.drop_table("issuers")
