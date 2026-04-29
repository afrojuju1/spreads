"""company valuation 13f schema

Revision ID: 20260429_0037
Revises: 20260429_0036
Create Date: 2026-04-29 17:20:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20260429_0037"
down_revision = "20260429_0036"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "security_identifier_history",
        sa.Column("security_identifier_id", sa.Text(), nullable=False),
        sa.Column("security_id", sa.Text(), nullable=False),
        sa.Column("issuer_id", sa.Text(), nullable=False),
        sa.Column("identifier_type", sa.Text(), nullable=False),
        sa.Column("identifier_value", sa.Text(), nullable=False),
        sa.Column("issuer_name_reported", sa.Text(), nullable=True),
        sa.Column("title_of_class", sa.Text(), nullable=True),
        sa.Column("effective_from", sa.Date(), nullable=True),
        sa.Column("effective_to", sa.Date(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_ref", sa.Text(), nullable=True),
        sa.Column("match_confidence", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["issuer_id"], ["issuers.issuer_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["security_id"], ["securities.security_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("security_identifier_id"),
        sa.UniqueConstraint(
            "identifier_type",
            "identifier_value",
            "effective_from",
            "security_id",
            name="ux_security_identifier_history",
        ),
    )
    op.create_index(
        "idx_security_identifier_history_lookup",
        "security_identifier_history",
        ["identifier_type", "identifier_value", "effective_to"],
        unique=False,
    )
    op.create_index(
        "idx_security_identifier_history_security",
        "security_identifier_history",
        ["security_id", "effective_from"],
        unique=False,
    )

    op.create_table(
        "institutional_filings",
        sa.Column("filing_id", sa.Text(), nullable=False),
        sa.Column("institutional_holder_id", sa.Text(), nullable=False),
        sa.Column("accession_no", sa.Text(), nullable=False),
        sa.Column("manager_cik", sa.Text(), nullable=True),
        sa.Column("manager_name", sa.Text(), nullable=False),
        sa.Column("submission_type", sa.Text(), nullable=False),
        sa.Column("report_period", sa.Date(), nullable=False),
        sa.Column("filed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("amendment_flag", sa.Boolean(), nullable=False),
        sa.Column("amendment_no", sa.Integer(), nullable=True),
        sa.Column("report_type", sa.Text(), nullable=True),
        sa.Column("form13f_file_number", sa.Text(), nullable=True),
        sa.Column("crd_number", sa.Text(), nullable=True),
        sa.Column("sec_file_number", sa.Text(), nullable=True),
        sa.Column("other_included_managers_count", sa.Integer(), nullable=True),
        sa.Column("table_entry_total", sa.Integer(), nullable=True),
        sa.Column("table_value_total", sa.Float(), nullable=True),
        sa.Column("is_confidential_omitted", sa.Boolean(), nullable=True),
        sa.Column("additional_information", sa.Text(), nullable=True),
        sa.Column("primary_document_url", sa.Text(), nullable=True),
        sa.Column("information_table_url", sa.Text(), nullable=True),
        sa.Column("source_dataset_url", sa.Text(), nullable=True),
        sa.Column("parse_status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["institutional_holder_id"],
            ["institutional_holders.institutional_holder_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("filing_id"),
        sa.UniqueConstraint("accession_no"),
    )
    op.create_index(
        "idx_institutional_filings_holder_period",
        "institutional_filings",
        ["institutional_holder_id", "report_period"],
        unique=False,
    )
    op.create_index(
        "idx_institutional_filings_manager_period",
        "institutional_filings",
        ["manager_cik", "report_period"],
        unique=False,
    )

    op.drop_index(
        "idx_institutional_positions_holder_report_period",
        table_name="institutional_positions",
    )
    op.drop_index(
        "idx_institutional_positions_issuer_report_period",
        table_name="institutional_positions",
    )
    op.drop_table("institutional_positions")

    op.create_table(
        "institutional_positions",
        sa.Column("institutional_position_id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("institutional_holder_id", sa.Text(), nullable=False),
        sa.Column("issuer_id", sa.Text(), nullable=False),
        sa.Column("filing_id", sa.Text(), nullable=False),
        sa.Column("report_period", sa.Date(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("issuer_name_reported", sa.Text(), nullable=True),
        sa.Column("title_of_class", sa.Text(), nullable=True),
        sa.Column("cusip", sa.Text(), nullable=True),
        sa.Column("figi", sa.Text(), nullable=True),
        sa.Column("share_count", sa.Float(), nullable=True),
        sa.Column("market_value_reported", sa.Float(), nullable=True),
        sa.Column("put_call", sa.Text(), nullable=True),
        sa.Column("discretion_type", sa.Text(), nullable=True),
        sa.Column("other_manager_refs_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("voting_authority_sole", sa.Float(), nullable=True),
        sa.Column("voting_authority_shared", sa.Float(), nullable=True),
        sa.Column("voting_authority_none", sa.Float(), nullable=True),
        sa.Column("resolution_source", sa.Text(), nullable=True),
        sa.Column("resolution_confidence", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(["filing_id"], ["institutional_filings.filing_id"], ondelete="CASCADE"),
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


def downgrade() -> None:
    op.drop_index(
        "idx_institutional_positions_holder_report_period",
        table_name="institutional_positions",
    )
    op.drop_index(
        "idx_institutional_positions_issuer_report_period",
        table_name="institutional_positions",
    )
    op.drop_table("institutional_positions")

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

    op.drop_index(
        "idx_institutional_filings_manager_period",
        table_name="institutional_filings",
    )
    op.drop_index(
        "idx_institutional_filings_holder_period",
        table_name="institutional_filings",
    )
    op.drop_table("institutional_filings")

    op.drop_index(
        "idx_security_identifier_history_security",
        table_name="security_identifier_history",
    )
    op.drop_index(
        "idx_security_identifier_history_lookup",
        table_name="security_identifier_history",
    )
    op.drop_table("security_identifier_history")
