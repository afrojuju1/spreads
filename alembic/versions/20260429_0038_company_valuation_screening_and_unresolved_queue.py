"""company valuation screening and unresolved queue

Revision ID: 20260429_0038
Revises: 20260429_0037
Create Date: 2026-04-29 18:05:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260429_0038"
down_revision = "20260429_0037"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "institutional_positions",
        sa.Column("source_row_hash", sa.Text(), nullable=True),
    )
    op.execute(
        """
        UPDATE institutional_positions
        SET source_row_hash = 'legacy13f:' || institutional_position_id::text
        WHERE source_row_hash IS NULL
        """
    )
    op.alter_column("institutional_positions", "source_row_hash", nullable=False)
    op.create_unique_constraint(
        "ux_institutional_positions_source_row_hash",
        "institutional_positions",
        ["source_row_hash"],
    )

    op.create_table(
        "unresolved_institutional_positions",
        sa.Column("source_row_hash", sa.Text(), nullable=False),
        sa.Column("institutional_holder_id", sa.Text(), nullable=False),
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
        sa.Column("official_list_issuer_name", sa.Text(), nullable=True),
        sa.Column("official_list_title_of_class", sa.Text(), nullable=True),
        sa.Column("resolution_status", sa.Text(), nullable=False),
        sa.Column("resolution_source", sa.Text(), nullable=True),
        sa.Column("resolution_confidence", sa.Float(), nullable=True),
        sa.Column("resolution_attempt_count", sa.Integer(), nullable=False),
        sa.Column("last_attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["filing_id"],
            ["institutional_filings.filing_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["institutional_holder_id"],
            ["institutional_holders.institutional_holder_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("source_row_hash"),
    )
    op.create_index(
        "idx_unresolved_institutional_positions_status_retry",
        "unresolved_institutional_positions",
        ["resolution_status", "next_retry_at"],
        unique=False,
    )
    op.create_index(
        "idx_unresolved_institutional_positions_cusip_status",
        "unresolved_institutional_positions",
        ["cusip", "resolution_status"],
        unique=False,
    )
    op.create_index(
        "idx_unresolved_institutional_positions_report_period",
        "unresolved_institutional_positions",
        ["report_period", "resolution_status"],
        unique=False,
    )

    op.add_column(
        "screening_rows",
        sa.Column("screen_rank_score", sa.Float(), nullable=True),
    )
    op.add_column(
        "screening_rows",
        sa.Column("template_rank", sa.Integer(), nullable=True),
    )
    op.add_column(
        "screening_rows",
        sa.Column("overall_rank", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("screening_rows", "overall_rank")
    op.drop_column("screening_rows", "template_rank")
    op.drop_column("screening_rows", "screen_rank_score")

    op.drop_index(
        "idx_unresolved_institutional_positions_report_period",
        table_name="unresolved_institutional_positions",
    )
    op.drop_index(
        "idx_unresolved_institutional_positions_cusip_status",
        table_name="unresolved_institutional_positions",
    )
    op.drop_index(
        "idx_unresolved_institutional_positions_status_retry",
        table_name="unresolved_institutional_positions",
    )
    op.drop_table("unresolved_institutional_positions")

    op.drop_constraint(
        "ux_institutional_positions_source_row_hash",
        "institutional_positions",
        type_="unique",
    )
    op.drop_column("institutional_positions", "source_row_hash")
