"""company valuation stressed operator overlay flag

Revision ID: 20260430_0040
Revises: 20260430_0039
Create Date: 2026-04-30 18:35:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260430_0040"
down_revision = "20260430_0039"
branch_labels = None
depends_on = None


_REASON_SUFFIX = ";stressed_operator_overlay"


def upgrade() -> None:
    op.add_column(
        "issuers",
        sa.Column(
            "stressed_operator_flag",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "company_valuation_snapshots",
        sa.Column(
            "stressed_operator_flag",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "screening_rows",
        sa.Column(
            "stressed_operator_flag",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )

    op.execute(
        f"""
        UPDATE issuers
        SET stressed_operator_flag = TRUE,
            template_id = 'energy_asset_heavy',
            updated_at = CURRENT_TIMESTAMP,
            template_assignment_reason = CASE
                WHEN template_assignment_reason LIKE '%{_REASON_SUFFIX}'
                    THEN template_assignment_reason
                ELSE COALESCE(template_assignment_reason, '') || '{_REASON_SUFFIX}'
            END
        WHERE template_id = 'stressed_operator'
        """
    )
    op.execute(
        """
        UPDATE company_valuation_snapshots
        SET stressed_operator_flag = TRUE
        WHERE template_id = 'stressed_operator'
        """
    )
    op.execute(
        """
        UPDATE screening_rows
        SET stressed_operator_flag = TRUE
        WHERE template_id = 'stressed_operator'
        """
    )

    op.alter_column("issuers", "stressed_operator_flag", server_default=None)
    op.alter_column(
        "company_valuation_snapshots",
        "stressed_operator_flag",
        server_default=None,
    )
    op.alter_column("screening_rows", "stressed_operator_flag", server_default=None)


def downgrade() -> None:
    op.execute(
        """
        UPDATE screening_rows
        SET template_id = 'stressed_operator'
        WHERE stressed_operator_flag IS TRUE
          AND template_id = 'energy_asset_heavy'
        """
    )
    op.execute(
        """
        UPDATE company_valuation_snapshots
        SET template_id = 'stressed_operator'
        WHERE stressed_operator_flag IS TRUE
          AND template_id = 'energy_asset_heavy'
        """
    )
    op.execute(
        f"""
        UPDATE issuers
        SET template_id = 'stressed_operator',
            template_assignment_reason = REPLACE(
                COALESCE(template_assignment_reason, ''),
                '{_REASON_SUFFIX}',
                ''
            )
        WHERE stressed_operator_flag IS TRUE
          AND template_id = 'energy_asset_heavy'
        """
    )
    op.drop_column("screening_rows", "stressed_operator_flag")
    op.drop_column("company_valuation_snapshots", "stressed_operator_flag")
    op.drop_column("issuers", "stressed_operator_flag")
