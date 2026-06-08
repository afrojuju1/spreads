"""normalize candidate run summary keys

Revision ID: 20260608_0057
Revises: 20260608_0056
Create Date: 2026-06-08 01:10:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260608_0057"
down_revision = "20260608_0056"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("""
            UPDATE candidate_runs
            SET summary_json = (
                CASE
                    WHEN summary_json ? 'scanner_strategy' AND NOT summary_json ? 'candidate_builder'
                    THEN jsonb_set(summary_json, '{candidate_builder}', summary_json -> 'scanner_strategy', true)
                    ELSE summary_json
                END
            )
            WHERE summary_json ? 'scanner_strategy'
            """))
    op.execute(sa.text("""
            UPDATE candidate_runs
            SET summary_json = (
                CASE
                    WHEN summary_json ? 'scanner_profile' AND NOT summary_json ? 'build_profile'
                    THEN jsonb_set(summary_json, '{build_profile}', summary_json -> 'scanner_profile', true)
                    ELSE summary_json
                END
            )
            WHERE summary_json ? 'scanner_profile'
            """))
    op.execute(sa.text("""
            UPDATE candidate_runs
            SET summary_json = summary_json - 'scanner_strategy' - 'scanner_profile'
            WHERE summary_json ? 'scanner_strategy' OR summary_json ? 'scanner_profile'
            """))


def downgrade() -> None:
    pass
