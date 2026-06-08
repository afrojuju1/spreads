"""normalize candidate evidence keys

Revision ID: 20260608_0056
Revises: 20260608_0055
Create Date: 2026-06-08 01:05:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260608_0056"
down_revision = "20260608_0055"
branch_labels = None
depends_on = None


def _normalize_evidence_keys(table_name: str) -> None:
    op.execute(sa.text(f"""
            UPDATE {table_name}
            SET evidence_json = (
                CASE
                    WHEN evidence_json ? 'scanner_strategy' AND NOT evidence_json ? 'candidate_builder'
                    THEN jsonb_set(evidence_json, '{{candidate_builder}}', evidence_json -> 'scanner_strategy', true)
                    ELSE evidence_json
                END
            )
            WHERE evidence_json ? 'scanner_strategy'
            """))
    op.execute(sa.text(f"""
            UPDATE {table_name}
            SET evidence_json = (
                CASE
                    WHEN evidence_json ? 'scanner_profile' AND NOT evidence_json ? 'build_profile'
                    THEN jsonb_set(evidence_json, '{{build_profile}}', evidence_json -> 'scanner_profile', true)
                    ELSE evidence_json
                END
            )
            WHERE evidence_json ? 'scanner_profile'
            """))
    op.execute(sa.text(f"""
            UPDATE {table_name}
            SET evidence_json = evidence_json - 'scanner_strategy' - 'scanner_profile'
            WHERE evidence_json ? 'scanner_strategy' OR evidence_json ? 'scanner_profile'
            """))


def upgrade() -> None:
    for table_name in (
        "candidate_runs",
        "candidate_symbol_diagnostics",
        "trade_candidates",
        "trade_signals",
        "trade_decisions",
    ):
        _normalize_evidence_keys(table_name)


def downgrade() -> None:
    pass
