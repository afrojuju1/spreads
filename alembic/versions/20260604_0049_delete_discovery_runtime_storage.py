"""delete legacy discovery runtime storage

Revision ID: 20260604_0049
Revises: 20260604_0048
Create Date: 2026-06-04 03:40:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260604_0049"
down_revision = "20260604_0048"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("""
            ALTER TABLE execution_attempts DROP CONSTRAINT IF EXISTS execution_attempts_cycle_id_fkey;
            ALTER TABLE execution_attempts DROP CONSTRAINT IF EXISTS execution_attempts_candidate_id_fkey;
            ALTER TABLE risk_decisions DROP CONSTRAINT IF EXISTS risk_decisions_cycle_id_fkey;
            ALTER TABLE risk_decisions DROP CONSTRAINT IF EXISTS risk_decisions_candidate_id_fkey;
            """))
    op.execute(sa.text("""
            DELETE FROM job_leases
            WHERE lease_key LIKE 'singleton:discovery_run:%'
               OR lease_key LIKE 'singleton:discovery_recovery:%'
               OR lease_state_json ->> 'lane' = 'discovery'
               OR lease_state_json ->> 'settings_name' = 'DiscoveryWorkerSettings'
               OR lease_state_json ->> 'queue_name' = 'arq:queue:discovery'
               OR job_run_id IN (
                    SELECT job_run_id
                    FROM job_runs
                    WHERE job_type IN ('discovery_run', 'discovery_recovery')
                       OR job_key LIKE 'discovery_run:%'
                       OR job_key LIKE 'discovery_recovery:%'
               );

            DELETE FROM job_runs
            WHERE job_type IN ('discovery_run', 'discovery_recovery')
               OR job_key LIKE 'discovery_run:%'
               OR job_key LIKE 'discovery_recovery:%';
            """))
    op.execute(sa.text("""
            DROP TABLE IF EXISTS live_session_slots CASCADE;
            DROP TABLE IF EXISTS discovery_run_events CASCADE;
            DROP TABLE IF EXISTS discovery_run_candidates CASCADE;
            DROP TABLE IF EXISTS pipeline_cycles CASCADE;
            DROP TABLE IF EXISTS pipelines CASCADE;
            DROP TABLE IF EXISTS discovery_runs CASCADE;
            """))


def downgrade() -> None:
    pass
