"""rename collector surfaces to discovery runs

Revision ID: 20260421_0033
Revises: 20260421_0032
Create Date: 2026-04-21 16:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260421_0033"
down_revision = "20260421_0032"
branch_labels = None
depends_on = None


def _has_table(name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return name in inspector.get_table_names()


def _rename_table_if_needed(old: str, new: str) -> None:
    if _has_table(old) and not _has_table(new):
        op.rename_table(old, new)


def _rename_index_if_exists(old: str, new: str) -> None:
    op.execute(sa.text(f"ALTER INDEX IF EXISTS {old} RENAME TO {new}"))


def _rename_sequence_if_exists(old: str, new: str) -> None:
    op.execute(sa.text(f"ALTER SEQUENCE IF EXISTS {old} RENAME TO {new}"))


def _rename_constraint_if_exists(table: str, old: str, new: str) -> None:
    op.execute(
        sa.text(
            f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM pg_constraint AS constraint_row
                    JOIN pg_class AS table_row
                      ON table_row.oid = constraint_row.conrelid
                    WHERE table_row.relname = '{table}'
                      AND constraint_row.conname = '{old}'
                ) THEN
                    EXECUTE 'ALTER TABLE {table} RENAME CONSTRAINT {old} TO {new}';
                END IF;
            END
            $$;
            """
        )
    )


def _job_key_sql(column_sql: str, *, reverse: bool) -> str:
    replacements = (
        (
            "post_close_analysis:discovery_runs",
            "post_close_analysis:live_collectors",
        ),
        (
            "post_market_analysis:discovery_runs",
            "post_market_analysis:live_collectors",
        ),
        ("discovery_run:", "live_collector:"),
        ("discovery_recovery:", "collector_recovery:"),
    )
    if not reverse:
        replacements = tuple((new, old) for old, new in replacements)
    expression = column_sql
    for old, new in replacements:
        expression = f"replace({expression}, '{old}', '{new}')"
    return expression


def _job_type_sql(column_sql: str, *, reverse: bool) -> str:
    from_type = "discovery_run" if reverse else "live_collector"
    to_type = "live_collector" if reverse else "discovery_run"
    from_recovery = "discovery_recovery" if reverse else "collector_recovery"
    to_recovery = "collector_recovery" if reverse else "discovery_recovery"
    return (
        f"CASE {column_sql} "
        f"WHEN '{from_type}' THEN '{to_type}' "
        f"WHEN '{from_recovery}' THEN '{to_recovery}' "
        f"ELSE {column_sql} END"
    )


def _lease_key_sql(column_sql: str, *, reverse: bool) -> str:
    replacements = (
        ("singleton:discovery_run:", "singleton:live_collector:"),
        ("singleton:discovery_recovery:", "singleton:collector_recovery:"),
    )
    if not reverse:
        replacements = tuple((new, old) for old, new in replacements)
    expression = column_sql
    for old, new in replacements:
        expression = f"replace({expression}, '{old}', '{new}')"
    return expression


def _trigger_type_sql(column_sql: str, *, reverse: bool) -> str:
    from_type = "discovery_run_cycle" if reverse else "collector_cycle"
    to_type = "collector_cycle" if reverse else "discovery_run_cycle"
    return f"CASE {column_sql} WHEN '{from_type}' THEN '{to_type}' ELSE {column_sql} END"


def _control_reason_code_sql(column_sql: str, *, reverse: bool) -> str:
    from_active = "discovery_run_gap_active" if reverse else "collector_gap_active"
    to_active = "collector_gap_active" if reverse else "discovery_run_gap_active"
    from_cleared = "discovery_run_gap_cleared" if reverse else "collector_gap_cleared"
    to_cleared = "collector_gap_cleared" if reverse else "discovery_run_gap_cleared"
    return (
        f"CASE {column_sql} "
        f"WHEN '{from_active}' THEN '{to_active}' "
        f"WHEN '{from_cleared}' THEN '{to_cleared}' "
        f"ELSE {column_sql} END"
    )


def _rewrite_text_column(
    table: str,
    column: str,
    *,
    sql_expression: str,
) -> None:
    op.execute(
        sa.text(
            f"""
            UPDATE {table}
            SET {column} = {sql_expression}
            WHERE {column} IS NOT NULL
              AND {column} <> {sql_expression}
            """
        )
    )


def _rewrite_jsonb_text_field(
    table: str,
    column: str,
    field: str,
    *,
    sql_expression: str,
) -> None:
    op.execute(
        sa.text(
            f"""
            UPDATE {table}
            SET {column} = jsonb_set(
                {column},
                '{{{field}}}',
                to_jsonb(({sql_expression})::text),
                true
            )
            WHERE {column} ? '{field}'
              AND ({column} ->> '{field}') <> {sql_expression}
            """
        )
    )


def _rename_tables_to_discovery_runs() -> None:
    _rename_table_if_needed("collector_cycles", "discovery_runs")
    _rename_table_if_needed(
        "collector_cycle_candidates", "discovery_run_candidates"
    )
    _rename_table_if_needed("collector_cycle_events", "discovery_run_events")

    _rename_index_if_exists(
        "idx_collector_cycles_label_session_generated_at",
        "idx_discovery_runs_label_session_generated_at",
    )
    _rename_index_if_exists(
        "idx_collector_cycles_session_id_generated_at",
        "idx_discovery_runs_session_id_generated_at",
    )
    _rename_index_if_exists(
        "idx_collector_cycle_candidates_cycle_bucket_position",
        "idx_discovery_run_candidates_cycle_bucket_position",
    )
    _rename_index_if_exists(
        "idx_collector_cycle_candidates_run_id",
        "idx_discovery_run_candidates_run_id",
    )
    _rename_index_if_exists(
        "idx_collector_cycle_candidates_identity",
        "idx_discovery_run_candidates_identity",
    )
    _rename_index_if_exists(
        "idx_collector_cycle_events_label_session_generated_at",
        "idx_discovery_run_events_label_session_generated_at",
    )

    _rename_sequence_if_exists(
        "collector_cycle_candidates_candidate_id_seq",
        "discovery_run_candidates_candidate_id_seq",
    )
    _rename_sequence_if_exists(
        "collector_cycle_events_event_id_seq",
        "discovery_run_events_event_id_seq",
    )

    _rename_constraint_if_exists(
        "discovery_runs",
        "collector_cycles_pkey",
        "discovery_runs_pkey",
    )
    _rename_constraint_if_exists(
        "discovery_runs",
        "fk_collector_cycles_job_run_id_job_runs",
        "fk_discovery_runs_job_run_id_job_runs",
    )
    _rename_constraint_if_exists(
        "discovery_run_candidates",
        "collector_cycle_candidates_pkey",
        "discovery_run_candidates_pkey",
    )
    _rename_constraint_if_exists(
        "discovery_run_candidates",
        "collector_cycle_candidates_cycle_id_fkey",
        "discovery_run_candidates_cycle_id_fkey",
    )
    _rename_constraint_if_exists(
        "discovery_run_events",
        "collector_cycle_events_pkey",
        "discovery_run_events_pkey",
    )
    _rename_constraint_if_exists(
        "discovery_run_events",
        "collector_cycle_events_cycle_id_fkey",
        "discovery_run_events_cycle_id_fkey",
    )


def _rename_tables_to_collectors() -> None:
    _rename_table_if_needed("discovery_run_events", "collector_cycle_events")
    _rename_table_if_needed(
        "discovery_run_candidates", "collector_cycle_candidates"
    )
    _rename_table_if_needed("discovery_runs", "collector_cycles")

    _rename_index_if_exists(
        "idx_discovery_runs_label_session_generated_at",
        "idx_collector_cycles_label_session_generated_at",
    )
    _rename_index_if_exists(
        "idx_discovery_runs_session_id_generated_at",
        "idx_collector_cycles_session_id_generated_at",
    )
    _rename_index_if_exists(
        "idx_discovery_run_candidates_cycle_bucket_position",
        "idx_collector_cycle_candidates_cycle_bucket_position",
    )
    _rename_index_if_exists(
        "idx_discovery_run_candidates_run_id",
        "idx_collector_cycle_candidates_run_id",
    )
    _rename_index_if_exists(
        "idx_discovery_run_candidates_identity",
        "idx_collector_cycle_candidates_identity",
    )
    _rename_index_if_exists(
        "idx_discovery_run_events_label_session_generated_at",
        "idx_collector_cycle_events_label_session_generated_at",
    )

    _rename_sequence_if_exists(
        "discovery_run_candidates_candidate_id_seq",
        "collector_cycle_candidates_candidate_id_seq",
    )
    _rename_sequence_if_exists(
        "discovery_run_events_event_id_seq",
        "collector_cycle_events_event_id_seq",
    )

    _rename_constraint_if_exists(
        "collector_cycles",
        "discovery_runs_pkey",
        "collector_cycles_pkey",
    )
    _rename_constraint_if_exists(
        "collector_cycles",
        "fk_discovery_runs_job_run_id_job_runs",
        "fk_collector_cycles_job_run_id_job_runs",
    )
    _rename_constraint_if_exists(
        "collector_cycle_candidates",
        "discovery_run_candidates_pkey",
        "collector_cycle_candidates_pkey",
    )
    _rename_constraint_if_exists(
        "collector_cycle_candidates",
        "discovery_run_candidates_cycle_id_fkey",
        "collector_cycle_candidates_cycle_id_fkey",
    )
    _rename_constraint_if_exists(
        "collector_cycle_events",
        "discovery_run_events_pkey",
        "collector_cycle_events_pkey",
    )
    _rename_constraint_if_exists(
        "collector_cycle_events",
        "discovery_run_events_cycle_id_fkey",
        "collector_cycle_events_cycle_id_fkey",
    )


def _rewrite_runtime_nouns(*, reverse: bool) -> None:
    _rewrite_text_column(
        "job_runs",
        "job_type",
        sql_expression=_job_type_sql("job_type", reverse=reverse),
    )
    _rewrite_text_column(
        "job_runs",
        "job_key",
        sql_expression=_job_key_sql("job_key", reverse=reverse),
    )
    _rewrite_jsonb_text_field(
        "job_runs",
        "payload_json",
        "job_type",
        sql_expression=_job_type_sql("payload_json ->> 'job_type'", reverse=reverse),
    )
    _rewrite_jsonb_text_field(
        "job_runs",
        "payload_json",
        "job_key",
        sql_expression=_job_key_sql("payload_json ->> 'job_key'", reverse=reverse),
    )

    _rewrite_text_column(
        "live_session_slots",
        "job_key",
        sql_expression=_job_key_sql("job_key", reverse=reverse),
    )

    _rewrite_text_column(
        "pipelines",
        "source_job_key",
        sql_expression=_job_key_sql("source_job_key", reverse=reverse),
    )

    _rewrite_text_column(
        "portfolio_positions",
        "source_job_type",
        sql_expression=_job_type_sql("source_job_type", reverse=reverse),
    )
    _rewrite_text_column(
        "portfolio_positions",
        "source_job_key",
        sql_expression=_job_key_sql("source_job_key", reverse=reverse),
    )

    _rewrite_text_column(
        "job_leases",
        "lease_key",
        sql_expression=_lease_key_sql("lease_key", reverse=reverse),
    )
    _rewrite_jsonb_text_field(
        "job_leases",
        "lease_state_json",
        "job_key",
        sql_expression=_job_key_sql("lease_state_json ->> 'job_key'", reverse=reverse),
    )

    degraded_topic_from = (
        "live.discovery_run.degraded" if reverse else "live.collector.degraded"
    )
    degraded_topic_to = (
        "live.collector.degraded" if reverse else "live.discovery_run.degraded"
    )
    op.execute(
        sa.text(
            """
            UPDATE event_log
            SET topic = :topic_to
            WHERE topic = :topic_from
            """
        ).bindparams(topic_from=degraded_topic_from, topic_to=degraded_topic_to)
    )
    _rewrite_jsonb_text_field(
        "event_log",
        "payload_json",
        "job_type",
        sql_expression=_job_type_sql("payload_json ->> 'job_type'", reverse=reverse),
    )
    _rewrite_jsonb_text_field(
        "event_log",
        "payload_json",
        "job_key",
        sql_expression=_job_key_sql("payload_json ->> 'job_key'", reverse=reverse),
    )
    _rewrite_jsonb_text_field(
        "event_log",
        "payload_json",
        "source_job_type",
        sql_expression=_job_type_sql(
            "payload_json ->> 'source_job_type'",
            reverse=reverse,
        ),
    )
    _rewrite_jsonb_text_field(
        "event_log",
        "payload_json",
        "source_job_key",
        sql_expression=_job_key_sql(
            "payload_json ->> 'source_job_key'",
            reverse=reverse,
        ),
    )
    _rewrite_jsonb_text_field(
        "event_log",
        "payload_json",
        "reason_code",
        sql_expression=_control_reason_code_sql(
            "payload_json ->> 'reason_code'",
            reverse=reverse,
        ),
    )

    _rewrite_text_column(
        "control_state",
        "reason_code",
        sql_expression=_control_reason_code_sql("reason_code", reverse=reverse),
    )
    _rewrite_jsonb_text_field(
        "operator_actions",
        "requested_payload_json",
        "reason_code",
        sql_expression=_control_reason_code_sql(
            "requested_payload_json ->> 'reason_code'",
            reverse=reverse,
        ),
    )
    _rewrite_jsonb_text_field(
        "operator_actions",
        "resulting_state_json",
        "reason_code",
        sql_expression=_control_reason_code_sql(
            "resulting_state_json ->> 'reason_code'",
            reverse=reverse,
        ),
    )

    _rewrite_text_column(
        "automation_runs",
        "trigger_type",
        sql_expression=_trigger_type_sql("trigger_type", reverse=reverse),
    )


def upgrade() -> None:
    _rename_tables_to_discovery_runs()
    _rewrite_runtime_nouns(reverse=False)


def downgrade() -> None:
    _rewrite_runtime_nouns(reverse=True)
    _rename_tables_to_collectors()
