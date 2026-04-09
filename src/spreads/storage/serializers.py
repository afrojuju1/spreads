from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from spreads.storage.alert_models import AlertEventModel, AlertStateModel
from spreads.storage.broker_models import AccountSnapshotModel, BrokerSyncStateModel
from spreads.storage.collector_models import (
    CollectorCycleCandidateModel,
    CollectorCycleEventModel,
    CollectorCycleModel,
)
from spreads.storage.execution_models import (
    ExecutionAttemptModel,
    ExecutionFillModel,
    ExecutionOrderModel,
    SessionPositionCloseModel,
    SessionPositionModel,
)
from spreads.storage.generator_job_models import GeneratorJobModel
from spreads.storage.job_models import JobDefinitionModel, JobLeaseModel, JobRunModel
from spreads.storage.models import OptionQuoteEventModel, ScanCandidateModel, ScanRunModel
from spreads.storage.post_market_models import PostMarketAnalysisRunModel
from spreads.storage.records import (
    AlertEventRecord,
    AlertStateRecord,
    AccountSnapshotRecord,
    BrokerSyncStateRecord,
    CollectorCycleCandidateRecord,
    CollectorCycleEventRecord,
    CollectorCycleRecord,
    ExecutionAttemptRecord,
    ExecutionFillRecord,
    ExecutionOrderRecord,
    SessionPositionCloseRecord,
    SessionPositionRecord,
    GeneratorJobRecord,
    JobDefinitionRecord,
    JobLeaseRecord,
    JobRunRecord,
    OptionQuoteEventRecord,
    PostMarketAnalysisRunRecord,
    ScanCandidateRecord,
    ScanRunRecord,
    SessionTopRunRecord,
)


def parse_datetime(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    normalized = value.replace("Z", "+00:00") if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def parse_date(value: str | date) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return date.fromisoformat(str(value))


def render_value(value: Any) -> Any:
    if isinstance(value, datetime):
        rendered = value.isoformat()
        return rendered.replace("+00:00", "Z") if rendered.endswith("+00:00") else rendered
    if isinstance(value, date):
        return value.isoformat()
    return value


def to_scan_run_record(model: ScanRunModel) -> ScanRunRecord:
    return ScanRunRecord(
        run_id=model.run_id,
        generated_at=render_value(model.generated_at),
        symbol=model.symbol,
        strategy=model.strategy,
        session_label=model.session_label,
        profile=model.profile,
        spot_price=model.spot_price,
        candidate_count=model.candidate_count,
        output_path=model.output_path,
        filters=model.filters_json,
        setup_status=model.setup_status,
        setup_score=model.setup_score,
        setup=model.setup_json,
    )


def to_scan_candidate_record(model: ScanCandidateModel) -> ScanCandidateRecord:
    return ScanCandidateRecord(
        run_id=model.run_id,
        rank=model.rank,
        strategy=model.strategy,
        expiration_date=render_value(model.expiration_date),
        short_symbol=model.short_symbol,
        long_symbol=model.long_symbol,
        short_strike=model.short_strike,
        long_strike=model.long_strike,
        width=model.width,
        midpoint_credit=model.midpoint_credit,
        natural_credit=model.natural_credit,
        breakeven=model.breakeven,
        max_profit=model.max_profit,
        max_loss=model.max_loss,
        quality_score=model.quality_score,
        return_on_risk=model.return_on_risk,
        short_otm_pct=model.short_otm_pct,
        calendar_status=model.calendar_status,
        setup_status=model.setup_status,
        expected_move=model.expected_move,
        short_vs_expected_move=model.short_vs_expected_move,
    )


def to_option_quote_event_record(model: OptionQuoteEventModel) -> OptionQuoteEventRecord:
    return OptionQuoteEventRecord(
        quote_id=model.quote_id,
        cycle_id=model.cycle_id,
        captured_at=render_value(model.captured_at),
        label=model.label,
        underlying_symbol=model.underlying_symbol,
        strategy=model.strategy,
        profile=model.profile,
        option_symbol=model.option_symbol,
        leg_role=model.leg_role,
        bid=model.bid,
        ask=model.ask,
        midpoint=model.midpoint,
        bid_size=model.bid_size,
        ask_size=model.ask_size,
        quote_timestamp=render_value(model.quote_timestamp),
        source=model.source,
    )


def to_alert_event_record(model: AlertEventModel) -> AlertEventRecord:
    return AlertEventRecord(
        alert_id=model.alert_id,
        created_at=render_value(model.created_at),
        session_date=render_value(model.session_date),
        label=model.label,
        cycle_id=model.cycle_id,
        symbol=model.symbol,
        alert_type=model.alert_type,
        dedupe_key=model.dedupe_key,
        status=model.status,
        delivery_target=model.delivery_target,
        payload=dict(model.payload_json or {}),
        response=None if model.response_json is None else dict(model.response_json),
        error_text=model.error_text,
    )


def to_alert_state_record(model: AlertStateModel) -> AlertStateRecord:
    return AlertStateRecord(
        dedupe_key=model.dedupe_key,
        last_alert_at=render_value(model.last_alert_at),
        last_cycle_id=model.last_cycle_id,
        last_alert_type=model.last_alert_type,
        state=dict(model.state_json or {}),
    )


def to_job_definition_record(model: JobDefinitionModel) -> JobDefinitionRecord:
    return JobDefinitionRecord(
        job_key=model.job_key,
        job_type=model.job_type,
        enabled=model.enabled,
        schedule_type=model.schedule_type,
        schedule=dict(model.schedule_json or {}),
        payload=dict(model.payload_json or {}),
        market_calendar=model.market_calendar,
        singleton_scope=model.singleton_scope,
        created_at=render_value(model.created_at),
        updated_at=render_value(model.updated_at),
    )


def to_job_run_record(model: JobRunModel) -> JobRunRecord:
    return JobRunRecord(
        job_run_id=model.job_run_id,
        job_key=model.job_key,
        arq_job_id=model.arq_job_id,
        job_type=model.job_type,
        status=model.status,
        scheduled_for=render_value(model.scheduled_for),
        session_id=model.session_id,
        slot_at=render_value(model.slot_at),
        retry_count=model.retry_count,
        started_at=render_value(model.started_at),
        finished_at=render_value(model.finished_at),
        heartbeat_at=render_value(model.heartbeat_at),
        worker_name=model.worker_name,
        payload=dict(model.payload_json or {}),
        result=None if model.result_json is None else dict(model.result_json),
        error_text=model.error_text,
    )


def to_job_lease_record(model: JobLeaseModel) -> JobLeaseRecord:
    return JobLeaseRecord(
        lease_key=model.lease_key,
        job_run_id=model.job_run_id,
        owner=model.owner,
        acquired_at=render_value(model.acquired_at),
        expires_at=render_value(model.expires_at),
        lease_state=dict(model.lease_state_json or {}),
    )


def to_post_market_analysis_run_record(model: PostMarketAnalysisRunModel) -> PostMarketAnalysisRunRecord:
    recommendations = model.recommendations_json
    rendered_recommendations = None if recommendations is None else list(recommendations)
    return PostMarketAnalysisRunRecord(
        analysis_run_id=model.analysis_run_id,
        job_run_id=model.job_run_id,
        session_date=render_value(model.session_date),
        label=model.label,
        created_at=render_value(model.created_at),
        completed_at=render_value(model.completed_at),
        status=model.status,
        summary=None if model.summary_json is None else dict(model.summary_json),
        diagnostics=None if model.diagnostics_json is None else dict(model.diagnostics_json),
        recommendations=rendered_recommendations,
        report_markdown=model.report_markdown,
        error_text=model.error_text,
    )


def to_generator_job_record(model: GeneratorJobModel) -> GeneratorJobRecord:
    return GeneratorJobRecord(
        generator_job_id=model.generator_job_id,
        arq_job_id=model.arq_job_id,
        symbol=model.symbol,
        status=model.status,
        created_at=render_value(model.created_at),
        started_at=render_value(model.started_at),
        finished_at=render_value(model.finished_at),
        request=dict(model.request_json or {}),
        result=None if model.result_json is None else dict(model.result_json),
        error_text=model.error_text,
    )


def to_execution_attempt_record(model: ExecutionAttemptModel) -> ExecutionAttemptRecord:
    return ExecutionAttemptRecord(
        execution_attempt_id=model.execution_attempt_id,
        session_id=model.session_id,
        session_date=render_value(model.session_date),
        label=model.label,
        cycle_id=model.cycle_id,
        candidate_id=model.candidate_id,
        bucket=model.bucket,
        candidate_generated_at=render_value(model.candidate_generated_at),
        run_id=model.run_id,
        job_run_id=model.job_run_id,
        underlying_symbol=model.underlying_symbol,
        strategy=model.strategy,
        expiration_date=render_value(model.expiration_date),
        short_symbol=model.short_symbol,
        long_symbol=model.long_symbol,
        trade_intent=model.trade_intent,
        session_position_id=model.session_position_id,
        quantity=model.quantity,
        limit_price=model.limit_price,
        requested_at=render_value(model.requested_at),
        submitted_at=render_value(model.submitted_at),
        completed_at=render_value(model.completed_at),
        status=model.status,
        broker=model.broker,
        broker_order_id=model.broker_order_id,
        client_order_id=model.client_order_id,
        request=dict(model.request_json or {}),
        candidate=dict(model.candidate_json or {}),
        error_text=model.error_text,
    )


def to_execution_order_record(model: ExecutionOrderModel) -> ExecutionOrderRecord:
    return ExecutionOrderRecord(
        execution_order_id=model.execution_order_id,
        execution_attempt_id=model.execution_attempt_id,
        broker=model.broker,
        broker_order_id=model.broker_order_id,
        parent_broker_order_id=model.parent_broker_order_id,
        client_order_id=model.client_order_id,
        order_status=model.order_status,
        order_type=model.order_type,
        time_in_force=model.time_in_force,
        order_class=model.order_class,
        side=model.side,
        symbol=model.symbol,
        leg_symbol=model.leg_symbol,
        leg_side=model.leg_side,
        position_intent=model.position_intent,
        quantity=model.quantity,
        limit_price=model.limit_price,
        filled_qty=model.filled_qty,
        filled_avg_price=model.filled_avg_price,
        submitted_at=render_value(model.submitted_at),
        updated_at=render_value(model.updated_at),
        order=dict(model.order_json or {}),
    )


def to_execution_fill_record(model: ExecutionFillModel) -> ExecutionFillRecord:
    return ExecutionFillRecord(
        execution_fill_id=model.execution_fill_id,
        execution_attempt_id=model.execution_attempt_id,
        execution_order_id=model.execution_order_id,
        broker=model.broker,
        broker_fill_id=model.broker_fill_id,
        broker_order_id=model.broker_order_id,
        symbol=model.symbol,
        side=model.side,
        fill_type=model.fill_type,
        quantity=model.quantity,
        cumulative_quantity=model.cumulative_quantity,
        remaining_quantity=model.remaining_quantity,
        price=model.price,
        filled_at=render_value(model.filled_at),
        fill=dict(model.fill_json or {}),
    )


def to_session_position_record(model: SessionPositionModel) -> SessionPositionRecord:
    return SessionPositionRecord(
        session_position_id=model.session_position_id,
        session_id=model.session_id,
        session_date=render_value(model.session_date),
        label=model.label,
        candidate_id=model.candidate_id,
        open_execution_attempt_id=model.open_execution_attempt_id,
        underlying_symbol=model.underlying_symbol,
        strategy=model.strategy,
        expiration_date=render_value(model.expiration_date),
        short_symbol=model.short_symbol,
        long_symbol=model.long_symbol,
        requested_quantity=model.requested_quantity,
        opened_quantity=model.opened_quantity,
        remaining_quantity=model.remaining_quantity,
        entry_credit=model.entry_credit,
        entry_notional=model.entry_notional,
        width=model.width,
        max_profit=model.max_profit,
        max_loss=model.max_loss,
        opened_at=render_value(model.opened_at),
        closed_at=render_value(model.closed_at),
        status=model.status,
        realized_pnl=model.realized_pnl,
        unrealized_pnl=model.unrealized_pnl,
        close_mark=model.close_mark,
        close_mark_source=model.close_mark_source,
        close_marked_at=render_value(model.close_marked_at),
        last_broker_status=model.last_broker_status,
        exit_policy=dict(model.exit_policy_json or {}),
        risk_policy=dict(model.risk_policy_json or {}),
        source_job_type=model.source_job_type,
        source_job_key=model.source_job_key,
        source_job_run_id=model.source_job_run_id,
        last_exit_evaluated_at=render_value(model.last_exit_evaluated_at),
        last_exit_reason=model.last_exit_reason,
        last_reconciled_at=render_value(model.last_reconciled_at),
        reconciliation_status=model.reconciliation_status,
        reconciliation_note=model.reconciliation_note,
        created_at=render_value(model.created_at),
        updated_at=render_value(model.updated_at),
    )


def to_session_position_close_record(model: SessionPositionCloseModel) -> SessionPositionCloseRecord:
    return SessionPositionCloseRecord(
        session_position_close_id=model.session_position_close_id,
        session_position_id=model.session_position_id,
        execution_attempt_id=model.execution_attempt_id,
        closed_quantity=model.closed_quantity,
        exit_debit=model.exit_debit,
        realized_pnl=model.realized_pnl,
        broker_order_id=model.broker_order_id,
        closed_at=render_value(model.closed_at),
        created_at=render_value(model.created_at),
        updated_at=render_value(model.updated_at),
    )


def to_account_snapshot_record(model: AccountSnapshotModel) -> AccountSnapshotRecord:
    return AccountSnapshotRecord(
        snapshot_id=model.snapshot_id,
        broker=model.broker,
        environment=model.environment,
        source=model.source,
        captured_at=render_value(model.captured_at),
        account=dict(model.account_json or {}),
        pnl=dict(model.pnl_json or {}),
        positions=list(model.positions_json or []),
        history=dict(model.history_json or {}),
    )


def to_broker_sync_state_record(model: BrokerSyncStateModel) -> BrokerSyncStateRecord:
    return BrokerSyncStateRecord(
        sync_key=model.sync_key,
        broker=model.broker,
        status=model.status,
        updated_at=render_value(model.updated_at),
        cursor=dict(model.cursor_json or {}),
        summary=dict(model.summary_json or {}),
        error_text=model.error_text,
    )


def to_collector_cycle_record(model: CollectorCycleModel) -> CollectorCycleRecord:
    return CollectorCycleRecord(
        cycle_id=model.cycle_id,
        label=model.label,
        session_date=render_value(model.session_date),
        generated_at=render_value(model.generated_at),
        job_run_id=model.job_run_id,
        session_id=model.session_id,
        universe_label=model.universe_label,
        strategy=model.strategy,
        profile=model.profile,
        greeks_source=model.greeks_source,
        symbols=list(model.symbols_json or []),
        failures=list(model.failures_json or []),
        selection_state=dict(model.selection_state_json or {}),
    )


def to_collector_cycle_candidate_record(
    model: CollectorCycleCandidateModel,
    *,
    label: str,
    session_date: date,
    generated_at: datetime,
) -> CollectorCycleCandidateRecord:
    return CollectorCycleCandidateRecord(
        candidate_id=model.candidate_id,
        cycle_id=model.cycle_id,
        label=label,
        session_date=render_value(session_date),
        generated_at=render_value(generated_at),
        bucket=model.bucket,
        position=model.position,
        run_id=model.run_id,
        underlying_symbol=model.underlying_symbol,
        strategy=model.strategy,
        expiration_date=render_value(model.expiration_date),
        short_symbol=model.short_symbol,
        long_symbol=model.long_symbol,
        quality_score=model.quality_score,
        midpoint_credit=model.midpoint_credit,
        candidate=dict(model.candidate_json or {}),
    )


def to_collector_cycle_event_record(model: CollectorCycleEventModel) -> CollectorCycleEventRecord:
    return CollectorCycleEventRecord(
        event_id=model.event_id,
        cycle_id=model.cycle_id,
        label=model.label,
        session_date=render_value(model.session_date),
        generated_at=render_value(model.generated_at),
        symbol=model.symbol,
        event_type=model.event_type,
        message=model.message,
        previous_candidate=None if model.previous_candidate_json is None else dict(model.previous_candidate_json),
        current_candidate=None if model.current_candidate_json is None else dict(model.current_candidate_json),
    )


def to_session_top_run_record(
    run: ScanRunModel,
    candidate: ScanCandidateModel | None,
) -> SessionTopRunRecord:
    return SessionTopRunRecord(
        run_id=run.run_id,
        generated_at=render_value(run.generated_at),
        symbol=run.symbol,
        strategy=run.strategy,
        profile=run.profile,
        spot_price=run.spot_price,
        candidate_count=run.candidate_count,
        setup_status=run.setup_status,
        setup_score=run.setup_score,
        setup_json=run.setup_json,
        short_symbol=None if candidate is None else candidate.short_symbol,
        long_symbol=None if candidate is None else candidate.long_symbol,
        short_strike=None if candidate is None else candidate.short_strike,
        long_strike=None if candidate is None else candidate.long_strike,
        midpoint_credit=None if candidate is None else candidate.midpoint_credit,
        quality_score=None if candidate is None else candidate.quality_score,
        calendar_status=None if candidate is None else candidate.calendar_status,
        expected_move=None if candidate is None else candidate.expected_move,
        short_vs_expected_move=None if candidate is None else candidate.short_vs_expected_move,
    )
