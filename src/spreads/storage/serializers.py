from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from spreads.storage.models import OptionQuoteEventModel, ScanCandidateModel, ScanRunModel
from spreads.storage.records import (
    OptionQuoteEventRecord,
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
