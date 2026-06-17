from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from pydantic import Field
from sqlalchemy import select

from core.domain.models import OptionContract, OptionSnapshot, SymbolMarketSlice
from core.integrations.calendar_events import classify_underlying_type
from core.model_contracts import DomainModel
from core.services.backtest.strategy_scope import load_backtest_strategy_scope
from core.services.market_dates import NEW_YORK
from core.services.option_quote_records import build_option_symbol_metadata
from core.services.option_symbols import OptionSymbolParts, parse_occ_option_symbol
from core.services.strategy_candidate_builders.market_data import build_market_slice_from_loaded_data
from core.services.strategy_candidate_builders.runtime_context import candidate_reference_date, candidate_reference_datetime
from core.storage.calendar_models import EarningsEventConsensusModel
from core.storage.engine_models import CandidateSymbolDiagnosticModel, TickerSourceObservationModel, TradeCandidateModel
from core.storage.serializers import parse_datetime, render_value
from core.value_coercion import as_mapping, as_text, coerce_float, coerce_int


class HistoricalMarketSliceRequest(DomainModel):
    market_date: str | None = None
    as_of: str | None = None
    trading_strategy_id: str | None = None
    routine: str = "entry"
    label: str | None = None
    profile: str | None = None
    max_contracts: int = Field(default=750, ge=1)


class HistoricalMarketSliceDiagnostics(DomainModel):
    symbol: str
    market_date: str
    as_of: str
    fidelity_labels: dict[str, str]
    counts: dict[str, int]
    source: dict[str, Any]
    calendar: dict[str, Any]
    latest_diagnostic: dict[str, Any]
    warnings: tuple[str, ...] = ()


@dataclass
class HistoricalMarketSliceProvider:
    storage: Any
    request: HistoricalMarketSliceRequest = field(default_factory=HistoricalMarketSliceRequest)
    greeks_provider: Any | None = None
    _diagnostics_by_symbol: dict[str, HistoricalMarketSliceDiagnostics] = field(default_factory=dict, init=False)

    def get_symbol_market_slice(
        self,
        *,
        symbol: str,
        parameters: Any,
    ) -> SymbolMarketSlice:
        normalized_symbol = symbol.upper()
        market_date = _market_date(self.request, parameters)
        as_of = _as_of(self.request, parameters, market_date=market_date)
        session_start, session_end = _session_bounds(market_date)
        captured_to = min(as_of, session_end)
        label = self.request.label or self.request.trading_strategy_id

        metadata, diagnostic = _load_postgres_context(
            storage=self.storage,
            symbol=normalized_symbol,
            request=self.request,
            market_date=market_date,
            as_of=captured_to,
            parameters=parameters,
        )
        quote_rows: list[dict[str, Any]] = []
        quote_scope = "missing"
        quote_attempts = []
        if label is not None:
            quote_attempts.append(("strategy_label_profile", label, self.request.profile))
            if self.request.profile is not None:
                quote_attempts.append(("strategy_label", label, None))
        if self.request.profile is not None:
            quote_attempts.append(("underlying_profile", None, self.request.profile))
        quote_attempts.append(("underlying_fallback", None, None))
        for attempt_scope, attempt_label, attempt_profile in quote_attempts:
            quote_rows = self.storage.market_data.list_latest_option_quotes_by_underlying(
                underlying_symbols=[normalized_symbol],
                captured_from=session_start,
                captured_to=captured_to,
                label=attempt_label,
                profile=attempt_profile,
                limit=self.request.max_contracts,
            )
            if quote_rows:
                quote_scope = attempt_scope
                break

        option_symbols = [str(row.get("option_symbol") or "").strip().upper() for row in quote_rows if row.get("option_symbol")]
        trade_label = label if quote_scope.startswith("strategy_label") else None
        trade_profile = self.request.profile if quote_scope.endswith("profile") else None
        trade_rows = self.storage.market_data.list_option_trade_ticks_window(
            option_symbols=option_symbols,
            captured_from=session_start,
            captured_to=captured_to,
            label=trade_label,
            profile=trade_profile,
        )
        latest_trade_price_by_symbol = _latest_trade_price_by_symbol(trade_rows)
        spot_price = _spot_price(diagnostic=diagnostic, source=metadata.source)
        if spot_price is None or spot_price <= 0:
            raise ValueError(f"No historical spot price available for {normalized_symbol} on {market_date.isoformat()}")

        contracts_and_snapshots = _contracts_and_snapshots(
            quote_rows=quote_rows,
            metadata=metadata.option_symbols,
            latest_trade_price_by_symbol=latest_trade_price_by_symbol,
        )
        market_slice = build_market_slice_from_loaded_data(
            symbol=normalized_symbol,
            underlying_type=classify_underlying_type(normalized_symbol),
            spot_price=spot_price,
            daily_bars=[],
            intraday_bars=[],
            call_contracts_by_expiration=contracts_and_snapshots.call_contracts,
            put_contracts_by_expiration=contracts_and_snapshots.put_contracts,
            call_snapshots_by_expiration=contracts_and_snapshots.call_snapshots,
            put_snapshots_by_expiration=contracts_and_snapshots.put_snapshots,
            greeks_provider=self.greeks_provider,
            greeks_as_of=captured_to,
            greeks_source_mode=getattr(parameters, "greeks_source", "auto"),
        )
        self._diagnostics_by_symbol[normalized_symbol] = _diagnostics(
            symbol=normalized_symbol,
            market_date=market_date,
            as_of=captured_to,
            quote_rows=quote_rows,
            trade_rows=trade_rows,
            quote_scope=quote_scope,
            max_quote_age_seconds=coerce_int(getattr(parameters, "max_quote_age_seconds", None)),
            metadata=metadata,
            diagnostic=diagnostic,
            market_slice=market_slice,
        )
        return market_slice

    def diagnostics_for_symbol(self, symbol: str) -> dict[str, Any] | None:
        diagnostics = self._diagnostics_by_symbol.get(symbol.upper())
        return None if diagnostics is None else diagnostics.to_payload()


@dataclass(frozen=True)
class _PostgresContext:
    option_symbols: dict[str, dict[str, Any]]
    source: dict[str, Any]
    calendar: dict[str, Any]


@dataclass(frozen=True)
class _ContractsAndSnapshots:
    call_contracts: dict[str, list[OptionContract]]
    put_contracts: dict[str, list[OptionContract]]
    call_snapshots: dict[str, dict[str, OptionSnapshot]]
    put_snapshots: dict[str, dict[str, OptionSnapshot]]


def _market_date(request: HistoricalMarketSliceRequest, parameters: Any) -> date:
    if request.market_date:
        return date.fromisoformat(request.market_date)
    return candidate_reference_date(parameters)


def _as_of(request: HistoricalMarketSliceRequest, parameters: Any, *, market_date: date) -> datetime:
    if request.as_of:
        resolved = parse_datetime(request.as_of)
        if resolved is None:
            raise ValueError(f"Invalid backtest market-slice as_of: {request.as_of!r}")
        return resolved
    reference_at = candidate_reference_datetime(parameters)
    if reference_at is not None:
        return reference_at
    return datetime.combine(market_date, time(16, 0), tzinfo=NEW_YORK).astimezone(UTC)


def _session_bounds(market_date: date) -> tuple[datetime, datetime]:
    start = datetime.combine(market_date, time.min, tzinfo=NEW_YORK)
    end = start + timedelta(days=1)
    return start.astimezone(UTC), end.astimezone(UTC)


def _load_postgres_context(
    *,
    storage: Any,
    symbol: str,
    request: HistoricalMarketSliceRequest,
    market_date: date,
    as_of: datetime,
    parameters: Any,
) -> tuple[_PostgresContext, dict[str, Any]]:
    strategy_id = request.trading_strategy_id
    source_id = _strategy_source_id(strategy_id)
    session_start, _ = _session_bounds(market_date)
    with storage.session_factory() as session:
        diagnostic_row = _latest_candidate_diagnostic(
            session=session,
            symbol=symbol,
            strategy_id=strategy_id,
            routine=request.routine,
            session_start=session_start,
            as_of=as_of,
        )
        source_row = _latest_source_observation(
            session=session,
            symbol=symbol,
            source_id=source_id,
            as_of=as_of,
        )
        candidate_payloads = _candidate_payloads(
            session=session,
            symbol=symbol,
            strategy_id=strategy_id,
            routine=request.routine,
            session_start=session_start,
            as_of=as_of,
        )
        candidate_payloads.extend(_diagnostic_example_payloads(diagnostic_row))
        calendar_rows = _earnings_consensus_rows(
            session=session,
            symbol=symbol,
            market_date=market_date,
            max_dte=coerce_int(getattr(parameters, "max_dte", None)) or 0,
        )
    source = {} if source_row is None else _source_payload(source_row)
    calendar = _calendar_payload(calendar_rows)
    return (
        _PostgresContext(
            option_symbols=build_option_symbol_metadata(candidate_payloads),
            source=source,
            calendar=calendar,
        ),
        {} if diagnostic_row is None else _diagnostic_payload(diagnostic_row),
    )


def _strategy_source_id(strategy_id: str | None) -> str | None:
    if not strategy_id:
        return None
    strategies = load_backtest_strategy_scope((strategy_id,))
    strategy = strategies.get(strategy_id)
    return None if strategy is None else strategy.source.ref


def _latest_candidate_diagnostic(
    *,
    session: Any,
    symbol: str,
    strategy_id: str | None,
    routine: str,
    session_start: datetime,
    as_of: datetime,
) -> CandidateSymbolDiagnosticModel | None:
    statement = (
        select(CandidateSymbolDiagnosticModel)
        .where(CandidateSymbolDiagnosticModel.underlying_symbol == symbol)
        .where(CandidateSymbolDiagnosticModel.routine == routine)
        .where(CandidateSymbolDiagnosticModel.observed_at >= session_start)
        .where(CandidateSymbolDiagnosticModel.observed_at <= as_of)
        .order_by(CandidateSymbolDiagnosticModel.observed_at.desc())
        .limit(1)
    )
    if strategy_id is not None:
        statement = statement.where(CandidateSymbolDiagnosticModel.trading_strategy_id == strategy_id)
    return session.scalar(statement)


def _latest_source_observation(
    *,
    session: Any,
    symbol: str,
    source_id: str | None,
    as_of: datetime,
) -> TickerSourceObservationModel | None:
    statement = (
        select(TickerSourceObservationModel)
        .where(TickerSourceObservationModel.symbol == symbol)
        .where(TickerSourceObservationModel.created_at <= as_of)
        .order_by(TickerSourceObservationModel.created_at.desc())
        .limit(1)
    )
    if source_id is not None:
        statement = statement.where(TickerSourceObservationModel.ticker_source_id == source_id)
    return session.scalar(statement)


def _candidate_payloads(
    *,
    session: Any,
    symbol: str,
    strategy_id: str | None,
    routine: str,
    session_start: datetime,
    as_of: datetime,
    limit: int = 250,
) -> list[dict[str, Any]]:
    statement = (
        select(TradeCandidateModel)
        .where(TradeCandidateModel.underlying_symbol == symbol)
        .where(TradeCandidateModel.routine == routine)
        .where(TradeCandidateModel.observed_at >= session_start)
        .where(TradeCandidateModel.observed_at <= as_of)
        .order_by(TradeCandidateModel.observed_at.desc())
        .limit(max(int(limit), 1))
    )
    if strategy_id is not None:
        statement = statement.where(TradeCandidateModel.trading_strategy_id == strategy_id)
    rows = session.scalars(statement).all()
    payloads: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row.candidate_json or {})
        if row.legs_json:
            payload["legs"] = list(row.legs_json)
        if row.expiration_date is not None and not payload.get("expiration_date"):
            payload["expiration_date"] = row.expiration_date.isoformat()
        payload.setdefault("underlying_symbol", row.underlying_symbol)
        payloads.append(payload)
    return payloads


def load_historical_trade_candidate_payloads(
    *,
    storage: Any,
    symbol: str,
    strategy_id: str | None,
    routine: str,
    market_date: date,
    as_of: datetime,
    limit: int = 250,
) -> list[dict[str, Any]]:
    session_start, _ = _session_bounds(market_date)
    with storage.session_factory() as session:
        return _candidate_payloads(
            session=session,
            symbol=symbol,
            strategy_id=strategy_id,
            routine=routine,
            session_start=session_start,
            as_of=as_of,
            limit=limit,
        )


def _diagnostic_example_payloads(row: CandidateSymbolDiagnosticModel | None) -> list[dict[str, Any]]:
    if row is None:
        return []
    examples = as_mapping(row.examples_json)
    payloads: list[dict[str, Any]] = []
    for value in examples.values():
        if isinstance(value, list):
            payloads.extend(dict(item) for item in value if isinstance(item, Mapping))
        elif isinstance(value, Mapping):
            payloads.extend(dict(item) for item in value.values() if isinstance(item, Mapping))
    return payloads


def _earnings_consensus_rows(
    *,
    session: Any,
    symbol: str,
    market_date: date,
    max_dte: int,
) -> list[EarningsEventConsensusModel]:
    window_end = market_date + timedelta(days=max(max_dte, 0))
    statement = (
        select(EarningsEventConsensusModel)
        .where(EarningsEventConsensusModel.symbol == symbol)
        .where(EarningsEventConsensusModel.event_date >= market_date)
        .where(EarningsEventConsensusModel.event_date <= window_end)
        .order_by(EarningsEventConsensusModel.event_date.asc())
    )
    return list(session.scalars(statement).all())


def _source_payload(row: TickerSourceObservationModel) -> dict[str, Any]:
    return {
        "ticker_source_id": row.ticker_source_id,
        "ticker_source_run_id": row.ticker_source_run_id,
        "observation_state": row.observation_state,
        "rank": row.rank,
        "score": row.score,
        "price": row.price,
        "daily_volume": row.daily_volume,
        "created_at": render_value(row.created_at),
    }


def _calendar_payload(rows: Iterable[EarningsEventConsensusModel]) -> dict[str, Any]:
    rendered = [
        {
            "consensus_id": row.consensus_id,
            "event_date": render_value(row.event_date),
            "scheduled_at": render_value(row.scheduled_at),
            "session_timing": row.session_timing,
            "event_status": row.event_status,
            "consensus_status": row.consensus_status,
            "source_confidence": row.source_confidence,
            "timing_confidence": row.timing_confidence,
            "stale_after": render_value(row.stale_after),
        }
        for row in rows
    ]
    return {
        "earnings_consensus_count": len(rendered),
        "earnings_consensus": rendered,
    }


def _diagnostic_payload(row: CandidateSymbolDiagnosticModel) -> dict[str, Any]:
    return {
        "candidate_run_id": row.candidate_run_id,
        "trading_strategy_id": row.trading_strategy_id,
        "routine": row.routine,
        "observed_at": render_value(row.observed_at),
        "spot_price": row.spot_price,
        "expiration_count": row.expiration_count,
        "contract_count": row.contract_count,
        "snapshot_count": row.snapshot_count,
        "raw_candidate_count": row.raw_candidate_count,
        "returned_candidate_count": row.returned_candidate_count,
        "diagnostic_status": row.diagnostic_status,
    }


def _spot_price(*, diagnostic: Mapping[str, Any], source: Mapping[str, Any]) -> float | None:
    return coerce_float(diagnostic.get("spot_price")) or coerce_float(source.get("price"))


def _latest_trade_price_by_symbol(rows: list[dict[str, Any]]) -> dict[str, float]:
    latest: dict[str, tuple[datetime, float]] = {}
    for row in rows:
        symbol = as_text(row.get("option_symbol"))
        price = coerce_float(row.get("price"))
        captured_at = parse_datetime(row.get("captured_at"))
        if symbol is None or price is None or captured_at is None:
            continue
        existing = latest.get(symbol)
        if existing is None or captured_at > existing[0]:
            latest[symbol] = (captured_at, price)
    return {symbol: price for symbol, (_, price) in latest.items()}


def _contracts_and_snapshots(
    *,
    quote_rows: list[dict[str, Any]],
    metadata: dict[str, dict[str, Any]],
    latest_trade_price_by_symbol: dict[str, float],
) -> _ContractsAndSnapshots:
    call_contracts: dict[str, list[OptionContract]] = {}
    put_contracts: dict[str, list[OptionContract]] = {}
    call_snapshots: dict[str, dict[str, OptionSnapshot]] = {}
    put_snapshots: dict[str, dict[str, OptionSnapshot]] = {}
    for row in quote_rows:
        option_symbol = str(row.get("option_symbol") or "").strip().upper()
        parsed = parse_occ_option_symbol(option_symbol)
        if parsed is None:
            continue
        enriched = {**_metadata_from_occ(parsed), **metadata.get(option_symbol, {})}
        expiration_date = as_text(enriched.get("expiration_date")) or parsed.expiration_date
        option_type = as_text(enriched.get("option_type")) or parsed.option_type
        strike_price = coerce_float(enriched.get("strike_price")) or parsed.strike_price
        bid = coerce_float(row.get("bid")) or 0.0
        ask = coerce_float(row.get("ask")) or 0.0
        midpoint = coerce_float(row.get("midpoint"))
        if midpoint is None:
            midpoint = (bid + ask) / 2.0
        contract = OptionContract(
            symbol=option_symbol,
            expiration_date=expiration_date,
            strike_price=strike_price,
            open_interest=coerce_int(enriched.get("open_interest")) or 0,
            close_price=latest_trade_price_by_symbol.get(option_symbol),
        )
        snapshot = OptionSnapshot(
            symbol=option_symbol,
            bid=bid,
            ask=ask,
            bid_size=coerce_int(row.get("bid_size")) or 0,
            ask_size=coerce_int(row.get("ask_size")) or 0,
            midpoint=midpoint,
            delta=coerce_float(enriched.get("delta")),
            gamma=coerce_float(enriched.get("gamma")),
            theta=coerce_float(enriched.get("theta")),
            vega=coerce_float(enriched.get("vega")),
            implied_volatility=coerce_float(enriched.get("implied_volatility")),
            last_trade_price=latest_trade_price_by_symbol.get(option_symbol),
            daily_volume=coerce_int(enriched.get("volume")),
            greeks_source="stored_candidate_metadata" if enriched.get("delta") is not None else None,
        )
        if option_type == "put":
            put_contracts.setdefault(expiration_date, []).append(contract)
            put_snapshots.setdefault(expiration_date, {})[option_symbol] = snapshot
        else:
            call_contracts.setdefault(expiration_date, []).append(contract)
            call_snapshots.setdefault(expiration_date, {})[option_symbol] = snapshot
    return _ContractsAndSnapshots(
        call_contracts={key: sorted(value, key=lambda item: item.strike_price) for key, value in call_contracts.items()},
        put_contracts={key: sorted(value, key=lambda item: item.strike_price) for key, value in put_contracts.items()},
        call_snapshots=call_snapshots,
        put_snapshots=put_snapshots,
    )


def _metadata_from_occ(parsed: OptionSymbolParts) -> dict[str, Any]:
    return {
        "underlying_symbol": parsed.underlying_symbol,
        "option_type": parsed.option_type,
        "expiration_date": parsed.expiration_date,
        "strike_price": parsed.strike_price,
    }


def _diagnostics(
    *,
    symbol: str,
    market_date: date,
    as_of: datetime,
    quote_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
    quote_scope: str,
    max_quote_age_seconds: int | None,
    metadata: _PostgresContext,
    diagnostic: dict[str, Any],
    market_slice: SymbolMarketSlice,
) -> HistoricalMarketSliceDiagnostics:
    call_snapshot_count = sum(len(rows) for rows in market_slice.call_snapshots_by_expiration.values())
    put_snapshot_count = sum(len(rows) for rows in market_slice.put_snapshots_by_expiration.values())
    delta_count = sum(
        1
        for snapshots in [*market_slice.call_snapshots_by_expiration.values(), *market_slice.put_snapshots_by_expiration.values()]
        for snapshot in snapshots.values()
        if snapshot.delta is not None
    )
    warnings = []
    if not quote_rows:
        warnings.append("no_clickhouse_quote_rows")
    if not market_slice.daily_bars:
        warnings.append("underlying_daily_bars_unavailable")
    if not market_slice.intraday_bars:
        warnings.append("underlying_intraday_bars_unavailable")
    return HistoricalMarketSliceDiagnostics(
        symbol=symbol,
        market_date=market_date.isoformat(),
        as_of=render_value(as_of),
        fidelity_labels={
            "source": "stored_ticker_source_observation" if metadata.source else "missing_source_observation",
            "calendar": "stored_earnings_consensus" if metadata.calendar.get("earnings_consensus_count") else "no_calendar_consensus_in_window",
            "contracts": _contract_fidelity(stored_metadata_count=len(metadata.option_symbols), quote_count=len(quote_rows)),
            "quotes": _quote_fidelity(
                quote_rows,
                quote_scope=quote_scope,
                as_of=as_of,
                max_quote_age_seconds=max_quote_age_seconds,
            ),
            "trades": "clickhouse_trade_ticks" if trade_rows else "no_trade_ticks_in_window",
            "greeks": _greeks_fidelity(delta_count=delta_count, snapshot_count=call_snapshot_count + put_snapshot_count),
            "underlying_bars": "missing_underlying_bars",
        },
        counts={
            "quote_contract_count": len(quote_rows),
            "trade_tick_count": len(trade_rows),
            "call_snapshot_count": call_snapshot_count,
            "put_snapshot_count": put_snapshot_count,
            "delta_snapshot_count": delta_count,
            "expected_move_count": len(market_slice.expected_moves_by_expiration),
            "latest_quote_age_seconds": _latest_quote_age_seconds(quote_rows, as_of=as_of) or 0,
        },
        source=metadata.source,
        calendar=metadata.calendar,
        latest_diagnostic=diagnostic,
        warnings=tuple(warnings),
    )


def _quote_fidelity(
    quote_rows: list[dict[str, Any]],
    *,
    quote_scope: str,
    as_of: datetime,
    max_quote_age_seconds: int | None,
) -> str:
    if not quote_rows:
        return "missing_clickhouse_quotes"
    age_seconds = _latest_quote_age_seconds(quote_rows, as_of=as_of)
    if max_quote_age_seconds is not None and age_seconds is not None and age_seconds > max_quote_age_seconds:
        if quote_scope.startswith("strategy_label"):
            return "stale_clickhouse_quotes_strategy_scoped"
        return "stale_clickhouse_quotes_underlying_fallback"
    if quote_scope == "strategy_label_profile":
        return "latest_clickhouse_quotes_strategy_scoped"
    if quote_scope == "strategy_label":
        return "latest_clickhouse_quotes_strategy_scoped_profile_missing"
    if quote_scope == "underlying_profile":
        return "latest_clickhouse_quotes_underlying_scoped"
    return "latest_clickhouse_quotes_underlying_fallback"


def _latest_quote_age_seconds(quote_rows: list[dict[str, Any]], *, as_of: datetime) -> int | None:
    timestamps = [parse_datetime(row.get("captured_at")) for row in quote_rows]
    latest = max((timestamp for timestamp in timestamps if timestamp is not None), default=None)
    if latest is None:
        return None
    return max(int((as_of - latest).total_seconds()), 0)


def _contract_fidelity(*, stored_metadata_count: int, quote_count: int) -> str:
    if quote_count <= 0:
        return "no_option_contracts"
    if stored_metadata_count <= 0:
        return "approximated_occ_contract_metadata_no_open_interest"
    if stored_metadata_count >= quote_count:
        return "stored_candidate_contract_metadata"
    return "partial_stored_candidate_metadata_with_occ_fallback"


def _greeks_fidelity(*, delta_count: int, snapshot_count: int) -> str:
    if snapshot_count <= 0:
        return "no_option_snapshots"
    if delta_count == snapshot_count:
        return "full_delta_coverage"
    if delta_count > 0:
        return "partial_delta_coverage"
    return "missing_delta_coverage"


__all__ = [
    "HistoricalMarketSliceDiagnostics",
    "HistoricalMarketSliceProvider",
    "HistoricalMarketSliceRequest",
    "load_historical_trade_candidate_payloads",
]
