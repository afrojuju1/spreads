from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from core.domain.models import SymbolMarketSlice
from core.services.strategy_builders import (
    DEFAULT_MARKET_BENCHMARK_SYMBOLS,
    build_entry_runtime_candidates_with_diagnostics_from_market_slices,
    build_symbol_market_slice_parameters,
    runtime_owner_key,
)
from core.services.strategy_candidate_builders.market_data import AlpacaMarketSliceProvider
from core.services.strategy_candidate_builders.settings import CandidateBuildParameters
from core.services.ticker_sources import resolve_ticker_source_symbols
from core.services.trading_engine.data import (
    CaptureTargetDeclaration,
    CaptureTargetRequest,
    CandidateBuildRequest,
    CandidateBuildResult,
    ResolvedTickerSet,
    TickerSourceFallback,
    TickerSourceSpec,
)
from core.services.trading_engine.entry_quality import FeatureSnapshot
from core.services.trading_engine.feature_snapshots import build_feature_snapshots_for_strategy
from core.services.trading_engine.kernel import EngineContext
from core.services.trading_engine.market_context import MarketContextSnapshot
from core.services.trading_engine.market_context_runtime import MarketContextEngine, MarketContextRequest
from core.services.trading_strategies import load_universe_symbols
from core.services.trading_strategy_runtime_models import EntryRuntime, StrategySource
from core.value_coercion import utc_expiry_iso

DEFAULT_ENTRY_CANDIDATE_LIMIT = 10
DEFAULT_GREEKS_SOURCE = "auto"


def engine_snapshot_label(
    *,
    universe_label: str,
    candidate_builder: str,
    build_profile: str,
    greeks_source: str,
) -> str:
    return f"{universe_label}_{candidate_builder}_{build_profile}_{greeks_source}".lower()


def ticker_source_spec_from_strategy_source(source: StrategySource) -> TickerSourceSpec:
    return TickerSourceSpec(
        source_type=source.kind,
        ref=source.ref,
        max_age_seconds=source.max_age_seconds,
        max_symbols=source.max_symbols,
        fallback=TickerSourceFallback(universe_ref=source.fallback_universe_ref),
    )


def entry_runtime_with_symbols(runtime: EntryRuntime, symbols: tuple[str, ...]) -> EntryRuntime:
    normalized = tuple(dict.fromkeys(str(symbol).upper().strip() for symbol in symbols if str(symbol or "").strip()))
    return replace(runtime, strategy=replace(runtime.strategy, symbols=normalized))


def entry_engine_label(runtime: EntryRuntime) -> str:
    return f"trading_strategy:{runtime.trading_strategy_id}:entry"


def entry_engine_strategy_run_id(run_id: str, trading_strategy_id: str) -> str:
    return f"strategy_run:{run_id}:{trading_strategy_id}:entry"


def entry_candidate_limit(request: CandidateBuildRequest) -> int:
    if request.candidate_limit not in (None, ""):
        return max(int(request.candidate_limit), 1)
    return DEFAULT_ENTRY_CANDIDATE_LIMIT


def entry_candidate_build_parameters(
    *,
    runtime: EntryRuntime,
    symbols: tuple[str, ...],
    request: CandidateBuildRequest,
    db_target: str,
    config_root: str | None,
) -> CandidateBuildParameters:
    greeks_source = str(request.greeks_source or DEFAULT_GREEKS_SOURCE)
    return CandidateBuildParameters(
        symbols=symbols,
        candidate_builder_key=runtime.build_settings.candidate_builder_key,
        build_profile=runtime.build_settings.build_profile,
        greeks_source=greeks_source,
        top=entry_candidate_limit(request),
        per_symbol_top=max(int(request.per_symbol_top or 1), 1),
        history_db=db_target,
        config_root=None if config_root in (None, "") else str(config_root),
        session_label=engine_snapshot_label(
            universe_label=entry_engine_label(runtime),
            candidate_builder=runtime.build_settings.candidate_builder_key,
            build_profile=runtime.build_settings.build_profile,
            greeks_source=greeks_source,
        ),
    )


class DataEngine:
    def __init__(self, context: EngineContext) -> None:
        self.context = context

    def resolve_tickers(
        self,
        *,
        source: TickerSourceSpec,
        as_of: datetime,
    ) -> ResolvedTickerSet:
        source_type = str(source.source_type or "").strip().lower()
        if source_type == "static":
            symbols = tuple(load_universe_symbols(source.ref, config_root=self.context.config_root))
            return self._resolved_ticker_set(
                source=source,
                symbols=symbols,
                resolved_at=as_of,
                reason_codes=("static_source",),
                evidence={
                    "kind": "static",
                    "universe_ref": source.ref,
                    "summary": {"symbol_count": len(symbols)},
                },
            )

        if source_type == "dynamic":
            job_key = f"ticker_source:{source.ref}"
            snapshot = resolve_ticker_source_symbols(
                self.context.storage.engine_facts,
                source_id=source.ref,
                job_key=job_key,
                max_age_seconds=source.max_age_seconds,
                fallback_universe_ref=self._fallback_universe_ref(source),
                config_root=self.context.config_root,
            )
            symbols = tuple(str(symbol).upper() for symbol in list(snapshot.get("symbols") or []) if str(symbol or "").strip())
            status = str(snapshot.get("status") or "").strip().lower()
            degradation = snapshot.get("degradation") if isinstance(snapshot.get("degradation"), Mapping) else {}
            reason = str(degradation.get("reason") or status or "unavailable")
            return self._resolved_ticker_set(
                source=source,
                symbols=symbols,
                resolved_at=as_of,
                ticker_source_run_id=None if snapshot.get("ticker_source_run_id") in (None, "") else str(snapshot["ticker_source_run_id"]),
                reason_codes=(f"ticker_source_{status or 'missing'}",),
                blockers=() if symbols and status in {"ready", "fallback"} else (reason,),
                evidence=dict(snapshot),
            )

        raise ValueError(f"Unsupported ticker source type: {source.source_type}")

    def build_trade_candidates(
        self,
        request: CandidateBuildRequest,
    ) -> CandidateBuildResult:
        runtime = request.entry_runtime
        if not isinstance(runtime, EntryRuntime):
            raise ValueError("CandidateBuildRequest.entry_runtime is required")
        return self.build_entry_trade_candidates(
            request=request,
            runtime=runtime,
        )

    def build_feature_snapshots(
        self,
        *,
        trade_structure: str,
        quality_profile_id: str,
        ticker_set: ResolvedTickerSet,
        candidate_result: CandidateBuildResult,
    ) -> tuple[FeatureSnapshot, ...]:
        return build_feature_snapshots_for_strategy(
            trade_structure=trade_structure,
            quality_profile_id=quality_profile_id,
            ticker_set=ticker_set,
            candidate_result=candidate_result,
        )

    def build_market_context(
        self,
        *,
        benchmark_slices: Mapping[str, SymbolMarketSlice],
        request: MarketContextRequest | None = None,
    ) -> MarketContextSnapshot:
        return MarketContextEngine().build_from_market_slices(
            benchmark_slices=benchmark_slices,
            request=request,
        )

    def build_entry_trade_candidates(
        self,
        *,
        request: CandidateBuildRequest,
        runtime: EntryRuntime,
    ) -> CandidateBuildResult:
        symbols = tuple(dict.fromkeys(str(symbol).upper().strip() for symbol in request.symbols if str(symbol or "").strip()))
        if not symbols:
            return CandidateBuildResult(
                run_ref=request.run_ref,
                candidate_run_id=self._candidate_run_id(request),
                candidates=(),
                summary={
                    "status": "skipped",
                    "reason": "no_symbols",
                    "symbol_count": 0,
                    "candidate_count": 0,
                },
            )

        runtime = entry_runtime_with_symbols(runtime, symbols)
        base_parameters = entry_candidate_build_parameters(
            runtime=runtime,
            symbols=symbols,
            request=request,
            db_target=self.context.db_target,
            config_root=self.context.config_root,
        )

        from core.common import env_or_die
        from core.integrations.alpaca.client import AlpacaClient, infer_trading_base_url
        from core.integrations.calendar_events import build_calendar_event_resolver
        from core.integrations.greeks import build_local_greeks_provider

        key_id = env_or_die("APCA_API_KEY_ID", "ALPACA_API_KEY")
        secret_key = env_or_die("APCA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
        client = AlpacaClient(
            key_id=key_id,
            secret_key=secret_key,
            trading_base_url=infer_trading_base_url(key_id, base_parameters.trading_base_url),
            data_base_url=base_parameters.data_base_url,
        )
        calendar_resolver = build_calendar_event_resolver(
            key_id=key_id,
            secret_key=secret_key,
            data_base_url=base_parameters.data_base_url,
            database_url=self.context.db_target,
        )
        greeks_provider = build_local_greeks_provider()
        try:
            provider = AlpacaMarketSliceProvider(
                client=client,
                greeks_provider=greeks_provider,
            )
            market_slices_by_symbol: dict[str, SymbolMarketSlice] = {}
            for symbol in symbols:
                market_slice_parameters = build_symbol_market_slice_parameters(
                    symbol=symbol,
                    base_parameters=base_parameters,
                    runtimes=[runtime],
                )
                market_slices_by_symbol[symbol] = provider.get_symbol_market_slice(
                    symbol=symbol,
                    parameters=market_slice_parameters,
                )
            benchmark_slices_by_symbol = self._build_benchmark_market_slices(
                benchmark_symbols=DEFAULT_MARKET_BENCHMARK_SYMBOLS,
                base_parameters=base_parameters,
                provider=provider,
                runtime=runtime,
                market_slices_by_symbol=market_slices_by_symbol,
            )
            market_context = self._build_and_persist_market_context(
                benchmark_slices=benchmark_slices_by_symbol,
                request=request,
            )
            market_context_payload = market_context.to_payload()
            candidates_by_owner, diagnostics_by_owner = build_entry_runtime_candidates_with_diagnostics_from_market_slices(
                entry_runtimes=[runtime],
                base_parameters=base_parameters,
                calendar_resolver=calendar_resolver,
                market_slices_by_symbol=market_slices_by_symbol,
                benchmark_slices_by_symbol=benchmark_slices_by_symbol,
                market_context=market_context_payload,
                per_runtime_limit=entry_candidate_limit(request),
            )
        finally:
            calendar_resolver.store.close()

        owner_candidates = candidates_by_owner.get(runtime_owner_key(runtime), {})
        owner_diagnostics = tuple(dict(row) for row in diagnostics_by_owner.get(runtime_owner_key(runtime), ()))
        flattened = tuple(dict(row) for rows in owner_candidates.values() for row in list(rows or []))
        return CandidateBuildResult(
            run_ref=request.run_ref,
            candidate_run_id=self._candidate_run_id(request),
            candidates=flattened,
            diagnostics=owner_diagnostics,
            summary={
                "status": "completed",
                "symbol_count": len(symbols),
                "candidate_count": len(flattened),
                "symbol_candidate_counts": {str(symbol): len(list(rows or [])) for symbol, rows in sorted(owner_candidates.items())},
                "label": entry_engine_label(runtime),
                "candidate_builder": runtime.build_settings.candidate_builder_key,
                "build_profile": runtime.build_settings.build_profile,
                "greeks_source": base_parameters.greeks_source,
                "market_context": market_context_payload,
            },
            market_context=market_context,
        )

    def declare_capture_targets(
        self,
        requests: Sequence[CaptureTargetRequest],
    ) -> CaptureTargetDeclaration:
        capture_store = self.context.storage.capture
        if not capture_store.target_schema_ready():
            return CaptureTargetDeclaration(
                status="skipped",
                reason="capture_schema_unavailable",
                request_count=0,
                target_counts={},
            )

        request_rows = [request for request in list(requests or []) if isinstance(request, CaptureTargetRequest)]
        counts: dict[str, int] = {}
        now = datetime.now(UTC)
        for request in request_rows:
            expires_at = utc_expiry_iso(from_time=now, seconds=request.ttl_seconds)
            rows = [
                {
                    "option_symbol": symbol,
                    "underlying_symbol": request.metadata.get("underlying_symbol"),
                    "strategy": request.metadata.get("strategy"),
                    "leg_role": request.metadata.get("leg_role") or "contract",
                    "quote_enabled": request.metadata.get("quote_enabled", True),
                    "trade_enabled": request.metadata.get("trade_enabled", False),
                    "feed": request.metadata.get("feed") or "opra",
                    "data_base_url": request.metadata.get("data_base_url"),
                    "expires_at": expires_at,
                    "priority": request.priority,
                    "metadata": dict(request.metadata),
                }
                for symbol in request.symbols
            ]
            persisted = capture_store.replace_capture_targets(
                owner_kind=request.owner_type,
                owner_key=request.owner_id,
                reason=request.reason,
                priority=request.priority,
                rows=rows,
            )
            counts[request.reason] = counts.get(request.reason, 0) + len(persisted)
        return CaptureTargetDeclaration(
            status="ok",
            request_count=len(request_rows),
            target_counts=counts,
        )

    def _resolved_ticker_set(
        self,
        *,
        source: TickerSourceSpec,
        symbols: tuple[str, ...],
        resolved_at: datetime,
        ticker_source_run_id: str | None = None,
        reason_codes: tuple[str, ...] = (),
        blockers: tuple[str, ...] = (),
        evidence: Mapping[str, Any] | None = None,
    ) -> ResolvedTickerSet:
        normalized = tuple(dict.fromkeys(str(symbol).upper().strip() for symbol in symbols if str(symbol or "").strip()))
        if source.max_symbols is not None:
            normalized = normalized[: max(int(source.max_symbols), 0)]
        resolved_blockers = tuple(blockers)
        if not normalized and not resolved_blockers:
            resolved_blockers = ("no_symbols",)
        return ResolvedTickerSet(
            symbols=normalized,
            source=source,
            resolved_at=resolved_at,
            ticker_source_run_id=ticker_source_run_id,
            reason_codes=tuple(reason_codes),
            blockers=resolved_blockers,
            evidence=dict(evidence or {}),
        )

    @staticmethod
    def _fallback_universe_ref(source: TickerSourceSpec) -> str | None:
        fallback_ref = source.fallback.universe_ref
        if fallback_ref in (None, ""):
            return None
        return str(fallback_ref)

    @staticmethod
    def _candidate_run_id(request: CandidateBuildRequest) -> str:
        return f"candidate_run:{request.run_ref.run_id}"

    def _build_benchmark_market_slices(
        self,
        *,
        benchmark_symbols: tuple[str, ...],
        base_parameters: CandidateBuildParameters,
        provider: AlpacaMarketSliceProvider,
        runtime: EntryRuntime,
        market_slices_by_symbol: Mapping[str, SymbolMarketSlice],
    ) -> dict[str, SymbolMarketSlice]:
        benchmark_slices: dict[str, SymbolMarketSlice] = {}
        for symbol in benchmark_symbols:
            benchmark_symbol = str(symbol or "").upper().strip()
            if not benchmark_symbol:
                continue
            existing = market_slices_by_symbol.get(benchmark_symbol)
            if existing is not None:
                benchmark_slices[benchmark_symbol] = existing
                continue
            benchmark_parameters = build_symbol_market_slice_parameters(
                symbol=benchmark_symbol,
                base_parameters=base_parameters,
                runtimes=[runtime],
            )
            benchmark_slices[benchmark_symbol] = provider.get_symbol_market_slice(
                symbol=benchmark_symbol,
                parameters=benchmark_parameters,
            )
        return benchmark_slices

    def _build_and_persist_market_context(
        self,
        *,
        benchmark_slices: Mapping[str, SymbolMarketSlice],
        request: CandidateBuildRequest,
    ) -> MarketContextSnapshot:
        snapshot = self.build_market_context(
            benchmark_slices=benchmark_slices,
            request=MarketContextRequest(
                benchmark_symbols=DEFAULT_MARKET_BENCHMARK_SYMBOLS,
                metadata={
                    "candidate_run_id": self._candidate_run_id(request),
                    "run_id": request.run_ref.run_id,
                    "trading_strategy_id": request.trading_strategy_id,
                    "trade_structure": request.trade_structure,
                    "job_run_id": request.run_ref.job_run_id,
                    "source_id": request.run_ref.source_id,
                },
            ),
        )
        engine_facts = self.context.storage.engine_facts
        if not engine_facts.market_context_schema_ready():
            return snapshot
        row = engine_facts.upsert_market_context_snapshot(snapshot)
        snapshot_id = row.get("market_context_snapshot_id")
        if snapshot_id in (None, ""):
            return snapshot
        return snapshot.model_copy(update={"snapshot_id": str(snapshot_id)})


__all__ = [
    "DataEngine",
    "entry_candidate_build_parameters",
    "entry_candidate_limit",
    "entry_engine_label",
    "entry_engine_strategy_run_id",
    "entry_runtime_with_symbols",
    "ticker_source_spec_from_strategy_source",
]
