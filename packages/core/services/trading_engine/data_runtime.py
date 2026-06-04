from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

from core.services.live_pipelines import build_live_snapshot_label
from core.services.opportunity_generation import build_trading_strategy_run_id
from core.services.scanners.config import parse_args as parse_scanner_args
from core.services.strategy_builders import build_entry_runtime_candidates, runtime_owner_key
from core.services.ticker_sources import resolve_ticker_source_symbols
from core.services.trading_engine.data import CaptureTargetRequest, CandidateBuildRequest, CandidateBuildResult, ResolvedTickerSet, TickerSourceSpec
from core.services.trading_engine.kernel import EngineContext
from core.services.trading_strategies import StrategySource, load_universe_symbols
from core.services.trading_strategy_runtime import EntryRuntime

DEFAULT_ENTRY_CANDIDATE_LIMIT = 10
DEFAULT_GREEKS_SOURCE = "auto"


def ticker_source_spec_from_strategy_source(source: StrategySource) -> TickerSourceSpec:
    fallback: dict[str, Any] = {}
    if source.fallback_universe_ref is not None:
        fallback["universe_ref"] = source.fallback_universe_ref
    return TickerSourceSpec(
        source_type=source.kind,
        ref=source.ref,
        max_age_seconds=source.max_age_seconds,
        max_symbols=source.max_symbols,
        fallback=fallback,
    )


def entry_runtime_with_symbols(runtime: EntryRuntime, symbols: tuple[str, ...]) -> EntryRuntime:
    normalized = tuple(dict.fromkeys(str(symbol).upper().strip() for symbol in symbols if str(symbol or "").strip()))
    return replace(runtime, strategy=replace(runtime.strategy, symbols=normalized))


def entry_engine_label(runtime: EntryRuntime) -> str:
    return f"trading_strategy:{runtime.trading_strategy_id}:entry"


def entry_engine_strategy_run_id(run_id: str, trading_strategy_id: str) -> str:
    return build_trading_strategy_run_id(run_id, trading_strategy_id)


class PostgresDataEngine:
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
                reason_codes=("static_universe",),
                evidence={
                    "kind": "static",
                    "universe_ref": source.ref,
                    "summary": {"symbol_count": len(symbols)},
                },
            )

        if source_type == "dynamic":
            job_key = f"ticker_source:{source.ref}"
            snapshot = resolve_ticker_source_symbols(
                self.context.storage.jobs,
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
                source_run_id=None if snapshot.get("job_run_id") in (None, "") else str(snapshot["job_run_id"]),
                reason_codes=(f"ticker_source_{status or 'missing'}",),
                blockers=() if symbols and status in {"ready", "fallback"} else (reason,),
                evidence=dict(snapshot),
            )

        raise ValueError(f"Unsupported ticker source type: {source.source_type}")

    def build_trade_candidates(
        self,
        request: CandidateBuildRequest,
    ) -> CandidateBuildResult:
        runtime = request.build_policy.get("entry_runtime")
        if not isinstance(runtime, EntryRuntime):
            raise ValueError("CandidateBuildRequest.build_policy.entry_runtime is required")
        return self.build_entry_trade_candidates(
            request=request,
            runtime=runtime,
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
        base_scanner_args = self._entry_scanner_args(
            runtime=runtime,
            symbols=symbols,
            request=request,
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
            trading_base_url=infer_trading_base_url(key_id, base_scanner_args.trading_base_url),
            data_base_url=base_scanner_args.data_base_url,
        )
        calendar_resolver = build_calendar_event_resolver(
            key_id=key_id,
            secret_key=secret_key,
            data_base_url=base_scanner_args.data_base_url,
            database_url=self.context.db_target,
        )
        greeks_provider = build_local_greeks_provider()
        try:
            candidates_by_owner = build_entry_runtime_candidates(
                entry_runtimes=[runtime],
                base_scanner_args=base_scanner_args,
                client=client,
                calendar_resolver=calendar_resolver,
                greeks_provider=greeks_provider,
                per_runtime_limit=self._candidate_limit(request),
                history_store=self.context.storage.history,
                session_label=entry_engine_label(runtime),
            )
        finally:
            calendar_resolver.store.close()

        owner_candidates = candidates_by_owner.get(runtime_owner_key(runtime), {})
        flattened = tuple(dict(row) for rows in owner_candidates.values() for row in list(rows or []))
        return CandidateBuildResult(
            run_ref=request.run_ref,
            candidate_run_id=self._candidate_run_id(request),
            candidates=flattened,
            summary={
                "status": "completed",
                "symbol_count": len(symbols),
                "candidate_count": len(flattened),
                "symbol_candidate_counts": {str(symbol): len(list(rows or [])) for symbol, rows in sorted(owner_candidates.items())},
                "label": entry_engine_label(runtime),
                "scanner_strategy": runtime.build_settings.scanner_strategy,
                "scanner_profile": runtime.build_settings.scanner_profile,
                "greeks_source": getattr(base_scanner_args, "greeks_source", DEFAULT_GREEKS_SOURCE),
            },
        )

    def declare_capture_targets(
        self,
        requests: Any,
    ) -> Mapping[str, Any]:
        capture_store = self.context.storage.capture
        if not capture_store.target_schema_ready():
            return {"status": "skipped", "reason": "capture_schema_unavailable"}

        request_rows = [request for request in list(requests or []) if isinstance(request, CaptureTargetRequest)]
        counts: dict[str, int] = {}
        now = datetime.now(UTC)
        for request in request_rows:
            expires_at = (now + timedelta(seconds=max(int(request.ttl_seconds), 1))).isoformat(timespec="seconds").replace("+00:00", "Z")
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
        return {
            "status": "ok",
            "request_count": len(request_rows),
            "target_counts": counts,
        }

    def _resolved_ticker_set(
        self,
        *,
        source: TickerSourceSpec,
        symbols: tuple[str, ...],
        resolved_at: datetime,
        source_run_id: str | None = None,
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
            source_run_id=source_run_id,
            reason_codes=tuple(reason_codes),
            blockers=resolved_blockers,
            evidence=dict(evidence or {}),
        )

    @staticmethod
    def _fallback_universe_ref(source: TickerSourceSpec) -> str | None:
        fallback_ref = source.fallback.get("universe_ref") if isinstance(source.fallback, Mapping) else None
        if fallback_ref in (None, ""):
            return None
        return str(fallback_ref)

    @staticmethod
    def _candidate_limit(request: CandidateBuildRequest) -> int:
        policy = request.build_policy
        for key in ("per_runtime_limit", "top", "candidate_limit"):
            value = policy.get(key)
            if value in (None, ""):
                continue
            return max(int(value), 1)
        return DEFAULT_ENTRY_CANDIDATE_LIMIT

    @staticmethod
    def _candidate_run_id(request: CandidateBuildRequest) -> str:
        return f"candidate_run:{request.run_ref.run_id}"

    def _entry_scanner_args(
        self,
        *,
        runtime: EntryRuntime,
        symbols: tuple[str, ...],
        request: CandidateBuildRequest,
    ) -> Any:
        args = parse_scanner_args([])
        args.symbol = None
        args.symbols = ",".join(symbols)
        args.symbols_file = None
        args.universe = None
        args.strategy = runtime.build_settings.scanner_strategy
        args.profile = runtime.build_settings.scanner_profile
        args.greeks_source = str(request.build_policy.get("greeks_source") or DEFAULT_GREEKS_SOURCE)
        args.top = self._candidate_limit(request)
        args.per_symbol_top = max(int(request.build_policy.get("per_symbol_top") or 1), 1)
        args.history_db = self.context.db_target
        args.session_label = build_live_snapshot_label(
            universe_label=entry_engine_label(runtime),
            strategy=runtime.build_settings.scanner_strategy,
            profile=runtime.build_settings.scanner_profile,
            greeks_source=args.greeks_source,
        )
        return args


__all__ = [
    "PostgresDataEngine",
    "entry_engine_label",
    "entry_engine_strategy_run_id",
    "entry_runtime_with_symbols",
    "ticker_source_spec_from_strategy_source",
]
