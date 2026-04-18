from __future__ import annotations

import asyncio
import os
import unittest
from argparse import Namespace
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from core.jobs.orchestration import isoformat_utc
from core.jobs.scheduler import _enqueue_definition_jobs, _reconcile_live_collector_jobs
from core.services.automation_runtime import resolve_entry_runtime
from core.services.collections.capture.runtime import capture_live_option_market_state
from core.services.collections.cycle import run_collection_cycle
from core.services.collections.models import LiveCaptureSnapshot, LiveTickContext
from core.services.bot_analytics import summarize_intent_counts
from core.services.bots import load_active_bots
from core.services.decision_engine import run_entry_automation_decision
from core.services.execution_intents import (
    PRE_DISPATCH_EXPIRE_REASON,
    dispatch_pending_execution_intents,
)
from core.services.live_collector_health.capture import (
    build_quote_capture_summary,
    build_trade_capture_summary,
)
from core.services.opportunity_generation import sync_entry_runtime_opportunities
from core.services.ops.jobs import build_job_run_view
from core.services.pipelines import get_pipeline_detail, list_pipelines
from core.services.live_recovery import LIVE_SLOT_STATUS_MISSED
from core.services.risk_manager import evaluate_open_execution
from core.services.strategy_positions import run_management_automation_decision
from core.services.uoa_state import get_latest_uoa_state
from core.storage.collector_repository import CollectorRepository
from core.storage.serializers import parse_datetime


def _candidate_payload(symbol: str = "AAPL") -> dict[str, object]:
    return {
        "run_id": "run-1",
        "underlying_symbol": symbol,
        "strategy": "call_debit",
        "profile": "weekly",
        "expiration_date": "2026-04-24",
        "days_to_expiration": 8,
        "quality_score": 90.0,
        "setup_score": 86.0,
        "setup_intraday_score": 87.0,
        "setup_status": "favorable",
        "fill_ratio": 0.94,
        "data_status": "clean",
        "calendar_status": "clean",
        "earnings_phase": "pre_event_runup",
        "earnings_timing_confidence": "high",
        "expected_move_pct": 0.028,
        "debit_width_ratio": 0.35,
        "short_symbol": f"{symbol}260424C210",
        "long_symbol": f"{symbol}260424C205",
        "short_strike": 210.0,
        "long_strike": 205.0,
        "short_bid": 1.8,
        "short_ask": 2.0,
        "long_bid": 3.5,
        "long_ask": 3.7,
        "short_midpoint": 1.9,
        "long_midpoint": 3.6,
        "short_bid_size": 25,
        "short_ask_size": 22,
        "long_bid_size": 24,
        "long_ask_size": 21,
        "short_relative_spread": 0.05,
        "long_relative_spread": 0.04,
        "short_open_interest": 1500,
        "long_open_interest": 1400,
        "short_volume": 900,
        "long_volume": 850,
        "short_implied_volatility": 0.34,
        "long_implied_volatility": 0.36,
        "midpoint_credit": 1.1,
        "return_on_risk": 0.24,
        "max_loss": 250.0,
    }


def _same_slot_capture_snapshot(symbol: str = "AAPL") -> LiveCaptureSnapshot:
    return LiveCaptureSnapshot(
        candidates=[_candidate_payload(symbol)],
        contract_metadata_by_symbol={},
        expected_quote_symbols=[f"{symbol}260424C205", f"{symbol}260424C210"],
        expected_trade_symbols=[f"{symbol}260424C205"],
        expected_uoa_roots=[symbol],
        quote_event_count=4,
        baseline_quote_event_count=2,
        stream_quote_event_count=2,
        recovery_quote_event_count=0,
        trade_event_count=3,
        stream_trade_event_count=3,
        latest_quote_records=[],
        stream_quote_records=[],
        recovery_quote_records=[],
        stream_trade_records=[],
        reactive_quote_records=[],
        quote_capture=build_quote_capture_summary(
            expected_quote_symbols=[f"{symbol}260424C205", f"{symbol}260424C210"],
            total_quote_events_saved=4,
            baseline_quote_events_saved=2,
            stream_quote_events_saved=2,
            recovery_quote_events_saved=0,
        ),
        trade_capture=build_trade_capture_summary(
            expected_trade_symbols=[f"{symbol}260424C205"],
            total_trade_events_saved=3,
            stream_trade_events_saved=3,
        ),
        uoa_summary={"overview": {"scoreable_trade_count": 3}},
        uoa_quote_summary={
            "roots": {
                symbol: {
                    "average_quality_score": 0.92,
                    "quality_state": "strong",
                    "fresh_contract_count": 4,
                    "liquid_contract_count": 4,
                }
            }
        },
        uoa_decisions={
            "overview": {"root_count": 1, "promotable_count": 1},
            "roots": [
                {
                    "underlying_symbol": symbol,
                    "decision_state": "promotable",
                    "current": {
                        "dominant_flow": "call",
                        "dominant_flow_ratio": 0.9,
                        "supporting_volume_oi_ratio": 1.2,
                        "max_volume_oi_ratio": 1.4,
                    },
                    "deltas": {
                        "max_premium_rate_ratio": 2.6,
                        "max_trade_rate_ratio": 1.8,
                    },
                }
            ],
        },
        stream_quote_error=None,
        stream_trade_error=None,
    )


class _CollectorStore:
    def __init__(self) -> None:
        self.saved_cycle: dict[str, object] | None = None

    def save_cycle(self, **kwargs: object) -> list[dict[str, object]]:
        self.saved_cycle = dict(kwargs)
        return [dict(row) for row in list(kwargs.get("opportunities") or [])]


class _SignalStore:
    pass


class _AutomationRuntimeSignalStore:
    def __init__(self) -> None:
        self.automation_runs: list[dict[str, object]] = []
        self.opportunities: list[dict[str, object]] = []
        self.expire_calls: list[dict[str, object]] = []

    def automation_runtime_schema_ready(self) -> bool:
        return True

    def upsert_automation_run(self, **kwargs: object) -> dict[str, object]:
        row = dict(kwargs)
        self.automation_runs.append(row)
        return row

    def list_automation_runs(self, **_: object) -> list[dict[str, object]]:
        return list(self.automation_runs)

    def list_opportunities(self, **kwargs: object) -> list[dict[str, object]]:
        automation_run_id = kwargs.get("automation_run_id")
        rows = list(self.opportunities)
        if automation_run_id is not None:
            rows = [
                row for row in rows if row.get("automation_run_id") == automation_run_id
            ]
        return rows

    def upsert_opportunity(self, **kwargs: object) -> tuple[dict[str, object], bool]:
        row = dict(kwargs)
        self.opportunities.append(row)
        return row, True

    def expire_absent_opportunities(self, **kwargs: object) -> list[dict[str, object]]:
        self.expire_calls.append(dict(kwargs))
        return []


class _DecisionSignalStore:
    def __init__(
        self, *, scoped_row: dict[str, object], generic_row: dict[str, object]
    ) -> None:
        self.scoped_row = dict(scoped_row)
        self.generic_row = dict(generic_row)
        self.list_calls: list[dict[str, object]] = []
        self.decisions: list[dict[str, object]] = []

    def schema_ready(self) -> bool:
        return True

    def decision_schema_ready(self) -> bool:
        return True

    def list_opportunities(self, **kwargs: object) -> list[dict[str, object]]:
        self.list_calls.append(dict(kwargs))
        if bool(kwargs.get("runtime_owned")):
            return [dict(self.scoped_row)]
        return [dict(self.generic_row)]

    def upsert_opportunity_decision(self, **kwargs: object) -> dict[str, object]:
        row = dict(kwargs)
        self.decisions.append(row)
        return row


class _DecisionExecutionStore:
    def __init__(self) -> None:
        self.upserted_intents: list[dict[str, object]] = []
        self.intent_events: list[dict[str, object]] = []

    def intent_schema_ready(self) -> bool:
        return True

    def list_execution_intents(self, **_: object) -> list[dict[str, object]]:
        return []

    def upsert_execution_intent(self, **kwargs: object) -> dict[str, object]:
        row = dict(kwargs)
        self.upserted_intents.append(row)
        return row

    def append_execution_intent_event(self, **kwargs: object) -> dict[str, object]:
        row = dict(kwargs)
        self.intent_events.append(row)
        return row


class _DecisionJobStore:
    def list_job_definitions(self, **_: object) -> list[dict[str, object]]:
        return []


class _DecisionStorage:
    def __init__(self, *, signals: object, execution: object, jobs: object) -> None:
        self.signals = signals
        self.execution = execution
        self.jobs = jobs


class _RepositoryCapabilities:
    def has_tables(self, *_: object) -> bool:
        return True


class _TrackingSession:
    def __init__(self) -> None:
        self.merged: list[object] = []

    def merge(self, value: object) -> None:
        self.merged.append(value)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


class _CollectorRepositoryWithoutLegacyPipelineWrites(CollectorRepository):
    def __init__(self, tracking_session: _TrackingSession) -> None:
        super().__init__(
            engine=object(),
            session_factory=lambda: tracking_session,
            capabilities=_RepositoryCapabilities(),
        )

    def list_cycle_candidates(
        self,
        cycle_id: str,
        selection_state: str | None = None,
        *,
        eligibility: str | None = None,
    ) -> list[dict[str, object]]:
        del cycle_id, selection_state, eligibility
        return []


class _EventStore:
    def schema_ready(self) -> bool:
        return False


class _AlertStore:
    pass


class _HistoryStore:
    def __init__(self) -> None:
        self.saved_quote_batches: list[list[dict[str, object]]] = []
        self.saved_trade_batches: list[list[dict[str, object]]] = []

    def schema_has_tables(self, *_: object) -> bool:
        return True

    def save_option_quote_events(
        self,
        *,
        cycle_id: str,
        label: str,
        profile: str,
        quotes: list[dict[str, object]],
    ) -> int:
        del cycle_id, label, profile
        self.saved_quote_batches.append([dict(row) for row in quotes])
        return len(quotes)

    def save_option_trade_events(
        self,
        *,
        cycle_id: str,
        label: str,
        profile: str,
        trades: list[dict[str, object]],
    ) -> int:
        del cycle_id, label, profile
        self.saved_trade_batches.append([dict(row) for row in trades])
        return len(trades)


class _RecoveryStore:
    def schema_ready(self) -> bool:
        return True


class _JobStoreThatMustNotServeStaleContext:
    def get_latest_live_collector_run(self, **_: object) -> dict[str, object] | None:
        raise AssertionError("collector should not score off the previous live run")


class _PipelineCollectorStore:
    def __init__(self) -> None:
        self.list_cycle_candidates_calls = 0

    def pipeline_schema_ready(self) -> bool:
        return True

    def get_cycle(self, cycle_id: str) -> dict[str, object] | None:
        del cycle_id
        return self.get_latest_cycle("explore_10_call_debit_weekly_auto")

    def get_pipeline(self, pipeline_id: str) -> dict[str, object] | None:
        return {
            "pipeline_id": pipeline_id,
            "label": "explore_10_call_debit_weekly_auto",
            "name": "Explore 10 Call Debit Weekly Auto",
            "style_profile": "active",
            "default_horizon_intent": "short_dated",
            "product_scope_json": {"product_class": "equity_options"},
            "policy_json": {"strategy_mode": "call_debit"},
            "updated_at": "2026-04-15T08:00:00Z",
        }

    def get_latest_cycle(self, label: str) -> dict[str, object] | None:
        del label
        return {
            "cycle_id": "cycle-live",
            "label": "explore_10_call_debit_weekly_auto",
            "session_date": "2026-04-15",
            "generated_at": "2026-04-15T14:35:00Z",
            "universe_label": "explore_10",
            "strategy": "call_debit",
            "profile": "weekly",
            "greeks_source": "auto",
            "symbols": ["MSFT"],
            "failures": [],
            "selection_memory": {},
        }

    def list_cycles(
        self, label: str, session_date: str | None = None, limit: int = 100
    ) -> list[dict[str, object]]:
        del label, limit
        if session_date not in {None, "2026-04-15"}:
            return []
        return [self.get_latest_cycle("explore_10_call_debit_weekly_auto")]

    def list_pipeline_cycles(
        self, *, pipeline_id: str, market_date: str | None = None, limit: int = 100
    ) -> list[dict[str, object]]:
        del limit
        row = self.get_latest_cycle("explore_10_call_debit_weekly_auto")
        if row is not None and market_date not in {None, str(row["session_date"])}:
            return []
        if row is not None:
            row = {
                **row,
                "pipeline_id": pipeline_id,
                "market_date": row["session_date"],
            }
        return [] if row is None else [row]

    def list_cycle_candidates(self, cycle_id: str) -> list[dict[str, object]]:
        del cycle_id
        self.list_cycle_candidates_calls += 1
        return [
            {
                "candidate_id": 1,
                "selection_state": "promotable",
                "selection_rank": 1,
                "eligibility": "live",
                "candidate": {
                    "underlying_symbol": "AAPL",
                    "strategy": "call_credit",
                },
            }
        ]

    def list_events(self, **_: object) -> list[dict[str, object]]:
        return []

    def list_cycle_events(self, cycle_id: str) -> list[dict[str, object]]:
        del cycle_id
        return []


class _PipelineSignalStore:
    def schema_ready(self) -> bool:
        return True

    def list_active_cycle_opportunities(
        self,
        cycle_id: str,
        *,
        eligibility_state: str | None = None,
        exclude_consumed: bool = True,
        limit: int = 200,
    ) -> list[dict[str, object]]:
        del cycle_id, eligibility_state, exclude_consumed, limit
        return [
            {
                "opportunity_id": "opp-live",
                "pipeline_id": "pipeline:explore_10_call_debit_weekly_auto",
                "market_date": "2026-04-15",
                "session_date": "2026-04-15",
                "cycle_id": "cycle-live",
                "selection_state": "promotable",
                "selection_rank": 1,
                "eligibility_state": "live",
                "strategy_family": "call_debit_spread",
                "candidate": {
                    "underlying_symbol": "MSFT",
                    "strategy": "call_debit",
                },
                "source_candidate_id": 42,
            }
        ]

    def list_opportunities(self, **_: object) -> list[dict[str, object]]:
        return []

    def count_active_cycle_opportunities_by_cycle_ids(
        self, cycle_ids: list[str], *, exclude_consumed: bool = True
    ) -> dict[str, dict[str, int]]:
        del exclude_consumed
        if cycle_ids != ["cycle-live"]:
            return {}
        return {
            "cycle-live": {
                "candidate_count": 1,
                "promotable": 1,
                "monitor": 0,
            }
        }


class _PipelineJobStore:
    def _latest_run(self) -> dict[str, object]:
        return {
            "job_run_id": "job-run-live",
            "job_key": "live_collector:explore_10_call_debit_weekly_auto",
            "job_type": "live_collector",
            "status": "succeeded",
            "scheduled_for": "2026-04-15T14:35:00Z",
            "started_at": "2026-04-15T14:35:01Z",
            "finished_at": "2026-04-15T14:35:15Z",
            "session_id": "live:explore_10_call_debit_weekly_auto:2026-04-15",
            "slot_at": "2026-04-15T14:35:00Z",
            "worker_name": "worker",
            "payload": {
                "label": "explore_10_call_debit_weekly_auto",
                "session_date": "2026-04-15",
            },
            "result": {
                "cycle_id": "cycle-live",
                "quote_capture": {},
                "trade_capture": {},
                "uoa_summary": {},
                "uoa_quote_summary": {},
                "uoa_decisions": {},
                "selection_summary": {"promotable_count": 1, "monitor_count": 0},
            },
        }

    def list_job_definitions(self, **_: object) -> list[dict[str, object]]:
        return [
            {
                "job_key": "live_collector:explore_10_call_debit_weekly_auto",
                "job_type": "live_collector",
                "enabled": True,
                "payload": {
                    "universe": "explore_10",
                    "strategy": "call_debit",
                    "profile": "weekly",
                    "greeks_source": "auto",
                    "execution_policy": {
                        "enabled": True,
                        "deployment_mode": "paper_auto",
                        "mode": "top_promotable",
                    },
                },
            }
        ]

    def list_job_runs(self, **_: object) -> list[dict[str, object]]:
        return [self._latest_run()]

    def get_latest_live_collector_run(self, **_: object) -> dict[str, object] | None:
        return self._latest_run()

    def list_latest_runs_by_session_ids(
        self,
        *,
        session_ids: list[str],
        job_type: str | None = None,
        statuses: list[str] | None = None,
    ) -> list[dict[str, object]]:
        del job_type, statuses
        run = self._latest_run()
        return [run] if str(run["session_id"]) in session_ids else []

    def get_live_collector_run_by_cycle_id(
        self,
        *,
        cycle_id: str,
        label: str | None = None,
        status: str | None = "succeeded",
    ) -> dict[str, object] | None:
        del label
        if cycle_id != "cycle-live" or status not in {"succeeded", None}:
            return None
        return self._latest_run()


class _PipelineAlertStore:
    def count_alert_events_by_session_keys(
        self, session_keys: list[tuple[str, str]]
    ) -> dict[tuple[str, str], int]:
        del session_keys
        return {}

    def list_alert_events(self, **_: object) -> list[dict[str, object]]:
        return []


class _PipelinePostMarketStore:
    def get_latest_run(self, **_: object) -> None:
        return None


class _PipelineRecoveryStore:
    def schema_ready(self) -> bool:
        return False


class _PipelineExecutionStore:
    pass


class _PipelineStorage:
    def __init__(self) -> None:
        self.collector = _PipelineCollectorStore()
        self.jobs = _PipelineJobStore()
        self.alerts = _PipelineAlertStore()
        self.post_market = _PipelinePostMarketStore()
        self.execution = _PipelineExecutionStore()
        self.recovery = _PipelineRecoveryStore()
        self.signals = _PipelineSignalStore()
        self.risk = None


class LiveCollectorArchitectureE2ETests(unittest.TestCase):
    def test_collection_cycle_writes_automation_scoped_runtime_opportunities(
        self,
    ) -> None:
        collector_store = _CollectorStore()
        signal_store = _AutomationRuntimeSignalStore()
        bot = load_active_bots()["short_dated_index_credit_bot"]
        runtime = next(
            item
            for item in bot.automations
            if item.automation.automation_id == "index_put_credit_entry"
        )
        symbol = runtime.symbols[0]
        candidate = {
            **_candidate_payload(symbol),
            "strategy": "put_credit",
            "profile": "weekly",
            "short_delta": 0.22,
            "width": 2.0,
            "short_open_interest": 1200,
            "long_open_interest": 1100,
            "short_relative_spread": 0.05,
            "long_relative_spread": 0.04,
        }
        args = Namespace(
            strategy="put_credit",
            profile="weekly",
            greeks_source="local",
            top=5,
            history_db="postgresql://example",
            execution_policy=None,
            quote_capture_seconds=0,
            trade_capture_seconds=0,
            session_end_offset_minutes=0,
            options_automation_scope={
                "enabled": True,
                "symbols": tuple(runtime.symbols),
                "entry_runtimes": [(bot, runtime)],
            },
        )
        scanner_args = Namespace(feed="opra", data_base_url="https://data.example")
        selection_payload = {
            "symbol_candidates": {symbol: [dict(candidate)]},
            "promotable_candidates": [dict(candidate)],
            "monitor_candidates": [],
            "opportunities": [
                {
                    **candidate,
                    "selection_state": "promotable",
                    "selection_rank": 1,
                    "state_reason": "selected_promotable",
                    "origin": "live_scan",
                    "eligibility": "live",
                    "candidate": dict(candidate),
                }
            ],
            "selection_memory": {},
            "events": [],
        }
        runtime_candidate_rows_by_owner = {
            (bot.bot.bot_id, runtime.automation.automation_id): {
                symbol: [dict(candidate)]
            }
        }

        with (
            patch(
                "core.services.collections.cycle.run_universe_cycle",
                return_value=([symbol], "liquid_index_etfs", [], [], []),
            ),
            patch(
                "core.services.collections.cycle.build_symbol_strategy_candidates",
                return_value={symbol: [candidate]},
            ),
            patch(
                "core.services.collections.cycle.capture_live_option_market_state",
                return_value=_same_slot_capture_snapshot(symbol),
            ),
            patch(
                "core.services.collections.cycle.read_previous_selection",
                return_value=({}, {}),
            ),
            patch(
                "core.services.collections.cycle.build_entry_runtime_candidates",
                return_value=runtime_candidate_rows_by_owner,
            ),
            patch(
                "core.services.collections.cycle.select_live_opportunities",
                return_value=selection_payload,
            ),
            patch(
                "core.services.opportunity_generation.select_live_opportunities",
                return_value=selection_payload,
            ),
            patch(
                "core.services.collections.cycle.sync_live_collector_signal_layer",
                return_value={
                    "signal_states_upserted": 0,
                    "signal_transitions_recorded": 0,
                    "opportunities_upserted": 0,
                    "opportunities_expired": 0,
                },
            ),
            patch(
                "core.services.collections.cycle.dispatch_cycle_alerts",
                return_value=[],
            ),
        ):
            result = run_collection_cycle(
                args,
                tick_context=None,
                scanner_args=scanner_args,
                client=object(),
                history_store=_HistoryStore(),
                alert_store=_AlertStore(),
                job_store=_JobStoreThatMustNotServeStaleContext(),
                collector_store=collector_store,
                event_store=_EventStore(),
                signal_store=signal_store,
                recovery_store=None,
                calendar_resolver=object(),
                greeks_provider=object(),
                emit_output=False,
            )

        self.assertEqual(result["automation_runs_upserted"], 1)
        self.assertGreaterEqual(result["runtime_opportunities_upserted"], 1)
        self.assertEqual(signal_store.automation_runs[0]["bot_id"], bot.bot.bot_id)
        self.assertEqual(
            signal_store.opportunities[0]["automation_id"],
            runtime.automation.automation_id,
        )
        self.assertEqual(
            signal_store.opportunities[0]["strategy_config_id"],
            runtime.strategy_config.strategy_config_id,
        )

    def test_collection_cycle_persists_canonical_opportunities_for_automation_scopes(
        self,
    ) -> None:
        collector_store = _CollectorStore()
        signal_store = _AutomationRuntimeSignalStore()
        bot = load_active_bots()["short_dated_index_credit_bot"]
        runtime = next(
            item
            for item in bot.automations
            if item.automation.automation_id == "index_put_credit_entry"
        )
        symbol = runtime.symbols[0]
        candidate = {
            **_candidate_payload(symbol),
            "strategy": "put_credit",
            "profile": "weekly",
            "short_delta": 0.22,
            "width": 2.0,
            "short_open_interest": 1200,
            "long_open_interest": 1100,
            "short_relative_spread": 0.05,
            "long_relative_spread": 0.04,
        }
        args = Namespace(
            strategy="put_credit",
            profile="weekly",
            greeks_source="local",
            top=5,
            history_db="postgresql://example",
            execution_policy=None,
            quote_capture_seconds=0,
            trade_capture_seconds=0,
            session_end_offset_minutes=0,
            options_automation_scope={
                "enabled": True,
                "symbols": tuple(runtime.symbols),
                "entry_runtimes": [(bot, runtime)],
            },
        )
        scanner_args = Namespace(feed="opra", data_base_url="https://data.example")
        collector_selection = {
            "symbol_candidates": {symbol: [dict(candidate)]},
            "promotable_candidates": [dict(candidate)],
            "monitor_candidates": [],
            "opportunities": [
                {
                    **candidate,
                    "selection_state": "promotable",
                    "selection_rank": 1,
                    "state_reason": "selected_promotable",
                    "origin": "live_scan",
                    "eligibility": "live",
                    "candidate": dict(candidate),
                }
            ],
            "selection_memory": {},
            "events": [],
        }
        runtime_selection = {
            "symbol_candidates": {symbol: [dict(candidate)]},
            "promotable_candidates": [],
            "monitor_candidates": [],
            "opportunities": [],
            "selection_memory": {},
            "events": [],
        }
        runtime_candidate_rows_by_owner = {
            (bot.bot.bot_id, runtime.automation.automation_id): {
                symbol: [dict(candidate)]
            }
        }

        with (
            patch(
                "core.services.collections.cycle.run_universe_cycle",
                return_value=([symbol], "liquid_index_etfs", [], [], []),
            ),
            patch(
                "core.services.collections.cycle.build_symbol_strategy_candidates",
                return_value={symbol: [candidate]},
            ),
            patch(
                "core.services.collections.cycle.capture_live_option_market_state",
                return_value=_same_slot_capture_snapshot(symbol),
            ),
            patch(
                "core.services.collections.cycle.read_previous_selection",
                return_value=({}, {}),
            ),
            patch(
                "core.services.collections.cycle.build_entry_runtime_candidates",
                return_value=runtime_candidate_rows_by_owner,
            ),
            patch(
                "core.services.collections.cycle.select_live_opportunities",
                return_value=collector_selection,
            ),
            patch(
                "core.services.opportunity_generation.select_live_opportunities",
                return_value=runtime_selection,
            ),
            patch(
                "core.services.collections.cycle.sync_live_collector_signal_layer",
                return_value={
                    "signal_states_upserted": 1,
                    "signal_transitions_recorded": 1,
                    "opportunities_upserted": 1,
                    "opportunities_expired": 0,
                },
            ) as signal_sync,
            patch(
                "core.services.collections.cycle.dispatch_cycle_alerts",
                return_value=[],
            ),
        ):
            result = run_collection_cycle(
                args,
                tick_context=None,
                scanner_args=scanner_args,
                client=object(),
                history_store=_HistoryStore(),
                alert_store=_AlertStore(),
                job_store=_JobStoreThatMustNotServeStaleContext(),
                collector_store=collector_store,
                event_store=_EventStore(),
                signal_store=signal_store,
                recovery_store=None,
                calendar_resolver=object(),
                greeks_provider=object(),
                emit_output=False,
            )

        self.assertIsNotNone(collector_store.saved_cycle)
        self.assertEqual(len(collector_store.saved_cycle["opportunities"]), 1)
        self.assertEqual(result["selection_summary"]["opportunity_count"], 1)
        self.assertEqual(
            result["automation_summary"]["runtime_selection_summary"][
                "opportunity_count"
            ],
            0,
        )
        self.assertEqual(result["promotable_opportunity_count"], 1)
        signal_sync.assert_called_once()
        self.assertEqual(
            len(signal_sync.call_args.kwargs["persisted_opportunities"]),
            1,
        )

    def test_runtime_sync_fallback_filters_below_floor_return_on_risk(self) -> None:
        signal_store = _AutomationRuntimeSignalStore()
        runtime = resolve_entry_runtime(
            bot_id="short_dated_index_credit_bot",
            automation_id="index_put_credit_entry",
        )
        symbol = runtime.symbols[0]
        low_ror_candidate = {
            **_candidate_payload(symbol),
            "strategy": "put_credit",
            "profile": "weekly",
            "setup_status": "favorable",
            "days_to_expiration": 7,
            "short_delta": 0.22,
            "width": 2.0,
            "short_open_interest": 1200,
            "long_open_interest": 1100,
            "short_relative_spread": 0.05,
            "long_relative_spread": 0.04,
            "return_on_risk": 0.12,
            "short_symbol": f"{symbol}260424P00500000",
            "long_symbol": f"{symbol}260424P00498000",
            "short_strike": 500.0,
            "long_strike": 498.0,
        }
        high_ror_candidate = {
            **low_ror_candidate,
            "return_on_risk": 0.15,
            "short_symbol": f"{symbol}260424P00495000",
            "long_symbol": f"{symbol}260424P00493000",
            "short_strike": 495.0,
            "long_strike": 493.0,
        }
        captured_symbol_candidates: list[dict[str, list[dict[str, object]]]] = []

        def _fake_select_live_opportunities(**kwargs: object) -> dict[str, object]:
            symbol_candidates = {
                str(candidate_symbol): [dict(row) for row in rows]
                for candidate_symbol, rows in dict(
                    kwargs.get("symbol_candidates") or {}
                ).items()
            }
            captured_symbol_candidates.append(symbol_candidates)
            selected_rows = [
                {
                    **dict(row),
                    "selection_state": "monitor",
                    "selection_rank": index,
                    "state_reason": "selected_monitor",
                    "eligibility": "live",
                    "candidate": dict(row),
                }
                for index, row in enumerate(symbol_candidates.get(symbol, []), start=1)
            ]
            return {
                "symbol_candidates": symbol_candidates,
                "promotable_candidates": [],
                "monitor_candidates": [
                    dict(row.get("candidate") or row) for row in selected_rows
                ],
                "opportunities": selected_rows,
                "selection_memory": {},
                "events": [],
            }

        with patch(
            "core.services.opportunity_generation.select_live_opportunities",
            side_effect=_fake_select_live_opportunities,
        ):
            result = sync_entry_runtime_opportunities(
                signal_store=signal_store,
                label="explore_10_put_credit_weekly_auto",
                session_date="2026-04-17",
                generated_at="2026-04-17T17:25:24Z",
                cycle_id="cycle-1",
                entry_runtimes=[runtime],
                symbol_candidates={symbol: [low_ror_candidate, high_ror_candidate]},
                runtime_candidate_rows_by_owner=None,
                persisted_opportunities=[],
                job_run_id=None,
                top_promotable=1,
                top_monitor=2,
            )

        self.assertEqual(len(captured_symbol_candidates), 1)
        self.assertEqual(len(captured_symbol_candidates[0][symbol]), 1)
        self.assertEqual(
            captured_symbol_candidates[0][symbol][0]["return_on_risk"],
            0.15,
        )
        self.assertEqual(result["runtime_opportunities_upserted"], 1)
        self.assertEqual(signal_store.opportunities[0]["candidate"]["return_on_risk"], 0.15)

    def test_save_cycle_does_not_materialize_legacy_pipeline_rows(self) -> None:
        tracking_session = _TrackingSession()
        repo = _CollectorRepositoryWithoutLegacyPipelineWrites(tracking_session)
        candidate = _candidate_payload()

        repo.save_cycle(
            cycle_id="cycle-live",
            label="explore_10_call_debit_weekly_auto",
            generated_at="2026-04-15T14:35:00Z",
            job_run_id="job-run-1",
            session_id="live:explore_10_call_debit_weekly_auto:2026-04-15",
            universe_label="explore_10",
            strategy="call_debit",
            profile="weekly",
            greeks_source="auto",
            symbols=["AAPL"],
            failures=[],
            selection_memory={},
            opportunities=[
                {
                    **candidate,
                    "selection_state": "promotable",
                    "selection_rank": 1,
                    "state_reason": "selected_promotable",
                    "origin": "scanner",
                    "eligibility": "live",
                    "candidate": dict(candidate),
                }
            ],
            events=[],
        )

        merged_type_names = [type(value).__name__ for value in tracking_session.merged]
        self.assertEqual(merged_type_names, ["CollectorCycleModel"])

    def test_execute_job_terminalizes_inactive_runtime_opportunities(self) -> None:
        stale_updated_at = (datetime.now(UTC) - timedelta(minutes=30)).isoformat()
        opportunity_id = (
            "opportunity:short_dated_index_credit_bot:index_put_credit_entry:"
            "2026-04-17:GLD:legs"
        )

        class _SignalStore:
            def __init__(self) -> None:
                self.rows = [
                    {
                        "opportunity_id": opportunity_id,
                        "label": "explore_10_put_credit_weekly_auto",
                        "lifecycle_state": "candidate",
                        "updated_at": stale_updated_at,
                        "consumed_by_execution_attempt_id": None,
                        "reason_codes": ["selected_monitor"],
                    }
                ]
                self.expired_ids: list[str] = []

            def schema_ready(self) -> bool:
                return True

            def list_opportunities(self, **_: object) -> list[dict[str, object]]:
                return [dict(row) for row in self.rows]

            def expire_opportunity(
                self,
                opportunity_id: str,
                *,
                expired_at: str,
                reason_code: str = "expired_manual",
            ) -> dict[str, object] | None:
                for row in self.rows:
                    if row["opportunity_id"] != opportunity_id:
                        continue
                    reasons = [str(value) for value in row.get("reason_codes") or []]
                    if reason_code not in reasons:
                        reasons.append(reason_code)
                    row["reason_codes"] = reasons
                    row["lifecycle_state"] = "expired"
                    row["updated_at"] = expired_at
                    row["expires_at"] = expired_at
                    self.expired_ids.append(opportunity_id)
                    return dict(row)
                return None

            def delete_opportunity(self, opportunity_id: str) -> bool:
                raise AssertionError(
                    f"cleanup must not delete opportunity {opportunity_id}"
                )

        class _ExecutionStore:
            def intent_schema_ready(self) -> bool:
                return True

            def list_execution_intents(self, **_: object) -> list[dict[str, object]]:
                return []

        class _JobStore:
            def schema_ready(self) -> bool:
                return True

            def list_job_definitions(self, **_: object) -> list[dict[str, object]]:
                return []

        class _Storage:
            def __init__(self) -> None:
                self.signals = _SignalStore()
                self.execution = _ExecutionStore()
                self.jobs = _JobStore()

        storage = _Storage()

        with (
            patch(
                "core.services.execution_intents.create_alpaca_client_from_env",
                return_value=type("Client", (), {"trading_base_url": "paper"})(),
            ),
            patch(
                "core.services.execution_intents.resolve_trading_environment",
                return_value="paper",
            ),
            patch(
                "core.services.execution_intents._cleanup_terminal_intent_history",
                return_value={"deleted": 0, "results": []},
            ),
            patch(
                "core.services.execution_intents._backfill_strategy_position_links",
                return_value={"linked": 0, "results": []},
            ),
            patch(
                "core.services.execution_intents._cleanup_slot_conflicts",
                return_value={"revoked": 0, "results": []},
            ),
            patch(
                "core.services.execution_intents._manage_submitted_open_intents",
                return_value={"managed": 0, "results": []},
            ),
        ):
            result = dispatch_pending_execution_intents(
                db_target="postgresql://example",
                storage=storage,
                limit=10,
            )

        cleanup = dict(result["opportunity_cleanup"])
        self.assertEqual(cleanup["deleted"], 0)
        self.assertEqual(cleanup["terminalized"], 1)
        self.assertEqual(storage.signals.expired_ids, [opportunity_id])
        self.assertEqual(storage.signals.rows[0]["lifecycle_state"], "expired")
        self.assertIn(
            "expired_inactive_automation_label",
            storage.signals.rows[0]["reason_codes"],
        )

    def test_execute_job_retains_terminal_execution_intents(self) -> None:
        stale_created_at = (datetime.now(UTC) - timedelta(minutes=30)).isoformat()
        intent_id = "execution_intent:terminal-1"

        class _SignalStore:
            def schema_ready(self) -> bool:
                return True

            def list_opportunities(self, **_: object) -> list[dict[str, object]]:
                return []

        class _ExecutionStore:
            def __init__(self) -> None:
                self.rows = [
                    {
                        "execution_intent_id": intent_id,
                        "bot_id": "short_dated_index_credit_bot",
                        "automation_id": "index_put_credit_entry",
                        "state": "expired",
                        "slot_key": "entry:GLD",
                        "created_at": stale_created_at,
                    }
                ]

            def intent_schema_ready(self) -> bool:
                return True

            def list_execution_intents(
                self, **kwargs: object
            ) -> list[dict[str, object]]:
                states = kwargs.get("states")
                if states == ["pending"]:
                    return []
                return [dict(row) for row in self.rows]

            def delete_execution_intent(self, execution_intent_id: str) -> bool:
                raise AssertionError(
                    f"cleanup must not delete execution intent {execution_intent_id}"
                )

        class _JobStore:
            def schema_ready(self) -> bool:
                return True

            def list_job_definitions(self, **_: object) -> list[dict[str, object]]:
                return []

        class _Storage:
            def __init__(self) -> None:
                self.signals = _SignalStore()
                self.execution = _ExecutionStore()
                self.jobs = _JobStore()

        storage = _Storage()

        with (
            patch(
                "core.services.execution_intents.create_alpaca_client_from_env",
                return_value=type("Client", (), {"trading_base_url": "paper"})(),
            ),
            patch(
                "core.services.execution_intents.resolve_trading_environment",
                return_value="paper",
            ),
            patch(
                "core.services.execution_intents._backfill_strategy_position_links",
                return_value={"linked": 0, "results": []},
            ),
            patch(
                "core.services.execution_intents._cleanup_slot_conflicts",
                return_value={"revoked": 0, "results": []},
            ),
            patch(
                "core.services.execution_intents._manage_submitted_open_intents",
                return_value={"managed": 0, "results": []},
            ),
        ):
            result = dispatch_pending_execution_intents(
                db_target="postgresql://example",
                storage=storage,
                limit=10,
            )

        cleanup = dict(result["intent_cleanup"])
        self.assertEqual(cleanup["deleted"], 0)
        self.assertEqual(cleanup["retained"], 1)
        self.assertEqual(cleanup["results"][0]["execution_intent_id"], intent_id)
        self.assertEqual(storage.execution.rows[0]["state"], "expired")

    def test_execute_job_records_dispatch_window_elapsed_for_expired_entry_intent(
        self,
    ) -> None:
        intent_id = "execution_intent:entry-expired-1"

        class _SignalStore:
            def schema_ready(self) -> bool:
                return True

            def list_opportunities(self, **_: object) -> list[dict[str, object]]:
                return []

        class _ExecutionStore:
            def __init__(self) -> None:
                self.intent = {
                    "execution_intent_id": intent_id,
                    "bot_id": "short_dated_index_credit_bot",
                    "automation_id": "index_put_credit_entry",
                    "opportunity_decision_id": "opportunity_decision:1",
                    "strategy_position_id": None,
                    "execution_attempt_id": None,
                    "action_type": "open",
                    "slot_key": "entry:short_dated_index_credit_bot:cfg:GLD",
                    "claim_token": None,
                    "policy_ref": {},
                    "config_hash": "cfg-1",
                    "state": "pending",
                    "expires_at": "2026-04-17T16:30:00Z",
                    "superseded_by_id": None,
                    "payload": {
                        "dispatch_status": "pending",
                        "approval_mode": "auto",
                        "execution_mode": "paper",
                    },
                    "created_at": "2026-04-17T16:25:00Z",
                    "updated_at": "2026-04-17T16:25:00Z",
                }
                self.events: list[dict[str, object]] = []

            def intent_schema_ready(self) -> bool:
                return True

            def list_execution_intents(
                self, **kwargs: object
            ) -> list[dict[str, object]]:
                states = kwargs.get("states")
                if states == ["pending"] and self.intent["state"] == "pending":
                    return [dict(self.intent)]
                return []

            def upsert_execution_intent(self, **kwargs: object) -> dict[str, object]:
                self.intent = dict(kwargs)
                return dict(self.intent)

            def append_execution_intent_event(self, **kwargs: object) -> dict[str, object]:
                row = dict(kwargs)
                self.events.append(row)
                return row

        class _JobStore:
            def schema_ready(self) -> bool:
                return True

            def list_job_definitions(self, **_: object) -> list[dict[str, object]]:
                return []

        class _Storage:
            def __init__(self) -> None:
                self.signals = _SignalStore()
                self.execution = _ExecutionStore()
                self.jobs = _JobStore()

        storage = _Storage()

        with (
            patch(
                "core.services.execution_intents.create_alpaca_client_from_env",
                return_value=type("Client", (), {"trading_base_url": "paper"})(),
            ),
            patch(
                "core.services.execution_intents.resolve_trading_environment",
                return_value="paper",
            ),
            patch(
                "core.services.execution_intents._cleanup_terminal_intent_history",
                return_value={"deleted": 0, "results": []},
            ),
            patch(
                "core.services.execution_intents._backfill_strategy_position_links",
                return_value={"linked": 0, "results": []},
            ),
            patch(
                "core.services.execution_intents._cleanup_slot_conflicts",
                return_value={"revoked": 0, "results": []},
            ),
            patch(
                "core.services.execution_intents._manage_submitted_open_intents",
                return_value={"managed": 0, "results": []},
            ),
        ):
            result = dispatch_pending_execution_intents(
                db_target="postgresql://example",
                storage=storage,
                limit=10,
            )

        self.assertEqual(result["expired"], 1)
        self.assertEqual(result["results"][0]["reason"], PRE_DISPATCH_EXPIRE_REASON)
        self.assertEqual(storage.execution.intent["state"], "expired")
        self.assertEqual(
            storage.execution.intent["payload"]["expire_reason"],
            PRE_DISPATCH_EXPIRE_REASON,
        )
        self.assertEqual(storage.execution.events[-1]["event_type"], "expired")
        self.assertEqual(
            storage.execution.events[-1]["payload"]["reason"],
            PRE_DISPATCH_EXPIRE_REASON,
        )

    def test_dispatch_revokes_close_intent_when_position_is_already_closed(self) -> None:
        intent_id = "execution_intent:manage:index_put_credit_manage:position-1"

        class _SignalStore:
            def schema_ready(self) -> bool:
                return True

            def list_opportunities(self, **_: object) -> list[dict[str, object]]:
                return []

        class _ExecutionStore:
            def __init__(self) -> None:
                self.intent = {
                    "execution_intent_id": intent_id,
                    "bot_id": "short_dated_index_credit_bot",
                    "automation_id": "index_put_credit_manage",
                    "opportunity_decision_id": None,
                    "strategy_position_id": "position-1",
                    "execution_attempt_id": None,
                    "action_type": "close",
                    "slot_key": "manage:position-1:close",
                    "claim_token": None,
                    "policy_ref": {},
                    "config_hash": "cfg-1",
                    "state": "pending",
                    "expires_at": None,
                    "superseded_by_id": None,
                    "payload": {
                        "position_id": "position-1",
                        "dispatch_status": "pending",
                        "approval_mode": "auto",
                        "execution_mode": "paper",
                    },
                    "created_at": "2026-04-17T18:00:00Z",
                    "updated_at": "2026-04-17T18:00:00Z",
                }
                self.events: list[dict[str, object]] = []

            def intent_schema_ready(self) -> bool:
                return True

            def list_execution_intents(
                self, **kwargs: object
            ) -> list[dict[str, object]]:
                states = kwargs.get("states")
                if states == ["pending"] and self.intent["state"] == "pending":
                    return [dict(self.intent)]
                return []

            def get_execution_intent(self, execution_intent_id: str) -> dict[str, object] | None:
                if execution_intent_id != intent_id:
                    return None
                return dict(self.intent)

            def upsert_execution_intent(self, **kwargs: object) -> dict[str, object]:
                self.intent = dict(kwargs)
                return dict(self.intent)

            def append_execution_intent_event(self, **kwargs: object) -> dict[str, object]:
                row = dict(kwargs)
                self.events.append(row)
                return row

            def get_position(self, position_id: str) -> dict[str, object] | None:
                if position_id != "position-1":
                    return None
                return {"position_id": position_id, "status": "closed"}

        class _JobStore:
            def schema_ready(self) -> bool:
                return True

            def list_job_definitions(self, **_: object) -> list[dict[str, object]]:
                return []

        class _Storage:
            def __init__(self) -> None:
                self.signals = _SignalStore()
                self.execution = _ExecutionStore()
                self.jobs = _JobStore()

        storage = _Storage()

        with (
            patch(
                "core.services.execution_intents.create_alpaca_client_from_env",
                return_value=type("Client", (), {"trading_base_url": "paper"})(),
            ),
            patch(
                "core.services.execution_intents.resolve_trading_environment",
                return_value="paper",
            ),
            patch(
                "core.services.execution_intents._cleanup_terminal_intent_history",
                return_value={"deleted": 0, "results": []},
            ),
            patch(
                "core.services.execution_intents._backfill_strategy_position_links",
                return_value={"linked": 0, "results": []},
            ),
            patch(
                "core.services.execution_intents._cleanup_slot_conflicts",
                return_value={"revoked": 0, "results": []},
            ),
            patch(
                "core.services.execution_intents._manage_submitted_open_intents",
                return_value={"managed": 0, "results": []},
            ),
        ):
            result = dispatch_pending_execution_intents(
                db_target="postgresql://example",
                storage=storage,
                limit=10,
            )

        self.assertEqual(result["failed"], 0)
        self.assertEqual(result["skipped"], 1)
        self.assertEqual(storage.execution.intent["state"], "revoked")
        self.assertEqual(
            storage.execution.intent["payload"]["revoke_reason"],
            "position_closed",
        )
        self.assertEqual(result["results"][0]["status"], "revoked")
        self.assertEqual(storage.execution.events[-1]["event_type"], "revoked")

    def test_management_automation_skips_positions_missing_after_refresh(self) -> None:
        position_id = "position-1"

        class _ExecutionStore:
            def __init__(self) -> None:
                self.position_queries = 0
                self.position_updates: list[dict[str, object]] = []
                self.created_intents: list[dict[str, object]] = []

            def portfolio_schema_ready(self) -> bool:
                return True

            def intent_schema_ready(self) -> bool:
                return True

            def list_positions(self, **_: object) -> list[dict[str, object]]:
                self.position_queries += 1
                if self.position_queries == 1:
                    return [
                        {
                            "position_id": position_id,
                            "bot_id": "short_dated_index_credit_bot",
                            "automation_id": "index_put_credit_entry",
                            "strategy_config_id": "cfg-1",
                            "strategy_id": "strategy-1",
                            "pipeline_id": "pipeline:explore_10_put_credit_weekly_auto",
                            "root_symbol": "QQQ",
                            "strategy_family": "put_credit_spread",
                            "status": "open",
                            "remaining_quantity": 1.0,
                            "closed_at": None,
                            "market_date_closed": None,
                            "market_date_opened": "2026-04-17",
                            "open_execution_attempt_id": "execution:open-1",
                        }
                    ]
                return []

            def list_open_attempts_for_position(self, **_: object) -> list[dict[str, object]]:
                return []

            def update_position(self, **kwargs: object) -> dict[str, object]:
                row = dict(kwargs)
                self.position_updates.append(row)
                return row

            def list_execution_intents(self, **_: object) -> list[dict[str, object]]:
                return []

            def upsert_execution_intent(self, **kwargs: object) -> dict[str, object]:
                row = dict(kwargs)
                self.created_intents.append(row)
                return row

            def append_execution_intent_event(self, **kwargs: object) -> dict[str, object]:
                return dict(kwargs)

        class _Storage:
            def __init__(self) -> None:
                self.execution = _ExecutionStore()

        runtime = Namespace(
            bot_id="short_dated_index_credit_bot",
            automation_id="index_put_credit_manage",
            strategy_config_id="cfg-1",
            strategy_family="put_credit_spread",
            symbols=("QQQ",),
            bot=Namespace(bot=Namespace(flatten_positions_at_et=None)),
            automation=Namespace(
                automation=Namespace(
                    execution_mode="paper",
                    approval_mode="auto",
                )
            ),
        )
        storage = _Storage()

        with (
            patch(
                "core.services.strategy_positions.resolve_management_runtime",
                return_value=runtime,
            ),
            patch(
                "core.services.strategy_positions.automation_should_run_now",
                return_value=True,
            ),
            patch(
                "core.services.strategy_positions.bot_time_reached",
                return_value=False,
            ),
            patch(
                "core.services.strategy_positions.refresh_session_position_marks",
                return_value={"refreshed": 0},
            ),
            patch(
                "core.services.strategy_positions.plan_position_management",
                return_value={
                    "should_close": True,
                    "reason": "force_close",
                    "limit_price": 1.0,
                    "limit_price_source": "width",
                },
            ),
        ):
            result = run_management_automation_decision(
                db_target="postgresql://example",
                bot_id=runtime.bot_id,
                automation_id=runtime.automation_id,
                storage=storage,
            )

        self.assertEqual(result["position_count"], 1)
        self.assertEqual(result["evaluated"], 1)
        self.assertEqual(result["created_intents"], 0)
        self.assertEqual(result["skipped"], 1)
        self.assertEqual(storage.execution.created_intents, [])
        self.assertEqual(
            storage.execution.position_updates[-1]["last_exit_reason"],
            "position_no_longer_open",
        )

    def test_collection_cycle_uses_same_slot_signal_context(self) -> None:
        collector_store = _CollectorStore()
        args = Namespace(
            strategy="call_debit",
            profile="weekly",
            greeks_source="local",
            top=5,
            history_db="postgresql://example",
            execution_policy=None,
            quote_capture_seconds=0,
            trade_capture_seconds=0,
            session_end_offset_minutes=0,
        )
        scanner_args = Namespace(feed="opra", data_base_url="https://data.example")

        with (
            patch(
                "core.services.collections.cycle.run_universe_cycle",
                return_value=(["AAPL"], "earnings", [], [], []),
            ),
            patch(
                "core.services.collections.cycle.build_symbol_strategy_candidates",
                return_value={"AAPL": [_candidate_payload()]},
            ),
            patch(
                "core.services.collections.cycle.capture_live_option_market_state",
                return_value=_same_slot_capture_snapshot(),
            ),
            patch(
                "core.services.collections.cycle.read_previous_selection",
                return_value=({}, {}),
            ),
            patch(
                "core.services.collections.cycle.sync_live_collector_signal_layer",
                return_value={
                    "signal_states_upserted": 0,
                    "signal_transitions_recorded": 0,
                    "opportunities_upserted": 0,
                    "opportunities_expired": 0,
                },
            ),
            patch(
                "core.services.collections.cycle.dispatch_cycle_alerts",
                return_value=[],
            ),
        ):
            result = run_collection_cycle(
                args,
                tick_context=None,
                scanner_args=scanner_args,
                client=object(),
                history_store=_HistoryStore(),
                alert_store=_AlertStore(),
                job_store=_JobStoreThatMustNotServeStaleContext(),
                collector_store=collector_store,
                event_store=_EventStore(),
                signal_store=_SignalStore(),
                recovery_store=None,
                calendar_resolver=object(),
                greeks_provider=object(),
                emit_output=False,
            )

        self.assertEqual(result["promotable_opportunity_count"], 1)
        self.assertIsNotNone(collector_store.saved_cycle)
        saved_opportunity = collector_store.saved_cycle["opportunities"][0]
        signal_bundle = saved_opportunity["score_evidence"]["signal_bundle"]
        self.assertEqual(signal_bundle["options_bias_alignment_source"], "evidence")
        self.assertEqual(
            signal_bundle["signals"]["direction_signal"]["source"], "evidence"
        )
        self.assertTrue(saved_opportunity["score_evidence"]["signal_gate"]["eligible"])

    def test_capture_live_market_state_uses_market_recorder_rows(self) -> None:
        history_store = _HistoryStore()
        args = Namespace(
            profile="weekly",
            quote_capture_seconds=20,
            trade_capture_seconds=20,
            session_end_offset_minutes=0,
        )
        scanner_args = Namespace(feed="opra", data_base_url="https://data.example")
        tick_context = LiveTickContext(
            job_run_id="job-run-1",
            session_id="live:earnings:2026-04-15",
            slot_at="2026-04-15T14:35:00Z",
        )
        capture_candidates = [_candidate_payload()]
        order: list[str] = []
        recorder_quotes = [
            {
                "option_symbol": "AAPL260424C210",
                "captured_at": "2026-04-15T14:35:21Z",
                "bid_price": 1.9,
                "ask_price": 2.0,
                "source": "market_recorder",
            }
        ]
        recorder_trades = [
            {
                "option_symbol": "AAPL260424C205",
                "captured_at": "2026-04-15T14:35:21Z",
                "price": 3.6,
                "size": 10,
                "conditions": [],
                "source": "market_recorder",
            }
        ]

        with (
            patch(
                "core.services.collections.capture.runtime.refresh_live_session_capture_targets",
                side_effect=lambda **_: order.append("targets")
                or {"status": "ok", "capture_targets": {}},
            ),
            patch(
                "core.services.collections.capture.runtime.collect_latest_quote_records",
                side_effect=lambda **_: order.append("baseline")
                or [
                    {
                        "option_symbol": "AAPL260424C210",
                        "captured_at": "2026-04-15T14:35:00Z",
                        "bid_price": 1.8,
                        "ask_price": 2.0,
                        "source": "alpaca_latest_quote",
                    }
                ],
            ),
            patch(
                "core.services.collections.capture.runtime.collect_recorded_market_data_records",
                side_effect=lambda **_: order.append("recorded")
                or {
                    "quotes": recorder_quotes,
                    "trades": recorder_trades,
                    "quote_error": None,
                    "trade_error": None,
                    "quote_complete": True,
                },
            ),
            patch(
                "core.services.collections.capture.runtime.build_uoa_trade_summary",
                return_value={"overview": {"scoreable_trade_count": 1}},
            ),
            patch(
                "core.services.collections.capture.runtime.build_uoa_quote_summary",
                return_value={"overview": {"observed_contract_count": 1}},
            ),
            patch(
                "core.services.collections.capture.runtime.build_uoa_trade_baselines",
                return_value={},
            ),
            patch(
                "core.services.collections.capture.runtime.build_uoa_root_decisions",
                return_value={"overview": {"root_count": 1}},
            ),
        ):
            snapshot = capture_live_option_market_state(
                args=args,
                scanner_args=scanner_args,
                client=object(),
                history_store=history_store,
                event_store=_EventStore(),
                recovery_store=_RecoveryStore(),
                label="earnings",
                cycle_id="cycle-live",
                generated_at="2026-04-15T14:35:00Z",
                session_date="2026-04-15",
                tick_context=tick_context,
                capture_candidates=capture_candidates,
            )

        self.assertEqual(order[:3], ["targets", "baseline", "recorded"])
        self.assertEqual(snapshot.stream_quote_event_count, 1)
        self.assertEqual(snapshot.stream_trade_event_count, 1)
        self.assertEqual(snapshot.quote_event_count, 2)
        self.assertEqual(snapshot.trade_event_count, 1)
        self.assertEqual(len(history_store.saved_quote_batches), 1)
        self.assertEqual(
            history_store.saved_quote_batches[0][0]["source"], "alpaca_latest_quote"
        )
        self.assertEqual(history_store.saved_trade_batches, [])

    def test_pipeline_detail_prefers_canonical_signal_opportunities(self) -> None:
        storage = _PipelineStorage()
        with (
            patch(
                "core.services.pipelines.build_session_execution_portfolio",
                return_value={"positions": []},
            ),
            patch(
                "core.services.pipelines.build_session_risk_snapshot",
                return_value={"status": "healthy", "note": "ok"},
            ),
            patch(
                "core.services.pipelines.get_control_state_snapshot",
                return_value={"mode": "normal"},
            ),
            patch(
                "core.services.pipelines.list_session_execution_attempts",
                return_value=[],
            ),
        ):
            detail = get_pipeline_detail(
                db_target="postgresql://example",
                pipeline_id="pipeline:explore_10_call_debit_weekly_auto",
                market_date="2026-04-15",
                profit_target=0.5,
                stop_multiple=2.0,
                storage=storage,
            )

        current_opportunity = detail["current_cycle"]["opportunities"][0]
        self.assertEqual(current_opportunity["opportunity_id"], "opp-live")
        self.assertEqual(current_opportunity["candidate"]["underlying_symbol"], "MSFT")
        self.assertEqual(storage.collector.list_cycle_candidates_calls, 0)

    def test_list_pipelines_uses_canonical_live_runtime_session_loader(self) -> None:
        storage = _PipelineStorage()

        listing = list_pipelines(
            db_target="postgresql://example",
            market_date="2026-04-15",
            storage=storage,
        )

        self.assertEqual(len(listing["pipelines"]), 1)
        pipeline = listing["pipelines"][0]
        self.assertEqual(
            pipeline["pipeline_id"], "pipeline:explore_10_call_debit_weekly_auto"
        )
        self.assertEqual(pipeline["promotable_count"], 1)
        self.assertEqual(pipeline["monitor_count"], 0)

    def test_job_run_view_surfaces_live_collector_automation_summary(self) -> None:
        run = {
            "job_run_id": "run-1",
            "job_key": "live_collector:explore_10_call_debit_weekly_auto",
            "job_type": "live_collector",
            "status": "succeeded",
            "scheduled_for": "2026-04-15T14:35:00Z",
            "started_at": "2026-04-15T14:35:01Z",
            "finished_at": "2026-04-15T14:35:15Z",
            "session_id": "live:explore_10_call_debit_weekly_auto:2026-04-15",
            "slot_at": "2026-04-15T14:35:00Z",
            "worker_name": "worker",
            "retry_count": 0,
            "payload": {
                "label": "explore_10_call_debit_weekly_auto",
                "session_date": "2026-04-15",
            },
            "result": {
                "label": "explore_10_call_debit_weekly_auto",
                "quote_capture": {},
                "trade_capture": {},
                "uoa_summary": {},
                "uoa_quote_summary": {},
                "uoa_decisions": {},
                "selection_summary": {
                    "opportunity_count": 1,
                    "selection_state_counts": {"promotable": 1},
                },
                "automation_summary": {
                    "automation_runs_upserted": 1,
                    "runtime_opportunities_upserted": 2,
                    "runtime_opportunities_expired": 1,
                    "runtime_selection_summary": {
                        "opportunity_count": 2,
                        "selection_state_counts": {
                            "promotable": 1,
                            "monitor": 1,
                        },
                    },
                },
            },
        }
        definition = {
            "job_key": "live_collector:explore_10_call_debit_weekly_auto",
            "job_type": "live_collector",
            "enabled": True,
            "payload": {},
        }

        class _JobStore:
            def schema_ready(self) -> bool:
                return True

            def get_job_run(self, job_run_id: str) -> dict[str, object] | None:
                return dict(run) if job_run_id == "run-1" else None

            def get_job_definition(self, job_key: str) -> dict[str, object] | None:
                return dict(definition) if job_key == definition["job_key"] else None

            def list_latest_runs_by_job_keys(
                self, **_: object
            ) -> list[dict[str, object]]:
                return [dict(run)]

            def get_lease(self, key: str) -> None:
                del key
                return None

        storage = type("Storage", (), {"jobs": _JobStore()})()

        job_view = build_job_run_view(job_run_id="run-1", storage=storage)

        self.assertEqual(job_view["summary"]["runtime_opportunity_count"], 2)
        self.assertEqual(
            job_view["details"]["automation_summary"][
                "runtime_opportunities_upserted"
            ],
            2,
        )
        self.assertEqual(
            job_view["details"]["automation_summary"]["runtime_selection_summary"][
                "selection_state_counts"
            ]["monitor"],
            1,
        )

    def test_uoa_state_prefers_canonical_signal_opportunities(self) -> None:
        storage = _PipelineStorage()

        detail = get_latest_uoa_state(storage=storage)

        self.assertEqual(detail["opportunities"][0]["opportunity_id"], "opp-live")
        self.assertEqual(
            detail["opportunities"][0]["candidate"]["underlying_symbol"], "MSFT"
        )
        self.assertEqual(storage.collector.list_cycle_candidates_calls, 0)

    def test_scheduler_coalesces_stale_queued_slot_to_latest_slot(self) -> None:
        old_slot = datetime(2026, 4, 15, 14, 30, tzinfo=UTC)
        current_slot = datetime(2026, 4, 15, 14, 31, tzinfo=UTC)

        class _SchedulerJobStore:
            def __init__(self) -> None:
                self.definition = {
                    "job_key": "live_collector:test",
                    "job_type": "live_collector",
                    "payload": {
                        "interval_seconds": 60,
                    },
                }
                self.runs: dict[str, dict[str, object]] = {
                    "live_collector:test:20260415T143000Z": {
                        "job_run_id": "live_collector:test:20260415T143000Z",
                        "job_key": "live_collector:test",
                        "job_type": "live_collector",
                        "status": "queued",
                        "scheduled_for": old_slot,
                        "slot_at": old_slot,
                        "session_id": "live:test:2026-04-15",
                        "retry_count": 0,
                        "payload": {
                            "job_key": "live_collector:test",
                            "job_type": "live_collector",
                            "label": "test",
                            "session_id": "live:test:2026-04-15",
                            "session_date": "2026-04-15",
                            "scheduled_for": isoformat_utc(old_slot),
                            "slot_at": isoformat_utc(old_slot),
                        },
                        "arq_job_id": "live_collector:test:20260415T143000Z",
                    }
                }
                self.created_runs: list[dict[str, object]] = []

            def list_job_definitions(
                self,
                **_: object,
            ) -> list[dict[str, object]]:
                return [dict(self.definition)]

            def list_job_runs(
                self,
                *,
                job_key: str,
                session_id: str,
                limit: int = 1,
                **_: object,
            ) -> list[dict[str, object]]:
                del limit
                rows = [
                    dict(row)
                    for row in self.runs.values()
                    if row["job_key"] == job_key and row["session_id"] == session_id
                ]
                rows.sort(
                    key=lambda row: (row["scheduled_for"], row["job_run_id"]),
                    reverse=True,
                )
                return rows

            def get_job_run_for_slot(
                self,
                *,
                job_key: str,
                session_id: str,
                slot_at: datetime,
            ) -> dict[str, object] | None:
                for row in self.runs.values():
                    if (
                        row["job_key"] == job_key
                        and row["session_id"] == session_id
                        and row["slot_at"] == slot_at
                    ):
                        return dict(row)
                return None

            def create_job_run(
                self,
                **kwargs: object,
            ) -> tuple[dict[str, object], bool]:
                record = dict(kwargs)
                self.runs[str(record["job_run_id"])] = record
                self.created_runs.append(record)
                return dict(record), True

            def requeue_job_run(
                self,
                *,
                job_run_id: str,
                arq_job_id: str,
                payload: dict[str, object] | None = None,
            ) -> dict[str, object]:
                row = self.runs[job_run_id]
                row["arq_job_id"] = arq_job_id
                row["retry_count"] = int(row.get("retry_count", 0)) + 1
                row["status"] = "queued"
                if payload is not None:
                    row["payload"] = dict(payload)
                return dict(row)

            def update_job_run_status(
                self,
                *,
                job_run_id: str,
                status: str,
                expected_arq_job_id: str | None = None,
                finished_at: datetime | None = None,
                error_text: str | None = None,
                **_: object,
            ) -> dict[str, object] | None:
                row = self.runs[job_run_id]
                if (
                    expected_arq_job_id is not None
                    and row["arq_job_id"] != expected_arq_job_id
                ):
                    return None
                row["status"] = status
                if finished_at is not None:
                    row["finished_at"] = finished_at
                if error_text is not None:
                    row["error_text"] = error_text
                return dict(row)

        class _SchedulerRecoveryStore:
            def __init__(self) -> None:
                self.rows: dict[tuple[str, str], dict[str, object]] = {}

            def ensure_live_session_slots(self, **_: object) -> None:
                return None

            def get_live_session_slot(
                self,
                *,
                session_id: str,
                slot_at: datetime,
            ) -> dict[str, object] | None:
                return self.rows.get((session_id, isoformat_utc(slot_at)))

            def upsert_live_session_slot(
                self,
                **kwargs: object,
            ) -> dict[str, object]:
                row = dict(kwargs)
                self.rows[(str(row["session_id"]), str(row["slot_at"]))] = row
                return row

        job_store = _SchedulerJobStore()
        recovery_store = _SchedulerRecoveryStore()

        async def run_test() -> dict[str, object]:
            with (
                patch(
                    "core.jobs.scheduler.resolve_live_tick_plan",
                    return_value={
                        "label": "test",
                        "session_id": "live:test:2026-04-15",
                        "session_date": "2026-04-15",
                        "interval_seconds": 60,
                        "slots": [old_slot, current_slot],
                        "current_slot": current_slot,
                        "payload": {"interval_seconds": 60},
                    },
                ),
                patch(
                    "core.jobs.scheduler._live_run_active",
                    return_value=True,
                ),
                patch(
                    "core.jobs.scheduler._enqueue_job_run",
                    return_value=True,
                ),
                patch(
                    "core.jobs.scheduler._enqueue_collector_recovery_if_needed",
                    return_value=None,
                ),
                patch(
                    "core.jobs.scheduler._publish_job_run_update",
                    return_value=None,
                ),
            ):
                return await _reconcile_live_collector_jobs(
                    job_store,
                    recovery_store,
                    object(),
                    now=datetime(2026, 4, 15, 14, 31, 5, tzinfo=UTC),
                )

        result = asyncio.run(run_test())

        old_run = job_store.runs["live_collector:test:20260415T143000Z"]
        self.assertEqual(old_run["status"], "skipped")
        self.assertNotEqual(
            old_run["arq_job_id"],
            "live_collector:test:20260415T143000Z",
        )
        current_run = next(
            row for row in job_store.created_runs if row["slot_at"] == current_slot
        )
        self.assertEqual(current_run["status"], "queued")
        old_slot_record = recovery_store.rows[
            ("live:test:2026-04-15", "2026-04-15T14:30:00Z")
        ]
        self.assertEqual(old_slot_record["status"], LIVE_SLOT_STATUS_MISSED)
        self.assertIn(str(current_run["job_run_id"]), result["enqueued"])

    def test_scheduler_supersedes_stale_definition_runs(self) -> None:
        now = datetime(2026, 4, 15, 14, 31, 5, tzinfo=UTC)
        latest_slot = datetime(2026, 4, 15, 14, 31, tzinfo=UTC)

        class _DefinitionJobStore:
            def __init__(self) -> None:
                self.definition = {
                    "job_key": "alert_reconcile:scheduled",
                    "job_type": "alert_reconcile",
                    "enabled": True,
                    "schedule_type": "interval_minutes",
                    "schedule": {"minutes": 1},
                    "singleton_scope": "global",
                    "payload": {"allow_off_hours": True},
                }
                self.runs = {
                    "alert_reconcile:scheduled:20260415T142900Z": {
                        "job_run_id": "alert_reconcile:scheduled:20260415T142900Z",
                        "job_key": "alert_reconcile:scheduled",
                        "job_type": "alert_reconcile",
                        "status": "queued",
                        "scheduled_for": datetime(2026, 4, 15, 14, 29, tzinfo=UTC),
                        "arq_job_id": "alert_reconcile:scheduled:20260415T142900Z",
                        "payload": {},
                    },
                    "alert_reconcile:scheduled:20260415T143000Z": {
                        "job_run_id": "alert_reconcile:scheduled:20260415T143000Z",
                        "job_key": "alert_reconcile:scheduled",
                        "job_type": "alert_reconcile",
                        "status": "queued",
                        "scheduled_for": datetime(2026, 4, 15, 14, 30, tzinfo=UTC),
                        "arq_job_id": "alert_reconcile:scheduled:20260415T143000Z",
                        "payload": {},
                    },
                    "alert_reconcile:scheduled:20260415T143100Z": {
                        "job_run_id": "alert_reconcile:scheduled:20260415T143100Z",
                        "job_key": "alert_reconcile:scheduled",
                        "job_type": "alert_reconcile",
                        "status": "succeeded",
                        "scheduled_for": latest_slot,
                        "arq_job_id": "alert_reconcile:scheduled:20260415T143100Z",
                        "payload": {},
                    },
                }

            def list_job_definitions(self, **_: object) -> list[dict[str, object]]:
                return [dict(self.definition)]

            def list_latest_runs_by_job_keys(
                self, *, job_keys: list[str], **_: object
            ) -> list[dict[str, object]]:
                if job_keys != ["alert_reconcile:scheduled"]:
                    return []
                return [dict(self.runs["alert_reconcile:scheduled:20260415T143100Z"])]

            def list_job_runs(self, **kwargs: object) -> list[dict[str, object]]:
                rows = [
                    dict(row)
                    for row in self.runs.values()
                    if (
                        kwargs.get("job_key") is None
                        or row["job_key"] == kwargs["job_key"]
                    )
                    and (
                        kwargs.get("status") is None
                        or row["status"] == kwargs["status"]
                    )
                ]
                rows.sort(
                    key=lambda row: (row["scheduled_for"], row["job_run_id"]),
                    reverse=True,
                )
                limit = int(kwargs.get("limit") or len(rows))
                return rows[:limit]

            def get_lease(self, _lease_key: str) -> None:
                return None

            def create_job_run(
                self, **kwargs: object
            ) -> tuple[dict[str, object], bool]:
                record = dict(kwargs)
                self.runs[str(record["job_run_id"])] = record
                return dict(record), True

            def update_job_run_status(
                self,
                *,
                job_run_id: str,
                status: str,
                expected_arq_job_id: str | None = None,
                finished_at: datetime | None = None,
                result: dict[str, object] | None = None,
                error_text: str | None = None,
                **_: object,
            ) -> dict[str, object] | None:
                row = self.runs[job_run_id]
                if (
                    expected_arq_job_id is not None
                    and row["arq_job_id"] != expected_arq_job_id
                ):
                    return None
                row["status"] = status
                if finished_at is not None:
                    row["finished_at"] = finished_at
                if result is not None:
                    row["result"] = dict(result)
                if error_text is not None:
                    row["error_text"] = error_text
                return dict(row)

        job_store = _DefinitionJobStore()

        async def run_test() -> dict[str, object]:
            with (
                patch(
                    "core.jobs.scheduler.due_job_payload",
                    return_value=(
                        "alert_reconcile:scheduled:20260415T143200Z",
                        datetime(2026, 4, 15, 14, 32, tzinfo=UTC),
                        {
                            "job_key": "alert_reconcile:scheduled",
                            "job_type": "alert_reconcile",
                            "scheduled_for": "2026-04-15T14:32:00Z",
                            "singleton_scope": "global",
                        },
                    ),
                ),
                patch(
                    "core.jobs.scheduler._enqueue_job_run",
                    return_value=True,
                ),
                patch(
                    "core.jobs.scheduler._publish_job_run_update",
                    return_value=None,
                ),
            ):
                return await _enqueue_definition_jobs(
                    job_store,
                    object(),
                    now=now,
                )

        result = asyncio.run(run_test())

        self.assertEqual(
            job_store.runs["alert_reconcile:scheduled:20260415T142900Z"]["status"],
            "skipped",
        )
        self.assertEqual(
            job_store.runs["alert_reconcile:scheduled:20260415T143000Z"]["status"],
            "skipped",
        )
        self.assertEqual(result["skipped"][0]["reason"], "superseded_stale_queued_runs")
        self.assertEqual(result["skipped"][0]["count"], "2")

    def test_entry_decision_prefers_automation_scoped_opportunities(self) -> None:
        bot = load_active_bots()["short_dated_index_credit_bot"]
        runtime = next(
            item
            for item in bot.automations
            if item.automation.automation_id == "index_put_credit_entry"
        )
        symbol = runtime.symbols[0]
        scoped_row = {
            "opportunity_id": "opp-scoped",
            "underlying_symbol": symbol,
            "strategy_family": runtime.strategy_config.strategy_family,
            "lifecycle_state": "ready",
            "consumed_by_execution_attempt_id": None,
            "execution_score": 91.0,
            "selection_rank": 1,
            "label": "runtime_label",
            "expires_at": "2026-04-15T15:30:00Z",
        }
        generic_row = {
            "opportunity_id": "opp-generic",
            "underlying_symbol": symbol,
            "strategy_family": runtime.strategy_config.strategy_family,
            "lifecycle_state": "ready",
            "consumed_by_execution_attempt_id": None,
            "execution_score": 75.0,
            "selection_rank": 1,
            "label": "generic_label",
            "expires_at": "2026-04-15T15:30:00Z",
        }
        signals = _DecisionSignalStore(scoped_row=scoped_row, generic_row=generic_row)
        execution = _DecisionExecutionStore()
        storage = _DecisionStorage(
            signals=signals,
            execution=execution,
            jobs=_DecisionJobStore(),
        )

        with (
            patch(
                "core.services.decision_engine.automation_should_run_now",
                return_value=True,
            ),
            patch(
                "core.services.decision_engine.evaluate_entry_controls",
                return_value=(True, None, {"open_positions": 0}),
            ),
        ):
            result = run_entry_automation_decision(
                db_target="postgresql://example",
                bot_id=bot.bot.bot_id,
                automation_id=runtime.automation.automation_id,
                market_date="2026-04-15",
                storage=storage,
            )

        self.assertEqual(result["selected_opportunity_id"], "opp-scoped")
        self.assertEqual(len(signals.list_calls), 1)
        self.assertTrue(signals.list_calls[0]["runtime_owned"])
        self.assertEqual(signals.list_calls[0]["bot_id"], bot.bot.bot_id)
        self.assertEqual(
            signals.list_calls[0]["automation_id"],
            runtime.automation.automation_id,
        )
        self.assertEqual(
            execution.upserted_intents[0]["payload"]["exit_policy"][
                "profit_target_pct"
            ],
            0.5,
        )

    def test_entry_decision_skips_blocked_scoped_opportunities(self) -> None:
        bot = load_active_bots()["short_dated_index_credit_bot"]
        runtime = next(
            item
            for item in bot.automations
            if item.automation.automation_id == "index_put_credit_entry"
        )
        symbol = runtime.symbols[0]
        scoped_row = {
            "opportunity_id": "opp-scoped",
            "underlying_symbol": symbol,
            "strategy_family": runtime.strategy_config.strategy_family,
            "lifecycle_state": "ready",
            "consumed_by_execution_attempt_id": None,
            "execution_score": 91.0,
            "selection_rank": 1,
            "label": "runtime_label",
            "expires_at": "2026-04-15T15:30:00Z",
            "candidate": {
                "execution_blockers": ["return_on_risk_below_promotable_floor"]
            },
        }
        generic_row = {
            "opportunity_id": "opp-generic",
            "underlying_symbol": symbol,
            "strategy_family": runtime.strategy_config.strategy_family,
            "lifecycle_state": "ready",
            "consumed_by_execution_attempt_id": None,
            "execution_score": 75.0,
            "selection_rank": 1,
            "label": "generic_label",
            "expires_at": "2026-04-15T15:30:00Z",
        }
        signals = _DecisionSignalStore(scoped_row=scoped_row, generic_row=generic_row)
        execution = _DecisionExecutionStore()
        storage = _DecisionStorage(
            signals=signals,
            execution=execution,
            jobs=_DecisionJobStore(),
        )

        with (
            patch(
                "core.services.decision_engine.automation_should_run_now",
                return_value=True,
            ),
            patch(
                "core.services.decision_engine.evaluate_entry_controls",
                return_value=(True, None, {"open_positions": 0}),
            ),
        ):
            result = run_entry_automation_decision(
                db_target="postgresql://example",
                bot_id=bot.bot.bot_id,
                automation_id=runtime.automation.automation_id,
                market_date="2026-04-15",
                storage=storage,
            )

        self.assertIsNone(result["selected_opportunity_id"])
        self.assertEqual(result["opportunity_count"], 0)
        self.assertEqual(len(signals.list_calls), 1)
        self.assertEqual(execution.upserted_intents, [])

    def test_entry_decision_sets_post_decision_intent_ttl(self) -> None:
        bot = load_active_bots()["short_dated_index_credit_bot"]
        runtime = next(
            item
            for item in bot.automations
            if item.automation.automation_id == "index_put_credit_entry"
        )
        symbol = runtime.symbols[0]
        scoped_row = {
            "opportunity_id": "opp-scoped",
            "underlying_symbol": symbol,
            "strategy_family": runtime.strategy_config.strategy_family,
            "lifecycle_state": "ready",
            "consumed_by_execution_attempt_id": None,
            "execution_score": 91.0,
            "selection_rank": 1,
            "label": "runtime_label",
            "expires_at": "2026-04-15T15:30:00Z",
        }
        generic_row = dict(scoped_row)
        generic_row["opportunity_id"] = "opp-generic"
        generic_row["execution_score"] = 75.0
        signals = _DecisionSignalStore(scoped_row=scoped_row, generic_row=generic_row)
        execution = _DecisionExecutionStore()
        storage = _DecisionStorage(
            signals=signals,
            execution=execution,
            jobs=_DecisionJobStore(),
        )

        with (
            patch(
                "core.services.decision_engine.automation_should_run_now",
                return_value=True,
            ),
            patch(
                "core.services.decision_engine.evaluate_entry_controls",
                return_value=(True, None, {"open_positions": 0}),
            ),
        ):
            run_entry_automation_decision(
                db_target="postgresql://example",
                bot_id=bot.bot.bot_id,
                automation_id=runtime.automation.automation_id,
                market_date="2026-04-15",
                storage=storage,
            )

        intent = execution.upserted_intents[0]
        created_at = parse_datetime(str(intent["created_at"]))
        expires_at = parse_datetime(str(intent["expires_at"]))

        self.assertIsNotNone(created_at)
        self.assertIsNotNone(expires_at)
        assert created_at is not None
        assert expires_at is not None
        self.assertGreaterEqual(expires_at - created_at, timedelta(minutes=5))
        self.assertNotEqual(intent["expires_at"], scoped_row["expires_at"])
        self.assertEqual(
            intent["payload"]["opportunity_expires_at"],
            scoped_row["expires_at"],
        )

    def test_entry_decision_requests_immediate_dispatch_for_selected_intent(self) -> None:
        bot = load_active_bots()["short_dated_index_credit_bot"]
        runtime = next(
            item
            for item in bot.automations
            if item.automation.automation_id == "index_put_credit_entry"
        )
        symbol = runtime.symbols[0]
        scoped_row = {
            "opportunity_id": "opp-scoped",
            "underlying_symbol": symbol,
            "strategy_family": runtime.strategy_config.strategy_family,
            "lifecycle_state": "ready",
            "consumed_by_execution_attempt_id": None,
            "execution_score": 91.0,
            "selection_rank": 1,
            "label": "runtime_label",
            "expires_at": "2026-04-15T15:30:00Z",
        }
        generic_row = dict(scoped_row)
        generic_row["opportunity_id"] = "opp-generic"
        generic_row["execution_score"] = 75.0
        signals = _DecisionSignalStore(scoped_row=scoped_row, generic_row=generic_row)
        execution = _DecisionExecutionStore()
        storage = _DecisionStorage(
            signals=signals,
            execution=execution,
            jobs=_DecisionJobStore(),
        )

        with (
            patch(
                "core.services.decision_engine.automation_should_run_now",
                return_value=True,
            ),
            patch(
                "core.services.decision_engine.evaluate_entry_controls",
                return_value=(True, None, {"open_positions": 0}),
            ),
            patch(
                "core.services.decision_engine.request_options_automation_dispatch",
                return_value={
                    "status": "queued",
                    "job_run_id": "options_automation_execute:adhoc:test",
                    "job_key": "options_automation_execute:adhoc",
                },
            ),
        ):
            result = run_entry_automation_decision(
                db_target="postgresql://example",
                bot_id=bot.bot.bot_id,
                automation_id=runtime.automation.automation_id,
                market_date="2026-04-15",
                storage=storage,
            )

        self.assertEqual(
            result["dispatch_job_run_id"],
            "options_automation_execute:adhoc:test",
        )
        self.assertEqual(execution.intent_events[-1]["event_type"], "dispatch_requested")
        self.assertEqual(
            execution.intent_events[-1]["payload"]["job_run_id"],
            "options_automation_execute:adhoc:test",
        )

    def test_intent_summary_separates_entry_and_management_states(self) -> None:
        summary = summarize_intent_counts(
            [
                ("open", "expired", 3),
                ("close", "revoked", 2),
                ("close", "filled", 1),
            ]
        )

        self.assertEqual(summary["intent_count"], 6)
        self.assertEqual(summary["entry_intent_count"], 3)
        self.assertEqual(summary["entry_intent_state_counts"], {"expired": 3})
        self.assertEqual(summary["management_intent_count"], 3)
        self.assertEqual(
            summary["management_intent_state_counts"],
            {"filled": 1, "revoked": 2},
        )

    def test_runtime_deployment_mode_controls_live_approval_with_legacy_backcompat(
        self,
    ) -> None:
        class _ExecutionStore:
            def list_positions(self, **_: object) -> list[dict[str, object]]:
                return []

            def list_session_attempts_by_status(
                self,
                **_: object,
            ) -> list[dict[str, object]]:
                return []

        candidate = {
            "underlying_symbol": "MSFT",
            "strategy": "call_debit",
            "candidate": {
                "midpoint_credit": 1.2,
                "max_loss": 250.0,
            },
        }
        cycle = {
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        }

        with (
            patch(
                "core.services.risk_manager._current_trading_environment",
                return_value="live",
            ),
            patch.dict(
                os.environ,
                {"SPREADS_ALLOW_LIVE_TRADING": "true"},
                clear=False,
            ),
        ):
            paper_auto_decision = evaluate_open_execution(
                execution_store=_ExecutionStore(),
                session_id="live:explore_10_call_debit_weekly_auto:2026-04-15",
                candidate=candidate,
                cycle=cycle,
                quantity=1,
                limit_price=1.2,
                risk_policy={"enabled": True, "allow_live": True},
                execution_policy={
                    "enabled": True,
                    "deployment_mode": "paper_auto",
                    "mode": "top_promotable",
                },
            )
            legacy_live_decision = evaluate_open_execution(
                execution_store=_ExecutionStore(),
                session_id="live:explore_10_call_debit_weekly_auto:2026-04-15",
                candidate=candidate,
                cycle=cycle,
                quantity=1,
                limit_price=1.2,
                risk_policy={"enabled": True, "allow_live": True},
                execution_policy={
                    "enabled": True,
                    "mode": "top_promotable",
                },
            )

        self.assertEqual(paper_auto_decision["status"], "blocked")
        self.assertIn(
            "live_environment_blocked",
            paper_auto_decision["reason_codes"],
        )
        self.assertEqual(legacy_live_decision["status"], "approved")
        self.assertEqual(legacy_live_decision["policy"]["allow_live"], True)


if __name__ == "__main__":
    unittest.main()
