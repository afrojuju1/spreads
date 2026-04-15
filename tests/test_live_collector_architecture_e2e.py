from __future__ import annotations

import unittest
from argparse import Namespace
from unittest.mock import patch

from spreads.jobs.live_collector import (
    LiveCaptureSnapshot,
    LiveTickContext,
    _run_collection_cycle,
    capture_live_option_market_state,
)
from spreads.services.live_collector_health import (
    build_quote_capture_summary,
    build_trade_capture_summary,
)
from spreads.services.pipelines import get_pipeline_detail, list_pipelines
from spreads.services.uoa_state import get_latest_uoa_state
from spreads.storage.collector_repository import CollectorRepository


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
            row = {**row, "pipeline_id": pipeline_id, "market_date": row["session_date"]}
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
        self, cycle_id: str, *, eligibility_state: str | None = None, exclude_consumed: bool = True, limit: int = 200
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

        with patch(
            "spreads.jobs.live_collector.run_universe_cycle",
            return_value=(["AAPL"], "earnings", [], [], []),
        ), patch(
            "spreads.jobs.live_collector.build_symbol_strategy_candidates",
            return_value={"AAPL": [_candidate_payload()]},
        ), patch(
            "spreads.jobs.live_collector.capture_live_option_market_state",
            return_value=_same_slot_capture_snapshot(),
        ), patch(
            "spreads.jobs.live_collector.read_previous_selection",
            return_value=({}, {}),
        ), patch(
            "spreads.jobs.live_collector.sync_live_collector_signal_layer",
            return_value={
                "signal_states_upserted": 0,
                "signal_transitions_recorded": 0,
                "opportunities_upserted": 0,
                "opportunities_expired": 0,
            },
        ), patch(
            "spreads.jobs.live_collector.dispatch_cycle_alerts",
            return_value=[],
        ):
            result = _run_collection_cycle(
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
        self.assertEqual(signal_bundle["signals"]["direction_signal"]["source"], "evidence")
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

        with patch(
            "spreads.jobs.live_collector.refresh_live_session_capture_targets",
            side_effect=lambda **_: order.append("targets")
            or {"status": "ok", "capture_targets": {}},
        ), patch(
            "spreads.jobs.live_collector.collect_latest_quote_records",
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
        ), patch(
            "spreads.jobs.live_collector.collect_recorded_market_data_records",
            side_effect=lambda **_: order.append("recorded")
            or {
                "quotes": recorder_quotes,
                "trades": recorder_trades,
                "quote_error": None,
                "trade_error": None,
                "quote_complete": True,
            },
        ), patch(
            "spreads.jobs.live_collector.build_uoa_trade_summary",
            return_value={"overview": {"scoreable_trade_count": 1}},
        ), patch(
            "spreads.jobs.live_collector.build_uoa_quote_summary",
            return_value={"overview": {"observed_contract_count": 1}},
        ), patch(
            "spreads.jobs.live_collector.build_uoa_trade_baselines",
            return_value={},
        ), patch(
            "spreads.jobs.live_collector.build_uoa_root_decisions",
            return_value={"overview": {"root_count": 1}},
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
        self.assertEqual(history_store.saved_quote_batches[0][0]["source"], "alpaca_latest_quote")
        self.assertEqual(history_store.saved_trade_batches, [])

    def test_pipeline_detail_prefers_canonical_signal_opportunities(self) -> None:
        storage = _PipelineStorage()
        with patch(
            "spreads.services.pipelines.build_session_execution_portfolio",
            return_value={"positions": []},
        ), patch(
            "spreads.services.pipelines.build_session_risk_snapshot",
            return_value={"status": "healthy", "note": "ok"},
        ), patch(
            "spreads.services.pipelines.get_control_state_snapshot",
            return_value={"mode": "normal"},
        ), patch(
            "spreads.services.pipelines.list_session_execution_attempts",
            return_value=[],
        ):
            detail = get_pipeline_detail(
                db_target="postgresql://example",
                pipeline_id="pipeline:explore_10_call_debit_weekly_auto",
                market_date="2026-04-15",
                include_replay="none",
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
        self.assertEqual(pipeline["pipeline_id"], "pipeline:explore_10_call_debit_weekly_auto")
        self.assertEqual(pipeline["promotable_count"], 1)
        self.assertEqual(pipeline["monitor_count"], 0)

    def test_uoa_state_prefers_canonical_signal_opportunities(self) -> None:
        storage = _PipelineStorage()

        detail = get_latest_uoa_state(storage=storage)

        self.assertEqual(detail["opportunities"][0]["opportunity_id"], "opp-live")
        self.assertEqual(detail["opportunities"][0]["candidate"]["underlying_symbol"], "MSFT")
        self.assertEqual(storage.collector.list_cycle_candidates_calls, 0)


if __name__ == "__main__":
    unittest.main()
