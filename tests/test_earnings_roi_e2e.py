from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from apps.api.routes.internal_ops import (
    get_internal_ops_status_route,
    get_internal_ops_trading_route,
)
from spreads.integrations.calendar_events.models import CalendarEventRecord
from spreads.integrations.calendar_events.resolver import (
    CalendarEventResolver,
    build_calendar_event_resolver,
)
from spreads.services.live_collector_health import build_selection_summary
from spreads.services.live_runtime import list_latest_live_sessions as _list_latest_live_sessions
from spreads.services.live_selection import select_live_opportunities
from spreads.services.ops_visibility import (
    build_job_run_view,
    build_system_status,
    build_trading_health,
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _future_iso(hours: int = 1) -> str:
    return (datetime.now(UTC) + timedelta(hours=hours)).isoformat(timespec="seconds").replace("+00:00", "Z")


def _collector_candidate(
    *,
    symbol: str,
    strategy: str,
    selection_state: str,
    eligibility: str,
    earnings_phase: str,
    timing_confidence: str,
    scoring_blockers: list[str] | None = None,
    execution_blockers: list[str] | None = None,
    signal_gate_blockers: list[str] | None = None,
) -> dict[str, object]:
    return {
        "underlying_symbol": symbol,
        "strategy": strategy,
        "selection_state": selection_state,
        "selection_rank": 1,
        "state_reason": "selected",
        "origin": "live_scan",
        "eligibility": eligibility,
        "candidate": {
            "underlying_symbol": symbol,
            "strategy": strategy,
            "profile": "weekly",
            "expiration_date": "2026-04-24",
            "days_to_expiration": 8,
            "quality_score": 88.0,
            "setup_score": 82.0,
            "setup_intraday_score": 84.0,
            "setup_status": "favorable",
            "fill_ratio": 0.92,
            "data_status": "clean",
            "calendar_status": "clean",
            "earnings_phase": earnings_phase,
            "earnings_timing_confidence": timing_confidence,
            "short_symbol": f"{symbol}260424C210",
            "long_symbol": f"{symbol}260424C205",
            "short_strike": 210.0,
            "long_strike": 205.0,
            "midpoint_credit": 1.25,
            "return_on_risk": 0.22,
            "max_loss": 200.0,
            "short_open_interest": 1200,
            "long_open_interest": 1100,
            "execution_blockers": list(execution_blockers or []),
            "scoring_blockers": list(scoring_blockers or []),
            "score_evidence": {
                "signal_gate": {
                    "active": True,
                    "eligible": not bool(signal_gate_blockers),
                    "coverage_count": 4,
                    "blockers": list(signal_gate_blockers or []),
                }
            },
        },
    }


def _live_selection_candidate(symbol: str = "AAPL") -> dict[str, object]:
    return {
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


def _uoa_cycle_context(*, quality_score: float, dominant_flow: str, dominant_flow_ratio: float) -> dict[str, object]:
    return {
        "uoa_decisions": {
            "roots": [
                {
                    "underlying_symbol": "AAPL",
                    "decision_state": "promotable",
                    "current": {
                        "dominant_flow": dominant_flow,
                        "dominant_flow_ratio": dominant_flow_ratio,
                        "supporting_volume_oi_ratio": 1.2,
                        "max_volume_oi_ratio": 1.4,
                    },
                    "deltas": {
                        "max_premium_rate_ratio": 2.6,
                        "max_trade_rate_ratio": 1.8,
                    },
                }
            ]
        },
        "uoa_quote_summary": {
            "roots": {
                "AAPL": {
                    "average_quality_score": quality_score,
                    "quality_state": "strong" if quality_score >= 0.8 else "weak",
                    "fresh_contract_count": 4,
                    "liquid_contract_count": 4,
                }
            }
        },
    }


def _earnings_record(
    *,
    source: str,
    date_text: str,
    when: str | None,
    confidence: str,
) -> CalendarEventRecord:
    payload = {} if when is None else {"when": when, "reportTime": when}
    scheduled_at = f"{date_text}T20:15:00+00:00"
    return CalendarEventRecord(
        event_id=f"{source}:AAPL:{date_text}",
        event_type="earnings",
        symbol="AAPL",
        asset_scope=None,
        scheduled_at=scheduled_at,
        window_start=scheduled_at,
        window_end=scheduled_at,
        source=source,
        source_confidence=confidence,
        status="scheduled",
        payload_json=str(payload).replace("'", '"'),
        ingested_at=_now_iso(),
        source_updated_at=_now_iso(),
    )


class _StaticAdapter:
    def __init__(self, source_name: str, source_confidence: str) -> None:
        self.source_name = source_name
        self.source_confidence = source_confidence
        self.refresh_always = False

    def applies_to(self, _query: object) -> bool:
        return True

    def scope_key(self, query: object) -> str:
        return str(getattr(query, "symbol", "AAPL"))

    def coverage_query(self, query: object) -> object:
        return query

    def fetch(self, _query: object) -> list[CalendarEventRecord]:
        return []


class _FakeCalendarStore:
    def __init__(self, records: list[CalendarEventRecord], fresh_sources: set[str]) -> None:
        self.records = list(records)
        self.fresh_sources = set(fresh_sources)

    def has_fresh_coverage(self, *, source: str, **_: object) -> bool:
        return source in self.fresh_sources

    def get_refresh_state(self, *, source: str, scope_key: str) -> dict[str, str] | None:
        if source not in self.fresh_sources:
            return None
        return {
            "source": source,
            "scope_key": scope_key,
            "coverage_start": "2026-04-01T00:00:00+00:00",
            "coverage_end": "2026-05-01T00:00:00+00:00",
            "refreshed_at": _now_iso(),
        }

    def upsert_events(self, records: list[CalendarEventRecord]) -> None:
        self.records.extend(records)

    def set_refresh_state(self, **_: object) -> None:
        return None

    def query_events(
        self,
        *,
        symbol: str,
        asset_scope: str | None,
        window_start: str,
        window_end: str,
    ) -> list[CalendarEventRecord]:
        start = datetime.fromisoformat(window_start.replace("Z", "+00:00"))
        end = datetime.fromisoformat(window_end.replace("Z", "+00:00"))
        rows: list[CalendarEventRecord] = []
        for record in self.records:
            if record.symbol != symbol and (asset_scope is None or record.asset_scope != asset_scope):
                continue
            scheduled_at = datetime.fromisoformat(record.scheduled_at.replace("Z", "+00:00"))
            if start <= scheduled_at <= end:
                rows.append(record)
        return rows


class _FakeJobStore:
    def __init__(self, run_record: dict[str, object]) -> None:
        self.run_record = dict(run_record)
        self.definition = {
            "job_key": "live_collector:earnings",
            "job_type": "live_collector",
            "enabled": True,
            "schedule_type": "manual",
            "schedule": {},
            "singleton_scope": "earnings",
            "market_calendar": "NYSE",
            "payload": {
                "symbol": "AAPL",
                "strategy": "call_debit",
                "profile": "weekly",
                "greeks_source": "auto",
            },
        }

    def schema_ready(self) -> bool:
        return True

    def get_lease(self, lease_key: str) -> dict[str, object] | None:
        if "scheduler" in lease_key:
            return {
                "lease_key": lease_key,
                "owner": "scheduler",
                "expires_at": _future_iso(),
                "job_run_id": "scheduler-run",
            }
        return None

    def list_active_leases(self, prefix: str) -> list[dict[str, object]]:
        if "worker" in prefix:
            return [
                {
                    "lease_key": f"{prefix}worker-main",
                    "owner": "worker-main",
                    "expires_at": _future_iso(),
                    "job_run_id": "worker-run",
                }
            ]
        return []

    def list_job_runs(self, **_: object) -> list[dict[str, object]]:
        return []

    def list_job_definitions(self, **_: object) -> list[dict[str, object]]:
        return [dict(self.definition)]

    def list_latest_runs_by_job_keys(self, *, job_keys: list[str], **_: object) -> list[dict[str, object]]:
        return [dict(self.run_record)] if self.definition["job_key"] in job_keys else []

    def list_latest_runs_by_session_ids(
        self,
        *,
        session_ids: list[str],
        **_: object,
    ) -> list[dict[str, object]]:
        return [dict(self.run_record)] if session_ids else []

    def get_job_run(self, job_run_id: str) -> dict[str, object] | None:
        return dict(self.run_record) if job_run_id == self.run_record["job_run_id"] else None

    def get_job_definition(self, job_key: str) -> dict[str, object] | None:
        return dict(self.definition) if job_key == self.definition["job_key"] else None

    def get_latest_live_collector_run(self, *, label: str | None = None, status: str | None = "succeeded") -> dict[str, object] | None:
        if status == "succeeded":
            return dict(self.run_record)
        return None

    def get_live_collector_run_by_cycle_id(
        self,
        *,
        cycle_id: str,
        label: str,
        status: str | None = "succeeded",
    ) -> dict[str, object] | None:
        if cycle_id == "cycle-1" and status == "succeeded":
            return dict(self.run_record)
        return None


class _FakeCollectorStore:
    def schema_ready(self) -> bool:
        return True

    def get_latest_cycle(self, label: str) -> dict[str, object]:
        return {
            "cycle_id": "cycle-1",
            "label": label,
            "market_date": "2026-04-15",
            "session_date": "2026-04-15",
            "selection_memory": {},
        }

    def count_cycle_candidates_by_cycle_ids(
        self,
        cycle_ids: list[str],
    ) -> dict[str, dict[str, int]]:
        return {
            cycle_id: {
                "candidate_count": 2,
                "promotable": 1,
                "monitor": 1,
            }
            for cycle_id in cycle_ids
        }


class _FakeRecoveryStore:
    def schema_ready(self) -> bool:
        return False


class _FakeSignalStore:
    def schema_ready(self) -> bool:
        return False


class _FakeBrokerStore:
    def schema_ready(self) -> bool:
        return True

    def get_sync_state(self, _key: str) -> dict[str, object]:
        return {
            "status": "healthy",
            "updated_at": _now_iso(),
            "summary": {},
        }


class _FakeAlertStore:
    def schema_ready(self) -> bool:
        return False


class _FakeExecutionStore:
    def schema_ready(self) -> bool:
        return True

    def list_attempts_by_status(self, **_: object) -> list[dict[str, object]]:
        return []

    def portfolio_schema_ready(self) -> bool:
        return True

    def list_positions(self, **_: object) -> list[dict[str, object]]:
        return []


class _FakeStorage:
    def __init__(self, run_record: dict[str, object]) -> None:
        self.jobs = _FakeJobStore(run_record)
        self.collector = _FakeCollectorStore()
        self.recovery = _FakeRecoveryStore()
        self.signals = _FakeSignalStore()
        self.broker = _FakeBrokerStore()
        self.alerts = _FakeAlertStore()
        self.execution = _FakeExecutionStore()


class EarningsRoiE2ETests(unittest.TestCase):
    def test_selection_summary_surfaces_in_ops_views_without_double_counting(self) -> None:
        selection_summary = build_selection_summary(
            [
                _collector_candidate(
                    symbol="AAPL",
                    strategy="call_debit",
                    selection_state="promotable",
                    eligibility="live",
                    earnings_phase="pre_event_runup",
                    timing_confidence="high",
                ),
                _collector_candidate(
                    symbol="SPY",
                    strategy="iron_condor",
                    selection_state="monitor",
                    eligibility="analysis_only",
                    earnings_phase="post_event_fresh",
                    timing_confidence="medium",
                    scoring_blockers=["post_event_iron_condor_horizon_blocked"],
                    execution_blockers=["midpoint_credit_below_promotable_floor"],
                    signal_gate_blockers=["neutral_regime_signal_too_low"],
                ),
            ]
        )
        run_record = {
            "job_run_id": "run-1",
            "job_key": "live_collector:earnings",
            "job_type": "live_collector",
            "status": "succeeded",
            "scheduled_for": _now_iso(),
            "started_at": _now_iso(),
            "finished_at": _now_iso(),
            "heartbeat_at": _now_iso(),
            "worker_name": "worker-main",
            "retry_count": 0,
            "session_id": "session-1",
            "slot_at": _now_iso(),
            "payload": {"label": "earnings", "profile": "weekly", "singleton_scope": "earnings"},
            "result": {
                "label": "earnings",
                "profile": "weekly",
                "quote_events_saved": 10,
                "baseline_quote_events_saved": 2,
                "stream_quote_events_saved": 8,
                "trade_events_saved": 5,
                "stream_trade_events_saved": 5,
                "expected_quote_symbols": ["AAPL260424C205", "AAPL260424C210"],
                "expected_trade_symbols": ["AAPL260424C205"],
                "selection_summary": selection_summary,
                "live_action_gate": {"status": "pass", "allow_auto_execution": True},
                "uoa_summary": {},
                "uoa_quote_summary": {},
                "uoa_decisions": {},
            },
        }
        storage = _FakeStorage(run_record)

        with patch("spreads.services.ops_visibility.get_control_state_snapshot", return_value={"mode": "normal"}), patch(
            "spreads.services.ops_visibility.get_account_overview",
            return_value={
                "source": "live",
                "environment": "paper",
                "account": {"equity": 10000.0, "cash": 5000.0, "buying_power": 10000.0},
                "pnl": {"day_change": 0.0, "day_change_percent": 0.0},
                "sync": {},
            },
        ), patch(
            "spreads.services.ops_visibility.list_latest_live_sessions",
            wraps=_list_latest_live_sessions,
        ) as live_sessions_loader:
            system_status = build_system_status(storage=storage)
            trading_health = build_trading_health(storage=storage)
            job_view = build_job_run_view(storage=storage, job_run_id="run-1")

        self.assertGreaterEqual(live_sessions_loader.call_count, 2)
        self.assertEqual(system_status["summary"]["collector_opportunity_count"], 2)
        self.assertEqual(system_status["summary"]["collector_shadow_only_count"], 1)
        self.assertEqual(system_status["summary"]["collector_auto_live_eligible_count"], 1)
        self.assertEqual(
            trading_health["summary"]["collector_opportunity_count"],
            2,
        )
        self.assertEqual(
            trading_health["details"]["collector_selection"]["selection_state_counts"]["promotable"],
            1,
        )
        self.assertEqual(
            job_view["details"]["selection_summary"]["earnings_phase_counts"]["post_event_fresh"],
            1,
        )
        self.assertEqual(
            job_view["details"]["selection_summary"]["blocker_counts"]["signal_gate"]["neutral_regime_signal_too_low"],
            1,
        )

    def test_internal_ops_routes_are_thin_adapters(self) -> None:
        with patch(
            "apps.api.routes.internal_ops.build_system_status",
            return_value={"status": "healthy", "summary": {"collector_opportunity_count": 2}},
        ), patch(
            "apps.api.routes.internal_ops.build_trading_health",
            return_value={"status": "healthy", "summary": {"collector_auto_live_eligible_count": 1}},
        ):
            status_response = get_internal_ops_status_route()
            trading_response = get_internal_ops_trading_route()

        self.assertEqual(status_response["summary"]["collector_opportunity_count"], 2)
        self.assertEqual(
            trading_response["summary"]["collector_auto_live_eligible_count"],
            1,
        )

    def test_calendar_resolver_reconciles_multi_source_earnings_confidence(self) -> None:
        adapters = [
            _StaticAdapter("dolt_earnings_calendar", "low"),
            _StaticAdapter("alpha_vantage_earnings_calendar", "medium"),
        ]

        resolver = CalendarEventResolver(
            store=_FakeCalendarStore(
                records=[
                    _earnings_record(
                        source="dolt_earnings_calendar",
                        date_text="2026-04-20",
                        when="Before Market Open",
                        confidence="low",
                    ),
                    _earnings_record(
                        source="alpha_vantage_earnings_calendar",
                        date_text="2026-04-20",
                        when="Before Market Open",
                        confidence="medium",
                    ),
                ],
                fresh_sources={"dolt_earnings_calendar", "alpha_vantage_earnings_calendar"},
            ),
            adapters=adapters,
        )
        context = resolver.resolve_calendar_context(
            symbol="AAPL",
            strategy="call_debit",
            window_start="2026-04-14T14:00:00+00:00",
            window_end="2026-04-24T20:00:00+00:00",
            underlying_type="single_name_equity",
        )
        self.assertEqual(context.earnings_consensus_status, "consensus")
        self.assertEqual(context.earnings_timing_confidence, "high")
        self.assertEqual(context.earnings_primary_source, "alpha_vantage_earnings_calendar")
        self.assertEqual(
            set(context.earnings_supporting_sources),
            {"dolt_earnings_calendar", "alpha_vantage_earnings_calendar"},
        )

        resolver_conflict = CalendarEventResolver(
            store=_FakeCalendarStore(
                records=[
                    _earnings_record(
                        source="dolt_earnings_calendar",
                        date_text="2026-04-20",
                        when="After Market Close",
                        confidence="low",
                    ),
                    _earnings_record(
                        source="alpha_vantage_earnings_calendar",
                        date_text="2026-04-21",
                        when=None,
                        confidence="medium",
                    ),
                ],
                fresh_sources={"dolt_earnings_calendar", "alpha_vantage_earnings_calendar"},
            ),
            adapters=adapters,
        )
        conflict_context = resolver_conflict.resolve_calendar_context(
            symbol="AAPL",
            strategy="call_debit",
            window_start="2026-04-14T14:00:00+00:00",
            window_end="2026-04-24T20:00:00+00:00",
            underlying_type="single_name_equity",
        )
        self.assertEqual(conflict_context.earnings_consensus_status, "conflict")
        self.assertEqual(conflict_context.earnings_primary_source, "alpha_vantage_earnings_calendar")
        self.assertEqual(conflict_context.earnings_event_date, "2026-04-21")

        resolver_single_source = CalendarEventResolver(
            store=_FakeCalendarStore(
                records=[
                    _earnings_record(
                        source="dolt_earnings_calendar",
                        date_text="2026-04-20",
                        when="After Market Close",
                        confidence="low",
                    ),
                ],
                fresh_sources={"dolt_earnings_calendar"},
            ),
            adapters=adapters,
        )
        single_source_context = resolver_single_source.resolve_calendar_context(
            symbol="AAPL",
            strategy="call_debit",
            window_start="2026-04-14T14:00:00+00:00",
            window_end="2026-04-24T20:00:00+00:00",
            underlying_type="single_name_equity",
        )
        self.assertEqual(single_source_context.earnings_consensus_status, "single_source")
        self.assertEqual(single_source_context.earnings_timing_confidence, "low")

    def test_builder_skips_alpha_vantage_when_no_api_key_is_available(self) -> None:
        class _BuilderStore:
            def __init__(self, _database_url: str) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        with patch(
            "spreads.integrations.calendar_events.resolver.CalendarEventStore",
            _BuilderStore,
        ), patch(
            "spreads.integrations.calendar_events.resolver.default_alpha_vantage_api_key",
            return_value=None,
        ):
            resolver = build_calendar_event_resolver(
                key_id="key",
                secret_key="secret",
                data_base_url="https://data.example",
                database_url="postgresql://example",
            )

        self.assertEqual(
            [adapter.source_name for adapter in resolver.adapters],
            [
                "dolt_earnings_calendar",
                "alpaca_corporate_actions",
                "macro_calendar",
            ],
        )

    def test_live_selection_uses_options_evidence_before_fallback(self) -> None:
        result = select_live_opportunities(
            label="earnings",
            cycle_id="cycle:test",
            generated_at="2026-04-14T15:00:00+00:00",
            symbol_candidates={"AAPL": [_live_selection_candidate()]},
            previous_promotable={},
            previous_selection_memory={},
            top_promotable=5,
            top_monitor=5,
            profile="weekly",
            signal_cycle_context=_uoa_cycle_context(
                quality_score=0.92,
                dominant_flow="call",
                dominant_flow_ratio=0.9,
            ),
        )
        opportunity = result["opportunities"][0]
        signal_bundle = opportunity["score_evidence"]["signal_bundle"]

        self.assertEqual(signal_bundle["options_bias_alignment_source"], "evidence")
        self.assertEqual(signal_bundle["signals"]["direction_signal"]["source"], "evidence")
        self.assertTrue(opportunity["score_evidence"]["signal_gate"]["eligible"])

    def test_weak_quote_evidence_blocks_signal_gate(self) -> None:
        candidate = _live_selection_candidate()
        candidate["candidate_quote_quality"] = {
            "quality_score": 0.05,
            "quality_state": "weak",
        }
        result = select_live_opportunities(
            label="earnings",
            cycle_id="cycle:test",
            generated_at="2026-04-14T15:00:00+00:00",
            symbol_candidates={"AAPL": [candidate]},
            previous_promotable={},
            previous_selection_memory={},
            top_promotable=5,
            top_monitor=5,
            profile="weekly",
            signal_cycle_context=_uoa_cycle_context(
                quality_score=0.10,
                dominant_flow="call",
                dominant_flow_ratio=0.9,
            ),
        )
        scored_candidate = result["symbol_candidates"]["AAPL"][0]
        signal_gate = scored_candidate["score_evidence"]["signal_gate"]

        self.assertFalse(signal_gate["eligible"])
        self.assertIn("missing_options_bias_alignment", signal_gate["blockers"])


if __name__ == "__main__":
    unittest.main()
