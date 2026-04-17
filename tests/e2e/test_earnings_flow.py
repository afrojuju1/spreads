from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from core.services.live_collector_health import build_selection_summary
from core.services.live_selection import select_live_opportunities
from core.services.ops import (
    build_job_run_view,
    build_system_status,
    build_trading_health,
)
from core.services.opportunity_replay import (
    _build_horizon_intents,
    _build_opportunities,
    _build_regime_snapshots,
    _build_strategy_intents,
    _flatten_opportunity_rows,
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _future_iso(hours: int = 1) -> str:
    return (
        (datetime.now(UTC) + timedelta(hours=hours))
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


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


def _uoa_cycle_context(
    *,
    quality_score: float,
    dominant_flow: str,
    dominant_flow_ratio: float,
) -> dict[str, object]:
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


def _cycle() -> dict[str, object]:
    return {
        "cycle_id": "cycle:test:earnings",
        "label": "earnings",
        "session_date": "2026-04-14",
        "session_id": "historical:earnings:2026-04-14",
        "profile": "weekly",
        "generated_at": "2026-04-14T15:00:00+00:00",
        "strategy": "mixed",
    }


def _candidate_row(
    *,
    candidate_id: int,
    symbol: str,
    strategy: str,
    expiration_date: str,
    short_symbol: str,
    long_symbol: str,
    days_to_expiration: int,
    earnings_phase: str,
    setup_score: float,
    setup_intraday_score: float,
    fill_ratio: float,
    quality_score: float,
    expected_move_pct: float | None = None,
    options_bias_alignment: bool | None = None,
    debit_width_ratio: float | None = None,
    modeled_move_vs_implied_move: float | None = None,
    order_legs: list[dict[str, object]] | None = None,
    candidate_overrides: dict[str, object] | None = None,
) -> dict[str, object]:
    candidate = {
        "underlying_symbol": symbol,
        "strategy": strategy,
        "profile": "weekly",
        "expiration_date": expiration_date,
        "days_to_expiration": days_to_expiration,
        "quality_score": quality_score,
        "setup_score": setup_score,
        "setup_status": "favorable",
        "setup_intraday_score": setup_intraday_score,
        "fill_ratio": fill_ratio,
        "data_status": "clean",
        "calendar_status": "clean",
        "earnings_phase": earnings_phase,
        "earnings_timing_confidence": "high",
        "earnings_event_date": "2026-04-18",
        "earnings_session_timing": "after_close",
        "return_on_risk": 0.22,
        "max_loss": 200.0,
        "width": 5.0,
        "midpoint_credit": 1.25,
        "natural_credit": 1.1,
        "order_payload": {
            "legs": order_legs
            or [
                {
                    "symbol": short_symbol,
                    "side": "sell",
                    "position_intent": "open",
                    "ratio_qty": "1",
                },
                {
                    "symbol": long_symbol,
                    "side": "buy",
                    "position_intent": "open",
                    "ratio_qty": "1",
                },
            ]
        },
        "short_open_interest": 1200,
        "long_open_interest": 1100,
        "short_volume": 600,
        "long_volume": 550,
    }
    if expected_move_pct is not None:
        candidate["expected_move_pct"] = expected_move_pct
    if options_bias_alignment is not None:
        candidate["options_bias_alignment"] = options_bias_alignment
    if debit_width_ratio is not None:
        candidate["debit_width_ratio"] = debit_width_ratio
    if modeled_move_vs_implied_move is not None:
        candidate["modeled_move_vs_implied_move"] = modeled_move_vs_implied_move
    if candidate_overrides:
        candidate.update(candidate_overrides)
    return {
        "candidate_id": candidate_id,
        "underlying_symbol": symbol,
        "strategy": strategy,
        "expiration_date": expiration_date,
        "short_symbol": short_symbol,
        "long_symbol": long_symbol,
        "bucket": "promotable",
        "position": "top",
        "candidate": candidate,
    }


def _run_replay_flow(rows: list[dict[str, object]]) -> dict[str, object]:
    cycle = _cycle()
    regime_snapshots = _build_regime_snapshots(cycle=cycle, candidates=rows)
    strategy_intents = _build_strategy_intents(
        cycle=cycle,
        candidates=rows,
        regime_snapshots=regime_snapshots,
    )
    horizon_intents = _build_horizon_intents(
        cycle=cycle,
        strategy_intents=strategy_intents,
        candidates=rows,
    )
    opportunities = _build_opportunities(
        cycle=cycle,
        candidates=rows,
        strategy_intents=strategy_intents,
        horizon_intents=horizon_intents,
        dimension_lookup={},
    )
    flat_rows = _flatten_opportunity_rows(
        session={
            "label": cycle["label"],
            "session_date": cycle["session_date"],
            "cycle_id": cycle["cycle_id"],
        },
        opportunities=opportunities,
        allocation_decisions=[],
        comparison={},
        outcome_matches={},
    )
    return {
        "strategy_intents": strategy_intents,
        "horizon_intents": horizon_intents,
        "opportunities": opportunities,
        "rows": flat_rows,
    }


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
                    "lease_key": f"{prefix}worker-runtime",
                    "owner": "worker-runtime",
                    "expires_at": _future_iso(),
                    "job_run_id": "worker-run",
                }
            ]
        return []

    def list_job_runs(self, **_: object) -> list[dict[str, object]]:
        return []

    def list_job_definitions(self, **_: object) -> list[dict[str, object]]:
        return [dict(self.definition)]

    def list_latest_runs_by_job_keys(
        self,
        *,
        job_keys: list[str],
        **_: object,
    ) -> list[dict[str, object]]:
        if self.definition["job_key"] not in job_keys:
            return []
        return [dict(self.run_record)]

    def list_latest_runs_by_session_ids(
        self,
        *,
        session_ids: list[str],
        **_: object,
    ) -> list[dict[str, object]]:
        return [dict(self.run_record)] if session_ids else []

    def get_job_run(self, job_run_id: str) -> dict[str, object] | None:
        if job_run_id != self.run_record["job_run_id"]:
            return None
        return dict(self.run_record)

    def get_job_definition(self, job_key: str) -> dict[str, object] | None:
        if job_key != self.definition["job_key"]:
            return None
        return dict(self.definition)

    def get_latest_live_collector_run(
        self,
        *,
        label: str | None = None,
        status: str | None = "succeeded",
    ) -> dict[str, object] | None:
        del label
        if status != "succeeded":
            return None
        return dict(self.run_record)

    def get_live_collector_run_by_cycle_id(
        self,
        *,
        cycle_id: str,
        label: str,
        status: str | None = "succeeded",
    ) -> dict[str, object] | None:
        del label
        if cycle_id != "cycle-1" or status != "succeeded":
            return None
        return dict(self.run_record)


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

    def intent_schema_ready(self) -> bool:
        return False

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


class EarningsFlowTests(unittest.TestCase):
    def test_earnings_runtime_views_keep_selection_counts_consistent(self) -> None:
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
            "worker_name": "worker-runtime",
            "retry_count": 0,
            "session_id": "session-1",
            "slot_at": _now_iso(),
            "payload": {
                "label": "earnings",
                "profile": "weekly",
                "singleton_scope": "earnings",
            },
            "result": {
                "label": "earnings",
                "profile": "weekly",
                "quote_events_saved": 10,
                "baseline_quote_events_saved": 2,
                "stream_quote_events_saved": 8,
                "trade_events_saved": 5,
                "stream_trade_events_saved": 5,
                "expected_quote_symbols": [
                    "AAPL260424C205",
                    "AAPL260424C210",
                ],
                "expected_trade_symbols": ["AAPL260424C205"],
                "selection_summary": selection_summary,
                "live_action_gate": {
                    "status": "pass",
                    "allow_auto_execution": True,
                },
                "uoa_summary": {},
                "uoa_quote_summary": {},
                "uoa_decisions": {},
            },
        }
        storage = _FakeStorage(run_record)

        with (
            patch(
                "core.services.ops.get_control_state_snapshot",
                return_value={"mode": "normal"},
            ),
            patch(
                "core.services.ops.get_account_overview",
                return_value={
                    "source": "live",
                    "environment": "paper",
                    "account": {
                        "equity": 10000.0,
                        "cash": 5000.0,
                        "buying_power": 10000.0,
                    },
                    "pnl": {"day_change": 0.0, "day_change_percent": 0.0},
                    "sync": {},
                },
            ),
        ):
            system_status = build_system_status(storage=storage)
            trading_health = build_trading_health(storage=storage)
            job_view = build_job_run_view(storage=storage, job_run_id="run-1")

        self.assertEqual(system_status["summary"]["collector_opportunity_count"], 2)
        self.assertEqual(system_status["summary"]["collector_shadow_only_count"], 1)
        self.assertEqual(
            system_status["summary"]["collector_auto_live_eligible_count"],
            1,
        )
        self.assertEqual(
            trading_health["summary"]["collector_opportunity_count"],
            2,
        )
        self.assertEqual(
            trading_health["details"]["collector_selection"]["selection_state_counts"][
                "promotable"
            ],
            1,
        )
        self.assertEqual(
            job_view["details"]["selection_summary"]["earnings_phase_counts"][
                "post_event_fresh"
            ],
            1,
        )
        self.assertEqual(
            job_view["details"]["selection_summary"]["blocker_counts"]["signal_gate"][
                "neutral_regime_signal_too_low"
            ],
            1,
        )

    def test_earnings_live_selection_prefers_options_evidence_and_blocks_weak_quotes(
        self,
    ) -> None:
        strong_result = select_live_opportunities(
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
        strong_opportunity = strong_result["opportunities"][0]
        strong_signal_bundle = strong_opportunity["score_evidence"]["signal_bundle"]

        self.assertEqual(
            strong_signal_bundle["options_bias_alignment_source"],
            "evidence",
        )
        self.assertEqual(
            strong_signal_bundle["signals"]["direction_signal"]["source"],
            "evidence",
        )
        self.assertTrue(strong_opportunity["score_evidence"]["signal_gate"]["eligible"])

        weak_candidate = _live_selection_candidate()
        weak_candidate["candidate_quote_quality"] = {
            "quality_score": 0.05,
            "quality_state": "weak",
        }
        weak_result = select_live_opportunities(
            label="earnings",
            cycle_id="cycle:test",
            generated_at="2026-04-14T15:00:00+00:00",
            symbol_candidates={"AAPL": [weak_candidate]},
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
        scored_candidate = weak_result["symbol_candidates"]["AAPL"][0]
        signal_gate = scored_candidate["score_evidence"]["signal_gate"]

        self.assertFalse(signal_gate["eligible"])
        self.assertIn("missing_options_bias_alignment", signal_gate["blockers"])

    def test_pre_event_call_debit_reaches_replay_row_with_derived_signals(self) -> None:
        payload = _run_replay_flow(
            [
                _candidate_row(
                    candidate_id=1,
                    symbol="AAPL",
                    strategy="call_debit",
                    expiration_date="2026-04-20",
                    short_symbol="AAPL260420C210",
                    long_symbol="AAPL260420C205",
                    days_to_expiration=6,
                    earnings_phase="pre_event_runup",
                    setup_score=84.0,
                    setup_intraday_score=86.0,
                    fill_ratio=0.93,
                    quality_score=88.0,
                    options_bias_alignment=True,
                    debit_width_ratio=0.40,
                )
            ]
        )

        strategy_intent = payload["strategy_intents"][0]
        opportunity = payload["opportunities"][0]
        row = payload["rows"][0]

        self.assertEqual(strategy_intent.strategy_family, "call_debit_spread")
        self.assertEqual(strategy_intent.policy_state, "preferred")
        self.assertEqual(opportunity.evidence["event_timing_rule"], "avoid_event")
        self.assertTrue(opportunity.evidence["signal_gate"]["active"])
        self.assertTrue(opportunity.evidence["signal_gate"]["eligible"])
        self.assertGreaterEqual(row["direction_signal"], 0.65)
        self.assertGreaterEqual(row["pricing_signal"], 0.55)
        self.assertEqual(row["event_state"], "pre_event_runup")
        self.assertTrue(row["signal_gate_eligible"])

    def test_through_event_long_straddle_reaches_replay_row_with_derived_signals(
        self,
    ) -> None:
        payload = _run_replay_flow(
            [
                _candidate_row(
                    candidate_id=2,
                    symbol="NFLX",
                    strategy="long_straddle",
                    expiration_date="2026-04-18",
                    short_symbol="NFLX260418C900",
                    long_symbol="NFLX260418P900",
                    days_to_expiration=4,
                    earnings_phase="through_event",
                    setup_score=78.0,
                    setup_intraday_score=79.0,
                    fill_ratio=0.91,
                    quality_score=91.0,
                    expected_move_pct=0.045,
                    modeled_move_vs_implied_move=1.20,
                )
            ]
        )

        strategy_intent = payload["strategy_intents"][0]
        horizon_intent = payload["horizon_intents"][0]
        opportunity = payload["opportunities"][0]
        row = payload["rows"][0]

        self.assertEqual(strategy_intent.strategy_family, "long_straddle")
        self.assertEqual(strategy_intent.policy_state, "preferred")
        self.assertEqual(horizon_intent.event_timing_rule, "include_event")
        self.assertEqual(opportunity.evidence["earnings_phase"], "through_event")
        self.assertTrue(opportunity.evidence["signal_gate"]["active"])
        self.assertTrue(opportunity.evidence["signal_gate"]["eligible"])
        self.assertGreaterEqual(row["jump_risk_signal"], 0.70)
        self.assertGreaterEqual(row["pricing_signal"], 0.60)
        self.assertEqual(row["event_state"], "through_event")
        self.assertTrue(row["signal_gate_active"])

    def test_post_event_iron_condor_reaches_replay_row_with_derived_signals(
        self,
    ) -> None:
        payload = _run_replay_flow(
            [
                _candidate_row(
                    candidate_id=3,
                    symbol="SPY",
                    strategy="iron_condor",
                    expiration_date="2026-04-22",
                    short_symbol="SPY260422P500",
                    long_symbol="SPY260422P495",
                    days_to_expiration=8,
                    earnings_phase="post_event_fresh",
                    setup_score=62.0,
                    setup_intraday_score=59.0,
                    fill_ratio=0.94,
                    quality_score=90.0,
                    expected_move_pct=0.033,
                    order_legs=[
                        {
                            "symbol": "SPY260422P500",
                            "side": "sell",
                            "position_intent": "open",
                            "ratio_qty": "1",
                        },
                        {
                            "symbol": "SPY260422P495",
                            "side": "buy",
                            "position_intent": "open",
                            "ratio_qty": "1",
                        },
                        {
                            "symbol": "SPY260422C520",
                            "side": "sell",
                            "position_intent": "open",
                            "ratio_qty": "1",
                        },
                        {
                            "symbol": "SPY260422C525",
                            "side": "buy",
                            "position_intent": "open",
                            "ratio_qty": "1",
                        },
                    ],
                    candidate_overrides={
                        "earnings_event_date": "2026-04-11",
                        "setup_status": "neutral",
                        "setup_spot_vs_vwap_pct": 0.0006,
                        "setup_intraday_return_pct": 0.0011,
                        "setup_distance_to_session_extreme_pct": 0.0105,
                        "setup_opening_range_break_pct": 0.0004,
                        "setup_latest_close": 505.1,
                        "setup_opening_range_high": 505.8,
                        "setup_opening_range_low": 504.4,
                        "short_implied_volatility": 0.47,
                        "long_implied_volatility": 0.44,
                        "dominant_flow": "mixed",
                    },
                )
            ]
        )

        strategy_intent = payload["strategy_intents"][0]
        horizon_intent = payload["horizon_intents"][0]
        opportunity = payload["opportunities"][0]
        row = payload["rows"][0]

        self.assertEqual(strategy_intent.strategy_family, "iron_condor")
        self.assertEqual(strategy_intent.policy_state, "allowed")
        self.assertEqual(horizon_intent.event_timing_rule, "post_event")
        self.assertEqual(len(opportunity.legs), 4)
        self.assertEqual(opportunity.evidence["earnings_phase"], "post_event_fresh")
        self.assertTrue(opportunity.evidence["signal_gate"]["active"])
        self.assertTrue(opportunity.evidence["signal_gate"]["eligible"])
        self.assertGreaterEqual(row["neutral_regime_signal"], 0.60)
        self.assertGreaterEqual(row["residual_iv_richness"], 0.60)
        self.assertEqual(row["event_state"], "post_event_fresh")
        self.assertTrue(row["signal_gate_eligible"])


if __name__ == "__main__":
    unittest.main()
