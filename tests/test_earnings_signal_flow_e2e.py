from __future__ import annotations

import unittest

from spreads.services.opportunity_replay import (
    _build_horizon_intents,
    _build_opportunities,
    _build_regime_snapshots,
    _build_strategy_intents,
    _flatten_opportunity_rows,
)


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
            "legs": [
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
        "cycle": cycle,
        "regime_snapshots": regime_snapshots,
        "strategy_intents": strategy_intents,
        "horizon_intents": horizon_intents,
        "opportunities": opportunities,
        "rows": flat_rows,
    }


class EarningsSignalFlowE2ETests(unittest.TestCase):
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
                ),
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

    def test_through_event_long_straddle_flows_to_replay_row(self) -> None:
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
                ),
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


if __name__ == "__main__":
    unittest.main()
