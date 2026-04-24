from __future__ import annotations

import unittest

from core.alerts.discord import build_discord_payload


class DiscordAlertPayloadTests(unittest.TestCase):
    def _runtime_alert(
        self,
        *,
        candidate: dict[str, object],
        execution_admission: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return {
            "symbol": str(candidate["underlying_symbol"]),
            "alert_type": "runtime_entry_selected",
            "created_at": "2026-04-23T15:05:00Z",
            "label": "weekly_auto",
            "profile": "weekly",
            "strategy_mode": str(candidate["strategy"]),
            "description": "noisy description that should not render for runtime alerts",
            "candidate": candidate,
            "details": {
                "execution_mode": "paper",
                "approval_mode": "auto",
                "execution_admission_status": (
                    None
                    if execution_admission is None
                    else execution_admission.get("status")
                ),
                "execution_admission_reason": (
                    None
                    if execution_admission is None
                    else execution_admission.get("reason")
                ),
            },
            "execution_admission": execution_admission,
        }

    def test_short_put_payload_surfaces_pop_and_runtime_status(self) -> None:
        payload = build_discord_payload(
            self._runtime_alert(
                candidate={
                    "underlying_symbol": "IWM",
                    "strategy": "short_put",
                    "expiration_date": "2026-04-27",
                    "days_to_expiration": 4,
                    "underlying_price": 274.11,
                    "probability_of_profit": 0.855,
                    "midpoint_credit": 0.38,
                    "fill_ratio": 0.987,
                    "return_on_risk": 0.426,
                    "short_strike": 270.0,
                    "breakeven": 269.62,
                    "breakeven_vs_expected_move": 3.55,
                    "expected_move": 4.43,
                    "short_open_interest": 3855,
                    "long_open_interest": 3855,
                    "min_quote_size": 7,
                    "short_delta": -0.14,
                    "quality_score": 71.1,
                    "selection_notes": ["outside-em", "good-fill"],
                    "setup_status": "favorable",
                    "calendar_status": "clean",
                    "data_status": "clean",
                    "legs": [
                        {
                            "symbol": "IWM260427P00270000",
                            "role": "short",
                            "position_intent": "sell_to_open",
                            "ratio_qty": "1",
                            "strike": 270.0,
                            "option_type": "put",
                        }
                    ],
                },
                execution_admission={
                    "status": "admissible",
                    "admissible_quantity": 3,
                    "required_buying_power": 27000.0,
                    "available_buying_power": 120000.0,
                    "reserved_buying_power": 0.0,
                },
            )
        )

        embed = payload["embeds"][0]
        self.assertEqual(
            embed["title"], "IWM 2026-04-27 (4DTE) SHORT PUT | ENTRY READY"
        )
        self.assertEqual(
            embed["description"],
            "2026-04-27 (4DTE) | selected for entry | paper | auto | acct qty 3",
        )
        ticket_field = next(
            field for field in embed["fields"] if field["name"] == "Ticket"
        )
        self.assertIn("STO 1x IWM 270P exp 2026-04-27", ticket_field["value"])
        self.assertIn("LIMIT CREDIT $0.38", ticket_field["value"])
        contracts_field = next(
            field for field in embed["fields"] if field["name"] == "Contracts"
        )
        self.assertIn("IWM260427P00270000", contracts_field["value"])
        execution_field = next(
            field for field in embed["fields"] if field["name"] == "Execution"
        )
        self.assertIn("status admissible", execution_field["value"])
        self.assertIn("qty 3", execution_field["value"])
        self.assertIn("req $27,000", execution_field["value"])
        self.assertIn("avail $120,000", execution_field["value"])
        edge_field = next(field for field in embed["fields"] if field["name"] == "Edge")
        self.assertIn("POP 85.5%", edge_field["value"])
        self.assertIn("credit $0.38", edge_field["value"])
        liquidity_field = next(
            field for field in embed["fields"] if field["name"] == "Liquidity"
        )
        self.assertIn("spot $274.11", liquidity_field["value"])
        thesis_field = next(
            field for field in embed["fields"] if field["name"] == "Thesis"
        )
        self.assertEqual(thesis_field["value"], "outside-em | good-fill | favorable")

    def test_credit_spread_payload_uses_width_and_max_loss(self) -> None:
        payload = build_discord_payload(
            {
                "symbol": "SPY",
                "alert_type": "score_breakout",
                "created_at": "2026-04-23T15:05:00Z",
                "label": "core_weekly",
                "profile": "weekly",
                "strategy_mode": "put_credit",
                "description": "SPY score breakout",
                "candidate": {
                    "underlying_symbol": "SPY",
                    "strategy": "put_credit",
                    "expiration_date": "2026-04-28",
                    "days_to_expiration": 5,
                    "underlying_price": 514.21,
                    "probability_of_profit": 0.819,
                    "midpoint_credit": 1.12,
                    "fill_ratio": 0.961,
                    "return_on_risk": 0.28,
                    "width": 5.0,
                    "max_loss": 388.0,
                    "breakeven": 508.88,
                    "expected_move": 6.2,
                    "short_open_interest": 4210,
                    "long_open_interest": 3988,
                    "min_quote_size": 4,
                    "short_delta": -0.18,
                    "quality_score": 74.3,
                    "selection_notes": ["outside-em", "risk-defined"],
                    "setup_status": "favorable",
                    "calendar_status": "clean",
                    "data_status": "clean",
                    "legs": [
                        {
                            "symbol": "SPY260428P00510000",
                            "role": "short",
                            "position_intent": "sell_to_open",
                            "ratio_qty": "1",
                            "strike": 510.0,
                            "option_type": "put",
                        },
                        {
                            "symbol": "SPY260428P00505000",
                            "role": "long",
                            "position_intent": "buy_to_open",
                            "ratio_qty": "1",
                            "strike": 505.0,
                            "option_type": "put",
                        },
                    ],
                },
            }
        )

        embed = payload["embeds"][0]
        self.assertEqual(
            embed["title"], "SPY 2026-04-28 (5DTE) PUT CREDIT SPREAD | SCORE BREAKOUT"
        )
        ticket_field = next(
            field for field in embed["fields"] if field["name"] == "Ticket"
        )
        self.assertIn("STO 1x SPY 510P exp 2026-04-28", ticket_field["value"])
        self.assertIn("BTO 1x SPY 505P exp 2026-04-28", ticket_field["value"])
        self.assertIn("LIMIT CREDIT $1.12", ticket_field["value"])
        risk_field = next(field for field in embed["fields"] if field["name"] == "Risk")
        self.assertIn("width 5", risk_field["value"])
        self.assertIn("max loss $388", risk_field["value"])

    def test_iron_condor_payload_includes_positioning_metrics(self) -> None:
        payload = build_discord_payload(
            self._runtime_alert(
                candidate={
                    "underlying_symbol": "SPY",
                    "strategy": "iron_condor",
                    "expiration_date": "2026-04-27",
                    "days_to_expiration": 4,
                    "underlying_price": 502.24,
                    "probability_of_profit": 0.794,
                    "midpoint_credit": 1.48,
                    "fill_ratio": 0.952,
                    "return_on_risk": 0.423,
                    "width": 5.0,
                    "max_loss": 352.0,
                    "expected_move": 7.10,
                    "lower_breakeven": 493.52,
                    "upper_breakeven": 511.48,
                    "short_vs_expected_move": 1.90,
                    "breakeven_vs_expected_move": 2.38,
                    "side_balance_score": 0.92,
                    "wing_symmetry_ratio": 1.0,
                    "short_open_interest": 5200,
                    "long_open_interest": 4700,
                    "min_quote_size": 3,
                    "quality_score": 76.8,
                    "selection_notes": ["defined-risk", "balanced"],
                    "setup_status": "range-bound",
                    "calendar_status": "clean",
                    "data_status": "clean",
                    "legs": [
                        {
                            "symbol": "SPY260427P00495000",
                            "role": "short",
                            "position_intent": "sell_to_open",
                            "ratio_qty": "1",
                            "strike": 495.0,
                            "option_type": "put",
                        },
                        {
                            "symbol": "SPY260427P00490000",
                            "role": "long",
                            "position_intent": "buy_to_open",
                            "ratio_qty": "1",
                            "strike": 490.0,
                            "option_type": "put",
                        },
                        {
                            "symbol": "SPY260427C00510000",
                            "role": "short",
                            "position_intent": "sell_to_open",
                            "ratio_qty": "1",
                            "strike": 510.0,
                            "option_type": "call",
                        },
                        {
                            "symbol": "SPY260427C00515000",
                            "role": "long",
                            "position_intent": "buy_to_open",
                            "ratio_qty": "1",
                            "strike": 515.0,
                            "option_type": "call",
                        },
                    ],
                },
                execution_admission={
                    "status": "blocked",
                    "reason": "insufficient_broker_buying_power",
                    "admissible_quantity": 0,
                    "required_buying_power": 352.0,
                    "available_buying_power": 200.0,
                    "reserved_buying_power": 100.0,
                },
            )
        )

        embed = payload["embeds"][0]
        self.assertEqual(
            embed["title"], "SPY 2026-04-27 (4DTE) IRON CONDOR | ENTRY READY"
        )
        self.assertEqual(
            embed["description"],
            "2026-04-27 (4DTE) | selected for entry | paper | auto | acct blocked",
        )
        ticket_field = next(
            field for field in embed["fields"] if field["name"] == "Ticket"
        )
        self.assertIn("STO 1x SPY 495P exp 2026-04-27", ticket_field["value"])
        self.assertIn("BTO 1x SPY 490P exp 2026-04-27", ticket_field["value"])
        self.assertIn("STO 1x SPY 510C exp 2026-04-27", ticket_field["value"])
        self.assertIn("BTO 1x SPY 515C exp 2026-04-27", ticket_field["value"])
        self.assertIn("LIMIT CREDIT $1.48", ticket_field["value"])
        positioning_field = next(
            field for field in embed["fields"] if field["name"] == "Positioning"
        )
        self.assertIn("short/EM +1.90", positioning_field["value"])
        self.assertIn("balance 92.0%", positioning_field["value"])
        execution_field = next(
            field for field in embed["fields"] if field["name"] == "Execution"
        )
        self.assertIn("status blocked", execution_field["value"])
        self.assertIn("qty 0", execution_field["value"])
        self.assertIn("why insufficient-broker-buying-power", execution_field["value"])
        self.assertIn("reserved $100", execution_field["value"])
        liquidity_field = next(
            field for field in embed["fields"] if field["name"] == "Liquidity"
        )
        self.assertIn("legs 4", liquidity_field["value"])


if __name__ == "__main__":
    unittest.main()
