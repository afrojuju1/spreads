from __future__ import annotations

import unittest

from spreads.services.selection_summary import (
    aggregate_selection_summaries,
    live_selection_counts,
    selection_summary_payload,
)


class SelectionSummaryTests(unittest.TestCase):
    def test_live_selection_counts_only_counts_live_promotable_and_monitor(self) -> None:
        counts = live_selection_counts(
            [
                {"selection_state": "promotable", "eligibility": "live"},
                {"selection_state": "monitor", "eligibility": "live"},
                {"selection_state": "promotable", "eligibility": "analysis_only"},
                {"selection_state": "rejected", "eligibility": "live"},
            ]
        )

        self.assertEqual(counts, {"promotable": 1, "monitor": 1})

    def test_selection_summary_payload_coerces_nested_count_maps(self) -> None:
        payload = selection_summary_payload(
            {
                "opportunity_count": "3",
                "selection_state_counts": {"promotable": "2", "monitor": 1},
                "blocker_counts": {"policy": {"risk_limit": "2"}},
                "shadow_only_count": "1",
                "auto_live_eligible_count": 1,
            }
        )

        self.assertEqual(payload["opportunity_count"], 3)
        self.assertEqual(payload["selection_state_counts"]["promotable"], 2)
        self.assertEqual(payload["blocker_counts"]["policy"]["risk_limit"], 2)
        self.assertEqual(payload["shadow_only_count"], 1)
        self.assertEqual(payload["auto_live_eligible_count"], 1)

    def test_aggregate_selection_summaries_merges_counts(self) -> None:
        summary = aggregate_selection_summaries(
            [
                {
                    "opportunity_count": 2,
                    "selection_state_counts": {"promotable": 1},
                    "strategy_family_counts": {"call_debit_spread": 1},
                    "blocker_counts": {"policy": {"risk_limit": 1}},
                    "shadow_only_count": 1,
                    "auto_live_eligible_count": 0,
                },
                {
                    "opportunity_count": "3",
                    "selection_state_counts": {"monitor": "2"},
                    "strategy_family_counts": {"call_debit_spread": "2"},
                    "blocker_counts": {"policy": {"risk_limit": "2"}},
                    "shadow_only_count": "0",
                    "auto_live_eligible_count": "1",
                },
            ]
        )

        self.assertEqual(summary["opportunity_count"], 5)
        self.assertEqual(summary["selection_state_counts"], {"promotable": 1, "monitor": 2})
        self.assertEqual(summary["strategy_family_counts"], {"call_debit_spread": 3})
        self.assertEqual(summary["blocker_counts"]["policy"]["risk_limit"], 3)
        self.assertEqual(summary["shadow_only_count"], 1)
        self.assertEqual(summary["auto_live_eligible_count"], 1)


if __name__ == "__main__":
    unittest.main()
