from __future__ import annotations

import unittest

from spreads.integrations.calendar_events.earnings_phase import (
    resolve_earnings_phase_snapshot,
)
from spreads.integrations.calendar_events.models import CalendarEventRecord
from spreads.services.opportunity_scoring import (
    candidate_earnings_phase,
    candidate_event_timing_rule,
)


def _earnings_record(
    *,
    scheduled_at: str,
    when: str = "After Market Close",
) -> CalendarEventRecord:
    return CalendarEventRecord(
        event_id=f"earnings:{scheduled_at}",
        event_type="earnings",
        symbol="AAPL",
        asset_scope=None,
        scheduled_at=scheduled_at,
        window_start=scheduled_at,
        window_end=scheduled_at,
        source="dolt_earnings_calendar",
        source_confidence="low",
        status="scheduled",
        payload_json=f'{{"when":"{when}"}}',
        ingested_at=scheduled_at,
        source_updated_at=scheduled_at,
    )


class EarningsPhaseTests(unittest.TestCase):
    def test_through_event_when_horizon_crosses_report(self) -> None:
        snapshot = resolve_earnings_phase_snapshot(
            records=[
                _earnings_record(scheduled_at="2026-04-16T21:15:00+00:00"),
            ],
            as_of="2026-04-14T15:00:00+00:00",
            horizon_end="2026-04-18T21:00:00+00:00",
        )

        self.assertEqual(snapshot.phase, "through_event")
        self.assertEqual(snapshot.event_date, "2026-04-16")
        self.assertEqual(snapshot.session_timing, "after_close")
        self.assertTrue(snapshot.horizon_crosses_report)

    def test_pre_event_runup_when_report_is_near_but_outside_horizon(self) -> None:
        snapshot = resolve_earnings_phase_snapshot(
            records=[
                _earnings_record(scheduled_at="2026-04-18T21:15:00+00:00"),
            ],
            as_of="2026-04-14T15:00:00+00:00",
            horizon_end="2026-04-16T21:00:00+00:00",
        )

        self.assertEqual(snapshot.phase, "pre_event_runup")
        self.assertEqual(snapshot.days_to_event, 4)
        self.assertFalse(snapshot.horizon_crosses_report)

    def test_post_event_fresh_when_report_was_recent(self) -> None:
        snapshot = resolve_earnings_phase_snapshot(
            records=[
                _earnings_record(scheduled_at="2026-04-12T21:15:00+00:00"),
            ],
            as_of="2026-04-14T15:00:00+00:00",
            horizon_end="2026-04-21T21:00:00+00:00",
        )

        self.assertEqual(snapshot.phase, "post_event_fresh")
        self.assertEqual(snapshot.days_since_event, 2)

    def test_post_event_settled_when_recent_window_has_passed(self) -> None:
        snapshot = resolve_earnings_phase_snapshot(
            records=[
                _earnings_record(scheduled_at="2026-04-05T21:15:00+00:00"),
            ],
            as_of="2026-04-14T15:00:00+00:00",
            horizon_end="2026-04-21T21:00:00+00:00",
        )

        self.assertEqual(snapshot.phase, "post_event_settled")
        self.assertEqual(snapshot.days_since_event, 9)

    def test_candidate_event_timing_rule_uses_explicit_phase(self) -> None:
        candidate = {
            "earnings_phase": "post_event_fresh",
            "calendar_status": "clean",
        }

        self.assertEqual(candidate_earnings_phase(candidate), "post_event_fresh")
        self.assertEqual(candidate_event_timing_rule(candidate), "post_event")


if __name__ == "__main__":
    unittest.main()
