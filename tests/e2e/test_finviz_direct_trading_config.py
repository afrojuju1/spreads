from __future__ import annotations

import unittest
from datetime import UTC, datetime

from core.jobs.orchestration import resolve_scheduled_for
from core.jobs.specs import get_declared_job_row


class FinvizDirectTradingConfigTests(unittest.TestCase):
    def test_finviz_feed_refreshes_during_market_hours(self) -> None:
        feed = get_declared_job_row("symbol_feed:finviz_momentum")

        self.assertIsNotNone(feed)
        assert feed is not None
        self.assertEqual(feed["schedule_type"], "interval_minutes")
        self.assertEqual(feed["schedule"], {"minutes": 2})
        self.assertEqual(feed["payload"]["allow_off_hours"], False)

        first_slot = resolve_scheduled_for(
            feed,
            now=datetime(2026, 4, 15, 14, 31, 5, tzinfo=UTC),
        )
        second_slot = resolve_scheduled_for(
            feed,
            now=datetime(2026, 4, 15, 14, 33, 5, tzinfo=UTC),
        )

        self.assertEqual(first_slot, datetime(2026, 4, 15, 14, 30, tzinfo=UTC))
        self.assertEqual(second_slot, datetime(2026, 4, 15, 14, 32, tzinfo=UTC))

    def test_finviz_feed_stays_market_hours_only(self) -> None:
        feed = get_declared_job_row("symbol_feed:finviz_momentum")

        self.assertIsNotNone(feed)
        assert feed is not None
        self.assertIsNone(
            resolve_scheduled_for(
                feed,
                now=datetime(2026, 4, 15, 13, 29, 0, tzinfo=UTC),
            )
        )
