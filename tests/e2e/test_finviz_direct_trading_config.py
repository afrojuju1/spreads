from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from core.domain.models import IntradayBar, LiveOptionQuote, OptionContract, OptionSnapshot
from core.jobs.orchestration import resolve_scheduled_for
from core.jobs.specs import get_declared_job_row
from core.services.finviz_direct_trading import run_finviz_direct_trading


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

    def test_finviz_direct_job_prefers_long_calls_with_safety_caps(self) -> None:
        job = get_declared_job_row("finviz_direct_trading:finviz_momentum")

        self.assertIsNotNone(job)
        assert job is not None
        payload = job["payload"]
        self.assertEqual(payload["instrument_mode"], "long_call")
        self.assertEqual(payload["option_execution_runtime"], "alpaca_direct")
        self.assertEqual(payload["equity_fallback"], False)
        self.assertEqual(payload["max_candidates"], 10)
        self.assertEqual(payload["max_new_positions_per_run"], 1)
        self.assertEqual(payload["max_open_positions"], 1)
        self.assertEqual(payload["option_entry_rules"]["max_premium"], 500)
        self.assertEqual(payload["option_exit_rules"]["profit_target_pct"], 0.40)

    def test_finviz_direct_job_arms_long_call_intent(self) -> None:
        fixed_now = datetime(2026, 6, 1, 14, 40, 0, tzinfo=UTC)
        expiration = "2026-06-12"

        class _ExecutionStore:
            def __init__(self) -> None:
                self.intents: dict[str, dict[str, object]] = {}
                self.events: list[dict[str, object]] = []

            def intent_schema_ready(self) -> bool:
                return True

            def portfolio_schema_ready(self) -> bool:
                return True

            def list_positions(self, **_: object) -> list[dict[str, object]]:
                return []

            def list_execution_intents(self, **_: object) -> list[dict[str, object]]:
                return []

            def get_execution_intent(
                self,
                execution_intent_id: str,
            ) -> dict[str, object] | None:
                row = self.intents.get(execution_intent_id)
                return None if row is None else dict(row)

            def upsert_execution_intent(self, **payload: object) -> dict[str, object]:
                self.intents[str(payload["execution_intent_id"])] = dict(payload)
                return dict(payload)

            def append_execution_intent_event(self, **payload: object) -> None:
                self.events.append(dict(payload))

        class _Storage:
            def __init__(self) -> None:
                self.execution = _ExecutionStore()

        class _Client:
            def get_clock(self) -> dict[str, object]:
                return {
                    "is_open": True,
                    "next_close": "2026-06-01T20:00:00Z",
                }

            def list_positions(self) -> list[dict[str, object]]:
                return []

            def list_optionable_underlyings(self) -> list[dict[str, object]]:
                return [{"symbol": "AAPL", "status": "active", "tradable": True}]

            def get_stock_snapshots(
                self,
                symbols: list[str],
                *,
                feed: str,
            ) -> dict[str, dict[str, object]]:
                return {
                    symbol: {
                        "latestQuote": {
                            "bp": 100.00,
                            "ap": 100.10,
                            "t": fixed_now.isoformat(),
                        }
                    }
                    for symbol in symbols
                }

            def get_intraday_bars(self, *_: object, **__: object) -> list[IntradayBar]:
                return [
                    IntradayBar(
                        timestamp=(fixed_now - timedelta(minutes=5 - index)).isoformat(),
                        open=99.5,
                        high=100.0,
                        low=99.0,
                        close=99.8,
                        volume=100_000,
                    )
                    for index in range(5)
                ]

            def list_option_contracts(
                self,
                symbol: str,
                min_expiration: str,
                max_expiration: str,
                *,
                option_type: str = "call",
                status: str = "active",
            ) -> list[OptionContract]:
                return [
                    OptionContract(
                        symbol="AAPL260612C00100000",
                        expiration_date=expiration,
                        strike_price=100.0,
                        open_interest=500,
                        close_price=None,
                    )
                ]

            def get_option_chain_snapshots(
                self,
                symbol: str,
                expiration_date: str,
                option_type: str,
                feed: str,
            ) -> dict[str, OptionSnapshot]:
                return {
                    "AAPL260612C00100000": OptionSnapshot(
                        symbol="AAPL260612C00100000",
                        bid=4.80,
                        ask=5.00,
                        bid_size=20,
                        ask_size=20,
                        midpoint=4.90,
                        delta=0.52,
                        gamma=None,
                        theta=None,
                        vega=None,
                        implied_volatility=0.35,
                        last_trade_price=None,
                        daily_volume=1000,
                        greeks_source="alpaca",
                    )
                }

            def get_latest_option_quotes(
                self,
                symbols: list[str],
                *,
                feed: str,
            ) -> dict[str, LiveOptionQuote]:
                return {
                    symbol: LiveOptionQuote(
                        symbol=symbol,
                        bid=4.85,
                        ask=4.95,
                        bid_size=30,
                        ask_size=30,
                        timestamp=(fixed_now - timedelta(seconds=10)).isoformat(),
                    )
                    for symbol in symbols
                }

        storage = _Storage()
        feed_snapshot = {
            "status": "ready",
            "feed_id": "finviz_momentum",
            "job_key": "symbol_feed:finviz_momentum",
            "job_run_id": "feed-run-1",
            "entries": [
                {
                    "symbol": "AAPL",
                    "score": 99.0,
                    "price": 100.0,
                    "daily_volume": 2_000_000,
                    "move_percent": 2.4,
                    "relative_volume": 2.0,
                }
            ],
        }
        payload = dict(get_declared_job_row("finviz_direct_trading:finviz_momentum")["payload"])
        payload["dispatch_after_trigger"] = False

        with (
            patch("core.services.finviz_direct_trading._now", return_value=fixed_now),
            patch(
                "core.services.finviz_direct_trading.get_latest_symbol_feed_snapshot",
                return_value=feed_snapshot,
            ),
            patch(
                "core.services.finviz_direct_trading.create_alpaca_client_from_env",
                return_value=_Client(),
            ),
        ):
            result = run_finviz_direct_trading(
                db_target="postgresql://example",
                storage=storage,
                job_store=object(),
                job_run_id="trading-run-1",
                payload=payload,
                heartbeat=lambda: None,
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["armed"], 1)
        self.assertEqual(result["entry_armed"], 1)
        intent = next(iter(storage.execution.intents.values()))
        intent_payload = intent["payload"]
        assert isinstance(intent_payload, dict)
        self.assertEqual(intent_payload["asset_class"], "option")
        self.assertEqual(intent_payload["symbol"], "AAPL260612C00100000")
        self.assertEqual(intent_payload["underlying_symbol"], "AAPL")
        self.assertEqual(intent_payload["strategy_family"], "long_call")
        self.assertEqual(intent_payload["execution_runtime"], "alpaca_direct")
        self.assertEqual(intent_payload["side"], "buy")
        self.assertEqual(intent_payload["quantity"], 1)
        self.assertEqual(intent_payload["limit_price"], 4.95)
