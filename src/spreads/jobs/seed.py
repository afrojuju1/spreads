from __future__ import annotations

import argparse

from spreads.runtime.config import default_database_url
from spreads.storage.factory import build_job_repository

DEFAULT_AUTO_EXECUTION_POLICY = {
    "enabled": True,
    "mode": "top_board",
    "quantity": 1,
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed default ARQ-managed job definitions.")
    parser.add_argument("--db", default=default_database_url(), help="Postgres database URL.")
    return parser.parse_args(argv)


def seed_definitions(db: str) -> list[str]:
    repo = build_job_repository(db)
    job_keys: list[str] = []
    try:
        definitions = [
            {
                "job_key": "live_collector:explore_10_combined_0dte_auto",
                "job_type": "live_collector",
                "enabled": True,
                "schedule_type": "market_open_plus_minutes",
                "schedule": {"minutes": -5},
                "payload": {
                    "universe": "explore_10",
                    "strategy": "combined",
                    "profile": "0dte",
                    "greeks_source": "auto",
                    "top": 10,
                    "per_symbol_top": 1,
                    "interval_seconds": 300,
                    "max_slot_retries": 3,
                    "quote_capture_seconds": 20,
                    "allow_off_hours": False,
                    "session_start_offset_minutes": -5,
                    "session_end_offset_minutes": 5,
                    "execution_policy": dict(DEFAULT_AUTO_EXECUTION_POLICY),
                },
                "singleton_scope": "explore_10_combined_0dte_auto",
            },
            {
                "job_key": "live_collector:explore_10_combined_weekly_auto",
                "job_type": "live_collector",
                "enabled": True,
                "schedule_type": "market_open_plus_minutes",
                "schedule": {"minutes": -5},
                "payload": {
                    "universe": "explore_10",
                    "strategy": "combined",
                    "profile": "weekly",
                    "greeks_source": "auto",
                    "top": 10,
                    "per_symbol_top": 1,
                    "interval_seconds": 300,
                    "max_slot_retries": 3,
                    "quote_capture_seconds": 20,
                    "allow_off_hours": False,
                    "session_start_offset_minutes": -5,
                    "session_end_offset_minutes": 5,
                    "execution_policy": dict(DEFAULT_AUTO_EXECUTION_POLICY),
                },
                "singleton_scope": "explore_10_combined_weekly_auto",
            },
            {
                "job_key": "live_collector:explore_10_combined_core_auto",
                "job_type": "live_collector",
                "enabled": True,
                "schedule_type": "market_open_plus_minutes",
                "schedule": {"minutes": -5},
                "payload": {
                    "universe": "explore_10",
                    "strategy": "combined",
                    "profile": "core",
                    "greeks_source": "auto",
                    "top": 10,
                    "per_symbol_top": 1,
                    "interval_seconds": 300,
                    "max_slot_retries": 3,
                    "quote_capture_seconds": 20,
                    "allow_off_hours": False,
                    "session_start_offset_minutes": -5,
                    "session_end_offset_minutes": 5,
                    "execution_policy": dict(DEFAULT_AUTO_EXECUTION_POLICY),
                },
                "singleton_scope": "explore_10_combined_core_auto",
            },
            {
                "job_key": "post_close_analysis:live_collectors",
                "job_type": "post_close_analysis",
                "enabled": True,
                "schedule_type": "market_close_plus_minutes",
                "schedule": {"minutes": 15},
                "payload": {
                    "date": "today",
                    "replay_profit_target": 0.5,
                    "replay_stop_multiple": 2.0,
                },
                "singleton_scope": None,
            },
            {
                "job_key": "post_market_analysis:live_collectors",
                "job_type": "post_market_analysis",
                "enabled": True,
                "schedule_type": "market_close_plus_minutes",
                "schedule": {"minutes": 45},
                "payload": {
                    "date": "today",
                    "replay_profit_target": 0.5,
                    "replay_stop_multiple": 2.0,
                },
                "singleton_scope": None,
            },
            {
                "job_key": "post_close_analysis:explore_10_combined_0dte_auto",
                "job_type": "post_close_analysis",
                "enabled": False,
                "schedule_type": "market_close_plus_minutes",
                "schedule": {"minutes": 15},
                "payload": {
                    "date": "today",
                    "label": "explore_10_combined_0dte_auto",
                    "replay_profit_target": 0.5,
                    "replay_stop_multiple": 2.0,
                },
                "singleton_scope": None,
            },
            {
                "job_key": "post_close_analysis:explore_10_combined_weekly_auto",
                "job_type": "post_close_analysis",
                "enabled": False,
                "schedule_type": "market_close_plus_minutes",
                "schedule": {"minutes": 15},
                "payload": {
                    "date": "today",
                    "label": "explore_10_combined_weekly_auto",
                    "replay_profit_target": 0.5,
                    "replay_stop_multiple": 2.0,
                },
                "singleton_scope": None,
            },
            {
                "job_key": "post_market_analysis:explore_10_combined_0dte_auto",
                "job_type": "post_market_analysis",
                "enabled": False,
                "schedule_type": "market_close_plus_minutes",
                "schedule": {"minutes": 45},
                "payload": {
                    "date": "today",
                    "label": "explore_10_combined_0dte_auto",
                    "replay_profit_target": 0.5,
                    "replay_stop_multiple": 2.0,
                },
                "singleton_scope": None,
            },
            {
                "job_key": "post_market_analysis:explore_10_combined_weekly_auto",
                "job_type": "post_market_analysis",
                "enabled": False,
                "schedule_type": "market_close_plus_minutes",
                "schedule": {"minutes": 45},
                "payload": {
                    "date": "today",
                    "label": "explore_10_combined_weekly_auto",
                    "replay_profit_target": 0.5,
                    "replay_stop_multiple": 2.0,
                },
                "singleton_scope": None,
            },
        ]
        for definition in definitions:
            repo.upsert_job_definition(
                job_key=definition["job_key"],
                job_type=definition["job_type"],
                enabled=definition["enabled"],
                schedule_type=definition["schedule_type"],
                schedule=definition["schedule"],
                payload=definition["payload"],
                singleton_scope=definition["singleton_scope"],
            )
            job_keys.append(definition["job_key"])
    finally:
        repo.close()
    return job_keys


def main() -> int:
    args = parse_args()
    keys = seed_definitions(args.db)
    for key in keys:
        print(key)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
