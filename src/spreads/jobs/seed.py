from __future__ import annotations

import argparse

from spreads.jobs.registry import (
    ALERT_RECONCILE_JOB_KEY,
    ALERT_RECONCILE_JOB_TYPE,
    BROKER_SYNC_JOB_TYPE,
    EXECUTION_SUBMIT_ADHOC_JOB_KEY,
    EXECUTION_SUBMIT_JOB_TYPE,
    GENERATOR_ADHOC_JOB_KEY,
    GENERATOR_JOB_TYPE,
    LIVE_COLLECTOR_JOB_TYPE,
    POST_CLOSE_ANALYSIS_ADHOC_JOB_KEY,
    POST_CLOSE_ANALYSIS_JOB_TYPE,
    POST_MARKET_ANALYSIS_JOB_TYPE,
    SESSION_EXIT_MANAGER_JOB_TYPE,
)
from spreads.runtime.config import default_database_url
from spreads.storage.factory import build_job_repository

DEFAULT_AUTO_EXECUTION_POLICY = {
    "enabled": True,
    "mode": "top_promotable",
    "quantity": 1,
    "pricing_mode": "adaptive_credit",
    "min_credit_retention_pct": 0.95,
    "max_credit_concession": 0.02,
}

DEFAULT_AUTO_RISK_POLICY = {
    "enabled": True,
    "allow_live": False,
    "max_open_positions_per_session": 1,
    "max_open_positions_per_underlying": 1,
    "max_open_positions_per_underlying_strategy": 1,
    "max_contracts_per_position": 1,
    "max_contracts_per_session": 1,
    "max_position_notional": 1000.0,
    "max_session_notional": 1000.0,
    "max_position_max_loss": 1000.0,
    "max_session_max_loss": 1000.0,
    "stale_quote_after_seconds": 900,
}

DEFAULT_AUTO_EXIT_POLICY = {
    "enabled": True,
    "profit_target_pct": 0.5,
    "stop_multiple": 2.0,
    "force_close_minutes_before_close": 10,
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
                "job_key": ALERT_RECONCILE_JOB_KEY,
                "job_type": ALERT_RECONCILE_JOB_TYPE,
                "enabled": True,
                "schedule_type": "interval_minutes",
                "schedule": {"minutes": 1},
                "payload": {
                    "limit": 200,
                    "stale_after_seconds": 300,
                    "allow_off_hours": True,
                },
                "singleton_scope": "global",
            },
            {
                "job_key": "broker_sync:alpaca",
                "job_type": BROKER_SYNC_JOB_TYPE,
                "enabled": True,
                "schedule_type": "interval_minutes",
                "schedule": {"minutes": 1},
                "payload": {
                    "history_range": "1D",
                    "activity_lookback_days": 1,
                    "allow_off_hours": False,
                    "post_close_grace_minutes": 5,
                },
                "singleton_scope": "alpaca",
            },
            {
                "job_key": "session_exit_manager:live",
                "job_type": SESSION_EXIT_MANAGER_JOB_TYPE,
                "enabled": True,
                "schedule_type": "interval_minutes",
                "schedule": {"minutes": 1},
                "payload": {
                    "allow_off_hours": False,
                },
                "singleton_scope": "global",
            },
            {
                "job_key": "live_collector:explore_10_combined_0dte_auto",
                "job_type": LIVE_COLLECTOR_JOB_TYPE,
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
                    "interval_seconds": 60,
                    "backfill_missed_slots": False,
                    "max_slot_retries": 3,
                    "quote_capture_seconds": 20,
                    "trade_capture_seconds": 10,
                    "allow_off_hours": False,
                    "session_start_offset_minutes": -5,
                    "session_end_offset_minutes": 5,
                    "execution_policy": dict(DEFAULT_AUTO_EXECUTION_POLICY),
                    "risk_policy": dict(DEFAULT_AUTO_RISK_POLICY),
                    "exit_policy": dict(DEFAULT_AUTO_EXIT_POLICY),
                },
                "singleton_scope": "explore_10_combined_0dte_auto",
            },
            {
                "job_key": "live_collector:explore_10_combined_weekly_auto",
                "job_type": LIVE_COLLECTOR_JOB_TYPE,
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
                    "trade_capture_seconds": 30,
                    "allow_off_hours": False,
                    "session_start_offset_minutes": -5,
                    "session_end_offset_minutes": 5,
                    "execution_policy": dict(DEFAULT_AUTO_EXECUTION_POLICY),
                    "risk_policy": dict(DEFAULT_AUTO_RISK_POLICY),
                    "exit_policy": dict(DEFAULT_AUTO_EXIT_POLICY),
                },
                "singleton_scope": "explore_10_combined_weekly_auto",
            },
            {
                "job_key": "live_collector:explore_10_combined_core_auto",
                "job_type": LIVE_COLLECTOR_JOB_TYPE,
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
                    "trade_capture_seconds": 30,
                    "allow_off_hours": False,
                    "session_start_offset_minutes": -5,
                    "session_end_offset_minutes": 5,
                    "execution_policy": dict(DEFAULT_AUTO_EXECUTION_POLICY),
                    "risk_policy": dict(DEFAULT_AUTO_RISK_POLICY),
                    "exit_policy": dict(DEFAULT_AUTO_EXIT_POLICY),
                },
                "singleton_scope": "explore_10_combined_core_auto",
            },
            {
                "job_key": "post_close_analysis:live_collectors",
                "job_type": POST_CLOSE_ANALYSIS_JOB_TYPE,
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
                "job_type": POST_MARKET_ANALYSIS_JOB_TYPE,
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
                "job_type": POST_CLOSE_ANALYSIS_JOB_TYPE,
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
                "job_type": POST_CLOSE_ANALYSIS_JOB_TYPE,
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
                "job_type": POST_MARKET_ANALYSIS_JOB_TYPE,
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
                "job_type": POST_MARKET_ANALYSIS_JOB_TYPE,
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
            {
                "job_key": POST_CLOSE_ANALYSIS_ADHOC_JOB_KEY,
                "job_type": POST_CLOSE_ANALYSIS_JOB_TYPE,
                "enabled": False,
                "schedule_type": "manual",
                "schedule": {},
                "payload": {},
                "singleton_scope": None,
            },
            {
                "job_key": GENERATOR_ADHOC_JOB_KEY,
                "job_type": GENERATOR_JOB_TYPE,
                "enabled": False,
                "schedule_type": "manual",
                "schedule": {},
                "payload": {},
                "singleton_scope": None,
            },
            {
                "job_key": EXECUTION_SUBMIT_ADHOC_JOB_KEY,
                "job_type": EXECUTION_SUBMIT_JOB_TYPE,
                "enabled": False,
                "schedule_type": "manual",
                "schedule": {},
                "payload": {},
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
