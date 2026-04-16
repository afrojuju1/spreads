from __future__ import annotations

import argparse

from spreads.jobs.registry import (
    ALERT_RECONCILE_JOB_KEY,
    ALERT_RECONCILE_JOB_TYPE,
    BROKER_SYNC_JOB_TYPE,
    COLLECTOR_RECOVERY_JOB_KEY,
    COLLECTOR_RECOVERY_JOB_TYPE,
    EXECUTION_SUBMIT_ADHOC_JOB_KEY,
    EXECUTION_SUBMIT_JOB_TYPE,
    LIVE_COLLECTOR_JOB_TYPE,
    OPTIONS_AUTOMATION_ENTRY_ADHOC_JOB_KEY,
    OPTIONS_AUTOMATION_ENTRY_JOB_TYPE,
    OPTIONS_AUTOMATION_MANAGEMENT_ADHOC_JOB_KEY,
    OPTIONS_AUTOMATION_MANAGEMENT_JOB_TYPE,
    POST_CLOSE_ANALYSIS_ADHOC_JOB_KEY,
    POST_CLOSE_ANALYSIS_JOB_TYPE,
    POST_MARKET_ANALYSIS_JOB_TYPE,
    POSITION_EXIT_MANAGER_JOB_TYPE,
)
from spreads.runtime.config import default_database_url
from spreads.services.automations import cadence_minutes
from spreads.services.bots import load_active_bots
from spreads.storage.factory import build_job_repository

DEFAULT_AUTO_EXECUTION_POLICY = {
    "enabled": True,
    "deployment_mode": "live_auto",
    "mode": "top_promotable",
    "quantity": 1,
    "pricing_mode": "adaptive_credit",
    "min_credit_retention_pct": 0.95,
    "max_credit_concession": 0.02,
}
DEFAULT_SHADOW_EXECUTION_POLICY = {
    **DEFAULT_AUTO_EXECUTION_POLICY,
    "enabled": False,
    "deployment_mode": "shadow",
}

DEFAULT_AUTO_RISK_POLICY = {
    "enabled": True,
    "allow_live": True,
    "max_open_positions_per_session": 20,
    "max_open_positions_per_underlying": 1,
    "max_open_positions_per_underlying_strategy": 1,
    "max_contracts_per_position": 1,
    "max_contracts_per_session": 20,
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

RETIRED_JOB_KEYS = (
    "generator:adhoc",
    "session_exit_manager:live",
)


def _options_automation_job_definitions() -> list[dict[str, object]]:
    definitions: list[dict[str, object]] = [
        {
            "job_key": OPTIONS_AUTOMATION_ENTRY_ADHOC_JOB_KEY,
            "job_type": OPTIONS_AUTOMATION_ENTRY_JOB_TYPE,
            "enabled": False,
            "schedule_type": "manual",
            "schedule": {},
            "payload": {},
            "singleton_scope": None,
        },
        {
            "job_key": OPTIONS_AUTOMATION_MANAGEMENT_ADHOC_JOB_KEY,
            "job_type": OPTIONS_AUTOMATION_MANAGEMENT_JOB_TYPE,
            "enabled": False,
            "schedule_type": "manual",
            "schedule": {},
            "payload": {},
            "singleton_scope": None,
        },
        {
            "job_key": "live_collector:options_automation_short_dated_index_put_credit",
            "job_type": LIVE_COLLECTOR_JOB_TYPE,
            "enabled": True,
            "schedule_type": "market_open_plus_minutes",
            "schedule": {"minutes": -5},
            "payload": {
                "universe": "explore_10",
                "strategy": "put_credit",
                "profile": "weekly",
                "greeks_source": "auto",
                "top": 10,
                "per_symbol_top": 1,
                "interval_seconds": 300,
                "backfill_missed_slots": False,
                "max_slot_retries": 3,
                "quote_capture_seconds": 20,
                "trade_capture_seconds": 30,
                "allow_off_hours": False,
                "session_start_offset_minutes": -5,
                "session_end_offset_minutes": 5,
                "options_automation_enabled": True,
                "execution_policy": dict(DEFAULT_SHADOW_EXECUTION_POLICY),
                "risk_policy": dict(DEFAULT_AUTO_RISK_POLICY),
                "exit_policy": dict(DEFAULT_AUTO_EXIT_POLICY),
            },
            "singleton_scope": "options_automation_short_dated_index_put_credit",
        },
    ]
    for bot in load_active_bots().values():
        for automation in bot.automations:
            cadence = cadence_minutes(automation.automation.schedule)
            base = {
                "bot_id": bot.bot.bot_id,
                "automation_id": automation.automation.automation_id,
                "allow_off_hours": not bool(
                    automation.automation.schedule.get("market_hours_only", False)
                ),
            }
            if automation.automation.is_entry:
                definitions.append(
                    {
                        "job_key": f"options_automation_entry:{bot.bot.bot_id}:{automation.automation.automation_id}",
                        "job_type": OPTIONS_AUTOMATION_ENTRY_JOB_TYPE,
                        "enabled": True,
                        "schedule_type": "interval_minutes",
                        "schedule": {"minutes": cadence},
                        "payload": dict(base),
                        "singleton_scope": f"{bot.bot.bot_id}:{automation.automation.automation_id}",
                    }
                )
            if automation.automation.is_management:
                definitions.append(
                    {
                        "job_key": f"options_automation_management:{bot.bot.bot_id}:{automation.automation.automation_id}",
                        "job_type": OPTIONS_AUTOMATION_MANAGEMENT_JOB_TYPE,
                        "enabled": True,
                        "schedule_type": "interval_minutes",
                        "schedule": {"minutes": cadence},
                        "payload": dict(base),
                        "singleton_scope": f"{bot.bot.bot_id}:{automation.automation.automation_id}",
                    }
                )
    return definitions


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed default ARQ-managed job definitions."
    )
    parser.add_argument(
        "--db", default=default_database_url(), help="Postgres database URL."
    )
    return parser.parse_args(argv)


def seed_definitions(db: str) -> list[str]:
    repo = build_job_repository(db)
    job_keys: list[str] = []
    try:
        for retired_job_key in RETIRED_JOB_KEYS:
            repo.delete_job_definition(retired_job_key)
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
                "job_key": COLLECTOR_RECOVERY_JOB_KEY,
                "job_type": COLLECTOR_RECOVERY_JOB_TYPE,
                "enabled": True,
                "schedule_type": "interval_minutes",
                "schedule": {"minutes": 1},
                "payload": {
                    "allow_off_hours": True,
                },
                "singleton_scope": "global",
            },
            {
                "job_key": "position_exit_manager:live",
                "job_type": POSITION_EXIT_MANAGER_JOB_TYPE,
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
                    "backfill_missed_slots": False,
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
                "job_key": "live_collector:explore_10_call_debit_weekly_auto",
                "job_type": LIVE_COLLECTOR_JOB_TYPE,
                "enabled": True,
                "schedule_type": "market_open_plus_minutes",
                "schedule": {"minutes": -5},
                "payload": {
                    "universe": "explore_10",
                    "strategy": "call_debit",
                    "profile": "weekly",
                    "greeks_source": "auto",
                    "top": 10,
                    "per_symbol_top": 1,
                    "interval_seconds": 300,
                    "backfill_missed_slots": False,
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
                "singleton_scope": "explore_10_call_debit_weekly_auto",
            },
            {
                "job_key": "live_collector:explore_10_put_debit_weekly_auto",
                "job_type": LIVE_COLLECTOR_JOB_TYPE,
                "enabled": True,
                "schedule_type": "market_open_plus_minutes",
                "schedule": {"minutes": -5},
                "payload": {
                    "universe": "explore_10",
                    "strategy": "put_debit",
                    "profile": "weekly",
                    "greeks_source": "auto",
                    "top": 10,
                    "per_symbol_top": 1,
                    "interval_seconds": 300,
                    "backfill_missed_slots": False,
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
                "singleton_scope": "explore_10_put_debit_weekly_auto",
            },
            {
                "job_key": "live_collector:explore_10_long_straddle_weekly_auto",
                "job_type": LIVE_COLLECTOR_JOB_TYPE,
                "enabled": True,
                "schedule_type": "market_open_plus_minutes",
                "schedule": {"minutes": -5},
                "payload": {
                    "universe": "explore_10",
                    "strategy": "long_straddle",
                    "profile": "weekly",
                    "greeks_source": "auto",
                    "top": 10,
                    "per_symbol_top": 1,
                    "interval_seconds": 300,
                    "backfill_missed_slots": False,
                    "max_slot_retries": 3,
                    "quote_capture_seconds": 20,
                    "trade_capture_seconds": 30,
                    "allow_off_hours": False,
                    "session_start_offset_minutes": -5,
                    "session_end_offset_minutes": 5,
                    "execution_policy": dict(DEFAULT_SHADOW_EXECUTION_POLICY),
                    "risk_policy": dict(DEFAULT_AUTO_RISK_POLICY),
                    "exit_policy": dict(DEFAULT_AUTO_EXIT_POLICY),
                },
                "singleton_scope": "explore_10_long_straddle_weekly_auto",
            },
            {
                "job_key": "live_collector:explore_10_long_strangle_weekly_auto",
                "job_type": LIVE_COLLECTOR_JOB_TYPE,
                "enabled": True,
                "schedule_type": "market_open_plus_minutes",
                "schedule": {"minutes": -5},
                "payload": {
                    "universe": "explore_10",
                    "strategy": "long_strangle",
                    "profile": "weekly",
                    "greeks_source": "auto",
                    "top": 10,
                    "per_symbol_top": 1,
                    "interval_seconds": 300,
                    "backfill_missed_slots": False,
                    "max_slot_retries": 3,
                    "quote_capture_seconds": 20,
                    "trade_capture_seconds": 30,
                    "allow_off_hours": False,
                    "session_start_offset_minutes": -5,
                    "session_end_offset_minutes": 5,
                    "execution_policy": dict(DEFAULT_SHADOW_EXECUTION_POLICY),
                    "risk_policy": dict(DEFAULT_AUTO_RISK_POLICY),
                    "exit_policy": dict(DEFAULT_AUTO_EXIT_POLICY),
                },
                "singleton_scope": "explore_10_long_strangle_weekly_auto",
            },
            {
                "job_key": "live_collector:explore_10_iron_condor_weekly_auto",
                "job_type": LIVE_COLLECTOR_JOB_TYPE,
                "enabled": True,
                "schedule_type": "market_open_plus_minutes",
                "schedule": {"minutes": -5},
                "payload": {
                    "universe": "explore_10",
                    "strategy": "iron_condor",
                    "profile": "weekly",
                    "greeks_source": "auto",
                    "top": 10,
                    "per_symbol_top": 1,
                    "interval_seconds": 300,
                    "backfill_missed_slots": False,
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
                "singleton_scope": "explore_10_iron_condor_weekly_auto",
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
                    "backfill_missed_slots": False,
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
                "job_key": EXECUTION_SUBMIT_ADHOC_JOB_KEY,
                "job_type": EXECUTION_SUBMIT_JOB_TYPE,
                "enabled": False,
                "schedule_type": "manual",
                "schedule": {},
                "payload": {},
                "singleton_scope": None,
            },
        ]
        definitions.extend(_options_automation_job_definitions())
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


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    keys = seed_definitions(args.db)
    for key in keys:
        print(key)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
