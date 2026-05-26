from __future__ import annotations

import json
import os
from typing import Any

from alembic import command
from alembic.config import Config

from core.runtime.config import default_database_url
from core.services.broker_sync import run_broker_sync


def _alpaca_credentials_present() -> bool:
    key = os.environ.get("APCA_API_KEY_ID") or os.environ.get("ALPACA_API_KEY")
    secret = os.environ.get("APCA_API_SECRET_KEY") or os.environ.get("ALPACA_SECRET_KEY")
    return bool(key and secret)


def run_startup_init() -> dict[str, Any]:
    alembic_cfg = Config("alembic.ini")
    command.upgrade(alembic_cfg, "head")

    if not _alpaca_credentials_present():
        broker_sync: dict[str, Any] = {
            "status": "skipped",
            "reason": "alpaca_credentials_missing",
        }
    else:
        try:
            result = run_broker_sync(
                db_target=default_database_url(),
                history_range=os.environ.get("SPREADS_STARTUP_BROKER_SYNC_HISTORY", "1D"),
                activity_lookback_days=int(
                    os.environ.get("SPREADS_STARTUP_BROKER_SYNC_LOOKBACK_DAYS", "1")
                ),
            )
            broker_sync = {
                "status": result.get("status"),
                "snapshot_id": result.get("snapshot_id"),
            }
        except Exception as exc:
            broker_sync = {"status": "failed", "error": str(exc)}

    return {
        "status": "ok",
        "migrations": {"status": "applied"},
        "broker_sync": broker_sync,
    }


def main() -> None:
    print(json.dumps(run_startup_init(), sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
