from __future__ import annotations

from typing import Any

from core.services.trading_lifecycle import STATE_ENUM_BY_OBJECT
from core.storage.lifecycle_models import TARGET_LIFECYCLE_TABLES


def build_lifecycle_schema_summary() -> dict[str, Any]:
    return {
        "status": "target_schema_defined",
        "posture": "breaking_rewrite",
        "tables": [dict(row) for row in TARGET_LIFECYCLE_TABLES],
        "states": [
            {
                "object_type": object_type.value,
                "state_enum": state_enum.__name__,
                "states": [state.value for state in state_enum],
            }
            for object_type, state_enum in STATE_ENUM_BY_OBJECT.items()
        ],
        "cutover": {
            "downtime_allowed": True,
            "compatibility_shims_required": False,
            "old_and_new_cli_schema_support_required": False,
            "live_writers_cut_over": False,
            "initial_target_tables_start_empty": True,
        },
    }


__all__ = ["build_lifecycle_schema_summary"]
