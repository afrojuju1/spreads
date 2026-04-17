from __future__ import annotations

from typing import Any

from core.services.control_plane import get_control_state_snapshot, set_control_mode
from core.services.value_coercion import as_text as _as_text

from .shared import RECOVERY_CONTROL_CLEAR_REASON_CODE, RECOVERY_CONTROL_REASON_CODE


def _set_recovery_control_mode(
    *,
    db_target: str,
    storage: Any,
    blocked_sessions: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    control = get_control_state_snapshot(storage=storage)
    configured_mode = str(control.get("configured_mode") or "normal")
    reason_code = _as_text(control.get("reason_code"))
    configured_source_kind = _as_text(control.get("configured_source_kind"))
    if blocked_sessions:
        if configured_mode == "normal" or (
            reason_code == RECOVERY_CONTROL_REASON_CODE
            and configured_source_kind == "recovery_manager"
        ):
            note = (
                f"{len(blocked_sessions)} live session(s) are blocked by collector gap recovery."
            )
            return set_control_mode(
                db_target=db_target,
                mode="degraded",
                reason_code=RECOVERY_CONTROL_REASON_CODE,
                note=note,
                source_kind="recovery_manager",
                actor_id="collector_recovery",
                metadata={
                    "blocked_session_count": len(blocked_sessions),
                    "session_ids": sorted(blocked_sessions.keys())[:25],
                },
                storage=storage,
            )
        return {
            "action": "set_mode",
            "changed": False,
            "message": "Control mode already reflects a stricter operator state.",
            "control": control,
        }
    if (
        configured_mode == "degraded"
        and reason_code == RECOVERY_CONTROL_REASON_CODE
        and configured_source_kind == "recovery_manager"
    ):
        return set_control_mode(
            db_target=db_target,
            mode="normal",
            reason_code=RECOVERY_CONTROL_CLEAR_REASON_CODE,
            note="Collector gaps cleared after a successful post-gap live slot.",
            source_kind="recovery_manager",
            actor_id="collector_recovery",
            metadata={},
            storage=storage,
        )
    return {
        "action": "set_mode",
        "changed": False,
        "message": "Recovery control mode already clear.",
        "control": control,
    }
