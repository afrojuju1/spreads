from __future__ import annotations

from collections.abc import Mapping

from core.jobs.contracts import RoutineHandler
from core.jobs.registry import (
    DATA_WORKFLOW_LANE,
    MAINTENANCE_WORKFLOW_LANE,
    RESEARCH_WORKFLOW_LANE,
    RUNTIME_WORKFLOW_LANE,
    VALUATION_WORKFLOW_LANE,
    get_job_types_for_lane,
)


class RoutineHandlerRegistryError(RuntimeError):
    pass


def build_lane_handlers(lane: str) -> Mapping[str, RoutineHandler]:
    normalized = str(lane or "").strip().lower()
    if normalized == RUNTIME_WORKFLOW_LANE:
        from core.jobs.handlers.runtime import HANDLERS
    elif normalized == DATA_WORKFLOW_LANE:
        from core.jobs.handlers.data import HANDLERS
    elif normalized == MAINTENANCE_WORKFLOW_LANE:
        from core.jobs.handlers.maintenance import HANDLERS
    elif normalized == VALUATION_WORKFLOW_LANE:
        from core.jobs.handlers.valuation import HANDLERS
    elif normalized == RESEARCH_WORKFLOW_LANE:
        from core.jobs.handlers.research import HANDLERS
    else:
        raise RoutineHandlerRegistryError(f"Workflow lane does not own scheduled routines: {normalized or '<empty>'}")

    expected = frozenset(get_job_types_for_lane(normalized))
    actual = frozenset(HANDLERS)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise RoutineHandlerRegistryError(
            f"Routine handler registry mismatch for lane {normalized}: missing={missing}, extra={extra}"
        )
    return HANDLERS


__all__ = ["RoutineHandlerRegistryError", "build_lane_handlers"]
