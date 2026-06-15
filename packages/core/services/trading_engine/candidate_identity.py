from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.services.option_structures import payload_structure_identity


def resolve_candidate_identity(candidate: Mapping[str, Any], *, strategy: Any = None) -> str:
    for key in ("candidate_identity", "structure_identity"):
        value = candidate.get(key)
        if value:
            return str(value).strip()
    derived = payload_structure_identity(dict(candidate), strategy=strategy)
    if derived:
        return str(derived).strip()
    return ""


__all__ = ["resolve_candidate_identity"]
