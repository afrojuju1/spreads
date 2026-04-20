from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.services.strategy_specs import resolve_strategy_spec


@dataclass(frozen=True)
class StrategyDefinition:
    strategy_id: str
    strategy_family: str

    def matches_candidate(self, candidate: dict[str, Any]) -> bool:
        return resolve_strategy_spec(self.strategy_family).matches_candidate(candidate)


def resolve_strategy_definition(strategy_id: str) -> StrategyDefinition:
    spec = resolve_strategy_spec(strategy_id)
    return StrategyDefinition(
        strategy_id=spec.strategy_family,
        strategy_family=spec.strategy_family,
    )


__all__ = ["StrategyDefinition", "resolve_strategy_definition"]
