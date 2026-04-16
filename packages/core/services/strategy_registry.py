from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.services.option_structures import normalize_strategy_family


@dataclass(frozen=True)
class StrategyDefinition:
    strategy_id: str
    strategy_family: str

    def matches_candidate(self, candidate: dict[str, Any]) -> bool:
        return (
            normalize_strategy_family(
                candidate.get("strategy_family") or candidate.get("strategy")
            )
            == self.strategy_family
        )


def resolve_strategy_definition(strategy_id: str) -> StrategyDefinition:
    return StrategyDefinition(
        strategy_id=str(strategy_id),
        strategy_family=normalize_strategy_family(strategy_id),
    )


__all__ = ["StrategyDefinition", "resolve_strategy_definition"]
