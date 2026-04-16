from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class RegimeSnapshot:
    regime_snapshot_id: str
    cycle_id: str
    session_id: str
    symbol: str
    style_profile: str
    direction_bias: str
    trend_strength: float
    intraday_structure: str
    vol_level: str
    vol_trend: str
    iv_vs_rv: str
    event_state: str
    liquidity_state: str
    confidence: float
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StrategyIntent:
    strategy_intent_id: str
    regime_snapshot_id: str
    cycle_id: str
    session_id: str
    symbol: str
    style_profile: str
    strategy_family: str
    thesis_direction: str
    policy_state: str
    desirability_score: float
    confidence: float
    blockers: list[str] = field(default_factory=list)
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class HorizonIntent:
    horizon_intent_id: str
    strategy_intent_id: str
    cycle_id: str
    session_id: str
    symbol: str
    style_profile: str
    strategy_family: str
    horizon_band: str
    target_dte_min: int
    target_dte_max: int
    preferred_expiration_type: str
    event_timing_rule: str
    urgency: str
    confidence: float
    blockers: list[str] = field(default_factory=list)
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OpportunityLeg:
    leg_index: int
    symbol: str
    side: str
    position_intent: str | None = None
    ratio_qty: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Opportunity:
    opportunity_id: str
    cycle_id: str
    session_id: str
    candidate_id: int
    symbol: str
    legacy_strategy: str
    expiration_date: str
    short_symbol: str
    long_symbol: str
    style_profile: str
    strategy_family: str
    regime_snapshot_id: str
    strategy_intent_id: str
    horizon_intent_id: str
    discovery_score: float
    promotion_score: float
    rank: int
    state: str
    state_reason: str
    expected_edge_value: float | None = None
    max_loss: float | None = None
    capital_usage: float | None = None
    execution_complexity: float | None = None
    product_class: str | None = None
    legacy_selection_state: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)
    legs: list[OpportunityLeg] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["legs"] = [leg.to_payload() for leg in self.legs]
        return payload


@dataclass(frozen=True)
class AllocationDecision:
    allocation_id: str
    opportunity_id: str
    cycle_id: str
    session_id: str
    allocation_state: str
    allocation_score: float | None
    slot_class: str
    allocation_reason: str
    rejection_codes: list[str] = field(default_factory=list)
    budget_impact: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionIntent:
    execution_intent_id: str
    opportunity_id: str
    cycle_id: str
    session_id: str
    symbol: str
    strategy_family: str
    order_structure: str
    entry_policy: str
    price_policy: str
    timeout_policy: str
    replace_policy: str
    exit_policy: str
    validation_state: str
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DecisionReplay:
    target: dict[str, Any]
    session: dict[str, Any]
    regime_snapshots: list[RegimeSnapshot] = field(default_factory=list)
    strategy_intents: list[StrategyIntent] = field(default_factory=list)
    horizon_intents: list[HorizonIntent] = field(default_factory=list)
    opportunities: list[Opportunity] = field(default_factory=list)
    allocation_decisions: list[AllocationDecision] = field(default_factory=list)
    execution_intents: list[ExecutionIntent] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)
    comparison: dict[str, Any] = field(default_factory=dict)
    scorecard: dict[str, Any] = field(default_factory=dict)
    rows: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        return {
            "target": dict(self.target),
            "session": dict(self.session),
            "regime_snapshots": [item.to_payload() for item in self.regime_snapshots],
            "strategy_intents": [item.to_payload() for item in self.strategy_intents],
            "horizon_intents": [item.to_payload() for item in self.horizon_intents],
            "opportunities": [item.to_payload() for item in self.opportunities],
            "allocation_decisions": [
                item.to_payload() for item in self.allocation_decisions
            ],
            "execution_intents": [item.to_payload() for item in self.execution_intents],
            "summary": dict(self.summary),
            "comparison": dict(self.comparison),
            "scorecard": dict(self.scorecard),
            "rows": [dict(row) for row in self.rows],
            "warnings": list(self.warnings),
        }


__all__ = [
    "AllocationDecision",
    "DecisionReplay",
    "ExecutionIntent",
    "HorizonIntent",
    "Opportunity",
    "OpportunityLeg",
    "RegimeSnapshot",
    "StrategyIntent",
]
