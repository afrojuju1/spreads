from __future__ import annotations

import argparse
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable

from core.domain.models import SpreadCandidate, SymbolMarketSlice
from core.services.option_structures import normalize_strategy_family
from core.services.options_automation_models import (
    IronCondorBuildConfig,
    LongVolBuildConfig,
    StrategyBuildConfig,
    StrategyLiquidityRules,
    StrategyRiskDefaults,
    VerticalSpreadBuildConfig,
)
from core.services.scanners.market_data import (
    count_alpaca_greeks_coverage,
    count_local_greeks_coverage,
    count_snapshot_delta_coverage,
)

CoverageCounter = Callable[[SymbolMarketSlice], tuple[int, int, int, int]]
CandidateBuilder = Callable[[SymbolMarketSlice, argparse.Namespace], list[SpreadCandidate]]
BuildValidator = Callable[[Mapping[str, Any] | None], StrategyBuildConfig]


def _single_side_coverage(
    *,
    market_slice: SymbolMarketSlice,
    option_type: str,
) -> tuple[int, int, int, int]:
    snapshots_by_expiration = (
        market_slice.call_snapshots_by_expiration
        if option_type == "call"
        else market_slice.put_snapshots_by_expiration
    )
    quoted_contract_count, delta_contract_count = count_snapshot_delta_coverage(
        snapshots_by_expiration
    )
    return (
        quoted_contract_count,
        count_alpaca_greeks_coverage(snapshots_by_expiration),
        delta_contract_count,
        count_local_greeks_coverage(snapshots_by_expiration),
    )


def _dual_side_coverage(
    market_slice: SymbolMarketSlice,
) -> tuple[int, int, int, int]:
    call_quoted_count, call_delta_count = count_snapshot_delta_coverage(
        market_slice.call_snapshots_by_expiration
    )
    put_quoted_count, put_delta_count = count_snapshot_delta_coverage(
        market_slice.put_snapshots_by_expiration
    )
    return (
        call_quoted_count + put_quoted_count,
        count_alpaca_greeks_coverage(market_slice.call_snapshots_by_expiration)
        + count_alpaca_greeks_coverage(market_slice.put_snapshots_by_expiration),
        call_delta_count + put_delta_count,
        count_local_greeks_coverage(market_slice.call_snapshots_by_expiration)
        + count_local_greeks_coverage(market_slice.put_snapshots_by_expiration),
    )


def _build_call_verticals(
    market_slice: SymbolMarketSlice,
    symbol_args: argparse.Namespace,
) -> list[SpreadCandidate]:
    from core.services.scanners.builders.verticals import build_vertical_spreads

    return build_vertical_spreads(
        symbol=market_slice.symbol,
        strategy="call_credit"
        if normalize_strategy_family(symbol_args.strategy) == "call_credit_spread"
        else "call_debit",
        spot_price=market_slice.spot_price,
        contracts_by_expiration=market_slice.call_contracts_by_expiration,
        snapshots_by_expiration=market_slice.call_snapshots_by_expiration,
        expected_moves_by_expiration=market_slice.expected_moves_by_expiration,
        args=symbol_args,
    )


def _build_put_verticals(
    market_slice: SymbolMarketSlice,
    symbol_args: argparse.Namespace,
) -> list[SpreadCandidate]:
    from core.services.scanners.builders.verticals import build_vertical_spreads

    return build_vertical_spreads(
        symbol=market_slice.symbol,
        strategy="put_credit"
        if normalize_strategy_family(symbol_args.strategy) == "put_credit_spread"
        else "put_debit",
        spot_price=market_slice.spot_price,
        contracts_by_expiration=market_slice.put_contracts_by_expiration,
        snapshots_by_expiration=market_slice.put_snapshots_by_expiration,
        expected_moves_by_expiration=market_slice.expected_moves_by_expiration,
        args=symbol_args,
    )


def _build_iron_condor_candidates(
    market_slice: SymbolMarketSlice,
    symbol_args: argparse.Namespace,
) -> list[SpreadCandidate]:
    from core.services.scanners.builders.iron_condors import build_iron_condors

    return build_iron_condors(
        symbol=market_slice.symbol,
        spot_price=market_slice.spot_price,
        call_contracts_by_expiration=market_slice.call_contracts_by_expiration,
        put_contracts_by_expiration=market_slice.put_contracts_by_expiration,
        call_snapshots_by_expiration=market_slice.call_snapshots_by_expiration,
        put_snapshots_by_expiration=market_slice.put_snapshots_by_expiration,
        expected_moves_by_expiration=market_slice.expected_moves_by_expiration,
        args=symbol_args,
    )


def _build_long_straddle_candidates(
    market_slice: SymbolMarketSlice,
    symbol_args: argparse.Namespace,
) -> list[SpreadCandidate]:
    from core.services.scanners.builders.long_vol import build_long_straddles

    return build_long_straddles(
        symbol=market_slice.symbol,
        spot_price=market_slice.spot_price,
        call_contracts_by_expiration=market_slice.call_contracts_by_expiration,
        put_contracts_by_expiration=market_slice.put_contracts_by_expiration,
        call_snapshots_by_expiration=market_slice.call_snapshots_by_expiration,
        put_snapshots_by_expiration=market_slice.put_snapshots_by_expiration,
        expected_moves_by_expiration=market_slice.expected_moves_by_expiration,
        args=symbol_args,
    )


def _build_long_strangle_candidates(
    market_slice: SymbolMarketSlice,
    symbol_args: argparse.Namespace,
) -> list[SpreadCandidate]:
    from core.services.scanners.builders.long_vol import build_long_strangles

    return build_long_strangles(
        symbol=market_slice.symbol,
        spot_price=market_slice.spot_price,
        call_contracts_by_expiration=market_slice.call_contracts_by_expiration,
        put_contracts_by_expiration=market_slice.put_contracts_by_expiration,
        call_snapshots_by_expiration=market_slice.call_snapshots_by_expiration,
        put_snapshots_by_expiration=market_slice.put_snapshots_by_expiration,
        expected_moves_by_expiration=market_slice.expected_moves_by_expiration,
        args=symbol_args,
    )


def _build_long_call_candidates(
    market_slice: SymbolMarketSlice,
    symbol_args: argparse.Namespace,
) -> list[SpreadCandidate]:
    from core.services.scanners.builders.single_legs import build_long_calls

    return build_long_calls(
        symbol=market_slice.symbol,
        spot_price=market_slice.spot_price,
        contracts_by_expiration=market_slice.call_contracts_by_expiration,
        snapshots_by_expiration=market_slice.call_snapshots_by_expiration,
        expected_moves_by_expiration=market_slice.expected_moves_by_expiration,
        args=symbol_args,
    )


def _build_long_put_candidates(
    market_slice: SymbolMarketSlice,
    symbol_args: argparse.Namespace,
) -> list[SpreadCandidate]:
    from core.services.scanners.builders.single_legs import build_long_puts

    return build_long_puts(
        symbol=market_slice.symbol,
        spot_price=market_slice.spot_price,
        contracts_by_expiration=market_slice.put_contracts_by_expiration,
        snapshots_by_expiration=market_slice.put_snapshots_by_expiration,
        expected_moves_by_expiration=market_slice.expected_moves_by_expiration,
        args=symbol_args,
    )


def _build_short_call_candidates(
    market_slice: SymbolMarketSlice,
    symbol_args: argparse.Namespace,
) -> list[SpreadCandidate]:
    from core.services.scanners.builders.single_legs import build_short_calls

    return build_short_calls(
        symbol=market_slice.symbol,
        spot_price=market_slice.spot_price,
        contracts_by_expiration=market_slice.call_contracts_by_expiration,
        snapshots_by_expiration=market_slice.call_snapshots_by_expiration,
        expected_moves_by_expiration=market_slice.expected_moves_by_expiration,
        args=symbol_args,
    )


def _build_short_put_candidates(
    market_slice: SymbolMarketSlice,
    symbol_args: argparse.Namespace,
) -> list[SpreadCandidate]:
    from core.services.scanners.builders.single_legs import build_short_puts

    return build_short_puts(
        symbol=market_slice.symbol,
        spot_price=market_slice.spot_price,
        contracts_by_expiration=market_slice.put_contracts_by_expiration,
        snapshots_by_expiration=market_slice.put_snapshots_by_expiration,
        expected_moves_by_expiration=market_slice.expected_moves_by_expiration,
        args=symbol_args,
    )


@dataclass(frozen=True)
class StrategySpec:
    strategy_family: str
    scanner_strategy: str
    display_label: str
    direction: str
    option_type: str | None
    aliases: tuple[str, ...]
    build_validator: BuildValidator
    candidate_builder: CandidateBuilder
    coverage_counter: CoverageCounter

    def matches_candidate(self, candidate: Mapping[str, Any]) -> bool:
        return normalize_strategy_family(
            candidate.get("strategy_family") or candidate.get("strategy")
        ) == self.strategy_family

    def validate_build(
        self,
        payload: Mapping[str, Any] | None,
    ) -> StrategyBuildConfig:
        return self.build_validator(payload)

    def build_candidates(
        self,
        *,
        market_slice: SymbolMarketSlice,
        symbol_args: argparse.Namespace,
    ) -> list[SpreadCandidate]:
        return self.candidate_builder(market_slice, symbol_args)

    def count_coverage(
        self,
        *,
        market_slice: SymbolMarketSlice,
    ) -> tuple[int, int, int, int]:
        return self.coverage_counter(market_slice)

    def apply_scan_overrides(
        self,
        *,
        args: argparse.Namespace,
        build: StrategyBuildConfig,
        liquidity: StrategyLiquidityRules,
        risk: StrategyRiskDefaults,
    ) -> argparse.Namespace:
        args.strategy = self.scanner_strategy
        builder_params = build.as_builder_params()
        for key, value in builder_params.items():
            if key == "width_points":
                if value:
                    args.min_width = min(float(item) for item in value)
                    args.max_width = max(float(item) for item in value)
                continue
            setattr(args, key, value)
        if liquidity.min_open_interest is not None:
            args.min_open_interest = liquidity.min_open_interest
        if liquidity.max_leg_spread_pct_mid is not None:
            args.max_relative_spread = liquidity.max_leg_spread_pct_mid
        if risk.min_return_on_risk is not None:
            args.min_return_on_risk = risk.min_return_on_risk
        return args


_SPEC_LIST = (
    StrategySpec(
        strategy_family="call_credit_spread",
        scanner_strategy="call_credit",
        display_label="Call Credit",
        direction="bearish",
        option_type="call",
        aliases=("call_credit", "call_credit_spread"),
        build_validator=VerticalSpreadBuildConfig.from_payload,
        candidate_builder=_build_call_verticals,
        coverage_counter=lambda market_slice: _single_side_coverage(
            market_slice=market_slice,
            option_type="call",
        ),
    ),
    StrategySpec(
        strategy_family="put_credit_spread",
        scanner_strategy="put_credit",
        display_label="Put Credit",
        direction="bullish",
        option_type="put",
        aliases=("put_credit", "put_credit_spread"),
        build_validator=VerticalSpreadBuildConfig.from_payload,
        candidate_builder=_build_put_verticals,
        coverage_counter=lambda market_slice: _single_side_coverage(
            market_slice=market_slice,
            option_type="put",
        ),
    ),
    StrategySpec(
        strategy_family="call_debit_spread",
        scanner_strategy="call_debit",
        display_label="Call Debit",
        direction="bullish",
        option_type="call",
        aliases=("call_debit", "call_debit_spread"),
        build_validator=VerticalSpreadBuildConfig.from_payload,
        candidate_builder=_build_call_verticals,
        coverage_counter=lambda market_slice: _single_side_coverage(
            market_slice=market_slice,
            option_type="call",
        ),
    ),
    StrategySpec(
        strategy_family="put_debit_spread",
        scanner_strategy="put_debit",
        display_label="Put Debit",
        direction="bearish",
        option_type="put",
        aliases=("put_debit", "put_debit_spread"),
        build_validator=VerticalSpreadBuildConfig.from_payload,
        candidate_builder=_build_put_verticals,
        coverage_counter=lambda market_slice: _single_side_coverage(
            market_slice=market_slice,
            option_type="put",
        ),
    ),
    StrategySpec(
        strategy_family="long_call",
        scanner_strategy="long_call",
        display_label="Long Call",
        direction="bullish",
        option_type="call",
        aliases=("long_call",),
        build_validator=LongVolBuildConfig.from_payload,
        candidate_builder=_build_long_call_candidates,
        coverage_counter=lambda market_slice: _single_side_coverage(
            market_slice=market_slice,
            option_type="call",
        ),
    ),
    StrategySpec(
        strategy_family="long_put",
        scanner_strategy="long_put",
        display_label="Long Put",
        direction="bearish",
        option_type="put",
        aliases=("long_put",),
        build_validator=LongVolBuildConfig.from_payload,
        candidate_builder=_build_long_put_candidates,
        coverage_counter=lambda market_slice: _single_side_coverage(
            market_slice=market_slice,
            option_type="put",
        ),
    ),
    StrategySpec(
        strategy_family="short_call",
        scanner_strategy="short_call",
        display_label="Short Call",
        direction="bearish",
        option_type="call",
        aliases=("short_call",),
        build_validator=LongVolBuildConfig.from_payload,
        candidate_builder=_build_short_call_candidates,
        coverage_counter=lambda market_slice: _single_side_coverage(
            market_slice=market_slice,
            option_type="call",
        ),
    ),
    StrategySpec(
        strategy_family="short_put",
        scanner_strategy="short_put",
        display_label="Short Put",
        direction="bullish",
        option_type="put",
        aliases=("short_put",),
        build_validator=LongVolBuildConfig.from_payload,
        candidate_builder=_build_short_put_candidates,
        coverage_counter=lambda market_slice: _single_side_coverage(
            market_slice=market_slice,
            option_type="put",
        ),
    ),
    StrategySpec(
        strategy_family="iron_condor",
        scanner_strategy="iron_condor",
        display_label="Iron Condor",
        direction="neutral",
        option_type=None,
        aliases=("iron_condor",),
        build_validator=IronCondorBuildConfig.from_payload,
        candidate_builder=_build_iron_condor_candidates,
        coverage_counter=_dual_side_coverage,
    ),
    StrategySpec(
        strategy_family="long_straddle",
        scanner_strategy="long_straddle",
        display_label="Long Straddle",
        direction="neutral",
        option_type=None,
        aliases=("long_straddle",),
        build_validator=LongVolBuildConfig.from_payload,
        candidate_builder=_build_long_straddle_candidates,
        coverage_counter=_dual_side_coverage,
    ),
    StrategySpec(
        strategy_family="long_strangle",
        scanner_strategy="long_strangle",
        display_label="Long Strangle",
        direction="neutral",
        option_type=None,
        aliases=("long_strangle",),
        build_validator=LongVolBuildConfig.from_payload,
        candidate_builder=_build_long_strangle_candidates,
        coverage_counter=_dual_side_coverage,
    ),
)

_SPECS_BY_ID = {
    identifier: spec
    for spec in _SPEC_LIST
    for identifier in spec.aliases
}


def resolve_strategy_spec(strategy_id: Any) -> StrategySpec:
    normalized = str(strategy_id or "").strip().lower()
    spec = _SPECS_BY_ID.get(normalized)
    if spec is None:
        raise ValueError(f"Unsupported strategy: {strategy_id}")
    return spec


def all_strategy_specs() -> tuple[StrategySpec, ...]:
    return _SPEC_LIST


def strategy_display_label(strategy: str) -> str:
    if strategy == "auto":
        return "Auto"
    if strategy == "combined":
        return "Combined"
    return resolve_strategy_spec(strategy).display_label


def strategy_option_type(strategy: str) -> str:
    if strategy in {"combined", "auto"}:
        return "call"
    spec = resolve_strategy_spec(strategy)
    return spec.option_type or "call"


def strategy_direction(strategy: str) -> str:
    if strategy in {"combined", "auto"}:
        return "neutral"
    return resolve_strategy_spec(strategy).direction


def concrete_strategies(strategy: str) -> tuple[str, ...]:
    if strategy == "auto":
        return (
            "call_credit",
            "put_credit",
            "call_debit",
            "put_debit",
            "long_call",
            "long_put",
            "iron_condor",
        )
    if strategy == "combined":
        return ("call_credit", "put_credit")
    return (resolve_strategy_spec(strategy).scanner_strategy,)


__all__ = [
    "StrategySpec",
    "all_strategy_specs",
    "concrete_strategies",
    "resolve_strategy_spec",
    "strategy_direction",
    "strategy_display_label",
    "strategy_option_type",
]
