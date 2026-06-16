from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable

from core.domain.models import SpreadCandidate, SymbolMarketSlice
from core.services.option_structures import normalize_strategy_family
from core.services.trading_strategy_build_models import (
    IronCondorBuildConfig,
    LongVolBuildConfig,
    StrategyBuildConfig,
    VerticalSpreadBuildConfig,
)
from core.services.strategy_candidate_builders.market_data import (
    count_alpaca_greeks_coverage,
    count_local_greeks_coverage,
    count_snapshot_delta_coverage,
)

CoverageCounter = Callable[[SymbolMarketSlice], tuple[int, int, int, int]]
CandidateBuilder = Callable[[SymbolMarketSlice, Any], list[SpreadCandidate]]
BuildValidator = Callable[[Mapping[str, Any]], StrategyBuildConfig]


def _single_side_coverage(
    *,
    market_slice: SymbolMarketSlice,
    option_type: str,
) -> tuple[int, int, int, int]:
    snapshots_by_expiration = market_slice.call_snapshots_by_expiration if option_type == "call" else market_slice.put_snapshots_by_expiration
    quoted_contract_count, delta_contract_count = count_snapshot_delta_coverage(snapshots_by_expiration)
    return (
        quoted_contract_count,
        count_alpaca_greeks_coverage(snapshots_by_expiration),
        delta_contract_count,
        count_local_greeks_coverage(snapshots_by_expiration),
    )


def _dual_side_coverage(
    market_slice: SymbolMarketSlice,
) -> tuple[int, int, int, int]:
    call_quoted_count, call_delta_count = count_snapshot_delta_coverage(market_slice.call_snapshots_by_expiration)
    put_quoted_count, put_delta_count = count_snapshot_delta_coverage(market_slice.put_snapshots_by_expiration)
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
    symbol_args: Any,
) -> list[SpreadCandidate]:
    from core.services.strategy_candidate_builders.verticals import build_vertical_spreads

    return build_vertical_spreads(
        symbol=market_slice.symbol,
        strategy="call_credit" if normalize_strategy_family(symbol_args.candidate_builder_key) == "call_credit_spread" else "call_debit",
        spot_price=market_slice.spot_price,
        contracts_by_expiration=market_slice.call_contracts_by_expiration,
        snapshots_by_expiration=market_slice.call_snapshots_by_expiration,
        expected_moves_by_expiration=market_slice.expected_moves_by_expiration,
        args=symbol_args,
    )


def _build_put_verticals(
    market_slice: SymbolMarketSlice,
    symbol_args: Any,
) -> list[SpreadCandidate]:
    from core.services.strategy_candidate_builders.verticals import build_vertical_spreads

    return build_vertical_spreads(
        symbol=market_slice.symbol,
        strategy="put_credit" if normalize_strategy_family(symbol_args.candidate_builder_key) == "put_credit_spread" else "put_debit",
        spot_price=market_slice.spot_price,
        contracts_by_expiration=market_slice.put_contracts_by_expiration,
        snapshots_by_expiration=market_slice.put_snapshots_by_expiration,
        expected_moves_by_expiration=market_slice.expected_moves_by_expiration,
        args=symbol_args,
    )


def _build_iron_condor_candidates(
    market_slice: SymbolMarketSlice,
    symbol_args: Any,
) -> list[SpreadCandidate]:
    from core.services.strategy_candidate_builders.iron_condors import build_iron_condors

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
    symbol_args: Any,
) -> list[SpreadCandidate]:
    from core.services.strategy_candidate_builders.long_vol import build_long_straddles

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
    symbol_args: Any,
) -> list[SpreadCandidate]:
    from core.services.strategy_candidate_builders.long_vol import build_long_strangles

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
    symbol_args: Any,
) -> list[SpreadCandidate]:
    from core.services.strategy_candidate_builders.single_legs import build_long_calls

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
    symbol_args: Any,
) -> list[SpreadCandidate]:
    from core.services.strategy_candidate_builders.single_legs import build_long_puts

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
    symbol_args: Any,
) -> list[SpreadCandidate]:
    from core.services.strategy_candidate_builders.single_legs import build_short_calls

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
    symbol_args: Any,
) -> list[SpreadCandidate]:
    from core.services.strategy_candidate_builders.single_legs import build_short_puts

    return build_short_puts(
        symbol=market_slice.symbol,
        spot_price=market_slice.spot_price,
        contracts_by_expiration=market_slice.put_contracts_by_expiration,
        snapshots_by_expiration=market_slice.put_snapshots_by_expiration,
        expected_moves_by_expiration=market_slice.expected_moves_by_expiration,
        args=symbol_args,
    )


@dataclass(frozen=True)
class TradeStructureSpec:
    strategy_family: str
    candidate_builder_key: str
    display_label: str
    direction: str
    option_type: str | None
    aliases: tuple[str, ...]
    build_validator: BuildValidator
    candidate_builder: CandidateBuilder
    coverage_counter: CoverageCounter

    def matches_candidate(self, candidate: Mapping[str, Any]) -> bool:
        return normalize_strategy_family(candidate.get("strategy_family") or candidate.get("strategy")) == self.strategy_family

    def validate_build(
        self,
        payload: Mapping[str, Any],
    ) -> StrategyBuildConfig:
        return self.build_validator(payload)

    def build_candidates(
        self,
        *,
        market_slice: SymbolMarketSlice,
        symbol_args: Any,
    ) -> list[SpreadCandidate]:
        return self.candidate_builder(market_slice, symbol_args)

    def count_coverage(
        self,
        *,
        market_slice: SymbolMarketSlice,
    ) -> tuple[int, int, int, int]:
        return self.coverage_counter(market_slice)


_SPEC_LIST = (
    TradeStructureSpec(
        strategy_family="call_credit_spread",
        candidate_builder_key="call_credit",
        display_label="Call Credit",
        direction="bearish",
        option_type="call",
        aliases=("call_credit", "call_credit_spread"),
        build_validator=VerticalSpreadBuildConfig.model_validate,
        candidate_builder=_build_call_verticals,
        coverage_counter=lambda market_slice: _single_side_coverage(
            market_slice=market_slice,
            option_type="call",
        ),
    ),
    TradeStructureSpec(
        strategy_family="put_credit_spread",
        candidate_builder_key="put_credit",
        display_label="Put Credit",
        direction="bullish",
        option_type="put",
        aliases=("put_credit", "put_credit_spread"),
        build_validator=VerticalSpreadBuildConfig.model_validate,
        candidate_builder=_build_put_verticals,
        coverage_counter=lambda market_slice: _single_side_coverage(
            market_slice=market_slice,
            option_type="put",
        ),
    ),
    TradeStructureSpec(
        strategy_family="call_debit_spread",
        candidate_builder_key="call_debit",
        display_label="Call Debit",
        direction="bullish",
        option_type="call",
        aliases=("call_debit", "call_debit_spread"),
        build_validator=VerticalSpreadBuildConfig.model_validate,
        candidate_builder=_build_call_verticals,
        coverage_counter=lambda market_slice: _single_side_coverage(
            market_slice=market_slice,
            option_type="call",
        ),
    ),
    TradeStructureSpec(
        strategy_family="put_debit_spread",
        candidate_builder_key="put_debit",
        display_label="Put Debit",
        direction="bearish",
        option_type="put",
        aliases=("put_debit", "put_debit_spread"),
        build_validator=VerticalSpreadBuildConfig.model_validate,
        candidate_builder=_build_put_verticals,
        coverage_counter=lambda market_slice: _single_side_coverage(
            market_slice=market_slice,
            option_type="put",
        ),
    ),
    TradeStructureSpec(
        strategy_family="long_call",
        candidate_builder_key="long_call",
        display_label="Long Call",
        direction="bullish",
        option_type="call",
        aliases=("long_call",),
        build_validator=LongVolBuildConfig.model_validate,
        candidate_builder=_build_long_call_candidates,
        coverage_counter=lambda market_slice: _single_side_coverage(
            market_slice=market_slice,
            option_type="call",
        ),
    ),
    TradeStructureSpec(
        strategy_family="long_put",
        candidate_builder_key="long_put",
        display_label="Long Put",
        direction="bearish",
        option_type="put",
        aliases=("long_put",),
        build_validator=LongVolBuildConfig.model_validate,
        candidate_builder=_build_long_put_candidates,
        coverage_counter=lambda market_slice: _single_side_coverage(
            market_slice=market_slice,
            option_type="put",
        ),
    ),
    TradeStructureSpec(
        strategy_family="short_call",
        candidate_builder_key="short_call",
        display_label="Short Call",
        direction="bearish",
        option_type="call",
        aliases=("short_call",),
        build_validator=LongVolBuildConfig.model_validate,
        candidate_builder=_build_short_call_candidates,
        coverage_counter=lambda market_slice: _single_side_coverage(
            market_slice=market_slice,
            option_type="call",
        ),
    ),
    TradeStructureSpec(
        strategy_family="short_put",
        candidate_builder_key="short_put",
        display_label="Short Put",
        direction="bullish",
        option_type="put",
        aliases=("short_put",),
        build_validator=LongVolBuildConfig.model_validate,
        candidate_builder=_build_short_put_candidates,
        coverage_counter=lambda market_slice: _single_side_coverage(
            market_slice=market_slice,
            option_type="put",
        ),
    ),
    TradeStructureSpec(
        strategy_family="iron_condor",
        candidate_builder_key="iron_condor",
        display_label="Iron Condor",
        direction="neutral",
        option_type=None,
        aliases=("iron_condor",),
        build_validator=IronCondorBuildConfig.model_validate,
        candidate_builder=_build_iron_condor_candidates,
        coverage_counter=_dual_side_coverage,
    ),
    TradeStructureSpec(
        strategy_family="long_straddle",
        candidate_builder_key="long_straddle",
        display_label="Long Straddle",
        direction="neutral",
        option_type=None,
        aliases=("long_straddle",),
        build_validator=LongVolBuildConfig.model_validate,
        candidate_builder=_build_long_straddle_candidates,
        coverage_counter=_dual_side_coverage,
    ),
    TradeStructureSpec(
        strategy_family="long_strangle",
        candidate_builder_key="long_strangle",
        display_label="Long Strangle",
        direction="neutral",
        option_type=None,
        aliases=("long_strangle",),
        build_validator=LongVolBuildConfig.model_validate,
        candidate_builder=_build_long_strangle_candidates,
        coverage_counter=_dual_side_coverage,
    ),
)

_SPECS_BY_ID = {identifier: spec for spec in _SPEC_LIST for identifier in spec.aliases}


def resolve_trade_structure_spec(strategy_id: Any) -> TradeStructureSpec:
    normalized = str(strategy_id or "").strip().lower()
    spec = _SPECS_BY_ID.get(normalized)
    if spec is None:
        raise ValueError(f"Unsupported strategy: {strategy_id}")
    return spec


def all_trade_structure_specs() -> tuple[TradeStructureSpec, ...]:
    return _SPEC_LIST


def trade_structure_display_label(strategy: str) -> str:
    if strategy == "auto":
        return "Auto"
    if strategy == "combined":
        return "Combined"
    return resolve_trade_structure_spec(strategy).display_label


def trade_structure_option_type(strategy: str) -> str:
    if strategy in {"combined", "auto"}:
        return "call"
    spec = resolve_trade_structure_spec(strategy)
    return spec.option_type or "call"


def trade_structure_direction(strategy: str) -> str:
    if strategy in {"combined", "auto"}:
        return "neutral"
    return resolve_trade_structure_spec(strategy).direction


def concrete_trade_structures(strategy: str) -> tuple[str, ...]:
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
    return (resolve_trade_structure_spec(strategy).candidate_builder_key,)


__all__ = [
    "TradeStructureSpec",
    "all_trade_structure_specs",
    "concrete_trade_structures",
    "resolve_trade_structure_spec",
    "trade_structure_direction",
    "trade_structure_display_label",
    "trade_structure_option_type",
]
