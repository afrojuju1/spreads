from __future__ import annotations

import argparse
from collections import defaultdict
from math import log1p
from typing import Any, Mapping

from core.common import clamp
from core.domain.models import OptionContract, OptionSnapshot, UniverseScanFailure
from core.integrations.alpaca.client import AlpacaClient
from core.services.scanners.config import resolve_symbol_scan_args
from core.services.scanners.builders.shared import days_from_reference, relative_spread
from core.services.scanners.runtime import build_symbol_market_slice

UOA_WATCHLIST_EXPIRY_LIMIT = 2
UOA_WATCHLIST_CONTRACTS_PER_TYPE_PER_EXPIRY = 3
UOA_WATCHLIST_MIN_OPEN_INTEREST_FLOOR = 25
UOA_WATCHLIST_MIN_OPEN_INTEREST_CAP = 100
UOA_WATCHLIST_MAX_RELATIVE_SPREAD_FLOOR = 0.25
UOA_WATCHLIST_ATM_DISTANCE_CEILING = 0.12
UOA_WATCHLIST_DELTA_DISTANCE_CEILING = 0.35
UOA_WATCHLIST_VOLUME_CEILING = 2500.0
UOA_WATCHLIST_OPEN_INTEREST_CEILING = 5000.0


def _watchlist_min_open_interest(args: argparse.Namespace) -> int:
    configured = int(getattr(args, "min_open_interest", 0) or 0)
    if configured <= 0:
        return UOA_WATCHLIST_MIN_OPEN_INTEREST_CAP
    return max(
        min(configured, UOA_WATCHLIST_MIN_OPEN_INTEREST_CAP),
        UOA_WATCHLIST_MIN_OPEN_INTEREST_FLOOR,
    )


def _watchlist_max_relative_spread(args: argparse.Namespace) -> float:
    configured = float(getattr(args, "max_relative_spread", 0.0) or 0.0)
    return max(configured, UOA_WATCHLIST_MAX_RELATIVE_SPREAD_FLOOR)


def _watchlist_target_delta(args: argparse.Namespace) -> float:
    configured = getattr(args, "short_delta_target", None)
    if configured in (None, ""):
        return 0.25
    return clamp(float(configured), 0.05, 0.5)


def _watchlist_score(
    *,
    contract: OptionContract,
    snapshot: OptionSnapshot,
    spot_price: float,
    target_delta: float,
    max_relative_spread: float,
) -> float:
    atm_distance_pct = (
        abs(contract.strike_price - spot_price) / spot_price if spot_price > 0 else 1.0
    )
    atm_score = clamp(1.0 - (atm_distance_pct / UOA_WATCHLIST_ATM_DISTANCE_CEILING))
    if snapshot.delta is None:
        delta_score = 0.5
    else:
        delta_score = clamp(
            1.0
            - (
                abs(abs(float(snapshot.delta)) - target_delta)
                / UOA_WATCHLIST_DELTA_DISTANCE_CEILING
            )
        )
    spread_score = clamp(1.0 - (relative_spread(snapshot) / max_relative_spread))
    open_interest_score = clamp(
        log1p(max(contract.open_interest, 0)) / log1p(UOA_WATCHLIST_OPEN_INTEREST_CEILING)
    )
    volume_score = clamp(
        log1p(max(int(snapshot.daily_volume or 0), 0)) / log1p(UOA_WATCHLIST_VOLUME_CEILING)
    )
    return round(
        atm_score * 45.0
        + delta_score * 15.0
        + spread_score * 20.0
        + open_interest_score * 10.0
        + volume_score * 10.0,
        1,
    )


def _option_candidate_row(
    *,
    underlying_symbol: str,
    option_type: str,
    contract: OptionContract,
    snapshot: OptionSnapshot,
    spot_price: float,
    args: argparse.Namespace,
    score: float,
    feed_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    dte = days_from_reference(contract.expiration_date, args)
    row = {
        "underlying_symbol": underlying_symbol,
        "strategy": "uoa_watchlist",
        "leg_role": "contract",
        "option_symbol": contract.symbol,
        "option_type": option_type,
        "expiration_date": contract.expiration_date,
        "days_to_expiration": dte,
        "dte": dte,
        "strike_price": float(contract.strike_price),
        "underlying_price": float(spot_price),
        "open_interest": int(contract.open_interest),
        "volume": int(snapshot.daily_volume or 0),
        "implied_volatility": snapshot.implied_volatility,
        "delta": snapshot.delta,
        "gamma": snapshot.gamma,
        "vega": snapshot.vega,
        "bid": float(snapshot.bid),
        "ask": float(snapshot.ask),
        "midpoint": float(snapshot.midpoint),
        "bid_size": int(snapshot.bid_size),
        "ask_size": int(snapshot.ask_size),
        "last_trade_price": snapshot.last_trade_price,
        "relative_spread": round(relative_spread(snapshot), 4),
        "quality_score": score,
    }
    if isinstance(feed_context, Mapping):
        row["feed_rank"] = feed_context.get("feed_rank")
        row["feed_score"] = feed_context.get("score")
        row["feed_reason_codes"] = list(feed_context.get("reason_codes") or [])
        row["feed_source_tags"] = list(feed_context.get("source_tags") or [])
        row["underlying_daily_volume"] = feed_context.get("daily_volume")
        row["underlying_move_percent"] = feed_context.get("move_percent")
        row["underlying_news_count"] = feed_context.get("news_count")
        row["underlying_most_active_rank"] = feed_context.get("most_active_rank")
        row["underlying_gainer_rank"] = feed_context.get("gainer_rank")
        row["underlying_loser_rank"] = feed_context.get("loser_rank")
        row["underlying_trade_count"] = feed_context.get("trade_count")
        row["underlying_price_snapshot"] = feed_context.get("price")
    return row


def _select_contract_rows(
    *,
    underlying_symbol: str,
    option_type: str,
    contracts: list[OptionContract],
    snapshots_by_symbol: dict[str, OptionSnapshot],
    spot_price: float,
    args: argparse.Namespace,
    contracts_per_type: int,
    min_open_interest: int,
    max_relative_spread: float,
    target_delta: float,
    feed_context: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    ranked_rows: list[dict[str, Any]] = []
    for contract in contracts:
        snapshot = snapshots_by_symbol.get(contract.symbol)
        if snapshot is None:
            continue
        if contract.open_interest < min_open_interest:
            continue
        if snapshot.bid_size <= 0 or snapshot.ask_size <= 0:
            continue
        if relative_spread(snapshot) > max_relative_spread:
            continue
        score = _watchlist_score(
            contract=contract,
            snapshot=snapshot,
            spot_price=spot_price,
            target_delta=target_delta,
            max_relative_spread=max_relative_spread,
        )
        ranked_rows.append(
            _option_candidate_row(
                underlying_symbol=underlying_symbol,
                option_type=option_type,
                contract=contract,
                snapshot=snapshot,
                spot_price=spot_price,
                args=args,
                score=score,
                feed_context=feed_context,
            )
        )
    ranked_rows.sort(
        key=lambda row: (
            float(row.get("quality_score") or 0.0),
            -int(row.get("days_to_expiration") or 0),
            str(row.get("option_symbol") or ""),
        ),
        reverse=True,
    )
    return ranked_rows[: max(contracts_per_type, 1)]


def build_uoa_capture_candidates_from_symbols(
    *,
    symbols: list[str],
    scanner_args: argparse.Namespace,
    client: AlpacaClient,
    greeks_provider: Any,
    feed_entries_by_symbol: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[UniverseScanFailure], dict[str, Any]]:
    normalized_symbols = [
        str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()
    ]
    expiry_limit = UOA_WATCHLIST_EXPIRY_LIMIT
    contracts_per_type = UOA_WATCHLIST_CONTRACTS_PER_TYPE_PER_EXPIRY
    candidates: list[dict[str, Any]] = []
    failures: list[UniverseScanFailure] = []
    per_symbol_contract_counts: dict[str, int] = {}
    per_expiration_contract_counts: dict[str, int] = defaultdict(int)
    per_symbol_constraints: dict[str, dict[str, float | int]] = {}
    feed_rank_by_symbol = {
        symbol: index for index, symbol in enumerate(normalized_symbols, start=1)
    }
    normalized_feed_entries = (
        {}
        if feed_entries_by_symbol is None
        else {
            str(symbol).strip().upper(): dict(payload)
            for symbol, payload in feed_entries_by_symbol.items()
            if str(symbol).strip() and isinstance(payload, Mapping)
        }
    )

    for symbol in normalized_symbols:
        try:
            symbol_args, _underlying_type = resolve_symbol_scan_args(
                symbol=symbol,
                base_args=scanner_args,
            )
            min_open_interest = _watchlist_min_open_interest(symbol_args)
            max_relative_spread = _watchlist_max_relative_spread(symbol_args)
            target_delta = _watchlist_target_delta(symbol_args)
            per_symbol_constraints[symbol] = {
                "min_open_interest": min_open_interest,
                "max_relative_spread": round(max_relative_spread, 4),
                "target_delta": round(target_delta, 4),
                "min_dte": int(symbol_args.min_dte),
                "max_dte": int(symbol_args.max_dte),
            }
            market_slice = build_symbol_market_slice(
                symbol=symbol,
                symbol_args=symbol_args,
                client=client,
                greeks_provider=greeks_provider,
            )
        except Exception as exc:
            failures.append(
                UniverseScanFailure(symbol=symbol, error=str(exc).splitlines()[0])
            )
            continue

        available_expirations = sorted(
            set(market_slice.call_contracts_by_expiration).union(
                market_slice.put_contracts_by_expiration
            )
        )[:expiry_limit]
        symbol_rows: list[dict[str, Any]] = []
        for expiration_date in available_expirations:
            feed_context = normalized_feed_entries.get(symbol)
            if feed_context is not None and feed_context.get("feed_rank") in (None, ""):
                feed_context = dict(feed_context)
                feed_context["feed_rank"] = feed_rank_by_symbol.get(symbol)
            symbol_rows.extend(
                _select_contract_rows(
                    underlying_symbol=symbol,
                    option_type="call",
                    contracts=list(
                        market_slice.call_contracts_by_expiration.get(
                            expiration_date, []
                        )
                    ),
                    snapshots_by_symbol=dict(
                        market_slice.call_snapshots_by_expiration.get(
                            expiration_date, {}
                        )
                    ),
                    spot_price=float(market_slice.spot_price),
                    args=symbol_args,
                    contracts_per_type=contracts_per_type,
                    min_open_interest=min_open_interest,
                    max_relative_spread=max_relative_spread,
                    target_delta=target_delta,
                    feed_context=feed_context,
                )
            )
            symbol_rows.extend(
                _select_contract_rows(
                    underlying_symbol=symbol,
                    option_type="put",
                    contracts=list(
                        market_slice.put_contracts_by_expiration.get(
                            expiration_date, []
                        )
                    ),
                    snapshots_by_symbol=dict(
                        market_slice.put_snapshots_by_expiration.get(
                            expiration_date, {}
                        )
                    ),
                    spot_price=float(market_slice.spot_price),
                    args=symbol_args,
                    contracts_per_type=contracts_per_type,
                    min_open_interest=min_open_interest,
                    max_relative_spread=max_relative_spread,
                    target_delta=target_delta,
                    feed_context=feed_context,
                )
            )
        per_symbol_contract_counts[symbol] = len(symbol_rows)
        for row in symbol_rows:
            per_expiration_contract_counts[str(row["expiration_date"])] += 1
        candidates.extend(symbol_rows)

    candidates.sort(
        key=lambda row: (
            int(feed_rank_by_symbol.get(str(row.get("underlying_symbol") or ""), 9999)),
            int(row.get("days_to_expiration") or 9999),
            str(row.get("option_type") or ""),
            -float(row.get("quality_score") or 0.0),
            str(row.get("option_symbol") or ""),
        )
    )
    summary = {
        "symbol_count": len(normalized_symbols),
        "captured_symbol_count": sum(
            1 for count in per_symbol_contract_counts.values() if count > 0
        ),
        "contract_count": len(candidates),
        "failed_symbol_count": len(failures),
        "empty_symbol_count": sum(
            1 for count in per_symbol_contract_counts.values() if count <= 0
        ),
        "expiry_limit": expiry_limit,
        "contracts_per_type_per_expiry": contracts_per_type,
        "per_symbol_constraints": dict(sorted(per_symbol_constraints.items())),
        "per_symbol_contract_counts": dict(sorted(per_symbol_contract_counts.items())),
        "per_expiration_contract_counts": dict(
            sorted(per_expiration_contract_counts.items())
        ),
    }
    return candidates, failures, summary


__all__ = ["build_uoa_capture_candidates_from_symbols"]
