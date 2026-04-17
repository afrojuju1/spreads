from __future__ import annotations

import argparse
from dataclasses import replace

from core.common import clamp
from core.domain.models import SpreadCandidate
from core.domain.profiles import (
    LONG_VOL_STRATEGIES,
    zero_dte_delta_target,
    zero_dte_session_bucket,
)
from core.services.option_structures import net_premium_kind

from .shared import log_scaled_score


def score_candidate(candidate: SpreadCandidate, args: argparse.Namespace) -> float:
    premium_kind = net_premium_kind(candidate.strategy)
    long_vol = candidate.strategy in LONG_VOL_STRATEGIES
    session_bucket = zero_dte_session_bucket() if args.profile == "0dte" else None
    if args.profile == "0dte":
        delta_target = zero_dte_delta_target(session_bucket or "off_hours")
    elif candidate.strategy == "long_straddle":
        delta_target = 0.50
    else:
        delta_target = args.short_delta_target
    delta_half_band = max((args.short_delta_max - args.short_delta_min) / 2.0, 0.01)
    if candidate.strategy == "long_straddle":
        delta_half_band = 0.20
    delta_score = 1.0
    if candidate.short_delta is not None:
        delta_score = 1.0 - min(
            abs(abs(candidate.short_delta) - delta_target) / delta_half_band, 1.0
        )

    dte_target = (args.min_dte + args.max_dte) / 2.0
    dte_half_band = max((args.max_dte - args.min_dte) / 2.0, 1.0)
    dte_score = 1.0 - min(
        abs(candidate.days_to_expiration - dte_target) / dte_half_band, 1.0
    )

    fill_score = clamp(candidate.fill_ratio)
    liquidity_score = 0.75 * log_scaled_score(
        min(candidate.short_open_interest, candidate.long_open_interest),
        floor=max(args.min_open_interest, 1),
        ceiling=max(args.min_open_interest * 8, 10),
    ) + 0.25 * clamp(candidate.min_quote_size / 100.0)

    if candidate.strategy == "long_straddle":
        width_target = 0.0
    elif candidate.strategy == "long_strangle" and candidate.expected_move is not None:
        width_target = max(candidate.expected_move * 2.0, args.min_width)
    elif args.profile == "0dte":
        width_target = 2.0 if session_bucket == "late" else 1.0
    else:
        width_target = max(
            args.min_width, 2.0 if args.profile == "core" else args.min_width
        )
    width_window = max(args.max_width - args.min_width, 1.0)
    if candidate.strategy == "long_straddle":
        width_window = 1.0
    elif candidate.strategy == "long_strangle" and candidate.expected_move is not None:
        width_window = max(candidate.expected_move, 1.0)
    width_score = 1.0 - min(abs(candidate.width - width_target) / width_window, 1.0)

    if long_vol:
        return_on_risk_score = clamp(candidate.return_on_risk / 1.10)
    else:
        return_on_risk_score = clamp(candidate.return_on_risk / 0.60)
    if long_vol:
        breakeven_cushion_score = clamp(1.0 - (candidate.breakeven_cushion_pct / 0.06))
    elif premium_kind == "debit":
        breakeven_cushion_score = clamp(1.0 - (candidate.breakeven_cushion_pct / 0.08))
    else:
        breakeven_cushion_score = clamp(candidate.breakeven_cushion_pct / 0.035)

    if long_vol:
        short_expected_move_score = clamp(
            ((candidate.modeled_move_vs_implied_move or 0.85) - 0.80) / 0.35
        )
        breakeven_expected_move_score = clamp(
            ((candidate.modeled_move_vs_break_even_move or 0.85) - 0.80) / 0.30
        )
    elif candidate.expected_move and candidate.expected_move > 0:
        short_expected_move_score = clamp(
            0.50 + (candidate.short_vs_expected_move or 0.0) / candidate.expected_move
        )
        breakeven_expected_move_score = clamp(
            0.45
            + (candidate.breakeven_vs_expected_move or 0.0) / candidate.expected_move
        )
    else:
        short_expected_move_score = clamp(candidate.short_otm_pct / 0.03)
        breakeven_expected_move_score = breakeven_cushion_score

    base_score = (
        0.24 * delta_score
        + 0.18 * short_expected_move_score
        + 0.16 * breakeven_expected_move_score
        + 0.14 * fill_score
        + 0.12 * liquidity_score
        + 0.08 * width_score
        + 0.05 * dte_score
        + 0.03 * return_on_risk_score
    )

    calendar_multiplier = {
        "clean": 1.0,
        "penalized": 0.92,
        "unknown": 0.82,
        "blocked": 0.0,
    }.get(candidate.calendar_status, 1.0)
    setup_multiplier = {
        "favorable": 1.0,
        "neutral": 0.93,
        "unfavorable": 0.78,
        "unknown": 0.88,
    }.get(candidate.setup_status, 0.88)
    data_multiplier = {
        "clean": 1.0,
        "penalized": 0.90,
        "blocked": 0.0,
    }.get(candidate.data_status, 1.0)
    return round(
        base_score * calendar_multiplier * setup_multiplier * data_multiplier * 100.0, 1
    )


def rank_candidates(
    candidates: list[SpreadCandidate], args: argparse.Namespace
) -> list[SpreadCandidate]:
    ranked = [
        replace(candidate, quality_score=score_candidate(candidate, args))
        for candidate in candidates
    ]
    return sort_candidates_for_display(ranked)


def sort_candidates_for_display(
    candidates: list[SpreadCandidate],
) -> list[SpreadCandidate]:
    ranked = list(candidates)
    ranked.sort(
        key=lambda candidate: (
            candidate.quality_score,
            candidate.return_on_risk,
            candidate.midpoint_credit,
            min(candidate.short_open_interest, candidate.long_open_interest),
        ),
        reverse=True,
    )
    return ranked
