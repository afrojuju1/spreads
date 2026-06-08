from __future__ import annotations

import argparse
import math

from core.common import clamp
from core.domain.models import OptionSnapshot
from core.services.strategy_candidate_builders.runtime_context import (
    candidate_reference_date,
    candidate_session_bucket,
)


def effective_min_credit(width: float, args: argparse.Namespace) -> float:
    threshold = args.min_credit
    if args.profile != "0dte":
        return threshold
    session_bucket = candidate_session_bucket(args) or "off_hours"
    if session_bucket != "late":
        return threshold
    if width <= 1.0:
        return max(threshold, 0.10)
    return max(threshold, 0.15)


def days_from_reference(expiration_date: str, args: argparse.Namespace) -> int:
    from datetime import date

    return (date.fromisoformat(expiration_date) - candidate_reference_date(args)).days


def relative_spread(snapshot: OptionSnapshot) -> float:
    return (snapshot.ask - snapshot.bid) / snapshot.midpoint


def relative_spread_exceeds(
    snapshot: OptionSnapshot,
    maximum: float,
    *,
    tolerance: float = 1e-9,
) -> bool:
    return relative_spread(snapshot) > float(maximum) + tolerance


def log_scaled_score(value: int, floor: int, ceiling: int) -> float:
    if value <= floor:
        return 0.0
    if value >= ceiling:
        return 1.0
    numerator = math.log10(value) - math.log10(max(floor, 1))
    denominator = math.log10(max(ceiling, 1)) - math.log10(max(floor, 1))
    if denominator <= 0:
        return 0.0
    return clamp(numerator / denominator)
