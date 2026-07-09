from __future__ import annotations

from collections.abc import Mapping
import os
from datetime import UTC, datetime, timedelta
from typing import Any


from core.money import money_sum_float
from core.services.risk.buying_power import (
    estimate_buying_power_requirement,
)
from core.services.option_structures import (
    candidate_legs,
    position_legs,
)
from core.services.trading_strategy_risk_models import PROTECTION_RULE_KEYS
from core.value_coercion import (
    as_mapping,
    as_text,
    coerce_bool,
    coerce_float,
    coerce_utc_datetime,
    unique_text_list,
    utc_now_iso,
)

from core.services.risk.candidates import (
    _candidate_max_loss,
    _candidate_root_symbol,
    _candidate_strategy_family,
    _date_text,
    _portfolio_correlation_group,
)
from core.services.risk.policy import (
    OPEN_POSITION_STATUSES,
    PROTECTION_ADMISSIBLE_STATUSES,
    PROTECTION_ADMISSION_BOUNDARY,
    ProtectionRuleConfig,
)
from core.services.risk.allocation import _allocation_plan_admission_evidence
from core.services.risk.exposures import _open_portfolio_exposures, _portfolio_schema_ready

def _protection_activity_at(row: Mapping[str, Any]) -> datetime | None:
    for key in ("closed_at", "opened_at", "updated_at", "created_at", "requested_at"):
        value = row.get(key)
        if value in (None, ""):
            continue
        parsed = coerce_utc_datetime(value)
        if parsed is not None:
            return parsed.astimezone(UTC)
    date_text = _date_text(row.get("market_date_closed") or row.get("market_date_opened") or row.get("market_date"))
    if date_text is None:
        return None
    try:
        return datetime.fromisoformat(date_text).replace(tzinfo=UTC)
    except ValueError:
        return None


def _position_net_pnl(row: Mapping[str, Any]) -> float:
    return money_sum_float(
        [
            coerce_float(row.get("realized_pnl")) or 0.0,
            coerce_float(row.get("unrealized_pnl")) or 0.0,
        ]
    )


def _scoped_positions(
    positions: list[dict[str, Any]],
    *,
    rule: ProtectionRuleConfig,
    trading_strategy_id: str,
    strategy_family: str | None,
) -> list[dict[str, Any]]:
    scope = as_text(rule.get("scope")) or "account"
    if scope == "strategy":
        return [row for row in positions if as_text(row.get("trading_strategy_id")) == trading_strategy_id]
    if scope in {"strategy_family", "family"}:
        return [row for row in positions if as_text(row.get("strategy_family") or row.get("trade_structure")) == strategy_family]
    return list(positions)


def _protection_block_item(reason: str, message: str, metrics: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "reason": reason,
        "message": message,
        "metrics": dict(metrics),
    }


def _account_emergency_stop_block(rule: ProtectionRuleConfig) -> dict[str, Any] | None:
    if not rule.enabled:
        return None
    configured_halt = bool(
        coerce_bool(rule.get("halted"), default=False)
        or coerce_bool(rule.get("emergency_stop"), default=False)
        or coerce_bool(rule.get("triggered"), default=False)
    )
    env_halt = bool(
        coerce_bool(os.environ.get("SPREADS_ACCOUNT_EMERGENCY_STOP"), default=False)
        or coerce_bool(os.environ.get("SPREADS_EXECUTION_KILL_SWITCH"), default=False)
    )
    metrics = {
        "configured_halt": configured_halt,
        "environment_halt": env_halt,
    }
    if configured_halt or env_halt:
        return _protection_block_item(
            "account_emergency_stop",
            "Account-level emergency stop is active.",
            metrics,
        )
    return None


def _drawdown_block(
    *,
    rule_name: str,
    rule: ProtectionRuleConfig,
    positions: list[dict[str, Any]],
    trading_strategy_id: str,
    strategy_family: str | None,
    session_date: str,
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if not rule.enabled:
        return {"enabled": False}, None
    scoped = _scoped_positions(
        positions,
        rule=rule,
        trading_strategy_id=trading_strategy_id,
        strategy_family=strategy_family,
    )
    if rule_name == "daily_drawdown_halt":
        window_positions = [
            row
            for row in scoped
            if session_date
            in {
                _date_text(row.get("market_date_opened") or row.get("opened_at")),
                _date_text(row.get("market_date_closed") or row.get("closed_at")),
                _date_text(row.get("updated_at")),
            }
        ]
    else:
        window_days = rule.positive_int("window_days") or 5
        start = now - timedelta(days=window_days)
        window_positions = [row for row in scoped if (activity_at := _protection_activity_at(row)) is not None and activity_at >= start]

    open_positions = [row for row in scoped if as_text(row.get("status")) in OPEN_POSITION_STATUSES]
    realized = money_sum_float(coerce_float(row.get("realized_pnl")) for row in window_positions)
    unrealized = money_sum_float(coerce_float(row.get("unrealized_pnl")) for row in open_positions)
    net = money_sum_float([realized, unrealized])
    max_realized_loss = rule.positive_float("max_realized_loss", "max_loss")
    max_net_loss = rule.positive_float("max_net_loss", "max_drawdown")
    metrics = {
        "enabled": True,
        "scope": as_text(rule.get("scope")) or "account",
        "position_count": len(window_positions),
        "open_position_count": len(open_positions),
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "net_pnl": net,
        "max_realized_loss": max_realized_loss,
        "max_net_loss": max_net_loss,
    }
    if rule_name == "rolling_drawdown_halt":
        metrics["window_days"] = rule.positive_int("window_days") or 5
    if max_realized_loss is not None and realized <= -abs(max_realized_loss):
        return metrics, _protection_block_item(
            f"{rule_name}_realized_loss",
            "Realized PnL breached the configured protection loss limit.",
            metrics,
        )
    if max_net_loss is not None and net <= -abs(max_net_loss):
        return metrics, _protection_block_item(
            f"{rule_name}_net_loss",
            "Net PnL breached the configured protection drawdown limit.",
            metrics,
        )
    return metrics, None


def _closed_loss_positions(
    positions: list[dict[str, Any]],
    *,
    rule: ProtectionRuleConfig,
    trading_strategy_id: str,
    strategy_family: str | None,
) -> list[dict[str, Any]]:
    scoped = _scoped_positions(
        positions,
        rule=rule,
        trading_strategy_id=trading_strategy_id,
        strategy_family=strategy_family,
    )
    closed = [
        row
        for row in scoped
        if as_text(row.get("status")) == "closed" or row.get("closed_at") is not None or row.get("market_date_closed") is not None
    ]
    closed.sort(key=lambda row: _protection_activity_at(row) or datetime.min.replace(tzinfo=UTC), reverse=True)
    return closed


def _loss_streak_block(
    *,
    rule: ProtectionRuleConfig,
    positions: list[dict[str, Any]],
    trading_strategy_id: str,
    strategy_family: str | None,
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if not rule.enabled:
        return {"enabled": False}, None
    closed = _closed_loss_positions(
        positions,
        rule=rule,
        trading_strategy_id=trading_strategy_id,
        strategy_family=strategy_family,
    )
    loss_threshold = abs(coerce_float(rule.get("loss_threshold")) or 0.0)
    streak = 0
    latest_loss_at: datetime | None = None
    for row in closed:
        net_pnl = _position_net_pnl(row)
        if net_pnl < -loss_threshold:
            streak += 1
            latest_loss_at = latest_loss_at or _protection_activity_at(row)
            continue
        break
    max_losses = rule.positive_int("max_consecutive_losses")
    cooldown_minutes = rule.positive_int("cooldown_minutes")
    cooldown_active = False
    if max_losses is not None and streak >= max_losses and latest_loss_at is not None:
        cooldown_active = cooldown_minutes is None or now < latest_loss_at + timedelta(minutes=cooldown_minutes)
    metrics = {
        "enabled": True,
        "scope": as_text(rule.get("scope")) or "account",
        "closed_position_count": len(closed),
        "consecutive_loss_count": streak,
        "max_consecutive_losses": max_losses,
        "cooldown_minutes": cooldown_minutes,
        "latest_loss_at": None if latest_loss_at is None else latest_loss_at.isoformat(),
        "cooldown_active": cooldown_active,
    }
    if cooldown_active:
        return metrics, _protection_block_item(
            "loss_streak_cooldown_active",
            "Recent consecutive losses activated the configured cooldown.",
            metrics,
        )
    return metrics, None


def _strategy_family_cooldown_block(
    *,
    rule: ProtectionRuleConfig,
    positions: list[dict[str, Any]],
    trading_strategy_id: str,
    strategy_family: str | None,
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if not rule.enabled:
        return {"enabled": False}, None
    family_positions = _scoped_positions(
        positions,
        rule=rule.model_copy(update={"scope": "strategy_family"}),
        trading_strategy_id=trading_strategy_id,
        strategy_family=strategy_family,
    )
    latest_entry_at = max(
        (_protection_activity_at(row) for row in family_positions if as_text(row.get("status")) in OPEN_POSITION_STATUSES),
        default=None,
    )
    loss_rule = rule.model_copy(update={"scope": "strategy_family"})
    latest_loss = next(
        (
            row
            for row in _closed_loss_positions(
                positions,
                rule=loss_rule,
                trading_strategy_id=trading_strategy_id,
                strategy_family=strategy_family,
            )
            if _position_net_pnl(row) < 0
        ),
        None,
    )
    latest_loss_at = None if latest_loss is None else _protection_activity_at(latest_loss)
    after_entry_minutes = rule.positive_int("cooldown_minutes_after_entry")
    after_loss_minutes = rule.positive_int("cooldown_minutes_after_loss")
    entry_cooldown_active = (
        after_entry_minutes is not None and latest_entry_at is not None and now < latest_entry_at + timedelta(minutes=after_entry_minutes)
    )
    loss_cooldown_active = (
        after_loss_minutes is not None and latest_loss_at is not None and now < latest_loss_at + timedelta(minutes=after_loss_minutes)
    )
    metrics = {
        "enabled": True,
        "strategy_family": strategy_family,
        "family_position_count": len(family_positions),
        "latest_entry_at": None if latest_entry_at is None else latest_entry_at.isoformat(),
        "latest_loss_at": None if latest_loss_at is None else latest_loss_at.isoformat(),
        "cooldown_minutes_after_entry": after_entry_minutes,
        "cooldown_minutes_after_loss": after_loss_minutes,
        "entry_cooldown_active": entry_cooldown_active,
        "loss_cooldown_active": loss_cooldown_active,
    }
    if entry_cooldown_active:
        return metrics, _protection_block_item(
            "strategy_family_entry_cooldown_active",
            "Strategy-family entry cooldown is active.",
            metrics,
        )
    if loss_cooldown_active:
        return metrics, _protection_block_item(
            "strategy_family_loss_cooldown_active",
            "Strategy-family loss cooldown is active.",
            metrics,
        )
    return metrics, None


def _event_calendar_block(
    *,
    rule: ProtectionRuleConfig,
    candidate_symbol: str | None,
    session_date: str,
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if not rule.enabled:
        return {"enabled": False}, None
    blocked_dates = set(unique_text_list(rule.get("blocked_dates"), accept_scalar=True))
    blocked_symbols = {item.upper() for item in unique_text_list(rule.get("blocked_symbols"), accept_scalar=True)}
    blocked_until_value = rule.get("blocked_until")
    blocked_until = None if blocked_until_value in (None, "") else coerce_utc_datetime(blocked_until_value)
    events = [dict(item) for item in rule.get("events", []) if isinstance(item, Mapping)]
    matching_events = [
        event
        for event in events
        if (
            as_text(event.get("date")) == session_date
            and (
                candidate_symbol is None
                or not unique_text_list(event.get("symbols"), accept_scalar=True)
                or candidate_symbol in {symbol.upper() for symbol in unique_text_list(event.get("symbols"), accept_scalar=True)}
            )
        )
    ]
    blocked = bool(
        coerce_bool(rule.get("blocked"), default=False)
        or session_date in blocked_dates
        or (candidate_symbol is not None and candidate_symbol in blocked_symbols)
        or (blocked_until is not None and now < blocked_until)
        or matching_events
    )
    metrics = {
        "enabled": True,
        "blocked": blocked,
        "blocked_date_count": len(blocked_dates),
        "blocked_symbol_count": len(blocked_symbols),
        "matching_event_count": len(matching_events),
        "blocked_until": None if blocked_until is None else blocked_until.isoformat(),
    }
    if blocked:
        return metrics, _protection_block_item(
            "event_calendar_block",
            "Event/news/calendar protection blocks new entries.",
            metrics,
        )
    return metrics, None


def _duplicate_exposure_block(
    *,
    rule: ProtectionRuleConfig,
    active_exposures: list[dict[str, Any]],
    candidate_symbol: str | None,
    candidate_correlation_group: str | None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if not rule.enabled:
        return {"enabled": False}, None
    same_underlying = [row for row in active_exposures if as_text(row.get("underlying_symbol")) == candidate_symbol]
    same_theme = [
        row
        for row in active_exposures
        if candidate_correlation_group is not None
        and candidate_correlation_group != candidate_symbol
        and as_text(row.get("correlation_group")) == candidate_correlation_group
    ]
    max_same_underlying = rule.positive_int("max_open_same_underlying")
    max_same_theme = rule.positive_int("max_open_same_theme")
    metrics = {
        "enabled": True,
        "candidate_symbol": candidate_symbol,
        "candidate_theme": candidate_correlation_group,
        "same_underlying_count": len(same_underlying),
        "same_theme_count": len(same_theme),
        "max_open_same_underlying": max_same_underlying,
        "max_open_same_theme": max_same_theme,
    }
    if max_same_underlying is not None and len(same_underlying) >= max_same_underlying:
        return metrics, _protection_block_item(
            "duplicate_underlying_exposure_cap",
            "Active portfolio exposure already exists for this underlying.",
            metrics,
        )
    if max_same_theme is not None and len(same_theme) >= max_same_theme:
        return metrics, _protection_block_item(
            "duplicate_theme_exposure_cap",
            "Active portfolio exposure already reaches the configured theme cap.",
            metrics,
        )
    return metrics, None


def _short_leg_contract_count(candidate: Mapping[str, Any], quantity: float) -> float:
    legs = candidate_legs(dict(candidate))
    if not legs:
        family = _candidate_strategy_family(candidate)
        return quantity if family in {"short_call", "short_put", "call_credit_spread", "put_credit_spread", "iron_condor"} else 0.0
    short_leg_count = sum(1 for leg in legs if as_text(leg.get("role")) == "short")
    return float(short_leg_count) * quantity


def _active_short_contract_count(positions: list[dict[str, Any]]) -> float:
    total = 0.0
    for row in positions:
        if as_text(row.get("status")) not in OPEN_POSITION_STATUSES:
            continue
        quantity = coerce_float(row.get("remaining_quantity")) or 0.0
        legs = position_legs(row)
        if not legs:
            continue
        total += quantity * sum(1 for leg in legs if as_text(leg.get("role")) == "short")
    return total


def _options_exposure_block(
    *,
    rule: ProtectionRuleConfig,
    active_exposures: list[dict[str, Any]],
    positions: list[dict[str, Any]],
    candidate: Mapping[str, Any],
    quantity: float,
    candidate_max_loss: float | None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if not rule.enabled:
        return {"enabled": False}, None
    active_contracts = money_sum_float(coerce_float(row.get("contract_count")) or 1.0 for row in active_exposures)
    active_max_loss = money_sum_float(coerce_float(row.get("max_loss")) for row in active_exposures)
    active_short_contracts = _active_short_contract_count(positions)
    candidate_short_contracts = _short_leg_contract_count(candidate, quantity)
    max_open_contracts = rule.positive_int("max_open_option_contracts")
    max_total_max_loss = rule.positive_float("max_total_max_loss", "max_scenario_loss")
    max_short_contracts = rule.positive_int("max_short_option_contracts")
    total_contracts_after = active_contracts + quantity
    total_max_loss_after = None if candidate_max_loss is None else money_sum_float([active_max_loss, candidate_max_loss])
    total_short_after = active_short_contracts + candidate_short_contracts
    metrics = {
        "enabled": True,
        "active_option_contract_count": active_contracts,
        "candidate_option_contract_count": quantity,
        "total_option_contract_count_after": total_contracts_after,
        "active_max_loss": active_max_loss,
        "candidate_max_loss": candidate_max_loss,
        "total_max_loss_after": total_max_loss_after,
        "active_short_option_contract_count": active_short_contracts,
        "candidate_short_option_contract_count": candidate_short_contracts,
        "total_short_option_contract_count_after": total_short_after,
        "max_open_option_contracts": max_open_contracts,
        "max_total_max_loss": max_total_max_loss,
        "max_short_option_contracts": max_short_contracts,
    }
    if max_open_contracts is not None and total_contracts_after > max_open_contracts:
        return metrics, _protection_block_item(
            "options_contract_exposure_cap",
            "Open option contract exposure would exceed the configured cap.",
            metrics,
        )
    if max_total_max_loss is not None and total_max_loss_after is not None and total_max_loss_after > max_total_max_loss:
        return metrics, _protection_block_item(
            "options_max_loss_scenario_cap",
            "Option max-loss scenario exposure would exceed the configured cap.",
            metrics,
        )
    if max_short_contracts is not None and total_short_after > max_short_contracts:
        return metrics, _protection_block_item(
            "options_short_contract_exposure_cap",
            "Short option contract exposure would exceed the configured cap.",
            metrics,
        )
    return metrics, None


def _protection_payload(
    *,
    status: str,
    reason: str,
    message: str,
    policy: Mapping[str, Any],
    metrics: Mapping[str, Any],
    evidence: Mapping[str, Any],
    blockers: list[str],
    evaluated_at: str,
) -> dict[str, Any]:
    reason_codes = [reason] if reason else []
    for blocker in blockers:
        if blocker not in reason_codes:
            reason_codes.append(blocker)
    return {
        "status": status,
        "reason": reason,
        "message": message,
        "admission_boundary": PROTECTION_ADMISSION_BOUNDARY,
        "admissible_quantity": 1 if status in PROTECTION_ADMISSIBLE_STATUSES else 0,
        "reason_codes": reason_codes,
        "blockers": blockers,
        "policy": dict(policy),
        "metrics": dict(metrics),
        "evidence": dict(evidence),
        "evaluated_at": evaluated_at,
    }


def build_protection_admission_snapshot(
    *,
    execution_store: Any,
    candidate: dict[str, Any],
    trading_strategy_id: str,
    strategy_family: str | None,
    session_date: str,
    policy: Mapping[str, Any] | None,
    quantity: int | float = 1,
    limit_price: float | None = None,
    allocation_plan: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    evaluated_at = utc_now_iso()
    now = coerce_utc_datetime(evaluated_at) or datetime.now(UTC)
    normalized_policy = dict(policy or {})
    raw_rules = as_mapping(normalized_policy.get("rules"))
    rules = {rule_name: ProtectionRuleConfig.model_validate(as_mapping(raw_rules.get(rule_name))) for rule_name in PROTECTION_RULE_KEYS}
    if not _portfolio_schema_ready(execution_store):
        return _protection_payload(
            status="unknown",
            reason="protection_schema_unavailable",
            message="Protection admission could not read the required lifecycle schemas.",
            policy=normalized_policy,
            metrics={},
            evidence={},
            blockers=["protection_schema_unavailable"],
            evaluated_at=evaluated_at,
        )

    try:
        active_exposures = _open_portfolio_exposures(execution_store)
        positions = [dict(row) for row in execution_store.list_positions(limit=1000)]
    except Exception as exc:
        return _protection_payload(
            status="unknown",
            reason="protection_state_unavailable",
            message=str(exc),
            policy=normalized_policy,
            metrics={},
            evidence={},
            blockers=["protection_state_unavailable"],
            evaluated_at=evaluated_at,
        )

    resolved_quantity = coerce_float(quantity) or 1.0
    candidate_symbol = _candidate_root_symbol(candidate)
    candidate_family = _candidate_strategy_family(candidate, strategy_family=strategy_family)
    candidate_correlation_group = _portfolio_correlation_group(candidate_symbol)
    candidate_max_loss = _candidate_max_loss(candidate, resolved_quantity)
    if candidate_max_loss is None:
        requirement = estimate_buying_power_requirement(candidate, resolved_quantity, limit_price=limit_price)
        candidate_max_loss = coerce_float(requirement.get("required_buying_power"))
    if candidate_symbol is None or candidate_family is None:
        return _protection_payload(
            status="unknown",
            reason="protection_candidate_identity_unavailable",
            message="Protection admission could not resolve the candidate symbol and strategy family.",
            policy=normalized_policy,
            metrics={},
            evidence={"candidate_symbol": candidate_symbol, "strategy_family": candidate_family},
            blockers=["protection_candidate_identity_unavailable"],
            evaluated_at=evaluated_at,
        )

    metrics: dict[str, Any] = {
        "rule_count": len([rule for rule in rules.values() if rule.configured]),
        "enabled_rule_count": sum(1 for rule in rules.values() if rule.enabled),
        "active_exposure_count": len(active_exposures),
        "position_count": len(positions),
        "candidate_symbol": candidate_symbol,
        "candidate_strategy_family": candidate_family,
        "candidate_correlation_group": candidate_correlation_group,
        "candidate_quantity": resolved_quantity,
        "candidate_max_loss": candidate_max_loss,
    }
    evidence: dict[str, Any] = {
        "candidate": {
            "underlying_symbol": candidate_symbol,
            "strategy_family": candidate_family,
            "trading_strategy_id": trading_strategy_id,
            "correlation_group": candidate_correlation_group,
        },
        "active_exposures": active_exposures[:25],
    }
    if isinstance(allocation_plan, Mapping):
        evidence["allocation_plan"] = _allocation_plan_admission_evidence(allocation_plan)

    blocks: list[dict[str, Any]] = []
    account_block = _account_emergency_stop_block(rules["account_emergency_stop"])
    if account_block is not None:
        blocks.append(account_block)

    for rule_name in ("daily_drawdown_halt", "rolling_drawdown_halt"):
        rule_metrics, block = _drawdown_block(
            rule_name=rule_name,
            rule=rules[rule_name],
            positions=positions,
            trading_strategy_id=trading_strategy_id,
            strategy_family=candidate_family,
            session_date=session_date,
            now=now,
        )
        metrics[rule_name] = rule_metrics
        if block is not None:
            blocks.append(block)

    rule_metrics, block = _loss_streak_block(
        rule=rules["loss_streak_cooldown"],
        positions=positions,
        trading_strategy_id=trading_strategy_id,
        strategy_family=candidate_family,
        now=now,
    )
    metrics["loss_streak_cooldown"] = rule_metrics
    if block is not None:
        blocks.append(block)

    rule_metrics, block = _strategy_family_cooldown_block(
        rule=rules["strategy_family_cooldown"],
        positions=positions,
        trading_strategy_id=trading_strategy_id,
        strategy_family=candidate_family,
        now=now,
    )
    metrics["strategy_family_cooldown"] = rule_metrics
    if block is not None:
        blocks.append(block)

    rule_metrics, block = _event_calendar_block(
        rule=rules["event_calendar_block"],
        candidate_symbol=candidate_symbol,
        session_date=session_date,
        now=now,
    )
    metrics["event_calendar_block"] = rule_metrics
    if block is not None:
        blocks.append(block)

    rule_metrics, block = _duplicate_exposure_block(
        rule=rules["duplicate_underlying_theme_cap"],
        active_exposures=active_exposures,
        candidate_symbol=candidate_symbol,
        candidate_correlation_group=candidate_correlation_group,
    )
    metrics["duplicate_underlying_theme_cap"] = rule_metrics
    if block is not None:
        blocks.append(block)

    rule_metrics, block = _options_exposure_block(
        rule=rules["options_exposure_scenario_cap"],
        active_exposures=active_exposures,
        positions=positions,
        candidate=candidate,
        quantity=resolved_quantity,
        candidate_max_loss=candidate_max_loss,
    )
    metrics["options_exposure_scenario_cap"] = rule_metrics
    if block is not None:
        blocks.append(block)

    if blocks:
        blockers = unique_text_list([block["reason"] for block in blocks], accept_scalar=True)
        evidence["blockers"] = blocks
        return _protection_payload(
            status="blocked",
            reason=blockers[0],
            message=as_text(blocks[0].get("message")) or "Protection policy blocked this entry.",
            policy=normalized_policy,
            metrics=metrics,
            evidence=evidence,
            blockers=blockers,
            evaluated_at=evaluated_at,
        )

    return _protection_payload(
        status="admissible",
        reason="protection_admissible",
        message="Protection policy allows this entry.",
        policy=normalized_policy,
        metrics=metrics,
        evidence=evidence,
        blockers=[],
        evaluated_at=evaluated_at,
    )
