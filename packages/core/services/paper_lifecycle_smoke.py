from __future__ import annotations

from dataclasses import asdict
from datetime import timedelta
from typing import Any
from uuid import uuid4

from core.db.decorators import with_storage
from core.money import money_float, option_contract_notional, option_limit_price, option_premium_from_notional
from core.services.alpaca import create_alpaca_client_from_env, resolve_trading_environment
from core.services.control_plane import (
    OPEN_ACTIVITY_MANUAL,
    assess_open_activity_gate,
)
from core.services.deployment_policy import DEPLOYMENT_MODE_PAPER_AUTO
from core.services.execution.attempts import _get_attempt_payload
from core.services.execution.runtimes import ALPACA_DIRECT_RUNTIME
from core.services.execution.shared import OPEN_STATUSES
from core.services.execution_intents import request_execution_lifecycle_start
from core.services.execution_intents.shared import (
    ACTIVE_INTENT_STATES,
    issue_pending_execution_intent,
)
from core.services.market_dates import NEW_YORK
from core.services.option_symbols import parse_occ_option_symbol
from core.services.ops.market_session import market_session_context
from core.services.session_positions import CLOSE_TRADE_INTENT, OPEN_TRADE_INTENT
from core.value_coercion import (
    as_text,
    coerce_float,
    safe_component,
    utc_expiry_iso,
    utc_iso,
    utc_now,
    utc_now_iso,
)

SYNTHETIC_VALIDATION_PROVENANCE = "synthetic_validation"
SYNTHETIC_TRADING_STRATEGY_ID = "synthetic_paper_lifecycle_smoke"
SYNTHETIC_PROFILE = "paper_smoke"
DEFAULT_ALLOWED_UNDERLYINGS = ("SPY", "QQQ")
DEFAULT_TTL_MINUTES = 5
DEFAULT_MAX_DEBIT_DOLLARS = 25.0
DEFAULT_AUTO_SELECT_MIN_DTE = 7
DEFAULT_AUTO_SELECT_MAX_DTE = 21


def _normalize_symbol(value: Any, *, field_name: str) -> str:
    normalized = as_text(value)
    if normalized is None:
        raise ValueError(f"{field_name} is required")
    return normalized.upper()


def _normalize_optional_symbol(value: Any) -> str | None:
    normalized = as_text(value)
    return None if normalized is None else normalized.upper()


def _normalize_allowlist(values: list[str] | tuple[str, ...] | None, *, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    raw_values = values if values else list(default)
    normalized: list[str] = []
    for item in raw_values:
        for chunk in str(item or "").split(","):
            rendered = chunk.strip().upper()
            if rendered and rendered not in normalized:
                normalized.append(rendered)
    return tuple(normalized)


def _option_type(value: Any) -> str:
    rendered = as_text(value)
    if rendered is None:
        raise ValueError("option_type is required")
    normalized = rendered.lower()
    if normalized in {"c", "call"}:
        return "call"
    if normalized in {"p", "put"}:
        return "put"
    raise ValueError("option_type must be call or put")


def _strategy_family(option_type: str) -> str:
    return "long_call" if option_type == "call" else "long_put"


def _parse_occ_option_symbol(symbol: str) -> dict[str, Any]:
    parsed = parse_occ_option_symbol(symbol)
    if parsed is None:
        return {}
    return {
        "underlying_symbol": parsed.underlying_symbol,
        "expiration_date": parsed.expiration_date,
        "option_type": parsed.option_type,
        "strike": parsed.strike_price,
    }


def _money(value: Any, *, field_name: str) -> float:
    parsed = coerce_float(value)
    if parsed is None or parsed <= 0:
        raise ValueError(f"{field_name} must be positive")
    return money_float(parsed) or 0.0


def _has_blocker(blockers: list[dict[str, str]], code: str) -> bool:
    return any(row.get("code") == code for row in blockers)


def _market_date() -> str:
    return utc_now().astimezone(NEW_YORK).date().isoformat()


def _run_id() -> str:
    return f"synthetic_validation:{uuid4().hex}"


def _intent_id(run_id: str) -> str:
    return f"execution_intent:{run_id}"


def _base_safety_snapshot(
    *,
    storage: Any,
    calendar_name: str,
) -> tuple[dict[str, Any], list[dict[str, str]], Any | None]:
    now = utc_now()
    blockers: list[dict[str, str]] = []
    market_session = market_session_context(now=now, calendar_name=calendar_name)
    if not bool(market_session.get("is_open")):
        blockers.append(
            {
                "code": "market_closed",
                "message": "Synthetic paper lifecycle smoke is market-hours only.",
            }
        )

    control_gate = assess_open_activity_gate(
        activity_kind=OPEN_ACTIVITY_MANUAL,
        storage=storage,
    )
    if not bool(control_gate.get("allowed")):
        blockers.append(
            {
                "code": str(control_gate.get("reason") or "control_plane_blocked"),
                "message": str(control_gate.get("message") or "Control plane blocked open execution."),
            }
        )

    client = None
    broker_environment = "unknown"
    broker_environment_source = "alpaca_client"
    try:
        client = create_alpaca_client_from_env()
        broker_environment = resolve_trading_environment(client.trading_base_url)
    except Exception as exc:
        blockers.append(
            {
                "code": "broker_environment_unresolved",
                "message": f"Could not resolve Alpaca broker environment: {exc}",
            }
        )
    if broker_environment != "paper":
        blockers.append(
            {
                "code": "paper_broker_required",
                "message": f"Synthetic paper lifecycle smoke requires Alpaca paper; observed {broker_environment}.",
            }
        )

    return (
        {
            "observed_at": utc_iso(now),
            "market_session": market_session,
            "control_gate": control_gate,
            "broker_environment": f"alpaca_{broker_environment}" if broker_environment in {"paper", "live"} else broker_environment,
            "broker_environment_source": broker_environment_source,
            "execution_posture": "paper",
            "execution_runtime": ALPACA_DIRECT_RUNTIME,
            "validation_provenance": SYNTHETIC_VALIDATION_PROVENANCE,
        },
        blockers,
        client,
    )


def _select_contract(
    *,
    client: Any,
    underlying_symbol: str,
    option_type: str,
    quantity: int,
    max_debit_dollars: float,
    min_dte: int,
    max_dte: int,
) -> dict[str, Any] | None:
    today = utc_now().astimezone(NEW_YORK).date()
    min_expiration = (today + timedelta(days=max(int(min_dte), 0))).isoformat()
    max_expiration = (today + timedelta(days=max(int(max_dte), int(min_dte), 0))).isoformat()
    contracts = client.list_option_contracts(
        underlying_symbol,
        min_expiration,
        max_expiration,
        option_type=option_type,
    )
    contracts_by_symbol = {contract.symbol: contract for contract in contracts}
    expirations = sorted({contract.expiration_date for contract in contracts})
    notional_cap_per_contract = option_premium_from_notional(max_debit_dollars, max(int(quantity), 1)) or 0.0
    candidates: list[dict[str, Any]] = []
    for expiration in expirations:
        snapshots = client.get_option_chain_snapshots(
            underlying_symbol,
            expiration,
            option_type,
            feed="opra",
        )
        for symbol, snapshot in snapshots.items():
            contract = contracts_by_symbol.get(symbol)
            if contract is None or snapshot.ask <= 0 or snapshot.ask > notional_cap_per_contract:
                continue
            if snapshot.ask_size <= 0 or snapshot.bid_size <= 0:
                continue
            notional = option_contract_notional(snapshot.ask, max(int(quantity), 1)) or 0.0
            candidates.append(
                {
                    "symbol": symbol,
                    "underlying_symbol": underlying_symbol,
                    "expiration_date": contract.expiration_date,
                    "option_type": option_type,
                    "strike": contract.strike_price,
                    "limit_price": option_limit_price(snapshot.ask) or 0.01,
                    "notional": notional,
                    "open_interest": contract.open_interest,
                    "quote_metrics": asdict(snapshot),
                }
            )
    if not candidates:
        return None
    candidates.sort(
        key=lambda row: (
            str(row["expiration_date"]),
            -float(row["limit_price"]),
            -int(row["open_interest"]),
            str(row["symbol"]),
        )
    )
    return candidates[0]


def _open_request_payload(
    *,
    run_id: str,
    contract_symbol: str,
    underlying_symbol: str,
    expiration_date: str,
    option_type: str,
    strike: float,
    quantity: int,
    limit_price: float,
    max_debit_dollars: float,
    allow_underlyings: tuple[str, ...],
    allow_contracts: tuple[str, ...],
    option_selection: dict[str, Any],
) -> dict[str, Any]:
    notional = option_contract_notional(limit_price, quantity) or 0.0
    return {
        "asset_class": "option",
        "symbol": contract_symbol,
        "side": "buy",
        "quantity": quantity,
        "limit_price": limit_price,
        "time_in_force": "day",
        "label": SYNTHETIC_TRADING_STRATEGY_ID,
        "market_date": _market_date(),
        "underlying_symbol": underlying_symbol,
        "root_symbol": underlying_symbol,
        "strategy_family": _strategy_family(option_type),
        "expiration_date": expiration_date,
        "option_type": option_type,
        "strike": strike,
        "trade_intent": OPEN_TRADE_INTENT,
        "execution_runtime": ALPACA_DIRECT_RUNTIME,
        "approval_mode": "auto",
        "execution_mode": "paper",
        "validation_provenance": SYNTHETIC_VALIDATION_PROVENANCE,
        "queue_submission": True,
        "profile": SYNTHETIC_PROFILE,
        "source": {
            "kind": SYNTHETIC_VALIDATION_PROVENANCE,
            "id": run_id,
            "operator_command": "spreads lifecycle paper-smoke open",
        },
        "option_selection": {
            **dict(option_selection),
            "validation_provenance": SYNTHETIC_VALIDATION_PROVENANCE,
            "max_debit_dollars": max_debit_dollars,
            "requested_notional": notional,
            "allow_underlyings": list(allow_underlyings),
            "allow_contracts": list(allow_contracts),
        },
        "risk_policy": {
            "enabled": True,
            "allow_live": False,
            "max_contracts_per_position": quantity,
            "max_position_notional": max_debit_dollars,
            "max_position_max_loss": max_debit_dollars,
        },
        "execution_policy": {
            "enabled": True,
            "deployment_mode": DEPLOYMENT_MODE_PAPER_AUTO,
            "mode": "top_promotable",
            "quantity": quantity,
            "pricing_mode": "midpoint",
        },
        "exit_policy": {
            "enabled": True,
            "profit_target_pct": 0.05,
            "stop_multiple": 2.0,
            "force_close_at": None,
        },
    }


def _preview_or_blocked(
    *,
    execute: bool,
    payload: dict[str, Any],
    blockers: list[dict[str, str]],
) -> dict[str, Any] | None:
    if execute and blockers:
        return {
            **payload,
            "status": "blocked",
            "created": False,
            "blockers": blockers,
        }
    if not execute:
        return {
            **payload,
            "status": "preview",
            "created": False,
            "blockers": blockers,
        }
    return None


@with_storage()
def create_synthetic_paper_open_smoke(
    *,
    db_target: str | None = None,
    execute: bool = False,
    auto_select: bool = False,
    underlying_symbol: str | None = None,
    contract_symbol: str | None = None,
    expiration_date: str | None = None,
    option_type: str = "call",
    strike: float | None = None,
    quantity: int = 1,
    limit_price: float | None = None,
    max_debit_dollars: float = DEFAULT_MAX_DEBIT_DOLLARS,
    ttl_minutes: int = DEFAULT_TTL_MINUTES,
    allow_underlyings: list[str] | tuple[str, ...] | None = None,
    allow_contracts: list[str] | tuple[str, ...] | None = None,
    calendar_name: str = "NYSE",
    auto_select_min_dte: int = DEFAULT_AUTO_SELECT_MIN_DTE,
    auto_select_max_dte: int = DEFAULT_AUTO_SELECT_MAX_DTE,
    request_lifecycle_start: bool = True,
    storage: Any | None = None,
) -> dict[str, Any]:
    normalized_quantity = max(int(quantity), 1)
    normalized_max_debit = _money(max_debit_dollars, field_name="max_debit_dollars")
    allowed_underlyings = _normalize_allowlist(
        list(allow_underlyings or ()),
        default=DEFAULT_ALLOWED_UNDERLYINGS,
    )
    allowed_contracts = _normalize_allowlist(list(allow_contracts or ()))
    safety, blockers, client = _base_safety_snapshot(
        storage=storage,
        calendar_name=calendar_name,
    )

    selected: dict[str, Any] | None = None
    normalized_option_type = _option_type(option_type)
    normalized_underlying = _normalize_optional_symbol(underlying_symbol)
    normalized_contract = _normalize_optional_symbol(contract_symbol)
    if auto_select:
        if normalized_underlying is None:
            normalized_underlying = DEFAULT_ALLOWED_UNDERLYINGS[0]
        if client is None:
            blockers.append(
                {
                    "code": "auto_select_broker_unavailable",
                    "message": "Auto-select requires an Alpaca client.",
                }
            )
        elif normalized_underlying not in allowed_underlyings:
            blockers.append(
                {
                    "code": "underlying_not_allowlisted",
                    "message": f"{normalized_underlying} is not in the underlying allowlist.",
                }
            )
        else:
            selected = _select_contract(
                client=client,
                underlying_symbol=normalized_underlying,
                option_type=normalized_option_type,
                quantity=normalized_quantity,
                max_debit_dollars=normalized_max_debit,
                min_dte=auto_select_min_dte,
                max_dte=auto_select_max_dte,
            )
            if selected is None:
                blockers.append(
                    {
                        "code": "auto_select_no_contract",
                        "message": "No quoted option contract fit the synthetic smoke debit cap.",
                    }
                )
            else:
                normalized_contract = str(selected["symbol"])
                expiration_date = str(selected["expiration_date"])
                strike = float(selected["strike"])
                limit_price = float(selected["limit_price"])
                normalized_option_type = str(selected["option_type"])
                if normalized_contract not in allowed_contracts:
                    blockers.append(
                        {
                            "code": "contract_not_allowlisted",
                            "message": (
                                f"{normalized_contract} was selected but is not in the exact contract allowlist; "
                                "pass --allow-contract with that symbol to execute."
                            ),
                        }
                    )

    if normalized_contract is None:
        raise ValueError("contract_symbol is required unless --auto-select is used")
    parsed_symbol = _parse_occ_option_symbol(normalized_contract)
    parsed_underlying = as_text(parsed_symbol.get("underlying_symbol"))
    if parsed_underlying is not None and normalized_underlying is not None and parsed_underlying != normalized_underlying:
        blockers.append(
            {
                "code": "contract_underlying_mismatch",
                "message": f"{normalized_contract} belongs to {parsed_underlying}, not {normalized_underlying}.",
            }
        )
    if normalized_underlying is None:
        normalized_underlying = parsed_underlying
    if expiration_date is None:
        expiration_date = as_text(parsed_symbol.get("expiration_date"))
    if strike is None:
        strike = coerce_float(parsed_symbol.get("strike"))
    if parsed_symbol.get("option_type"):
        normalized_option_type = str(parsed_symbol["option_type"])
    if normalized_underlying is None:
        raise ValueError("underlying_symbol is required when it cannot be derived from the contract symbol")
    if expiration_date is None:
        raise ValueError("expiration_date is required when it cannot be derived from the contract symbol")
    if strike is None:
        raise ValueError("strike is required when it cannot be derived from the contract symbol")
    if limit_price is None:
        raise ValueError("limit_price is required unless --auto-select is used")
    normalized_limit_price = _money(limit_price, field_name="limit_price")
    requested_notional = option_contract_notional(normalized_limit_price, normalized_quantity) or 0.0
    if requested_notional > normalized_max_debit:
        blockers.append(
            {
                "code": "max_debit_exceeded",
                "message": f"Requested debit ${requested_notional:.2f} exceeds cap ${normalized_max_debit:.2f}.",
            }
        )
    if normalized_underlying not in allowed_underlyings:
        blockers.append(
            {
                "code": "underlying_not_allowlisted",
                "message": f"{normalized_underlying} is not in the underlying allowlist.",
            }
        )
    if normalized_contract not in allowed_contracts and not _has_blocker(blockers, "contract_not_allowlisted"):
        blockers.append(
            {
                "code": "contract_not_allowlisted",
                "message": f"{normalized_contract} is not in the exact contract allowlist.",
            }
        )

    run_id = _run_id()
    option_selection = {
        "selection_mode": "auto_select" if auto_select else "operator_supplied",
        **({} if selected is None else {"selected_contract": selected}),
    }
    request_payload = _open_request_payload(
        run_id=run_id,
        contract_symbol=normalized_contract,
        underlying_symbol=normalized_underlying,
        expiration_date=str(expiration_date),
        option_type=normalized_option_type,
        strike=float(strike),
        quantity=normalized_quantity,
        limit_price=normalized_limit_price,
        max_debit_dollars=normalized_max_debit,
        allow_underlyings=allowed_underlyings,
        allow_contracts=allowed_contracts,
        option_selection=option_selection,
    )
    payload = {
        "action": "synthetic_paper_open_smoke",
        "run_id": run_id,
        "execution_intent_id": _intent_id(run_id),
        "request": request_payload,
        "safety": {
            **safety,
            "allowed_underlyings": list(allowed_underlyings),
            "allowed_contracts": list(allowed_contracts),
            "requested_notional": requested_notional,
            "max_debit_dollars": normalized_max_debit,
            "ttl_minutes": max(int(ttl_minutes), 1),
        },
    }
    preview = _preview_or_blocked(execute=execute, payload=payload, blockers=blockers)
    if preview is not None:
        return preview

    intent = issue_pending_execution_intent(
        storage.execution,
        execution_intent_id=str(payload["execution_intent_id"]),
        trading_strategy_id=SYNTHETIC_TRADING_STRATEGY_ID,
        strategy_position_id=None,
        action_type=OPEN_TRADE_INTENT,
        slot_key=f"synthetic_validation:open:{safe_component(normalized_contract)}:{uuid4().hex[:8]}",
        policy_ref={
            "family": "synthetic_validation",
            "key": run_id,
            "source_kind": "operator_command",
            "version": "1",
        },
        config_hash="synthetic_validation",
        expires_at=utc_expiry_iso(minutes=ttl_minutes, minimum_seconds=60),
        payload=request_payload,
        created_event_payload={
            "validation_provenance": SYNTHETIC_VALIDATION_PROVENANCE,
            "run_id": run_id,
        },
    )
    lifecycle_start = (
        request_execution_lifecycle_start(
            job_store=storage.jobs,
            limit=5,
            requested_by={
                "source": "paper_lifecycle_smoke",
                "run_id": run_id,
            },
        )
        if request_lifecycle_start
        else None
    )
    return {
        **payload,
        "status": "created",
        "created": True,
        "blockers": [],
        "execution_intent": dict(intent),
        "lifecycle_start": lifecycle_start,
    }


def _position_open_validation_provenance(execution_store: Any, position: dict[str, Any]) -> str | None:
    open_attempt_id = as_text(position.get("open_execution_attempt_id"))
    if open_attempt_id is None:
        return None
    attempt = execution_store.get_attempt(open_attempt_id)
    request = attempt.get("request") if isinstance(attempt, dict) else None
    return as_text(request.get("validation_provenance")) if isinstance(request, dict) else None


@with_storage()
def create_synthetic_paper_close_smoke(
    *,
    db_target: str | None = None,
    execute: bool = False,
    position_id: str,
    limit_price: float | None = None,
    ttl_minutes: int = DEFAULT_TTL_MINUTES,
    calendar_name: str = "NYSE",
    request_lifecycle_start: bool = True,
    storage: Any | None = None,
) -> dict[str, Any]:
    safety, blockers, _client = _base_safety_snapshot(
        storage=storage,
        calendar_name=calendar_name,
    )
    execution_store = storage.execution
    position = execution_store.get_position(position_id)
    if position is None:
        blockers.append(
            {
                "code": "position_missing",
                "message": f"Unknown position_id: {position_id}.",
            }
        )
        position_payload: dict[str, Any] = {}
    else:
        position_payload = dict(position)
        status = str(position_payload.get("status") or position_payload.get("position_status") or "")
        if status == "closed":
            blockers.append(
                {
                    "code": "position_closed",
                    "message": f"Position {position_id} is already closed.",
                }
            )
        provenance = _position_open_validation_provenance(execution_store, position_payload)
        if provenance != SYNTHETIC_VALIDATION_PROVENANCE:
            blockers.append(
                {
                    "code": "position_not_synthetic_validation",
                    "message": "Synthetic close smoke only closes positions opened by synthetic_validation.",
                }
            )
        active_close_attempts = execution_store.list_open_attempts_for_position(
            position_id=position_id,
            statuses=sorted(OPEN_STATUSES),
        )
        active_close_intents = execution_store.list_execution_intents(
            strategy_position_id=position_id,
            states=sorted(ACTIVE_INTENT_STATES),
            limit=10,
        )
        if active_close_attempts or active_close_intents:
            blockers.append(
                {
                    "code": "active_close_exists",
                    "message": f"Position {position_id} already has active close lifecycle work.",
                }
            )

    run_id = _run_id()
    close_payload: dict[str, Any] = {
        "validation_provenance": SYNTHETIC_VALIDATION_PROVENANCE,
        "approval_mode": "auto",
        "execution_mode": "paper",
        "execution_runtime": ALPACA_DIRECT_RUNTIME,
        "trade_intent": CLOSE_TRADE_INTENT,
        "profile": SYNTHETIC_PROFILE,
        "source": {
            "kind": SYNTHETIC_VALIDATION_PROVENANCE,
            "id": run_id,
            "operator_command": "spreads lifecycle paper-smoke close",
        },
        "close_decision": {
            "reason": "synthetic_validation_close",
            "decided_at": utc_now_iso(),
        },
    }
    if limit_price not in (None, ""):
        close_payload["limit_price"] = _money(limit_price, field_name="limit_price")
    payload = {
        "action": "synthetic_paper_close_smoke",
        "run_id": run_id,
        "execution_intent_id": _intent_id(run_id),
        "position_id": position_id,
        "position": position_payload,
        "request": close_payload,
        "safety": {
            **safety,
            "ttl_minutes": max(int(ttl_minutes), 1),
        },
    }
    preview = _preview_or_blocked(execute=execute, payload=payload, blockers=blockers)
    if preview is not None:
        return preview

    intent = issue_pending_execution_intent(
        execution_store,
        execution_intent_id=str(payload["execution_intent_id"]),
        trading_strategy_id=SYNTHETIC_TRADING_STRATEGY_ID,
        strategy_position_id=position_id,
        action_type=CLOSE_TRADE_INTENT,
        slot_key=f"synthetic_validation:close:{safe_component(position_id)}:{uuid4().hex[:8]}",
        policy_ref={
            "family": "synthetic_validation",
            "key": run_id,
            "source_kind": "operator_command",
            "version": "1",
        },
        config_hash="synthetic_validation",
        expires_at=utc_expiry_iso(minutes=ttl_minutes, minimum_seconds=60),
        payload=close_payload,
        created_event_payload={
            "validation_provenance": SYNTHETIC_VALIDATION_PROVENANCE,
            "run_id": run_id,
        },
    )
    lifecycle_start = (
        request_execution_lifecycle_start(
            job_store=storage.jobs,
            limit=5,
            requested_by={
                "source": "paper_lifecycle_smoke",
                "run_id": run_id,
            },
        )
        if request_lifecycle_start
        else None
    )
    return {
        **payload,
        "status": "created",
        "created": True,
        "blockers": [],
        "execution_intent": dict(intent),
        "lifecycle_start": lifecycle_start,
    }


@with_storage()
def inspect_synthetic_paper_smoke(
    *,
    db_target: str | None = None,
    execution_intent_id: str,
    storage: Any | None = None,
) -> dict[str, Any]:
    resolved_intent_id = execution_intent_id
    if not resolved_intent_id.startswith("execution_intent:"):
        resolved_intent_id = f"execution_intent:{resolved_intent_id}"
    execution_store = storage.execution
    intent = execution_store.get_execution_intent(resolved_intent_id)
    if intent is None:
        raise ValueError(f"Unknown execution_intent_id: {execution_intent_id}")
    intent_payload = intent.get("payload") if isinstance(intent.get("payload"), dict) else intent.get("payload_json")
    if not isinstance(intent_payload, dict):
        intent_payload = {}
    attempt_id = as_text(intent.get("execution_attempt_id")) or as_text(intent_payload.get("execution_attempt_id"))
    attempt = None
    position = None
    closes: list[dict[str, Any]] = []
    if attempt_id is not None:
        attempt = _get_attempt_payload(execution_store, attempt_id)
        if str(attempt.get("trade_intent") or OPEN_TRADE_INTENT) == OPEN_TRADE_INTENT:
            position = execution_store.get_position_by_open_attempt(attempt_id)
        else:
            position_id = as_text(attempt.get("position_id"))
            position = None if position_id is None else execution_store.get_position(position_id)
        if position is not None:
            closes = [dict(row) for row in execution_store.list_position_closes(position_id=str(position["position_id"]))]
    status = "pending_intent"
    if attempt is not None:
        status = str(attempt.get("status") or "unknown")
    return {
        "status": status,
        "execution_intent_id": resolved_intent_id,
        "validation_provenance": as_text(intent_payload.get("validation_provenance")),
        "execution_intent": dict(intent),
        "attempt": attempt,
        "position": None if position is None else dict(position),
        "closes": closes,
        "lifecycle_checks": {
            "intent_created": True,
            "attempt_created": attempt is not None,
            "order_recorded": bool((attempt or {}).get("orders")),
            "fill_recorded": bool((attempt or {}).get("fills")),
            "position_recorded": position is not None,
            "close_recorded": bool(closes),
        },
    }


__all__ = [
    "DEFAULT_ALLOWED_UNDERLYINGS",
    "DEFAULT_AUTO_SELECT_MAX_DTE",
    "DEFAULT_AUTO_SELECT_MIN_DTE",
    "DEFAULT_MAX_DEBIT_DOLLARS",
    "DEFAULT_TTL_MINUTES",
    "SYNTHETIC_PROFILE",
    "SYNTHETIC_TRADING_STRATEGY_ID",
    "SYNTHETIC_VALIDATION_PROVENANCE",
    "create_synthetic_paper_close_smoke",
    "create_synthetic_paper_open_smoke",
    "inspect_synthetic_paper_smoke",
]
