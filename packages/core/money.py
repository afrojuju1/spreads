from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from moneyed import Money, USD, format_money

USD_CENT = Decimal("0.01")
OPTION_PREMIUM_QUANTUM = Decimal("0.0001")
OPTION_LIMIT_QUANTUM = Decimal("0.01")
OPTION_CONTRACT_MULTIPLIER = Decimal("100")


def decimal_value(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    if isinstance(value, Money):
        if value.currency != USD:
            raise ValueError(f"Expected USD money, got {value.currency.code}")
        return Decimal(value.amount)
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def quantize_decimal(value: Any, quantum: Decimal = USD_CENT) -> Decimal | None:
    parsed = decimal_value(value)
    if parsed is None:
        return None
    return parsed.quantize(quantum, rounding=ROUND_HALF_UP)


def usd_money(value: Any, quantum: Decimal = USD_CENT) -> Money | None:
    amount = quantize_decimal(value, quantum)
    if amount is None:
        return None
    return Money(amount, USD)


def money_float(value: Any, quantum: Decimal = USD_CENT) -> float | None:
    money = usd_money(value, quantum)
    if money is None:
        return None
    return float(money.amount)


def money_scaled_float(value: Any, scale: Any, quantum: Decimal = USD_CENT) -> float | None:
    amount = decimal_value(value)
    scale_value = decimal_value(scale)
    if amount is None or scale_value is None:
        return None
    return money_float(amount * scale_value, quantum)


def money_sum_float(values: Iterable[Any], quantum: Decimal = USD_CENT) -> float:
    total = Decimal("0")
    for value in values:
        parsed = decimal_value(value)
        if parsed is not None:
            total += parsed
    return money_float(total, quantum) or 0.0


def money_sum_or_none(values: Iterable[Any], quantum: Decimal = USD_CENT) -> float | None:
    total = Decimal("0")
    found = False
    for value in values:
        parsed = decimal_value(value)
        if parsed is None:
            continue
        found = True
        total += parsed
    if not found:
        return None
    return money_float(total, quantum)


def premium_float(value: Any) -> float | None:
    return money_float(value, OPTION_PREMIUM_QUANTUM)


def option_limit_price(value: Any, *, minimum: Decimal = OPTION_LIMIT_QUANTUM) -> float | None:
    parsed = decimal_value(value)
    if parsed is None:
        return None
    bounded = max(parsed, minimum)
    return money_float(bounded, OPTION_LIMIT_QUANTUM)


def option_contract_notional(
    premium: Any,
    quantity: Any,
    *,
    multiplier: Decimal = OPTION_CONTRACT_MULTIPLIER,
    quantum: Decimal = USD_CENT,
) -> float | None:
    premium_value = decimal_value(premium)
    quantity_value = decimal_value(quantity)
    if premium_value is None or quantity_value is None:
        return None
    return money_float(premium_value * multiplier * quantity_value, quantum)


def option_premium_from_notional(
    notional: Any,
    quantity: Any,
    *,
    multiplier: Decimal = OPTION_CONTRACT_MULTIPLIER,
) -> float | None:
    notional_value = decimal_value(notional)
    quantity_value = decimal_value(quantity)
    if notional_value is None or quantity_value is None or quantity_value <= 0 or multiplier <= 0:
        return None
    return premium_float(notional_value / (multiplier * quantity_value))


def equity_notional(price: Any, quantity: Any) -> float | None:
    price_value = decimal_value(price)
    quantity_value = decimal_value(quantity)
    if price_value is None or quantity_value is None:
        return None
    return money_float(price_value * quantity_value)


def option_spread_exposure(
    *,
    entry_value: Any,
    width: Any,
    quantity: Any,
    premium_kind: str | None,
) -> dict[str, float | None]:
    entry_notional = option_contract_notional(entry_value, quantity)
    entry_value_dec = decimal_value(entry_value)
    width_dec = decimal_value(width)
    quantity_dec = decimal_value(quantity)
    if entry_value_dec is None or width_dec is None or quantity_dec is None:
        return {"entry_notional": entry_notional, "max_profit": entry_notional, "max_loss": None}

    risk_value = max(width_dec - entry_value_dec, Decimal("0")) * OPTION_CONTRACT_MULTIPLIER * quantity_dec
    risk_amount = money_float(risk_value)
    if str(premium_kind or "").strip().lower() == "debit":
        return {"entry_notional": entry_notional, "max_profit": risk_amount, "max_loss": entry_notional}
    return {"entry_notional": entry_notional, "max_profit": entry_notional, "max_loss": risk_amount}


def option_long_exposure(*, entry_value: Any, quantity: Any) -> dict[str, float | None]:
    entry_notional = option_contract_notional(entry_value, quantity)
    return {"entry_notional": entry_notional, "max_profit": None, "max_loss": entry_notional}


def close_pnl(
    *,
    entry_value: Any,
    exit_value: Any,
    quantity: Any,
    premium_kind: str | None,
    equity: bool = False,
) -> float:
    entry = decimal_value(entry_value)
    exit_ = decimal_value(exit_value)
    qty = decimal_value(quantity)
    if entry is None or exit_ is None or qty is None or qty <= 0:
        return 0.0
    if equity:
        return money_float((exit_ - entry) * qty) or 0.0
    multiplier = OPTION_CONTRACT_MULTIPLIER * qty
    if str(premium_kind or "").strip().lower() == "debit":
        return money_float((exit_ - entry) * multiplier) or 0.0
    return money_float((entry - exit_) * multiplier) or 0.0


def repriced_limit_price(
    *,
    current_limit: Any,
    original_limit: Any,
    step: Any,
    max_concession: Any,
    premium_kind: str | None,
    natural_value: Any = None,
) -> float | None:
    current = decimal_value(current_limit)
    original = decimal_value(original_limit)
    if current is None:
        return None
    if original is None:
        original = current
    step_value = max(decimal_value(step) or OPTION_LIMIT_QUANTUM, OPTION_LIMIT_QUANTUM)
    concession = max(decimal_value(max_concession) or Decimal("0.02"), Decimal("0"))
    kind = str(premium_kind or "").strip().lower()
    natural = decimal_value(natural_value)

    if kind == "debit":
        ceiling = original + concession
        target = min(current + step_value, ceiling)
        if natural is not None:
            target = min(target, max(natural, current))
        target_float = option_limit_price(target)
        if target_float is None or Decimal(str(target_float)) <= current:
            return None
        return target_float

    floor = max(original - concession, OPTION_LIMIT_QUANTUM)
    target = max(current - step_value, floor)
    if natural is not None:
        target = min(target, current - step_value)
    target_float = option_limit_price(target)
    if target_float is None or Decimal(str(target_float)) >= current:
        return None
    return target_float


def format_usd(value: Any, *, locale: str = "en_US_POSIX") -> str | None:
    money = usd_money(value)
    if money is None:
        return None
    return format_money(money, locale=locale)
