from __future__ import annotations

import re
from typing import Any

from core.model_contracts import DomainModel

_OCC_SYMBOL_RE = re.compile(r"^([A-Z]{1,6})(\d{6})([CP])(\d{8})$")


class OptionSymbolParts(DomainModel):
    option_symbol: str
    underlying_symbol: str
    expiration_date: str
    option_type: str
    strike_price: float


def parse_occ_option_symbol(symbol: Any) -> OptionSymbolParts | None:
    normalized = str(symbol or "").strip().replace(" ", "").upper()
    match = _OCC_SYMBOL_RE.match(normalized)
    if match is None:
        return None
    root, yymmdd, type_code, raw_strike = match.groups()
    year = int(yymmdd[:2])
    month = int(yymmdd[2:4])
    day = int(yymmdd[4:6])
    return OptionSymbolParts(
        option_symbol=normalized,
        underlying_symbol=root,
        expiration_date=f"{2000 + year:04d}-{month:02d}-{day:02d}",
        option_type="call" if type_code == "C" else "put",
        strike_price=round(int(raw_strike) / 1000.0, 4),
    )


__all__ = ["OptionSymbolParts", "parse_occ_option_symbol"]
