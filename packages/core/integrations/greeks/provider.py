from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def build_local_greeks_provider() -> Any | None:
    try:
        from .local_engine import LocalGreeksProvider
    except ModuleNotFoundError as exc:
        if exc.name in {"_testcapi", "py_vollib", "py_lets_be_rational"}:
            logger.warning("local_greeks_provider_unavailable", extra={"reason": str(exc)})
            return None
        raise
    return LocalGreeksProvider()


__all__ = ["build_local_greeks_provider"]
