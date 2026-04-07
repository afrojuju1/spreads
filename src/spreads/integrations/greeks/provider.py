from __future__ import annotations

from .local_engine import LocalGreeksProvider


def build_local_greeks_provider() -> LocalGreeksProvider:
    return LocalGreeksProvider()


__all__ = ["LocalGreeksProvider", "build_local_greeks_provider"]
