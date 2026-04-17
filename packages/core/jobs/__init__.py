from __future__ import annotations


def live_collector_main() -> int:
    from core.services.collections.runtime import main

    return main()


__all__ = ["live_collector_main"]
