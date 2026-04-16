from __future__ import annotations


def live_collector_main() -> int:
    from core.jobs.live_collector import main

    return main()


__all__ = ["live_collector_main"]
