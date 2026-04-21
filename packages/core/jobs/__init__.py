from __future__ import annotations


def discovery_run_main() -> int:
    from core.services.discovery_runs.runtime import main

    return main()


__all__ = ["discovery_run_main"]
