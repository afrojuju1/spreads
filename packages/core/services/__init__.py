from __future__ import annotations


def scan_main() -> int:
    from core.services.scanners.service import main

    return main()


__all__ = ["scan_main"]
