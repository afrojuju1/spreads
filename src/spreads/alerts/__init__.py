from __future__ import annotations


def dispatch_cycle_alerts(*args, **kwargs):
    from spreads.alerts.dispatcher import dispatch_cycle_alerts as _dispatch_cycle_alerts

    return _dispatch_cycle_alerts(*args, **kwargs)


__all__ = ["dispatch_cycle_alerts"]
