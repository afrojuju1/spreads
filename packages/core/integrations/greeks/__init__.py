from .provider import build_local_greeks_provider


def __getattr__(name: str):
    if name == "LocalGreeksProvider":
        from .local_engine import LocalGreeksProvider

        return LocalGreeksProvider
    raise AttributeError(name)


__all__ = ["LocalGreeksProvider", "build_local_greeks_provider"]
