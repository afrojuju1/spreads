from __future__ import annotations

from functools import wraps
from typing import Callable, ParamSpec, TypeVar

from spreads.db.core import open_storage, resolve_database_url, session_scope

P = ParamSpec("P")
R = TypeVar("R")


def with_storage(
    *,
    db_arg: str = "db_target",
    storage_arg: str = "storage",
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def decorator(function: Callable[P, R]) -> Callable[P, R]:
        @wraps(function)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            if kwargs.get(storage_arg) is not None:
                return function(*args, **kwargs)
            database_url = resolve_database_url(kwargs.get(db_arg))
            with open_storage(database_url) as storage:
                kwargs[storage_arg] = storage
                return function(*args, **kwargs)

        return wrapper

    return decorator


def with_session(
    *,
    db_arg: str = "db_target",
    session_arg: str = "session",
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def decorator(function: Callable[P, R]) -> Callable[P, R]:
        @wraps(function)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            if kwargs.get(session_arg) is not None:
                return function(*args, **kwargs)
            database_url = resolve_database_url(kwargs.get(db_arg))
            with session_scope(database_url) as session:
                kwargs[session_arg] = session
                return function(*args, **kwargs)

        return wrapper

    return decorator
