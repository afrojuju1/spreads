from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Query

from apps.api.errors import bad_request_error, service_unavailable_error
from spreads.services.account_state import get_account_overview

router = APIRouter()


@router.get("/account/overview")
def get_account_overview_route(
    history_range: Literal["1D", "1W", "1M"] = Query(default="1D"),
) -> dict[str, object]:
    try:
        return get_account_overview(history_range=history_range)
    except ValueError as exc:
        raise bad_request_error(exc) from exc
    except Exception as exc:
        raise service_unavailable_error(exc) from exc
