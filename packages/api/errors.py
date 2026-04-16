from __future__ import annotations

from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse

from core.services.control_plane import CONTROL_SCHEMA_MESSAGE
from core.services.execution import EXECUTION_SCHEMA_MESSAGE


class ApiError(Exception):
    def __init__(self, *, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = int(status_code)
        self.detail = str(detail)


async def api_error_handler(_: Request, exc: ApiError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


def _detail(value: Any) -> str:
    return str(value)


def bad_request_error(value: Any) -> ApiError:
    return ApiError(status_code=400, detail=_detail(value))


def not_found_error(value: Any) -> ApiError:
    return ApiError(status_code=404, detail=_detail(value))


def upstream_error(value: Any) -> ApiError:
    return ApiError(status_code=502, detail=_detail(value))


def service_unavailable_error(value: Any) -> ApiError:
    return ApiError(status_code=503, detail=_detail(value))


def control_runtime_error(value: RuntimeError) -> ApiError:
    detail = _detail(value)
    if detail == CONTROL_SCHEMA_MESSAGE:
        return ApiError(status_code=409, detail=detail)
    return ApiError(status_code=502, detail=detail)


def execution_runtime_error(value: RuntimeError) -> ApiError:
    detail = _detail(value)
    if detail == EXECUTION_SCHEMA_MESSAGE:
        return ApiError(status_code=409, detail=detail)
    return ApiError(status_code=502, detail=detail)
