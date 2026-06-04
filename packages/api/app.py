from __future__ import annotations

# ruff: noqa: E402

import logging
import sys
from pathlib import Path
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI, Request

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = ROOT / "packages"
if str(PACKAGES) not in sys.path:
    sys.path.insert(0, str(PACKAGES))

from api.errors import ApiError, api_error_handler
from api.lifespan import api_lifespan
from api.routes.account import router as account_router
from api.routes.company_valuation import router as company_valuation_router
from api.routes.control import router as control_router
from api.routes.events_ws import router as events_ws_router
from api.routes.executions import router as executions_router
from api.routes.health import router as health_router
from api.routes.internal_ops import router as internal_ops_router
from api.routes.opportunities import router as opportunities_router
from api.routes.pipelines import router as pipelines_router
from api.routes.positions import router as positions_router
from api.routes.uoa import router as uoa_router
from core.observability.logging import configure_logging, log_event

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    configure_logging(service="api", force=True)
    logging.getLogger("uvicorn.access").disabled = True
    app = FastAPI(title="Spreads API", version="0.2.0", lifespan=api_lifespan)
    app.add_exception_handler(ApiError, api_error_handler)

    @app.middleware("http")
    async def request_logging_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
        request_id = request.headers.get("x-request-id") or str(uuid4())
        started_at = perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            log_event(
                logger,
                logging.ERROR,
                "api_request_failed",
                request_id=request_id,
                method=request.method,
                path=request.url.path,
                query=request.url.query or None,
                client_host=None if request.client is None else request.client.host,
                duration_ms=round((perf_counter() - started_at) * 1000, 1),
                exc_info=True,
            )
            raise
        response.headers["x-request-id"] = request_id
        log_event(
            logger,
            logging.INFO,
            "api_request_completed",
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=round((perf_counter() - started_at) * 1000, 1),
            client_host=None if request.client is None else request.client.host,
        )
        return response

    app.include_router(health_router)
    app.include_router(account_router)
    app.include_router(control_router)
    app.include_router(pipelines_router)
    app.include_router(opportunities_router)
    app.include_router(positions_router)
    app.include_router(executions_router)
    app.include_router(internal_ops_router)
    app.include_router(company_valuation_router)
    app.include_router(uoa_router)
    app.include_router(events_ws_router)
    return app


app = create_app()
