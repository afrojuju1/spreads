from __future__ import annotations

import sys
from pathlib import Path

from fastapi import FastAPI

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from apps.api.errors import ApiError, api_error_handler
from apps.api.lifespan import api_lifespan
from apps.api.routes.account import router as account_router
from apps.api.routes.control import router as control_router
from apps.api.routes.events_ws import router as events_ws_router
from apps.api.routes.executions import router as executions_router
from apps.api.routes.health import router as health_router
from apps.api.routes.internal_market_data import router as internal_market_data_router
from apps.api.routes.opportunities import router as opportunities_router
from apps.api.routes.pipelines import router as pipelines_router
from apps.api.routes.positions import router as positions_router
from apps.api.routes.uoa import router as uoa_router


def create_app() -> FastAPI:
    app = FastAPI(title="Spreads API", version="0.2.0", lifespan=api_lifespan)
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(health_router)
    app.include_router(account_router)
    app.include_router(control_router)
    app.include_router(pipelines_router)
    app.include_router(opportunities_router)
    app.include_router(positions_router)
    app.include_router(executions_router)
    app.include_router(internal_market_data_router)
    app.include_router(uoa_router)
    app.include_router(events_ws_router)
    return app


app = create_app()
