from __future__ import annotations

# ruff: noqa: E402

import sys
from pathlib import Path

from fastapi import FastAPI

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = ROOT / "packages"
if str(PACKAGES) not in sys.path:
    sys.path.insert(0, str(PACKAGES))

from api.errors import ApiError, api_error_handler
from api.lifespan import api_lifespan
from api.routes.account import router as account_router
from api.routes.automations import router as automations_router
from api.routes.control import router as control_router
from api.routes.events_ws import router as events_ws_router
from api.routes.executions import router as executions_router
from api.routes.health import router as health_router
from api.routes.internal_ops import router as internal_ops_router
from api.routes.opportunities import router as opportunities_router
from api.routes.pipelines import router as pipelines_router
from api.routes.positions import router as positions_router
from api.routes.uoa import router as uoa_router


def create_app() -> FastAPI:
    app = FastAPI(title="Spreads API", version="0.2.0", lifespan=api_lifespan)
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(health_router)
    app.include_router(account_router)
    app.include_router(automations_router)
    app.include_router(control_router)
    app.include_router(pipelines_router)
    app.include_router(opportunities_router)
    app.include_router(positions_router)
    app.include_router(executions_router)
    app.include_router(internal_ops_router)
    app.include_router(uoa_router)
    app.include_router(events_ws_router)
    return app


app = create_app()
