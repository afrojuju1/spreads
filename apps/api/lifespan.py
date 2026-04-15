from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI


@asynccontextmanager
async def api_lifespan(app: FastAPI):
    del app
    yield
