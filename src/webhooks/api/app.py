"""FastAPI application factory."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.responses import JSONResponse

from webhooks import __version__
from webhooks.api.deps import DatabaseDep
from webhooks.api.errors import install_error_handlers
from webhooks.api.routes_endpoints import router as endpoints_router
from webhooks.api.routes_events import router as events_router
from webhooks.config import Settings, get_settings
from webhooks.db import Database
from webhooks.logging_setup import configure_logging

logger = logging.getLogger(__name__)


def create_app(
    settings: Settings | None = None,
    database: Database | None = None,
    *,
    configure_logs: bool = True,
) -> FastAPI:
    """Build the API application.

    ``settings``/``database`` are injectable so tests can point the app at an
    isolated database and keep the app's real dependencies.
    """
    resolved_settings = settings or get_settings()
    if configure_logs:
        configure_logging(resolved_settings.log_level, resolved_settings.log_format)

    resolved_db = database or Database.from_settings(resolved_settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        logger.info("api started", extra={"version": __version__})
        try:
            yield
        finally:
            await resolved_db.dispose()
            logger.info("api stopped")

    app = FastAPI(
        title="Reliable Webhook Delivery Platform",
        version=__version__,
        summary=(
            "Durable event submission, signed asynchronous delivery, retries and dead-lettering."
        ),
        lifespan=lifespan,
    )
    app.state.settings = resolved_settings
    app.state.db = resolved_db

    install_error_handlers(app)
    app.include_router(endpoints_router)
    app.include_router(events_router)

    @app.get("/healthz", tags=["health"], summary="Liveness probe")
    async def healthz() -> dict[str, str]:
        """Process is up; does not touch the database."""
        return {"status": "ok", "version": __version__}

    @app.get("/readyz", tags=["health"], summary="Readiness probe")
    async def readyz(database: DatabaseDep) -> JSONResponse:
        """Process is ready to serve traffic (database reachable)."""
        try:
            await database.ping()
        except Exception as exc:
            logger.warning("readiness check failed: %s", exc)
            return JSONResponse(
                status_code=503, content={"status": "unavailable", "database": "error"}
            )
        return JSONResponse(status_code=200, content={"status": "ready", "database": "ok"})

    return app
