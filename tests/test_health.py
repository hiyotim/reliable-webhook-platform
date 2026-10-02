"""Liveness and readiness probes."""

from __future__ import annotations

from httpx import ASGITransport, AsyncClient

from webhooks.api.app import create_app
from webhooks.config import Settings
from webhooks.db import Database


async def test_healthz_is_ok(client: AsyncClient) -> None:
    response = await client.get("/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


async def test_readyz_reports_database_up(client: AsyncClient) -> None:
    response = await client.get("/readyz")
    assert response.status_code == 200
    assert response.json() == {"status": "ready", "database": "ok"}


async def test_readyz_reports_database_down(settings: Settings) -> None:
    """A reachable process with an unreachable database must be unready."""
    broken = settings.model_copy(
        update={"database_url": "postgresql+asyncpg://postgres@127.0.0.1:1/absent"}
    )
    database = Database.from_settings(broken)
    app = create_app(broken, database, configure_logs=False)
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://api") as client:
            response = await client.get("/readyz")
        assert response.status_code == 503
        assert response.json()["database"] == "error"
    finally:
        await database.dispose()
