"""Shared fixtures: real PostgreSQL, real HTTP, no mocked layers.

A test run needs a database. It uses ``TEST_DATABASE_URL`` when provided (CI
passes the service container's URL), otherwise it starts an embedded PostgreSQL
through ``pgserver`` so the suite runs anywhere, Docker or not.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any

import pgserver
import pytest
import pytest_asyncio
import uvicorn
from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from webhooks.api.app import create_app
from webhooks.config import Settings
from webhooks.db import Database
from webhooks.testing.client import ReceiverClient
from webhooks.testing.receiver import create_receiver_app

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
TRUNCATE_SQL = text(
    "TRUNCATE delivery_attempts, events, webhook_endpoints RESTART IDENTITY CASCADE"
)


@pytest.fixture(scope="session")
def database_url(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    """URL of the PostgreSQL instance used by the whole session."""
    provided = os.environ.get("TEST_DATABASE_URL")
    if provided:
        yield provided
        return

    data_dir = tmp_path_factory.mktemp("pgdata") / "data"
    server = pgserver.get_server(data_dir, cleanup_mode="stop")
    server.psql("CREATE DATABASE webhooks_test")
    query = server.get_uri().partition("?")[2]
    yield f"postgresql+asyncpg://postgres@/webhooks_test?{query}"
    server.cleanup()


@pytest.fixture(scope="session", autouse=True)
def migrated_database(database_url: str) -> Iterator[None]:
    """Apply the real Alembic migrations once per session."""
    config = Config(str(REPO_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(REPO_ROOT / "migrations"))
    config.attributes["database_url"] = database_url
    command.upgrade(config, "head")
    yield


@pytest.fixture(scope="session")
def settings(database_url: str) -> Settings:
    """Fast but production-shaped settings (small backoff, short timeouts)."""
    return Settings(
        database_url=database_url,
        delivery_timeout_seconds=1.0,
        delivery_max_attempts=3,
        delivery_backoff_base_seconds=0.05,
        delivery_backoff_max_seconds=0.2,
        delivery_backoff_jitter_ratio=0.0,
        worker_lease_seconds=3.0,
        worker_poll_interval_seconds=0.02,
        worker_batch_size=25,
        worker_concurrency=16,
        log_level="WARNING",
        log_format="console",
    )


@pytest_asyncio.fixture(scope="session")
async def database(settings: Settings) -> AsyncIterator[Database]:
    db = Database.from_settings(settings)
    yield db
    await db.dispose()


@pytest_asyncio.fixture
async def clean_database(database: Database) -> AsyncIterator[None]:
    """Empty every table before each test (identity sequences included)."""
    async with database.engine.begin() as connection:
        await connection.execute(TRUNCATE_SQL)
    yield


@pytest_asyncio.fixture
async def app(
    settings: Settings, database: Database, clean_database: None
) -> AsyncIterator[FastAPI]:
    application = create_app(settings, database, configure_logs=False)
    yield application


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://api") as http_client:
        yield http_client


class RunningReceiver:
    """A live reference receiver on a real TCP port."""

    def __init__(self, server: uvicorn.Server, task: asyncio.Task[None], client: ReceiverClient):
        self._server = server
        self._task = task
        self.client = client

    @property
    def base_url(self) -> str:
        return self.client.base_url

    async def stop(self) -> None:
        self._server.should_exit = True
        await self._task


@pytest_asyncio.fixture
async def receiver() -> AsyncIterator[RunningReceiver]:
    """Start the reference receiver in-process on a free port."""
    config = uvicorn.Config(
        create_receiver_app(), host="127.0.0.1", port=0, log_level="warning", access_log=False
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:  # noqa: ASYNC110 - simple readiness poll for the test server
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    client = ReceiverClient(f"http://127.0.0.1:{port}")
    running = RunningReceiver(server, task, client)
    try:
        yield running
    finally:
        await client.aclose()
        await running.stop()


DrainFn = Callable[..., Any]


@pytest_asyncio.fixture
async def drain() -> DrainFn:
    """Run worker batches until a condition holds (or fail loudly)."""

    async def _drain(
        workers: list[Any],
        condition: Callable[[], Any],
        *,
        timeout: float = 15.0,
        tick: float = 0.02,
    ) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            results = await asyncio.gather(*(worker.run_once() for worker in workers))
            if await condition():
                return
            if loop.time() > deadline:
                raise AssertionError(
                    f"condition not met within {timeout}s (last batch sizes: {results})"
                )
            await asyncio.sleep(tick)

    return _drain
