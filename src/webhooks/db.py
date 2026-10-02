"""Async SQLAlchemy engine/session wiring.

A ``Database`` instance owns one engine; the API keeps it in ``app.state`` and
the worker keeps it local, so tests can build isolated instances against their
own PostgreSQL database.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from webhooks.config import Settings

_SELECT_ONE = text("SELECT 1")


def build_engine(settings: Settings) -> AsyncEngine:
    return create_async_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,
        # Checked-out connections are recycled before PostgreSQL's idle timeout
        # can silently drop them under a long-lived worker.
        pool_recycle=1800,
    )


class Database:
    """Owns the engine and hands out short-lived sessions."""

    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine
        self.session_factory = async_sessionmaker(engine, expire_on_commit=False)

    @classmethod
    def from_settings(cls, settings: Settings) -> Database:
        return cls(build_engine(settings))

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Session with commit-on-success / rollback-on-error semantics."""
        session = self.session_factory()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    async def ping(self) -> None:
        """Raise if the database is not reachable (readiness probe)."""
        async with self.engine.connect() as connection:
            await connection.execute(_SELECT_ONE)

    async def dispose(self) -> None:
        await self.engine.dispose()
