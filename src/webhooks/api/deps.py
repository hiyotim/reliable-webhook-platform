"""FastAPI dependencies.

``get_session`` wraps the request in one database transaction: the dependency
commits when the handler returns successfully and rolls back when it raises, so
route handlers never manage transactions themselves.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from webhooks.config import Settings
from webhooks.db import Database


def get_database(request: Request) -> Database:
    database: Database = request.app.state.db
    return database


def get_settings(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    database: Database = request.app.state.db
    async with database.session() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]
SettingsDep = Annotated[Settings, Depends(get_settings)]
DatabaseDep = Annotated[Database, Depends(get_database)]
