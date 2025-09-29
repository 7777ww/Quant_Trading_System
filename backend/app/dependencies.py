from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession

from dal.db import db


async def get_async_session() -> AsyncIterator[AsyncSession]:
    async with db.async_session_scope() as session:
        yield session
