"""Exchange-level repository helpers."""

from __future__ import annotations

from sqlalchemy import Select, exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from dal.models.kline import Symbol


async def exchange_exists(session: AsyncSession, exchange: str) -> bool:
    """Return True when the exchange is present in the symbols table."""
    stmt: Select[bool] = select(
        exists().where(Symbol.exchange == exchange)
    )
    result = await session.execute(stmt)
    exists_flag = result.scalar()
    return bool(exists_flag)
