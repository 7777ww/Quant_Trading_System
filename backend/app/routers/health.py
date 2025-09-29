from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ..dependencies import get_async_session
from ..schemas import APIResponse


class HealthPayload(BaseModel):
    status: str
    version: str
    db: Optional[str] = None


router = APIRouter(prefix="/health", tags=["health"])


@router.get("/", response_model=APIResponse)
async def read_health() -> APIResponse:
    payload = HealthPayload(status="ok", version="1.0.0")
    return APIResponse.ok(
        message="Service is healthy",
        context=payload.model_dump(),
    )


@router.get("/db", response_model=APIResponse)
async def read_health_db(
    session: AsyncSession = Depends(get_async_session),
) -> APIResponse:
    try:
        result = await session.execute(text("SELECT version();"))
        db_version = result.scalar_one()
    except Exception as exc:  # pragma: no cover - 健康檢查仍需拋錯細節
        return APIResponse.fail(
            status_code=503,
            message="Database health check failed",
            context={"error": str(exc)},
        )

    payload = HealthPayload(status="ok", version="1.0.0", db=db_version)
    return APIResponse.ok(
        message="Service and database are healthy",
        context=payload.model_dump(),
    )
