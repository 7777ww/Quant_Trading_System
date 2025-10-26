"""Screener router providing strong-symbol endpoints."""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from ..dependencies import get_async_session
from ..repositories import exchange_exists
from ..schemas import APIResponse
from ..schemas.screener import ScreenerItem, ScreenerQuery, ScreenerResponse
from ..services.screener import ScreenerCriteria, ScreenerResult, ScreenerService

router = APIRouter(prefix="/screener", tags=["screener"])


def get_screener_service(request: Request) -> ScreenerService:
    """Dependency factory returning a ScreenerService instance."""
    state = request.app.state
    cached = getattr(state, "screener_service", None)
    if isinstance(cached, ScreenerService):
        return cached

    dependencies = getattr(state, "screener_dependencies", None)
    if isinstance(dependencies, dict) and dependencies:
        try:
            service = ScreenerService(**dependencies)
        except TypeError:
            service = ScreenerService()
    else:
        service = ScreenerService()

    setattr(state, "screener_service", service)
    return service


@router.get("/strong", response_model=APIResponse)
async def get_strong_symbols(
    query: ScreenerQuery = Depends(),
    service: ScreenerService = Depends(get_screener_service),
    session: AsyncSession = Depends(get_async_session),
) -> APIResponse:
    """
    Retrieve currently strong symbols based on momentum and volume.
    """
    exchange = query.exchange
    has_exchange = await exchange_exists(session, exchange)
    if not has_exchange:

        return APIResponse.fail(
            status_code=status.HTTP_404_NOT_FOUND,
            message="Exchange not found",
            context={"exchange": exchange},
        )
    criteria = ScreenerCriteria(
        exchange=exchange,
        timeframe=query.timeframe,
        lookback=query.lookback,
        volume_window=query.volume_window,
        top_n=query.top_n,
        symbols=query.symbols,
        start=query.start,
        end=query.end or datetime.now(timezone.utc),
    )

    results: list[ScreenerResult] = await service.compute(session, criteria)

    items = [
        ScreenerItem(
            symbol=result.symbol,
            rank=result.rank,
            strength_score=result.strength_score,
            volume_score=result.volume_score,
            timestamp=result.timestamp,
        )
        for result in results
    ]
    payload = ScreenerResponse(items=items).model_dump()
    return APIResponse.ok(message="Strong symbols retrieved", context=payload)
