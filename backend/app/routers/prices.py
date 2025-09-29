from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from ..dependencies import get_async_session
from ..schemas import APIResponse
from ..services.finlab_price import get_price_dataframe_async


router = APIRouter(prefix="/prices", tags=["prices"])


@router.get("/", response_model=APIResponse)
async def read_prices(
    exchange: str = Query(..., description="Exchange identifier, e.g. binance"),
    timeframe: str = Query(..., description="Timeframe identifier, e.g. 1h"),
    field: str = Query("close", description="Kline field to retrieve"),
    symbols: Optional[List[str]] = Query(
        default=None,
        description="Optional list of symbols; repeat this parameter for multiple entries",
    ),
    start: Optional[datetime] = Query(
        default=None,
        description="Optional inclusive start timestamp (ISO 8601)",
    ),
    end: Optional[datetime] = Query(
        default=None,
        description="Optional inclusive end timestamp (ISO 8601)",
    ),
    session: AsyncSession = Depends(get_async_session),
) -> APIResponse:
    try:
        frame = await get_price_dataframe_async(
            session,
            exchange=exchange,
            timeframe=timeframe,
            field=field,
            symbols=symbols,
            start=start,
            end=end,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    payload = frame.to_payload()
    return APIResponse.ok(
        message="Price data retrieved",
        context=payload,
    )
