"""Pydantic schemas for screener endpoints."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Sequence

from pydantic import BaseModel, Field, model_validator


class ScreenerQuery(BaseModel):
    """Query parameters accepted by the strong-symbol screener."""

    exchange: str = Field(..., description="Exchange identifier (e.g. binance)")
    timeframe: str = Field(..., description="Timeframe identifier (e.g. 1h)")
    lookback: int = Field(60, ge=1, description="Lookback window for momentum calculation")
    volume_window: int = Field(20, ge=1, description="Lookback window for volume comparison")
    top_n: int = Field(10, ge=1, description="Number of symbols to return")
    symbols: Sequence[str] | None = Field(
        default=None,
        description="Optional allow-list of symbols",
    )
    start: datetime | None = Field(
        default=None,
        description="Optional inclusive start timestamp",
    )
    end: datetime | None = Field(
        default=None,
        description="Optional inclusive end timestamp (defaults to current time)",
    )

    @model_validator(mode="after")
    def _validate_time_order(self) -> "ScreenerQuery":
        if self.start and self.end and self.start > self.end:
            msg = "start must be earlier than end"
            raise ValueError(msg)
        return self


class ScreenerItem(BaseModel):
    """Single result item returned by the screener."""

    symbol: str = Field(..., description="Asset symbol")
    rank: int = Field(..., ge=1, description="Rank based on combined screening score")
    strength_score: float = Field(..., description="Momentum-based strength score")
    volume_score: float = Field(..., description="Volume-based score")
    timestamp: datetime = Field(..., description="Timestamp of the latest observation")


class ScreenerResponse(BaseModel):
    """Envelope for screener results."""

    items: list[ScreenerItem] = Field(default_factory=list)
