"""Screener service for strong-symbol selection."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Callable, Sequence

import numpy as np
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession

from .finlab_price import FinlabDataFrame, get_price_dataframe_async

PriceLoader = Callable[[AsyncSession, "ScreenerCriteria", str], pd.DataFrame] | Callable[
    [AsyncSession, "ScreenerCriteria", str], Awaitable[pd.DataFrame]
]


@dataclass(slots=True)
class ScreenerCriteria:
    """Input parameters used to compute strong symbols."""

    exchange: str
    timeframe: str
    lookback: int
    volume_window: int
    top_n: int
    symbols: Sequence[str] | None = None
    start: datetime | None = None
    end: datetime | None = None


@dataclass(slots=True)
class ScreenerResult:
    """Result payload for a single strong symbol."""

    symbol: str
    rank: int
    strength_score: float
    volume_score: float
    timestamp: datetime


class ScreenerService:
    """Aggregate dependencies required to compute strong symbols."""

    def __init__(self, price_loader: PriceLoader | None = None) -> None:
        # Inject shared clients or repositories here in the future.
        self._price_loader: PriceLoader = price_loader or self._default_price_loader

    async def fetch_prices(
        self,
        session: AsyncSession,
        criteria: ScreenerCriteria,
        *,
        field: str = "close",
    ) -> pd.DataFrame:
        """Fetch price data for the given criteria using the configured loader."""
        loader = self._price_loader
        result = loader(session, criteria, field)

        if asyncio.iscoroutine(result):
            frame = await result
        else:
            loop = asyncio.get_running_loop()
            frame = await loop.run_in_executor(None, lambda: result)

        if not isinstance(frame, pd.DataFrame):
            msg = "price loader must return a pandas DataFrame"
            raise TypeError(msg)

        return frame.sort_index()

    async def compute(self, session: AsyncSession, criteria: ScreenerCriteria) -> list[ScreenerResult]:
        """Compute strong symbols based on the provided criteria."""
        # 取得對應標的的收盤價與成交量時間序列
        close_prices = await self.fetch_prices(session, criteria, field="close")
        volume = await self.fetch_prices(session, criteria, field="volume")

        if close_prices.empty or volume.empty:
            return []

        # 僅保留兩份資料都有的標的，避免資料不齊導致結果錯亂
        shared_symbols = close_prices.columns.intersection(volume.columns)
        if shared_symbols.empty:
            return []

        # 取索引交集，確保價格與成交量的時間戳一致
        shared_index = close_prices.index.intersection(volume.index)
        if shared_index.empty:
            return []

        # 對齊後再排序一次以防外部 loader 未依時間排序
        prices = close_prices.loc[shared_index, shared_symbols].sort_index()
        volumes = volume.loc[shared_index, shared_symbols].sort_index()

        # 若觀測不足，無法計算動能與量能分數
        if len(prices.index) <= criteria.lookback:
            return []

        if len(volumes.index) < criteria.volume_window:
            return []

        # 動能分數：與 lookback 期前的價格相比的百分比變化
        momentum = (prices / prices.shift(criteria.lookback)) - 1
        strength_series = momentum.iloc[-1]

        # 量能分數：最新成交量除以 rolling 均量，反映量能是否放大
        volume_ma = volumes.rolling(window=criteria.volume_window, min_periods=1).mean()
        last_volume = volumes.iloc[-1]
        last_volume_ma = volume_ma.iloc[-1].replace(0, pd.NA)
        volume_score = last_volume.divide(last_volume_ma)

        scores = pd.DataFrame({
            "strength_score": strength_series,
            "volume_score": volume_score,
        }).dropna()

        if scores.empty:
            return []

        scores = scores.replace([np.inf, -np.inf], pd.NA).dropna()
        if scores.empty:
            return []

        scores = scores.sort_values(
            by=["strength_score", "volume_score"],
            ascending=[False, False],
        )

        top_scores = scores.head(max(criteria.top_n, 0))
        if top_scores.empty:
            return []

        latest_timestamp = prices.index[-1].to_pydatetime()
        results = [
            ScreenerResult(
                symbol=symbol,
                rank=index,
                strength_score=float(row["strength_score"]),
                volume_score=float(row["volume_score"]),
                timestamp=latest_timestamp,
            )
            for index, (symbol, row) in enumerate(top_scores.iterrows(), start=1)
        ]
        return results

    @staticmethod
    async def _default_price_loader(
        session: AsyncSession,
        criteria: ScreenerCriteria,
        field: str,
    ) -> pd.DataFrame:
        """Default loader backed by get_price_dataframe_async."""
        finlab_frame: FinlabDataFrame = await get_price_dataframe_async(
            session,
            exchange=criteria.exchange,
            timeframe=criteria.timeframe,
            field=field,
            symbols=criteria.symbols,
            start=criteria.start,
            end=criteria.end,
        )
        frame = pd.DataFrame(finlab_frame.copy())
        tzinfo = getattr(frame.index, "tz", None)
        if not frame.empty and tzinfo is not None:
            frame.index = frame.index.tz_convert(None)
        return frame
