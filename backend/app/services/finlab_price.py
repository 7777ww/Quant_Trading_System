"""FinLab-style dataframe helpers for price retrieval."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import pandas as pd
from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from dal.models.kline import Kline



def _dataclass_slotted(cls: type | None = None, **kwargs: Any):
    """Decorate a dataclass using slots when the runtime supports it."""

    def wrap(target_cls: type) -> type:
        try:
            return dataclass(target_cls, slots=True, **kwargs)
        except TypeError:
            # Python < 3.10 does not support the slots parameter.
            return dataclass(target_cls, **kwargs)

    if cls is None:
        return wrap
    return wrap(cls)


@_dataclass_slotted
class FinlabFrameMeta:
    exchange: str
    timeframe: str
    field: str
    symbols: list[str]
    start: datetime | None = None
    end: datetime | None = None


class FinlabDataFrame(pd.DataFrame):
    """Minimal FinLab dataframe that preserves metadata."""

    _metadata = ["meta"]

    def __init__(
        self,
        *args: Any,
        meta: FinlabFrameMeta | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.meta: FinlabFrameMeta | None = meta

    @property
    def _constructor(self) -> type["FinlabDataFrame"]:
        return FinlabDataFrame

    def __finalize__(self, other: Any, method: str | None = None, **_: Any) -> "FinlabDataFrame":
        if isinstance(other, FinlabDataFrame):
            self.meta = other.meta
        return self

    def average(self, n: int) -> "FinlabDataFrame":
        """Return the n-period moving average with FinLab's NaN rule."""
        if not isinstance(n, int) or n <= 0:
            msg = "n must be a positive integer"
            raise ValueError(msg)

        # 依照 FinLab 慣例設定最少有效樣本數，避免過度跳動
        min_periods = n // 2 + 1
        averaged = self.rolling(window=n, min_periods=min_periods).mean()
        return FinlabDataFrame(averaged, meta=self.meta)

    def filter(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        symbols: Sequence[str] | None = None,
    ) -> "FinlabDataFrame":
        """Return a filtered copy limited by time window and symbols (依時間區間與標的清單篩選後回傳副本)。"""
        frame = self
        if start is not None:
            frame = frame.loc[frame.index >= start]
        if end is not None:
            frame = frame.loc[frame.index <= end]
        if symbols:
            keep = [symbol for symbol in symbols if symbol in frame.columns]
            frame = frame.loc[:, keep]

        filtered = FinlabDataFrame(frame.copy()) if not isinstance(frame, FinlabDataFrame) else frame.copy()

        if self.meta and not filtered.empty:
            # 當有資料時同步更新中繼資訊的時間區間與標的清單
            start_ts = filtered.index.min().to_pydatetime()
            end_ts = filtered.index.max().to_pydatetime()
            filtered.meta = FinlabFrameMeta(
                exchange=self.meta.exchange,
                timeframe=self.meta.timeframe,
                field=self.meta.field,
                symbols=[str(symbol) for symbol in filtered.columns],
                start=start_ts,
                end=end_ts,
            )
        elif self.meta:
            filtered.meta = FinlabFrameMeta(
                exchange=self.meta.exchange,
                timeframe=self.meta.timeframe,
                field=self.meta.field,
                symbols=[],
                start=None,
                end=None,
            )
        return filtered

    def to_payload(self) -> dict[str, Any]:
        """Serialise dataframe + metadata for JSON responses (將資料與中繼資訊序列化成 JSON 結構)。"""
        json_ready = self.reset_index().copy()
        if "ts" in json_ready.columns:
            json_ready["ts"] = json_ready["ts"].map(
                lambda value: value.isoformat() if isinstance(value, datetime) else value
            )
        meta_dict: dict[str, Any] | None = None
        if self.meta:
            meta_dict = asdict(self.meta)
            if meta_dict.get("start"):
                meta_dict["start"] = meta_dict["start"].isoformat()  # type: ignore[assignment]
            if meta_dict.get("end"):
                meta_dict["end"] = meta_dict["end"].isoformat()  # type: ignore[assignment]
        return {
            "meta": meta_dict,
            "data": json_ready.to_dict(orient="records"),
        }


def _resolve_field(field: str) -> Any:
    """Return the ORM column for the requested field (依欄位名稱取得 Kline 對應欄位)。"""
    if not hasattr(Kline, field):
        msg = f"Unsupported Kline field: {field}"
        raise ValueError(msg)
    return getattr(Kline, field)


def _build_price_stmt(
    *,
    exchange: str,
    timeframe: str,
    field: str,
    symbols: Sequence[str] | None,
    start: datetime | None,
    end: datetime | None,
) -> Select[Any]:
    """Build the SQLAlchemy Select for kline values (組出查詢 K 線數值的 Select)。"""
    value_column = _resolve_field(field)
    # 基礎條件鎖定交易所、時間框與欄位
    stmt = select(
        Kline.symbol.label("symbol"),
        Kline.ts.label("ts"),
        value_column.label("value"),
    ).where(
        Kline.exchange == exchange,
        Kline.timeframe == timeframe,
    )
    if symbols:
        stmt = stmt.where(Kline.symbol.in_(symbols))
    if start:
        stmt = stmt.where(Kline.ts >= start)
    if end:
        stmt = stmt.where(Kline.ts <= end)
    return stmt.order_by(Kline.ts.asc())


def _rows_to_finlab_frame(
    rows: Iterable[tuple[str, datetime, float | None]],
    *,
    meta: FinlabFrameMeta,
) -> FinlabDataFrame:
    """Pivot rows into a FinlabDataFrame while keeping metadata (將查詢結果轉成 FinlabDataFrame 並保留 metadata)。"""
    data_frame = pd.DataFrame(rows, columns=["symbol", "ts", "value"])
    if data_frame.empty:
        # 無資料時回傳結構化的空框，保留原本的 meta 設定
        empty = FinlabDataFrame(pd.DataFrame(), meta=FinlabFrameMeta(
            exchange=meta.exchange,
            timeframe=meta.timeframe,
            field=meta.field,
            symbols=[],
            start=None,
            end=None,
        ))
        return empty

    data_frame["ts"] = pd.to_datetime(data_frame["ts"], utc=True)
    data_frame = data_frame.sort_values("ts")
    pivot = (
        data_frame.pivot(index="ts", columns="symbol", values="value")
        .sort_index()
    )

    pivot.index = pd.DatetimeIndex(pivot.index)
    resolved_symbols = [str(symbol) for symbol in pivot.columns]
    start_ts = pivot.index.min().to_pydatetime() if not pivot.empty else None
    end_ts = pivot.index.max().to_pydatetime() if not pivot.empty else None
    frame_meta = FinlabFrameMeta(
        exchange=meta.exchange,
        timeframe=meta.timeframe,
        field=meta.field,
        symbols=resolved_symbols,
        start=start_ts,
        end=end_ts,
    )
    # 回傳帶著整理後 metadata 的 FinlabDataFrame
    return FinlabDataFrame(pivot, meta=frame_meta)


def get_price_dataframe(
    session: Session,
    *,
    exchange: str,
    timeframe: str,
    field: str = "close",
    symbols: Sequence[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> FinlabDataFrame:
    """Fetch one OHLCV field via sync Session (以同步 Session 拉取單一欄位)。"""
    stmt = _build_price_stmt(
        exchange=exchange,
        timeframe=timeframe,
        field=field,
        symbols=symbols,
        start=start,
        end=end,
    )
    result = session.execute(stmt)
    rows = [(row.symbol, row.ts, row.value) for row in result]
    meta = FinlabFrameMeta(
        exchange=exchange,
        timeframe=timeframe,
        field=field,
        symbols=list(symbols or []),
        start=start,
        end=end,
    )
    return _rows_to_finlab_frame(rows, meta=meta)


async def get_price_dataframe_async(
    session: AsyncSession,
    *,
    exchange: str,
    timeframe: str,
    field: str = "close",
    symbols: Sequence[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> FinlabDataFrame:
    """Fetch one OHLCV field via async Session (以非同步 Session 拉取單一欄位)。"""
    stmt = _build_price_stmt(
        exchange=exchange,
        timeframe=timeframe,
        field=field,
        symbols=symbols,
        start=start,
        end=end,
    )
    result = await session.execute(stmt)
    rows = [(row.symbol, row.ts, row.value) for row in result]
    meta = FinlabFrameMeta(
        exchange=exchange,
        timeframe=timeframe,
        field=field,
        symbols=list(symbols or []),
        start=start,
        end=end,
    )
    return _rows_to_finlab_frame(rows, meta=meta)


def get_price_dataframes(
    session: Session,
    *,
    exchange: str,
    timeframe: str,
    fields: Sequence[str],
    symbols: Sequence[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> dict[str, FinlabDataFrame]:
    """Batch-fetch multiple OHLCV fields synchronously (同步批次取得多個欄位)。"""
    return {
        field: get_price_dataframe(
            session,
            exchange=exchange,
            timeframe=timeframe,
            field=field,
            symbols=symbols,
            start=start,
            end=end,
        )
        for field in fields
    }


async def get_price_dataframes_async(
    session: AsyncSession,
    *,
    exchange: str,
    timeframe: str,
    fields: Sequence[str],
    symbols: Sequence[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> dict[str, FinlabDataFrame]:
    """Batch-fetch multiple OHLCV fields asynchronously (非同步批次取得多個欄位)。"""
    result: dict[str, FinlabDataFrame] = {}
    for field in fields:
        result[field] = await get_price_dataframe_async(
            session,
            exchange=exchange,
            timeframe=timeframe,
            field=field,
            symbols=symbols,
            start=start,
            end=end,
        )
    return result



