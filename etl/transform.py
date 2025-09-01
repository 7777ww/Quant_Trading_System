from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, List, Optional, Sequence


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_symbol_rows(markets: Iterable[dict], exchange_id: str) -> List[dict]:
    """
    將 ccxt markets 轉為可 upsert 的 symbols 列表（最小欄位）。
    """
    ts = now_utc()
    return [
        {
            "exchange": exchange_id,
            "symbol": m.get("symbol"),
            "base": m.get("base"),
            "quote": m.get("quote"),
            "market_id": m.get("id"),
            "active": bool(m.get("active", True)),
            "first_seen": ts,
            "last_seen": ts,
        }
        for m in markets
    ]


def to_kline_rows(
    ohlcv: Sequence[Sequence[Optional[float]]],
    exchange_id: str,
    symbol: str,
    timeframe: str,
) -> List[dict]:
    """
    將 raw OHLCV 轉為可 upsert 的 kline 列表。
    """
    rows: List[dict] = []
    for r in ohlcv:
        ts_ms, o, h, l, c, v = r[:6]
        rows.append(
            {
                "exchange": exchange_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "ts": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v,
                "trades": None,
                "vwap": None,
            }
        )
    return rows
