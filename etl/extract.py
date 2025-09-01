from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import List, Optional, Sequence

import ccxt


def list_usdt_spot_markets(exchange_id: str) -> List[dict]:
    """
    從指定交易所載入 USDT 現貨市場清單。
    """
    exchange_class = getattr(ccxt, exchange_id)
    ex = exchange_class({"enableRateLimit": True})
    markets = ex.load_markets()
    return [
        m for m in markets.values()
        if m.get("quote") == "USDT" and m.get("spot") is True
    ]


def fetch_ohlcv_incremental(
    exchange_id: str,
    symbol: str,
    timeframe: str,
    since_ms: int,
    limit: int,
) -> List[List[Optional[float]]]:
    """
    以增量方式抓取 OHLCV；會遵守交易所速率限制。
    回傳 raw 列表：[ms, open, high, low, close, volume, ...]
    """
    exchange_class = getattr(ccxt, exchange_id)
    ex = exchange_class({"enableRateLimit": True})

    all_rows: List[List[Optional[float]]] = []
    cursor = since_ms

    while True:
        batch = ex.fetch_ohlcv(symbol, timeframe=timeframe, since=cursor, limit=limit)
        if not batch:
            break
        all_rows.extend(batch)
        cursor = batch[-1][0] + 1
        time.sleep(ex.rateLimit / 1000.0)

    return all_rows


def iso_to_ms(iso_str: str) -> int:
    """
    將 ISO8601 轉毫秒 timestamp。
    """
    return int(datetime.fromisoformat(iso_str.replace("Z", "+00:00")).timestamp() * 1000)


def ms_to_utc(ms: int) -> datetime:
    """
    將毫秒 timestamp 轉 UTC datetime。
    """
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
