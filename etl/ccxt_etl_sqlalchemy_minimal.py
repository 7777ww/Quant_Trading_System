#!/usr/bin/env python3
"""
ETL（Minimal symbols 版）：SQLAlchemy 2.0 + ccxt + APScheduler

- 不存 info JSONB，僅存必要欄位（含可選 market_id）
- 依據 DB 最新 ts 做增量抓取 K 線
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import List, Optional

import ccxt
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import case, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from dal.db import get_engine
from dal.models import Kline, Symbol


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOGGER = logging.getLogger(__name__)

ENGINE = get_engine()


def env(name: str, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if val is None:
        raise RuntimeError(f"缺少環境變數：{name}")
    return val


def iso_to_ms(iso_str: str) -> int:
    return int(datetime.fromisoformat(iso_str.replace("Z", "+00:00")).timestamp() * 1000)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def list_usdt_spot_markets(exchange_id: str) -> List[dict]:
    exchange_class = getattr(ccxt, exchange_id)
    ex = exchange_class({"enableRateLimit": True})
    markets = ex.load_markets()
    items: List[dict] = []
    for m in markets.values():
        if m.get("quote") == "USDT" and m.get("spot") is True:
            items.append(m)
    LOGGER.info("載入 %s USDT 市場數：%d", exchange_id, len(items))
    return items


def upsert_symbols(session: Session, exchange_id: str, markets: List[dict]) -> None:
    ts = now_utc()
    rows = [
        dict(
            exchange=exchange_id,
            symbol=m.get("symbol"),
            base=m.get("base"),
            quote=m.get("quote"),
            market_id=m.get("id"),  # 交易所原生代號
            active=bool(m.get("active", True)),
            first_seen=ts,
            last_seen=ts,
        )
        for m in markets
    ]

    if not rows:
        LOGGER.info("沒有可 upsert 的 symbols")
        return

    stmt = insert(Symbol).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Symbol.exchange, Symbol.symbol],
        set_={
            "base": stmt.excluded.base,
            "quote": stmt.excluded.quote,
            "market_id": stmt.excluded.market_id,
            "active": stmt.excluded.active,
            "last_seen": stmt.excluded.last_seen,
        },
    )
    session.execute(stmt)
    session.commit()
    LOGGER.info("symbols upsert 完成：%d 筆", len(rows))


def get_latest_ts(session: Session, exchange_id: str, symbol: str, timeframe: str):
    q = (
        select(func.max(Kline.ts))
        .where(
            Kline.exchange == exchange_id,
            Kline.symbol == symbol,
            Kline.timeframe == timeframe,
        )
        .limit(1)
    )
    return session.execute(q).scalar_one_or_none()


def fetch_ohlcv_incremental(
    exchange_id: str, symbol: str, timeframe: str, since_ms: int, limit: int
) -> List[List[Optional[float]]]:
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


def upsert_klines(
    session: Session,
    exchange_id: str,
    symbol: str,
    timeframe: str,
    ohlcv_rows: List[List[Optional[float]]],
) -> int:
    if not ohlcv_rows:
        return 0

    rows: List[dict] = []
    for r in ohlcv_rows:
        ts_ms, open_p, high_p, low_p, close_p, volume = r[:6]
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        rows.append(
            dict(
                exchange=exchange_id,
                symbol=symbol,
                timeframe=timeframe,
                ts=dt,
                open=open_p,
                high=high_p,
                low=low_p,
                close=close_p,
                volume=volume,
                trades=None,
                vwap=None,
            )
        )

    stmt = insert(Kline).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Kline.exchange, Kline.symbol, Kline.timeframe, Kline.ts],
        set_={
            "open": stmt.excluded.open,
            "high": stmt.excluded.high,
            "low": stmt.excluded.low,
            "close": stmt.excluded.close,
            "volume": stmt.excluded.volume,
            "trades": case(
                (stmt.excluded.trades.isnot(None), stmt.excluded.trades), else_=Kline.trades
            ),
            "vwap": case(
                (stmt.excluded.vwap.isnot(None), stmt.excluded.vwap), else_=Kline.vwap
            ),
        },
    )
    session.execute(stmt)
    session.commit()
    return len(rows)


def job_update_symbols() -> None:
    exchange_id = env("EXCHANGE_ID", "binance")
    with Session(ENGINE) as sess:
        markets = list_usdt_spot_markets(exchange_id)
        upsert_symbols(sess, exchange_id, markets)


def job_update_klines() -> None:
    exchange_id = env("EXCHANGE_ID", "binance")
    timeframes = [x.strip() for x in env("TIMEFRAMES", "1h,1d").split(",") if x.strip()]
    batch_limit = int(env("BATCH_LIMIT", "1000"))
    since_iso = env("SINCE_ISO", "2021-01-01T00:00:00Z")

    with Session(ENGINE) as sess:
        symbols = (
            sess.execute(
                select(Symbol.symbol).where(
                    Symbol.exchange == exchange_id,
                    Symbol.quote == "USDT",
                    Symbol.active.is_(True),
                )
            )
            .scalars()
            .all()
        )

        for sym in symbols:
            for tf in timeframes:
                latest = get_latest_ts(sess, exchange_id, sym, tf)
                if latest is None:
                    since_ms = iso_to_ms(since_iso)
                else:
                    since_ms = int(latest.timestamp() * 1000) + 1

                LOGGER.info("抓取 %s %s %s 從 %s", exchange_id, sym, tf, since_ms)
                rows = fetch_ohlcv_incremental(
                    exchange_id=exchange_id,
                    symbol=sym,
                    timeframe=tf,
                    since_ms=since_ms,
                    limit=batch_limit,
                )
                n = upsert_klines(sess, exchange_id, sym, tf, rows)
                LOGGER.info("寫入 %s %s %s 筆數：%d", exchange_id, sym, tf, n)


def main() -> None:
    # 先跑一次，再啟動排程
    job_update_symbols()
    job_update_klines()

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        job_update_symbols,
        CronTrigger.from_crontab(env("SCHEDULE_CRON_SYMBOLS", "0 3 * * *")),
    )
    scheduler.add_job(
        job_update_klines,
        CronTrigger.from_crontab(env("SCHEDULE_CRON_KLINES", "*/20 * * * *")),
    )

    LOGGER.info("排程啟動")
    scheduler.start()

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        LOGGER.info("停止排程")
        scheduler.shutdown()


if __name__ == "__main__":
    main()