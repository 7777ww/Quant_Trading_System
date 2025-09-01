#!/usr/bin/env python3
"""
SQLAlchemy 2.0 版本：ccxt + APScheduler ETL

功能：
1) 抓所有 USDT（現貨）交易對並 upsert 至 market.symbols
2) 依 DB 最新 ts 以增量方式抓取 OHLCV，upsert 至 market.klines

相依套件：ccxt, SQLAlchemy>=2.0, psycopg2-binary, apscheduler

環境變數：
- DATABASE_URL（必填）：postgresql+psycopg2://user:pass@host:5432/db
- EXCHANGE_ID：預設 binance
- TIMEFRAMES：如 '1h,4h,1d'
- SINCE_ISO：如 '2021-01-01T00:00:00Z'（初次無資料時的起始）
- BATCH_LIMIT：預設 1000
- SCHEDULE_CRON_SYMBOLS：預設 '0 3 * * *'
- SCHEDULE_CRON_KLINES：預設 '*/20 * * * *'
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple

import ccxt
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import (
    BigInteger,
    Boolean,
    TIMESTAMP,
    select,
    func,
    case,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
from sqlalchemy import create_engine


# -------------------------- 設定 --------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOGGER = logging.getLogger(__name__)


def env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"缺少環境變數：{name}")
    return value


# -------------------------- ORM --------------------------
class Base(DeclarativeBase):
    pass


class Symbol(Base):
    __tablename__ = "symbols"
    __table_args__ = {"schema": "market"}

    exchange: Mapped[str] = mapped_column(primary_key=True)
    symbol: Mapped[str] = mapped_column(primary_key=True)
    base: Mapped[Optional[str]]
    quote: Mapped[Optional[str]]
    active: Mapped[Optional[bool]] = mapped_column(Boolean, default=True)
    info: Mapped[Optional[dict]] = mapped_column(JSONB)
    first_seen: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("NOW()")
    )
    last_seen: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("NOW()")
    )


class Kline(Base):
    __tablename__ = "klines"
    __table_args__ = {"schema": "market"}

    exchange: Mapped[str] = mapped_column(primary_key=True)
    symbol: Mapped[str] = mapped_column(primary_key=True)
    timeframe: Mapped[str] = mapped_column(primary_key=True)
    ts: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    open: Mapped[Optional[float]]
    high: Mapped[Optional[float]]
    low: Mapped[Optional[float]]
    close: Mapped[Optional[float]]
    volume: Mapped[Optional[float]]
    trades: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    vwap: Mapped[Optional[float]]


# -------------------------- DB 連線 --------------------------
ENGINE = create_engine(env("DATABASE_URL"), pool_pre_ping=True)


def iso_to_ms(iso_str: str) -> int:
    return int(datetime.fromisoformat(iso_str.replace("Z", "+00:00")).timestamp() * 1000)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# -------------------------- 交易所資料 --------------------------
def load_usdt_symbols(exchange_id: str) -> List[dict]:
    exchange_class = getattr(ccxt, exchange_id)
    exchange = exchange_class({"enableRateLimit": True})
    markets = exchange.load_markets()
    items: List[dict] = []
    for m in markets.values():
        if m.get("quote") == "USDT" and m.get("spot") is True:
            items.append(m)
    LOGGER.info("載入 %s USDT 市場數：%d", exchange_id, len(items))
    return items


# -------------------------- DB 操作（SQLAlchemy） --------------------------
def upsert_symbols(session: Session, exchange_id: str, markets: Iterable[dict]) -> None:
    ts = now_utc()
    rows = []
    for m in markets:
        rows.append(
            dict(
                exchange=exchange_id,
                symbol=m.get("symbol"),
                base=m.get("base"),
                quote=m.get("quote"),
                active=bool(m.get("active", True)),
                info=m,
                first_seen=ts,
                last_seen=ts,
            )
        )

    if not rows:
        LOGGER.info("沒有可 upsert 的 symbols")
        return

    stmt = insert(Symbol).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Symbol.exchange, Symbol.symbol],
        set_={
            "base": stmt.excluded.base,
            "quote": stmt.excluded.quote,
            "active": stmt.excluded.active,
            "info": stmt.excluded.info,
            "last_seen": stmt.excluded.last_seen,
        },
    )
    session.execute(stmt)
    session.commit()
    LOGGER.info("symbols upsert 完成：%d 筆", len(rows))


def get_latest_ts(
    session: Session, exchange_id: str, symbol: str, timeframe: str
) -> Optional[datetime]:
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
            # 僅當新值非 NULL 才覆蓋
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


# -------------------------- 抓取 OHLCV --------------------------
def fetch_ohlcv_incremental(
    exchange_id: str, symbol: str, timeframe: str, since_ms: int, limit: int
) -> List[List[Optional[float]]]:
    exchange_class = getattr(ccxt, exchange_id)
    exchange = exchange_class({"enableRateLimit": True})
    all_rows: List[List[Optional[float]]] = []
    cursor = since_ms
    while True:
        batch = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=cursor, limit=limit)
        if not batch:
            break
        all_rows.extend(batch)
        cursor = batch[-1][0] + 1
        time.sleep(exchange.rateLimit / 1000.0)
    return all_rows


# -------------------------- Jobs --------------------------
def job_update_symbols() -> None:
    exchange_id = env("EXCHANGE_ID", "binance")
    with Session(ENGINE) as sess:
        markets = load_usdt_symbols(exchange_id)
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
    scheduler.add_job(job_update_symbols, CronTrigger.from_crontab(env("SCHEDULE_CRON_SYMBOLS", "0 3 * * *")))
    scheduler.add_job(job_update_klines, CronTrigger.from_crontab(env("SCHEDULE_CRON_KLINES", "*/20 * * * *")))

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