
from __future__ import annotations
import logging

import os
from typing import List, Type

from sqlalchemy.orm import Session
from sqlalchemy import select

from .extract import (
    list_usdt_spot_markets,
    fetch_ohlcv_incremental, 
    iso_to_ms, 
    fetch_ohlcv_range
)
from .transform import to_symbol_rows, to_kline_rows
from .load import upsert_symbols, upsert_klines, get_latest_ts, get_earliest_ts


LOGGER = logging.getLogger(__name__)

def _norm_symbol(s: str) -> str:
    """把 symbol 正規化成大寫（e.g., btc/usdt -> BTC/USDT）。"""
    return s.upper()
def _list_symbols(sess: Session, symbol_model: Type, cfg: AppConfig) -> List[str]:
    """
    1) 若 config 的 only_symbols 有設定 → 直接使用（並轉大寫）。
    2) 否則從 DB 依 exchange、quote、active 撈清單（SQLAlchemy 2.0 風格）。
    """
    if cfg.etl.only_symbols:
        return [_norm_symbol(x) for x in cfg.etl.only_symbols]

    stmt = (
        select(symbol_model.symbol)
        .where(
            symbol_model.exchange == cfg.etl.exchange_id,
            symbol_model.quote == cfg.etl.quote_filter,
            symbol_model.active.is_(True),
        )
        .order_by(symbol_model.symbol.asc())
    )
    return list(sess.execute(stmt).scalars().all())
def sync_symbols(sess: Session, symbol_model: Type, exchange_id: str) -> int:
    """
    抓 USDT/spot 市場，轉為 rows 並 upsert 到 symbols。
    """
    markets = list_usdt_spot_markets(exchange_id)
    rows = to_symbol_rows(markets, exchange_id)
    return upsert_symbols(sess, symbol_model, rows)


def backfill_one(
    sess: Session,
    kline_model: Type,
    exchange_id: str,
    symbol: str,
    timeframe: str,
    since_iso: str,
    batch_limit: int,
) -> int:
    """
    對單一 (symbol, timeframe) 以 DB 最新 ts 作為 since_ms 進行增量回補。
    """
    latest = get_latest_ts(sess, kline_model, exchange_id, symbol, timeframe)
    since_ms = iso_to_ms(since_iso) if latest is None else int(latest.timestamp() * 1000) + 1

    ohlcv = fetch_ohlcv_incremental(exchange_id, symbol, timeframe, since_ms, batch_limit)
    rows = to_kline_rows(ohlcv, exchange_id, symbol, timeframe)
    return upsert_klines(sess, kline_model, rows)




def backfill_left(
    sess: Session,
    kline_model: Type,
    exchange_id: str,
    symbol: str,
    timeframe: str,
    since_iso: str,
    batch_limit: int,
) -> int:
    """
    若 DB 最早一筆 ts > since_iso，代表左側有缺口，先把 [since_iso, earliest-1] 補齊。
    回傳寫入筆數。
    """
    earliest = get_earliest_ts(sess, kline_model, exchange_id, symbol, timeframe)
    if earliest is None:
        # DB 完全沒有資料，交給之後的「右側增量」一次吃掉（會從 since_iso 開始）
        return 0

    start_ms = iso_to_ms(since_iso)
    end_ms = int(earliest.timestamp() * 1000) - 1
    if start_ms > end_ms:
        return 0  # 沒有左側缺口

    raw = fetch_ohlcv_range(exchange_id, symbol, timeframe, start_ms, end_ms, batch_limit)
    rows = to_kline_rows(raw, exchange_id, symbol, timeframe)
    written = upsert_klines(sess, kline_model, rows)
    LOGGER.info("left-backfill | %s %s | written=%d | range=[%s, %s]",
                symbol, timeframe, written, start_ms, end_ms)
    return written


def backfill_forward(
    sess: Session,
    kline_model: Type,
    exchange_id: str,
    symbol: str,
    timeframe: str,
    since_iso: str,
    batch_limit: int,
) -> int:
    """
    原本的「往右補」：從 DB max(ts)+1 開始；若 DB 無資料，從 since_iso 開始。
    """
    latest = get_latest_ts(sess, kline_model, exchange_id, symbol, timeframe)
    since_ms = iso_to_ms(since_iso) if latest is None else int(latest.timestamp() * 1000) + 1
    raw = fetch_ohlcv_incremental(exchange_id, symbol, timeframe, since_ms, batch_limit)
    rows = to_kline_rows(raw, exchange_id, symbol, timeframe)
    written = upsert_klines(sess, kline_model, rows)
    LOGGER.info("forward-backfill | %s %s | written=%d | since_ms=%s | latest_db=%s",
                symbol, timeframe, written, since_ms, latest)
    return written


def sync_one_symbol_tf(
    sess: Session,
    kline_model: Type,
    cfg: AppConfig,
    symbol: str,
    timeframe: str,
) -> None:
    # 先補左側缺口，再做右側增量
    backfill_left(
        sess=sess,
        kline_model=kline_model,
        exchange_id=cfg.etl.exchange_id,
        symbol=symbol,
        timeframe=timeframe,
        since_iso=cfg.etl.since_iso,
        batch_limit=cfg.etl.batch_limit,
    )
    backfill_forward(
        sess=sess,
        kline_model=kline_model,
        exchange_id=cfg.etl.exchange_id,
        symbol=symbol,
        timeframe=timeframe,
        since_iso=cfg.etl.since_iso,
        batch_limit=cfg.etl.batch_limit,
    )
def sync_klines(
    sess: Session,
    symbol_model: Type,
    kline_model: Type,
    cfg: AppConfig,
) -> None:
    """
    依設定檔（cfg）逐一更新 (symbol, timeframe) 的 K 線：
    - 增量起點：DB 目前的 max(ts)，沒有資料則以 cfg.etl.since_iso 為起點。
    - 寫入：PostgreSQL ON CONFLICT DO UPDATE（冪等、可覆蓋修正）。
    """
    exchange_id = cfg.etl.exchange_id
    timeframes = cfg.etl.timeframes
    batch_limit = cfg.etl.batch_limit
    since_iso = cfg.etl.since_iso

    symbols = _list_symbols(sess, symbol_model, cfg)
    LOGGER.info(
        "sync_klines | exchange=%s | symbols=%s | timeframes=%s | batch_limit=%d | since=%s",
        exchange_id, symbols, timeframes, batch_limit, since_iso,
    )

    for sym in symbols:
        for tf in cfg.etl.timeframes:
            sync_one_symbol_tf(sess, kline_model, cfg, sym, tf)