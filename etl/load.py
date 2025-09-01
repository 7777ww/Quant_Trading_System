from __future__ import annotations

from typing import Sequence, Type

from sqlalchemy import case, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session


def upsert_symbols(session: Session, model: Type, rows: Sequence[dict]) -> int:
    """
    將 symbols 以 PostgreSQL ON CONFLICT upsert。
    需要 model 上有 (exchange, symbol) 唯一鍵或 unique 索引。
    """
    if not rows:
        return 0

    stmt = insert(model).values(list(rows))
    stmt = stmt.on_conflict_do_update(
        index_elements=[model.exchange, model.symbol],
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
    return len(rows)


def upsert_klines(session: Session, model: Type, rows: Sequence[dict]) -> int:
    """
    將 klines 以 PostgreSQL ON CONFLICT upsert。
    需要 model 上有 (exchange, symbol, timeframe, ts) 複合唯一鍵。
    """
    if not rows:
        return 0

    stmt = insert(model).values(list(rows))
    stmt = stmt.on_conflict_do_update(
        index_elements=[model.exchange, model.symbol, model.timeframe, model.ts],
        set_={
            "open": stmt.excluded.open,
            "high": stmt.excluded.high,
            "low": stmt.excluded.low,
            "close": stmt.excluded.close,
            "volume": stmt.excluded.volume,
            "trades": case(
                (stmt.excluded.trades.isnot(None), stmt.excluded.trades),
                else_=model.trades,
            ),
            "vwap": case(
                (stmt.excluded.vwap.isnot(None), stmt.excluded.vwap),
                else_=model.vwap,
            ),
        },
    )
    session.execute(stmt)
    session.commit()
    return len(rows)


def get_latest_ts(
    session: Session,
    model: Type,
    exchange_id: str,
    symbol: str,
    timeframe: str,
):
    """
    取得該 (exchange, symbol, timeframe) 目前 DB 的最大 ts。
    """
    q = (
        select(func.max(model.ts))
        .where(
            model.exchange == exchange_id,
            model.symbol == symbol,
            model.timeframe == timeframe,
        )
        .limit(1)
    )
    return session.execute(q).scalar_one_or_none()
    
def get_earliest_ts(
    session: Session,
    model: Type,
    exchange_id: str,
    symbol: str,
    timeframe: str,
) -> Optional[datetime]:
    """
    取得該 (exchange, symbol, timeframe) 目前 DB 的最小 ts。
    用來判斷左側是否有缺口需要回補。
    """
    q = (
        select(func.min(model.ts))
        .where(
            model.exchange == exchange_id,
            model.symbol == symbol,
            model.timeframe == timeframe,
        )
        .limit(1)
    )
    return session.execute(q).scalar_one_or_none()
