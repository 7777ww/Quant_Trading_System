"""SQLAlchemy ORM 模型（Minimal symbols 版, PEP 8）。"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import TIMESTAMP, BigInteger, Boolean, text
from sqlalchemy.orm import  Mapped, mapped_column

from .base_model import Base



class Symbol(Base):
    """極簡 symbols 表：只保留抓 K 線與治理需要的欄位。"""

    __tablename__ = "symbols"
    __table_args__ = {"schema": "market"}

    exchange: Mapped[str] = mapped_column(primary_key=True)
    symbol: Mapped[str] = mapped_column(primary_key=True)  # ccxt unified symbol，如 BTC/USDT
    base: Mapped[Optional[str]]
    quote: Mapped[Optional[str]]
    market_id: Mapped[Optional[str]]  # 交易所原生代號，如 BTCUSDT（可選）
    active: Mapped[Optional[bool]] = mapped_column(Boolean, default=True)
    first_seen: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("NOW()")
    )
    last_seen: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("NOW()")
    )


class Kline(Base):
    """K 線表：最小必需欄位。"""

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