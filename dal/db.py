"""集中管理資料庫連線與 Session 工廠。"""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import AsyncIterator, Iterator, TypedDict

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")
DATABASE_CONFIG_FILE = os.path.join(CONFIG_DIR, "database.json")


class DBConfig(TypedDict, total=False):
    driver: str
    user: str
    password: str
    host: str
    port: int
    database: str
    url: str


def load_db_config() -> DBConfig:
    with Path(DATABASE_CONFIG_FILE).open(encoding="utf-8") as fp:
        config: dict[str, DBConfig] = json.load(fp)
    db_section = config.get("db")
    if db_section is None:
        msg = "database.json 需包含 `db` 欄位"
        raise KeyError(msg)
    return db_section


class Database:
    """封裝同步/非同步 Engine 與 Session 建立流程。"""

    def __init__(self, *, echo: bool = False, pool_pre_ping: bool = True) -> None:
        self._echo = echo
        self._pool_pre_ping = pool_pre_ping
        self._sync_engine: Engine | None = None
        self._async_engine: AsyncEngine | None = None
        self._session_factory: sessionmaker[Session] | None = None
        self._async_session_factory: async_sessionmaker[AsyncSession] | None = None

    def _build_url(self, *, async_mode: bool) -> str:
        cfg = load_db_config()
        if "url" in cfg:
            url = cfg["url"]
        else:
            url = (
                f"{cfg['driver']}://{cfg['user']}:{cfg['password']}@"
                f"{cfg['host']}:{cfg['port']}/{cfg['database']}"
            )
        if async_mode:
            return url.replace("psycopg2", "asyncpg", 1)
        return url

    @property
    def sync_engine(self) -> Engine:
        if self._sync_engine is None:
            self._sync_engine = create_engine(
                self._build_url(async_mode=False),
                echo=self._echo,
                pool_pre_ping=self._pool_pre_ping,
                future=True,
            )
        return self._sync_engine

    @property
    def async_engine(self) -> AsyncEngine:
        if self._async_engine is None:
            self._async_engine = create_async_engine(
                self._build_url(async_mode=True),
                echo=self._echo,
                pool_pre_ping=self._pool_pre_ping,
            )
        return self._async_engine

    @property
    def session_factory(self) -> sessionmaker[Session]:
        if self._session_factory is None:
            self._session_factory = sessionmaker(
                bind=self.sync_engine,
                autoflush=False,
                autocommit=False,
                expire_on_commit=False,
                future=True,
            )
        return self._session_factory

    @property
    def async_session_factory(self) -> async_sessionmaker[AsyncSession]:
        if self._async_session_factory is None:
            self._async_session_factory = async_sessionmaker(
                bind=self.async_engine,
                expire_on_commit=False,
            )
        return self._async_session_factory

    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @asynccontextmanager
    async def async_session_scope(self) -> AsyncIterator[AsyncSession]:
        async with self.async_session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise


db = Database()


def build_sync_engine() -> Engine:
    """向後相容：回傳同步 Engine。"""
    return db.sync_engine


def build_async_engine() -> AsyncEngine:
    """向後相容：回傳非同步 Engine。"""
    return db.async_engine
