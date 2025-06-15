"""
db_connection.py
以 JSON 讀取資料庫設定並建立 SQLAlchemy Engine
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TypedDict

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
CONFIG_DIR = os.path.join(PROJECT_ROOT, 'config')
DATABASE_CONFIG_FILE = os.path.join(CONFIG_DIR, 'database.json')


class DBConfig(TypedDict, total=False):
    """型別提示：方法 A 各欄位 or 方法 B 的 url"""
    driver: str
    user: str
    password: str
    host: str
    port: int
    database: str
    url: str  # 方法 B


def load_db_config() -> DBConfig:
    """讀取當前目錄下的 config.json 並回傳 db 欄位"""
    with Path(DATABASE_CONFIG_FILE).open(encoding="utf-8") as fp:
        return json.load(fp)


def build_sync_engine() -> "Engine":
    """建立同步 Engine（psycopg2）"""
    config_file = load_db_config()
    db = config_file["db"]
    # 方法 B：若有 url 直接用
    if "url" in db:
        db_url = db["url"]
    else:  # 方法 A：自行組合
        db_url = (
            f"{db['driver']}://{db['user']}:{db['password']}@"
            f"{db['host']}:{db['port']}/{db['database']}"
        )

    return create_engine(db_url, echo=True, pool_pre_ping=True)


def build_async_engine() -> "AsyncEngine":
    """建立非同步 Engine（asyncpg）"""
    config_file = load_db_config()
    db = config_file["db"]

    if "url" in db:
        async_url = db["url"].replace("psycopg2", "asyncpg", 1)
    else:
        async_url = (
            f"{db['driver'].replace('psycopg2', 'asyncpg', 1)}://"
            f"{db['user']}:{db['password']}@{db['host']}:{db['port']}/"
            f"{db['database']}"
        )

    return create_async_engine(async_url, echo=False, pool_pre_ping=True)


if __name__ == "__main__":
    # 測試同步連線
    engine = build_sync_engine()
    with engine.connect() as conn:
        version = conn.exec_driver_sql("SELECT version();").scalar_one()
        print("PostgreSQL 版本：", version)
