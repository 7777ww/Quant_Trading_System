# etl/config.py
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional


LOGGER = logging.getLogger(__name__)


@dataclass
class EtlSection:
    exchange_id: str = "binance"
    timeframes: List[str] = field(default_factory=lambda: ["1h"])
    batch_limit: int = 1000
    since_iso: str = "2021-01-01T00:00:00Z"
    quote_filter: str = "USDT"
    only_symbols: Optional[List[str]] = None  # None 表示不限制

    def validate(self) -> None:
        if not self.exchange_id:
            raise ValueError("etl.exchange_id 不可為空")
        if not self.timeframes:
            raise ValueError("etl.timeframes 至少需一個 timeframe")
        if self.batch_limit <= 0:
            raise ValueError("etl.batch_limit 必須為正整數")
        # 驗證 ISO 格式
        try:
            _ = datetime.fromisoformat(self.since_iso.replace("Z", "+00:00"))
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"etl.since_iso 非合法 ISO8601: {self.since_iso}") from exc


@dataclass
class LoggingSection:
    level: str = "INFO"


@dataclass
class AppConfig:
    etl: EtlSection = field(default_factory=EtlSection)
    logging: LoggingSection = field(default_factory=LoggingSection)


def _load_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"找不到設定檔：{path}")
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def load_config(path: str | Path) -> AppConfig:
    """
    從 JSON 設定檔載入配置，並做基本驗證。
    """
    p = Path(path).expanduser().resolve()
    raw = _load_json(p)

    etl_raw = raw.get("etl", {})
    logging_raw = raw.get("logging", {})

    cfg = AppConfig(
        etl=EtlSection(
            exchange_id=etl_raw.get("exchange_id", "binance"),
            timeframes=list(etl_raw.get("timeframes", ["1h"])),
            batch_limit=int(etl_raw.get("batch_limit", 1000)),
            since_iso=etl_raw.get("since_iso", "2021-01-01T00:00:00Z"),
            quote_filter=etl_raw.get("quote_filter", "USDT"),
            only_symbols=etl_raw.get("only_symbols"),
        ),
        logging=LoggingSection(
            level=logging_raw.get("level", "INFO"),
        ),
    )
    cfg.etl.validate()
    return cfg
