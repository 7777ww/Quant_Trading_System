from __future__ import annotations

import argparse
import logging

from sqlalchemy.orm import Session

# 從你的 DAL 匯入「連線」與「模型」
from dal.db import  build_sync_engine  # 你現有的連線工廠
from dal.models import Symbol, Kline  # 你現有的 ORM 模型

# 匯入 ETL pipeline
from etl.pipeline import sync_symbols, sync_klines
from etl.config import load_config            # ← 新增：讀 JSON

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)

def main() -> None:
    parser = argparse.ArgumentParser(description="Crypto Kline ETL (DAL-injected)")
    parser.add_argument("--symbols", action="store_true", help="同步 USDT/spot 交易對列表")
    parser.add_argument("--klines", action="store_true", help="回補/更新 K 線資料")
    parser.add_argument(
        "--config",
        type=str,
        default="jsonfilexu3",
        help="設定檔路徑（JSON），預設為 ./jsonfilexu3",
    )

    parser.add_argument("--exchange", type=str, default=None, help="覆寫 EXCHANGE_ID")
    args = parser.parse_args()

    cfg = load_config(args.config)            # ← 真的載入 JSON

    engine = build_sync_engine()
    exchange_id = args.exchange if args.exchange else None

    with Session(engine) as sess:
        if args.symbols:
            eid = exchange_id or "binance"
            n = sync_symbols(sess, Symbol, eid)
            print(f"symbols upserted: {n}")

        if args.klines:
            # 依 .env 之 EXCHANGE_ID/TIMEFRAMES/BATCH_LIMIT/SINCE_ISO 執行
            sync_klines(sess, Symbol, Kline, cfg)


if __name__ == "__main__":
    main()
