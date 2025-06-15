# Quant_Trading_System
quant-crypto/                       # 專案根目錄
├─ .env.example                     # 範例環境變數
├─ .gitignore
├─ docker-compose.yml               # 啟動所有服務
├─ prometheus.yml                   # Prometheus 抓取設定
├─ README.md
│
├─ storage/                         # 資料掛載點（不進 Git）
│   └─ pgdata/                      # PostgreSQL 資料
│
├─ backend/
│   ├─ Dockerfile
│   ├─ requirements.txt
│   └─ app/
│       ├─ __init__.py
│       ├─ api.py                   # FastAPI 入口
│       ├─ db.py                    # async SQLAlchemy 連線
│       ├─ models.py                # Pydantic schema
│       ├─ research.py              # 因子計算 / 回測
│       ├─ metrics.py               # /metrics 中介層
│       └─ scripts/
│           ├─ __init__.py
│           └─ import_parquet_to_pg.py
│
├─ etl/
│   ├─ Dockerfile
│   ├─ requirements.txt
│   ├─ main.py                      # 啟動 APScheduler
│   ├─ jobs.py                      # 抓行情與寫入 PG
│   ├─ metrics.py                   # ETL 端 /metrics
│   └─ sql/
│       └─ ddl.sql                  # 建表／Hypertable 腳本
│
├─ frontend/
│   ├─ Dockerfile
│   ├─ package.json
│   ├─ vite.config.ts
│   └─ src/
│       ├─ main.tsx
│       ├─ App.tsx
│       ├─ pages/
│       │   └─ Backtest.tsx         # 因子回測頁
│       └─ components/
│           └─ ui/                  # shadcn/ui 元件
│
└─ prometheus/ （可選：放 Grafana dashboard JSON）
