# Quant Trading System

A notebook-first crypto quant workflow that ingests market data, runs strategy research directly inside Jupyter notebooks, and wires validated signals into an automated trading module. The goal is to shorten the research-to-trade loop: load data, prototype factors, backtest in notebooks, then push live orders through the execution layer with minimal glue code.

## Overview
- **Notebook research loop** – `notebooks/` hosts exploratory studies and backtests; strategies are iterated and validated here before moving to production.
- **Data ingestion** – `etl/` pipelines pull spot market metadata and klines from exchanges (via `ccxt`) and upsert them into TimescaleDB with idempotent operations.
- **Data access** – `backend/` FastAPI service provides notebook-friendly endpoints that return pandas DataFrames for rapid experimentation and live signal consumption.
- **Trading module** – `backtest/` and forthcoming execution packages share abstractions so a vetted notebook strategy can be promoted to an automated trader with minimal refactoring.
- **Infrastructure helpers** – `dal/` 和 `export_watchlist/` 維持資料存取與觀察名單流程的一致性，支援 notebook 研究與交易模組部署。


## Repository Layout
```
Quant_Trading_System/
├─ backend/              # FastAPI application, routers, services, schemas
├─ backtest/             # Backtesting module scaffold (to be implemented)
├─ config/               # Database connection settings (JSON)
├─ dal/                  # Database access layer, Alembic migrations, ORM models
├─ etl/                  # ETL configuration, extract/transform/load pipeline
├─ export_watchlist/     # TradingView watchlist export helper
├─ frontend/             # React UI source skeleton (build tooling pending)
├─ storage/              # Local volumes (PostgreSQL data)
├─ docker-compose.yml    # Local TimescaleDB/PostgreSQL + pgAdmin stack
├─ architecture.md       # Mermaid system architecture diagram
├─ systemDesign.png      # High-level architecture illustration
└─ requirements.txt      # Python dependencies for API + ETL tooling
```

## Prerequisites
- Python 3.11 (recommended) with `pip`
- Docker & Docker Compose for local PostgreSQL/TimescaleDB
- Node.js 18+ (planned; frontend scaffolding still WIP)
- Optional research libraries: `ccxt`, `pandas`, `twstock`, `finlab`

## Setup
1. **Clone & create virtual environment**
   ```bash
   git clone <repo-url>
   cd Quant_Trading_System
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   ```
2. **Install Python dependencies**
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```
   Additional ETL/export helpers may need extras:
   ```bash
   pip install ccxt twstock finlab python-dotenv
   ```
3. **Copy environment variables**
   - Use `.env` as the base for local runs. Keep API keys (e.g., `FINLAB_TOKEN`) out of version control for shared environments.
   - Configure database credentials in `config/database.json` or override via environment variables.

## Database & Migrations
1. **Start PostgreSQL/TimescaleDB locally**
   ```bash
   docker compose up -d postgres pgadmin
   ```
2. **Adjust Alembic connection**
   - Edit `dal/alembic.ini` or export `DATABASE_URL` for migrations.
   - Example: `export DATABASE_URL=postgresql+psycopg2://eason:777@localhost:5432/quant`
3. **Apply migrations**
   ```bash
   alembic -c dal/alembic.ini upgrade head
   ```

## Running Services
### Backend API
```bash
uvicorn backend.app.app:app --host 0.0.0.0 --port 8000 --reload
```
Key endpoints:
- `GET /health/` – Service heartbeat
- `GET /health/db` – Database connectivity check
- `GET /prices/` – Retrieve FinLab-compatible price frames (query by exchange, timeframe, field, symbols, date window)

### ETL Pipeline
Sync symbols and klines into the database:
```bash
python -m etl.run_etl --config etl/config.json --symbols
python -m etl.run_etl --config etl/config.json --klines
```
`etl/config.json` controls exchange ID, timeframes, batch limits, and seed timestamp; update it per environment.

### Frontend
`frontend/` currently contains UI placeholders. Add `package.json` / build tooling (Vite or Next.js) before running the client. A future README update should document the chosen stack and commands.

## Development Notes
- **Database access layer** – `dal/db.py` exposes synchronous and asynchronous session factories. Import `dal.db.db` in API/ETL code to reuse connection pools.
- **Price frame service** – `backend/app/services/finlab_price.py` pivots klines into FinLab-style `DataFrame` objects with metadata for downstream analytics.
- **ETL strategy** – `etl/pipeline.py` performs symbol syncing, left-gap backfill, and forward-filling using idempotent upserts. Rate limits respect exchange throttling via `ccxt`.
- **Watchlist exporter** – `export_watchlist/export.py` demonstrates how to format signals for TradingView uploads, including Taiwan market prefix handling.

## Roadmap
- Implement factor ETL modules and persistence for computed features.
- Build the backtesting engine (portfolio accounting, execution cost modelling, signal evaluation APIs).
- Finalise React frontend scaffolding and integrate with API endpoints.
- Harden cloud deployment (Helm/Argo manifests, Terraform/IaC, observability stack).
- Add automated tests (unit + integration) and CI workflows.

## Additional Resources
- `architecture.md` – Clickable diagram with cloud components and CI/CD flow.
- `systemDesign.png` – Static system overview image.
- `ccxt.ipynb` – Notebook for exploratory exchange data checks.

> **Security note:** Rotate or remove real secrets from `.env` and `config/` before publishing the repository. Use secret managers (AWS Secret Manager/Kubernetes secrets) in production environments.
