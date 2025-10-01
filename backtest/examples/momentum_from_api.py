"""Momentum backtest driven by live price API data."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backtest import MomentumConfig, run_momentum_backtest


def main() -> None:
    config = MomentumConfig(
        lookback=60,
        top_n=3,
        bottom_n=0,
        signal_delay=1,
        transaction_cost=0.0000,
        rebalance_frequency="D" )

    result = run_momentum_backtest(
        exchange="binance",
        timeframe="1h",
        symbols=None,  # None 代表從 API 取得所有可用標的
        start=datetime(2024, 1, 1),
        end=datetime(2024, 9, 1),
        config=config,
        api_base_url=None,  # 使用 QUANT_API_BASE_URL 或預設 http://localhost:8000
    )

    print("Backtest statistics:")
    for key, value in result.stats.items():
        print(f"  {key}: {value:.4f}")

    if not result.equity_curve.empty:
        symbol_count = len(result.positions.columns) if not result.positions.empty else 0
        print(f"Symbols included: {symbol_count}")
        print("\nFinal equity:", result.equity_curve.iloc[-1])
    else:
        print("\n無回測結果，請確認 API 是否回傳資料。")


if __name__ == "__main__":
    main()
