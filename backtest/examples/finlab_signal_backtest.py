"""Example demonstrating backtesting with FinLab-style boolean signals."""

from __future__ import annotations

import numpy as np
import pandas as pd

from backtest import BacktestEngine, EngineConfig, FinlabDataAdapter


def build_mock_finlab_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create simple price & signal frames using FinLab's boolean style."""
    index = pd.date_range("2025-05-01", periods=40, freq="4H", tz="UTC")
    symbols = ["AAA/USDT", "BBB/USDT", "CCC/USDT"]

    base_prices = np.linspace(10, 12, len(index))
    noise = np.random.default_rng(42).normal(scale=0.5, size=(len(index), len(symbols)))
    prices = pd.DataFrame(base_prices[:, None] + noise, index=index, columns=symbols)

    rolling_mean = prices.rolling(window=5, min_periods=3).mean()
    signals = prices > rolling_mean
    return prices, signals


def main() -> None:
    prices, signals = build_mock_finlab_frames()

    price_frame = FinlabDataAdapter.to_price_frame(prices)
    signal_frame = FinlabDataAdapter.to_signal_frame(signals)

    # 不需自備資金配置：設定 max_active_positions 後，引擎會挑出訊號為 True 的標的等權投入，
    # 同時維持持股數上限，直到舊部位出場才釋出額度給新訊號。
    engine = BacktestEngine(
        EngineConfig(
            initial_capital=1_000_000,
            transaction_cost=0.001,
            max_active_positions=2,
        )
    )
    result = engine.run(prices=price_frame, signals=signal_frame)

    print("Backtest statistics:")
    if result.stats:
        for key, value in result.stats.items():
            print(f"  {key}: {value:.4f}")
    else:
        print("  (no statistics – check your input data)")

    if not result.equity_curve.empty:
        print("\nFinal equity:", f"{result.equity_curve.iloc[-1]:.2f}")


if __name__ == "__main__":
    main()
