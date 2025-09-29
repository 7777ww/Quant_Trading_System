"""Example script that demonstrates the momentum strategy on synthetic data."""

from __future__ import annotations

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datetime import datetime

import numpy as np
import pandas as pd

from backtest import MomentumConfig, MomentumStrategy


def build_fake_prices(symbols: list[str], periods: int = 400) -> pd.DataFrame:
    start = datetime(2020, 1, 1)
    dates = pd.date_range(start=start, periods=periods, freq="D")
    levels = {}
    rng = np.random.default_rng(seed=42)
    for symbol in symbols:
        shocks = rng.normal(loc=0.0005, scale=0.02, size=periods)
        price = 100 * np.exp(np.cumsum(shocks))
        levels[symbol] = price
    return pd.DataFrame(levels, index=dates)


def main() -> None:
    prices = build_fake_prices(["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"], periods=500)

    config = MomentumConfig(lookback=60, top_n=2, bottom_n=1, signal_delay=1)
    strategy = MomentumStrategy(config)
    result = strategy.backtest(prices)

    print("Backtest statistics:")
    for key, value in result.stats.items():
        print(f"  {key}: {value:.4f}")

    print("\nFinal equity:", result.equity_curve.iloc[-1])


if __name__ == "__main__":
    main()
