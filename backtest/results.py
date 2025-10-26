"""Backtest result structures and helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

import numpy as np
import pandas as pd


@dataclass(slots=True)
class BacktestResult:
    """Container holding the key artefacts produced by a backtest run."""

    equity_curve: pd.Series
    positions: pd.DataFrame
    weights: pd.DataFrame
    turnover: pd.Series
    stats: Mapping[str, float]

    def to_frame(self) -> pd.DataFrame:
        """Return a consolidated frame combining equity, turnover, and stats."""
        combined = pd.DataFrame({
            "equity": self.equity_curve,
            "turnover": self.turnover,
        })
        return combined


def compute_performance_stats(
    returns: pd.Series,
    *,
    annualization_factor: int = 252,
) -> dict[str, float]:
    """Compute a standard set of performance statistics for a return series."""
    if returns.empty:
        return {}

    mean_daily = returns.mean()
    vol = returns.std(ddof=0)
    sharpe = mean_daily / vol * np.sqrt(annualization_factor) if vol != 0 else np.nan
    cumulative = (1 + returns).prod() - 1
    ann_return = (1 + cumulative) ** (annualization_factor / len(returns)) - 1
    ann_vol = vol * np.sqrt(annualization_factor)
    max_drawdown = _max_drawdown(returns)

    return {
        "ann_return": float(ann_return),
        "ann_vol": float(ann_vol),
        "sharpe": float(sharpe),
        "cumulative_return": float(cumulative),
        "max_drawdown": float(max_drawdown),
    }


def _max_drawdown(returns: pd.Series) -> float:
    if returns.empty:
        return 0.0
    equity = (1 + returns).cumprod()
    rolling_max = equity.cummax()
    drawdown = (equity / rolling_max) - 1
    return float(drawdown.min())
