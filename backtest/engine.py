"""Core backtesting engine that transforms signals into performance metrics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from pandas.api.types import is_bool_dtype, is_numeric_dtype

from .results import BacktestResult, compute_performance_stats


@dataclass(slots=True)
class EngineConfig:
    initial_capital: float = 1.0
    transaction_cost: float = 0.0
    annualization_factor: int = 252
    max_active_positions: int | None = None


class BacktestEngine:
    """Convert position or weight data into equity curves and statistics."""

    def __init__(self, config: EngineConfig | None = None) -> None:
        self.config = config or EngineConfig()

    def run(
        self,
        *,
        prices: pd.DataFrame,
        weights: pd.DataFrame | None = None,
        positions: pd.DataFrame | None = None,
        signals: pd.DataFrame | None = None,
    ) -> BacktestResult:
        price_frame = self._coerce_frame(prices, name="prices")
        returns = price_frame.pct_change().fillna(0.0)
        if returns.empty:
            empty_series = pd.Series(dtype=float, index=price_frame.index)
            empty_frame = pd.DataFrame(index=price_frame.index, columns=price_frame.columns)
            return BacktestResult(
                equity_curve=empty_series,
                positions=empty_frame,
                weights=empty_frame,
                turnover=empty_series,
                stats={},
            )

        resolved_positions: pd.DataFrame | None = None
        if weights is None:
            if positions is None and signals is not None:
                positions = self._signals_to_positions(signals)
            if positions is None:
                raise ValueError("Must provide either weights or positions/signals for backtesting")
            resolved_positions = self._coerce_frame(positions, name="positions").fillna(0.0)
            if self.config.max_active_positions is not None:
                resolved_positions = self._apply_position_cap(
                    resolved_positions,
                    self.config.max_active_positions,
                )
            weights = self._rebalance_weights(resolved_positions)
        else:
            weights = self._coerce_frame(weights, name="weights").astype(float)

        weights = weights.reindex(returns.index).ffill().fillna(0.0)
        if resolved_positions is None:
            resolved_positions = weights.apply(np.sign)
        else:
            resolved_positions = resolved_positions.reindex(returns.index).ffill().fillna(0.0)

        shifted_weights = weights.shift(1).fillna(0.0)
        portfolio_returns = (shifted_weights * returns).sum(axis=1)

        turnover = weights.diff().abs().sum(axis=1).fillna(0.0)
        if self.config.transaction_cost > 0:
            cost = turnover * self.config.transaction_cost
            portfolio_returns = portfolio_returns - cost

        equity_curve = (1 + portfolio_returns).cumprod() * self.config.initial_capital
        stats = compute_performance_stats(portfolio_returns, annualization_factor=self.config.annualization_factor)

        return BacktestResult(
            equity_curve=equity_curve,
            positions=resolved_positions,
            weights=weights,
            turnover=turnover,
            stats=stats,
        )

    @staticmethod
    def _coerce_frame(frame: Any, *, name: str) -> pd.DataFrame:
        if frame is None:
            raise ValueError(f"{name} frame is required")
        if isinstance(frame, pd.Series):
            frame = frame.to_frame()
        if not isinstance(frame, pd.DataFrame):
            raise TypeError(f"{name} must be a pandas DataFrame")
        if frame.empty:
            return frame.copy()
        if not isinstance(frame.index, pd.DatetimeIndex):
            raise TypeError(f"{name} index must be a pandas.DatetimeIndex")
        coerced = frame.copy()
        if coerced.index.tz is not None:
            coerced.index = coerced.index.tz_convert(None)
        coerced = coerced.sort_index()
        return coerced

    def _signals_to_positions(self, signals: pd.DataFrame) -> pd.DataFrame:
        frame = self._coerce_frame(signals, name="signals")
        if frame.empty:
            return frame.copy()

        converted = frame.copy()
        for column in converted.columns:
            series = converted[column]
            if is_bool_dtype(series):
                converted[column] = series.astype(float)
            elif is_numeric_dtype(series):
                converted[column] = series.astype(float)
            else:
                raise TypeError(
                    "signals must contain boolean or numeric values that can be converted to positions"
                )
        return converted

    @staticmethod
    def _rebalance_weights(positions: pd.DataFrame) -> pd.DataFrame:
        if positions.empty:
            return positions.copy()
        long_mask = positions > 0
        short_mask = positions < 0

        long_counts = long_mask.sum(axis=1).replace(0, np.nan)
        short_counts = short_mask.sum(axis=1).replace(0, np.nan)

        long_weights = long_mask.astype(float).div(long_counts, axis=0).fillna(0.0)
        short_weights = short_mask.astype(float).div(short_counts, axis=0).fillna(0.0)
        weights = long_weights - short_weights
        return weights

    @staticmethod
    def _apply_position_cap(positions: pd.DataFrame, limit: int) -> pd.DataFrame:
        if limit <= 0:
            raise ValueError("max_active_positions 必須為正整數")
        if positions.empty:
            return positions

        capped = positions.copy()
        columns = list(positions.columns)
        active_longs: list[str] = []
        active_shorts: list[str] = []

        for idx in positions.index:
            row = positions.loc[idx]

            # Remove exposures whose signal turned off
            active_longs = [symbol for symbol in active_longs if row[symbol] > 0]
            active_shorts = [symbol for symbol in active_shorts if row[symbol] < 0]

            long_candidates = [symbol for symbol in columns if row[symbol] > 0 and symbol not in active_longs]
            short_candidates = [symbol for symbol in columns if row[symbol] < 0 and symbol not in active_shorts]

            long_slots = max(0, limit - len(active_longs))
            short_slots = max(0, limit - len(active_shorts))

            if long_slots:
                active_longs.extend(long_candidates[:long_slots])
            if short_slots:
                active_shorts.extend(short_candidates[:short_slots])

            allowed: set[str] = set(active_longs + active_shorts)
            updated = row.copy()
            for symbol in columns:
                if symbol not in allowed:
                    updated[symbol] = 0.0
            capped.loc[idx] = updated

        return capped
