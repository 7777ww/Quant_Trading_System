"""Helpers for converting FinLab-style DataFrames into backtest inputs."""

from __future__ import annotations

from typing import Any

import pandas as pd
from pandas.api.types import is_bool_dtype, is_numeric_dtype

try:
    # Prefer the FinLab-specific DataFrame subclass when backend package is available.
    from backend.app.services.finlab_price import FinlabDataFrame  # type: ignore
except ImportError:  # pragma: no cover - fallback for environments without backend package
    FinlabDataFrame = pd.DataFrame  # type: ignore[misc,assignment]


class FinlabDataAdapter:
    """Utility functions that tidy FinLab data for the backtest engine."""

    @staticmethod
    def to_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
        """Return a float-valued price matrix indexed by timestamps."""
        resolved = FinlabDataAdapter._coerce_dataframe(frame)
        return resolved.astype(float, copy=False)

    @staticmethod
    def to_signal_frame(frame: pd.DataFrame) -> pd.DataFrame:
        """Return a frame of numeric signals (booleans converted to 0/1)."""
        resolved = FinlabDataAdapter._coerce_dataframe(frame)
        if resolved.empty:
            return resolved

        converted = resolved.copy()
        for column in converted.columns:
            series = converted[column]
            if is_bool_dtype(series):
                converted[column] = series.astype(float)
            elif is_numeric_dtype(series):
                converted[column] = series.astype(float)
            else:
                raise TypeError("Signal columns must be boolean or numeric")
        return converted

    @staticmethod
    def to_positions(
        frame: pd.DataFrame,
        *,
        long_value: float = 1.0,
        flat_value: float = 0.0,
        short_value: float = -1.0,
    ) -> pd.DataFrame:
        """Convert a boolean/ternary FinLab frame into position indicators."""
        signals = FinlabDataAdapter.to_signal_frame(frame)
        if signals.empty:
            return signals

        # Boolean style 0/1 signals → map成 long/flat 值，維持使用者自訂的數值。
        if signals.isin({0.0, 1.0}).all(axis=None):
            return signals.replace({1.0: long_value, 0.0: flat_value})
        # 三元訊號 -1/0/1 → 直接映射成 long/flat/short。
        if signals.isin({-1.0, 0.0, 1.0}).all(axis=None):
            return signals.replace({1.0: long_value, 0.0: flat_value, -1.0: short_value})
        return signals

    @staticmethod
    def _coerce_dataframe(frame: Any) -> pd.DataFrame:
        if isinstance(frame, pd.Series):
            frame = frame.to_frame()
        if isinstance(frame, FinlabDataFrame):
            resolved = frame.copy()
        elif isinstance(frame, pd.DataFrame):
            resolved = frame.copy()
        else:
            resolved = pd.DataFrame(frame)

        if resolved.empty:
            return resolved

        if not isinstance(resolved.index, pd.DatetimeIndex):
            raise TypeError("FinLab frames must be indexed by timestamp")

        index = resolved.index
        if index.tz is not None:
            resolved.index = index.tz_convert(None)
        return resolved.sort_index()
