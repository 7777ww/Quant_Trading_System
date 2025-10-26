"""Common base classes for reusable strategy implementations."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from ..data_bundle import DataBundle


class BaseStrategy(ABC):
    """Define the contract required by the backtest engine."""

    @abstractmethod
    def generate_positions(self, bundle: DataBundle) -> pd.DataFrame:
        """Return a position matrix (-1/0/1 or weights) indexed by time."""

    def prepare_prices(self, bundle: DataBundle) -> pd.DataFrame:
        """Hook for strategies that need to adjust raw price data before use."""
        return bundle.prices
