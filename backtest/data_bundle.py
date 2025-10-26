"""Container objects for grouping backtest input data."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass(slots=True)
class DataBundle:
    """Lightweight wrapper collecting price data and optional auxiliary fields."""

    prices: pd.DataFrame
    signals: pd.DataFrame | None = None
    extras: dict[str, Any] = field(default_factory=dict)

    def copy(self) -> "DataBundle":
        return DataBundle(
            prices=self.prices.copy(),
            signals=None if self.signals is None else self.signals.copy(),
            extras={key: value for key, value in self.extras.items()},
        )
