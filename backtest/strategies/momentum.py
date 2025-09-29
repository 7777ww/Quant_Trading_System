"""Cross-sectional momentum strategy utilities."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Sequence
from urllib.parse import urljoin

import numpy as np
import pandas as pd
import requests
from requests import HTTPError

DEFAULT_API_BASE_URL = "http://localhost:8000"


@dataclass
class MomentumConfig:
    lookback: int = 60
    top_n: int = 5
    bottom_n: int = 0
    signal_delay: int = 1
    rebalance_frequency: str = "D"
    transaction_cost: float = 0.0

    def __post_init__(self) -> None:
        if self.lookback <= 0:
            msg = "lookback 必須大於 0"
            raise ValueError(msg)
        if self.top_n < 0 or self.bottom_n < 0:
            msg = "top_n 與 bottom_n 需為非負整數"
            raise ValueError(msg)
        if self.signal_delay < 0:
            msg = "signal_delay 需為非負整數"
            raise ValueError(msg)


@dataclass
class BacktestResult:
    equity_curve: pd.Series
    positions: pd.DataFrame
    weights: pd.DataFrame
    turnover: pd.Series
    stats: dict[str, float]


def load_close_prices(
    *,
    exchange: str,
    timeframe: str,
    symbols: Sequence[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    api_base_url: str | None = None,
    timeout: float = 30.0,
) -> pd.DataFrame:
    """Call the price API and return a tz-naive close-price frame."""
    if symbols is not None and not list(symbols):
        msg = "symbols 不可為空列表"
        raise ValueError(msg)

    base_url = (api_base_url or os.getenv("QUANT_API_BASE_URL") or DEFAULT_API_BASE_URL).rstrip("/")
    endpoint = urljoin(base_url + "/", "prices/")

    params: dict[str, object] = {
        "exchange": exchange,
        "timeframe": timeframe,
        "field": "close",
    }
    if symbols:
        params["symbols"] = list(dict.fromkeys(symbols))
    if start is not None:
        params["start"] = start.isoformat()
    if end is not None:
        params["end"] = end.isoformat()

    try:
        response = requests.get(endpoint, params=params, timeout=timeout)
        response.raise_for_status()
    except HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        msg = f"價格 API 回傳錯誤 (status={status})"
        raise RuntimeError(msg) from exc
    except requests.RequestException as exc:
        raise RuntimeError("無法連線至價格 API") from exc

    payload = response.json()
    context = payload.get("context", {}) if isinstance(payload, dict) else {}
    records = context.get("data", []) if isinstance(context, dict) else []
    meta = context.get("meta") if isinstance(context, dict) else None

    if not records:
        if isinstance(meta, dict) and meta.get("symbols"):
            requested_columns = [str(symbol) for symbol in meta["symbols"]]
        elif symbols:
            requested_columns = list(dict.fromkeys(symbols))
        else:
            requested_columns = []
        return pd.DataFrame(columns=requested_columns)

    frame = pd.DataFrame.from_records(records)
    if "ts" not in frame.columns:
        raise RuntimeError("價格 API 回傳資料缺少 ts 欄位")

    frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
    frame = frame.set_index("ts").sort_index()

    resolved_symbols: Sequence[str]
    if isinstance(meta, dict) and meta.get("symbols"):
        resolved_symbols = [str(symbol) for symbol in meta["symbols"]]
    elif symbols:
        resolved_symbols = list(dict.fromkeys(symbols))
    else:
        resolved_symbols = [str(column) for column in frame.columns]

    frame = frame.reindex(columns=resolved_symbols)
    frame.index = frame.index.tz_convert(None)
    return frame


class MomentumStrategy:
    def __init__(self, config: MomentumConfig | None = None) -> None:
        self.config = config or MomentumConfig()

    @staticmethod
    def _align_prices(prices: pd.DataFrame, frequency: str) -> pd.DataFrame:
        if frequency.upper() == "D":
            return prices.asfreq("1D", method=None)
        return prices

    def compute_momentum(self, prices: pd.DataFrame) -> pd.DataFrame:
        aligned = self._align_prices(prices, self.config.rebalance_frequency)
        momentum = aligned.pct_change(periods=self.config.lookback)
        if self.config.signal_delay:
            momentum = momentum.shift(self.config.signal_delay)
        return momentum

    def generate_positions(self, prices: pd.DataFrame) -> pd.DataFrame:
        momentum = self.compute_momentum(prices)
        momentum = momentum.dropna(how="all")
        if momentum.empty:
            return momentum.copy()

        ranks = momentum.rank(axis=1, ascending=False, method="first")
        total_assets = momentum.shape[1]

        long_mask = ranks <= self.config.top_n
        short_mask = (
            self.config.bottom_n > 0
            and ranks >= total_assets - self.config.bottom_n + 1
        )

        positions = pd.DataFrame(0, index=momentum.index, columns=momentum.columns)
        if self.config.top_n:
            positions = positions.where(~long_mask, 1)
        if self.config.bottom_n:
            positions = positions.where(~short_mask, -1)
        positions = positions.where(momentum.notna(), 0)
        return positions

    @staticmethod
    def _rebalance_weights(positions: pd.DataFrame) -> pd.DataFrame:
        if positions.empty:
            return positions
        long_counts = (positions > 0).sum(axis=1).replace(0, np.nan)
        short_counts = (positions < 0).sum(axis=1).replace(0, np.nan)

        long_weights = (positions > 0).astype(float).div(long_counts, axis=0).fillna(0.0)
        short_weights = (positions < 0).astype(float).div(short_counts, axis=0).fillna(0.0)
        weights = long_weights - short_weights
        return weights

    def backtest(self, prices: pd.DataFrame, *, initial_capital: float = 1.0) -> BacktestResult:
        prices = prices.sort_index()
        positions = self.generate_positions(prices)
        if positions.empty:
            empty_series = pd.Series(dtype=float)
            return BacktestResult(
                equity_curve=empty_series,
                positions=positions,
                weights=positions,
                turnover=empty_series,
                stats={},
            )

        weights = self._rebalance_weights(positions)
        asset_returns = prices.pct_change().reindex(weights.index).fillna(0.0)
        portfolio_returns = (weights.shift(1).fillna(0.0) * asset_returns).sum(axis=1)

        turnover = weights.diff().abs().sum(axis=1).fillna(0.0)
        if self.config.transaction_cost > 0:
            cost = turnover * self.config.transaction_cost
            portfolio_returns = portfolio_returns - cost

        equity_curve = (1 + portfolio_returns).cumprod() * initial_capital

        stats = self._compute_stats(portfolio_returns)
        return BacktestResult(
            equity_curve=equity_curve,
            positions=positions,
            weights=weights,
            turnover=turnover,
            stats=stats,
        )

    @staticmethod
    def _compute_stats(returns: pd.Series) -> dict[str, float]:
        if returns.empty:
            return {}
        ann_factor = 252
        mean_daily = returns.mean()
        vol = returns.std(ddof=0)
        sharpe = mean_daily / vol * np.sqrt(ann_factor) if vol != 0 else np.nan
        cumulative = (1 + returns).prod() - 1
        ann_return = (1 + cumulative) ** (ann_factor / len(returns)) - 1
        max_drawdown = MomentumStrategy._max_drawdown(returns)
        return {
            "ann_return": ann_return,
            "ann_vol": vol * np.sqrt(ann_factor),
            "sharpe": sharpe,
            "cumulative_return": cumulative,
            "max_drawdown": max_drawdown,
        }

    @staticmethod
    def _max_drawdown(returns: pd.Series) -> float:
        if returns.empty:
            return 0.0
        equity = (1 + returns).cumprod()
        rolling_max = equity.cummax()
        drawdown = (equity / rolling_max) - 1
        return drawdown.min()

    @classmethod
    def from_prices_api(
        cls,
        *,
        exchange: str,
        timeframe: str,
        symbols: Sequence[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        config: MomentumConfig | None = None,
        api_base_url: str | None = None,
        timeout: float = 30.0,
    ) -> tuple["MomentumStrategy", pd.DataFrame]:
        prices = load_close_prices(
            exchange=exchange,
            timeframe=timeframe,
            symbols=symbols,
            start=start,
            end=end,
            api_base_url=api_base_url,
            timeout=timeout,
        )
        return cls(config), prices

    @classmethod
    def from_database(cls, **kwargs):  # type: ignore[override]
        msg = "MomentumStrategy.from_database 已改為透過價格 API 取數，請改用 from_prices_api。"
        raise NotImplementedError(msg)


def run_momentum_backtest(
    *,
    prices: pd.DataFrame | None = None,
    exchange: str | None = None,
    timeframe: str | None = None,
    symbols: Sequence[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    config: MomentumConfig | None = None,
    api_base_url: str | None = None,
    timeout: float = 30.0,
) -> BacktestResult:
    if prices is None:
        if exchange is None or timeframe is None:
            msg = "需提供 prices 或 (exchange, timeframe) 組合"
            raise ValueError(msg)
        strategy, prices = MomentumStrategy.from_prices_api(
            exchange=exchange,
            timeframe=timeframe,
            symbols=symbols,
            start=start,
            end=end,
            config=config,
            api_base_url=api_base_url,
            timeout=timeout,
        )
    else:
        strategy = MomentumStrategy(config)
    return strategy.backtest(prices)
