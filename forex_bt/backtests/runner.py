from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from backtesting import Backtest

from ..storage.parquet_store import parquet_load_range
from .sma_cross import SMACross

LOG = logging.getLogger(__name__)


def run_sma_cross(
    root: Path,
    asset: str,
    timeframe: str,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    fast: int = 20,
    slow: int = 50,
    cash: float = 10_000.0,
    commission: float = 0.0,
) -> tuple[pd.DataFrame, dict]:
    df = parquet_load_range(root, asset, timeframe, start, end)
    if df.empty:
        raise ValueError("No data loaded for requested range")

    df.index = pd.to_datetime(df.index).tz_localize(None)

    SMACross.fast = fast
    SMACross.slow = slow

    bt = Backtest(df, SMACross, cash=cash, commission=commission, exclusive_orders=True)
    stats = bt.run()
    return df, stats
