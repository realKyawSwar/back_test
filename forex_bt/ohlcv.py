from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Sequence

import pandas as pd

from .storage.parquet_store import parquet_load_range, parquet_upsert

LOG = logging.getLogger(__name__)

SUPPORTED_TFS = ["1m", "5m", "15m", "30m", "1h", "4h", "1D"]
TF_TO_PANDAS = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "4h": "4h",
    "1D": "1D",
}


def resample_from_1m(df_1m: pd.DataFrame, target_tf: str) -> pd.DataFrame:
    if target_tf == "1m":
        return df_1m.copy()
    rule = TF_TO_PANDAS[target_tf]
    df = df_1m.copy()
    df = df.set_index(pd.to_datetime(df.index, utc=True))
    agg = df.resample(rule, label="left", closed="left").agg(
        Open=("Open", "first"),
        High=("High", "max"),
        Low=("Low", "min"),
        Close=("Close", "last"),
        Volume=("Volume", "sum"),
    )
    agg = agg.dropna(subset=["Open", "High", "Low", "Close"], how="any")
    agg = agg.reset_index().rename(columns={"index": "datetime"})
    return agg

def upsert_ohlcv_from_minutes(
    root: Path,
    asset: str,
    df_1m_chunks: Iterable[pd.DataFrame],
) -> int:
    total = 0
    for chunk in df_1m_chunks:
        chunk = chunk.copy()
        chunk["datetime"] = pd.to_datetime(chunk["datetime"], utc=True)
        total += parquet_upsert(root, asset, "1m", chunk)
    return total

def resample_and_store(
    root: Path,
    asset: str,
    target_tfs: Sequence[str],
    month_start: datetime,
    month_end: datetime,
) -> List[int]:
    wrote_counts: List[int] = []
    current = month_start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while current <= month_end:
        next_month = (current + pd.offsets.MonthBegin()).replace(day=1)
        start_dt = current
        end_dt = next_month - pd.Timedelta(seconds=1)
        df_1m = parquet_load_range(root, asset, "1m", start=start_dt, end=end_dt)
        if df_1m.empty:
            current = next_month
            continue

        for tf in target_tfs:
            df_tf = resample_from_1m(df_1m, tf)
            if df_tf.empty:
                continue
            wrote_counts.append(parquet_upsert(root, asset, tf, df_tf))
        current = next_month
    return wrote_counts
