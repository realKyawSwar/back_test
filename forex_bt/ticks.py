from __future__ import annotations

import logging
import lzma
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generator, Iterable, Tuple

import numpy as np
import pandas as pd

LOG = logging.getLogger(__name__)


DTYPE = np.dtype([
    ("ms", ">i4"),
    ("bidp", ">i4"),
    ("askp", ">i4"),
    ("bidv", ">f4"),
    ("askv", ">f4"),
])


def dukascopy_hour_path(download_root: Path, asset: str, dt: datetime) -> Path:
    year = dt.year
    month = dt.month - 1  # Dukascopy months are 0-11
    day = dt.day
    hour = dt.hour
    return Path(download_root) / asset / f"{year:04d}" / f"{month:02d}" / f"{day:02d}" / f"{hour:02d}h_ticks.bi5"


def iter_hour_paths(download_root: Path, asset: str, start: datetime, end: datetime) -> Generator[Tuple[datetime, Path], None, None]:
    cur = start.replace(minute=0, second=0, microsecond=0)
    while cur <= end:
        yield cur, dukascopy_hour_path(download_root, asset, cur)
        cur += timedelta(hours=1)


def decode_hour_file(path: Path, origin: datetime) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)

    # Some downloads may leave behind zero-length or otherwise invalid files.
    # Treat them as missing data instead of aborting the whole run.
    if path.stat().st_size == 0:
        LOG.warning("Skipping empty hour file: %s", path)
        return pd.DataFrame(columns=["datetime", "price", "volume"])

    try:
        with lzma.open(path, "rb") as f:
            raw = f.read()
    except lzma.LZMAError as exc:
        LOG.warning("Failed to decode hour file %s: %s", path, exc)
        return pd.DataFrame(columns=["datetime", "price", "volume"])

    if not raw:
        return pd.DataFrame(columns=["datetime", "price", "volume"])

    arr = np.frombuffer(raw, dtype=DTYPE)
    if arr.size == 0:
        return pd.DataFrame(columns=["datetime", "price", "volume"])

    dt = [origin + timedelta(milliseconds=int(ms)) for ms in arr["ms"]]
    price = arr["bidp"] / 100000.0
    volume = arr["bidv"]
    return pd.DataFrame({"datetime": dt, "price": price, "volume": volume})


def aggregate_ticks_to_1m(ticks: pd.DataFrame) -> pd.DataFrame:
    if ticks.empty:
        return pd.DataFrame(columns=["datetime", "Open", "High", "Low", "Close", "Volume"])

    ticks = ticks.copy()
    ticks["datetime"] = pd.to_datetime(ticks["datetime"], utc=True)
    ticks = ticks.set_index("datetime")

    ohlcv = ticks.resample("1min", label="left", closed="left").agg(
        Open=("price", "first"),
        High=("price", "max"),
        Low=("price", "min"),
        Close=("price", "last"),
        Volume=("volume", "sum"),
    )

    ohlcv = ohlcv.dropna(subset=["Open", "High", "Low", "Close"], how="any")
    ohlcv = ohlcv.reset_index()
    return ohlcv


def stream_aggregate_1m(
    download_root: Path,
    asset: str,
    start: datetime,
    end: datetime,
) -> Iterable[pd.DataFrame]:
    for origin, path in iter_hour_paths(download_root, asset, start, end):
        try:
            ticks = decode_hour_file(path, origin.replace(tzinfo=timezone.utc))
        except FileNotFoundError:
            LOG.warning("Missing hour file: %s", path)
            continue
        if ticks.empty:
            continue
        minute_bars = aggregate_ticks_to_1m(ticks)
        if not minute_bars.empty:
            yield minute_bars
