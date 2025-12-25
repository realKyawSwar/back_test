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


def _ensure_native_endian(df: pd.DataFrame, cols: tuple[str, ...]) -> pd.DataFrame:
    """
    Convert big-endian numeric buffers (e.g. >f4 from Dukascopy BI5)
    to native-endian so pandas resample/groupby works on Windows.
    """
    for c in cols:
        if c in df.columns:
            a = df[c].to_numpy(copy=False)
            if getattr(a.dtype, "byteorder", "=") == ">":
                df[c] = a.byteswap().view(a.dtype.newbyteorder("="))
    return df


def aggregate_ticks_to_1m(ticks: pd.DataFrame) -> pd.DataFrame:
    ticks = ticks.copy()

    # --- FIX: make price/volume native-endian BEFORE any pandas agg ---
    ticks = _ensure_native_endian(ticks, ("price", "volume"))

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


def _iter_missing_minutes_for_hour(download_root: Path, asset: str, origin: datetime) -> Iterable[datetime]:
    """Yield missing minute timestamps (UTC) for the given hour.

    - If the hour file is missing or empty/undecodable, yield all 60 minutes.
    - Otherwise aggregate ticks to 1m and yield any minute within the hour with no bar.
    """
    path = dukascopy_hour_path(download_root, asset, origin)
    try:
        ticks = decode_hour_file(path, origin)
    except FileNotFoundError:
        # Entire hour missing
        for m in range(60):
            yield origin + timedelta(minutes=m)
        return

    if ticks.empty:
        for m in range(60):
            yield origin + timedelta(minutes=m)
        return

    bars = aggregate_ticks_to_1m(ticks)
    present = set(pd.to_datetime(bars["datetime"], utc=True).to_pydatetime())
    for m in range(60):
        ts = origin + timedelta(minutes=m)
        if ts not in present:
            yield ts


def iter_missing_minutes(download_root: Path, asset: str, start: datetime, end: datetime) -> Generator[datetime, None, None]:
    """Yield all missing minute timestamps (UTC) between start and end (inclusive)."""
    cur = start.replace(minute=0, second=0, microsecond=0)
    while cur <= end:
        for ts in _iter_missing_minutes_for_hour(download_root, asset, cur.replace(tzinfo=timezone.utc)):
            yield ts
        cur += timedelta(hours=1)


def iter_missing_hours(download_root: Path, asset: str, start: datetime, end: datetime) -> Generator[datetime, None, None]:
    """Yield origin timestamps (UTC) for any missing/empty hour files."""
    for origin, path in iter_hour_paths(download_root, asset, start, end):
        missing = False
        if not path.exists() or (path.exists() and path.stat().st_size == 0):
            missing = True
        else:
            try:
                _ = decode_hour_file(path, origin.replace(tzinfo=timezone.utc))
            except Exception:
                missing = True
        if missing:
            yield origin.replace(tzinfo=timezone.utc)


def write_gaps_logs(download_root: Path, asset: str, start: datetime, end: datetime, gaps_root: Path) -> Tuple[int, int]:
    """Write CSV logs of missing minutes and hours under gaps_root/asset=<ASSET>/.

    Returns: (missing_minutes_count, missing_hours_count)
    """
    gaps_dir = Path(gaps_root) / f"asset={asset}"
    gaps_dir.mkdir(parents=True, exist_ok=True)

    minutes_path = gaps_dir / "missing_minutes.csv"
    hours_path = gaps_dir / "missing_hours.csv"

    # Append to existing logs to accumulate over multiple runs
    min_written = 0
    hr_written = 0

    # Hours first (smaller set)
    with hours_path.open("a", encoding="utf-8") as fhrs:
        # Write header if new file
        if hours_path.stat().st_size == 0:
            fhrs.write("datetime\n")
        for ts in iter_missing_hours(download_root, asset, start, end):
            fhrs.write(f"{ts.isoformat()}\n")
            hr_written += 1

    with minutes_path.open("a", encoding="utf-8") as fmin:
        if minutes_path.stat().st_size == 0:
            fmin.write("datetime\n")
        for ts in iter_missing_minutes(download_root, asset, start, end):
            fmin.write(f"{ts.isoformat()}\n")
            min_written += 1

    return min_written, hr_written
