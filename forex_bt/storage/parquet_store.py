from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

LOG = logging.getLogger(__name__)


PARQUET_COLUMNS = ["datetime", "Open", "High", "Low", "Close", "Volume"]


def parquet_partition_path(root: Path, asset: str, tf: str, year: int, month: int) -> Path:
    root = Path(root)
    return root / f"asset={asset}" / f"tf={tf}" / f"year={year:04d}" / f"month={month:02d}" / "bars.parquet"


def _ensure_dt_index(df: pd.DataFrame) -> pd.DataFrame:
    if "datetime" in df.columns:
        dt = pd.to_datetime(df["datetime"], utc=True)
    else:
        dt = pd.to_datetime(df.index, utc=True)
    df = df.copy()
    df["datetime"] = dt
    return df


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = _ensure_dt_index(df)
    missing = [c for c in PARQUET_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for parquet store: {missing}")
    return df[PARQUET_COLUMNS].sort_values("datetime")


def parquet_get_last_timestamp(root: Path, asset: str, tf: str) -> Optional[datetime]:
    root = Path(root)
    asset_dir = root / f"asset={asset}" / f"tf={tf}"
    if not asset_dir.exists():
        return None

    years = sorted(p for p in asset_dir.iterdir() if p.is_dir() and p.name.startswith("year="))
    if not years:
        return None

    last_year = years[-1]
    months = sorted(p for p in last_year.iterdir() if p.is_dir() and p.name.startswith("month="))
    if not months:
        return None

    last_month = months[-1]
    parquet_path = last_month / "bars.parquet"
    if not parquet_path.exists():
        return None

    df = pd.read_parquet(parquet_path)
    if df.empty:
        return None

    return pd.to_datetime(df["datetime"], utc=True).max().to_pydatetime()


def parquet_upsert(root: Path, asset: str, tf: str, df_new: pd.DataFrame) -> int:
    root = Path(root)
    df_new = _normalize_columns(df_new)
    df_new["datetime"] = pd.to_datetime(df_new["datetime"], utc=True)

    total_written = 0
    for (year, month), df_part in df_new.groupby([df_new["datetime"].dt.year, df_new["datetime"].dt.month]):
        part_path = parquet_partition_path(root, asset, tf, int(year), int(month))
        part_path.parent.mkdir(parents=True, exist_ok=True)

        if part_path.exists():
            existing = pd.read_parquet(part_path)
        else:
            existing = pd.DataFrame(columns=PARQUET_COLUMNS)

        if not existing.empty:
            existing["datetime"] = pd.to_datetime(existing["datetime"], utc=True)

        combined = pd.concat([existing, df_part], ignore_index=True)
        combined = combined.drop_duplicates(subset=["datetime"], keep="last")
        combined = combined.sort_values("datetime")

        combined.to_parquet(part_path, index=False)
        total_written += len(combined)
        LOG.debug("Upserted %s rows into %s", len(combined), part_path)

    return total_written


def parquet_load_range(
    root: Path,
    asset: str,
    tf: str,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> pd.DataFrame:
    root = Path(root)
    start_dt = pd.to_datetime(start, utc=True) if start else None
    end_dt = pd.to_datetime(end, utc=True) if end else None

    if start_dt and end_dt and start_dt > end_dt:
        raise ValueError("start must be <= end")

    # Determine range of months
    if start_dt:
        cur = start_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        cur = None

    parts = []
    base_dir = root / f"asset={asset}" / f"tf={tf}"
    if not base_dir.exists():
        return pd.DataFrame(columns=PARQUET_COLUMNS).set_index("datetime")

    months = []
    if cur:
        while True:
            months.append((cur.year, cur.month))
            next_month = (cur + pd.offsets.MonthBegin()).replace(day=1)
            if end_dt and next_month > end_dt:
                break
            cur = next_month
            if not end_dt and len(months) > 2400:  # safety
                break
    else:
        for year_dir in sorted(base_dir.glob("year=*")):
            year = int(year_dir.name.split("=")[1])
            for month_dir in sorted(year_dir.glob("month=*")):
                month = int(month_dir.name.split("=")[1])
                months.append((year, month))

    for year, month in months:
        part_path = parquet_partition_path(root, asset, tf, year, month)
        if part_path.exists():
            parts.append(pd.read_parquet(part_path))

    if not parts:
        return pd.DataFrame(columns=PARQUET_COLUMNS).set_index("datetime")

    df = pd.concat(parts, ignore_index=True)
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)

    if start_dt:
        df = df[df["datetime"] >= start_dt]
    if end_dt:
        df = df[df["datetime"] <= end_dt]

    df = df.sort_values("datetime")
    df = df.drop_duplicates(subset=["datetime"], keep="last")
    df = df.set_index("datetime")
    return df
