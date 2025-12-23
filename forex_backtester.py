#!/usr/bin/env python3
"""
forex_backtester.py
A complete Dukascopy (tick) -> OHLCV (multi-timeframe) -> SQLite -> backtesting.py workflow.

It *integrates* the provided `dukascopy-data-manager.py` by importing it dynamically and reusing its
download/update logic + tick decoding conventions (BI5/LZMA, BIDP/BIDV, TIME origin per hour file). :contentReference[oaicite:0]{index=0}

------------------------------------------------------------------------------
What this script does
------------------------------------------------------------------------------
1) Download / update Dukascopy tick data (hourly .bi5 files) for selected FX assets.
2) Build 1-minute OHLCV bars incrementally from tick files (streamed, low memory).
3) Resample 1m -> 5m, 15m, 30m, 1h, 4h, 1D and store each timeframe in SQLite tables:
      {ASSET}_{TF}  e.g., EURUSD_1h
   Columns: datetime (UTC), Open, High, Low, Close, Volume
4) Load data from SQLite into a pandas DataFrame compatible with backtesting.py.
5) Run an example SMA crossover backtest on a chosen asset/timeframe and date range.

------------------------------------------------------------------------------
Dependencies (assumed installed, do NOT pip install in code)
------------------------------------------------------------------------------
typer, rich, requests, concurrent.futures, pathlib, datetime, lzma, numpy, pandas, sqlite3 (built-in), backtesting

------------------------------------------------------------------------------
Example usage
------------------------------------------------------------------------------
# 1) Download EURUSD from 2020-01-01 to today (UTC)
python forex_backtester.py download-data EURUSD --start 2020-01-01

# 2) Build & store OHLCV for 1h and 1D (1m is built automatically as the base)
python forex_backtester.py resample-and-store EURUSD --start 2020-01-01 --timeframes 1h 1D

# 3) Run a SMA(20/50) crossover backtest on EURUSD 1h during 2022
python forex_backtester.py backtest-strategy EURUSD --timeframe 1h --start 2022-01-01 --end 2022-12-31 --fast 20 --slow 50

------------------------------------------------------------------------------
Notes
------------------------------------------------------------------------------
- Dukascopy tick timestamps are constructed from (hour file origin + TIME ms offset).
- This script treats all timestamps as UTC and stores ISO-like strings in SQLite.
- Incremental updates:
    * For 1m, we skip any hour files entirely before the last stored 1m bar.
    * Inserts are "INSERT OR REPLACE" keyed by datetime to avoid duplicates.
- If you want custom download locations, pass --download-path / --export-path
  and we will keep the imported dukascopy module in sync.

------------------------------------------------------------------------------
"""

from __future__ import annotations

import importlib.util
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import lzma
import numpy as np
import pandas as pd
import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.table import Table

from backtesting import Backtest, Strategy
from backtesting.lib import crossover


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

app = typer.Typer(add_completion=False)
console = Console()

LOG = logging.getLogger("forex_backtester")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)


# -----------------------------------------------------------------------------
# Timeframes
# -----------------------------------------------------------------------------

# Supported targets (as requested)
SUPPORTED_TFS = ["1m", "5m", "15m", "30m", "1h", "4h", "1D"]

TF_TO_PANDAS = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1H",
    "4h": "4H",
    "1D": "1D",
}


# -----------------------------------------------------------------------------
# Dukascopy integration (dynamic import of the provided script)
# -----------------------------------------------------------------------------

def load_dukas_module(duka_script_path: Path):
    """
    Dynamically import the user's provided `dukascopy-data-manager.py`.

    We rely on:
      - download(...)
      - update(...)
      - DOWNLOAD_PATH / EXPORT_PATH globals (we override to match CLI if requested)
    """
    if not duka_script_path.is_file():
        raise FileNotFoundError(f"Cannot find dukascopy script at: {duka_script_path}")

    spec = importlib.util.spec_from_file_location("dukascopy_data_manager", str(duka_script_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Failed to import: {duka_script_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return module


# -----------------------------------------------------------------------------
# SQLite helpers
# -----------------------------------------------------------------------------

def sqlite_connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn


def ensure_table(conn: sqlite3.Connection, table: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "{table}" (
            datetime TEXT PRIMARY KEY,
            Open REAL NOT NULL,
            High REAL NOT NULL,
            Low  REAL NOT NULL,
            Close REAL NOT NULL,
            Volume REAL NOT NULL
        );
        """
    )
    conn.commit()


def get_max_datetime(conn: sqlite3.Connection, table: str) -> Optional[datetime]:
    ensure_table(conn, table)
    cur = conn.execute(f'SELECT MAX(datetime) FROM "{table}";')
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    # stored as ISO-ish string: "YYYY-MM-DD HH:MM:SS" (naive UTC)
    return datetime.fromisoformat(row[0])


def upsert_ohlcv(conn: sqlite3.Connection, table: str, df: pd.DataFrame) -> int:
    """
    Upsert rows by datetime primary key.
    Expects df index = datetime-like, columns = Open/High/Low/Close/Volume
    """
    if df.empty:
        return 0

    ensure_table(conn, table)

    out = df.copy()
    out = out[["Open", "High", "Low", "Close", "Volume"]]

    # Use naive UTC string for SQLite key
    idx = pd.to_datetime(out.index, utc=True).tz_convert("UTC").tz_localize(None)
    out.index = idx
    out.reset_index(inplace=True)
    out.rename(columns={"index": "datetime"}, inplace=True)
    out["datetime"] = out["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

    rows = list(out.itertuples(index=False, name=None))
    conn.executemany(
        f'INSERT OR REPLACE INTO "{table}" (datetime, Open, High, Low, Close, Volume) VALUES (?, ?, ?, ?, ?, ?);',
        rows,
    )
    conn.commit()
    return len(rows)


def load_ohlcv(
    conn: sqlite3.Connection,
    table: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load OHLCV for backtesting.py.
    Returns DataFrame with datetime index and columns: Open, High, Low, Close, Volume
    """
    ensure_table(conn, table)

    where = []
    params: List[str] = []
    if start:
        where.append("datetime >= ?")
        params.append(f"{start} 00:00:00" if len(start) == 10 else start)
    if end:
        where.append("datetime <= ?")
        params.append(f"{end} 23:59:59" if len(end) == 10 else end)

    sql = f'SELECT datetime, Open, High, Low, Close, Volume FROM "{table}"'
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY datetime ASC;"

    df = pd.read_sql_query(sql, conn, params=params, parse_dates=["datetime"])
    if df.empty:
        return df

    df = df.set_index("datetime")
    # backtesting.py expects columns with these exact names:
    df = df[["Open", "High", "Low", "Close", "Volume"]]
    return df


# -----------------------------------------------------------------------------
# Dukascopy tick file decoding + streamed 1m aggregation
# -----------------------------------------------------------------------------

DT_TICK = np.dtype(
    [
        ("TIME", ">i4"),   # ms offset from hour origin
        ("ASKP", ">i4"),
        ("BIDP", ">i4"),
        ("ASKV", ">f4"),
        ("BIDV", ">f4"),
    ]
)


def iter_hour_files(download_root: Path, asset: str, start_dt: datetime, end_dt: datetime) -> Iterable[Tuple[Path, datetime]]:
    """
    Yield (path, hour_origin_datetime_utc_naive) for each expected hour in range.
    Dukascopy folder layout (as in provided script):
      download/{ASSET}/{YYYY}/{MM-1}/{DD}/{HH}h_ticks.bi5
    where month is zero-based in URL/path naming.
    """
    cur = start_dt.replace(minute=0, second=0, microsecond=0)
    end = end_dt.replace(minute=0, second=0, microsecond=0)

    while cur <= end:
        year = cur.year
        month0 = cur.month - 1
        day = cur.day
        hour = cur.hour
        f = download_root / asset / f"{year}" / f"{month0:0>2}" / f"{day:0>2}" / f"{hour:0>2}h_ticks.bi5"
        yield f, datetime(year, cur.month, day, hour)
        cur += timedelta(hours=1)


def read_ticks_from_bi5(file_path: Path, hour_origin: datetime) -> pd.DataFrame:
    """
    Decode one BI5 tick file into a DataFrame with a UTC datetime index (naive UTC).
    Uses BID price as the traded price, BIDV as volume (matching provided script). :contentReference[oaicite:1]{index=1}
    """
    if not file_path.is_file() or file_path.stat().st_size == 0:
        return pd.DataFrame()

    try:
        raw = lzma.open(file_path, mode="rb").read()
        data = np.frombuffer(raw, DT_TICK)
        if data.size == 0:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        # TIME is ms offset from hour origin (naive)
        df["TIME"] = pd.to_datetime(df["TIME"], unit="ms", origin=hour_origin)
        # scale prices (as in provided script) :contentReference[oaicite:2]{index=2}
        df["BIDP"] = df["BIDP"].astype(np.int32) / 100_000.0
        df["BIDV"] = df["BIDV"].astype(np.float32)

        df = df[["TIME", "BIDP", "BIDV"]].rename(columns={"TIME": "datetime", "BIDP": "price", "BIDV": "volume"})
        df = df.set_index("datetime")
        # Ensure sorted
        df = df.sort_index()
        # Deduplicate any same-timestamp records by keeping last
        df = df[~df.index.duplicated(keep="last")]
        return df
    except Exception as e:
        LOG.exception("Failed to read ticks from %s: %s", file_path, e)
        return pd.DataFrame()


def ticks_to_1m_ohlcv(ticks: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate ticks -> 1-minute OHLCV.
    ticks: index=datetime, columns: price, volume
    """
    if ticks.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])

    ohlc = ticks["price"].resample("1min").ohlc()
    vol = ticks["volume"].resample("1min").sum()
    out = ohlc.join(vol.rename("Volume"))

    out.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close"}, inplace=True)
    out.dropna(inplace=True)
    return out


def resample_from_1m(df_1m: pd.DataFrame, tf: str) -> pd.DataFrame:
    """
    Resample 1m OHLCV -> higher timeframe.
    """
    if df_1m.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])

    freq = TF_TO_PANDAS[tf]
    agg = {
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
        "Volume": "sum",
    }
    out = df_1m.resample(freq).agg(agg)
    out.dropna(inplace=True)
    return out


# -----------------------------------------------------------------------------
# Backtesting.py example strategy
# -----------------------------------------------------------------------------

class SMACross(Strategy):
    fast: int = 20
    slow: int = 50

    def init(self):
        close = self.data.Close
        # Using pandas rolling mean via backtesting.py built-in helper
        self.sma_fast = self.I(lambda x: pd.Series(x).rolling(self.fast).mean().values, close)
        self.sma_slow = self.I(lambda x: pd.Series(x).rolling(self.slow).mean().values, close)

    def next(self):
        if crossover(self.sma_fast, self.sma_slow):
            self.position.close()
            self.buy()
        elif crossover(self.sma_slow, self.sma_fast):
            self.position.close()
            self.sell()


# -----------------------------------------------------------------------------
# CLI Commands
# -----------------------------------------------------------------------------

@app.command("download-data")
def download_data(
    assets: List[str] = typer.Argument(..., help="Assets to download, e.g. EURUSD AUDUSD"),
    start: str = typer.Option(..., help="Start date YYYY-MM-DD"),
    end: str = typer.Option("", help="Optional end date YYYY-MM-DD (default: now UTC)"),
    update: bool = typer.Option(False, help="Use dukascopy update command instead of download"),
    concurrent: int = typer.Option(0, help="Max concurrent downloads (passed through)"),
    force: bool = typer.Option(False, help="Force redownload (passed through)"),
    duka_script: Path = typer.Option(Path("dukascopy-data-manager.py"), help="Path to dukascopy-data-manager.py"),
    download_path: Path = typer.Option(Path("./download/"), help="Download root folder"),
    export_path: Path = typer.Option(Path("./export/"), help="Export folder (not required, but kept consistent)"),
):
    """
    Download or update Dukascopy tick files using the provided dukascopy-data-manager.py. :contentReference[oaicite:3]{index=3}
    """
    duka = load_dukas_module(duka_script)

    # keep paths consistent with this wrapper
    duka.DOWNLOAD_PATH = str(download_path.as_posix().rstrip("/") + "/")
    duka.EXPORT_PATH = str(export_path.as_posix().rstrip("/") + "/")

    if update:
        # dukascopy update expects assets list + optional start override
        duka.update(assets, start="", concurrent=concurrent, force=force) if start == "" else duka.update(
            assets, start=start, concurrent=concurrent, force=force
        )
    else:
        duka.download(assets, start=start, end=end, concurrent=concurrent, force=force)

    console.print("[green]Download/update completed.[/green]")
    console.print(f"Download folder: {download_path.resolve()}")


@app.command("resample-and-store")
def resample_and_store(
    assets: List[str] = typer.Argument(..., help="Assets to process, e.g. EURUSD AUDUSD"),
    start: str = typer.Option(..., help="Start date YYYY-MM-DD for processing"),
    end: str = typer.Option("", help="Optional end date YYYY-MM-DD (default: now UTC)"),
    timeframes: List[str] = typer.Option(["1h", "1D"], help="Target timeframes, e.g. --timeframes 1m 1h 1D"),
    db_path: Path = typer.Option(Path("forex_data.db"), help="SQLite DB path"),
    download_path: Path = typer.Option(Path("./download/"), help="Download root folder"),
):
    """
    Stream tick files -> build 1m bars -> resample -> store into SQLite incrementally.
    """
    # validate timeframes
    for tf in timeframes:
        if tf not in SUPPORTED_TFS:
            raise typer.BadParameter(f"Unsupported timeframe '{tf}'. Supported: {SUPPORTED_TFS}")

    # Always build 1m as the base if any higher timeframe requested
    want_1m = "1m" in timeframes or any(tf != "1m" for tf in timeframes)
    target_tfs = sorted(set(timeframes))
    if want_1m and "1m" not in target_tfs:
        target_tfs = ["1m"] + target_tfs

    start_dt = datetime.fromisoformat(start)
    if end:
        end_dt = datetime.fromisoformat(end)
    else:
        end_dt = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=1)

    conn = sqlite_connect(db_path)

    for asset in assets:
        table_1m = f"{asset}_1m"
        ensure_table(conn, table_1m)
        max_1m = get_max_datetime(conn, table_1m)

        # We will skip hour files completely before the last stored 1m bar's hour
        skip_before_hour: Optional[datetime] = None
        if max_1m:
            skip_before_hour = max_1m.replace(minute=0, second=0, microsecond=0)

        hour_files = list(iter_hour_files(download_path, asset, start_dt, end_dt))
        if not hour_files:
            console.print(f"[yellow]{asset}: no hour files in range.[/yellow]")
            continue

        console.rule(f"[bold]Processing {asset}[/bold]")

        rows_written_1m = 0
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console,
        )

        with progress:
            task = progress.add_task(f"Ticks -> 1m -> DB ({asset})", total=len(hour_files))

            for file_path, hour_origin in hour_files:
                progress.advance(task)

                if skip_before_hour and hour_origin < skip_before_hour:
                    continue

                ticks = read_ticks_from_bi5(file_path, hour_origin)
                if ticks.empty:
                    continue

                df_1m = ticks_to_1m_ohlcv(ticks)
                if df_1m.empty:
                    continue

                # Avoid re-writing everything: if max_1m exists, only keep strictly newer bars
                if max_1m:
                    df_1m = df_1m[df_1m.index > max_1m]
                    if df_1m.empty:
                        continue

                rows_written_1m += upsert_ohlcv(conn, table_1m, df_1m)
                if not df_1m.empty:
                    max_1m = df_1m.index.max().to_pydatetime()

        console.print(f"[green]{asset}: wrote {rows_written_1m:,} rows into {table_1m}[/green]")

        # Resample 1m -> higher tfs
        higher_tfs = [tf for tf in target_tfs if tf != "1m"]
        if not higher_tfs:
            continue

        # Load 1m data in chunks to reduce memory: by month
        # Determine range from stored 1m
        min_1m = None
        cur = conn.execute(f'SELECT MIN(datetime), MAX(datetime) FROM "{table_1m}";').fetchone()
        if cur and cur[0] and cur[1]:
            min_1m = datetime.fromisoformat(cur[0])
            max_1m_all = datetime.fromisoformat(cur[1])
        else:
            console.print(f"[yellow]{asset}: no 1m data found for resampling.[/yellow]")
            continue

        # For each target tf, track last stored datetime to do incremental resample
        max_by_tf: Dict[str, Optional[datetime]] = {}
        for tf in higher_tfs:
            table = f"{asset}_{tf}"
            ensure_table(conn, table)
            max_by_tf[tf] = get_max_datetime(conn, table)

        # Iterate month-by-month over 1m data
        month_start = min_1m.replace(day=1, hour=0, minute=0, second=0, microsecond=0) if min_1m else start_dt
        month_end = max_1m_all

        def next_month(d: datetime) -> datetime:
            # advance to first day of next month
            if d.month == 12:
                return d.replace(year=d.year + 1, month=1, day=1)
            return d.replace(month=d.month + 1, day=1)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console,
        ) as p2:
            # approximate total months
            months = 0
            tmp = month_start
            while tmp <= month_end:
                months += 1
                tmp = next_month(tmp)

            t2 = p2.add_task(f"Resampling 1m -> {', '.join(higher_tfs)} ({asset})", total=months)

            cur_month = month_start
            while cur_month <= month_end:
                next_m = next_month(cur_month)
                chunk_start = cur_month.strftime("%Y-%m-%d %H:%M:%S")
                chunk_end = (next_m - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")

                df_1m_chunk = load_ohlcv(conn, table_1m, start=chunk_start, end=chunk_end)
                if not df_1m_chunk.empty:
                    # ensure UTC-like, though stored naive
                    df_1m_chunk.index = pd.to_datetime(df_1m_chunk.index, utc=True)

                    for tf in higher_tfs:
                        table_tf = f"{asset}_{tf}"
                        df_tf = resample_from_1m(df_1m_chunk, tf)

                        # only write new bars beyond current max for that tf
                        max_tf = max_by_tf[tf]
                        if max_tf:
                            df_tf = df_tf[df_tf.index.tz_convert("UTC").tz_localize(None) > max_tf]
                        if df_tf.empty:
                            continue

                        wrote = upsert_ohlcv(conn, table_tf, df_tf)
                        max_by_tf[tf] = df_tf.index.tz_convert("UTC").tz_localize(None).max().to_pydatetime()
                        LOG.info("%s: wrote %s rows into %s", asset, wrote, table_tf)

                p2.advance(t2)
                cur_month = next_m

        # Summary table
        tbl = Table(title=f"{asset} SQLite tables updated")
        tbl.add_column("Table")
        tbl.add_column("Rows", justify="right")
        for tf in target_tfs:
            table = f"{asset}_{tf}"
            ensure_table(conn, table)
            n = conn.execute(f'SELECT COUNT(*) FROM "{table}";').fetchone()[0]
            tbl.add_row(table, f"{n:,}")
        console.print(tbl)

    conn.close()
    console.print(f"[bold green]Done. DB at:[/bold green] {db_path.resolve()}")


@app.command("backtest-strategy")
def backtest_strategy(
    asset: str = typer.Argument(..., help="Asset symbol, e.g. EURUSD"),
    timeframe: str = typer.Option("1h", help="Timeframe, e.g. 1m, 1h, 1D"),
    start: str = typer.Option("", help="Start date YYYY-MM-DD (optional)"),
    end: str = typer.Option("", help="End date YYYY-MM-DD (optional)"),
    fast: int = typer.Option(20, help="Fast SMA period"),
    slow: int = typer.Option(50, help="Slow SMA period"),
    cash: float = typer.Option(10_000.0, help="Initial cash"),
    commission: float = typer.Option(0.0, help="Commission fraction (e.g. 0.0002)"),
    db_path: Path = typer.Option(Path("forex_data.db"), help="SQLite DB path"),
):
    """
    Load OHLCV from SQLite and run a simple SMA crossover backtest using backtesting.py.
    """
    if timeframe not in SUPPORTED_TFS:
        raise typer.BadParameter(f"Unsupported timeframe '{timeframe}'. Supported: {SUPPORTED_TFS}")

    table = f"{asset}_{timeframe}"
    conn = sqlite_connect(db_path)

    df = load_ohlcv(conn, table, start=start or None, end=end or None)
    conn.close()

    if df.empty:
        console.print(f"[red]No data found in table {table} for the requested range.[/red]")
        raise typer.Exit(code=1)

    # backtesting.py wants datetime index (can be tz-naive) and OHLCV columns
    df.index = pd.to_datetime(df.index).tz_localize(None)

    # Configure strategy parameters
    SMACross.fast = fast
    SMACross.slow = slow

    bt = Backtest(df, SMACross, cash=cash, commission=commission, exclusive_orders=True)
    stats = bt.run()

    # Print key stats
    console.rule("[bold]Backtest Results[/bold]")
    key_rows = [
        ("Start", str(df.index.min())),
        ("End", str(df.index.max())),
        ("Bars", f"{len(df):,}"),
        ("Fast SMA", str(fast)),
        ("Slow SMA", str(slow)),
        ("Equity Final [$]", f"{stats.get('Equity Final [$]', float('nan')):,.2f}"),
        ("Return [%]", f"{stats.get('Return [%]', float('nan')):,.2f}"),
        ("Win Rate [%]", f"{stats.get('Win Rate [%]', float('nan')):,.2f}"),
        ("# Trades", str(stats.get('# Trades', ''))),
        ("Max Drawdown [%]", f"{stats.get('Max. Drawdown [%]', float('nan')):,.2f}"),
        ("Sharpe Ratio", f"{stats.get('Sharpe Ratio', float('nan')):.3f}"),
    ]
    t = Table(show_header=False)
    t.add_column("Metric")
    t.add_column("Value")
    for k, v in key_rows:
        t.add_row(k, v)
    console.print(t)

    # Optional: open interactive plot in browser (requires matplotlib)
    # bt.plot()


# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    app()
