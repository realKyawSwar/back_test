from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import pandas as pd
import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.table import Table

from .dukascopy_integration import load_dukas_module
from .ohlcv import SUPPORTED_TFS, resample_and_store, upsert_ohlcv_from_minutes
from .storage.parquet_store import parquet_get_last_timestamp
from .ticks import stream_aggregate_1m
from .backtests.runner import run_sma_cross

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")

app = typer.Typer(add_completion=False)
console = Console()


def _parse_date(val: str) -> datetime:
    return datetime.strptime(val, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _chunked_months(start: datetime, end: datetime) -> Iterable[tuple[datetime, datetime]]:
    cur = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cur <= end:
        next_month = (cur + pd.offsets.MonthBegin()).replace(day=1)
        yield cur, min(end, next_month - timedelta(seconds=1))
        cur = next_month


@app.command("download-data")
def download_data(
    assets: List[str] = typer.Argument(..., help="Asset symbols"),
    start: str = typer.Option(..., help="Start date YYYY-MM-DD"),
    end: Optional[str] = typer.Option(None, help="End date YYYY-MM-DD"),
    update: bool = typer.Option(False, help="Use module.update instead of download"),
    duka_script: Path = typer.Option(Path("dukascopy-data-manager.py"), help="Path to dukascopy script"),
    download_path: Path = typer.Option(Path("download"), help="Download directory"),
):
    start_dt = _parse_date(start)
    end_dt = _parse_date(end) if end else datetime.utcnow().replace(tzinfo=timezone.utc)

    duka = load_dukas_module(duka_script)
    duka.download_path = download_path

    console.print(f"[bold]Using dukascopy script:[/bold] {duka_script}")
    for asset in assets:
        console.print(f"[cyan]Fetching {asset} from {start_dt.date()} to {end_dt.date()}[/cyan]")
        if update:
            duka.update(asset, start_dt, end_dt)  # type: ignore[arg-type]
        else:
            duka.download(asset, start_dt, end_dt)  # type: ignore[arg-type]


@app.command("resample-and-store")
def resample_and_store_cmd(
    assets: List[str] = typer.Argument(..., help="Assets to process"),
    start: str = typer.Option(..., help="Start date YYYY-MM-DD"),
    end: Optional[str] = typer.Option(None, help="End date YYYY-MM-DD"),
    timeframes: List[str] = typer.Option(..., help="Target timeframes (include 1m if desired)"),
    download_path: Path = typer.Option(Path("download"), help="Tick download root"),
    parquet_root: Path = typer.Option(Path("data_parquet"), help="Parquet root"),
):
    for tf in timeframes:
        if tf not in SUPPORTED_TFS:
            raise typer.BadParameter(f"Unsupported timeframe {tf}")

    start_dt = _parse_date(start)
    end_dt = _parse_date(end) if end else datetime.utcnow().replace(tzinfo=timezone.utc)

    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        for asset in assets:
            console.rule(f"[bold blue]{asset}[/bold blue]")

            last_1m = parquet_get_last_timestamp(parquet_root, asset, "1m")
            hour_start = start_dt
            if last_1m:
                hour_start = max(hour_start, last_1m.replace(minute=0, second=0, microsecond=0))

            t1 = progress.add_task(f"Building 1m for {asset}")
            df_stream = stream_aggregate_1m(download_path, asset, hour_start, end_dt)
            wrote_1m = upsert_ohlcv_from_minutes(parquet_root, asset, df_stream)
            progress.update(t1, completed=1)
            console.print(f"[green]1m bars written:[/green] {wrote_1m}")

            higher_tfs = [tf for tf in timeframes if tf != "1m"]
            if not higher_tfs:
                continue

            month_start = start_dt.replace(day=1)
            month_end = end_dt
            total_months = sum(1 for _ in _chunked_months(month_start, month_end))
            t2 = progress.add_task(f"Resampling {asset}", total=total_months)
            for m_start, m_end in _chunked_months(month_start, month_end):
                resample_and_store(parquet_root, asset, higher_tfs, m_start, m_end)
                progress.advance(t2)


@app.command("backtest-strategy")
def backtest_strategy_cmd(
    asset: str = typer.Argument(..., help="Asset symbol"),
    timeframe: str = typer.Option("1h", help="Timeframe"),
    start: str = typer.Option("", help="Start date YYYY-MM-DD"),
    end: str = typer.Option("", help="End date YYYY-MM-DD"),
    fast: int = typer.Option(20, help="Fast SMA"),
    slow: int = typer.Option(50, help="Slow SMA"),
    cash: float = typer.Option(10_000.0, help="Initial cash"),
    commission: float = typer.Option(0.0, help="Commission fraction"),
    parquet_root: Path = typer.Option(Path("data_parquet"), help="Parquet root"),
):
    if timeframe not in SUPPORTED_TFS:
        raise typer.BadParameter(f"Unsupported timeframe {timeframe}")

    start_dt = _parse_date(start) if start else None
    end_dt = _parse_date(end) if end else None

    try:
        df, stats = run_sma_cross(parquet_root, asset, timeframe, start_dt, end_dt, fast, slow, cash, commission)
    except ValueError as exc:  # no data
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1)

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


if __name__ == "__main__":
    app()
