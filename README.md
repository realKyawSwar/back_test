# Forex Parquet Backtester

Parquet-first, modular Forex backtesting pipeline built around Dukascopy tick data.

## Highlights
- **Partitioned Parquet storage** under `data_parquet/asset=<ASSET>/tf=<TF>/year=YYYY/month=MM/bars.parquet` for very fast selective reads.
- **Streaming tick → 1m aggregation** (hour-by-hour) to keep memory usage low and speed up incremental refreshes.
- **Typer CLI** for downloads, resampling, and running sample strategies.
- **backtesting.py integration** with an example SMA crossover strategy.

## Installation
1. Clone the repo and move into it:
   ```
   git clone <repo-url>
   cd back_test
   ```

2. Create and activate a Python environment (recommended):
   ```
   python -m venv .venv
   source .venv/bin/activate
   ```

3. Install the Python dependencies from `requirements.txt`:
   ```
   pip install -r requirements.txt
   ```

## Running the CLI
All commands are exposed via `forex_backtester.py`, which delegates to the Typer app in `forex_bt.cli`. Use `--help` to explore subcommands:

```
python forex_backtester.py --help
```

### Workflow
1) Download Dukascopy ticks (requires `dukascopy-data-manager.py` in the repo):
```
python forex_backtester.py download-data EURUSD --start 2023-01-01 --duka-script dukascopy-data-manager.py --download-path download
```

2) Build 1m bars and resample to higher timeframes (stored directly into partitioned Parquet):
```
python forex_backtester.py resample-and-store EURUSD --start 2023-01-01 --timeframes 1m 1h 1D --download-path download --parquet-root data_parquet
```

If you already have 1m bars and only want to convert minute → hour (without reading tick hour files), use `--skip-1m`:
```
python forex_backtester.py resample-and-store EURUSD --start 2023-01-01 --timeframes 1h 1D --parquet-root data_parquet --skip-1m
```

When building 1m from Dukascopy ticks, you can automatically log missing timeframes (for later backfill from another source) by keeping `--log-missing` enabled (default) and specifying the date range:
```
python forex_backtester.py resample-and-store EURUSD --start 2023-01-01 --timeframes 1m 1h --download-path download --parquet-root data_parquet --log-missing --gaps-root gaps
```
This will append CSVs:
- `gaps/asset=EURUSD/missing_hours.csv` — hour files missing or undecodable
- `gaps/asset=EURUSD/missing_minutes.csv` — exact 1m timestamps missing (UTC)

3) Run the example SMA crossover backtest using Parquet data:
```
python forex_backtester.py backtest-strategy EURUSD --timeframe 1h --start 2023-02-01 --end 2023-03-01 --fast 20 --slow 50 --parquet-root data_parquet
```

### Additional tips
- The downloaded tick CSVs in `--download-path` are read incrementally; you can rerun resampling to refresh only the latest partitions.
- Use the `--timeframes` flag on `resample-and-store` to control which bars are produced from the 1m base bars.
- Pass `--parquet-root` to point at an existing Parquet store if you already have one; the layout is shown below.
 - Use `--skip-1m` to avoid reading tick hour files and resample from existing 1m data only.

## Storage layout
Bars are partitioned for efficient selective reading:
```
data_parquet/
  asset=EURUSD/
    tf=1m/
      year=2023/
        month=01/
          bars.parquet
        month=02/
          bars.parquet
    tf=1h/
      ...
```
Each Parquet file stores columns: `datetime` (UTC), `Open`, `High`, `Low`, `Close`, `Volume`. Timestamps are de-duplicated and sorted, and only affected partitions are rewritten during incremental updates (typically the current month).

## Notes
- All timestamps are handled as UTC internally. backtesting.py uses tz-naive indexes so the loader drops timezone information after selection.
- Missing tick hours are skipped with warnings so long downloads continue uninterrupted.
- Missing timeframes are logged under `--gaps-root` when `--log-missing` is enabled.
- The original `forex_backtester.py` remains the entrypoint and simply delegates to the new `forex_bt.cli` Typer app.
