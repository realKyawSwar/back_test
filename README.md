# Forex Parquet Backtester

Parquet-first, modular Forex backtesting pipeline built around Dukascopy tick data.

## Highlights
- **Partitioned Parquet storage** under `data_parquet/asset=<ASSET>/tf=<TF>/year=YYYY/month=MM/bars.parquet` for very fast selective reads.
- **Streaming tick → 1m aggregation** (hour-by-hour) to keep memory usage low and speed up incremental refreshes.
- **Typer CLI** for downloads, resampling, and running sample strategies.
- **backtesting.py integration** with an example SMA crossover strategy.

## Requirements
Pre-install the following Python packages (no inline installers are used):

```
typer
rich
pandas
numpy
pyarrow
requests
backtesting
```

## Quickstart
1) Download Dukascopy ticks (requires `dukascopy-data-manager.py` in the repo):
```
python forex_backtester.py download-data EURUSD --start 2023-01-01 --duka-script dukascopy-data-manager.py --download-path download
```

2) Build 1m bars and resample to higher timeframes (stored directly into partitioned Parquet):
```
python forex_backtester.py resample-and-store EURUSD --start 2023-01-01 --timeframes 1m 1h 1D --download-path download --parquet-root data_parquet
```

3) Run the example SMA crossover backtest using Parquet data:
```
python forex_backtester.py backtest-strategy EURUSD --timeframe 1h --start 2023-02-01 --end 2023-03-01 --fast 20 --slow 50 --parquet-root data_parquet
```

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
- The original `forex_backtester.py` remains the entrypoint and simply delegates to the new `forex_bt.cli` Typer app.
