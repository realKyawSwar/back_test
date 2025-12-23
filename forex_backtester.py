#!/usr/bin/env python3
"""
Thin wrapper entrypoint for the forex_bt package CLI.

Run commands exactly as before:
    python forex_backtester.py download-data ...
    python forex_backtester.py resample-and-store ...
    python forex_backtester.py backtest-strategy ...
"""
from forex_bt.cli import app


if __name__ == "__main__":
    app()
