from __future__ import annotations

from backtesting import Strategy
from backtesting.lib import crossover
import pandas as pd


class SMACross(Strategy):
    fast: int = 20
    slow: int = 50

    def init(self):  # type: ignore[override]
        price = self.data.Close
        self.sma_fast = self.I(lambda s: pd.Series(s).rolling(self.fast).mean(), price)
        self.sma_slow = self.I(lambda s: pd.Series(s).rolling(self.slow).mean(), price)

    def next(self):  # type: ignore[override]
        if crossover(self.sma_fast, self.sma_slow):
            self.position.close()
            self.buy()
        elif crossover(self.sma_slow, self.sma_fast):
            self.position.close()
            self.sell()
