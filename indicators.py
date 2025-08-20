import numpy as np
import pandas as pd

class Indicators:
    @staticmethod
    def ema(series: pd.Series, period: int) -> pd.Series:
        return series.ewm(span=period, adjust=False).mean()

    @staticmethod
    def atr(df: pd.DataFrame, period: int = 14, method: str = "wilder") -> pd.Series:
        # df: columns ['open','high','low','close']
        high = df['high'].astype(float)
        low = df['low'].astype(float)
        close = df['close'].astype(float)

        prev_close = close.shift(1)
        tr = pd.concat([
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs()
        ], axis=1).max(axis=1)

        if method.lower().startswith("wilder"):
            # Wilder smoothing == RMA
            atr = tr.ewm(alpha=1/period, adjust=False).mean()
        else:
            atr = tr.rolling(period).mean()
        return atr

    @staticmethod
    def swing_extreme(df: pd.DataFrame, side: str, lookback: int = 5):
        """Return recent swing low/high index within lookback window."""
        if side == "long":
            idx = df['low'].tail(lookback + 1).idxmin()
        else:
            idx = df['high'].tail(lookback + 1).idxmax()
        return idx
