import pandas as pd
from indicators import Indicators
from helpers import slope

class Strategies:
    def __init__(self, data_fetcher):
        self.df = None
        self.ind = Indicators()
        self.data_fetcher = data_fetcher

    # --- подготовка и индикаторы ---
    def prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ожидает df с колонками ['ts','open','high','low','close','volume']"""
        df = df.copy().reset_index(drop=True)
        df['ema20'] = self.ind.ema(df['close'], 20)
        df['ema50'] = self.ind.ema(df['close'], 50)
        df['ema200'] = self.ind.ema(df['close'], 200)
        df['atr14'] = self.ind.atr(df[['open', 'high', 'low', 'close']], 14, method="wilder")
        return df

    # --- условия стратегии ---
    @staticmethod
    def _in_zone(latest_row, lo, hi):
        c = latest_row['close']
        low_wick = latest_row['low']
        return (lo <= c <= hi) or (low_wick <= hi and c >= lo)

    def trend_side(self, row_prev, row_latest):
        """Вернёт 'long' | 'short' | None на основе EMA200 + EMA20/50."""
        if pd.isna(row_latest['ema200']) or pd.isna(row_latest['ema50']):
            return None
        ema200_slope = row_latest['ema200'] - row_prev['ema200']

        if row_latest['close'] > row_latest['ema200'] and ema200_slope > 0 and row_latest['ema20'] >= row_latest['ema50']:
            return "long"
        if row_latest['close'] < row_latest['ema200'] and ema200_slope < 0 and row_latest['ema20'] <= row_latest['ema50']:
            return "short"
        return None

    def touch_pullback_recent(self, df: pd.DataFrame, side: str, lookback_bars: int = 2) -> bool:
        """За последние N баров был контакт с зоной [ema50; ema20]."""
        sub = df.tail(lookback_bars + 1)
        lo = sub['ema50'].min()
        hi = sub['ema20'].max()
        if side == "long":
            return ((sub['close'].between(lo, hi)) | (sub['low'] <= hi)).any()
        else:
            # зеркально (касание сверху)
            return ((sub['close'].between(lo, hi)) | (sub['high'] >= lo)).any()

    def confirm(self, latest_row, side: str) -> bool:
        if side == "long":
            return latest_row['close'] > latest_row['ema20']
        else:
            return latest_row['close'] < latest_row['ema20']

    def swing_extreme(self, df: pd.DataFrame, side: str, lookback: int = 5):
        idx = self.ind.swing_extreme(df, side, lookback)
        return idx

    def prev_extreme_target(self, df: pd.DataFrame, side: str, lookback: int = 20):
        """Простой таргет по предыдущему экстремуму за lookback баров."""
        if side == "long":
            return df['high'].tail(lookback + 1).max()
        else:
            return df['low'].tail(lookback + 1).min()
