import pandas as pd
from bybit_demo_session import BybitDemoSession

class DataFetcher:
    def __init__(self, api_key: str, api_secret: str):
        self.session = BybitDemoSession(api_key, api_secret)

    def ohlcv(self, symbol: str, interval: str = "60", limit: int = 400) -> pd.DataFrame:
        """
        Возвращает DataFrame в хрон. порядке с колонками:
        ['ts','open','high','low','close','volume']
        """
        raw = self.session.get_historical_data(symbol, interval, limit)
        # Bybit часто отдаёт с "свежими в начале", нормализуем:
        rows = list(reversed(raw))
        df = pd.DataFrame(rows, columns=[
            'start', 'open', 'high', 'low', 'close', 'volume', 'turnover'
        ])
        df = df.astype({
            'start': 'int64',
            'open': 'float64',
            'high': 'float64',
            'low': 'float64',
            'close': 'float64',
            'volume': 'float64'
        })
        df.rename(columns={'start': 'ts'}, inplace=True)
        return df[['ts', 'open', 'high', 'low', 'close', 'volume']]

    def last_price(self, symbol: str) -> float:
        return self.session.get_real_time_price(symbol)

    def symbol_meta(self, symbol: str):
        return self.session.get_instrument_info(symbol)
