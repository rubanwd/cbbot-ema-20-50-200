import os
import time
import schedule
import logging
from typing import Dict, Optional

from dotenv import load_dotenv

from data_fetcher import DataFetcher
from strategies import Strategies
from risk_management import RiskManagement
from helpers import setup_logger


class SymbolContext:
    """
    Пер-символьное состояние для машины состояний.
    """
    __slots__ = (
        "state",
        "side",
        "setup_bar_ts",
        "last_bar_ts_processed",
        "last_closed_position_time",
        "position_open_time",
    )

    def __init__(self):
        self.state: str = "FLAT"
        self.side: Optional[str] = None
        self.setup_bar_ts: Optional[int] = None
        self.last_bar_ts_processed: Optional[int] = None
        self.last_closed_position_time: float = 0.0
        self.position_open_time: Optional[float] = None


class TradingBot:
    """
    Мульти-активный бот:
      - Раз в минуту проходит по списку символов
      - Для каждого символа — своя машина состояний и кулдаун
      - Вход по стратегии EMA-20/50/200 (1h), тейки/стопы/трейл/автозакрытие
    """
    def __init__(self):
        setup_logger()
        load_dotenv()

        self.api_key = os.getenv("BYBIT_API_KEY")
        self.api_secret = os.getenv("BYBIT_API_SECRET")
        if not self.api_key or not self.api_secret:
            raise ValueError("Set BYBIT_API_KEY / BYBIT_API_SECRET in .env")

        # ---- символы ----
        symbols_env = os.getenv("TRADING_SYMBOLS", "BTCUSDT")
        self.symbols = [s.strip() for s in symbols_env.split(",") if s.strip()]
        if not self.symbols:
            raise ValueError("TRADING_SYMBOLS is empty")

        # ---- общие параметры ----
        self.tf = os.getenv("TIMEFRAME", "60")  # "60" == 1h
        self.equity_usdt = float(os.getenv("EQUITY_USDT", 1000))
        self.risk_pct = float(os.getenv("RISK_PCT", 0.01))
        self.rr = float(os.getenv("RISK_RR", 2.0))
        self.leverage = int(os.getenv("LEVERAGE", 10))
        self.max_position_hours = float(os.getenv("POSITION_MAX_HOURS", 12))
        self.cooldown_seconds = int(os.getenv("COOLDOWN_SECONDS", 7200))

        # ---- сервисы ----
        self.fetcher = DataFetcher(self.api_key, self.api_secret)
        self.strategy = Strategies(self.fetcher)
        self.risk = RiskManagement()

        # ---- кэш метаданных и контексты по символам ----
        self.meta_map: Dict[str, dict] = {}
        self.ctx: Dict[str, SymbolContext] = {sym: SymbolContext() for sym in self.symbols}

        # предзагрузим метаданные (лот/тик/минимальный лот) для всех символов
        for sym in self.symbols:
            try:
                self.meta_map[sym] = self.fetcher.symbol_meta(sym)
                logging.info(f"[{sym}] Meta loaded: {self.meta_map[sym]}")
            except Exception as e:
                logging.exception(f"[{sym}] Failed to load symbol meta: {e}")
                # дефолтные шаги (на случай временной ошибки)
                self.meta_map[sym] = {"lot_step": 0.001, "tick_step": 0.1, "min_order_qty": 0.001}

    # ---------- утилиты ----------
    def _now(self) -> float:
        return time.time()

    def _cooldown_ok(self, sym: str) -> bool:
        c = self.ctx[sym]
        if c.last_closed_position_time <= 0:
            return True
        left = self.cooldown_seconds - (self._now() - c.last_closed_position_time)
        if left > 0:
            logging.info(f"[{sym}] Cooldown active: {int(left)}s left")
            return False
        return True

    def _auto_close_if_overtime(self, sym: str, open_positions) -> bool:
        """
        Если позиция старше max_position_hours — закрываем по рынку.
        """
        if not open_positions:
            return False
        pos = open_positions[0]
        size = float(pos.get('size', 0.0))
        if size == 0:
            return False

        c = self.ctx[sym]
        open_ms = pos.get('createdTime') or pos.get('updatedTime')
        if open_ms:
            opened_at = int(open_ms) / 1000
        elif c.position_open_time:
            opened_at = c.position_open_time
        else:
            return False

        age_sec = self._now() - opened_at
        if age_sec >= self.max_position_hours * 3600:
            logging.info(f"[{sym}] Position age {age_sec/3600:.2f}h >= {self.max_position_hours}h -> closing by market")
            side_in_position = pos.get('side')  # 'Buy' or 'Sell'
            self.fetcher.session.close_position(sym, size, side_in_position=side_in_position)
            c.last_closed_position_time = self._now()
            c.position_open_time = None
            c.state = "FLAT"
            return True
        return False

    # ---------- ядро для одного символа ----------
    def process_symbol(self, sym: str):
        # 1) данные
        df = self.fetcher.ohlcv(sym, self.tf, 500)
        df = self.strategy.prepare_dataframe(df)
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        c = self.ctx[sym]

        # реагируем только на закрытие НОВОЙ свечи 1h
        if c.last_bar_ts_processed is not None and latest['ts'] == c.last_bar_ts_processed:
            logging.info(f"[{sym}] No new bar yet.")
            return
        c.last_bar_ts_processed = latest['ts']

        # 2) позиция открыта? менеджим
        open_positions = self.fetcher.session.get_open_positions(sym)
        if open_positions:
            logging.info(f"[{sym}] Position is open -> manage")
            # автозакрытие по возрасту
            if self._auto_close_if_overtime(sym, open_positions):
                logging.info(f"[{sym}] Closed overtime position.")
            else:
                # трейл по EMA20 (1h): close против fast-EMA -> выход
                pos = open_positions[0]
                side_in_position = pos.get('side')   # 'Buy'|'Sell'
                size = float(pos.get('size', 0))
                if side_in_position == "Buy" and latest['close'] < latest['ema20']:
                    logging.info(f"[{sym}] Trail rule (below EMA20) -> exit long")
                    self.fetcher.session.close_position(sym, size, side_in_position="Buy")
                    c.last_closed_position_time = self._now()
                    c.state = "FLAT"
                elif side_in_position == "Sell" and latest['close'] > latest['ema20']:
                    logging.info(f"[{sym}] Trail rule (above EMA20) -> exit short")
                    self.fetcher.session.close_position(sym, size, side_in_position="Sell")
                    c.last_closed_position_time = self._now()
                    c.state = "FLAT"
            return

        # 3) когда позиции нет — сбрасываем "время открытия"
        c.position_open_time = None

        # 4) кулдаун
        if not self._cooldown_ok(sym):
            return

        # 5) фильтр тренда
        side = self.strategy.trend_side(prev, latest)  # 'long'|'short'|None
        if side is None:
            logging.info(f"[{sym}] No clear trend (EMA200 flat/cross). Skip.")
            c.state = "FLAT"
            return

        # 6) touch + confirm
        touched = self.strategy.touch_pullback_recent(df, side, lookback_bars=2)
        confirmed = self.strategy.confirm(latest, side)
        logging.info(f"[{sym}] Trend={side} | touched={touched} | confirmed={confirmed}")

        if not (touched and confirmed):
            c.state = "SETUP" if touched else "FLAT"
            return

        # --- 7) ВХОД ---
        entry = float(latest['close'])
        atr = float(latest['atr14'])
        swing_idx = self.strategy.swing_extreme(df, side, lookback=5)
        swing_val = float(df.loc[swing_idx, 'low' if side == "long" else 'high'])

        # актуализируем мету и риск-менеджер для этого символа
        meta = self.meta_map.get(sym) or {"lot_step": 0.001, "tick_step": 0.1, "min_order_qty": 0.001}
        self.risk.set_symbol_meta(meta)

        stop = self.risk.compute_sl(side, entry, swing_val, atr, atr_mult=1.2)
        qty = self.risk.position_from_risk(self.equity_usdt, self.risk_pct, entry, stop)
        tp = self.risk.compute_tp(side, entry, stop, self.rr)

        if qty <= 0:
            logging.info(f"[{sym}] Qty <= 0, skip.")
            return

        logging.info(f"[{sym}] ENTRY {side} | entry={entry} stop={stop} tp={tp} qty={qty}")

        try:
            # плечо на символ
            self.fetcher.session.set_leverage(sym, self.leverage)
            # рыночный ордер с TP/SL
            res = self.fetcher.session.place_order(
                symbol=sym,
                side="Buy" if side == "long" else "Sell",
                qty=qty,
                order_type="Market",
                stop_loss=stop,
                take_profit=tp
            )
            logging.info(f"[{sym}] Order placed: {res}")
            c.position_open_time = self._now()
            c.state = "ENTERED"
            c.side = side
        except Exception as e:
            logging.exception(f"[{sym}] Place order error: {e}")
            c.state = "FLAT"

    # ---------- общий цикл ----------
    def job(self):
        logging.info("====== Bot iteration (multi-symbol) ======")
        for i, sym in enumerate(self.symbols):
            try:
                self.process_symbol(sym)
            except Exception as e:
                logging.exception(f"[{sym}] Unexpected error: {e}")
            # мягкий интервал между символами, чтобы не бить лимиты
            time.sleep(0.2)

    def run(self):
        # первый проход сразу
        self.job()
        # затем каждую минуту
        schedule.every(60).seconds.do(self.job)
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    bot = TradingBot()
    bot.run()
