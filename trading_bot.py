# trading_bot.py
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
from event_logger import EventLogger


class SymbolContext:
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
    Мульти-активный бот по EMA-20/50/200 (1h).
    Теперь:
      - на каждой итерации логируются EMA20/EMA50/EMA200 и строка Trend/touched/confirmed по каждому символу
      - все ордера/закрытия пишутся в logs/trades.csv и logs/trades.jsonl
    """
    def __init__(self):
        setup_logger()
        load_dotenv()

        self.api_key = os.getenv("BYBIT_API_KEY")
        self.api_secret = os.getenv("BYBIT_API_SECRET")
        if not self.api_key or not self.api_secret:
            raise ValueError("Set BYBIT_API_KEY / BYBIT_API_SECRET in .env")

        symbols_env = os.getenv("TRADING_SYMBOLS", "BTCUSDT")
        self.symbols = [s.strip() for s in symbols_env.split(",") if s.strip()]
        if not self.symbols:
            raise ValueError("TRADING_SYMBOLS is empty")

        self.tf = os.getenv("TIMEFRAME", "60")  # 1h
        self.equity_usdt = float(os.getenv("EQUITY_USDT", 1000))
        self.risk_pct = float(os.getenv("RISK_PCT", 0.01))
        self.rr = float(os.getenv("RISK_RR", 2.0))
        self.leverage = int(os.getenv("LEVERAGE", 10))
        self.max_position_hours = float(os.getenv("POSITION_MAX_HOURS", 12))
        self.cooldown_seconds = int(os.getenv("COOLDOWN_SECONDS", 7200))

        self.fetcher = DataFetcher(self.api_key, self.api_secret)
        self.strategy = Strategies(self.fetcher)
        self.risk = RiskManagement()
        self.ev = EventLogger()

        self.meta_map: Dict[str, dict] = {}
        self.ctx: Dict[str, SymbolContext] = {sym: SymbolContext() for sym in self.symbols}

        for sym in self.symbols:
            try:
                self.meta_map[sym] = self.fetcher.symbol_meta(sym)
                logging.info(f"[{sym}] Meta loaded: {self.meta_map[sym]}")
            except Exception as e:
                logging.exception(f"[{sym}] Failed to load symbol meta: {e}")
                self.meta_map[sym] = {"lot_step": 0.001, "tick_step": 0.1, "min_order_qty": 0.001}

    # ---------- utils ----------
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
            try:
                res = self.fetcher.session.close_position(sym, size, side_in_position=side_in_position)
                self.ev.log_event(
                    event="exit_overtime",
                    symbol=sym,
                    side=side_in_position,
                    qty=size,
                    reason=f"overtime >= {self.max_position_hours}h",
                    extra={"api_response": res}
                )
            except Exception as e:
                self.ev.log_event(
                    event="error",
                    symbol=sym,
                    side=side_in_position,
                    qty=size,
                    reason=f"close_overtime_error: {e}",
                    extra={"exception": str(e)}
                )
                raise

            c.last_closed_position_time = self._now()
            c.position_open_time = None
            c.state = "FLAT"
            return True
        return False

    # ---------- per-symbol core ----------
    def process_symbol(self, sym: str):
        # 1) данные
        df = self.fetcher.ohlcv(sym, self.tf, 500)
        df = self.strategy.prepare_dataframe(df)
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        c = self.ctx[sym]

        # EMA логируем всегда (две ситуации ниже)
        ema20, ema50, ema200 = float(latest['ema20']), float(latest['ema50']), float(latest['ema200'])

        # реагируем только на закрытие НОВОЙ свечи 1h
        if c.last_bar_ts_processed is not None and latest['ts'] == c.last_bar_ts_processed:
            logging.info(f"[{sym}] No new bar yet.")

            # ---- ДОБАВЛЕНО: всегда логируем Trend/touched/confirmed даже без нового бара ----
            side_tmp = self.strategy.trend_side(prev, latest)  # 'long'|'short'|None
            if side_tmp is None:
                touched_tmp = False
                confirmed_tmp = False
                side_print = "none"
            else:
                touched_tmp = self.strategy.touch_pullback_recent(df, side_tmp, lookback_bars=2)
                confirmed_tmp = self.strategy.confirm(latest, side_tmp)
                side_print = side_tmp
            logging.info(f"[{sym}] Trend={side_print} | touched={touched_tmp} | confirmed={confirmed_tmp}")
            # -----------------------------------------------------------------------------

            logging.info(f"[{sym}] EMA20={ema20:.6f} | EMA50={ema50:.6f} | EMA200={ema200:.6f}")
            return
        c.last_bar_ts_processed = latest['ts']

        # 2) позиция открыта? менеджим
        open_positions = self.fetcher.session.get_open_positions(sym)
        if open_positions:
            logging.info(f"[{sym}] Position is open -> manage")
            # текущие EMA
            logging.info(f"[{sym}] EMA20={ema20:.6f} | EMA50={ema50:.6f} | EMA200={ema200:.6f}")

            # автозакрытие по возрасту
            if self._auto_close_if_overtime(sym, open_positions):
                logging.info(f"[{sym}] Closed overtime position.")
            else:
                pos = open_positions[0]
                side_in_position = pos.get('side')   # 'Buy'|'Sell'
                size = float(pos.get('size', 0))
                if side_in_position == "Buy" and latest['close'] < latest['ema20']:
                    logging.info(f"[{sym}] Trail rule (below EMA20) -> exit long")
                    try:
                        res = self.fetcher.session.close_position(sym, size, side_in_position="Buy")
                        self.ev.log_event(
                            event="exit_trail",
                            symbol=sym,
                            side="Buy",
                            qty=size,
                            reason="close < EMA20 (1h)",
                            extra={"api_response": res}
                        )
                    except Exception as e:
                        self.ev.log_event(
                            event="error",
                            symbol=sym,
                            side="Buy",
                            qty=size,
                            reason=f"trail_exit_error: {e}",
                            extra={"exception": str(e)}
                        )
                        raise
                    c.last_closed_position_time = self._now()
                    c.state = "FLAT"
                elif side_in_position == "Sell" and latest['close'] > latest['ema20']:
                    logging.info(f"[{sym}] Trail rule (above EMA20) -> exit short")
                    try:
                        res = self.fetcher.session.close_position(sym, size, side_in_position="Sell")
                        self.ev.log_event(
                            event="exit_trail",
                            symbol=sym,
                            side="Sell",
                            qty=size,
                            reason="close > EMA20 (1h)",
                            extra={"api_response": res}
                        )
                    except Exception as e:
                        self.ev.log_event(
                            event="error",
                            symbol=sym,
                            side="Sell",
                            qty=size,
                            reason=f"trail_exit_error: {e}",
                            extra={"exception": str(e)}
                        )
                        raise
                    c.last_closed_position_time = self._now()
                    c.state = "FLAT"
            return

        # 3) когда позиции нет — сбрасываем "время открытия"
        c.position_open_time = None

        # 4) кулдаун
        if not self._cooldown_ok(sym):
            # тоже выводим Trend/touched/confirmed при кулдауне
            side_tmp = self.strategy.trend_side(prev, latest)
            if side_tmp is None:
                touched_tmp = False
                confirmed_tmp = False
                side_print = "none"
            else:
                touched_tmp = self.strategy.touch_pullback_recent(df, side_tmp, lookback_bars=2)
                confirmed_tmp = self.strategy.confirm(latest, side_tmp)
                side_print = side_tmp
            logging.info(f"[{sym}] Trend={side_print} | touched={touched_tmp} | confirmed={confirmed_tmp}")
            logging.info(f"[{sym}] EMA20={ema20:.6f} | EMA50={ema50:.6f} | EMA200={ema200:.6f}")
            return

        # 5) фильтр тренда
        side = self.strategy.trend_side(prev, latest)  # 'long'|'short'|None

        # 6) touch + confirm
        touched = False
        confirmed = False
        if side is not None:
            touched = self.strategy.touch_pullback_recent(df, side, lookback_bars=2)
            confirmed = self.strategy.confirm(latest, side)

        logging.info(f"[{sym}] Trend={side or 'none'} | touched={touched} | confirmed={confirmed}")
        logging.info(f"[{sym}] EMA20={ema20:.6f} | EMA50={ema50:.6f} | EMA200={ema200:.6f}")

        if side is None or not (touched and confirmed):
            c.state = "SETUP" if (side is not None and touched) else "FLAT"
            return

        # --- 7) ВХОД ---
        entry = float(latest['close'])
        atr = float(latest['atr14'])
        swing_idx = self.strategy.swing_extreme(df, side, lookback=5)
        swing_val = float(df.loc[swing_idx, 'low' if side == "long" else 'high'])

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
            self.fetcher.session.set_leverage(sym, self.leverage)
            res = self.fetcher.session.place_order(
                symbol=sym,
                side="Buy" if side == "long" else "Sell",
                qty=qty,
                order_type="Market",
                stop_loss=stop,
                take_profit=tp
            )

            # Попробуем вытащить полезные ID/цены из ответа (если есть)
            order_id = None
            avg_price = None
            position_idx = 0
            if isinstance(res, dict):
                order_id = res.get("orderId") or res.get("orderIdStr") or res.get("orderIdLong")
                avg_price = res.get("avgPrice") or res.get("price")
                position_idx = res.get("positionIdx", 0)

            self.ev.log_event(
                event="order_placed",
                symbol=sym,
                side="Buy" if side == "long" else "Sell",
                qty=qty,
                entry_price=entry,       # ожидаемая цена (факт. средняя может отличаться)
                stop_loss=stop,
                take_profit=tp,
                order_id=order_id,
                position_idx=position_idx,
                avg_price=avg_price,
                extra={"api_response": res, "ema20": ema20, "ema50": ema50, "ema200": ema200}
            )

            logging.info(f"[{sym}] Order placed: {res}")
            c.position_open_time = self._now()
            c.state = "ENTERED"
            c.side = side
        except Exception as e:
            self.ev.log_event(
                event="error",
                symbol=sym,
                side="Buy" if side == "long" else "Sell",
                qty=qty,
                entry_price=entry,
                stop_loss=stop,
                take_profit=tp,
                reason=f"place_order_error: {e}",
                extra={"exception": str(e)}
            )
            logging.exception(f"[{sym}] Place order error: {e}")
            c.state = "FLAT"

    # ---------- main loop ----------
    def job(self):
        logging.info("====== Bot iteration (multi-symbol) ======")
        for sym in self.symbols:
            try:
                self.process_symbol(sym)
            except Exception as e:
                logging.exception(f"[{sym}] Unexpected error: {e}")
            time.sleep(0.2)

    def run(self):
        self.job()
        schedule.every(60).seconds.do(self.job)
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    bot = TradingBot()
    bot.run()
