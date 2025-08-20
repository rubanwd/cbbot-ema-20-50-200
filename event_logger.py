# event_logger.py
import os
import csv
import json
import time
from typing import Optional, Dict, Any

class EventLogger:
    def __init__(self, base_dir: str = "logs"):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)
        self.csv_path = os.path.join(self.base_dir, "trades.csv")
        self.jsonl_path = os.path.join(self.base_dir, "trades.jsonl")
        self._ensure_csv_header()

    def _ensure_csv_header(self):
        if not os.path.exists(self.csv_path) or os.path.getsize(self.csv_path) == 0:
            with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self._csv_fields())
                writer.writeheader()

    @staticmethod
    def _csv_fields():
        return [
            "ts_iso", "ts_ms",
            "event",                 # order_placed | position_closed | exit_trail | exit_overtime | exit_manual | error
            "symbol",
            "side",                  # Buy|Sell (для ордеров) / фактическая сторона закрываемой позиции
            "qty",
            "entry_price",
            "stop_loss",
            "take_profit",
            "reason",                # причина выхода/комментарий
            "order_id",
            "position_idx",
            "avg_price",
            "raw"                    # укороченный JSON-ответ / полезные детали
        ]

    def _now(self):
        ms = int(time.time() * 1000)
        iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ms / 1000))
        return iso, ms

    def log_event(
        self,
        event: str,
        symbol: str,
        side: Optional[str] = None,
        qty: Optional[float] = None,
        entry_price: Optional[float] = None,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        reason: Optional[str] = None,
        order_id: Optional[str] = None,
        position_idx: Optional[int] = None,
        avg_price: Optional[float] = None,
        extra: Optional[Dict[str, Any]] = None
    ):
        iso, ms = self._now()
        row = {
            "ts_iso": iso,
            "ts_ms": ms,
            "event": event,
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "reason": reason,
            "order_id": order_id,
            "position_idx": position_idx,
            "avg_price": avg_price,
            "raw": None
        }

        # Короткая версия raw для CSV (до 800 символов)
        raw_short = None
        if extra is not None:
            try:
                raw_str = json.dumps(extra, ensure_ascii=False, separators=(",", ":"))
                raw_short = raw_str[:800]
            except Exception:
                raw_short = str(extra)[:800]
        row["raw"] = raw_short

        # CSV
        with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self._csv_fields())
            writer.writerow(row)

        # Полная версия в JSONL
        full = {
            **row,
            "raw_full": extra  # полный payload
        }
        with open(self.jsonl_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(full, ensure_ascii=False) + "\n")
