import math
import logging

def round_to_step(value: float, step: float) -> float:
    if step is None or step == 0:
        return value
    return math.floor(value / step) * step

def round_qty_to_step(qty: float, step: float) -> float:
    if step is None or step == 0:
        return qty
    precision = max(0, -int(math.floor(math.log10(step))) if step < 1 else 0)
    return float(f"{math.floor(qty / step) * step:.{precision}f}")

def slope(series, lookback: int = 1):
    if len(series) < lookback + 1:
        return 0.0
    return series.iloc[-1] - series.iloc[-1 - lookback]

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("trading_bot.log", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )
