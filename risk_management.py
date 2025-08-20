from helpers import round_qty_to_step, round_to_step

class RiskManagement:
    def __init__(self, symbol_meta: dict = None):
        self.meta = symbol_meta or {"lot_step": 0.001, "tick_step": 0.1, "min_order_qty": 0.001}

    def set_symbol_meta(self, meta: dict):
        self.meta = meta

    def compute_sl(self, side: str, entry: float, swing: float, atr: float, atr_mult: float = 1.2) -> float:
        if side == "long":
            raw = min(swing, entry) - atr * atr_mult
        else:
            raw = max(swing, entry) + atr * atr_mult
        return round_to_step(raw, self.meta["tick_step"])

    def compute_tp(self, side: str, entry: float, stop: float, rr: float) -> float:
        if side == "long":
            raw = entry + rr * (entry - stop)
        else:
            raw = entry - rr * (stop - entry)
        return round_to_step(raw, self.meta["tick_step"])

    def position_from_risk(self, equity_usdt: float, risk_pct: float, entry: float, stop: float) -> float:
        """
        Для линейных контрактов qty ~ в базовой валюте.
        Риск $ ≈ |entry - stop| * qty  =>  qty = risk$ / |entry - stop|
        """
        risk_usd = equity_usdt * risk_pct
        dist = abs(entry - stop)
        if dist <= 0:
            return 0.0
        qty = risk_usd / dist
        qty = max(qty, self.meta["min_order_qty"])
        qty = round_qty_to_step(qty, self.meta["lot_step"])
        return qty
