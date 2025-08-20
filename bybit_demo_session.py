import requests
import time
import hashlib
import hmac
import json
from typing import Dict, Any, Optional


class BybitDemoSession:
    """
    Лёгкая обёртка над Bybit v5 (Demo).
    Публичные эндпоинты без подписи, приватные — через заголовки v5.
    """

    def __init__(self, api_key: str, api_secret: str, base_url: Optional[str] = None, recv_window: int = 5000):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url or "https://api-demo.bybit.com"
        self.recv_window = recv_window

    # -------------------- helpers --------------------

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    def _sign_v5(self, ts: int, body_str: str) -> str:
        """
        prehash = str(ts) + api_key + str(recv_window) + body_str
        body_str: JSON для POST, "" для GET.
        """
        prehash = f"{ts}{self.api_key}{self.recv_window}{body_str}"
        return hmac.new(self.api_secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).hexdigest()

    def _request_public(self, method: str, endpoint: str, params: Dict[str, Any] = None):
        url = f"{self.base_url}{endpoint}"
        if method.upper() == "GET":
            r = requests.get(url, params=params or {}, timeout=15)
        else:
            r = requests.post(url, json=params or {}, timeout=15)
        r.raise_for_status()
        data = r.json()
        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit error: {data.get('retMsg')} | {json.dumps(data)[:300]}")
        return data.get("result")

    def _request_private(self, method: str, endpoint: str, params: Dict[str, Any] = None):
        """
        v5 private auth via headers.
        GET: sign over sorted query string.
        POST: sign over compact JSON body.
        """
        url = f"{self.base_url}{endpoint}"
        params = params or {}
        ts = self._now_ms()
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": str(ts),
            "X-BAPI-RECV-WINDOW": str(self.recv_window),
            "Content-Type": "application/json",
        }

        if method.upper() == "GET":
            # canonical query string: keys sorted ascending
            qs = "&".join(f"{k}={params[k]}" for k in sorted(params))
            prehash = f"{ts}{self.api_key}{self.recv_window}{qs}"
            headers["X-BAPI-SIGN"] = hmac.new(
                self.api_secret.encode("utf-8"),
                prehash.encode("utf-8"),
                hashlib.sha256
            ).hexdigest()
            r = requests.get(url, params=params, headers=headers, timeout=15)
        else:
            body_str = json.dumps(params, separators=(",", ":"), ensure_ascii=False)
            prehash = f"{ts}{self.api_key}{self.recv_window}{body_str}"
            headers["X-BAPI-SIGN"] = hmac.new(
                self.api_secret.encode("utf-8"),
                prehash.encode("utf-8"),
                hashlib.sha256
            ).hexdigest()
            r = requests.post(url, data=body_str, headers=headers, timeout=15)

        r.raise_for_status()
        data = r.json()
        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit error: {data.get('retMsg')} | {json.dumps(data)[:300]}")
        return data.get("result")


    # -------------------- Market (public) --------------------

    def get_historical_data(self, symbol: str, interval: str, limit: int = 400):
        """
        interval: "60" для 1h, "15" для 15m и т.д.
        """
        return self._request_public("GET", "/v5/market/kline", {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        })["list"]

    def get_real_time_price(self, symbol: str) -> float:
        res = self._request_public("GET", "/v5/market/tickers", {
            "category": "linear",
            "symbol": symbol
        })
        return float(res["list"][0]["lastPrice"])

    def get_instrument_info(self, symbol: str):
        res = self._request_public("GET", "/v5/market/instruments-info", {
            "category": "linear",
            "symbol": symbol
        })
        info = res["list"][0]
        lot_step = float(info["lotSizeFilter"]["qtyStep"])
        tick_step = float(info["priceFilter"]["tickSize"])
        min_order_qty = float(info["lotSizeFilter"]["minOrderQty"])
        return {
            "lot_step": lot_step,
            "tick_step": tick_step,
            "min_order_qty": min_order_qty
        }

    # -------------------- Trading (private) --------------------

    def set_leverage(self, symbol: str, leverage: int):
        return self._request_private("POST", "/v5/position/set-leverage", {
            "category": "linear",
            "symbol": symbol,
            "buyLeverage": str(leverage),
            "sellLeverage": str(leverage)
        })

    def place_order(
        self,
        symbol: str,
        side: str,                 # "Buy" | "Sell"
        qty: float,
        order_type: str = "Market",
        price: Optional[float] = None,
        position_mode: str = "one_way",
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None
    ):
        position_idx = 0 if position_mode == "one_way" else (1 if side.lower() == "buy" else 2)
        params: Dict[str, Any] = {
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": order_type,
            "qty": str(qty),
            "positionIdx": position_idx
        }
        if order_type == "Limit" and price is not None:
            params["price"] = str(price)
        if stop_loss is not None:
            params["stopLoss"] = str(stop_loss)
        if take_profit is not None:
            params["takeProfit"] = str(take_profit)

        return self._request_private("POST", "/v5/order/create", params)

    def close_position(self, symbol: str, size: float, side_in_position: Optional[str] = None):
        """
        Закрыть позицию рынком с reduceOnly.
        side_in_position: "Buy" (лонг) / "Sell" (шорт)
        """
        close_side = "Sell" if side_in_position == "Buy" else "Buy"
        params = {
            "category": "linear",
            "symbol": symbol,
            "side": close_side,
            "orderType": "Market",
            "qty": str(abs(float(size))),
            "reduceOnly": True,
            "positionIdx": 0
        }
        return self._request_private("POST", "/v5/order/create", params)

    def get_open_positions(self, symbol: str):
        res = self._request_private("GET", "/v5/position/list", {
            "category": "linear",
            "symbol": symbol
        })
        positions = res.get("list", [])
        return [p for p in positions if float(p.get("size", 0)) > 0]

    def get_open_orders(self, symbol: str):
        res = self._request_private("GET", "/v5/order/realtime", {
            "category": "linear",
            "symbol": symbol
        })
        return res.get("list", [])

    def cancel_order(self, order_id: str, symbol: str):
        return self._request_private("POST", "/v5/order/cancel", {
            "category": "linear",
            "symbol": symbol,
            "orderId": order_id
        })
