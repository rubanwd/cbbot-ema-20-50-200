import requests
import time
import hashlib
import hmac
import json
import os
from typing import Optional, Dict, Any


class BybitDemoSession:
    """
    Простая совместимая обёртка под Bybit Demo v5 с подписью через параметры
    (api_key, timestamp, sign) — как в твоём рабочем примере.

    Особенности:
    - Гибкий place_order: принимает как (current_price, leverage, stop_loss, take_profit),
      так и (order_type, price, stop_loss, take_profit, reduce_only).
    - positionIdx берём из режима:
        * one_way -> 0
        * hedge   -> 1 (для Buy), 2 (для Sell)
      Режим можно задать через env POSITION_MODE=one_way|hedge (по умолчанию one_way).
    - set_leverage: игнорирует retCode 110043 ("leverage not modified"), чтобы не падать.
    """

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api-demo.bybit.com"
        # режим позиции из .env, по умолчанию one_way
        self.position_mode = os.getenv("POSITION_MODE", "one_way").strip().lower()
        self._detected_mode = None  # кеш: "one_way" | "hedge"

    # ---------- подпись и базовый запрос ----------
    def _generate_signature(self, params: Dict[str, Any]) -> str:
        # ВАЖНО: сортируем по ключу, как в твоём примере
        param_str = '&'.join([f'{k}={params[k]}' for k in sorted(params)])
        return hmac.new(self.api_secret.encode('utf-8'), param_str.encode('utf-8'), hashlib.sha256).hexdigest()

    def _get_timestamp(self) -> str:
        return str(int(time.time() * 1000))

    def send_request(self, method: str, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if params is None:
            params = {}

        # подпись для v5 как в твоём «рабочем» коде
        params['api_key'] = self.api_key
        params['timestamp'] = self._get_timestamp()
        params['sign'] = self._generate_signature(params)

        url = f"{self.base_url}{endpoint}"
        if method.upper() == "GET":
            resp = requests.get(url, params=params, timeout=15)
        elif method.upper() == "POST":
            resp = requests.post(url, json=params, timeout=15)
        else:
            raise ValueError("Unsupported HTTP method")
        return resp.json()

    # ---------- Public ----------
    def get_historical_data(self, symbol: str, interval: str, limit: int):
        try:
            endpoint = "/v5/market/kline"
            params = {
                "category": "linear",
                "symbol": symbol,
                "interval": interval,
                "limit": limit
            }
            r = self.send_request("GET", endpoint, params)
            if r.get('retCode') != 0:
                raise Exception(f"API Error: {r.get('retMsg')}")
            return r['result']['list']
        except Exception as e:
            print(f"Error fetching historical data: {e}")
            return None

    def get_real_time_price(self, symbol: str) -> Optional[float]:
        try:
            endpoint = "/v5/market/tickers"
            params = {"category": "linear", "symbol": symbol}
            r = self.send_request("GET", endpoint, params)
            if r.get('retCode') != 0:
                raise Exception(f"API Error: {r.get('retMsg')}")
            return float(r['result']['list'][0]['lastPrice'])
        except Exception as e:
            print(f"Ошибка при получении текущей цены: {e}")
            return None

    # ---------- Private ----------
    def set_leverage(self, symbol: str, leverage: int):
        try:
            endpoint = "/v5/position/set-leverage"
            params = {
                "category": "linear",
                "symbol": symbol,
                "buyLeverage": str(leverage),
                "sellLeverage": str(leverage)
            }
            r = self.send_request("POST", endpoint, params)
            # игнорируем 110043: leverage not modified
            if r.get('retCode') not in (0, 110043):
                raise Exception(f"API Error: {r.get('retMsg')}")
            if r.get('retCode') == 0:
                print(f"Leverage set to {leverage}x for {symbol}.")
            else:
                print(f"Leverage already {leverage}x for {symbol}.")
        except Exception as e:
            print(f"Ошибка при установке плеча: {e}")

    def _position_idx(self, side: str) -> int:
        """
        Определяем positionIdx по установленному режиму (env POSITION_MODE).
        """
        if self.position_mode == "hedge":
            return 1 if side.lower() == "buy" else 2
        return 0  # one_way

    def place_order(self, symbol, side, qty, current_price=None, leverage=None,
                    order_type=None, price=None, stop_loss=None, take_profit=None, reduce_only=False):
        try:
            if leverage is not None:
                self.set_leverage(symbol, leverage)

            endpoint = "/v5/order/create"

            # Маппинг "long/short" -> "Buy/Sell"
            side_norm = side.lower()
            if side_norm in ("long", "buy"):
                api_side = "Buy"
            elif side_norm in ("short", "sell"):
                api_side = "Sell"
            else:
                raise ValueError(f"Unsupported side value: {side}")

            position_idx = self._detect_position_mode_and_idx(symbol, api_side)

            orderType = (order_type or "Market").capitalize()  # Market|Limit

            params = {
                "category": "linear",
                "symbol": symbol,
                "side": api_side,
                "orderType": orderType,
                "qty": str(qty),
                "positionIdx": position_idx,
            }
            if orderType == "Limit":
                px = price if price is not None else current_price
                if px is None:
                    raise ValueError("Limit order requires price/current_price")
                params["price"] = str(px)

            if stop_loss is not None:
                params["stopLoss"] = str(stop_loss)
            if take_profit is not None:
                params["takeProfit"] = str(take_profit)

            if reduce_only:
                params["reduceOnly"] = True

            r = self.send_request("POST", endpoint, params)
            if r.get("retCode") != 0:
                raise Exception(f"API Error: {r.get('retMsg')}")
            return r["result"]
        except Exception as e:
            print(f"Error placing order: {e}")
            return None

    def close_position(self, symbol, size, side_in_position=None):
        try:
            endpoint = "/v5/order/create"
            if side_in_position:
                close_side = "Sell" if side_in_position == "Buy" else "Buy"
            else:
                close_side = "Sell" if float(size) > 0 else "Buy"

            position_idx = self._detect_position_mode_and_idx(symbol, close_side)

            params = {
                "category": "linear",
                "symbol": symbol,
                "side": close_side,
                "orderType": "Market",
                "qty": str(abs(float(size))),
                "reduceOnly": True,
                "positionIdx": position_idx
            }
            r = self.send_request("POST", endpoint, params)
            if r.get('retCode') != 0:
                raise Exception(f"API Error: {r.get('retMsg')}")
            print(f"Position closed successfully: {r}")
            return r
        except Exception as e:
            print(f"Error closing position: {e}")
            return None


    def get_open_positions(self, symbol: str):
        try:
            endpoint = "/v5/position/list"
            params = {"category": "linear", "symbol": symbol}
            r = self.send_request("GET", endpoint, params)
            if r.get('retCode') != 0:
                raise Exception(f"API Error: {r.get('retMsg')}")
            positions = r['result']['list']
            active = [p for p in positions if float(p.get('size', 0.0)) > 0]
            if active:
                print("Active Open Positions:")
                print(json.dumps(active, indent=4))
            else:
                print("No opened positions.")
            return active
        except Exception as e:
            print(f"Ошибка при получении позиций: {e}")
            return None

    def get_last_closed_position(self, symbol: str):
        try:
            endpoint = "/v5/position/list"
            params = {"category": "linear", "symbol": symbol}
            r = self.send_request("GET", endpoint, params)
            if r.get('retCode') != 0:
                raise Exception(f"API Error: {r.get('retMsg')}")
            positions = r['result']['list']
            closed = [p for p in positions if float(p.get('size', 0.0)) == 0.0]
            if closed:
                last_closed = max(closed, key=lambda x: int(x['updatedTime']))
                return last_closed
            return None
        except Exception as e:
            print(f"Error fetching last closed position: {e}")
            return None

    def get_open_orders(self, symbol: str):
        try:
            endpoint = "/v5/order/realtime"
            params = {"category": "linear", "symbol": symbol}
            r = self.send_request("GET", endpoint, params)
            if r.get('retCode') != 0:
                raise Exception(f"API Error: {r.get('retMsg')}")
            orders = r['result']['list']
            # авто-отмена «старых» лимиток > 3 минут (оставляю как у тебя)
            now = time.time()
            to_cancel = []
            for o in orders:
                created = int(o['createdTime']) / 1000
                if now - created > 180:
                    to_cancel.append(o)
            for o in to_cancel:
                self.cancel_order(o['orderId'], symbol)
                print(f"Order {o['orderId']} cancelled as it was older than 3 minutes.")
            return orders
        except Exception as e:
            print(f"Ошибка при получении лимитных ордеров: {e}")
            return None

    def cancel_order(self, order_id: str, symbol: str):
        try:
            endpoint = "/v5/order/cancel"
            params = {"category": "linear", "symbol": symbol, "orderId": order_id}
            r = self.send_request("POST", endpoint, params)
            if r.get('retCode') != 0:
                raise Exception(f"API Error: {r.get('retMsg')}")
            print(f"Order {order_id} successfully cancelled.")
        except Exception as e:
            print(f"Ошибка при отмене ордера {order_id}: {e}")

    def get_instrument_info(self, symbol: str):
        try:
            endpoint = "/v5/market/instruments-info"
            params = {
                "category": "linear",
                "symbol": symbol
            }
            r = self.send_request("GET", endpoint, params)
            if r.get('retCode') != 0:
                raise Exception(f"API Error: {r.get('retMsg')}")
            if not r['result']['list']:
                raise Exception(f"No instrument info for {symbol}")
            info = r['result']['list'][0]
            return {
                "lot_step": float(info['lotSizeFilter']['qtyStep']),
                "tick_step": float(info['priceFilter']['tickSize']),
                "min_order_qty": float(info['lotSizeFilter']['minOrderQty'])
            }
        except Exception as e:
            print(f"Ошибка при получении инструмента {symbol}: {e}")
            return None
        
        


    def _detect_mode(self, symbol: str) -> str:
        """
        Определяет режим аккаунта по /v5/position/list.
        Если в списке позиций встречаются positionIdx 1/2 — это hedge.
        Иначе считаем one_way. Результат кешируется.
        """
        if self._detected_mode:
            return self._detected_mode
        try:
            r = self.send_request("GET", "/v5/position/list", {
                "category": "linear",
                "symbol": symbol
            })
            if r.get("retCode") != 0:
                raise Exception(r.get("retMsg"))
            lst = r["result"]["list"]
            # если есть записи с idx 1/2 — режим hedge
            for p in lst:
                idx = int(p.get("positionIdx", 0))
                if idx in (1, 2):
                    self._detected_mode = "hedge"
                    break
            if not self._detected_mode:
                self._detected_mode = "one_way"
        except Exception:
            # на всякий случай fallback: если .env явно задан — вернём его
            if self.position_mode in ("one_way", "hedge"):
                self._detected_mode = self.position_mode
            else:
                self._detected_mode = "one_way"
        return self._detected_mode

    def _detect_position_mode_and_idx(self, symbol: str, side: str) -> int:
        """
        Возвращает корректный positionIdx для текущего режимa.
        - one_way -> 0
        - hedge   -> 1 для Buy (лонг), 2 для Sell (шорт)
        """
        mode = self._detect_mode(symbol)
        s = side.lower()
        if mode == "hedge":
            return 1 if s == "buy" else 2
        return 0

