#!/usr/bin/env python3
"""
Order Executor — исполнение ордеров на Binance Testnet.
Вход: EXECUTION.ORDER
Действие: Отправка рыночного/лимитного ордера + установка SL/TP через OCO.
"""

import time
import hmac
import hashlib
import requests
from loguru import logger
from urllib.parse import urlencode


class OrderExecutor:
    """Исполняет одобренные риск-менеджером ордера на Binance."""
    
    def __init__(self, api_key: str, secret_key: str, base_url: str = "https://testnet.binance.vision"):
        self.api_key = api_key
        self.secret_key = secret_key
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "X-MBX-APIKEY": self.api_key,
            "Content-Type": "application/x-www-form-urlencoded"
        })
        
    def _sign_params(self, params: dict) -> dict:
        """Добавляет подпись к параметрам запроса."""
        params["timestamp"] = int(time.time() * 1000)
        query_string = urlencode(params)
        signature = hmac.new(
            self.secret_key.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        params["signature"] = signature
        return params
    
    def _request(self, method: str, endpoint: str, params: dict = None):
        """Отправляет подписанный запрос к API Binance."""
        url = f"{self.base_url}{endpoint}"
        if params:
            params = self._sign_params(params)
        
        try:
            if method == "GET":
                resp = self.session.get(url, params=params, timeout=5)
            elif method == "POST":
                resp = self.session.post(url, data=params, timeout=5)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ API request failed: {e}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            return None
    
    def get_balance(self, asset: str = "USDT") -> float:
        """Получает баланс актива."""
        data = self._request("GET", "/api/v3/account")
        if not data:
            return 0.0
        
        for balance in data.get("balances", []):
            if balance["asset"] == asset:
                return float(balance["free"])
        return 0.0
    
    def get_symbol_info(self, symbol: str) -> dict:
        """Получает информацию о символе (лоты, фильтры)."""
        data = self._request("GET", "/api/v3/exchangeInfo")
        if not data:
            return {}
        
        for s in data.get("symbols", []):
            if s["symbol"] == symbol:
                return s
        return {}
    
    def place_oco_order(self, symbol: str, side: str, quantity: float, 
                        entry_price: float, stop_price: float, take_profit_price: float) -> dict:
        """
        Размещает OCO ордер (One-Cancels-the-Other):
        - Лимитный ордер на вход (или рыночный, если нужно)
        - Stop-Loss и Take-Profit привязаны к позиции
        
        Для упрощения: сначала MARKET ордер на вход, затем OCO на выход.
        """
        logger.info(f"📤 Размещение MARKET {side} ордера на {quantity} {symbol}...")
        
        # 1. Рыночный ордер на вход
        order_params = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": quantity
        }
        
        result = self._request("POST", "/api/v3/order", order_params)
        if not result or "orderId" not in result:
            logger.error(f"❌ Не удалось открыть позицию: {result}")
            return {"status": "ERROR", "reason": "Entry order failed"}
        
        logger.success(f"✅ Позиция открыта! OrderID: {result['orderId']} | Price: {result.get('cummulativeQuoteQty', 'N/A')}")
        
        # 2. OCO ордер на выход (SL + TP)
        # Для LONG: Sell Limit @ TP, Sell Stop @ SL
        # Для SHORT: Buy Limit @ TP, Buy Stop @ SL
        oco_side = "SELL" if side == "BUY" else "BUY"
        
        # Корректируем цены под шаг тика
        tick_size = 0.00001  # Для DOGEUSDT, позже брать из exchangeInfo
        tp_rounded = round(take_profit_price / tick_size) * tick_size
        sl_rounded = round(stop_price / tick_size) * tick_size
        
        logger.info(f"📤 Размещение OCO {oco_side} ордера: TP={tp_rounded}, SL={sl_rounded}...")
        
        oco_params = {
            "symbol": symbol,
            "side": oco_side,
            "stopLimitTimeInForce": "GTC",
            "quantity": quantity,
            "price": tp_rounded,  # Лимитная цена для TP
            "stopPrice": sl_rounded,  # Стоп-цена для SL
            "stopLimitPrice": sl_rounded  # Лимитная цена после срабатывания стопа
        }
        
        oco_result = self._request("POST", "/api/v3/order/oco", oco_params)
        if not oco_result or "orderListId" not in oco_result:
            logger.error(f"⚠️ Не удалось установить OCO (возможно, требуется маржинальная торговля): {oco_result}")
            # Возвращаем успех по входу, но предупреждаем про SL/TP
            return {
                "status": "PARTIAL",
                "entry_order": result,
                "oco_order": None,
                "reason": "OCO setup failed"
            }
        
        logger.success(f"✅ OCO установлен! ListID: {oco_result['orderListId']}")
        
        return {
            "status": "SUCCESS",
            "entry_order": result,
            "oco_order": oco_result
        }
    
    def execute(self, order: dict) -> dict:
        """
        Исполняет одобренный ордер.
        order: словарь с полями qty, entry_price, stop_price, target_price, direction
        """
        symbol = order.get("symbol", "DOGEUSDT")
        qty = order.get("qty", 0)
        direction = order.get("direction", 0)
        stop_price = order.get("stop_price", 0)
        target_price = order.get("target_price", 0)
        
        if qty <= 0:
            return {"status": "ERROR", "reason": "Invalid quantity"}
        
        side = "BUY" if direction == 1 else "SELL"
        
        return self.place_oco_order(
            symbol=symbol,
            side=side,
            quantity=qty,
            entry_price=order.get("entry_price", 0),
            stop_price=stop_price,
            take_profit_price=target_price
        )
