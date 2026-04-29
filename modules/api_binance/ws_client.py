import websocket
import json
import threading
import time
from loguru import logger

class BinanceWebSocket:
    """
    WebSocket клиент для получения Live-свечей.
    Работает в отдельном потоке. Автоматически переподключается.
    """

    def __init__(self, symbol: str, interval: str, callback):
        self.symbol = symbol.lower()
        self.interval = interval
        self.callback = callback  # Функция, которая примет данные
        self.ws = None
        self.keep_running = True

    def start(self):
        """Запуск в отдельном потоке (daemon), чтобы не блокировать main.py"""
        thread = threading.Thread(target=self._run)
        thread.daemon = True
        thread.start()
        logger.info(f"👂 WS Listener started for {self.symbol} {self.interval}")

    def _run(self):
        """Основной цикл с обработкой обрывов"""
        while self.keep_running:
            url = f"wss://stream.binance.com:9443/ws/{self.symbol}@kline_{self.interval}"
            try:
                logger.debug(f"🔌 Connecting WS: {url}")
                self.ws = websocket.WebSocketApp(url,
                                                  on_message=self.on_message,
                                                  on_error=self.on_error,
                                                  on_close=self.on_close,
                                                  on_open=self.on_open)
                self.ws.run_forever()
            except Exception as e:
                logger.error(f"⚠️ WS Crash: {e}")
            
            if self.keep_running:
                logger.warning("🔄 Reconnecting WS in 5s...")
                time.sleep(5)

    def stop(self):
        self.keep_running = False
        if self.ws:
            self.ws.close()

    def on_open(self, ws):
        logger.info(f"✅ WS Connected: {self.symbol} {self.interval}")

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            if 'k' in data:
                k = data['k']
                # 'x' = True означает, что свеча ЗАКРЫТА.
                # Мы ждем закрытия, чтобы записать финальную цену.
                if k['x']: 
                    candle = {
                        'timestamp': int(k['t']),
                        'open': float(k['o']),
                        'high': float(k['h']),
                        'low': float(k['l']),
                        'close': float(k['c']),
                        'volume': float(k['v'])
                    }
                    self.callback(candle)
        except Exception as e:
            logger.error(f"❌ WS Parse Error: {e}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.warning(f"⚠️ WS Closed: {close_status_code} {close_msg}")

    def on_error(self, ws, error):
        logger.error(f"💥 WS Error: {error}")