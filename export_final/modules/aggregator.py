import time
import threading
import zmq
from loguru import logger
from modules.api_binance.rest_client import BinanceRESTClient
from modules.api_binance.ws_client import BinanceWebSocket
from core.data_rotator import DataRotator

class Aggregator:
    """Card 2.3: Мульти-ТФ агрегатор. REST + WS по каждому ТФ → ZMQ PUB"""
    def __init__(self, symbol: str, intervals: list = None, zmq_pub_socket: zmq.Socket = None, data_path: str = "data"):
        self.symbol = symbol
        self.intervals = intervals or ["5m", "15m", "1h", "4h"]
        self.pub_socket = zmq_pub_socket
        self.running = False
        
        self.rest = BinanceRESTClient()
        self.rotator = DataRotator(symbol, base_path=data_path, flush_rows=50)
        self.ws_clients = {}
        self.buffer = []
        self.lock = threading.Lock()

    def start(self):
        logger.info(f"🔄 Aggregator starting for {self.symbol} on {self.intervals}...")
        self.running = True
        
        for tf in self.intervals:
            self._load_history(tf)
            # WS-клиент на каждый ТФ (Binance допускает параллельные подключения)
            self.ws_clients[tf] = BinanceWebSocket(self.symbol, tf, lambda c, t=tf: self._on_live_candle(c, t))
            self.ws_clients[tf].start()
            
        threading.Thread(target=self._publish_loop, daemon=True).start()
        logger.success("✅ Aggregator running (Multi-TF).")

    def _load_history(self, tf: str):
        logger.info(f"📥 Loading REST history for {tf}...")
        try:
            df = self.rest.fetch_history(self.symbol, tf, limit=500)
            if df.empty: return
            
            df_reset = df.reset_index()
            df_reset['timestamp'] = df_reset['timestamp'].astype(int) // 1_000_000
            for _, row in df_reset.iterrows():
                candle = row.to_dict()
                candle['_tf'] = tf
                self.buffer.append(candle)
                # В CSV сохраняем только 5m (база для бэктеста). Старшие ТФ живут в памяти.
                if tf == "5m":
                    self.rotator.push(candle)
            logger.info(f"✅ History {tf}: {len(df)} candles queued.")
        except Exception as e:
            logger.error(f"❌ History load failed for {tf}: {e}")

    def _on_live_candle(self, candle: dict, tf: str):
        with self.lock:
            candle['_tf'] = tf
            self.buffer.append(candle)
            if tf == "5m":
                self.rotator.push(candle)

    def _publish_loop(self):
        while self.running:
            batch = []
            with self.lock:
                if self.buffer:
                    batch = self.buffer.copy()
                    self.buffer.clear()
            
            for candle in batch:
                tf = candle.pop('_tf', '5m')
                if self.pub_socket:
                    topic = f"DATA.CANDLE.{tf.upper()}"
                    self.pub_socket.send_string(topic, zmq.SNDMORE)
                    self.pub_socket.send_json(candle)
            time.sleep(0.5)

    def stop(self):
        logger.info("🛑 Stopping Aggregator...")
        self.running = False
        for ws in self.ws_clients.values():
            ws.stop()
        self.rotator.force_close()
        logger.info("✅ Aggregator stopped.")