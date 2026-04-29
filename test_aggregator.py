import time
import zmq
from loguru import logger
from core.logger import setup_logger
from core.bus import BusManager
from modules.aggregator import Aggregator

if __name__ == "__main__":
    setup_logger("INFO")
    
    # 1. Поднимаем шину
    bus = BusManager(pub_port=5555, rep_port=5556)
    bus.start()
    
    # 2. Создаём подписчика для проверки (эмулирует будущий Анализатор)
    sub = bus.context.socket(zmq.SUB)
    sub.connect("tcp://localhost:5555")
    sub.setsockopt_string(zmq.SUBSCRIBE, "DATA.CANDLE.5M")
    logger.info("👂 Test subscriber connected.")
    
    # 3. Запускаем Агрегатор
    agg = Aggregator("DOGEUSDT", "5m", zmq_pub_socket=bus.pub_socket)
    agg.start()
    
    try:
        logger.info("⏳ Waiting for live candles (approx 5 min)...")
        while True:
            # Ждём сообщение от шины
            topic = sub.recv_string()
            msg = sub.recv_json()
            logger.info(f"📦 Received: {topic} | Close: {msg['close']} | Vol: {msg['volume']}")
    except KeyboardInterrupt:
        logger.info("👋 Stopping...")
        agg.stop()
        bus.stop()