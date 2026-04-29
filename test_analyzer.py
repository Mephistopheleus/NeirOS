import zmq
import time
from loguru import logger
from core.logger import setup_logger
from core.bus import BusManager
from modules.analyzer_math import AnalyzerMath

if __name__ == "__main__":
    setup_logger("INFO")
    bus = BusManager(pub_port=5555, rep_port=5556)
    bus.start()
    
    # ✅ ОТДЕЛЬНЫЙ SUB-сокет для прослушивания прогнозов
    sub = bus.context.socket(zmq.SUB)
    sub.connect("tcp://localhost:5555")
    sub.setsockopt_string(zmq.SUBSCRIBE, "ANALYSIS.FORECAST")
    
    # ✅ sub_socket = SUB, pub_socket = PUB
    analyzer = AnalyzerMath("DOGEUSDT", sub_socket=sub, pub_socket=bus.pub_socket)
    analyzer.start()
    
    try:
        logger.info("⏳ Waiting for forecasts (approx 1 min)...")
        while True:
            topic = sub.recv_string()
            msg = sub.recv_json()
            logger.info(f"📦 {topic} | Conf: {msg['overall_confidence']} | Trend: {msg['dominant_trend']} | Pred: {msg['timeframes'].get('5m',{}).get('predicted_price','N/A')}")
    except KeyboardInterrupt:
        analyzer.stop()
        bus.stop()