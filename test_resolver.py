import zmq
import time
from loguru import logger
from core.logger import setup_logger
from core.bus import BusManager
from modules.analyzer_math import AnalyzerMath
from modules.resolver_multitf import MultiTFResolver

if __name__ == "__main__":
    setup_logger("INFO")
    bus = BusManager(pub_port=5555, rep_port=5556)
    bus.start()
    
    # 1. Анализатор (генерирует прогнозы)
    sub_an = bus.context.socket(zmq.SUB)
    sub_an.connect("tcp://localhost:5555")
    sub_an.setsockopt_string(zmq.SUBSCRIBE, "ANALYSIS.FORECAST")
    analyzer = AnalyzerMath("DOGEUSDT", sub_socket=sub_an, pub_socket=bus.pub_socket)
    analyzer.start()
    
    # 2. Резолвер (читает прогнозы, выдаёт контекст)
    sub_res = bus.context.socket(zmq.SUB)
    sub_res.connect("tcp://localhost:5555")
    sub_res.setsockopt_string(zmq.SUBSCRIBE, "ANALYSIS.FORECAST")
    resolver = MultiTFResolver("DOGEUSDT", sub_socket=sub_res, pub_socket=bus.pub_socket)
    resolver.start()
    
    # 3. Подписчик на итоговый контекст (эмулирует TradeCalc/RiskManager)
    sub_ctx = bus.context.socket(zmq.SUB)
    sub_ctx.connect("tcp://localhost:5555")
    sub_ctx.setsockopt_string(zmq.SUBSCRIBE, "TRADE.CONTEXT")
    
    try:
        logger.info("⏳ Waiting for TRADE.CONTEXT (approx 1-2 min)...")
        while True:
            topic = sub_ctx.recv_string()
            msg = sub_ctx.recv_json()
            logger.info(f"🎯 {topic} | Dir: {msg['direction']} | Conf: {msg['final_confidence']} | Entry: {msg['entry_zone']} | Target: {msg['target']} | Stop: {msg['stop']}")
            logger.info(f"📝 Reasoning: {msg['reasoning']}")
    except KeyboardInterrupt:
        analyzer.stop()
        resolver.stop()
        bus.stop()